// Package testutil provides an in-memory fake of the EBS Direct APIs so
// the whole download pipeline can be exercised offline: pagination,
// token expiry, stale tokens, and per-block call accounting.
package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"snaport/internal/ebsx"
)

// FakeSnapshot implements ebsx.Source against an in-memory snapshot.
type FakeSnapshot struct {
	SnapshotID string
	VolumeGiB  int64
	BlockSize  int64

	// ListPageSize caps the number of blocks per ListPage response.
	ListPageSize int
	// TokenTTL is how long listing tokens stay valid.
	TokenTTL time.Duration

	// FailGetEveryNth injects a transient error on every Nth GetBlock
	// call (0 = never). The error is retryable-shaped.
	FailGetEveryNth int

	mu       sync.Mutex
	data     map[int64][]byte
	gen      atomic.Int64 // token generation; bumping invalidates all tokens
	expiry   time.Time
	failSeen atomic.Int64

	listCalls atomic.Int64
	getCalls  atomic.Int64
	perIndex  sync.Map // int64 → *atomic.Int64
}

// NewFake builds a fake snapshot. Block data is deterministic per index
// via a keyed hash; len(lastBlockLen) truncates the final listed block.
func NewFake(id string, volumeGiB, blockSize int64, indices []int64, lastBlockLen int64) *FakeSnapshot {
	if lastBlockLen <= 0 || lastBlockLen > blockSize {
		lastBlockLen = blockSize
	}
	f := &FakeSnapshot{
		SnapshotID:   id,
		VolumeGiB:    volumeGiB,
		BlockSize:    blockSize,
		ListPageSize: 10000,
		TokenTTL:     time.Hour,
		data:         make(map[int64][]byte, len(indices)),
	}
	sorted := append([]int64(nil), indices...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	seed := maphash.MakeSeed()
	for i, idx := range sorted {
		length := blockSize
		if i == len(sorted)-1 && idx == (volumeGiB*(1<<30))/blockSize-1 {
			// Only truncate when the listed last block really is the
			// volume's final block.
			length = lastBlockLen
		}
		f.data[idx] = blockContent(seed, id, idx, length)
	}
	return f
}

// blockContent derives deterministic pseudo-random block data from the
// snapshot ID and block index. Indexes that are a multiple of 97 offset
// by 13 are deliberately all zeros to exercise hole handling.
func blockContent(seed maphash.Seed, id string, idx, length int64) []byte {
	if idx%97 == 13 { // periodic all-zero allocated block
		return make([]byte, length)
	}
	payload := make([]byte, length)
	h := maphash.Hash{}
	h.SetSeed(seed)
	var num [8]byte
	s := uint64(0)
	for i := 0; i < len(payload); i++ {
		if i%8 == 0 {
			h.Reset()
			h.WriteString(id)
			binary.LittleEndian.PutUint64(num[:], uint64(idx*1000003+int64(i)))
			h.Write(num[:])
			s = h.Sum64()
		}
		payload[i] = byte(s >> (uint(i%8) * 8))
	}
	return payload
}

// SetBlock replaces one block's content (for corruption tests).
func (f *FakeSnapshot) SetBlock(index int64, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[index] = append([]byte(nil), data...)
}

// InvalidateTokens bumps the token generation: previously issued tokens
// become stale even if the store still considers them fresh.
func (f *FakeSnapshot) InvalidateTokens() { f.gen.Add(1) }

// ExpireTokens moves the listing expiry into the past so the token store
// refreshes proactively.
func (f *FakeSnapshot) ExpireTokens() {
	f.mu.Lock()
	f.expiry = time.Now().Add(-time.Second)
	f.mu.Unlock()
}

// ListCalls returns the number of ListPage invocations.
func (f *FakeSnapshot) ListCalls() int64 { return f.listCalls.Load() }

// GetCalls returns the total number of GetBlock invocations.
func (f *FakeSnapshot) GetCalls() int64 { return f.getCalls.Load() }

// GetCallsFor returns how many times one block index was fetched.
func (f *FakeSnapshot) GetCallsFor(index int64) int64 {
	if v, ok := f.perIndex.Load(index); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// AllocatedBytes sums the block lengths.
func (f *FakeSnapshot) AllocatedBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, b := range f.data {
		n += int64(len(b))
	}
	return n
}

// ListPage implements ebsx.Source.
func (f *FakeSnapshot) ListPage(ctx context.Context, snapshotID, nextToken string, startingIndex int64, maxResults int32) (ebsx.ListPage, error) {
	if snapshotID != f.SnapshotID {
		return ebsx.ListPage{}, fmt.Errorf("ResourceNotFoundException: snapshot %s not found", snapshotID)
	}
	f.listCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	indices := make([]int64, 0, len(f.data))
	for idx := range f.data {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	var start int
	if nextToken != "" {
		last, err := strconv.ParseInt(nextToken, 10, 64)
		if err != nil {
			return ebsx.ListPage{}, fmt.Errorf("ValidationException: invalid NextToken")
		}
		start = sort.Search(len(indices), func(i int) bool { return indices[i] > last })
	} else if startingIndex > 0 {
		start = sort.Search(len(indices), func(i int) bool { return indices[i] >= startingIndex })
	}

	pageSize := int(maxResults)
	if pageSize <= 0 || pageSize > f.ListPageSize {
		pageSize = f.ListPageSize
	}

	gen := f.gen.Load()
	end := min(start+pageSize, len(indices))
	page := ebsx.ListPage{
		VolumeGiB: f.VolumeGiB,
		BlockSize: f.BlockSize,
	}
	for _, idx := range indices[start:end] {
		page.Blocks = append(page.Blocks, ebsx.BlockRef{Index: idx, Token: f.token(gen, idx)})
	}
	if end < len(indices) {
		page.NextToken = strconv.FormatInt(indices[end-1], 10)
	}
	if f.expiry.IsZero() || time.Now().After(f.expiry) {
		f.expiry = time.Now().Add(f.TokenTTL)
	}
	page.ExpiryTime = f.expiry
	return page, nil
}

func (f *FakeSnapshot) token(gen int64, idx int64) string {
	return fmt.Sprintf("g%d-i%d", gen, idx)
}

// GetBlock implements ebsx.Source.
func (f *FakeSnapshot) GetBlock(ctx context.Context, snapshotID string, index int64, token string) (ebsx.BlockData, error) {
	if snapshotID != f.SnapshotID {
		return ebsx.BlockData{}, fmt.Errorf("ResourceNotFoundException: snapshot %s not found", snapshotID)
	}
	f.getCalls.Add(1)
	if v, _ := f.perIndex.LoadOrStore(index, &atomic.Int64{}); v != nil {
		v.(*atomic.Int64).Add(1)
	}

	if f.FailGetEveryNth > 0 && f.getCalls.Load()%int64(f.FailGetEveryNth) == 0 {
		f.failSeen.Add(1)
		return ebsx.BlockData{}, fmt.Errorf("RequestThrottledException: slow down")
	}

	var gen int64
	var tokIdx int64
	if _, err := fmt.Sscanf(token, "g%d-i%d", &gen, &tokIdx); err != nil || tokIdx != index {
		return ebsx.BlockData{}, fmt.Errorf("ValidationException: bad block token")
	}
	if gen != f.gen.Load() {
		return ebsx.BlockData{}, fmt.Errorf("AccessDeniedException: block token expired")
	}

	f.mu.Lock()
	data, ok := f.data[index]
	f.mu.Unlock()
	if !ok {
		return ebsx.BlockData{}, fmt.Errorf("ResourceNotFoundException: block %d not in snapshot", index)
	}
	sum := sha256.Sum256(data)
	return ebsx.BlockData{
		Data:   append([]byte(nil), data...),
		SHA256: base64.StdEncoding.EncodeToString(sum[:]),
	}, nil
}

// Data returns a copy of one block's content (for test assertions).
func (f *FakeSnapshot) Data(index int64) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.data[index]
	if !ok {
		return nil
	}
	return append([]byte(nil), d...)
}

// Indices returns the sorted allocated block indices.
func (f *FakeSnapshot) Indices() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, 0, len(f.data))
	for idx := range f.data {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

var _ ebsx.Source = (*FakeSnapshot)(nil)

// ScratchIndices builds a deterministic scattered index set covering the
// given count, always including index 0 and the final block index.
func ScratchIndices(count, finalIndex int64) []int64 {
	if count <= 0 {
		return nil
	}
	set := map[int64]bool{0: true, finalIndex: true}
	h := maphash.Hash{}
	h.WriteString("scratch")
	s := h.Sum64()
	for int64(len(set)) < count {
		s = s*2862933555777941757 + 3037000493
		idx := s % uint64(finalIndex)
		set[int64(idx)] = true
	}
	out := make([]int64, 0, len(set))
	for idx := range set {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
