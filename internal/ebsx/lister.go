package ebsx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// defaultTokenTTL is assumed when the API does not report an expiry time.
// AWS documents block tokens as short-lived; 10 minutes is conservative.
const defaultTokenTTL = 10 * time.Minute

// refreshMargin: refresh tokens this long before their reported expiry.
const refreshMargin = 2 * time.Minute

// SnapshotInfo is the complete listing of a snapshot's allocated blocks.
type SnapshotInfo struct {
	SnapshotID string
	VolumeGiB  int64
	BlockSize  int64
	Blocks     []BlockRef // sorted by index
	// Pages is the number of ListSnapshotBlocks calls ListAll made
	// (token-refresh re-lists excluded). Used for cost estimation.
	Pages int
}

// AllocatedBlockCount returns the number of allocated blocks.
func (s *SnapshotInfo) AllocatedBlockCount() int { return len(s.Blocks) }

// Lister walks ListSnapshotBlocks pages and keeps a token cache with
// expiry so workers can refresh individual tokens later in the download.
type Lister struct {
	src        Source
	snapshotID string
	pageSize   int32
	store      *TokenStore
}

// NewLister creates a lister for one snapshot.
func NewLister(src Source, snapshotID string, pageSize int32) *Lister {
	if pageSize <= 0 {
		pageSize = DefaultListPageSize
	}
	return &Lister{src: src, snapshotID: snapshotID, pageSize: pageSize, store: NewTokenStore()}
}

// TokenStore caches block tokens keyed by block index, along with the
// expiry time of the listing that produced them. Concurrent-safe.
type TokenStore struct {
	mu     sync.Mutex
	tokens map[int64]tokenEntry
}

type tokenEntry struct {
	token  string
	expiry time.Time
}

func NewTokenStore() *TokenStore {
	return &TokenStore{tokens: make(map[int64]tokenEntry)}
}

// Get returns the cached token for a block index and whether it is fresh.
func (s *TokenStore) Get(index int64) (token string, fresh bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.tokens[index]
	if !ok {
		return "", false
	}
	return e.token, !expired(e.expiry)
}

// Inscribe stores tokens for a listing page batch.
func (s *TokenStore) Inscribe(blocks []BlockRef, expiry time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range blocks {
		s.tokens[b.Index] = tokenEntry{token: b.Token, expiry: expiry}
	}
}

// Drop invalidates the token for one index (forcing a refresh).
func (s *TokenStore) Drop(index int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, index)
}

func expired(expiry time.Time) bool {
	if expiry.IsZero() {
		return false // unknown expiry; treat as fresh until an API error says otherwise
	}
	return time.Now().After(expiry.Add(-refreshMargin))
}

// ListAll pages through ListSnapshotBlocks and returns the complete block
// list, validating geometry consistency across pages. Tokens are entered
// into the shared store.
func (l *Lister) ListAll(ctx context.Context) (*SnapshotInfo, error) {
	var (
		blocks    []BlockRef
		nextToken string
		volumeGiB int64
		blockSize int64
		pages     int
	)
	for page := 0; ; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := l.listWithRetry(ctx, nextToken, 0)
		if err != nil {
			return nil, fmt.Errorf("listing page %d: %w", page+1, err)
		}
		pages++
		if page == 0 {
			volumeGiB, blockSize = p.VolumeGiB, p.BlockSize
		} else if p.VolumeGiB != volumeGiB || p.BlockSize != blockSize {
			return nil, fmt.Errorf("snapshot %s changed geometry mid-listing (page %d): volume %d->%d GiB, block %d->%d",
				l.snapshotID, page+1, volumeGiB, p.VolumeGiB, blockSize, p.BlockSize)
		}
		blocks = append(blocks, p.Blocks...)
		l.store.Inscribe(p.Blocks, p.ExpiryTime)
		if p.NextToken == "" {
			break
		}
		nextToken = p.NextToken
	}
	// Sanity: strictly increasing indexes.
	for i := 1; i < len(blocks); i++ {
		if blocks[i].Index <= blocks[i-1].Index {
			return nil, fmt.Errorf("snapshot %s listing returned non-monotonic block index %d after %d",
				l.snapshotID, blocks[i].Index, blocks[i-1].Index)
		}
	}
	if volumeGiB <= 0 || blockSize <= 0 {
		return nil, fmt.Errorf("snapshot %s listing returned invalid geometry: volume %d GiB, block size %d",
			l.snapshotID, volumeGiB, blockSize)
	}
	return &SnapshotInfo{SnapshotID: l.snapshotID, VolumeGiB: volumeGiB, BlockSize: blockSize, Blocks: blocks, Pages: pages}, nil
}

// listWithRetry wraps ListPage with backoff for transient errors.
func (l *Lister) listWithRetry(ctx context.Context, nextToken string, startingIndex int64) (ListPage, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		p, err := l.src.ListPage(ctx, l.snapshotID, nextToken, startingIndex, l.pageSize)
		if err == nil {
			return p, nil
		}
		if !IsRetryable(err) {
			return ListPage{}, err
		}
		lastErr = err
		if err := Backoff(ctx, attempt, IsThrottle(err)); err != nil {
			return ListPage{}, err
		}
	}
	return ListPage{}, fmt.Errorf("giving up after 5 attempts: %w", lastErr)
}

// ErrBlockMissingFromListing is returned when a refresh listing does not
// contain a block that a previous listing reported as allocated.
var ErrBlockMissingFromListing = errors.New("block missing from refreshed listing")

// ListChangedAll pages through ListChangedBlocks between firstSnapshotID
// and the lister's snapshot (the newer one). Second-snapshot tokens from
// the changed pages are entered into the shared token store so workers
// can fetch them (and refresh them) exactly like full-listing tokens.
func (l *Lister) ListChangedAll(ctx context.Context, firstSnapshotID string) (changed []ChangedBlockRef, volumeGiB int64, blockSize int64, err error) {
	var nextToken string
	for page := 0; ; page++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}
		p, err := l.listChangedWithRetry(ctx, firstSnapshotID, nextToken, 0)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("changed-block listing page %d: %w", page+1, err)
		}
		if page == 0 {
			volumeGiB, blockSize = p.VolumeGiB, p.BlockSize
		} else if p.VolumeGiB != volumeGiB || p.BlockSize != blockSize {
			return nil, 0, 0, fmt.Errorf("snapshot %s changed geometry mid-listing (page %d): volume %d->%d GiB, block %d->%d",
				l.snapshotID, page+1, volumeGiB, p.VolumeGiB, blockSize, p.BlockSize)
		}
		// Only data-bearing entries carry fetchable tokens; index them in
		// the token store for the download workers.
		withTokens := make([]BlockRef, 0, len(p.Changed))
		for _, c := range p.Changed {
			if c.SecondToken != "" {
				withTokens = append(withTokens, BlockRef{Index: c.Index, Token: c.SecondToken})
			}
		}
		l.store.Inscribe(withTokens, p.ExpiryTime)
		changed = append(changed, p.Changed...)
		if p.NextToken == "" {
			break
		}
		nextToken = p.NextToken
	}
	for i := 1; i < len(changed); i++ {
		if changed[i].Index <= changed[i-1].Index {
			return nil, 0, 0, fmt.Errorf("changed-block listing for %s returned non-monotonic index %d after %d",
				l.snapshotID, changed[i].Index, changed[i-1].Index)
		}
	}
	if volumeGiB <= 0 || blockSize <= 0 {
		return nil, 0, 0, fmt.Errorf("changed-block listing for %s returned invalid geometry: volume %d GiB, block size %d",
			l.snapshotID, volumeGiB, blockSize)
	}
	return changed, volumeGiB, blockSize, nil
}

// listChangedWithRetry wraps ListChangedPage with backoff for transient errors.
func (l *Lister) listChangedWithRetry(ctx context.Context, firstID, nextToken string, startingIndex int64) (ChangedPage, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		p, err := l.src.ListChangedPage(ctx, firstID, l.snapshotID, nextToken, startingIndex, l.pageSize)
		if err == nil {
			return p, nil
		}
		if !IsRetryable(err) {
			return ChangedPage{}, err
		}
		lastErr = err
		if err := Backoff(ctx, attempt, IsThrottle(err)); err != nil {
			return ChangedPage{}, err
		}
	}
	return ChangedPage{}, fmt.Errorf("giving up after 5 attempts: %w", lastErr)
}

// RefreshToken re-lists starting at the given block index to obtain a
// fresh token for it (block tokens expire). The whole returned page is
// entered into the token store, so nearby refreshes are amortized.
func (l *Lister) RefreshToken(ctx context.Context, index int64) (BlockRef, error) {
	p, err := l.listWithRetry(ctx, "", index)
	if err != nil {
		return BlockRef{}, fmt.Errorf("refreshing token for block %d: %w", index, err)
	}
	l.store.Inscribe(p.Blocks, p.ExpiryTime)
	for _, b := range p.Blocks {
		if b.Index == index {
			return b, nil
		}
	}
	return BlockRef{}, fmt.Errorf("block %d: %w (snapshot geometry may have changed)", index, ErrBlockMissingFromListing)
}

// Store exposes the shared token store (used by the download workers).
func (l *Lister) Store() *TokenStore { return l.store }
