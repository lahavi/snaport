package img

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// SZCWriter builds a chunked container from out-of-order block data.
// Worker goroutines Submit blocks as they arrive; an internal emitter
// writes them in logical-offset order, so at most the in-flight window
// (bounded by the submit channel) is ever buffered and the raw image is
// never materialized.
type SZCWriter struct {
	path       string // final .img.szc path
	partPath   string
	f          *os.File
	blockSize  int64
	logical    int64
	snapshotID string
	volumeGiB  int64

	enc *zstd.Encoder

	// ordered is the sorted list of logical offsets that will be
	// submitted; the emitter advances through it.
	ordered  []int64
	orderIdx int
	pending  map[int64][]byte

	submit    chan szcSubmit
	done      chan struct{}
	errMu     sync.Mutex
	err       error
	imageHash hash.Hash

	written int64 // file position after the fixed header
	chunks  []SZCChunk
}

// szcSubmit is one block handed to the emitter.
type szcSubmit struct {
	offset int64
	data   []byte
}

// NewSZCWriter creates the container at path (via a .part temp file).
// orderedOffsets is the sorted list of logical offsets expected to be
// submitted; blocks whose data is all zero may be skipped by the caller
// (they become holes) - Submit of zero data is also fine and treated as
// a hole.
func NewSZCWriter(path, snapshotID string, volumeGiB, blockSize, logical int64, orderedOffsets []int64, level int) (*SZCWriter, error) {
	part := path + ".part"
	f, err := os.Create(part)
	if err != nil {
		return nil, err
	}
	// Fixed header: magic + version; the trailer lives at the end.
	hdr := make([]byte, 8)
	copy(hdr[0:4], SZCMagic[:])
	binary.BigEndian.PutUint32(hdr[4:8], SZCVersion)
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return nil, err
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithEncoderConcurrency(2))
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &SZCWriter{
		path:       path,
		partPath:   part,
		f:          f,
		blockSize:  blockSize,
		logical:    logical,
		snapshotID: snapshotID,
		volumeGiB:  volumeGiB,
		enc:        enc,
		ordered:    append([]int64(nil), orderedOffsets...),
		orderIdx:   0,
		pending:    make(map[int64][]byte),
		submit:     make(chan szcSubmit, 64),
		done:       make(chan struct{}),
		imageHash:  sha256.New(),
		written:    8,
	}
	go w.emit()
	return w, nil
}

// Submit hands one fetched block to the container. It blocks when the
// emitter is behind, which - combined with dispatching fetch jobs in
// ordinal order - bounds buffering to roughly (in-flight workers +
// channel capacity) blocks. After the channel send the buffer belongs
// to the emitter.
func (w *SZCWriter) Submit(offset int64, data []byte) error {
	w.errMu.Lock()
	err := w.err
	w.errMu.Unlock()
	if err != nil {
		return err
	}
	w.submit <- szcSubmit{offset: offset, data: data}
	w.errMu.Lock()
	err = w.err
	w.errMu.Unlock()
	return err
}

// emit is the single writer goroutine: consumes submissions, stores
// chunks in offset order, hashes the virtual image (holes as zeros).
func (w *SZCWriter) emit() {
	defer close(w.done)
	zeros := make([]byte, 4096)
	var hashedPos int64
	zeroTo := func(end int64) {
		for hashedPos < end {
			n := min(end-hashedPos, int64(len(zeros)))
			w.imageHash.Write(zeros[:n])
			hashedPos += n
		}
	}

	fail := func(err error) {
		w.errMu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.errMu.Unlock()
	}

	// Drain mode: after a write error the emitter must keep consuming
	// (and discarding) submissions so no worker blocks on a full
	// channel.
	draining := false
	for sub := range w.submit {
		if draining {
			continue
		}
		w.pending[sub.offset] = sub.data
		// Advance as far as the ordered list allows.
		for w.orderIdx < len(w.ordered) {
			next := w.ordered[w.orderIdx]
			data, ok := w.pending[next]
			if !ok {
				// Drain any submissions already queued for later offsets
				// to keep the channel from clogging, then wait.
				break
			}
			delete(w.pending, next)
			w.orderIdx++

			zeroTo(next)
			if !isAllZero(data) {
				frame := w.enc.EncodeAll(data, nil)
				sum := sha256.Sum256(data)
				hdr := make([]byte, 16)
				binary.BigEndian.PutUint64(hdr[0:8], uint64(next))
				binary.BigEndian.PutUint32(hdr[8:12], uint32(len(data)))
				binary.BigEndian.PutUint32(hdr[12:16], uint32(len(frame)))
				if _, err := w.f.Write(hdr); err != nil {
					fail(fmt.Errorf("szc: writing chunk header: %w", err))
					draining = true
					continue
				}
				if _, err := w.f.Write(frame); err != nil {
					fail(fmt.Errorf("szc: writing chunk frame: %w", err))
					draining = true
					continue
				}
				w.chunks = append(w.chunks, SZCChunk{
					Offset:   next,
					Length:   int32(len(data)),
					StoreOff: w.written + 16,
					StoreLen: int32(len(frame)),
					SHA256:   hex.EncodeToString(sum[:]),
				})
				w.written += 16 + int64(len(frame))
				w.imageHash.Write(data)
				hashedPos = next + int64(len(data))
			} else {
				// All-zero block: hole; the virtual image hash already
				// advanced to `next`, zeros are hashed below on demand.
				hashedPos = next
			}
		}
	}
	// All submissions received: any offsets never submitted are treated
	// as holes (e.g. zero blocks the caller skipped).
	zeroTo(w.logical)
}

// Finalize writes the trailer, fsyncs and renames into place. It waits
// for the emitter to drain; Close must be called after.
func (w *SZCWriter) Finalize() error {
	close(w.submit)
	<-w.done
	w.errMu.Lock()
	err := w.err
	w.errMu.Unlock()
	if err != nil {
		w.Close()
		return err
	}
	if w.orderIdx != len(w.ordered) {
		w.Close()
		return fmt.Errorf("szc: %d of %d blocks were submitted", w.orderIdx, len(w.ordered))
	}

	t := SZCTrailer{
		Version:     SZCVersion,
		SnapshotID:  w.snapshotID,
		VolumeGiB:   w.volumeGiB,
		BlockSize:   w.blockSize,
		LogicalSize: w.logical,
		ImageSHA256: hex.EncodeToString(w.imageHash.Sum(nil)),
		Chunks:      w.chunks,
	}
	raw, err := json.Marshal(&t)
	if err != nil {
		w.Close()
		return err
	}
	footer := make([]byte, szcFooterLen)
	binary.BigEndian.PutUint64(footer[0:8], uint64(w.written))
	binary.BigEndian.PutUint32(footer[8:12], uint32(len(raw)))
	binary.BigEndian.PutUint32(footer[12:16], crc32.ChecksumIEEE(raw))
	if _, err := w.f.Write(raw); err != nil {
		w.Close()
		return err
	}
	if _, err := w.f.Write(footer); err != nil {
		w.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.Close()
		return err
	}
	if err := w.f.Close(); err != nil {
		os.Remove(w.partPath)
		return err
	}
	w.f = nil // already closed; Close() must not double-close
	return os.Rename(w.partPath, w.path)
}

// Close releases resources without finalizing (error path).
func (w *SZCWriter) Close() {
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
	os.Remove(w.partPath)
}

// StoredBytes reports bytes written so far (progress display).
func (w *SZCWriter) StoredBytes() int64 {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.written
}

// Err surfaces the first emitter error.
func (w *SZCWriter) Err() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.err
}

// ImageSHA256 returns the whole-image hash once Finalize succeeded.
func (w *SZCWriter) ImageSHA256() string { return hex.EncodeToString(w.imageHash.Sum(nil)) }

// SortedOffsets sanitizes and sorts an offset list for NewSZCWriter.
func SortedOffsets(offsets []int64) []int64 {
	out := append([]int64(nil), offsets...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
