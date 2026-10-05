package dl

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"snaport/internal/ebsx"
	"snaport/internal/img"
	"snaport/internal/manifest"
)

// Phase names reported through progress updates.
const (
	PhaseList     = "list"
	PhaseDownload = "download"
	PhaseVerify   = "verify"
	PhaseCompress = "compress"
)

// Options tunes Manager behavior.
type Options struct {
	// Concurrency is the maximum number of parallel GetSnapshotBlock
	// requests (subject to AIMD throttling). Default 32.
	Concurrency int
	// MaxAttempts is the per-block fetch attempt limit. Default 5.
	MaxAttempts int
	// ListPageSize is MaxResults per ListSnapshotBlocks call.
	ListPageSize int32
	// CheckpointEvery is how often the image is fsynced and the journal
	// flushed. Default 5s.
	CheckpointEvery time.Duration
	// ParanoidResume spot-checks already-downloaded blocks against their
	// recorded checksums before trusting resume state.
	ParanoidResume bool
	// NTFSCompress enables transparent NTFS compression on the raw image
	// (best effort, Windows only).
	NTFSCompress bool
	// ToolVersion is recorded in the manifest.
	ToolVersion string
	// Progress, when non-nil, receives progress updates. It must be safe
	// to call from worker goroutines.
	Progress func(Update)
}

// Update is a progress snapshot.
type Update struct {
	Phase       string
	BlocksDone  int
	BlocksTotal int
	BytesDone   int64
	BytesTotal  int64 // estimated: pending blocks are assumed full-size
	Concurrency int   // current AIMD limit
}

// CompressOptions controls the post-download zstd stage.
type CompressOptions struct {
	Level   int  // zstd level 1-19
	KeepRaw bool // keep the sparse raw image alongside the .zst
}

// Request describes one snapshot download.
type Request struct {
	SnapshotID   string
	ImagePath    string
	ManifestPath string
	StatePath    string // journal location
	Compress     *CompressOptions
	Force        bool // discard any existing state and restart
}

// Result summarizes a completed run.
type Result struct {
	Manifest       *manifest.Manifest
	NewBlocks      int // blocks fetched this run
	ResumedBlocks  int // blocks already complete from a previous run
	ZeroBlocks     int // allocated blocks whose data was all zeros (kept as holes)
	ImageSHA256    string
	ImageOnDisk    int64 // actual disk usage of the raw image, if kept
	CompressedSize int64
	Duration       time.Duration
}

// Manager downloads snapshots using a worker pool over a block source.
type Manager struct {
	src  ebsx.Source
	opts Options
}

// New creates a Manager over a block source.
func New(src ebsx.Source, opts Options) *Manager {
	fillDefaults(&opts)
	return &Manager{src: src, opts: opts}
}

func fillDefaults(o *Options) {
	if o.Concurrency <= 0 {
		o.Concurrency = 32
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.ListPageSize <= 0 {
		o.ListPageSize = ebsx.DefaultListPageSize
	}
	if o.CheckpointEvery <= 0 {
		o.CheckpointEvery = 5 * time.Second
	}
}

// Run executes the full pipeline for one snapshot: list, download
// (resumable), verify (restore-test), optionally compress + restore-test
// the archive, then finalize.
func (m *Manager) Run(ctx context.Context, req *Request) (*Result, error) {
	start := time.Now()

	if req.Force {
		for _, p := range []string{req.ManifestPath, req.StatePath, req.ImagePath, req.ImagePath + ".zst"} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return nil, fmt.Errorf("force-cleaning %s: %w", p, err)
			}
		}
	}

	m.progress(Update{Phase: PhaseList})
	lister := ebsx.NewLister(m.src, req.SnapshotID, m.opts.ListPageSize)
	info, err := lister.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing blocks of %s: %w", req.SnapshotID, err)
	}

	man, err := m.reconcileManifest(req, info)
	if err != nil {
		return nil, err
	}

	jrn, err := OpenJournal(req.StatePath, req.SnapshotID, len(info.Blocks))
	if err != nil {
		var mm *JournalMismatchError
		if errors.As(err, &mm) {
			return nil, fmt.Errorf("%w (use --force to discard it)", err)
		}
		return nil, err
	}
	defer jrn.Close()

	// Merge journal completions into the manifest so we know both the
	// checksums (for verify) and what is left to fetch.
	resumed := 0
	for ordinal := range man.Blocks {
		if man.Blocks[ordinal].SHA256 == "" {
			if e, ok := jrn.Get(ordinal); ok {
				man.Blocks[ordinal].Length = e.Length
				man.Blocks[ordinal].SHA256 = e.SHA256
				resumed++
			}
		}
	}

	var pending []int
	for ordinal := range man.Blocks {
		if man.Blocks[ordinal].SHA256 == "" {
			pending = append(pending, ordinal)
		}
	}

	var stats downloadStats
	var zeroBlocks int
	// Open the image even with nothing pending when a paranoid spot-check
	// is requested: the check may invalidate journal entries and create
	// pending work.
	if len(pending) > 0 || (m.opts.ParanoidResume && resumed > 0) {
		writer, err := img.NewWriter(req.ImagePath, man.LogicalSize)
		if err != nil {
			return nil, err
		}

		if m.opts.NTFSCompress {
			// Best effort; non-fatal on non-NTFS volumes.
			if err := writer.EnableNTFSCompression(); err != nil {
				fmt.Fprintf(os.Stderr, "warning: NTFS compression not applied: %v\n", err)
			}
		}

		if m.opts.ParanoidResume && resumed > 0 {
			if err := paranoidCheck(ctx, writer, man, jrn); err != nil {
				writer.Close()
				return nil, err
			}
			// Blocks invalidated by the spot-check need re-downloading.
			pending = pending[:0]
			for ordinal := range man.Blocks {
				if !jrn.Has(ordinal) {
					pending = append(pending, ordinal)
				}
			}
		}

		if len(pending) > 0 {
			stats, err = m.download(ctx, req, man, jrn, lister, writer, pending)
			if err != nil {
				writer.Close()
				return nil, err
			}
			// Fill in lengths/checksums discovered during this run.
			for i := range man.Blocks {
				if e, ok := jrn.Get(i); ok {
					man.Blocks[i].Length = e.Length
					man.Blocks[i].SHA256 = e.SHA256
				}
			}
			var alloc int64
			for _, b := range man.Blocks {
				alloc += b.Length
			}
			man.AllocatedBytes = alloc
		}
		writer.Close()
	}

	if man.ImageSHA256 == "" {
		if _, err := os.Stat(req.ImagePath); os.IsNotExist(err) {
			return nil, fmt.Errorf("raw image %s is missing but the manifest has no verified hash (re-run with --force)", req.ImagePath)
		}
		m.progress(Update{Phase: PhaseVerify, BlocksTotal: man.BlockCount, BytesTotal: man.LogicalSize})
		vr, err := img.Verify(ctx, req.ImagePath, man, func(done, total int64) {
			m.progress(Update{Phase: PhaseVerify, BlocksTotal: man.BlockCount, BytesDone: done, BytesTotal: total})
		})
		if err != nil {
			return nil, fmt.Errorf("verifying %s: %w", req.ImagePath, err)
		}
		if vr.BlocksChecked != man.BlockCount {
			return nil, fmt.Errorf("internal error: verified %d blocks, manifest has %d", vr.BlocksChecked, man.BlockCount)
		}
		man.ImageSHA256 = vr.ImageSHA256
		zeroBlocks = vr.ZeroBlocks
		if err := manifest.Save(req.ManifestPath, man); err != nil {
			return nil, fmt.Errorf("saving manifest: %w", err)
		}
	}

	res := &Result{
		Manifest:      man,
		NewBlocks:     stats.NewBlocks,
		ResumedBlocks: resumed,
		ZeroBlocks:    zeroBlocks,
		ImageSHA256:   man.ImageSHA256,
		Duration:      time.Since(start),
	}

	if req.Compress != nil && man.Compression == nil {
		m.progress(Update{Phase: PhaseCompress, BytesTotal: man.LogicalSize})
		zstPath := req.ImagePath + ".zst"
		size, err := img.CompressImage(ctx, req.ImagePath, zstPath, req.Compress.Level, func(done, total int64) {
			m.progress(Update{Phase: PhaseCompress, BytesDone: done, BytesTotal: total})
		})
		if err != nil {
			return nil, fmt.Errorf("compressing %s: %w", req.ImagePath, err)
		}
		rt, err := img.RestoreTest(ctx, zstPath, man.ImageSHA256, man.LogicalSize)
		if err != nil {
			return nil, fmt.Errorf("restore test of %s: %w", zstPath, err)
		}
		man.Compression = &manifest.Compression{
			Algorithm: "zstd",
			Level:     req.Compress.Level,
			Size:      size,
			SHA256:    rt.SHA256,
			Verified:  rt.RestoreTestOK,
		}
		res.CompressedSize = size
		if err := manifest.Save(req.ManifestPath, man); err != nil {
			return nil, fmt.Errorf("saving manifest: %w", err)
		}
	}

	// Everything succeeded: the journal is no longer needed.
	if err := jrn.Remove(); err != nil {
		return nil, fmt.Errorf("removing journal: %w", err)
	}

	keepRaw := req.Compress == nil || req.Compress.KeepRaw
	if !keepRaw {
		if err := os.Remove(req.ImagePath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("removing raw image after verified compression: %w", err)
		}
	} else if onDisk, err := img.OnDiskSize(req.ImagePath); err == nil {
		res.ImageOnDisk = onDisk
	}

	return res, nil
}

// downloadStats carries per-run counters out of the worker pool.
type downloadStats struct {
	NewBlocks int
}

// download runs the worker pool for all pending block ordinals. It owns
// the checkpoint loop and guarantees the image fsync precedes journal
// checkpoint flushes.
func (m *Manager) download(ctx context.Context, req *Request, man *manifest.Manifest, jrn *Journal, lister *ebsx.Lister, writer *img.Writer, pending []int) (downloadStats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var stats downloadStats

	limiter := NewLimiter(m.opts.Concurrency)
	fetcher := ebsx.NewFetcher(m.src, lister, m.opts.MaxAttempts, limiter.OnThrottle)

	// Estimated bytes: resumed blocks are exact, pending assumed full.
	var estTotal int64
	for _, b := range man.Blocks {
		if b.Length > 0 {
			estTotal += b.Length
		} else {
			estTotal += man.BlockSize
		}
	}
	statsOffset := resumedBytes(man, jrn)

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
		cntMu    sync.Mutex
		bytesNow int64
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	// Checkpoint loop: fsync image, then journal (ordering matters).
	checkpointDone := make(chan struct{})
	go func() {
		defer close(checkpointDone)
		t := time.NewTicker(m.opts.CheckpointEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := writer.Sync(); err != nil {
					fail(fmt.Errorf("fsync image: %w", err))
					return
				}
				if err := jrn.Checkpoint(); err != nil {
					fail(fmt.Errorf("journal checkpoint: %w", err))
					return
				}
			}
		}
	}()

	jobs := make(chan int)
	go func() {
		defer close(jobs)
		for _, ordinal := range pending {
			select {
			case jobs <- ordinal:
			case <-ctx.Done():
				return
			}
		}
	}()

	for w := 0; w < m.opts.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ordinal := range jobs {
				if err := limiter.Acquire(ctx); err != nil {
					return
				}
				block := man.Blocks[ordinal]
				bd, err := fetcher.FetchBlock(ctx, block.Index)
				if err != nil {
					limiter.Release()
					fail(fmt.Errorf("fetching block %d: %w", block.Index, err))
					return
				}
				if err := writer.WriteBlock(block.Offset, bd.Data); err != nil {
					limiter.Release()
					fail(fmt.Errorf("writing block %d: %w", block.Index, err))
					return
				}
				limiter.Release()

				jrn.Record(JournalEntry{Ordinal: ordinal, Length: int64(len(bd.Data)), SHA256: bd.SHA256})

				cntMu.Lock()
				stats.NewBlocks++
				bytesNow += int64(len(bd.Data))
				done := stats.NewBlocks
				bd.Data = nil
				cntMu.Unlock()

				m.progress(Update{
					Phase:       PhaseDownload,
					BlocksDone:  statsOffset + done,
					BlocksTotal: man.BlockCount,
					BytesDone:   bytesNow,
					BytesTotal:  estTotal,
					Concurrency: limiter.Current(),
				})
			}
		}()
	}

	wg.Wait()
	cancel()
	<-checkpointDone

	if firstErr != nil {
		// Best effort: persist what completed so resume works cleanly.
		_ = writer.Sync()
		_ = jrn.Checkpoint()
		return stats, firstErr
	}

	if err := writer.Sync(); err != nil {
		return stats, fmt.Errorf("fsync image: %w", err)
	}
	if err := jrn.Checkpoint(); err != nil {
		return stats, fmt.Errorf("journal checkpoint: %w", err)
	}
	return stats, nil
}

func resumedBytes(man *manifest.Manifest, jrn *Journal) int {
	n := 0
	for i, b := range man.Blocks {
		if b.Length > 0 && jrn.Has(i) {
			n++
		}
	}
	return n
}

// paranoidCheck re-verifies a sample of already-complete blocks on disk
// against their recorded checksums; mismatches clear the journal entry so
// the block is re-downloaded.
func paranoidCheck(ctx context.Context, writer *img.Writer, man *manifest.Manifest, jrn *Journal) error {
	var ordinals []int
	for i := range man.Blocks {
		if jrn.Has(i) {
			ordinals = append(ordinals, i)
		}
	}
	const sample = 32
	if len(ordinals) > sample {
		// Even stride across the image.
		stride := len(ordinals) / sample
		sampled := ordinals[:0]
		for i := 0; i < len(ordinals) && len(sampled) < sample; i += stride {
			sampled = append(sampled, ordinals[i])
		}
		ordinals = sampled
	}
	for _, ordinal := range ordinals {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := man.Blocks[ordinal]
		if b.SHA256 == "" || b.Length == 0 {
			continue
		}
		buf := make([]byte, b.Length)
		if err := writer.ReadBlock(b.Offset, buf); err != nil {
			return fmt.Errorf("paranoid resume read at offset %d: %w", b.Offset, err)
		}
		sum := sha256.Sum256(buf)
		if base64.StdEncoding.EncodeToString(sum[:]) != b.SHA256 {
			jrn.Clear(ordinal)
		}
	}
	return nil
}

// reconcileManifest loads an existing manifest (validating it against the
// fresh listing) or builds a new skeleton. Snapshots are immutable, so a
// geometry or block-index mismatch means something is wrong.
func (m *Manager) reconcileManifest(req *Request, info *ebsx.SnapshotInfo) (*manifest.Manifest, error) {
	logical := info.VolumeGiB * manifest.GiB
	if st, err := os.Stat(req.ManifestPath); err == nil && !st.IsDir() {
		man, err := manifest.Load(req.ManifestPath)
		if err != nil {
			return nil, fmt.Errorf("existing manifest: %w", err)
		}
		if man.SnapshotID != info.SnapshotID || man.VolumeSizeGiB != info.VolumeGiB ||
			man.BlockSize != info.BlockSize || man.LogicalSize != logical {
			return nil, fmt.Errorf("existing manifest does not match listing for %s (use --force to discard)", info.SnapshotID)
		}
		if len(man.Blocks) != len(info.Blocks) {
			return nil, fmt.Errorf("existing manifest has %d blocks, listing found %d (use --force to discard)",
				len(man.Blocks), len(info.Blocks))
		}
		for i, b := range man.Blocks {
			if b.Index != info.Blocks[i].Index {
				return nil, fmt.Errorf("existing manifest block %d has index %d, listing says %d (use --force to discard)",
					i, b.Index, info.Blocks[i].Index)
			}
		}
		man.ToolVersion = m.opts.ToolVersion
		return man, nil
	}

	blocks := make([]manifest.Block, len(info.Blocks))
	for i, ref := range info.Blocks {
		blocks[i] = manifest.Block{Index: ref.Index, Offset: ref.Index * info.BlockSize}
	}
	return &manifest.Manifest{
		Version:       manifest.CurrentVersion,
		ToolVersion:   m.opts.ToolVersion,
		Kind:          manifest.KindSnapshot,
		SnapshotID:    info.SnapshotID,
		VolumeSizeGiB: info.VolumeGiB,
		BlockSize:     info.BlockSize,
		LogicalSize:   logical,
		BlockCount:    len(blocks),
		ImageFile:     filepath.Base(req.ImagePath),
		Blocks:        blocks,
	}, nil
}

func (m *Manager) progress(u Update) {
	if m.opts.Progress != nil {
		m.opts.Progress(u)
	}
}
