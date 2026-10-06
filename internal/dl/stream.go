package dl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"snaport/internal/ebsx"
	"snaport/internal/img"
	"snaport/internal/manifest"
)

// SZCPath is the container path derived from a request's image path.
func (r *Request) SZCPath() string { return r.ImagePath + ".szc" }

// runStream is the one-shot streaming pipeline: fetch blocks in parallel
// and compress them chunk-by-chunk directly into an .img.szc container,
// never materializing the raw image. There is no journal and therefore
// no resume; an interrupted run restarts from scratch (documented).
// Incremental mode is not supported either.
func (m *Manager) runStream(ctx context.Context, req *Request, man *manifest.Manifest, lister *ebsx.Lister) (*Result, error) {
	start := time.Now()
	level := 3
	if req.Compress != nil && req.Compress.Level > 0 {
		level = req.Compress.Level
	}

	szcPath := req.SZCPath()
	offsets := make([]int64, 0, len(man.Blocks))
	for _, b := range man.Blocks {
		offsets = append(offsets, b.Offset)
	}

	cw, err := img.NewSZCWriter(szcPath, req.SnapshotID, man.VolumeSizeGiB, man.BlockSize, man.LogicalSize, offsets, level)
	if err != nil {
		return nil, err
	}

	limiter := NewLimiter(m.opts.Concurrency)
	fetcher := ebsx.NewFetcher(m.src, lister, m.opts.MaxAttempts, limiter.OnThrottle)

	shas := make([]string, len(man.Blocks))
	lengths := make([]int64, len(man.Blocks))

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
		cntMu    sync.Mutex
		bytesNow int64
		done     int
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	jobs := make(chan int)
	go func() {
		defer close(jobs)
		for ordinal := range man.Blocks {
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
				if err := cw.Submit(block.Offset, bd.Data); err != nil {
					limiter.Release()
					fail(fmt.Errorf("storing block %d: %w", block.Index, err))
					return
				}
				limiter.Release()

				shas[ordinal] = bd.SHA256
				lengths[ordinal] = int64(len(bd.Data))

				cntMu.Lock()
				bytesNow += int64(len(bd.Data))
				shown := bytesNow
				done++
				shownDone := done
				cntMu.Unlock()

				m.progress(Update{
					Phase:       PhaseDownload,
					BlocksDone:  shownDone,
					BlocksTotal: man.BlockCount,
					BytesDone:   shown,
					Concurrency: limiter.Current(),
				})
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		cw.Close()
		return nil, firstErr
	}
	if err := cw.Finalize(); err != nil {
		return nil, fmt.Errorf("finalizing %s: %w", szcPath, err)
	}

	// Self-verification: decode every chunk, per-chunk checksums and the
	// whole-image hash from the trailer (this is the restore test).
	m.progress(Update{Phase: PhaseVerify, BytesTotal: man.LogicalSize})
	trailer, err := img.OpenSZC(szcPath)
	if err != nil {
		return nil, fmt.Errorf("reopening %s: %w", szcPath, err)
	}
	if err := img.VerifySZC(ctx, szcPath, trailer, func(done, total int64) {
		m.progress(Update{Phase: PhaseVerify, BytesDone: done, BytesTotal: total})
	}); err != nil {
		return nil, fmt.Errorf("restore test of %s: %w", szcPath, err)
	}

	// Manifest: fill block checksums, geometry hash, compression info.
	var alloc int64
	for i := range man.Blocks {
		if shas[i] == "" {
			return nil, fmt.Errorf("internal error: block ordinal %d was never fetched", i)
		}
		man.Blocks[i].SHA256 = shas[i]
		man.Blocks[i].Length = lengths[i]
		alloc += lengths[i]
	}
	man.AllocatedBytes = alloc
	man.ImageSHA256 = trailer.ImageSHA256

	st, err := os.Stat(szcPath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(szcPath)
	if err != nil {
		return nil, err
	}
	cHash := sha256.New()
	if _, err := io.Copy(cHash, f); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()

	man.Compression = &manifest.Compression{
		Algorithm: "zstd-chunked",
		Level:     level,
		Size:      st.Size(),
		SHA256:    hex.EncodeToString(cHash.Sum(nil)),
		Verified:  true,
	}
	if err := manifest.Save(req.ManifestPath, man); err != nil {
		return nil, fmt.Errorf("saving manifest: %w", err)
	}

	return &Result{
		Manifest:       man,
		NewBlocks:      man.BlockCount,
		ImageSHA256:    man.ImageSHA256,
		CompressedSize: st.Size(),
		Duration:       time.Since(start),
		Streamed:       true,
	}, nil
}
