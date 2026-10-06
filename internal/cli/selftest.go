package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"snaport/internal/dl"
	"snaport/internal/testutil"
)

var selftestCmd = &cobra.Command{
	Use:   "selftest",
	Short: "Run the offline end-to-end pipeline test (no AWS access needed)",
	Long: `Builds a synthetic snapshot in memory (scattered allocated blocks,
all-zero blocks, a short final block) and drives the real download
pipeline over it: listing, parallel fetch with checksums, sparse writes,
resume after a simulated crash, full verification, zstd compression and a
restore test. Exits non-zero on any failure.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dir, err := os.MkdirTemp("", "snaport-selftest-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		res, err := RunSelfTest(dir)
		if err != nil {
			return err
		}
		printSelfTestSummary(res)
		return nil
	},
}

// SelfTestResult summarizes the offline pipeline test.
type SelfTestResult struct {
	VolumeGiB    int64
	Blocks       int
	AllocatedMB  float64
	Fetched      int
	ZeroBlocks   int
	Resumed      int
	Refetched    int64 // extra GetBlock calls caused by the simulated crash
	CompressedMB float64
	Duration     time.Duration
	// Delta leg: changed blocks fetched incrementally, blocks reused
	// from the base, and whether the delta image hash matched a full
	// download's.
	DeltaFetched int
	DeltaReused  int
	DeltaMatched bool
}

// RunSelfTest drives the full pipeline against an in-memory snapshot.
// It is shared between the selftest command and the Go test suite.
func RunSelfTest(dir string) (*SelfTestResult, error) {
	const (
		snapshotID = "snap-selftest000000001"
		volumeGiB  = 1
		blockSize  = 512 * 1024
		blockCount = 140
	)
	ctx := context.Background()

	finalIndex := int64(volumeGiB*(1<<30)/blockSize) - 1
	indices := testutil.ScratchIndices(blockCount, finalIndex)
	fake := testutil.NewFake(snapshotID, volumeGiB, blockSize, indices, 300_000)

	image := filepath.Join(dir, snapshotID+".img")
	manifestPath := strings.TrimSuffix(image, ".img") + ".manifest.json"
	statePath := strings.TrimSuffix(image, ".img") + ".state.jsonl"

	opts := dl.Options{Concurrency: 8, ToolVersion: Version}

	// Phase 1: interrupt the download mid-flight (simulated crash).
	{
		cancelAt := 30
		intCtx, cancel := context.WithCancel(ctx)
	interrupted:
		for {
			o := opts
			o.Progress = func(u dl.Update) {
				if u.Phase == dl.PhaseDownload && u.BlocksDone >= cancelAt {
					cancel()
				}
			}
			_, err := dl.New(fake, o).Run(intCtx, &dl.Request{
				SnapshotID:   snapshotID,
				ImagePath:    image,
				ManifestPath: manifestPath,
				StatePath:    statePath,
			})
			if err == nil {
				break interrupted // finished before the cancel point (small machine); fine
			}
			if intCtx.Err() == nil {
				return nil, fmt.Errorf("download failed unexpectedly: %w", err)
			}
			break
		}
		cancel()
		if _, err := os.Stat(statePath); err != nil {
			return nil, fmt.Errorf("resume journal missing after interrupted run: %w", err)
		}
	}

	// Phase 2: resume to completion, verify, compress, restore-test.
	var res *dl.Result
	{
		o := opts
		o.ParanoidResume = true
		var err error
		res, err = dl.New(fake, o).Run(ctx, &dl.Request{
			SnapshotID:   snapshotID,
			ImagePath:    image,
			ManifestPath: manifestPath,
			StatePath:    statePath,
			Compress:     &dl.CompressOptions{Level: 3},
		})
		if err != nil {
			return nil, fmt.Errorf("resumed download failed: %w", err)
		}
	}

	// Assertions.
	if res.NewBlocks+res.ResumedBlocks != len(indices) {
		return nil, fmt.Errorf("block accounting: fetched %d + resumed %d != %d listed",
			res.NewBlocks, res.ResumedBlocks, len(indices))
	}
	if res.ImageSHA256 == "" {
		return nil, fmt.Errorf("image hash missing after verify")
	}
	if res.Manifest.Compression == nil || !res.Manifest.Compression.Verified {
		return nil, fmt.Errorf("compression not verified")
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		return nil, fmt.Errorf("journal still present after successful run")
	}
	if _, err := os.Stat(image); !os.IsNotExist(err) {
		return nil, fmt.Errorf("raw image still present although KeepRaw was false")
	}
	zst := image + ".zst"
	if _, err := os.Stat(zst); err != nil {
		return nil, fmt.Errorf("compressed image missing: %w", err)
	}

	// Phase 3: re-run must be a no-op (idempotent complete state).
	{
		o := opts
		before := fake.GetCalls()
		res2, err := dl.New(fake, o).Run(ctx, &dl.Request{
			SnapshotID:   snapshotID,
			ImagePath:    image,
			ManifestPath: manifestPath,
			StatePath:    statePath,
			Compress:     &dl.CompressOptions{Level: 3},
		})
		if err != nil {
			return nil, fmt.Errorf("idempotent re-run failed: %w (raw image is expected to be gone; re-run means verify-only)", err)
		}
		if fake.GetCalls() != before || res2.NewBlocks != 0 {
			return nil, fmt.Errorf("idempotent re-run re-fetched blocks (%d calls before, %d after)", before, fake.GetCalls())
		}
	}

	expectedZero := 0
	for _, idx := range indices {
		if idx%97 == 13 {
			expectedZero++
		}
	}
	if res.ZeroBlocks != expectedZero {
		return nil, fmt.Errorf("zero-block detection: got %d, expected %d", res.ZeroBlocks, expectedZero)
	}

	// Phase 4: incremental sync (full base, changed-block delta, hash
	// comparison with a full download of the target).
	delta := runDeltaSelfTest(ctx, dir)
	if delta.err != nil {
		return nil, delta.err
	}

	return &SelfTestResult{
		VolumeGiB:    volumeGiB,
		Blocks:       len(indices),
		AllocatedMB:  float64(fake.AllocatedBytes()) / (1 << 20),
		Fetched:      res.NewBlocks,
		ZeroBlocks:   res.ZeroBlocks,
		Resumed:      res.ResumedBlocks,
		Refetched:    fake.GetCalls() - int64(len(indices)),
		CompressedMB: float64(res.CompressedSize) / (1 << 20),
		Duration:     res.Duration,
		DeltaFetched: delta.fetched,
		DeltaReused:  delta.reused,
		DeltaMatched: delta.matched,
	}, nil
}

// deltaSelfTestOutcome carries the delta leg's result.
type deltaSelfTestOutcome struct {
	fetched int
	reused  int
	matched bool
	err     error
}

// runDeltaSelfTest exercises the incremental path against a derived
// fake: full base download, changed-block sync, then a hash comparison
// against a full download of the target snapshot.
func runDeltaSelfTest(ctx context.Context, dir string) deltaSelfTestOutcome {
	const (
		baseID = "snap-selftestbase001"
		nextID = "snap-selftestnext002"
		volGiB = 1
		bs     = 512 * 1024
		count  = 90
	)
	final := int64(volGiB*(1<<30)/bs) - 1
	indices := testutil.ScratchIndices(count, final)
	base := testutil.NewFake(baseID, volGiB, bs, indices, bs)
	next := base.Derive(nextID, volGiB)

	// Mutate: 6 modified, 3 removed, 5 added.
	nextIndices := base.Indices()
	for i := 0; i < 6; i++ {
		idx := nextIndices[(i*9+1)%len(nextIndices)]
		if content := next.Data(idx); content != nil {
			content[len(content)-1] ^= 0x5A
			next.UpdateBlockContent(idx, content)
		}
	}
	for i := 0; i < 3; i++ {
		next.RemoveBlock(nextIndices[(i*13+7)%len(nextIndices)])
	}
	allocated := map[int64]bool{}
	for _, idx := range indices {
		allocated[idx] = true
	}
	for added, k := 0, int64(0); added < 5 && k < final; k++ {
		idx := final - 1 - k*11
		if idx < 0 || allocated[idx] {
			continue
		}
		next.AddBlock(idx, bs)
		added++
	}

	paths := func(id string) (image, mani, state string) {
		image = filepath.Join(dir, id+".img")
		return image, filepath.Join(dir, id+".manifest.json"), filepath.Join(dir, id+".state.jsonl")
	}

	// Full base download (raw kept by skipping compression).
	baseImg, baseMan, baseState := paths(baseID)
	baseRes, err := dl.New(base, dl.Options{Concurrency: 8}).Run(ctx, &dl.Request{
		SnapshotID: baseID, ImagePath: baseImg, ManifestPath: baseMan, StatePath: baseState,
	})
	if err != nil {
		return deltaSelfTestOutcome{err: fmt.Errorf("delta selftest base download: %w", err)}
	}

	// Delta sync on top of the base image.
	_, nextMan, nextState := paths(nextID)
	deltaRes, err := dl.New(next, dl.Options{Concurrency: 8}).Run(ctx, &dl.Request{
		SnapshotID:   nextID,
		ImagePath:    baseImg, // incremental: build on the base image
		ManifestPath: nextMan,
		StatePath:    nextState,
		BaseManifest: baseRes.Manifest,
	})
	if err != nil {
		return deltaSelfTestOutcome{err: fmt.Errorf("delta selftest sync: %w", err)}
	}

	// Snapshot the delta's fetch count before the reference download
	// reuses the same fake.
	deltaFetches := next.GetCalls()

	// Full download of the target elsewhere must produce the same hash.
	fullImg, fullMan, fullState := paths("full-" + nextID)
	fullRes, err := dl.New(next, dl.Options{Concurrency: 8}).Run(ctx, &dl.Request{
		SnapshotID: nextID, ImagePath: fullImg, ManifestPath: fullMan, StatePath: fullState,
	})
	if err != nil {
		return deltaSelfTestOutcome{err: fmt.Errorf("delta selftest full reference: %w", err)}
	}
	if deltaRes.ImageSHA256 != fullRes.ImageSHA256 {
		return deltaSelfTestOutcome{err: fmt.Errorf("delta selftest: delta hash %s != full hash %s",
			deltaRes.ImageSHA256, fullRes.ImageSHA256)}
	}
	if deltaFetches != int64(deltaRes.ChangedBlocks) {
		return deltaSelfTestOutcome{err: fmt.Errorf("delta selftest: %d target fetches != %d changed blocks",
			deltaFetches, deltaRes.ChangedBlocks)}
	}
	return deltaSelfTestOutcome{
		fetched: deltaRes.ChangedBlocks,
		reused:  baseRes.Manifest.BlockCount - deltaRes.ZeroedBlocks,
		matched: true,
	}
}

func printSelfTestSummary(r *SelfTestResult) {
	fmt.Println("selftest PASS")
	fmt.Printf("  synthetic volume: %d GiB logical, %d allocated blocks (%.1f MiB), 512 KiB blocks\n",
		r.VolumeGiB, r.Blocks, r.AllocatedMB)
	fmt.Printf("  interrupted+resumed: %d blocks resumed, %d refetched after simulated crash\n",
		r.Resumed, r.Refetched)
	fmt.Printf("  all-zero blocks kept as holes: %d\n", r.ZeroBlocks)
	fmt.Printf("  block checksums: verified; whole-image sha256 recorded\n")
	fmt.Printf("  compressed output: %.1f MiB, restore test PASS\n", r.CompressedMB)
	fmt.Printf("  incremental sync: %d changed blocks fetched, %d reused from base; delta hash %s\n",
		r.DeltaFetched, r.DeltaReused, matchLabel(r.DeltaMatched))
}

func matchLabel(ok bool) string {
	if ok {
		return "matches full download"
	}
	return "MISMATCH"
}
