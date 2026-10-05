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
	}, nil
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
}
