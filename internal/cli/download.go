package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/spf13/cobra"

	"snaport/internal/dl"
	"snaport/internal/ebsx"
	"snaport/internal/img"
	"snaport/internal/manifest"
)

var downloadFlags struct {
	output         string
	concurrency    int
	zstdLevel      int
	keepRaw        bool
	noCompress     bool
	ntfsCompress   bool
	paranoidResume bool
	force          bool
	dryRun         bool
	stateDir       string
	blockPageSize  int
	maxAttempts    int
	rootOnly       bool
	quiet          bool
	egressPerGB    float64
	wait           time.Duration
}

var downloadCmd = &cobra.Command{
	Use:   "download <snap-...|ami-...>",
	Short: "Download a snapshot or all EBS snapshots of an AMI",
	Long: `Download a single EBS snapshot (snap-...) or every EBS-backed volume of
an AMI (ami-...) into sparse raw images, verified and optionally
compressed with zstd.

Examples:
  snaport download snap-0123456789abcdef0
  snaport download ami-0123456789abcdef0 -o D:\backups
  snaport download snap-0123456789abcdef0 --dry-run`,
	Args: cobra.ExactArgs(1),
	RunE: runDownload,
}

func init() {
	f := downloadCmd.Flags()
	f.StringVarP(&downloadFlags.output, "output", "o", "", "output file (snapshot) or directory (AMI); default: ./<id>.img or ./<ami-id>/")
	f.IntVar(&downloadFlags.concurrency, "concurrency", 32, "parallel GetSnapshotBlock requests")
	f.IntVar(&downloadFlags.zstdLevel, "zstd-level", 6, "zstd compression level (1-19)")
	f.BoolVar(&downloadFlags.keepRaw, "keep-raw", false, "keep the sparse raw image next to the .zst (default: delete after verified compression)")
	f.BoolVar(&downloadFlags.noCompress, "no-compress", false, "skip the zstd stage, keep only the raw sparse image")
	f.BoolVar(&downloadFlags.ntfsCompress, "ntfs-compress", false, "apply transparent NTFS compression to the raw image (best effort)")
	f.BoolVar(&downloadFlags.paranoidResume, "paranoid-resume", false, "spot-check already-downloaded blocks against checksums when resuming")
	f.BoolVar(&downloadFlags.force, "force", false, "discard existing manifest/journal/image and restart")
	f.BoolVar(&downloadFlags.dryRun, "dry-run", false, "list blocks and print the download plan without transferring")
	f.StringVar(&downloadFlags.stateDir, "state-dir", "", "directory for resume journals (default: alongside the output)")
	f.IntVar(&downloadFlags.blockPageSize, "block-pagesize", int(ebsx.DefaultListPageSize), "MaxResults per ListSnapshotBlocks call")
	f.IntVar(&downloadFlags.maxAttempts, "max-attempts", 5, "fetch attempts per block before failing")
	f.BoolVar(&downloadFlags.rootOnly, "root-only", false, "AMI: download only the root volume")
	f.DurationVar(&downloadFlags.wait, "wait", 0, "wait up to this duration for pending snapshots to complete, e.g. --wait 15m (default: fail fast)")
	f.BoolVar(&downloadFlags.quiet, "quiet", false, "suppress the progress display")
	f.Float64Var(&downloadFlags.egressPerGB, "egress-per-gb", 0.09, "USD per GB of internet data-transfer-out used in --dry-run cost estimates (set 0 to ignore egress, e.g. when running inside AWS)")
}

// volumeJob is one snapshot to download.
type volumeJob struct {
	snapshotID string
	device     string
	root       bool
	encrypted  bool
	// output paths
	image    string
	state    string
	manifest string
}

func runDownload(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	target := args[0]

	if downloadFlags.noCompress && downloadFlags.keepRaw {
		return fmt.Errorf("--keep-raw has no effect with --no-compress")
	}
	if downloadFlags.zstdLevel < 1 || downloadFlags.zstdLevel > 19 {
		return fmt.Errorf("--zstd-level must be 1-19")
	}

	cfg, err := loadAWSConfig(ctx)
	if err != nil {
		return err
	}
	ebsClient := ebsx.NewEBSClient(cfg)
	ec2Client := ec2.NewFromConfig(cfg)
	source := ebsx.NewAWSSource(ebsClient)

	var jobs []volumeJob
	switch {
	case strings.HasPrefix(target, "snap-"):
		jobs, err = snapshotJobs(target)
		if err != nil {
			return err
		}
	case strings.HasPrefix(target, "ami-"):
		jobs, err = amiJobs(ctx, ec2Client, target)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized target %q: expected a snap-... or ami-... ID", target)
	}

	// The EBS Direct read APIs only serve completed snapshots; fail fast
	// (or wait) on pending ones instead of surfacing a raw API error.
	ids := make([]string, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.snapshotID)
	}
	if err := ensureSnapshotsReady(ctx, ec2Client, ids, downloadFlags.wait); err != nil {
		return err
	}

	if downloadFlags.dryRun {
		return dryRun(ctx, source, jobs)
	}

	prog := newProgress(downloadFlags.quiet)
	opts := dl.Options{
		Concurrency:    downloadFlags.concurrency,
		MaxAttempts:    downloadFlags.maxAttempts,
		ListPageSize:   int32(downloadFlags.blockPageSize),
		ParanoidResume: downloadFlags.paranoidResume,
		NTFSCompress:   downloadFlags.ntfsCompress,
		ToolVersion:    Version,
		Progress:       prog.Update,
	}

	var results []*dl.Result
	for i, job := range jobs {
		if len(jobs) > 1 {
			prog.Finish()
			fmt.Fprintf(os.Stderr, "volume %d/%d: %s (%s)\n", i+1, len(jobs), job.snapshotID, job.device)
		}
		req := &dl.Request{
			SnapshotID:   job.snapshotID,
			ImagePath:    job.image,
			ManifestPath: job.manifest,
			StatePath:    job.state,
			Force:        downloadFlags.force,
		}
		if !downloadFlags.noCompress {
			req.Compress = &dl.CompressOptions{Level: downloadFlags.zstdLevel, KeepRaw: downloadFlags.keepRaw}
		}
		res, err := dl.New(source, opts).Run(ctx, req)
		if err != nil {
			prog.Finish()
			return volumeError(job, err)
		}
		results = append(results, res)
	}
	prog.Finish()

	// AMI top-level manifest.
	if strings.HasPrefix(target, "ami-") {
		if err := writeAMIManifest(target, jobs, results); err != nil {
			return err
		}
	}

	for i, res := range results {
		printResult(jobs[i], res, downloadFlags.noCompress)
	}
	return nil
}

// snapshotJobs derives output paths for a single-snapshot download.
func snapshotJobs(snapshotID string) ([]volumeJob, error) {
	out := downloadFlags.output
	if out == "" {
		out = snapshotID + ".img"
	}
	if st, err := os.Stat(out); err == nil && st.IsDir() {
		out = filepath.Join(out, snapshotID+".img")
	}
	image, err := filepath.Abs(out)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSuffix(image, ".img")
	job := volumeJob{
		snapshotID: snapshotID,
		device:     "",
		image:      image,
		manifest:   base + ".manifest.json",
	}
	job.state = statePathFor(job)
	return []volumeJob{job}, nil
}

// amiJobs resolves an AMI's EBS-backed volumes via DescribeImages.
func amiJobs(ctx context.Context, client *ec2.Client, amiID string) ([]volumeJob, error) {
	out, err := client.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{amiID}})
	if err != nil {
		return nil, fmt.Errorf("DescribeImages %s: %w", amiID, err)
	}
	if len(out.Images) == 0 {
		return nil, fmt.Errorf("AMI %s not found in this region", amiID)
	}
	image := out.Images[0]

	dir := downloadFlags.output
	if dir == "" {
		dir = amiID
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir = absDir

	rootDevice := aws.ToString(image.RootDeviceName)
	var jobs []volumeJob
	for _, m := range image.BlockDeviceMappings {
		if m.Ebs == nil || aws.ToString(m.Ebs.SnapshotId) == "" {
			continue // instance-store mapping
		}
		device := aws.ToString(m.DeviceName)
		if downloadFlags.rootOnly && device != rootDevice {
			continue
		}
		file := sanitizeDeviceName(device)
		if file == "" {
			file = fmt.Sprintf("volume%d", len(jobs))
		}
		base := filepath.Join(dir, file)
		job := volumeJob{
			snapshotID: aws.ToString(m.Ebs.SnapshotId),
			device:     device,
			root:       device == rootDevice,
			encrypted:  aws.ToBool(m.Ebs.Encrypted),
			image:      base + ".img",
			manifest:   base + ".manifest.json",
		}
		job.state = statePathFor(job)
		jobs = append(jobs, job)
	}
	if len(jobs) == 0 {
		if downloadFlags.rootOnly {
			return nil, fmt.Errorf("AMI %s has no EBS snapshot matching root device %q", amiID, rootDevice)
		}
		return nil, fmt.Errorf("AMI %s has no EBS-backed volumes (instance-store only)", amiID)
	}
	return jobs, nil
}

// statePathFor decides where the resume journal lives.
func statePathFor(job volumeJob) string {
	if downloadFlags.stateDir != "" {
		return filepath.Join(downloadFlags.stateDir, job.snapshotID+".state.jsonl")
	}
	return strings.TrimSuffix(job.image, ".img") + ".state.jsonl"
}

// sanitizeDeviceName converts /dev/sda1 or /dev/xvdf into a file-safe name.
func sanitizeDeviceName(device string) string {
	name := strings.TrimPrefix(device, "/dev/")
	name = strings.ReplaceAll(name, "/", "-")
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// dryRun lists each snapshot's blocks and prints the plan plus a local
// disk-space check.
func dryRun(ctx context.Context, source ebsx.Source, jobs []volumeJob) error {
	var totalAlloc int64
	var totalCost costEstimate
	for _, job := range jobs {
		lister := ebsx.NewLister(source, job.snapshotID, int32(downloadFlags.blockPageSize))
		info, err := lister.ListAll(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", job.snapshotID, err)
		}
		logical := info.VolumeGiB * manifest.GiB
		allocated := int64(info.AllocatedBlockCount()) * info.BlockSize
		cost := estimateCost(info.Pages, info.AllocatedBlockCount(), allocated, downloadFlags.egressPerGB)
		totalAlloc += allocated
		totalCost.ListRequests += cost.ListRequests
		totalCost.GetRequests += cost.GetRequests
		totalCost.RequestCostUSD += cost.RequestCostUSD
		totalCost.EgressGB += cost.EgressGB
		totalCost.EgressCostUSD += cost.EgressCostUSD
		totalCost.TotalUSD += cost.TotalUSD
		fmt.Printf("%s (device %s):\n", job.snapshotID, job.device)
		fmt.Printf("  volume:      %d GiB logical (%s), block size %d KiB\n",
			info.VolumeGiB, humanBytes(float64(logical)), info.BlockSize/1024)
		fmt.Printf("  allocated:   %d blocks; assuming full blocks: %s on disk\n",
			info.AllocatedBlockCount(), humanBytes(float64(allocated)))
		fmt.Printf("  output:      %s\n", job.image)
		fmt.Printf("  compressed:  %s.zst\n", job.image)
		fmt.Printf("  cost est:    %s (%s list + %s get requests%s)\n",
			usd(cost.TotalUSD), humanCount(cost.ListRequests), humanCount(cost.GetRequests), egressNote(cost))
	}
	fmt.Printf("\nplan: up to %s of sparse disk during download, plus the compressed output\n",
		humanBytes(float64(totalAlloc)))
	if len(jobs) > 0 {
		dir := filepath.Dir(jobs[0].image)
		if free, err := img.FreeSpace(dir + string(os.PathSeparator)); err == nil {
			fmt.Printf("local disk:    %s free in %s\n", humanBytes(float64(free)), dir)
			if free < uint64(totalAlloc)+uint64(totalAlloc)/10 {
				fmt.Printf("warning: free space is below the estimated need (%s)\n", humanBytes(float64(totalAlloc)))
			}
		}
	}
	if len(jobs) > 1 {
		fmt.Printf("\ntotal cost estimate: %s (%s list + %s get requests%s)\n",
			usd(totalCost.TotalUSD), humanCount(totalCost.ListRequests), humanCount(totalCost.GetRequests), egressNote(totalCost))
	}
	fmt.Println("cost notes:   happy-path requests only (retries/token refreshes excluded); allocated bytes are")
	fmt.Println("              an upper-bound estimate; verify current EBS Direct API pricing")
	fmt.Println("no data transferred (--dry-run)")
	return nil
}

// egressNote describes the data-transfer component, if priced in.
func egressNote(c costEstimate) string {
	if c.EgressCostUSD == 0 {
		return ""
	}
	return fmt.Sprintf("; egress %.1f GB @ %s/GB = %s", c.EgressGB,
		usd(downloadFlags.egressPerGB), usd(c.EgressCostUSD))
}

// humanCount renders a request count with a thousands separator.
func humanCount(v int64) string {
	s := fmt.Sprintf("%d", v)
	var b []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, s[i])
	}
	return string(b)
}

// volumeError enriches failures with actionable context.
func volumeError(job volumeJob, err error) error {
	var denied *ebsx.AccessDeniedError
	if errors.As(err, &denied) {
		hint := "grant ebs:ListSnapshotBlocks and ebs:GetSnapshotBlock on the snapshot"
		if job.encrypted {
			hint += "; the snapshot is encrypted, so the caller also needs kms:Decrypt on its KMS key"
		}
		return fmt.Errorf("%s: %w\n  hint: %s", job.snapshotID, err, hint)
	}
	return fmt.Errorf("%s: %w", job.snapshotID, err)
}

// writeAMIManifest writes the top-level index of an AMI download. The
// per-volume manifests are the source of truth; this one aggregates.
func writeAMIManifest(amiID string, jobs []volumeJob, results []*dl.Result) error {
	m := &manifest.Manifest{
		Version:     manifest.CurrentVersion,
		ToolVersion: Version,
		Kind:        manifest.KindAMI,
		SnapshotID:  amiID,
		Volumes:     make([]manifest.Volume, 0, len(jobs)),
	}
	for i, job := range jobs {
		m.Volumes = append(m.Volumes, manifest.Volume{
			Device:     job.device,
			Root:       job.root,
			SnapshotID: job.snapshotID,
			ImageFile:  filepath.Base(job.image),
			Manifest:   filepath.Base(job.manifest),
		})
		if i < len(results) && results[i] != nil {
			m.VolumeSizeGiB += results[i].Manifest.VolumeSizeGiB
			m.LogicalSize += results[i].Manifest.LogicalSize
			m.AllocatedBytes += results[i].Manifest.AllocatedBytes
		}
	}
	return manifest.Save(filepath.Join(filepath.Dir(jobs[0].manifest), "ami-manifest.json"), m)
}

func printResult(job volumeJob, res *dl.Result, noCompress bool) {
	m := res.Manifest
	who := job.snapshotID
	if job.device != "" {
		who = fmt.Sprintf("%s (%s)", job.snapshotID, job.device)
	}
	fmt.Printf("%s:\n", who)
	fmt.Printf("  volume: %d GiB logical, %d allocated blocks (%s), block size %d KiB\n",
		m.VolumeSizeGiB, m.BlockCount, humanBytes(float64(m.AllocatedBytes)), m.BlockSize/1024)
	fmt.Printf("  blocks fetched this run: %d, resumed: %d, zero (kept sparse): %d\n",
		res.NewBlocks, res.ResumedBlocks, res.ZeroBlocks)
	fmt.Printf("  image sha256: %s  [verify PASS: %d/%d blocks]\n", res.ImageSHA256, m.BlockCount, m.BlockCount)
	if noCompress {
		fmt.Printf("  output: %s (sparse raw, %s on disk)\n", job.image, humanBytes(float64(res.ImageOnDisk)))
	} else if m.Compression != nil {
		fmt.Printf("  output: %s.zst (%s, zstd-%d) [restore-test PASS]\n",
			job.image, humanBytes(float64(m.Compression.Size)), m.Compression.Level)
		if downloadFlags.keepRaw {
			fmt.Printf("  raw kept: %s (%s on disk)\n", job.image, humanBytes(float64(res.ImageOnDisk)))
		}
	}
	fmt.Printf("  elapsed: %s\n", res.Duration.Round(time.Second))
}
