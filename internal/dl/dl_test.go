package dl

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"snaport/internal/manifest"
	"snaport/internal/testutil"
)

func TestJournalRoundtripAndClear(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.jsonl")

	j, err := OpenJournal(path, "snap-1", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	j.Record(JournalEntry{Ordinal: 1, Length: 100, SHA256: "aa"})
	j.Record(JournalEntry{Ordinal: 2, Length: 200, SHA256: "bb"})
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j2, err := OpenJournal(path, "snap-1", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if j2.Count() != 2 || !j2.Has(1) {
		t.Fatalf("replay: count=%d", j2.Count())
	}
	j2.Clear(1)
	if err := j2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	j2.Close()

	j3, err := OpenJournal(path, "snap-1", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if j3.Count() != 1 || j3.Has(1) || !j3.Has(2) {
		t.Fatalf("after clear: count=%d has1=%v", j3.Count(), j3.Has(1))
	}
	j3.Close()

	// Mismatched snapshot ID must fail.
	if _, err := OpenJournal(path, "snap-2", 10, ""); err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestJournalTornTailIsIgnored(t *testing.T) {
	// Crash-safety property: an entry torn by a crash mid-append must be
	// ignored on replay, not corrupt the journal.
	path := filepath.Join(t.TempDir(), "j2.jsonl")
	j, err := OpenJournal(path, "snap-1", 5, "")
	if err != nil {
		t.Fatal(err)
	}
	j.Record(JournalEntry{Ordinal: 0, Length: 1, SHA256: "x"})
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	j.Close()

	// Simulate a torn append: half a JSON line at the end of the file.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"o":1,"len":52`)
	f.Close()

	j2, err := OpenJournal(path, "snap-1", 5, "")
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if j2.Count() != 1 || !j2.Has(0) {
		t.Fatalf("torn tail corrupted state: count=%d", j2.Count())
	}
}

func TestLimiterShrinksAndRamps(t *testing.T) {
	l := NewLimiter(8)
	if l.Current() != 8 {
		t.Fatalf("initial %d", l.Current())
	}
	for i := 0; i < 4; i++ {
		if err := l.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	l.OnThrottle()
	if cur := l.Current(); cur != 4 {
		t.Fatalf("after throttle %d, want 4", cur)
	}
	l.OnThrottle()
	if cur := l.Current(); cur != 2 {
		t.Fatalf("after second throttle %d, want 2", cur)
	}
	// Release the four acquired slots while still hot: no ramp allowed.
	for i := 0; i < 4; i++ {
		l.Release()
	}
	if cur := l.Current(); cur != 2 {
		t.Fatalf("ramped while hot: %d", cur)
	}
	// After a calm period, each release ramps usable by one.
	l.mu.Lock()
	l.lastThrottle = time.Now().Add(-time.Minute)
	l.mu.Unlock()
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	l.Release()
	if cur := l.Current(); cur != 3 {
		t.Fatalf("after ramp %d, want 3", cur)
	}
}

func TestLimiterRespectsContext(t *testing.T) {
	l := NewLimiter(1)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Acquire(ctx); err == nil {
		t.Fatal("expected context deadline")
	}
}

// newTestEnv builds a fake snapshot + request paths for manager tests.
func newTestEnv(t *testing.T, blocks int) (*testutil.FakeSnapshot, *Request) {
	t.Helper()
	const (
		snapshotID = "snap-testenv0000001"
		volumeGiB  = 1
		blockSize  = 512 * 1024
	)
	dir := t.TempDir()
	final := int64(volumeGiB*(1<<30)/blockSize) - 1
	indices := testutil.ScratchIndices(int64(blocks), final)
	fake := testutil.NewFake(snapshotID, volumeGiB, blockSize, indices, 400_000)
	image := filepath.Join(dir, snapshotID+".img")
	req := &Request{
		SnapshotID:   snapshotID,
		ImagePath:    image,
		ManifestPath: filepath.Join(dir, snapshotID+".manifest.json"),
		StatePath:    filepath.Join(dir, snapshotID+".state.jsonl"),
	}
	return fake, req
}

func TestManagerHappyPath(t *testing.T) {
	fake, req := newTestEnv(t, 80)
	res, err := New(fake, Options{Concurrency: 4, ToolVersion: "test"}).Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.NewBlocks != 80 || res.ResumedBlocks != 0 {
		t.Fatalf("new=%d resumed=%d", res.NewBlocks, res.ResumedBlocks)
	}
	if res.ImageSHA256 == "" || res.Manifest.Compression != nil {
		t.Fatalf("unexpected result shape: %+v", res)
	}
	// Raw kept by default when not compressing.
	if _, err := os.Stat(req.ImagePath); err != nil {
		t.Fatalf("raw image missing: %v", err)
	}
	if _, err := os.Stat(req.StatePath); !os.IsNotExist(err) {
		t.Fatal("journal still present")
	}
	// Every block's checksum must be present in the manifest.
	for i, b := range res.Manifest.Blocks {
		if b.SHA256 == "" || b.Length == 0 {
			t.Fatalf("block ordinal %d missing checksum/length", i)
		}
	}
}

func TestManagerResumeAfterCancellation(t *testing.T) {
	fake, req := newTestEnv(t, 90)

	// Cancel mid-download.
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	opts := Options{Concurrency: 4, Progress: func(u Update) {
		if u.Phase == PhaseDownload && u.BlocksDone >= 20 {
			once.Do(cancel)
		}
	}}
	if _, err := New(fake, opts).Run(ctx, req); err == nil {
		t.Fatal("expected cancellation error")
	}
	cancel()
	if _, err := os.Stat(req.StatePath); err != nil {
		t.Fatalf("journal missing after cancel: %v", err)
	}

	// Resume to completion.
	res, err := New(fake, Options{Concurrency: 4, ParanoidResume: true}).Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.NewBlocks+res.ResumedBlocks != 90 {
		t.Fatalf("new=%d resumed=%d", res.NewBlocks, res.ResumedBlocks)
	}
	if res.ResumedBlocks == 0 {
		t.Log("warning: nothing was checkpointed before cancellation (fast run)")
	}
	// No block fetched more than a reasonable number of times.
	for _, idx := range fake.Indices() {
		if n := fake.GetCallsFor(idx); n > 3 {
			t.Fatalf("block %d fetched %d times", idx, n)
		}
	}
}

func TestManagerParanoidResumeRedownloadsCorruptBlocks(t *testing.T) {
	fake, req := newTestEnv(t, 50)

	// Run once to completion, keep raw.
	if _, err := New(fake, Options{Concurrency: 4}).Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	m, err := manifest.Load(req.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate a "resume" state: rebuild a journal claiming all blocks
	// done, but corrupt one block on disk.
	j, err := OpenJournal(req.StatePath, req.SnapshotID, len(m.Blocks), "")
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range m.Blocks {
		j.Record(JournalEntry{Ordinal: i, Length: b.Length, SHA256: b.SHA256})
	}
	if err := j.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	j.Close()

	f, err := os.OpenFile(req.ImagePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	victim := m.Blocks[7]
	f.WriteAt([]byte{0xEE, 0xEE, 0xEE, 0xEE}, victim.Offset)
	f.Close()

	// Strip the manifest's completion so the manager re-runs the
	// download stage using the journal (as it would after a crash).
	os.Remove(req.ManifestPath)

	res, err := New(fake, Options{Concurrency: 4, ParanoidResume: true}).Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// The corrupted block must have been detected and re-fetched.
	if fake.GetCallsFor(victim.Index) < 2 {
		t.Fatalf("corrupted block %d was not re-fetched (calls=%d)", victim.Index, fake.GetCallsFor(victim.Index))
	}
	if res.ImageSHA256 == "" {
		t.Fatal("no verified hash after repair")
	}
}

func TestManagerCompressAndCleanup(t *testing.T) {
	fake, req := newTestEnv(t, 40)
	req.Compress = &CompressOptions{Level: 3}
	res, err := New(fake, Options{Concurrency: 4}).Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Manifest.Compression == nil || !res.Manifest.Compression.Verified {
		t.Fatalf("compression: %+v", res.Manifest.Compression)
	}
	if _, err := os.Stat(req.ImagePath + ".zst"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(req.ImagePath); !os.IsNotExist(err) {
		t.Fatal("raw image not removed by default")
	}
	// Idempotent re-run: nothing re-fetched, everything still verifies.
	before := fake.GetCalls()
	res2, err := New(fake, Options{Concurrency: 4}).Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if fake.GetCalls() != before || res2.NewBlocks != 0 {
		t.Fatalf("re-run refetched: calls %d->%d new=%d", before, fake.GetCalls(), res2.NewBlocks)
	}
}

func TestManagerForceDiscardsState(t *testing.T) {
	fake, req := newTestEnv(t, 30)
	// Seed a journal for a different block count to trigger mismatch.
	j, err := OpenJournal(req.StatePath, req.SnapshotID, 999, "")
	if err == nil {
		j.Close()
	}
	if _, err := New(fake, Options{}).Run(context.Background(), req); err == nil {
		t.Fatal("expected journal mismatch error")
	}
	req.Force = true
	if _, err := New(fake, Options{Concurrency: 4}).Run(context.Background(), req); err != nil {
		t.Fatalf("force run failed: %v", err)
	}
}

// newDeltaEnv builds a fake snapshot A plus a derived B with a known
// change set: modified, removed, and added blocks. B's data matches A
// except for those blocks, so a delta download must equal a full one.
func newDeltaEnv(t *testing.T, blocks int) (a, b *testutil.FakeSnapshot, modified, removed, added []int64) {
	t.Helper()
	const (
		volumeGiB = 1
		blockSize = 512 * 1024
	)
	final := int64(volumeGiB*(1<<30)/blockSize) - 1
	indices := testutil.ScratchIndices(int64(blocks), final)
	a = testutil.NewFake("snap-base00000000001", volumeGiB, blockSize, indices, blockSize)
	aIndices := a.Indices()
	allocated := make(map[int64]bool, len(aIndices))
	for _, idx := range aIndices {
		allocated[idx] = true
	}

	b = a.Derive("snap-next00000000002", volumeGiB)
	used := map[int64]bool{}

	pick := func(stride, offset int) (int64, bool) {
		for i := offset; i < len(aIndices); i += stride {
			idx := aIndices[i]
			if !used[idx] {
				used[idx] = true
				return idx, true
			}
		}
		return 0, false
	}

	// Modify five existing blocks (flip the first byte).
	for k := 0; k < 5; k++ {
		idx, ok := pick(3, k)
		if !ok {
			break
		}
		content := b.Data(idx)
		if content == nil {
			content = a.Data(idx)
		}
		content[0] ^= 0xFF
		b.UpdateBlockContent(idx, content)
		modified = append(modified, idx)
	}
	// Remove three existing blocks.
	for k := 0; k < 3; k++ {
		idx, ok := pick(4, 1+2*k)
		if !ok {
			break
		}
		b.RemoveBlock(idx)
		removed = append(removed, idx)
	}
	// Add four blocks in regions A never allocated.
	for k := int64(0); len(added) < 4; k++ {
		idx := final - 1 - k*7
		if idx < 0 || allocated[idx] {
			continue
		}
		b.AddBlock(idx, blockSize)
		added = append(added, idx)
	}
	return a, b, modified, removed, added
}

// deltaPaths builds request paths for a snapshot id in dir.
func deltaPaths(dir, snapshotID string) *Request {
	image := filepath.Join(dir, snapshotID+".img")
	return &Request{
		SnapshotID:   snapshotID,
		ImagePath:    image,
		ManifestPath: filepath.Join(dir, snapshotID+".manifest.json"),
		StatePath:    filepath.Join(dir, snapshotID+".state.jsonl"),
	}
}

func TestManagerDeltaSyncMatchesFullDownload(t *testing.T) {
	a, b, modified, removed, added := newDeltaEnv(t, 80)
	dir := t.TempDir()

	// Full download of the base.
	baseReq := deltaPaths(dir, a.SnapshotID)
	baseRes, err := New(a, Options{Concurrency: 4}).Run(context.Background(), baseReq)
	if err != nil {
		t.Fatal(err)
	}

	// Incremental download of B on top of A's image.
	deltaReq := deltaPaths(dir, b.SnapshotID)
	deltaReq.ImagePath = baseReq.ImagePath // delta builds on the base image
	deltaReq.BaseManifest = baseRes.Manifest
	deltaRes, err := New(b, Options{Concurrency: 4}).Run(context.Background(), deltaReq)
	if err != nil {
		t.Fatal(err)
	}

	wantChanged := len(modified) + len(added)
	if deltaRes.DeltaFrom != a.SnapshotID ||
		deltaRes.ChangedBlocks != wantChanged ||
		deltaRes.ZeroedBlocks != len(removed) {
		t.Fatalf("delta stats: from=%s changed=%d (want %d) zeroed=%d (want %d)",
			deltaRes.DeltaFrom, deltaRes.ChangedBlocks, wantChanged, deltaRes.ZeroedBlocks, len(removed))
	}

	// Only the changed blocks may be fetched from B.
	if got := b.GetCalls(); got != int64(wantChanged) {
		t.Fatalf("B GetBlock calls = %d, want exactly %d", got, wantChanged)
	}

	// The delta manifest must list exactly B's allocated blocks.
	bIndices := b.Indices()
	if deltaRes.Manifest.BlockCount != len(bIndices) {
		t.Fatalf("manifest blocks = %d, want %d", deltaRes.Manifest.BlockCount, len(bIndices))
	}
	for i, idx := range bIndices {
		if deltaRes.Manifest.Blocks[i].Index != idx {
			t.Fatalf("manifest block %d index %d, want %d", i, deltaRes.Manifest.Blocks[i].Index, idx)
		}
	}

	// Full download of B elsewhere must produce a byte-identical image.
	fullReq := deltaPaths(t.TempDir(), b.SnapshotID)
	fullRes, err := New(b, Options{Concurrency: 4}).Run(context.Background(), fullReq)
	if err != nil {
		t.Fatal(err)
	}
	if fullRes.ImageSHA256 != deltaRes.ImageSHA256 {
		t.Fatalf("delta image hash %s != full image hash %s", deltaRes.ImageSHA256, fullRes.ImageSHA256)
	}
	if b.GetCalls() != int64(wantChanged)+int64(len(bIndices)) {
		t.Fatalf("total B calls after full download = %d", b.GetCalls())
	}
}

func TestManagerDeltaSyncResume(t *testing.T) {
	a, b, _, _, _ := newDeltaEnv(t, 60)
	dir := t.TempDir()

	baseReq := deltaPaths(dir, a.SnapshotID)
	baseRes, err := New(a, Options{Concurrency: 4}).Run(context.Background(), baseReq)
	if err != nil {
		t.Fatal(err)
	}

	deltaReq := deltaPaths(dir, b.SnapshotID)
	deltaReq.ImagePath = baseReq.ImagePath
	deltaReq.BaseManifest = baseRes.Manifest

	// Interrupt mid-delta.
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	opts := Options{Concurrency: 4, Progress: func(u Update) {
		if u.Phase == PhaseDownload && u.BlocksDone >= 3 {
			once.Do(cancel)
		}
	}}
	if _, err := New(b, opts).Run(ctx, deltaReq); err == nil {
		t.Fatal("expected cancellation error")
	}
	cancel()

	// Resume to completion.
	res, err := New(b, Options{Concurrency: 4, ParanoidResume: true}).Run(context.Background(), deltaReq)
	if err != nil {
		t.Fatal(err)
	}
	if res.ImageSHA256 == "" || res.DeltaFrom != a.SnapshotID {
		t.Fatalf("unexpected resumed delta result: %+v", res)
	}
}

func TestManagerDeltaRejectsCorruptBase(t *testing.T) {
	a, b, _, _, _ := newDeltaEnv(t, 40)
	dir := t.TempDir()

	baseReq := deltaPaths(dir, a.SnapshotID)
	baseRes, err := New(a, Options{Concurrency: 4}).Run(context.Background(), baseReq)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt one byte of the base image; the sampled verification must
	// refuse to build the delta on it.
	f, err := os.OpenFile(baseReq.ImagePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	victim := baseRes.Manifest.Blocks[0] // stride sampling always checks ordinal 0
	f.WriteAt([]byte{0xAA}, victim.Offset+7)
	f.Close()

	deltaReq := deltaPaths(dir, b.SnapshotID)
	deltaReq.ImagePath = baseReq.ImagePath
	deltaReq.BaseManifest = baseRes.Manifest
	if _, err := New(b, Options{Concurrency: 4, BaseVerifySample: 8}).Run(context.Background(), deltaReq); err == nil {
		t.Fatal("expected base verification failure")
	}
}

func TestManagerDeltaGrowVolume(t *testing.T) {
	a, _, _, _, _ := newDeltaEnv(t, 30)
	// Derived with a larger volume and one new block beyond the old end.
	const grownGiB = 2
	const blockSize = 512 * 1024
	b := a.Derive("snap-grown0000000003", grownGiB)
	newIdx := int64(1)*(grownGiB*(1<<30)/blockSize) - 5 // beyond A's last index
	b.AddBlock(newIdx, blockSize)

	dir := t.TempDir()
	baseReq := deltaPaths(dir, a.SnapshotID)
	baseRes, err := New(a, Options{Concurrency: 4}).Run(context.Background(), baseReq)
	if err != nil {
		t.Fatal(err)
	}

	deltaReq := deltaPaths(dir, b.SnapshotID)
	deltaReq.ImagePath = baseReq.ImagePath
	deltaReq.BaseManifest = baseRes.Manifest
	res, err := New(b, Options{Concurrency: 4}).Run(context.Background(), deltaReq)
	if err != nil {
		t.Fatal(err)
	}
	if res.Manifest.LogicalSize != grownGiB*(1<<30) {
		t.Fatalf("logical size %d, want grown %d", res.Manifest.LogicalSize, grownGiB*(1<<30))
	}
	// The new block must be present and verified.
	found := false
	for _, blk := range res.Manifest.Blocks {
		if blk.Index == newIdx {
			found = blk.SHA256 != ""
		}
	}
	if !found {
		t.Fatal("new block beyond the old volume end missing from delta manifest")
	}
}
