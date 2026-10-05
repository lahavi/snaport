package img

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"snaport/internal/manifest"
)

func testManifest(blockSize int64, blocks []manifest.Block, logical int64) *manifest.Manifest {
	return &manifest.Manifest{
		Kind:          manifest.KindSnapshot,
		SnapshotID:    "snap-test",
		VolumeSizeGiB: logical / manifest.GiB,
		BlockSize:     blockSize,
		LogicalSize:   logical,
		BlockCount:    len(blocks),
		Blocks:        blocks,
	}
}

func TestSparseWriterHolesAndBounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.img")
	const blockSize = 512 * 1024

	w, err := NewWriter(path, 2*manifest.GiB)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0xAB}, blockSize)
	if err := w.WriteBlock(5*blockSize, data); err != nil {
		t.Fatal(err)
	}
	// All-zero block: skipped, stays a hole.
	if err := w.WriteBlock(9*blockSize, make([]byte, blockSize)); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	// Out-of-bounds write must fail.
	if err := w.WriteBlock(2*manifest.GiB, data); err == nil {
		t.Fatal("expected bounds error")
	}
	w.Close()

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 2*manifest.GiB {
		t.Fatalf("logical size %d, want %d", st.Size(), 2*manifest.GiB)
	}

	// Holes must read as zeros.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	hole := make([]byte, 4096)
	if _, err := f.ReadAt(hole, blockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hole, make([]byte, 4096)) {
		t.Fatal("hole did not read as zeros")
	}

	// On Windows/NTFS the on-disk size must be far below logical.
	if runtime.GOOS == "windows" {
		onDisk, err := OnDiskSize(path)
		if err != nil {
			t.Fatalf("OnDiskSize: %v", err)
		}
		if onDisk >= 2*manifest.GiB {
			t.Fatalf("sparse file occupies %d bytes of a 2 GiB logical image", onDisk)
		}
		t.Logf("2 GiB logical image occupies %d bytes on disk", onDisk)
	}
}

func TestVerifyDetectsCorruptionAndMatchesHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v.img")
	const blockSize = 512 * 1024

	payload := make([]byte, blockSize)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	sum := sha256.Sum256(payload)
	b64 := base64.StdEncoding.EncodeToString(sum[:])

	w, err := NewWriter(path, manifest.GiB)
	if err != nil {
		t.Fatal(err)
	}
	blocks := []manifest.Block{
		{Index: 2, Offset: 2 * blockSize, Length: blockSize, SHA256: b64},
	}
	if err := w.WriteBlock(2*blockSize, payload); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	m := testManifest(blockSize, blocks, manifest.GiB)
	vr, err := Verify(context.Background(), path, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vr.BlocksChecked != 1 {
		t.Fatalf("blocks checked %d", vr.BlocksChecked)
	}
	// Recompute the expected whole-image hash independently: one payload
	// at offset 2*blockSize inside 1 GiB of zeros.
	h := sha256.New()
	h.Write(make([]byte, 2*blockSize))
	h.Write(payload)
	h.Write(make([]byte, manifest.GiB-3*blockSize))
	want := hex.EncodeToString(h.Sum(nil))
	if vr.ImageSHA256 != want {
		t.Fatalf("image hash %s, want %s", vr.ImageSHA256, want)
	}

	// Corrupt one byte -> verification must fail.
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt([]byte{0xFF}, 2*blockSize+100)
	f.Close()
	if _, err := Verify(context.Background(), path, m, nil); err == nil {
		t.Fatal("corrupted block passed verification")
	}
}

func TestVerifyRejectsWrongLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "short.img")
	const blockSize = 512 * 1024
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	m := testManifest(blockSize, nil, manifest.GiB)
	if _, err := Verify(context.Background(), path, m, nil); err == nil {
		t.Fatal("expected size mismatch error")
	}
}

func TestCompressAndRestoreTestRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.img")
	zst := path + ".zst"
	const blockSize = 512 * 1024

	w, err := NewWriter(path, manifest.GiB)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, blockSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := w.WriteBlock(3*blockSize, payload); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	// Image hash (independent computation).
	h := sha256.New()
	h.Write(make([]byte, 3*blockSize))
	h.Write(payload)
	h.Write(make([]byte, manifest.GiB-4*blockSize))
	wantHex := hex.EncodeToString(h.Sum(nil))

	size, err := CompressImage(context.Background(), path, zst, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if size >= manifest.GiB {
		t.Fatalf("compressed %d bytes of mostly-zero 1 GiB image", size)
	}
	rt, err := RestoreTest(context.Background(), zst, wantHex, manifest.GiB)
	if err != nil {
		t.Fatal(err)
	}
	if !rt.RestoreTestOK || rt.DecodedBytes != manifest.GiB {
		t.Fatalf("restore test: %+v", rt)
	}

	// Corrupt the archive -> restore test must fail.
	f, _ := os.OpenFile(zst, os.O_RDWR, 0)
	f.WriteAt([]byte{0x00}, size-50)
	f.Close()
	if _, err := RestoreTest(context.Background(), zst, wantHex, manifest.GiB); err == nil {
		t.Fatal("corrupted archive passed restore test")
	}
}

func TestFreeSpace(t *testing.T) {
	free, err := FreeSpace(t.TempDir() + string(os.PathSeparator))
	if err != nil {
		t.Fatal(err)
	}
	if free == 0 {
		t.Fatal("no free space reported in temp dir")
	}
}
