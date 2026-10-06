package img

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// buildSZC writes a container with the given chunk contents
// (offset -> data), returning its path and the expected whole-image
// hash over logical bytes.
func buildSZC(t *testing.T, logical, blockSize int64, chunks map[int64][]byte) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.img.szc")
	offsets := make([]int64, 0, len(chunks))
	for off := range chunks {
		offsets = append(offsets, off)
	}
	w, err := NewSZCWriter(path, "snap-szc-test", 1, blockSize, logical, SortedOffsets(offsets), 3)
	if err != nil {
		t.Fatal(err)
	}
	// Submit deliberately out of order.
	var offs []int64
	for off := range chunks {
		offs = append(offs, off)
	}
	// Reverse order submission.
	for i := len(offs) - 1; i >= 0; i-- {
		if err := w.Submit(offs[i], chunks[offs[i]]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}

	// Expected image hash: chunk data at offsets, zeros elsewhere.
	h := sha256.New()
	var pos int64
	zero := make([]byte, 4096)
	addZero := func(n int64) {
		for n > 0 {
			w := min(n, int64(len(zero)))
			h.Write(zero[:w])
			n -= w
		}
	}
	sorted := SortedOffsets(offsets)
	for _, off := range sorted {
		if off > pos {
			addZero(off - pos)
		}
		h.Write(chunks[off])
		pos = off + int64(len(chunks[off]))
	}
	addZero(logical - pos)
	return path, hex.EncodeToString(h.Sum(nil))
}

func TestSZCRoundtripAndVerify(t *testing.T) {
	const blockSize = 512 * 1024
	chunks := map[int64][]byte{
		0:             bytes.Repeat([]byte{0x11}, blockSize),
		2 * blockSize: pseudoRandom(0x22, 0, blockSize),
		5 * blockSize: pseudoRandom(0x33, 0, blockSize/2), // short chunk
		9 * blockSize: bytes.Repeat([]byte{0}, blockSize), // all-zero -> hole
	}
	path, wantHash := buildSZC(t, 16*blockSize, blockSize, chunks)

	trailer, err := OpenSZC(path)
	if err != nil {
		t.Fatal(err)
	}
	if trailer.ImageSHA256 != wantHash {
		t.Fatalf("trailer hash %s, want %s", trailer.ImageSHA256, wantHash)
	}
	// The all-zero chunk must not be stored.
	for _, c := range trailer.Chunks {
		if c.Offset == 9*blockSize {
			t.Fatal("all-zero chunk was stored")
		}
	}
	if len(trailer.Chunks) != 3 {
		t.Fatalf("stored chunks = %d, want 3", len(trailer.Chunks))
	}
	if err := VerifySZC(context.Background(), path, trailer, nil); err != nil {
		t.Fatal(err)
	}

	// Random access through the reader.
	r, err := NewSZCReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, 64)
	if _, err := r.ReadAt(buf, 2*blockSize+100); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, pseudoRandom(0x22, 100, 64)) { // chunk-relative stream position
		t.Fatal("random read mismatch in stored chunk")
	}
	if _, err := r.ReadAt(buf, 7*blockSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, make([]byte, 64)) {
		t.Fatal("hole read did not return zeros")
	}
	// Read spanning hole -> chunk boundary.
	span := make([]byte, blockSize)
	if _, err := r.ReadAt(span, blockSize+blockSize/2); err != nil {
		t.Fatal(err)
	}
	want := append(make([]byte, blockSize/2), pseudoRandom(0x22, 0, blockSize/2)...) // chunk-relative
	if !bytes.Equal(span, want) {
		t.Fatal("spanning read mismatch")
	}
}

func TestSZCDetectsCorruption(t *testing.T) {
	const blockSize = 512 * 1024
	chunks := map[int64][]byte{
		blockSize: pseudoRandom(0x55, 0, blockSize),
	}
	path, _ := buildSZC(t, 8*blockSize, blockSize, chunks)

	// Corrupt one stored frame byte (inside the file, before the trailer).
	trailer, err := OpenSZC(path)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt([]byte{0x00}, trailer.Chunks[0].StoreOff+40)
	f.Close()

	if _, err := OpenSZC(path); err == nil {
		// Trailer itself may still parse; verification must fail.
		t2, _ := OpenSZC(path)
		if err := VerifySZC(context.Background(), path, t2, nil); err == nil {
			t.Fatal("corrupted chunk passed verification")
		}
	} else {
		if err := VerifySZC(context.Background(), path, trailer, nil); err == nil {
			t.Fatal("corrupted container passed verification")
		}
	}
}

func TestSZCRejectsBadMagicAndTruncatedFooter(t *testing.T) {
	dir := t.TempDir()
	notSzc := filepath.Join(dir, "plain.img.szc")
	os.WriteFile(notSzc, []byte("this is not a container, but it is long enough to pass the size check.........."), 0o644)
	if _, err := OpenSZC(notSzc); err == nil {
		t.Fatal("expected magic rejection")
	}

	const blockSize = 512 * 1024
	chunks := map[int64][]byte{0: pseudoRandom(0x77, 0, blockSize)}
	path, _ := buildSZC(t, 4*blockSize, blockSize, chunks)
	st, _ := os.Stat(path)
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.Truncate(st.Size() - 5) // cut into the footer
	f.Close()
	if _, err := OpenSZC(path); err == nil {
		t.Fatal("expected truncated-footer rejection")
	}
}

func TestSZCWriterRequiresAllSubmissions(t *testing.T) {
	const blockSize = 512 * 1024
	path := filepath.Join(t.TempDir(), "partial.img.szc")
	w, err := NewSZCWriter(path, "snap-x", 1, blockSize, 4*blockSize, []int64{0, blockSize}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Submit(0, pseudoRandom(0x99, 0, blockSize)); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(); err == nil {
		t.Fatal("expected missing-submission error")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("partial container should not exist after failed finalize")
	}
}

// pseudoRandom generates a deterministic byte stream; the returned slice
// is the stream's [start, start+n) window, so slices taken at different
// offsets of the same seed align.
func pseudoRandom(seed byte, start, n int64) []byte {
	buf := make([]byte, start+n)
	s := uint64(seed)*0x0101010101010101 | 1
	for i := range buf {
		if i%8 == 0 {
			s = s*6364136223846793005 + 1442695040888963407
		}
		buf[i] = byte(s >> (uint(i%8) * 8))
	}
	return buf[start:]
}
