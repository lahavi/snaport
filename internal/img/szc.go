// Package img: the .img.szc chunked zstd container.
//
// Format (all integers big-endian):
//
//	[4]  magic "SZTC"
//	[4]  format version (u32)
//	...  chunk records, each:
//	     [8] logical offset in the image (u64)
//	     [4] uncompressed length (u32; <= blockSize)
//	     [4] compressed length (u32)
//	     [N] zstd frame (independently decompressible)
//	...  trailer JSON (geometry, per-chunk index, whole-image SHA-256)
//	[8]  trailer offset (u64)
//	[4]  trailer length (u32)
//	[4]  CRC32 (IEEE) of the trailer bytes
//
// Regions of the logical image without a stored chunk read as zeros:
// unallocated snapshot blocks and allocated-but-zero blocks are simply
// omitted, giving the container the same storage efficiency as a sparse
// raw image. Chunks are independently compressed, so any 512 KiB block
// can be decoded by seeking to it alone.
package img

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// SZCMagic identifies the chunked container format.
var SZCMagic = [4]byte{'S', 'Z', 'T', 'C'}

// szcFooterLen is the fixed footer: trailer offset + length + CRC.
const szcFooterLen = 16

// SZCVersion is the current container format version.
const SZCVersion uint32 = 1

// SZCChunk is one stored chunk's index entry.
type SZCChunk struct {
	Offset   int64  `json:"o"` // logical offset in the image
	Length   int32  `json:"l"` // uncompressed length
	StoreOff int64  `json:"s"` // byte offset of the zstd frame in the file
	StoreLen int32  `json:"c"` // compressed length
	SHA256   string `json:"h"` // hex SHA-256 of the uncompressed chunk
}

// SZCTrailer is the container's index and geometry record.
type SZCTrailer struct {
	Version     uint32     `json:"version"`
	SnapshotID  string     `json:"snapshotId"`
	VolumeGiB   int64      `json:"volumeGiB"`
	BlockSize   int64      `json:"blockSize"`
	LogicalSize int64      `json:"logicalSize"`
	ImageSHA256 string     `json:"imageSha256"`
	Chunks      []SZCChunk `json:"chunks"`
}

// OpenSZC parses the trailer of an existing container.
func OpenSZC(path string) (*SZCTrailer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(4+4+szcFooterLen) {
		return nil, fmt.Errorf("%s is too small to be an SZC container", path)
	}
	// Footer.
	footer := make([]byte, szcFooterLen)
	if _, err := f.ReadAt(footer, st.Size()-szcFooterLen); err != nil {
		return nil, err
	}
	trailerOff := int64(binary.BigEndian.Uint64(footer[0:8]))
	trailerLen := binary.BigEndian.Uint32(footer[8:12])
	trailerCRC := binary.BigEndian.Uint32(footer[12:16])
	if trailerOff < 8 || trailerOff+int64(trailerLen)+szcFooterLen != st.Size() {
		return nil, fmt.Errorf("%s: corrupt trailer geometry", path)
	}
	raw := make([]byte, trailerLen)
	if _, err := f.ReadAt(raw, trailerOff); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(raw) != trailerCRC {
		return nil, fmt.Errorf("%s: trailer checksum mismatch", path)
	}
	// Magic.
	var magic [4]byte
	if _, err := f.ReadAt(magic[:], 0); err != nil {
		return nil, err
	}
	if magic != SZCMagic {
		return nil, fmt.Errorf("%s: not an SZC container", path)
	}
	var t SZCTrailer
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("%s: bad trailer: %w", path, err)
	}
	if t.Version != SZCVersion {
		return nil, fmt.Errorf("%s: unsupported container version %d", path, t.Version)
	}
	if t.BlockSize <= 0 || t.LogicalSize <= 0 {
		return nil, fmt.Errorf("%s: invalid trailer geometry", path)
	}
	sort.Slice(t.Chunks, func(i, j int) bool { return t.Chunks[i].Offset < t.Chunks[j].Offset })
	for i, c := range t.Chunks {
		if c.Offset < 0 || c.Length <= 0 || c.Length > int32(t.BlockSize) ||
			c.Offset+int64(c.Length) > t.LogicalSize {
			return nil, fmt.Errorf("%s: chunk %d has invalid geometry", path, i)
		}
		if i > 0 && t.Chunks[i-1].Offset+int64(t.Chunks[i-1].Length) > c.Offset {
			return nil, fmt.Errorf("%s: chunks %d..%d overlap", path, i-1, i)
		}
	}
	return &t, nil
}

// ReadSZCChunk decodes one stored chunk.
func ReadSZCChunk(f *os.File, c SZCChunk) ([]byte, error) {
	frame := make([]byte, c.StoreLen)
	if _, err := f.ReadAt(frame, c.StoreOff); err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(newBytesReader(frame))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	out := make([]byte, c.Length)
	if _, err := io.ReadFull(dec, out); err != nil {
		return nil, fmt.Errorf("decoding chunk at %d: %w", c.Offset, err)
	}
	sum := sha256.Sum256(out)
	if hex.EncodeToString(sum[:]) != c.SHA256 {
		return nil, fmt.Errorf("chunk at logical offset %d: checksum mismatch", c.Offset)
	}
	return out, nil
}

// VerifySZC walks every stored chunk, decompressing and checksumming it,
// streaming the full logical image (holes as zeros) into a whole-image
// SHA-256 compared against the trailer. Progress reports logical bytes.
func VerifySZC(ctx context.Context, path string, t *SZCTrailer, progress func(done, total int64)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	imageHash := sha256.New()
	zeros := make([]byte, 4096)
	var pos int64
	for _, c := range t.Chunks {
		if c.Offset > pos {
			n := c.Offset - pos
			for n > 0 {
				w := min(n, int64(len(zeros)))
				imageHash.Write(zeros[:w])
				n -= w
			}
			pos = c.Offset
		}
		data, err := ReadSZCChunk(f, c)
		if err != nil {
			return err
		}
		imageHash.Write(data)
		pos += int64(c.Length)
		if progress != nil {
			progress(pos, t.LogicalSize)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if t.LogicalSize > pos {
		n := t.LogicalSize - pos
		for n > 0 {
			w := min(n, int64(len(zeros)))
			imageHash.Write(zeros[:w])
			n -= w
		}
	}
	got := hex.EncodeToString(imageHash.Sum(nil))
	if got != t.ImageSHA256 {
		return fmt.Errorf("whole-image hash %s does not match trailer %s", got, t.ImageSHA256)
	}
	return nil
}

// SZCReader provides random access to logical byte ranges of a container
// (holes read as zeros).
type SZCReader struct {
	f  *os.File
	t  *SZCTrailer
	by map[int64]SZCChunk // logical offset -> chunk
}

// NewSZCReader opens a container for random access.
func NewSZCReader(path string) (*SZCReader, error) {
	t, err := OpenSZC(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &SZCReader{f: f, t: t, by: make(map[int64]SZCChunk, len(t.Chunks))}
	for _, c := range t.Chunks {
		r.by[c.Offset] = c
	}
	return r, nil
}

// ReadAt implements logical reads, synthesizing zeros for holes. Chunks
// are decompressed on demand and cached by the caller if needed.
func (r *SZCReader) ReadAt(p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		pos := off + int64(total)
		if pos >= r.t.LogicalSize {
			return total, io.EOF
		}
		// Find the chunk covering pos, if any.
		c, ok := r.by[pos-(pos%r.t.BlockSize)]
		if !ok || pos >= c.Offset+int64(c.Length) {
			// Hole (or the zero tail of a short chunk): zeros until the
			// next chunk or block boundary.
			end := (pos/r.t.BlockSize + 1) * r.t.BlockSize
			if next := r.nextChunkStart(pos); next >= 0 && next < end {
				end = next
			}
			n := int(min(int64(len(p)-total), end-pos))
			for i := 0; i < n; i++ {
				p[total+i] = 0
			}
			total += n
			continue
		}
		data, err := ReadSZCChunk(r.f, c)
		if err != nil {
			return total, err
		}
		within := pos - c.Offset
		n := copy(p[total:], data[within:])
		total += n
	}
	return total, nil
}

func (r *SZCReader) nextChunkStart(after int64) int64 {
	i := sort.Search(len(r.t.Chunks), func(i int) bool { return r.t.Chunks[i].Offset > after })
	if i < len(r.t.Chunks) {
		return r.t.Chunks[i].Offset
	}
	return -1
}

// Close releases the underlying file.
func (r *SZCReader) Close() error { return r.f.Close() }

// Trailer returns the container's parsed trailer.
func (r *SZCReader) Trailer() *SZCTrailer { return r.t }

// ErrNotSZC is returned when a file lacks the container magic.
var ErrNotSZC = errors.New("not an SZC container")

// bytesReader adapts a byte slice to io.Reader for zstd.
type bytesReader struct {
	b []byte
	i int
}

func newBytesReader(b []byte) *bytesReader { return &bytesReader{b: b} }
func (r *bytesReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
