package img

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"snaport/internal/manifest"
)

// VerifyResult carries the outcome of a full image verification pass.
type VerifyResult struct {
	// ImageSHA256 is the hex SHA-256 over the entire logical image
	// (allocated data plus zero holes).
	ImageSHA256 string
	// BlocksChecked is the number of allocated blocks re-verified.
	BlocksChecked int
	// AllocatedBytes is the sum of verified block lengths.
	AllocatedBytes int64
	// ZeroBlocks counts allocated blocks whose content is all zeros;
	// they are stored as holes in the sparse image.
	ZeroBlocks int
}

// Verify performs the restore-test of the raw image against its manifest:
//
//  1. the file's logical length must equal the manifest's logical size;
//  2. every allocated block is re-read and its SHA-256 compared with the
//     checksum recorded when it was downloaded (holes implicitly read as
//     zeros, exactly what EBS semantics require);
//  3. the SHA-256 of the entire logical image is computed.
//
// It performs a single sequential pass: chunks are aligned to the block
// size, so each chunk contains at most one block and both hashes are fed
// from the same read.
func Verify(ctx context.Context, path string, m *manifest.Manifest, progress func(bytesDone, bytesTotal int64)) (*VerifyResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != m.LogicalSize {
		return nil, fmt.Errorf("image size %d does not match manifest logical size %d", st.Size(), m.LogicalSize)
	}

	imageHash := sha256.New()
	byOffset := make(map[int64]manifest.Block, len(m.Blocks))
	for _, b := range m.Blocks {
		byOffset[b.Offset] = b
	}

	// Chunks must be exactly one block so every block starts on a chunk
	// boundary and both hashes are fed from the same read.
	chunk := m.BlockSize
	buf := make([]byte, chunk)
	var done int64
	var allocBytes int64
	var blocksChecked int
	var zeroBlocks int

	for done < m.LogicalSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n := int64(len(buf))
		if remaining := m.LogicalSize - done; remaining < n {
			n = remaining
		}
		if _, err := readFullAt(f, buf[:n], done); err != nil {
			return nil, fmt.Errorf("reading image at offset %d: %w", done, err)
		}
		imageHash.Write(buf[:n])

		if b, ok := byOffset[done]; ok {
			sum := sha256.Sum256(buf[:b.Length])
			got := base64.StdEncoding.EncodeToString(sum[:])
			if got != b.SHA256 {
				return nil, fmt.Errorf("block at offset %d (index %d): checksum mismatch: manifest %s, on disk %s",
					b.Offset, b.Index, b.SHA256, got)
			}
			blocksChecked++
			allocBytes += b.Length
			if isAllZero(buf[:b.Length]) {
				zeroBlocks++
			}
		}
		done += n
		if progress != nil {
			progress(done, m.LogicalSize)
		}
	}

	return &VerifyResult{
		ImageSHA256:    hex.EncodeToString(imageHash.Sum(nil)),
		BlocksChecked:  blocksChecked,
		AllocatedBytes: allocBytes,
		ZeroBlocks:     zeroBlocks,
	}, nil
}

// readFullAt fills buf starting at off, retrying short reads.
func readFullAt(f *os.File, buf []byte, off int64) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.ReadAt(buf[total:], off+int64(total))
		total += n
		if err != nil {
			if err == io.EOF && total == len(buf) {
				return total, nil
			}
			return total, err
		}
	}
	return total, nil
}
