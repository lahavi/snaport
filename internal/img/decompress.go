package img

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// DecompressImage streams a .img.zst archive back into a sparse raw
// image, verifying the decoded bytes against expectSHAHex/expectSize as
// it goes. It is used to rebuild a raw base image (deleted after
// verified compression) before an incremental sync builds on it.
func DecompressImage(ctx context.Context, zstPath, dstPath, expectSHAHex string, expectSize int64) error {
	f, err := os.Open(zstPath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return fmt.Errorf("opening zstd stream: %w", err)
	}
	defer dec.Close()

	// Create the destination through NewWriter so it is marked sparse
	// and pre-extended to the logical size.
	w, err := NewWriter(dstPath, expectSize)
	if err != nil {
		return err
	}
	defer w.Close()

	hash := sha256.New()
	buf := make([]byte, 4<<20)
	var written int64
	for written < expectSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, rerr := io.ReadFull(dec, buf)
		if n > 0 {
			// Positional write at the current offset keeps holes for the
			// zero regions of the decoded stream.
			if _, werr := w.f.WriteAt(buf[:n], written); werr != nil {
				return fmt.Errorf("writing %s: %w", dstPath, werr)
			}
			hash.Write(buf[:n])
			written += int64(n)
		}
		if rerr != nil {
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				return fmt.Errorf("archive %s ended after %d of %d bytes", zstPath, written, expectSize)
			}
			return fmt.Errorf("decoding %s: %w", zstPath, rerr)
		}
	}
	// Drain any surplus bytes: the archive must decode to exactly the
	// expected size.
	var sink [1]byte
	if n, _ := dec.Read(sink[:]); n != 0 {
		return fmt.Errorf("archive %s decodes to more than %d bytes", zstPath, expectSize)
	}
	if err := w.Sync(); err != nil {
		return err
	}

	got := hex.EncodeToString(hash.Sum(nil))
	if got != expectSHAHex {
		return fmt.Errorf("decoded hash %s does not match expected %s", got, expectSHAHex)
	}
	return nil
}
