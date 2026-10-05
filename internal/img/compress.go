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

// CompressResult describes the compressed artifact.
type CompressResult struct {
	Size          int64  // compressed file size in bytes
	SHA256        string // hex SHA-256 of the compressed file
	DecodedBytes  int64  // bytes produced when decoding (restore test)
	RestoreTestOK bool   // decoded stream hash matched the image hash
}

// CompressImage streams the raw image into a zstd file at dstPath
// (level 1-19). Zeros in sparse holes compress to nearly nothing. The
// compressed file is finalized with an atomic rename.
func CompressImage(ctx context.Context, srcPath, dstPath string, level int, progress func(bytesDone, bytesTotal int64)) (int64, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer src.Close()

	srcStat, err := src.Stat()
	if err != nil {
		return 0, err
	}

	tmpPath := dstPath + ".part"
	dst, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmpPath) // no-op after successful rename

	enc, err := zstd.NewWriter(dst,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
		zstd.WithEncoderConcurrency(0), // GOMAXPROCS
	)
	if err != nil {
		dst.Close()
		return 0, fmt.Errorf("zstd encoder (level %d): %w", level, err)
	}

	buf := make([]byte, 4<<20)
	var written int64
	for written < srcStat.Size() {
		if err := ctx.Err(); err != nil {
			enc.Close()
			dst.Close()
			return 0, err
		}
		n, rerr := io.ReadFull(src, buf)
		if n > 0 {
			if _, werr := enc.Write(buf[:n]); werr != nil {
				enc.Close()
				dst.Close()
				return 0, fmt.Errorf("compressing %s: %w", srcPath, werr)
			}
			written += int64(n)
			if progress != nil {
				progress(written, srcStat.Size())
			}
		}
		if rerr != nil && rerr != io.ErrUnexpectedEOF && rerr != io.EOF {
			enc.Close()
			dst.Close()
			return 0, fmt.Errorf("reading %s: %w", srcPath, rerr)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			if written != srcStat.Size() {
				enc.Close()
				dst.Close()
				return 0, fmt.Errorf("short read of %s: %d of %d bytes", srcPath, written, srcStat.Size())
			}
			break
		}
	}

	if err := enc.Close(); err != nil {
		dst.Close()
		return 0, fmt.Errorf("finalizing zstd stream: %w", err)
	}
	if err := dst.Sync(); err != nil {
		dst.Close()
		return 0, err
	}
	if err := dst.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return 0, err
	}

	st, err := os.Stat(dstPath)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// RestoreTest decodes the compressed file end to end, verifying the zstd
// checksums embedded in the stream and comparing the SHA-256 of the
// decoded bytes against the expected image hash. This is the "can I get
// my data back" gate before the raw image is allowed to be deleted.
func RestoreTest(ctx context.Context, zstPath, expectSHAHex string, expectSize int64) (*CompressResult, error) {
	f, err := os.Open(zstPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, fmt.Errorf("opening zstd stream: %w", err)
	}
	defer dec.Close()

	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(dec, expectSize+1))
	if err != nil {
		return nil, fmt.Errorf("decoding %s: %w", zstPath, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Drain any trailing bytes to force the decoder to validate the end
	// of the stream and its content checksums.
	var sink [4096]byte
	trailing, _ := dec.Read(sink[:])
	if n+int64(trailing) != expectSize {
		return nil, fmt.Errorf("decoded size %d does not match image size %d", n+int64(trailing), expectSize)
	}

	gotHex := hex.EncodeToString(hash.Sum(nil))
	res := &CompressResult{
		DecodedBytes:  n,
		RestoreTestOK: gotHex == expectSHAHex,
	}
	if !res.RestoreTestOK {
		return res, fmt.Errorf("restore test failed: decoded hash %s does not match image hash %s", gotHex, expectSHAHex)
	}

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	res.Size = st.Size()

	cf, err := os.Open(zstPath)
	if err != nil {
		return nil, err
	}
	defer cf.Close()
	cHash := sha256.New()
	if _, err := io.Copy(cHash, cf); err != nil {
		return nil, err
	}
	res.SHA256 = hex.EncodeToString(cHash.Sum(nil))
	return res, nil
}
