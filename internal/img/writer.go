// Package img handles the local image side of snaport: creating and
// writing sparse raw images, verifying them against a manifest, and
// compressing them with zstd including a decode restore-test.
package img

import (
	"bytes"
	"fmt"
	"os"
)

// Writer writes blocks into a sparse raw image file at their correct
// offsets. Unallocated regions are never touched, so on NTFS (file marked
// sparse) and POSIX filesystems they occupy no disk space. Writers are
// safe for concurrent positional writes.
type Writer struct {
	f    *os.File
	path string
	// logical size in bytes (volume GiB * 1 GiB)
	logical int64
}

// NewWriter opens (creating if needed) the image at path, marks it sparse
// and sets its logical length to logicalSize. If the file already exists
// with a different length it is truncated - resume callers must ensure a
// matching manifest before calling.
func NewWriter(path string, logicalSize int64) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := markSparse(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("marking %s sparse: %w", path, err)
	}
	if err := f.Truncate(logicalSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("setting image length to %d: %w", logicalSize, err)
	}
	return &Writer{f: f, path: path, logical: logicalSize}, nil
}

// Path returns the image file path.
func (w *Writer) Path() string { return w.path }

// LogicalSize returns the logical length of the image in bytes.
func (w *Writer) LogicalSize() int64 { return w.logical }

// WriteBlock writes one block's data at its offset. All-zero blocks are
// skipped entirely so the region stays a hole; reading it later yields
// zeros, which is semantically identical.
func (w *Writer) WriteBlock(offset int64, data []byte) error {
	if offset < 0 || offset+int64(len(data)) > w.logical {
		return fmt.Errorf("write [%d,%d) outside image bounds [0,%d)", offset, offset+int64(len(data)), w.logical)
	}
	if isAllZero(data) {
		return nil
	}
	if _, err := w.f.WriteAt(data, offset); err != nil {
		return fmt.Errorf("writing %d bytes at offset %d: %w", len(data), offset, err)
	}
	return nil
}

// Sync flushes written data to durable storage.
func (w *Writer) Sync() error { return w.f.Sync() }

// ReadBlock reads len(buf) bytes at offset (fills holes with zeros, as
// the filesystem presents them).
func (w *Writer) ReadBlock(offset int64, buf []byte) error {
	_, err := readFullAt(w.f, buf, offset)
	return err
}

// EnableNTFSCompression turns on transparent NTFS compression for the
// image file (Windows only, best effort).
func (w *Writer) EnableNTFSCompression() error { return EnableNTFSCompression(w.f) }

// Close closes the underlying file.
func (w *Writer) Close() error { return w.f.Close() }

// IsAllZero reports whether b consists only of zero bytes. Allocated
// blocks whose data is all zeros are kept as holes in the sparse image.
func IsAllZero(b []byte) bool { return isAllZero(b) }

// zeroBlock is the comparison buffer for hole detection.
var zeroBlock = make([]byte, 4096)

// isAllZero reports whether b consists only of zero bytes. Used to keep
// allocated-but-zero snapshot blocks as holes in the sparse image.
func isAllZero(b []byte) bool {
	for len(b) > 0 {
		n := min(len(b), len(zeroBlock))
		if !bytes.Equal(b[:n], zeroBlock[:n]) {
			return false
		}
		b = b[n:]
	}
	return true
}
