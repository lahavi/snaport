//go:build !windows

package img

import (
	"fmt"
	"os"
	"syscall"
)

// On POSIX filesystems (ext4, XFS, APFS, ...) files written past EOF via
// positional writes are naturally sparse; no flag is needed.

func markSparse(f *os.File) error { return nil }

// zeroBuffer backs the POSIX zeroRange fallback.
var zeroBuffer = make([]byte, 4096)

// zeroRange overwrites the range with zeros; POSIX sparse files re-punch
// all-zero pages automatically in most filesystems.
func zeroRange(f *os.File, offset, length int64) error {
	buf := zeroBuffer
	for length > 0 {
		n := min(length, int64(len(buf)))
		if _, err := f.WriteAt(buf[:n], offset); err != nil {
			return err
		}
		offset += n
		length -= n
	}
	return nil
}

// EnableNTFSCompression is a no-op outside Windows.
func EnableNTFSCompression(f *os.File) error {
	return fmt.Errorf("NTFS compression is only available on Windows")
}

// OnDiskSize reports allocated blocks * 512 for the file.
func OnDiskSize(path string) (int64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Blocks) * 512, nil
}

// FreeSpace reports available bytes on the filesystem containing path.
func FreeSpace(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}
