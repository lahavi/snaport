//go:build windows

package img

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// markSparse flags the file as sparse so regions we never write (the
// snapshot's unallocated blocks) consume no disk space. Must be called
// before data is written.
func markSparse(f *os.File) error {
	var bytesReturned uint32
	err := windows.DeviceIoControl(
		windows.Handle(f.Fd()),
		windows.FSCTL_SET_SPARSE,
		nil, 0,
		nil, 0,
		&bytesReturned, nil,
	)
	if err != nil {
		return fmt.Errorf("FSCTL_SET_SPARSE: %w", err)
	}
	return nil
}

// zeroRange wipes [offset, offset+length) via FSCTL_SET_ZERO_DATA: the
// region reads as zeros and, in a sparse file, is deallocated back to a
// hole.
func zeroRange(f *os.File, offset, length int64) error {
	info := windows.FileZeroDataInformation{
		FileOffset:      offset,
		BeyondFinalZero: offset + length,
	}
	var bytesReturned uint32
	err := windows.DeviceIoControl(
		windows.Handle(f.Fd()),
		windows.FSCTL_SET_ZERO_DATA,
		(*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)),
		nil, 0,
		&bytesReturned, nil,
	)
	if err != nil {
		return fmt.Errorf("FSCTL_SET_ZERO_DATA: %w", err)
	}
	return nil
}

// EnableNTFSCompression turns on NTFS transparent compression for the
// file (best effort). On non-NTFS volumes or without privileges this
// fails and is reported as a warning by the caller.
func EnableNTFSCompression(f *os.File) error {
	// COMPRESSION_FORMAT_DEFAULT = 1 (LZNT1+)
	var format uint16 = 1
	var bytesReturned uint32
	err := windows.DeviceIoControl(
		windows.Handle(f.Fd()),
		windows.FSCTL_SET_COMPRESSION,
		(*byte)(unsafe.Pointer(&format)), uint32(unsafe.Sizeof(format)),
		nil, 0,
		&bytesReturned, nil,
	)
	if err != nil {
		return fmt.Errorf("FSCTL_SET_COMPRESSION: %w", err)
	}
	return nil
}

// OnDiskSize reports how much space the file actually occupies (for
// sparse files this is far below the logical size). It uses the
// allocation size reported by the file system.
func OnDiskSize(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var info struct {
		AllocationSize int64
		EndOfFile      int64
		NumberOfLinks  uint32
		DeletePending  bool
		Directory      bool
		_              [3]byte // C struct padding
	}
	err = windows.GetFileInformationByHandleEx(
		windows.Handle(f.Fd()),
		windows.FileStandardInfo,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
	if err != nil {
		return 0, err
	}
	return info.AllocationSize, nil
}

// FreeSpace reports available bytes on the volume containing path.
func FreeSpace(path string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var free, total, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(p, &free, &total, &totalFree); err != nil {
		return 0, err
	}
	return free, nil
}
