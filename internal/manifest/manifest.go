// Package manifest defines the on-disk manifest that describes a downloaded
// snapshot image: its geometry, every allocated block with its SHA-256
// checksum, the full-image hash, and compression metadata.
//
// The manifest is what makes resume, verification and restore-testing
// possible without re-downloading anything.
package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// CurrentVersion is the manifest schema version written by this tool.
const CurrentVersion = 1

// Kind values for Manifest.Kind.
const (
	KindSnapshot = "snapshot"
	KindAMI      = "ami"
)

// Block describes a single allocated block inside the image.
type Block struct {
	// Index is the snapshot block index as returned by ListSnapshotBlocks.
	Index int64 `json:"index"`
	// Offset is Index * block-size, the byte offset inside the image.
	Offset int64 `json:"offset"`
	// Length is the data length in bytes; the final block of a volume may be
	// shorter than the block size.
	Length int64 `json:"length"`
	// SHA256 is the base64-encoded SHA-256 of the block data as returned by
	// GetSnapshotBlock.
	SHA256 string `json:"sha256"`
}

// Compression describes the compressed artifact derived from the raw image.
type Compression struct {
	Algorithm string `json:"algorithm"` // "zstd"
	Level     int    `json:"level"`
	Size      int64  `json:"size"`     // compressed size in bytes
	SHA256    string `json:"sha256"`   // hex SHA-256 of the compressed file
	Verified  bool   `json:"verified"` // restore-test (decode + hash match) passed
}

// AMIInfo records which AMI an image came from, when downloaded in AMI mode.
type AMIInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	RootDevice  string `json:"rootDevice,omitempty"`
	Device      string `json:"device"`
}

// Manifest is the complete record of one downloaded image (or, for KindAMI,
// the index of all volumes of an AMI download).
type Manifest struct {
	Version        int    `json:"version"`
	ToolVersion    string `json:"toolVersion"`
	Kind           string `json:"kind"`
	SnapshotID     string `json:"snapshotId"`
	VolumeSizeGiB  int64  `json:"volumeSizeGiB"`
	BlockSize      int64  `json:"blockSize"`
	LogicalSize    int64  `json:"logicalSize"`
	BlockCount     int    `json:"blockCount"`
	AllocatedBytes int64  `json:"allocatedBytes"`
	// ImageSHA256 is the hex SHA-256 over the entire logical image
	// (holes read as zeros), filled in by the verification pass.
	ImageSHA256 string       `json:"imageSha256,omitempty"`
	ImageFile   string       `json:"imageFile,omitempty"`
	Blocks      []Block      `json:"blocks"`
	Compression *Compression `json:"compression,omitempty"`
	AMI         *AMIInfo     `json:"ami,omitempty"`

	// Volumes is set only for KindAMI: the per-volume images of the AMI.
	Volumes []Volume `json:"volumes,omitempty"`
}

// Volume is one entry of an AMI manifest pointing at a per-volume image.
type Volume struct {
	Device     string `json:"device"`
	Root       bool   `json:"root"`
	SnapshotID string `json:"snapshotId"`
	ImageFile  string `json:"imageFile"`
	Manifest   string `json:"manifest"`
}

// GiB is the number of bytes in one gibibyte.
const GiB = int64(1) << 30

// Validate performs basic sanity checks on the manifest geometry.
// Snapshot manifests are validated strictly; AMI manifests are indexes
// over per-volume manifests and carry aggregate sizes only.
func (m *Manifest) Validate() error {
	if m.SnapshotID == "" {
		return fmt.Errorf("manifest: missing snapshot id")
	}
	if m.Kind == KindAMI {
		if len(m.Volumes) == 0 {
			return fmt.Errorf("manifest: AMI manifest lists no volumes")
		}
		return nil
	}
	if m.VolumeSizeGiB <= 0 {
		return fmt.Errorf("manifest: invalid volume size %d GiB", m.VolumeSizeGiB)
	}
	if m.BlockSize <= 0 {
		return fmt.Errorf("manifest: invalid block size %d", m.BlockSize)
	}
	if m.LogicalSize != m.VolumeSizeGiB*GiB {
		return fmt.Errorf("manifest: logical size %d does not match volume size %d GiB",
			m.LogicalSize, m.VolumeSizeGiB)
	}
	if m.BlockCount != len(m.Blocks) {
		return fmt.Errorf("manifest: block count %d does not match %d block entries",
			m.BlockCount, len(m.Blocks))
	}
	for _, b := range m.Blocks {
		if b.Offset != b.Index*m.BlockSize {
			return fmt.Errorf("manifest: block %d offset %d is not index*blockSize", b.Index, b.Offset)
		}
		if b.Length <= 0 || b.Length > m.BlockSize {
			return fmt.Errorf("manifest: block %d has invalid length %d (block size %d)",
				b.Index, b.Length, m.BlockSize)
		}
		if b.Offset+b.Length > m.LogicalSize {
			return fmt.Errorf("manifest: block %d extends past end of volume", b.Index)
		}
	}
	return nil
}

// IndexMap returns a map from block index to position in the Blocks slice.
func (m *Manifest) IndexMap() map[int64]int {
	idx := make(map[int64]int, len(m.Blocks))
	for i, b := range m.Blocks {
		idx[b.Index] = i
	}
	return idx
}

// Save writes the manifest atomically: temp file in the same directory,
// fsync, then rename over the destination.
func Save(path string, m *Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(path, data)
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return nil
}

// Load reads a manifest from disk and validates it.
func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}
