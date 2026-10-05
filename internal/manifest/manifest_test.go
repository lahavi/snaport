package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestRoundtripAndValidate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.json")
	m := &Manifest{
		Version:        CurrentVersion,
		ToolVersion:    "test",
		Kind:           KindSnapshot,
		SnapshotID:     "snap-abc",
		VolumeSizeGiB:  2,
		BlockSize:      524288,
		LogicalSize:    2 * GiB,
		BlockCount:     2,
		AllocatedBytes: 524288 + 1000,
		Blocks: []Block{
			{Index: 0, Offset: 0, Length: 524288, SHA256: "aa=="},
			{Index: 4095, Offset: 4095 * 524288, Length: 1000, SHA256: "bb=="},
		},
	}
	if err := Save(path, m); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.BlockCount != 2 || got.Blocks[1].Index != 4095 || got.Blocks[1].Length != 1000 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	// Atomic save must leave no temp files behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestManifestValidationFailures(t *testing.T) {
	base := func() *Manifest {
		return &Manifest{
			Kind:          KindSnapshot,
			SnapshotID:    "snap-x",
			VolumeSizeGiB: 1,
			BlockSize:     524288,
			LogicalSize:   GiB,
			BlockCount:    1,
			Blocks:        []Block{{Index: 0, Offset: 0, Length: 524288, SHA256: "z"}},
		}
	}
	cases := []func(m *Manifest){
		func(m *Manifest) { m.LogicalSize = 12345 },       // geometry
		func(m *Manifest) { m.Blocks[0].Offset = 999 },    // offset
		func(m *Manifest) { m.Blocks[0].Length = 0 },      // length
		func(m *Manifest) { m.Blocks[0].Length = 524289 }, // over block size
		func(m *Manifest) { m.BlockCount = 7 },            // count mismatch
		func(m *Manifest) { m.SnapshotID = "" },           // id
		func(m *Manifest) {
			m.Blocks[0].Index = 2048
			m.Blocks[0].Offset = 2048 * 524288
			m.Blocks[0].Length = 524289
			m.Blocks[0].Offset = 2048 * 524288
		}, // past end via length
	}
	for i, mutate := range cases {
		m := base()
		mutate(m)
		if err := m.Validate(); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}

func TestAMIManifestValidation(t *testing.T) {
	m := &Manifest{Kind: KindAMI, SnapshotID: "ami-1", Volumes: []Volume{{Device: "sda1", SnapshotID: "snap-1"}}}
	if err := m.Validate(); err != nil {
		t.Fatalf("AMI manifest rejected: %v", err)
	}
	m.Volumes = nil
	if err := m.Validate(); err == nil {
		t.Fatal("AMI manifest without volumes should fail")
	}
}
