package cli

import (
	"testing"
)

// TestSelfTest runs the offline end-to-end pipeline (shared with the
// `snaport selftest` command): synthetic snapshot, interrupted download,
// resume, verification, compression, restore test, idempotent re-run.
func TestSelfTest(t *testing.T) {
	res, err := RunSelfTest(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if res.Blocks == 0 || res.Fetched == 0 {
		t.Fatalf("unexpected selftest result: %+v", res)
	}
}
