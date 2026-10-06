package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// snapshotStater is the subset of ec2.Client used for state checks.
type snapshotStater interface {
	DescribeSnapshots(ctx context.Context, in *ec2.DescribeSnapshotsInput, opts ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
}

// pollInterval is how often --wait re-checks pending snapshots (var for
// tests).
var pollInterval = 10 * time.Second

// ensureSnapshotsReady verifies every snapshot is in the completed state
// before a download (the EBS Direct read APIs reject pending snapshots).
// With wait > 0 it polls until they complete or the budget runs out.
// A DescribeSnapshots failure is downgraded to a warning - callers whose
// IAM policy only grants the ebs:* read actions still work.
func ensureSnapshotsReady(ctx context.Context, client snapshotStater, ids []string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	waited := false
	for {
		out, err := client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: ids})
		if err != nil {
			fmt.Fprintf(stderr(), "warning: cannot check snapshot state (ec2:DescribeSnapshots may be missing): %v\n", err)
			return nil
		}
		pending, failed := classifyStates(out.Snapshots)
		if len(failed) > 0 {
			return fmt.Errorf("snapshot(s) in unrecoverable state: %s", joinStates(failed))
		}
		if len(pending) == 0 {
			if waited {
				fmt.Fprintln(stderr(), "snapshot(s) completed")
			}
			return nil
		}
		if wait <= 0 {
			return fmt.Errorf("snapshot(s) %s are still being created (pending); the EBS Direct APIs only serve completed snapshots - retry when done or pass --wait 15m",
				joinStates(pending))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for snapshot(s) %s to complete; retry the command later or with a larger --wait",
				wait, joinStates(pending))
		}
		waited = true
		fmt.Fprintf(stderr(), "waiting for snapshot(s) %s to complete (%s left)...\n",
			joinStates(pending), time.Until(deadline).Round(time.Second))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// classifyStates splits snapshots into pending and unrecoverable ones.
func classifyStates(snaps []ec2types.Snapshot) (pending, failed []string) {
	for _, s := range snaps {
		id := ""
		if s.SnapshotId != nil {
			id = *s.SnapshotId
		}
		switch s.State {
		case ec2types.SnapshotStatePending:
			pending = append(pending, id)
		case ec2types.SnapshotStateError, ec2types.SnapshotStateRecoverable:
			state := string(s.State)
			if s.StateMessage != nil {
				state += " (" + *s.StateMessage + ")"
			}
			failed = append(failed, id+": "+state)
		}
	}
	return pending, failed
}

func joinStates(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ", "
		}
		out += id
	}
	return out
}

// stderr is split out so tests can capture progress output.
var stderr = func() io.Writer { return os.Stderr }
