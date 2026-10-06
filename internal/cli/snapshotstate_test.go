package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// fakeStater scripts DescribeSnapshots responses in order.
type fakeStater struct {
	responses []*ec2.DescribeSnapshotsOutput
	err       error
	calls     int
}

func (f *fakeStater) DescribeSnapshots(ctx context.Context, in *ec2.DescribeSnapshotsInput, opts ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.calls >= len(f.responses) {
		f.calls++
		return f.responses[len(f.responses)-1], nil // keep returning the last
	}
	out := f.responses[f.calls]
	f.calls++
	return out, nil
}

func snapOut(state ec2types.SnapshotState, id string) *ec2.DescribeSnapshotsOutput {
	return &ec2.DescribeSnapshotsOutput{Snapshots: []ec2types.Snapshot{{
		SnapshotId: aws.String(id),
		State:      state,
	}}}
}

func TestEnsureSnapshotsReadyCompleted(t *testing.T) {
	f := &fakeStater{responses: []*ec2.DescribeSnapshotsOutput{snapOut(ec2types.SnapshotStateCompleted, "snap-1")}}
	if err := ensureSnapshotsReady(context.Background(), f, []string{"snap-1"}, 0); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatalf("calls = %d", f.calls)
	}
}

func TestEnsureSnapshotsReadyPendingFailsFast(t *testing.T) {
	f := &fakeStater{responses: []*ec2.DescribeSnapshotsOutput{snapOut(ec2types.SnapshotStatePending, "snap-1")}}
	err := ensureSnapshotsReady(context.Background(), f, []string{"snap-1"}, 0)
	if err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("expected pending error, got %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("should not poll without --wait, calls = %d", f.calls)
	}
}

func TestEnsureSnapshotsReadyWaitsForCompletion(t *testing.T) {
	f := &fakeStater{responses: []*ec2.DescribeSnapshotsOutput{
		snapOut(ec2types.SnapshotStatePending, "snap-1"),
		snapOut(ec2types.SnapshotStatePending, "snap-1"),
		snapOut(ec2types.SnapshotStateCompleted, "snap-1"),
	}}
	// Shrink the poll interval for the test.
	old := pollInterval
	pollInterval = time.Millisecond
	defer func() { pollInterval = old }()

	var buf bytes.Buffer
	oldStderr := stderr
	stderr = func() io.Writer { return &buf }
	defer func() { stderr = oldStderr }()

	if err := ensureSnapshotsReady(context.Background(), f, []string{"snap-1"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if f.calls != 3 {
		t.Fatalf("calls = %d, want 3", f.calls)
	}
	if !strings.Contains(buf.String(), "waiting for") || !strings.Contains(buf.String(), "completed") {
		t.Fatalf("unexpected progress output: %q", buf.String())
	}
}

func TestEnsureSnapshotsReadyTimeout(t *testing.T) {
	f := &fakeStater{responses: []*ec2.DescribeSnapshotsOutput{snapOut(ec2types.SnapshotStatePending, "snap-1")}}
	old := pollInterval
	pollInterval = time.Millisecond
	defer func() { pollInterval = old }()

	err := ensureSnapshotsReady(context.Background(), f, []string{"snap-1"}, 5*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestEnsureSnapshotsReadyErrorState(t *testing.T) {
	out := &ec2.DescribeSnapshotsOutput{Snapshots: []ec2types.Snapshot{{
		SnapshotId:   aws.String("snap-1"),
		State:        ec2types.SnapshotStateError,
		StateMessage: aws.String("something broke"),
	}}}
	f := &fakeStater{responses: []*ec2.DescribeSnapshotsOutput{out}}
	err := ensureSnapshotsReady(context.Background(), f, []string{"snap-1"}, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "something broke") {
		t.Fatalf("expected error-state failure, got %v", err)
	}
}

func TestEnsureSnapshotsReadyDescribeFailsIsWarning(t *testing.T) {
	f := &fakeStater{err: errors.New("AccessDenied")}
	// Must NOT fail the download: the state check is best-effort.
	if err := ensureSnapshotsReady(context.Background(), f, []string{"snap-1"}, 0); err != nil {
		t.Fatalf("describe failure should be a warning, got %v", err)
	}
}
