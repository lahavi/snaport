package ebsx

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/aws/smithy-go"
)

// Throttling and transient server error codes we retry with backoff.
var throttleCodes = map[string]bool{
	"Throttling":                true,
	"ThrottlingException":       true,
	"RequestThrottled":          true,
	"RequestThrottledException": true,
	"RequestLimitExceeded":      true,
	"TooManyRequestsException":  true,
}

var serverErrorCodes = map[string]bool{
	"ServiceUnavailable":          true,
	"InternalError":               true,
	"InternalFailure":             true,
	"InternalServerError":         true,
	"InternalServerException":     true,
	"ServiceUnavailableException": true,
	"SlowDown":                    true,
}

// apiErrorCode extracts the smithy API error code if err is one.
func apiErrorCode(err error) (string, bool) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode(), true
	}
	return "", false
}

// IsThrottle reports whether err is an AWS throttling response.
func IsThrottle(err error) bool {
	code, ok := apiErrorCode(err)
	return ok && throttleCodes[code]
}

// IsRetryable reports whether a failed request is worth retrying:
// throttling, 5xx server errors, and transient network failures.
// Permanent failures (auth, validation, not-found) are not retryable here.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrChecksumMismatch) {
		return true // corrupted transfer; a refetch is reasonable
	}
	if code, ok := apiErrorCode(err); ok {
		return throttleCodes[code] || serverErrorCodes[code]
	}
	// The SDK occasionally wraps errors in *url.Error during body reads.
	var uerr *url.Error
	if errors.As(err, &uerr) {
		err = uerr.Err
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true // timeouts, resets, aborted connections
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	for _, transient := range []error{syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ECONNREFUSED, syscall.ETIMEDOUT, syscall.EPIPE} {
		if errors.Is(err, transient) {
			return true
		}
	}
	// Truncated response bodies surface like this from io.ReadFull.
	if strings.Contains(err.Error(), "unexpected EOF") {
		return true
	}
	return false
}

// IsAccessDeniedErr reports whether AWS rejected the call with an
// access-denied class error (missing IAM action or KMS key use).
func IsAccessDeniedErr(err error) bool {
	code, ok := apiErrorCode(err)
	if !ok {
		return false
	}
	switch code {
	case "AccessDeniedException", "AccessDenied", "UnauthorizedOperation", "UnrecognizedClientException":
		return true
	}
	return false
}

// classifyListError wraps ListSnapshotBlocks failures for actionable
// messages at the CLI layer.
func classifyListError(err error) error {
	if IsAccessDeniedErr(err) {
		return &AccessDeniedError{Op: "ListSnapshotBlocks", Message: errorMessage(err)}
	}
	return err
}

// classifyGetError wraps GetSnapshotBlock failures likewise.
func classifyGetError(err error) error {
	if IsAccessDeniedErr(err) {
		return &AccessDeniedError{Op: "GetSnapshotBlock", Message: errorMessage(err)}
	}
	return err
}

func errorMessage(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorMessage()
	}
	return err.Error()
}

// Backoff sleeps for a randomized exponential backoff after the given
// attempt (0-based), or until ctx is done. Throttling errors back off
// harder than other transient errors.
func Backoff(ctx context.Context, attempt int, throttled bool) error {
	base := 500 * time.Millisecond
	if throttled {
		base = time.Second
	}
	// attempt 0 -> ~0.5s, 1 -> ~1s, 2 -> ~2s, ... capped at 30s
	max := 30 * time.Second
	d := base << attempt
	if d > max || d <= 0 {
		d = max
	}
	jitter := time.Duration(rand.Int64N(int64(d / 2)))
	d = d/2 + jitter
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
