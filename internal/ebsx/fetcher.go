package ebsx

import (
	"context"
	"fmt"
)

// Fetcher downloads individual blocks with the full hardening stack:
// token freshness checks, token refresh on expiry/stale-token errors,
// exponential backoff on transient failures, and a throttle callback so
// the caller can shed concurrency (AIMD) when AWS throttles.
type Fetcher struct {
	src         Source
	lister      *Lister
	snapshotID  string
	maxAttempts int
	onThrottle  func()
}

// NewFetcher creates a block fetcher for one snapshot.
func NewFetcher(src Source, lister *Lister, maxAttempts int, onThrottle func()) *Fetcher {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	return &Fetcher{src: src, lister: lister, snapshotID: lister.snapshotID, maxAttempts: maxAttempts, onThrottle: onThrottle}
}

// FetchBlock downloads one block by index, handling token refresh and
// retries. It returns a verified BlockData or the last error.
func (f *Fetcher) FetchBlock(ctx context.Context, index int64) (BlockData, error) {
	var lastErr error
	for attempt := 0; attempt < f.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return BlockData{}, err
		}
		token, fresh := f.lister.Store().Get(index)
		if !fresh {
			ref, err := f.lister.RefreshToken(ctx, index)
			if err != nil {
				return BlockData{}, err
			}
			token = ref.Token
		}

		bd, err := f.src.GetBlock(ctx, f.snapshotID, index, token)
		if err == nil {
			return bd, nil
		}
		lastErr = err

		if IsThrottle(err) && f.onThrottle != nil {
			f.onThrottle()
		}

		if !IsRetryable(err) {
			// An access-denied on a valid-looking request is sometimes a
			// stale token rather than an IAM problem: refresh once before
			// giving up so the real IAM/KMS error surfaces cleanly.
			if IsAccessDeniedErr(err) && attempt == 0 {
				f.lister.Store().Drop(index)
				continue
			}
			return BlockData{}, err
		}

		// After a failed attempt, suspect the token for the next one.
		if attempt >= 1 {
			f.lister.Store().Drop(index)
		}
		if err := Backoff(ctx, attempt, IsThrottle(err)); err != nil {
			return BlockData{}, err
		}
	}
	return BlockData{}, fmt.Errorf("block %d: giving up after %d attempts: %w", index, f.maxAttempts, lastErr)
}
