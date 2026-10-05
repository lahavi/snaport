// Package ebsx wraps the AWS EBS Direct APIs (ListSnapshotBlocks /
// GetSnapshotBlock) behind a small, mockable interface and implements the
// operational hardening around them: pagination, retries, throttling
// backoff, block-token expiry refresh, and checksum verification.
package ebsx

import (
	"context"
	"errors"
	"time"
)

// BlockRef is one allocated block as reported by ListSnapshotBlocks.
type BlockRef struct {
	Index int64
	Token string
}

// ListPage is one page of ListSnapshotBlocks results.
type ListPage struct {
	Blocks    []BlockRef
	VolumeGiB int64
	BlockSize int64
	// ExpiryTime is when the tokens in this page expire (per the API
	// response). Zero means unknown; callers should then assume a
	// conservative TTL.
	ExpiryTime time.Time
	// NextToken continues the listing; empty when this was the last page.
	NextToken string
}

// BlockData is one downloaded block with its verified checksum.
type BlockData struct {
	Data   []byte
	SHA256 string // base64, as returned by the API
}

// ErrChecksumMismatch is returned when the SHA-256 returned by the service
// does not match the downloaded bytes. Retrying the fetch may succeed.
var ErrChecksumMismatch = errors.New("block data failed checksum verification")

// ErrAccessDenied is returned when AWS rejects the request for missing
// permissions (IAM or KMS). Wrapped errors carry the original message.
type AccessDeniedError struct {
	Op      string // "ListSnapshotBlocks" or "GetSnapshotBlock"
	Message string
}

func (e *AccessDeniedError) Error() string {
	return e.Op + ": access denied: " + e.Message
}

// Source is the minimal EBS Direct API surface the downloader needs.
// Implementations must be safe for concurrent use.
type Source interface {
	// ListPage lists one page of allocated blocks.
	//   - nextToken != "" continues an existing listing (startingIndex ignored).
	//   - nextToken == "" starts (or restarts, via startingIndex) a listing.
	ListPage(ctx context.Context, snapshotID string, nextToken string, startingIndex int64, maxResults int32) (ListPage, error)

	// GetBlock downloads one block by index using a block token from a
	// listing. It verifies the block checksum before returning.
	GetBlock(ctx context.Context, snapshotID string, index int64, token string) (BlockData, error)
}
