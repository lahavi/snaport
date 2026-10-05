package ebsx

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	ebstypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/aws/smithy-go"
)

// DefaultListPageSize is the MaxResults requested per ListSnapshotBlocks
// call. The service caps this; if it rejects the value we transparently
// fall back to its default page size.
const DefaultListPageSize = 10000

// getBlockTimeout bounds each GetSnapshotBlock call including body read.
const getBlockTimeout = 5 * time.Minute

// AWSSource implements Source against the real EBS Direct API using the
// official AWS SDK. The SDK's built-in retryer handles per-request
// retries; ebsx layers token-refresh and stronger backoff on top.
type AWSSource struct {
	client *ebs.Client
	// listPageSizeFallback is used when the service rejects the requested
	// MaxResults (set on first successful fallback, reused afterwards).
	fallbackPageSize bool
}

// NewAWSSource builds a Source from a configured EBS client.
func NewAWSSource(client *ebs.Client) *AWSSource {
	return &AWSSource{client: client}
}

// NewEBSClient returns an EBS client with adaptive retries.
func NewEBSClient(cfg aws.Config) *ebs.Client {
	return ebs.NewFromConfig(cfg, func(o *ebs.Options) {
		o.RetryMaxAttempts = 4
		o.RetryMode = aws.RetryModeAdaptive
	})
}

func (s *AWSSource) ListPage(ctx context.Context, snapshotID, nextToken string, startingIndex int64, maxResults int32) (ListPage, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	input := &ebs.ListSnapshotBlocksInput{SnapshotId: aws.String(snapshotID)}
	if nextToken != "" {
		input.NextToken = aws.String(nextToken)
	} else {
		if startingIndex > 0 {
			input.StartingBlockIndex = aws.Int32(int32(startingIndex))
		}
		if maxResults > 0 && !s.fallbackPageSize {
			input.MaxResults = aws.Int32(maxResults)
		}
	}

	out, err := s.client.ListSnapshotBlocks(ctx, input)
	if err != nil {
		// If the service rejects our MaxResults, retry once with its default.
		if input.MaxResults != nil && isMaxResultsRejected(err) {
			s.fallbackPageSize = true
			input.MaxResults = nil
			out, err = s.client.ListSnapshotBlocks(ctx, input)
		}
		if err != nil {
			return ListPage{}, classifyListError(err)
		}
	}

	page := ListPage{}
	if out.VolumeSize != nil {
		page.VolumeGiB = *out.VolumeSize
	}
	if out.BlockSize != nil {
		page.BlockSize = int64(*out.BlockSize)
	}
	if out.ExpiryTime != nil {
		page.ExpiryTime = *out.ExpiryTime
	}
	if out.NextToken != nil {
		page.NextToken = *out.NextToken
	}
	page.Blocks = make([]BlockRef, 0, len(out.Blocks))
	for _, b := range out.Blocks {
		if b.BlockIndex == nil || b.BlockToken == nil {
			continue
		}
		page.Blocks = append(page.Blocks, BlockRef{
			Index: int64(*b.BlockIndex),
			Token: *b.BlockToken,
		})
	}
	return page, nil
}

func (s *AWSSource) GetBlock(ctx context.Context, snapshotID string, index int64, token string) (BlockData, error) {
	ctx, cancel := context.WithTimeout(ctx, getBlockTimeout)
	defer cancel()

	out, err := s.client.GetSnapshotBlock(ctx, &ebs.GetSnapshotBlockInput{
		SnapshotId: aws.String(snapshotID),
		BlockIndex: aws.Int32(int32(index)),
		BlockToken: aws.String(token),
	})
	if err != nil {
		return BlockData{}, classifyGetError(err)
	}
	defer out.BlockData.Close()

	if out.DataLength == nil {
		return BlockData{}, fmt.Errorf("GetSnapshotBlock(%d): response missing data length", index)
	}
	length := int64(*out.DataLength)
	if length <= 0 {
		return BlockData{}, fmt.Errorf("GetSnapshotBlock(%d): invalid data length %d", index, length)
	}
	if out.Checksum == nil {
		return BlockData{}, fmt.Errorf("GetSnapshotBlock(%d): response missing checksum", index)
	}
	if out.ChecksumAlgorithm != ebstypes.ChecksumAlgorithmChecksumAlgorithmSha256 {
		return BlockData{}, fmt.Errorf("GetSnapshotBlock(%d): unsupported checksum algorithm %q",
			index, out.ChecksumAlgorithm)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(out.BlockData, buf); err != nil {
		return BlockData{}, fmt.Errorf("GetSnapshotBlock(%d): reading %d bytes: %w", index, length, err)
	}
	sum := sha256.Sum256(buf)
	got := base64.StdEncoding.EncodeToString(sum[:])
	if got != *out.Checksum {
		return BlockData{}, fmt.Errorf("block %d: %w (service %s, computed %s)",
			index, ErrChecksumMismatch, *out.Checksum, got)
	}
	return BlockData{Data: buf, SHA256: got}, nil
}

// isMaxResultsRejected detects a validation error naming MaxResults.
func isMaxResultsRejected(err error) bool {
	code, ok := apiErrorCode(err)
	if !ok {
		return false
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	return code == "ValidationException" && strings.Contains(strings.ToLower(ae.ErrorMessage()), "maxresults")
}
