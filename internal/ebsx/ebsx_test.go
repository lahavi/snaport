package ebsx

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

func TestGenericRetryableBranches(t *testing.T) {
	if !IsRetryable(ErrChecksumMismatch) {
		t.Error("checksum mismatch should be retryable")
	}
	if !IsRetryable(errors.New("read tcp: unexpected EOF")) {
		t.Error("unexpected EOF should be retryable")
	}
	if IsRetryable(errors.New("plain failure")) {
		t.Error("plain error should not be retryable")
	}
}

// stubSource is a programmable Source for unit tests.
type stubSource struct {
	listFn    func(ctx context.Context, snapshotID, nextToken string, startingIndex int64, maxResults int32) (ListPage, error)
	getFn     func(ctx context.Context, snapshotID string, index int64, token string) (BlockData, error)
	changedFn func(ctx context.Context, firstID, secondID, nextToken string, startingIndex int64, maxResults int32) (ChangedPage, error)
}

func (s *stubSource) ListPage(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
	return s.listFn(ctx, id, nt, si, mr)
}
func (s *stubSource) GetBlock(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
	return s.getFn(ctx, id, idx, tok)
}
func (s *stubSource) ListChangedPage(ctx context.Context, firstID, secondID, nt string, si int64, mr int32) (ChangedPage, error) {
	if s.changedFn == nil {
		return ChangedPage{}, fmt.Errorf("stubSource: ListChangedPage not configured")
	}
	return s.changedFn(ctx, firstID, secondID, nt, si, mr)
}

func TestListAllPaginationAndGeometry(t *testing.T) {
	// 25 blocks, page size 10 -> 3 pages.
	var pages [][]BlockRef
	for p := 0; p < 3; p++ {
		var page []BlockRef
		for i := 0; i < 10 && p*10+i < 25; i++ {
			page = append(page, BlockRef{Index: int64(p*10 + i), Token: fmt.Sprintf("t-%d", p*10+i)})
		}
		pages = append(pages, page)
	}
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			page := 0
			if nt != "" {
				fmt.Sscanf(nt, "p%d", &page)
				page++
			}
			out := ListPage{VolumeGiB: 8, BlockSize: 524288, Blocks: pages[page], ExpiryTime: time.Now().Add(time.Hour)}
			if page+1 < len(pages) {
				out.NextToken = fmt.Sprintf("p%d", page)
			}
			return out, nil
		},
	}
	l := NewLister(src, "snap-test", 10)
	info, err := l.ListAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.VolumeGiB != 8 || info.BlockSize != 524288 {
		t.Fatalf("geometry: %+v", info)
	}
	if len(info.Blocks) != 25 {
		t.Fatalf("got %d blocks, want 25", len(info.Blocks))
	}
	if info.Blocks[24].Index != 24 {
		t.Fatalf("last block index %d", info.Blocks[24].Index)
	}
	// Tokens from listing must be in the store.
	if tok, fresh := l.Store().Get(7); !fresh || tok != "t-7" {
		t.Fatalf("token store missing block 7: %q %v", tok, fresh)
	}
}

func TestListAllRejectsNonMonotonicIndex(t *testing.T) {
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			return ListPage{
				VolumeGiB: 1, BlockSize: 524288,
				Blocks:     []BlockRef{{Index: 5, Token: "a"}, {Index: 3, Token: "b"}},
				ExpiryTime: time.Now().Add(time.Hour),
			}, nil
		},
	}
	_, err := NewLister(src, "snap-test", 10).ListAll(context.Background())
	if err == nil {
		t.Fatal("expected non-monotonic detection error")
	}
}

func TestRefreshTokenFindsRequestedBlock(t *testing.T) {
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			return ListPage{
				VolumeGiB: 1, BlockSize: 524288,
				Blocks:     []BlockRef{{Index: si, Token: fmt.Sprintf("fresh-%d", si)}, {Index: si + 1, Token: "next"}},
				ExpiryTime: time.Now().Add(time.Hour),
			}, nil
		},
	}
	l := NewLister(src, "snap-test", 10)
	ref, err := l.RefreshToken(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Index != 42 || ref.Token != "fresh-42" {
		t.Fatalf("refresh returned %+v", ref)
	}
	// The whole refreshed page is cached.
	if tok, fresh := l.Store().Get(43); !fresh || tok != "next" {
		t.Fatalf("page cache miss for 43: %q %v", tok, fresh)
	}
}

func TestFetcherRetriesOnChecksumMismatch(t *testing.T) {
	calls := 0
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			return ListPage{VolumeGiB: 1, BlockSize: 512, Blocks: []BlockRef{{Index: 1, Token: "tok"}}, ExpiryTime: time.Now().Add(time.Hour)}, nil
		},
		getFn: func(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
			calls++
			if calls == 1 {
				return BlockData{}, ErrChecksumMismatch
			}
			return BlockData{Data: []byte("hello"), SHA256: "whatever"}, nil
		},
	}
	f := NewFetcher(src, NewLister(src, "snap-test", 10), 5, nil)
	bd, err := f.FetchBlock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(bd.Data) != "hello" || calls != 2 {
		t.Fatalf("calls=%d bd=%+v", calls, bd)
	}
}

func TestFetcherRefreshesStaleTokens(t *testing.T) {
	// Tokens expire (generation bump) while the store still thinks they
	// are fresh: the fetcher must recover via refresh on retry.
	gen := 0
	lists := 0
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			lists++
			gen++ // every listing issues a new generation
			return ListPage{
				VolumeGiB: 1, BlockSize: 512,
				Blocks:     []BlockRef{{Index: 9, Token: fmt.Sprintf("g%d-9", gen)}},
				ExpiryTime: time.Now().Add(time.Hour),
			}, nil
		},
		getFn: func(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
			if tok == "g1-9" {
				// AWS returns typed AccessDeniedException for expired tokens.
				return BlockData{}, &fakeAPIError{code: "AccessDeniedException", msg: "block token expired"}
			}
			return BlockData{Data: []byte("ok"), SHA256: "x"}, nil
		},
	}
	l := NewLister(src, "snap-test", 10)
	l.ListAll(context.Background()) // fills store with g1 tokens
	f := NewFetcher(src, l, 5, nil)
	if _, err := f.FetchBlock(context.Background(), 9); err != nil {
		t.Fatalf("expected recovery via refresh, got: %v", err)
	}
	if lists < 2 {
		t.Fatalf("expected a refresh listing, got %d lists", lists)
	}
}

func TestFetcherProactivelyRefreshesExpiredTokens(t *testing.T) {
	lists := 0
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			lists++
			return ListPage{
				VolumeGiB: 1, BlockSize: 512,
				Blocks:     []BlockRef{{Index: 4, Token: "tok2"}},
				ExpiryTime: time.Now().Add(time.Hour),
			}, nil
		},
		getFn: func(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
			if tok != "tok2" {
				return BlockData{}, errors.New("stale token used")
			}
			return BlockData{Data: []byte("v"), SHA256: "h"}, nil
		},
	}
	l := NewLister(src, "snap-test", 10)
	if _, err := l.ListAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Force expiry in the store.
	l.Store().Inscribe([]BlockRef{{Index: 4, Token: "tok1"}}, time.Now().Add(-time.Minute))
	f := NewFetcher(src, l, 5, nil)
	if _, err := f.FetchBlock(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	if lists != 2 {
		t.Fatalf("expected exactly one refresh list, got %d", lists)
	}
}

func TestFetcherGivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			return ListPage{VolumeGiB: 1, BlockSize: 512, Blocks: []BlockRef{{Index: 2, Token: "t"}}, ExpiryTime: time.Now().Add(time.Hour)}, nil
		},
		getFn: func(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
			calls++
			return BlockData{}, ErrChecksumMismatch
		},
	}
	f := NewFetcher(src, NewLister(src, "s", 10), 3, nil)
	_, err := f.FetchBlock(context.Background(), 2)
	if err == nil {
		t.Fatal("expected exhaustion error")
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestThrottleCallbackFires(t *testing.T) {
	throttled := 0
	calls := 0
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			return ListPage{VolumeGiB: 1, BlockSize: 512, Blocks: []BlockRef{{Index: 1, Token: "t"}}, ExpiryTime: time.Now().Add(time.Hour)}, nil
		},
		getFn: func(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
			calls++
			return BlockData{}, &fakeAPIError{code: "ThrottlingException", msg: "slow down"}
		},
	}
	f := NewFetcher(src, NewLister(src, "s", 10), 2, func() { throttled++ })
	_, err := f.FetchBlock(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
	if throttled != 2 {
		t.Fatalf("throttle callback fired %d times, want 2", throttled)
	}
}

// fakeAPIError is a minimal smithy.APIError implementation.
type fakeAPIError struct{ code, msg string }

func (e *fakeAPIError) Error() string                 { return e.code + ": " + e.msg }
func (e *fakeAPIError) ErrorCode() string             { return e.code }
func (e *fakeAPIError) ErrorMessage() string          { return e.msg }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

func TestSmithyErrorClassification(t *testing.T) {
	throttle := &fakeAPIError{code: "ThrottlingException", msg: "x"}
	if !IsRetryable(throttle) || !IsThrottle(throttle) {
		t.Fatal("throttling error should be retryable+throttle")
	}
	denied := &fakeAPIError{code: "AccessDeniedException", msg: "no kms"}
	if IsRetryable(denied) || !IsAccessDeniedErr(denied) {
		t.Fatal("access denied should be non-retryable and detected")
	}
	internal := &fakeAPIError{code: "InternalError", msg: "x"}
	if !IsRetryable(internal) {
		t.Fatal("internal error should be retryable")
	}
}

func TestListChangedAllPaginationAndTokens(t *testing.T) {
	// 7 changed blocks (one deallocated) served 2 per page.
	all := []ChangedBlockRef{
		{Index: 5, SecondToken: "t5"},
		{Index: 9, SecondToken: ""}, // deallocated
		{Index: 14, SecondToken: "t14"},
		{Index: 20, SecondToken: "t20"},
		{Index: 33, SecondToken: "t33"},
		{Index: 41, SecondToken: "t41"},
		{Index: 50, SecondToken: "t50"},
	}
	calls := 0
	src := &stubSource{
		listFn: func(ctx context.Context, id, nt string, si int64, mr int32) (ListPage, error) {
			return ListPage{}, fmt.Errorf("unexpected ListPage call")
		},
		getFn: func(ctx context.Context, id string, idx int64, tok string) (BlockData, error) {
			return BlockData{}, fmt.Errorf("unexpected GetBlock call")
		},
		changedFn: func(ctx context.Context, firstID, secondID, nt string, si int64, mr int32) (ChangedPage, error) {
			calls++
			start := 0
			if nt != "" {
				var last int
				fmt.Sscanf(nt, "p%d", &last)
				start = last + 1
			}
			end := min(start+2, len(all))
			page := ChangedPage{VolumeGiB: 4, BlockSize: 512 * 1024, ExpiryTime: time.Now().Add(time.Hour)}
			page.Changed = append(page.Changed, all[start:end]...)
			if end < len(all) {
				page.NextToken = fmt.Sprintf("p%d", end-1)
			}
			return page, nil
		},
	}
	l := NewLister(src, "snap-next", 2)
	changed, volGiB, blockSize, err := l.ListChangedAll(context.Background(), "snap-base")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 4 {
		t.Fatalf("pages = %d, want 4", calls)
	}
	if volGiB != 4 || blockSize != 512*1024 {
		t.Fatalf("geometry: %d GiB, block %d", volGiB, blockSize)
	}
	if len(changed) != len(all) {
		t.Fatalf("changed = %d entries, want %d", len(changed), len(all))
	}
	for i := range all {
		if changed[i] != all[i] {
			t.Fatalf("changed[%d] = %+v, want %+v", i, changed[i], all[i])
		}
	}
	// Data-bearing tokens must be usable through the shared store.
	if tok, fresh := l.Store().Get(41); !fresh || tok != "t41" {
		t.Fatalf("store token for 41: %q fresh=%v", tok, fresh)
	}
	if _, fresh := l.Store().Get(9); fresh {
		t.Fatal("deallocated block 9 must not carry a token")
	}
}
