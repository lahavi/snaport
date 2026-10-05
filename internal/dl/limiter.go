package dl

import (
	"context"
	"sync"
	"time"
)

// Limiter is an adaptive concurrency limiter implementing additive
// increase / multiplicative decrease against AWS throttling:
//
//   - Acquire blocks while in-flight requests are at the current usable limit;
//   - OnThrottle halves the usable limit (down to 1);
//   - each Release, after a calm period, grows the limit by one until max.
//
// The SDK retryer handles individual requests; this exists to shape how
// many blocks we fetch in parallel, which is what actually drives the
// request rate.
type Limiter struct {
	mu           sync.Mutex
	max          int
	usable       int
	inflight     int
	waiters      chan struct{} // closed-and-replaced to broadcast capacity changes
	lastThrottle time.Time
	rampEvery    time.Duration
}

// NewLimiter allows up to max concurrent operations.
func NewLimiter(max int) *Limiter {
	if max < 1 {
		max = 1
	}
	return &Limiter{
		max:       max,
		usable:    max,
		waiters:   make(chan struct{}),
		rampEvery: 15 * time.Second,
	}
}

// Acquire takes a concurrency slot, respecting ctx cancellation.
func (l *Limiter) Acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.inflight < l.usable {
			l.inflight++
			l.mu.Unlock()
			return nil
		}
		ch := l.waiters
		l.mu.Unlock()

		select {
		case <-ch: // capacity changed; retry
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release returns a slot. After a calm period each release also grows the
// usable limit by one (additive increase), up to max.
func (l *Limiter) Release() {
	l.mu.Lock()
	l.inflight--
	if l.usable < l.max && time.Since(l.lastThrottle) > l.rampEvery {
		l.usable++
	}
	l.wake()
	l.mu.Unlock()
}

// OnThrottle halves the usable limit. In-flight requests keep running;
// new ones wait until capacity frees up.
func (l *Limiter) OnThrottle() {
	l.mu.Lock()
	l.lastThrottle = time.Now()
	if l.usable > 1 {
		l.usable = l.usable / 2
	}
	l.mu.Unlock()
}

// Current returns the current usable limit (for progress display).
func (l *Limiter) Current() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.usable
}

// wake broadcasts a capacity change to all waiters. Callers must hold mu.
func (l *Limiter) wake() {
	close(l.waiters)
	l.waiters = make(chan struct{})
}
