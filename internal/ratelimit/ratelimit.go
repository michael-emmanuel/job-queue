// Package ratelimit provides a token-bucket rate limiter.
//
// Rate limiting and backpressure are related but different. A rate limit
// enforces a policy about how fast a caller MAY go, regardless of system
// state: it protects fairness between tenants. Backpressure reports how fast
// the system CAN go right now (a full queue, a slow disk) and pushes that
// back to callers. A tenant can be under its rate limit and still be
// rejected by backpressure, and vice versa.
package ratelimit

import (
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
)

// Bucket is a token bucket: it holds up to burst tokens and refills at rate
// tokens per second. A nil *Bucket is an unlimited limiter, so callers can
// hold an optional limit without nil checks.
type Bucket struct {
	mu     sync.Mutex
	clk    clock.Clock
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket. A rate <= 0 means unlimited and returns nil.
func NewBucket(clk clock.Clock, rate float64, burst int) *Bucket {
	if rate <= 0 {
		return nil
	}
	if clk == nil {
		clk = clock.Real{}
	}
	if burst < 1 {
		burst = max(1, int(rate))
	}
	return &Bucket{clk: clk, rate: rate, burst: float64(burst), tokens: float64(burst), last: clk.Now()}
}

func (b *Bucket) refillLocked() {
	now := b.clk.Now()
	if d := now.Sub(b.last); d > 0 {
		b.tokens = min(b.burst, b.tokens+d.Seconds()*b.rate)
	}
	// A clock that moved backwards (skew, NTP step) must not mint tokens or
	// freeze the bucket: just re-anchor.
	b.last = now
}

// Take removes n tokens atomically. If not enough are available nothing is
// removed and the time until n tokens will exist is returned. A request for
// more than the burst size can never succeed; it reports a one-second wait.
func (b *Bucket) Take(n float64) (ok bool, retryAfter time.Duration) {
	if b == nil {
		return true, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	if n > b.burst {
		return false, time.Second
	}
	if b.tokens >= n {
		b.tokens -= n
		return true, 0
	}
	return false, time.Duration((n - b.tokens) / b.rate * float64(time.Second))
}

// TakeUpTo removes as many whole tokens as are available, up to n, and
// returns how many it took. It is how a consumer-side rate limit is applied
// to a batch pull.
func (b *Bucket) TakeUpTo(n int) int {
	if b == nil {
		return n
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	k := min(n, int(b.tokens))
	b.tokens -= float64(k)
	return k
}

// Refund returns n unused tokens, for a request that was admitted by this
// limiter and then rejected by another.
func (b *Bucket) Refund(n float64) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	b.tokens = min(b.burst, b.tokens+n)
	b.mu.Unlock()
}

// Keyed lazily creates one bucket per key (tenant, queue). The key space is
// expected to be small and authenticated; this type does not evict.
type Keyed struct {
	mu   sync.Mutex
	m    map[string]*Bucket
	make func() *Bucket
}

// NewKeyed returns a Keyed whose buckets are created by make. If make returns
// nil (unlimited), Get returns nil for every key.
func NewKeyed(make func() *Bucket) *Keyed {
	return &Keyed{m: map[string]*Bucket{}, make: make}
}

// Get returns the bucket for key.
func (k *Keyed) Get(key string) *Bucket {
	k.mu.Lock()
	defer k.mu.Unlock()
	b, ok := k.m[key]
	if !ok {
		b = k.make()
		k.m[key] = b
	}
	return b
}
