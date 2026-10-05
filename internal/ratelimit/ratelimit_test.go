package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func TestBurstThenRefill(t *testing.T) {
	clk := clock.NewFake(t0)
	b := NewBucket(clk, 10, 5)
	for i := 0; i < 5; i++ {
		if ok, _ := b.Take(1); !ok {
			t.Fatalf("burst token %d denied", i)
		}
	}
	ok, wait := b.Take(1)
	if ok || wait != 100*time.Millisecond {
		t.Fatalf("ok=%v wait=%v, want denied with 100ms", ok, wait)
	}
	clk.Advance(100 * time.Millisecond)
	if ok, _ := b.Take(1); !ok {
		t.Fatal("token not refilled")
	}
	clk.Advance(time.Hour) // refill is capped at burst
	if got := b.TakeUpTo(100); got != 5 {
		t.Fatalf("TakeUpTo after long idle = %d, want burst 5", got)
	}
}

func TestTakeIsAllOrNothing(t *testing.T) {
	b := NewBucket(clock.NewFake(t0), 1, 3)
	if ok, _ := b.Take(5); ok {
		t.Fatal("take beyond burst succeeded")
	}
	if ok, _ := b.Take(2); !ok {
		t.Fatal("take 2 denied")
	}
	if ok, _ := b.Take(2); ok {
		t.Fatal("partial take must not succeed")
	}
	if ok, _ := b.Take(1); !ok {
		t.Fatal("the failed take must not have consumed tokens")
	}
}

func TestRefund(t *testing.T) {
	b := NewBucket(clock.NewFake(t0), 1, 2)
	b.Take(2)
	b.Refund(1)
	if ok, _ := b.Take(1); !ok {
		t.Fatal("refunded token unavailable")
	}
	b.Refund(100)
	if got := b.TakeUpTo(10); got != 2 {
		t.Fatalf("refund exceeded burst: %d", got)
	}
}

func TestNilBucketIsUnlimited(t *testing.T) {
	var b *Bucket
	if ok, _ := b.Take(1e9); !ok {
		t.Fatal("nil bucket limited")
	}
	if b.TakeUpTo(7) != 7 {
		t.Fatal("nil bucket TakeUpTo")
	}
	b.Refund(1)
	if NewBucket(nil, 0, 0) != nil {
		t.Fatal("rate 0 must mean unlimited")
	}
}

func TestClockMovingBackwardsDoesNotMintTokens(t *testing.T) {
	clk := clock.NewFake(t0)
	b := NewBucket(clk, 1, 2)
	b.TakeUpTo(2)
	clk.Set(t0.Add(-time.Hour))
	if ok, _ := b.Take(1); ok {
		t.Fatal("backwards clock minted a token")
	}
	clk.Advance(time.Second) // forward again from the new anchor
	if ok, _ := b.Take(1); !ok {
		t.Fatal("bucket frozen after clock step")
	}
}

func TestConcurrentTakesNeverExceedBurst(t *testing.T) {
	b := NewBucket(clock.NewFake(t0), 1, 1000) // fake clock: no refill during the test
	var got atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if ok, _ := b.Take(1); ok {
					got.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got.Load() != 1000 {
		t.Fatalf("granted %d tokens, want exactly 1000", got.Load())
	}
}

func TestKeyedIsolatesTenants(t *testing.T) {
	clk := clock.NewFake(t0)
	k := NewKeyed(func() *Bucket { return NewBucket(clk, 1, 1) })
	if ok, _ := k.Get("a").Take(1); !ok {
		t.Fatal()
	}
	if ok, _ := k.Get("a").Take(1); ok {
		t.Fatal("tenant a exceeded its limit")
	}
	if ok, _ := k.Get("b").Take(1); !ok {
		t.Fatal("tenant b was affected by tenant a (noisy neighbour)")
	}
	if k.Get("a") != k.Get("a") {
		t.Fatal("Get must return a stable bucket")
	}
}
