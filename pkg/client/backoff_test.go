package client

import (
	"testing"
	"time"
)

// Full jitter: every delay lies in [0, min(max, base*2^(n-1))], and the
// delays of a crowd of clients are spread out rather than identical.
func TestBackoffIsBoundedAndSpread(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}
	for attempt, cap := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 6: 2 * time.Second, 50: 2 * time.Second} {
		distinct := map[time.Duration]bool{}
		for i := 0; i < 500; i++ {
			d := backoff(p, attempt)
			if d < 0 || d > cap {
				t.Fatalf("attempt %d: %v outside [0,%v]", attempt, d, cap)
			}
			distinct[d] = true
		}
		if len(distinct) < 400 {
			t.Errorf("attempt %d: only %d distinct delays in 500 draws; clients would retry in lockstep", attempt, len(distinct))
		}
	}
}
