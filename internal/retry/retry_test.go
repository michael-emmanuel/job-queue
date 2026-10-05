package retry

import (
	"testing"
	"time"
)

var pol = Policy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 30 * time.Second}

func TestBackoffDoublesThenCaps(t *testing.T) {
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if got := pol.Backoff(i + 1); got != w*time.Second {
			t.Errorf("attempt %d: %v, want %v", i+1, got, w*time.Second)
		}
	}
}

func TestBackoffNeverOverflows(t *testing.T) {
	if got := pol.Backoff(10_000); got != pol.MaxDelay {
		t.Fatalf("got %v", got)
	}
	huge := Policy{MaxAttempts: 100, BaseDelay: time.Hour, MaxDelay: 1<<62 - 1}
	if got := huge.Backoff(100); got <= 0 {
		t.Fatalf("overflowed to %v", got)
	}
}

// Property: for every attempt and every random draw, delay stays inside the
// window the jitter mode promises.
func TestJitterBounds(t *testing.T) {
	draws := []float64{0, 0.001, 0.25, 0.5, 0.75, 0.999999}
	for attempt := 1; attempt <= 12; attempt++ {
		b := pol.Backoff(attempt)
		for _, r := range draws {
			rnd := func() float64 { return r }
			full := Policy{BaseDelay: pol.BaseDelay, MaxDelay: pol.MaxDelay, Jitter: JitterFull}
			if d := full.Delay(attempt, rnd); d < 0 || d > b {
				t.Fatalf("full jitter: attempt %d draw %v -> %v outside [0,%v]", attempt, r, d, b)
			}
			eq := Policy{BaseDelay: pol.BaseDelay, MaxDelay: pol.MaxDelay, Jitter: JitterEqual}
			if d := eq.Delay(attempt, rnd); d < b/2 || d > b {
				t.Fatalf("equal jitter: attempt %d draw %v -> %v outside [%v,%v]", attempt, r, d, b/2, b)
			}
			none := Policy{BaseDelay: pol.BaseDelay, MaxDelay: pol.MaxDelay}
			if d := none.Delay(attempt, rnd); d != b {
				t.Fatalf("no jitter changed the delay: %v != %v", d, b)
			}
		}
	}
}

func TestValidate(t *testing.T) {
	bad := []Policy{{}, {MaxAttempts: 1}, {MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Millisecond}}
	for i, p := range bad {
		if p.Validate() == nil {
			t.Errorf("case %d accepted", i)
		}
	}
	if err := DefaultPolicy().Validate(); err != nil {
		t.Error(err)
	}
}
