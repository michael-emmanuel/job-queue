// Package clock abstracts time so that time-dependent logic (visibility
// timeouts, retry delays, scheduling, idempotency TTLs) can be tested
// deterministically without sleeping.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal time source used by the queue engine.
type Clock interface {
	Now() time.Time
}

// Real is the wall clock.
type Real struct{}

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually advanced clock for tests and fault injection.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake starting at start.
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

// Set jumps the clock to t. Moving backwards is allowed on purpose: it is
// how clock-skew faults are injected.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
}
