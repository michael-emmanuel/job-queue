// Package retry implements exponential backoff with jitter.
package retry

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Jitter selects how randomness is applied to the exponential delay.
type Jitter int

const (
	// JitterNone returns the raw exponential delay. Tests only: deterministic
	// delays make every failed worker retry in lockstep (a retry storm).
	JitterNone Jitter = iota
	// JitterEqual returns d/2 + rand[0, d/2]. It keeps a floor of half the
	// backoff so a recovering dependency is never hit immediately, while still
	// spreading retries across a window.
	JitterEqual
	// JitterFull returns rand[0, d]. Best spread, but individual retries can
	// fire almost immediately.
	JitterFull
)

// Policy describes how often and how quickly a failed job is retried.
type Policy struct {
	// MaxAttempts is the total number of deliveries allowed, including the first.
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      Jitter
}

// DefaultPolicy is a conservative general-purpose policy.
func DefaultPolicy() Policy {
	return Policy{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: 5 * time.Minute, Jitter: JitterEqual}
}

// Validate reports configuration errors.
func (p Policy) Validate() error {
	switch {
	case p.MaxAttempts < 1:
		return errors.New("retry: MaxAttempts must be >= 1")
	case p.BaseDelay <= 0:
		return errors.New("retry: BaseDelay must be > 0")
	case p.MaxDelay < p.BaseDelay:
		return errors.New("retry: MaxDelay must be >= BaseDelay")
	}
	return nil
}

// Backoff returns the pre-jitter delay after the given failed attempt
// (1-based): min(MaxDelay, BaseDelay * 2^(attempt-1)). The first failure
// waits BaseDelay.
func (p Policy) Backoff(attempt int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= p.MaxDelay || d <= 0 { // d <= 0 guards overflow
			return p.MaxDelay
		}
	}
	if d > p.MaxDelay {
		return p.MaxDelay
	}
	return d
}

// Delay returns the jittered delay. rnd must return values in [0,1); pass
// nil to use the process-wide math/rand/v2 source, which is goroutine-safe.
func (p Policy) Delay(attempt int, rnd func() float64) time.Duration {
	if rnd == nil {
		rnd = rand.Float64
	}
	d := p.Backoff(attempt)
	switch p.Jitter {
	case JitterEqual:
		half := d / 2
		return half + time.Duration(rnd()*float64(d-half))
	case JitterFull:
		return time.Duration(rnd() * float64(d))
	default:
		return d
	}
}
