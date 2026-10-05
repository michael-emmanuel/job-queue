package broker

import (
	"errors"
	"fmt"
	"time"
)

// Errors returned by the broker.
var (
	ErrQueueNotFound = errors.New("broker: queue not found")
	ErrQueueExists   = errors.New("broker: queue already exists")
	ErrInvalidQueue  = errors.New("broker: invalid queue")
	ErrInvalidID     = errors.New("broker: malformed job id")
	ErrClosed        = errors.New("broker: closed")
)

// RateLimitedError reports a rejected request and when to retry it.
type RateLimitedError struct {
	Scope      string // "queue_enqueue" or "queue_consume"
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("broker: rate limited (%s), retry after %s", e.Scope, e.RetryAfter.Round(time.Millisecond))
}
