package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
)

// apiError is the one error shape every endpoint returns:
//
//	{"error": {"code": "...", "message": "...", "request_id": "..."}}
//
// Codes are stable and machine-readable; messages are for humans.
type apiError struct {
	status     int
	code       string
	msg        string
	retryAfter time.Duration
}

func (e *apiError) Error() string { return e.code + ": " + e.msg }

func newErr(status int, code, format string, args ...any) *apiError {
	return &apiError{status: status, code: code, msg: fmt.Sprintf(format, args...)}
}

func rateLimited(scope string, wait time.Duration) *apiError {
	e := newErr(http.StatusTooManyRequests, "rate_limited", "%s rate limit exceeded", scope)
	e.retryAfter = wait
	return e
}

// toAPIError maps internal errors to API errors. Anything unrecognised
// becomes an opaque 500: internal error text can leak paths and structure.
func toAPIError(err error) (*apiError, bool) {
	var ae *apiError
	var rl *broker.RateLimitedError
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &ae):
		return ae, true
	case errors.As(err, &rl):
		e := newErr(http.StatusTooManyRequests, "rate_limited", "%s rate limit exceeded", strings.TrimPrefix(rl.Scope, "queue_"))
		e.retryAfter = rl.RetryAfter
		return e, true
	case errors.As(err, &tooBig):
		return newErr(http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds %d bytes", tooBig.Limit), true
	case errors.Is(err, partition.ErrInvalidJob):
		return newErr(http.StatusBadRequest, "invalid_job", "%s", strings.TrimPrefix(err.Error(), partition.ErrInvalidJob.Error()+": ")), true
	case errors.Is(err, broker.ErrInvalidQueue):
		return newErr(http.StatusBadRequest, "invalid_queue", "%s", strings.TrimPrefix(err.Error(), broker.ErrInvalidQueue.Error()+": ")), true
	case errors.Is(err, broker.ErrInvalidID):
		return newErr(http.StatusBadRequest, "invalid_job_id", "malformed job id"), true
	case errors.Is(err, broker.ErrQueueNotFound):
		return newErr(http.StatusNotFound, "queue_not_found", "queue not found"), true
	case errors.Is(err, broker.ErrQueueExists):
		return newErr(http.StatusConflict, "queue_exists", "queue already exists"), true
	case errors.Is(err, partition.ErrNotFound):
		return newErr(http.StatusNotFound, "job_not_found", "job not found (it may already have completed)"), true
	case errors.Is(err, partition.ErrStaleLease):
		return newErr(http.StatusConflict, "stale_lease", "the lease for this attempt has expired or was superseded; stop working on this job"), true
	case errors.Is(err, partition.ErrQueueFull):
		e := newErr(http.StatusTooManyRequests, "queue_full", "the queue is at capacity; slow down and retry")
		e.retryAfter = time.Second
		return e, true
	case errors.Is(err, ErrUnavailable):
		e := newErr(http.StatusServiceUnavailable, "unavailable", "the cluster cannot serve this request right now; retry shortly")
		e.retryAfter = time.Second
		return e, true
	case errors.Is(err, ErrStaleGeneration):
		return newErr(http.StatusConflict, "stale_generation", "consumer group membership changed; pull again to rejoin"), true
	case errors.Is(err, partition.ErrClosed), errors.Is(err, broker.ErrClosed):
		e := newErr(http.StatusServiceUnavailable, "unavailable", "the service is shutting down")
		e.retryAfter = time.Second
		return e, true
	case errors.Is(err, context.DeadlineExceeded):
		return newErr(http.StatusGatewayTimeout, "timeout",
			"the request timed out; its effect is unknown, so retry with the same idempotency key"), true
	case errors.Is(err, context.Canceled):
		return newErr(499, "client_closed", "request cancelled"), true
	}
	return nil, false
}

func retryAfterSeconds(d time.Duration) string {
	s := int((d + time.Second - 1) / time.Second) // round up: never tell a client to retry too early
	return strconv.Itoa(max(s, 1))
}
