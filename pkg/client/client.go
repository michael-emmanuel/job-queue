// Package client is a Go client for the job queue's HTTP API.
//
// It encodes the client side of the delivery contract:
//
//   - A rejected request (429, 503) was not applied, so it is always safe to
//     retry. Retries honour Retry-After and add jitter so that thousands of
//     clients released at the same moment do not return in lockstep.
//   - A request whose outcome is unknown (timeout, connection reset, 504) may
//     have been applied. Submit retries those only when the job carries an
//     idempotency key, which makes the retry harmless.
//   - Everything else (400, 401, 403, 404, 409) is returned immediately.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to a gateway (or a single-node broker).
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// Retry controls automatic retries in Submit; the zero value uses DefaultRetry.
	Retry RetryPolicy
}

// RetryPolicy is exponential backoff with full jitter.
type RetryPolicy struct {
	MaxAttempts int           // total tries including the first; 0 means 5
	BaseDelay   time.Duration // 0 means 100ms
	MaxDelay    time.Duration // 0 means 5s
}

// DefaultRetry is used when Client.Retry is the zero value.
var DefaultRetry = RetryPolicy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second}

// New returns a Client with a pooled HTTP transport.
func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
		},
	}}
}

// APIError is an error response from the server.
type APIError struct {
	Status     int
	Code       string
	Message    string
	RequestID  string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s (HTTP %d, code %s, request %s)", e.Message, e.Status, e.Code, e.RequestID)
}

// Rejected reports that the server refused the request without applying it
// (rate limit, full queue, overload, shutting down). Retrying is always safe.
func (e *APIError) Rejected() bool { return e.Status == 429 || e.Status == 503 }

// Unknown reports that the request may or may not have been applied.
func (e *APIError) Unknown() bool { return e.Status == 504 || e.Status >= 500 && e.Status != 503 }

// IsStaleLease reports a fenced-out worker: the job was redelivered to
// someone else, so this worker must stop and not report a result.
func IsStaleLease(err error) bool { return hasCode(err, "stale_lease") }

// IsNotFound reports a missing job or queue. For a worker acking a job it
// usually means the ack already succeeded or the job was cancelled.
func IsNotFound(err error) bool {
	return hasCode(err, "job_not_found") || hasCode(err, "queue_not_found")
}

func hasCode(err error, code string) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == code
}

func (c *Client) do(ctx context.Context, method, path string, in, out any, hdr map[string]string) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("User-Agent", "jobqueue-client/1")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		ae := &APIError{Status: res.StatusCode, RequestID: res.Header.Get("X-Request-ID")}
		var env struct {
			Error struct{ Code, Message string }
		}
		if json.Unmarshal(raw, &env) == nil {
			ae.Code, ae.Message = env.Error.Code, env.Error.Message
		}
		if ae.Message == "" {
			ae.Message = http.StatusText(res.StatusCode)
		}
		if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
			ae.RetryAfter = time.Duration(s) * time.Second
		}
		return ae
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// ---- jobs ----

// SubmitRequest describes a job.
type SubmitRequest struct {
	Queue          string
	Type           string
	Payload        any
	Priority       int // 1-10, default 5
	PartitionKey   string
	IdempotencyKey string // strongly recommended: makes retries safe
	MaxAttempts    int
	RunAt          time.Time // zero = now
}

// SubmitResult is the outcome of Submit.
type SubmitResult struct {
	JobID     string `json:"job_id"`
	Queue     string `json:"queue"`
	State     string `json:"state"`
	Duplicate bool   `json:"duplicate"`
	// Attempts is how many HTTP attempts it took (1 = no retry).
	Attempts int `json:"-"`
}

// Submit enqueues a job, retrying per the safety rules in the package doc.
func (c *Client) Submit(ctx context.Context, r SubmitRequest) (SubmitResult, error) {
	body := map[string]any{"queue": r.Queue, "type": r.Type, "payload": r.Payload}
	if r.Priority != 0 {
		body["priority"] = r.Priority
	}
	if r.PartitionKey != "" {
		body["partition_key"] = r.PartitionKey
	}
	if r.MaxAttempts != 0 {
		body["max_attempts"] = r.MaxAttempts
	}
	if !r.RunAt.IsZero() {
		body["run_at"] = r.RunAt.UTC().Format(time.RFC3339Nano)
	}
	var hdr map[string]string
	if r.IdempotencyKey != "" {
		hdr = map[string]string{"Idempotency-Key": r.IdempotencyKey}
	}
	pol := c.Retry
	if pol == (RetryPolicy{}) {
		pol = DefaultRetry
	}
	var last error
	for attempt := 1; ; attempt++ {
		var res SubmitResult
		err := c.do(ctx, http.MethodPost, "/v1/jobs", body, &res, hdr)
		if err == nil {
			res.Attempts = attempt
			return res, nil
		}
		last = err
		if attempt >= max(pol.MaxAttempts, 1) || !c.retryable(err, r.IdempotencyKey != "") {
			return SubmitResult{Attempts: attempt}, last
		}
		var ae *APIError
		wait := backoff(pol, attempt)
		if errors.As(err, &ae) && ae.RetryAfter > wait {
			wait = ae.RetryAfter
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return SubmitResult{Attempts: attempt}, ctx.Err()
		}
	}
}

func (c *Client) retryable(err error, idempotent bool) bool {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Rejected() || (idempotent && ae.Unknown())
	}
	// Transport errors: the request may have reached the server.
	return idempotent && !errors.Is(err, context.Canceled)
}

// backoff returns a full-jitter delay: uniform in [0, min(max, base*2^(n-1))].
func backoff(p RetryPolicy, attempt int) time.Duration {
	base, maxD := p.BaseDelay, p.MaxDelay
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if maxD <= 0 {
		maxD = 5 * time.Second
	}
	d := base
	for i := 1; i < attempt && d < maxD; i++ {
		d *= 2
	}
	d = min(d, maxD)
	return time.Duration(rand.Float64() * float64(d))
}

// Job is a delivered job.
type Job struct {
	ID                 string          `json:"job_id"`
	Queue              string          `json:"queue"`
	Tenant             string          `json:"tenant"`
	Type               string          `json:"type"`
	Payload            json.RawMessage `json:"payload"`
	Priority           int             `json:"priority"`
	State              string          `json:"state"`
	Attempt            int             `json:"attempt"`
	MaxAttempts        int             `json:"max_attempts"`
	CreatedAt          time.Time       `json:"created_at"`
	VisibilityDeadline *time.Time      `json:"visibility_deadline"`
	LastError          string          `json:"last_error"`
}

// PullOptions controls Pull.
type PullOptions struct {
	Worker     string
	Group      string // optional consumer group
	Max        int
	Visibility time.Duration
	Wait       time.Duration // long poll
}

// Pull leases jobs. An empty result means none were ready.
func (c *Client) Pull(ctx context.Context, queue string, o PullOptions) ([]Job, error) {
	body := map[string]any{"worker": o.Worker, "max": o.Max}
	if o.Group != "" {
		body["group"] = o.Group
	}
	if o.Visibility > 0 {
		body["visibility"] = o.Visibility.String()
	}
	if o.Wait > 0 {
		body["wait"] = o.Wait.String()
	}
	var out struct {
		Jobs []Job `json:"jobs"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/queues/"+url.PathEscape(queue)+"/pull", body, &out, nil); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

// Ack completes a job.
func (c *Client) Ack(ctx context.Context, id string, attempt int) error {
	return c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/ack", map[string]int{"attempt": attempt}, nil, nil)
}

// Fail reports a failure; it returns "retry" or "dead".
func (c *Client) Fail(ctx context.Context, id string, attempt int, msg string, retryable bool) (string, error) {
	var out struct {
		Outcome string `json:"outcome"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/fail",
		map[string]any{"attempt": attempt, "error": msg, "retryable": retryable}, &out, nil)
	return out.Outcome, err
}

// Renew extends a lease and returns the new deadline.
func (c *Client) Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error) {
	var out struct {
		Deadline time.Time `json:"visibility_deadline"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/renew",
		map[string]any{"attempt": attempt, "extend": extend.String()}, &out, nil)
	return out.Deadline, err
}

// Get returns a job's status (payload omitted).
func (c *Client) Get(ctx context.Context, id string) (Job, error) {
	var j Job
	err := c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &j, nil)
	return j, err
}

// Cancel removes a live job.
func (c *Client) Cancel(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/jobs/"+url.PathEscape(id), nil, nil, nil)
}

// ---- queues (admin) ----

// QueueConfig is the subset of queue settings clients usually set. Zero
// fields take server defaults.
type QueueConfig struct {
	Name              string  `json:"name"`
	Partitions        int     `json:"partitions,omitempty"`
	ReplicationFactor int     `json:"replication_factor,omitempty"`
	Ack               string  `json:"ack,omitempty"`
	DefaultVisibility string  `json:"default_visibility,omitempty"`
	MaxDepth          int     `json:"max_depth,omitempty"`
	EnqueueRate       float64 `json:"enqueue_rate,omitempty"`
	ConsumeRate       float64 `json:"consume_rate,omitempty"`
	Retry             *struct {
		MaxAttempts int    `json:"max_attempts,omitempty"`
		BaseDelay   string `json:"base_delay,omitempty"`
		MaxDelay    string `json:"max_delay,omitempty"`
		Jitter      string `json:"jitter,omitempty"`
	} `json:"retry,omitempty"`
}

// CreateQueue creates a queue.
func (c *Client) CreateQueue(ctx context.Context, cfg QueueConfig) error {
	return c.do(ctx, http.MethodPost, "/v1/queues", cfg, nil, nil)
}

// QueueStats is a queue's counts.
type QueueStats struct {
	Name  string `json:"name"`
	Total struct {
		Ready        int    `json:"ready"`
		Scheduled    int    `json:"scheduled"`
		InFlight     int    `json:"in_flight"`
		Dead         int    `json:"dead"`
		Enqueued     uint64 `json:"enqueued"`
		Acked        uint64 `json:"acked"`
		Retried      uint64 `json:"retried"`
		DeadLettered uint64 `json:"dead_lettered"`
		Rejected     uint64 `json:"rejected"`
		LeaseExpired uint64 `json:"lease_expired"`
	} `json:"total"`
	Partitions []struct {
		Ready    int    `json:"ready"`
		InFlight int    `json:"in_flight"`
		Enqueued uint64 `json:"enqueued"`
	} `json:"partitions"`
}

// QueueStats returns statistics for a queue (admin).
func (c *Client) QueueStats(ctx context.Context, queue string) (QueueStats, error) {
	var s QueueStats
	err := c.do(ctx, http.MethodGet, "/v1/queues/"+url.PathEscape(queue), nil, &s, nil)
	return s, err
}

// DeadLetter is a DLQ entry.
type DeadLetter struct {
	JobID     string          `json:"job_id"`
	Queue     string          `json:"original_queue"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Reason    string          `json:"reason"`
	LastError string          `json:"last_error"`
	WorkerID  string          `json:"worker_id"`
	Attempts  int             `json:"attempts"`
	DeadAt    time.Time       `json:"dead_at"`
}

// DeadLetters lists DLQ entries (admin).
func (c *Client) DeadLetters(ctx context.Context, queue string, limit int) ([]DeadLetter, error) {
	var out struct {
		Dead []DeadLetter `json:"dead_letters"`
	}
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/queues/%s/dlq?limit=%d", url.PathEscape(queue), max(limit, 1)), nil, &out, nil)
	return out.Dead, err
}

// ReplayDead moves a dead-lettered job back to ready (admin).
func (c *Client) ReplayDead(ctx context.Context, queue, id string) error {
	return c.do(ctx, http.MethodPost, "/v1/queues/"+url.PathEscape(queue)+"/dlq/"+url.PathEscape(id)+"/replay", nil, nil, nil)
}

// DeleteDead permanently removes a DLQ entry (admin).
func (c *Client) DeleteDead(ctx context.Context, queue, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/queues/"+url.PathEscape(queue)+"/dlq/"+url.PathEscape(id), nil, nil, nil)
}
