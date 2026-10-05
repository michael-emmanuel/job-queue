package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/api"
	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

const key = "admin-key-0123456789"

var bg = context.Background()

// newServer runs the real API over an in-memory broker.
func newServer(t *testing.T) *client.Client {
	t.Helper()
	b, err := broker.Open(broker.Config{})
	if err != nil {
		t.Fatal(err)
	}
	auth, _ := api.ParseKeys(key + ":acme:admin")
	srv := httptest.NewServer(api.New(api.Options{Broker: b, Auth: auth, Logger: observability.Discard(), MaxLongPoll: 3 * time.Second}).Handler())
	t.Cleanup(func() { srv.Close(); b.Close() })
	c := client.New(srv.URL, key)
	c.Retry = client.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	return c
}

func fastQueue(t *testing.T, c *client.Client, name string, attempts int) {
	t.Helper()
	q := client.QueueConfig{Name: name, Partitions: 2, DefaultVisibility: "30s"}
	q.Retry = &struct {
		MaxAttempts int    `json:"max_attempts,omitempty"`
		BaseDelay   string `json:"base_delay,omitempty"`
		MaxDelay    string `json:"max_delay,omitempty"`
		Jitter      string `json:"jitter,omitempty"`
	}{MaxAttempts: attempts, BaseDelay: "20ms", MaxDelay: "40ms", Jitter: "none"}
	if err := c.CreateQueue(bg, q); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitPullAckRoundTrip(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	res, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "send_email",
		Payload: map[string]any{"user": "123"}, Priority: 7, IdempotencyKey: "signup-123"})
	if err != nil || res.Duplicate || res.State != "ready" || res.Attempts != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	again, _ := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "send_email", IdempotencyKey: "signup-123"})
	if !again.Duplicate || again.JobID != res.JobID {
		t.Fatalf("retry was not deduplicated: %+v", again)
	}
	jobs, err := c.Pull(bg, "q", client.PullOptions{Worker: "w", Max: 5, Visibility: time.Minute})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("%v %v", jobs, err)
	}
	j := jobs[0]
	if j.ID != res.JobID || j.Attempt != 1 || j.Type != "send_email" || j.Priority != 7 || j.VisibilityDeadline == nil {
		t.Fatalf("job: %+v", j)
	}
	if st, err := c.Get(bg, j.ID); err != nil || st.State != "in_flight" {
		t.Fatalf("%+v %v", st, err)
	}
	if dl, err := c.Renew(bg, j.ID, 1, time.Minute); err != nil || dl.IsZero() {
		t.Fatalf("renew: %v %v", dl, err)
	}
	if err := c.Ack(bg, j.ID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(bg, j.ID); !client.IsNotFound(err) {
		t.Fatalf("completed job: %v", err)
	}
	s, err := c.QueueStats(bg, "q")
	if err != nil || s.Total.Acked != 1 || s.Total.Enqueued != 1 {
		t.Fatalf("%+v %v", s, err)
	}
}

func TestErrorsAreTyped(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	res, _ := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"})
	jobs, _ := c.Pull(bg, "q", client.PullOptions{Worker: "w", Max: 1})
	err := c.Ack(bg, res.JobID, jobs[0].Attempt+5)
	var ae *client.APIError
	if !errors.As(err, &ae) || ae.Status != 409 || !client.IsStaleLease(err) || ae.RequestID == "" || ae.Rejected() {
		t.Fatalf("stale ack: %#v", err)
	}
	if _, err := c.Submit(bg, client.SubmitRequest{Queue: "Bad Name", Type: "t"}); !errors.As(err, &ae) || ae.Status != 400 || ae.Code != "invalid_queue" {
		t.Fatalf("bad queue: %v", err)
	}
	if err := c.Cancel(bg, "q.0.nope"); !client.IsNotFound(err) {
		t.Fatalf("cancel missing: %v", err)
	}
}

func TestDeadLetterOperations(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 1)
	res, _ := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"})
	jobs, _ := c.Pull(bg, "q", client.PullOptions{Worker: "w1", Max: 1})
	if out, err := c.Fail(bg, jobs[0].ID, 1, "boom", true); err != nil || out != "dead" {
		t.Fatalf("%q %v", out, err)
	}
	dl, err := c.DeadLetters(bg, "q", 10)
	if err != nil || len(dl) != 1 || dl[0].JobID != res.JobID || dl[0].Reason != "max_attempts_exceeded" || dl[0].WorkerID != "w1" {
		t.Fatalf("%+v %v", dl, err)
	}
	if err := c.ReplayDead(bg, "q", res.JobID); err != nil {
		t.Fatal(err)
	}
	jobs, _ = c.Pull(bg, "q", client.PullOptions{Worker: "w2", Max: 1})
	if len(jobs) != 1 || jobs[0].Attempt != 1 {
		t.Fatalf("replay: %+v", jobs)
	}
	c.Fail(bg, jobs[0].ID, 1, "again", false)
	if err := c.DeleteDead(bg, "q", res.JobID); err != nil {
		t.Fatal(err)
	}
}

// fakeServer answers from a script so tests control the exact failure sequence.
func fakeServer(t *testing.T, script ...func(w http.ResponseWriter, r *http.Request)) (*client.Client, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		script[min(i, len(script)-1)](w, r)
	}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, "x")
	c.Retry = client.RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
	return c, &n
}

func status(code int, body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		w.Write([]byte(body))
	}
}

const created = `{"job_id":"q.0.abc","queue":"q","state":"ready","duplicate":false}`
const errBody = `{"error":{"code":"x","message":"m","request_id":"r"}}`

func TestSubmitRetriesRejectedRequestsEvenWithoutAKey(t *testing.T) {
	// 429 and 503 mean "not applied", so retrying cannot create a duplicate.
	for _, code := range []int{429, 503} {
		c, n := fakeServer(t, status(code, errBody), status(code, errBody), status(201, created))
		res, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"})
		if err != nil || res.Attempts != 3 || n.Load() != 3 {
			t.Fatalf("code %d: %+v %v after %d calls", code, res, err, n.Load())
		}
	}
}

func TestSubmitDoesNotRetryUnknownOutcomesWithoutAnIdempotencyKey(t *testing.T) {
	for _, code := range []int{500, 504} {
		c, n := fakeServer(t, status(code, errBody), status(201, created))
		_, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"})
		if err == nil || n.Load() != 1 {
			t.Fatalf("code %d: retried a request whose outcome was unknown, with nothing to make a retry safe (%d calls, err %v)", code, n.Load(), err)
		}
		c, n = fakeServer(t, status(code, errBody), status(201, created))
		res, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t", IdempotencyKey: "k"})
		if err != nil || n.Load() != 2 || res.Attempts != 2 {
			t.Fatalf("code %d with a key: %+v %v after %d calls", code, res, err, n.Load())
		}
	}
}

func TestSubmitNeverRetriesClientErrors(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 409, 413} {
		c, n := fakeServer(t, status(code, errBody), status(201, created))
		_, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t", IdempotencyKey: "k"})
		var ae *client.APIError
		if !errors.As(err, &ae) || ae.Status != code || n.Load() != 1 {
			t.Fatalf("code %d: err=%v calls=%d", code, err, n.Load())
		}
	}
}

func TestSubmitTransportErrorsRetryOnlyWithAKey(t *testing.T) {
	drop := func(w http.ResponseWriter, _ *http.Request) {
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close() // the connection dies mid-request: did the server see it? unknown.
	}
	c, n := fakeServer(t, drop, status(201, created))
	if _, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"}); err == nil || n.Load() != 1 {
		t.Fatalf("keyless transport error was retried: %d calls, %v", n.Load(), err)
	}
	c, n = fakeServer(t, drop, status(201, created))
	if res, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t", IdempotencyKey: "k"}); err != nil || n.Load() != 2 {
		t.Fatalf("%+v %v after %d", res, err, n.Load())
	}
}

func TestSubmitGivesUpAfterMaxAttempts(t *testing.T) {
	c, n := fakeServer(t, status(429, errBody))
	res, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"})
	if err == nil || n.Load() != 4 || res.Attempts != 4 {
		t.Fatalf("%+v %v after %d calls; want exactly MaxAttempts=4", res, err, n.Load())
	}
}

func TestSubmitHonoursRetryAfter(t *testing.T) {
	c, _ := fakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		status(429, errBody)(w, nil)
	}, status(201, created))
	start := time.Now()
	if _, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("retried after %v, ignoring Retry-After: 1", d)
	}
}

func TestSubmitStopsWhenContextEnds(t *testing.T) {
	c, _ := fakeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		status(429, errBody)(w, nil)
	})
	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Submit(ctx, client.SubmitRequest{Queue: "q", Type: "t"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("did not stop promptly")
	}
}
