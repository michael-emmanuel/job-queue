package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
)

const (
	adminKey = "admin-key-0123456789"
	prodA    = "prodA-key-0123456789"
	prodB    = "prodB-key-0123456789"
	workKey  = "work-key-0123456789abc"
)

const keySpec = adminKey + ":ops:admin," + prodA + ":a:producer," + prodB + ":b:producer," + workKey + ":workers:worker"

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type env struct {
	t     *testing.T
	srv   *Server
	ts    *httptest.Server
	admin *httptest.Server
	b     *broker.Broker
	logs  *syncBuf
}

func newEnv(t *testing.T, mut ...func(*Options, *broker.Config)) *env {
	t.Helper()
	auth, err := ParseKeys(keySpec)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuf{}
	lg := observability.NewLogger(logs, "debug", true)
	bc := broker.Config{Logger: lg}
	o := Options{Auth: auth, Logger: lg}
	for _, m := range mut {
		m(&o, &bc)
	}
	b, err := broker.Open(bc)
	if err != nil {
		t.Fatal(err)
	}
	o.Broker = b
	s := New(o)
	e := &env{t: t, srv: s, b: b, logs: logs,
		ts: httptest.NewServer(s.Handler()), admin: httptest.NewServer(s.AdminHandler(false))}
	t.Cleanup(func() { e.ts.Close(); e.admin.Close(); b.Close() })
	return e
}

type resp struct {
	*http.Response
	body []byte
}

func (r resp) json() map[string]any {
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		panic(fmt.Sprintf("not json: %q", r.body))
	}
	return m
}

func (r resp) errCode() string {
	e, _ := r.json()["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func (e *env) call(method, path, key string, body any, hdr ...string) resp {
	e.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, e.ts.URL+path, rd)
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{res, b}
}

func (e *env) want(r resp, status int) resp {
	e.t.Helper()
	if r.StatusCode != status {
		e.t.Fatalf("status = %d, want %d; body: %s", r.StatusCode, status, r.body)
	}
	return r
}

func (e *env) queue(name string, cfg map[string]any) {
	e.t.Helper()
	if cfg == nil {
		cfg = map[string]any{}
	}
	cfg["name"] = name
	e.want(e.call("POST", "/v1/queues", adminKey, cfg), 201)
}

func (e *env) submit(key string, body map[string]any, hdr ...string) resp {
	return e.call("POST", "/v1/jobs", key, body, hdr...)
}

func (e *env) pull(queue, worker string, extra map[string]any) []map[string]any {
	e.t.Helper()
	body := map[string]any{"worker": worker, "max": 10}
	for k, v := range extra {
		body[k] = v
	}
	r := e.want(e.call("POST", "/v1/queues/"+queue+"/pull", workKey, body), 200)
	var out []map[string]any
	for _, j := range r.json()["jobs"].([]any) {
		out = append(out, j.(map[string]any))
	}
	return out
}

func jobBody(queue string) map[string]any {
	return map[string]any{"queue": queue, "type": "send_email", "payload": map[string]any{"user_id": "123"}}
}

func TestSubmitPullAckLifecycle(t *testing.T) {
	e := newEnv(t)
	e.queue("email", map[string]any{"partitions": 2})

	body := jobBody("email")
	body["priority"] = 7
	r := e.want(e.submit(prodA, body), 201)
	id := r.json()["job_id"].(string)
	if !strings.HasPrefix(id, "email.") || r.json()["state"] != "ready" || r.json()["duplicate"] != false {
		t.Fatalf("submit response: %s", r.body)
	}

	jobs := e.pull("email", "worker-1", nil)
	if len(jobs) != 1 {
		t.Fatalf("pulled %d jobs", len(jobs))
	}
	j := jobs[0]
	if j["job_id"] != id || j["type"] != "send_email" || j["attempt"].(float64) != 1 ||
		j["priority"].(float64) != 7 || j["visibility_deadline"] == nil || j["created_at"] == nil {
		t.Fatalf("delivery: %v", j)
	}
	if p := j["payload"].(map[string]any); p["user_id"] != "123" {
		t.Fatalf("payload: %v", p)
	}

	st := e.want(e.call("GET", "/v1/jobs/"+id, prodA, nil), 200).json()
	if st["state"] != "in_flight" || st["payload"] != nil {
		t.Fatalf("status view: %v (payload must not appear in status)", st)
	}
	e.want(e.call("POST", "/v1/jobs/"+id+"/ack", workKey, map[string]any{"attempt": 1}), 204)
	e.want(e.call("GET", "/v1/jobs/"+id, prodA, nil), 404)
	if got := e.pull("email", "worker-1", nil); len(got) != 0 {
		t.Fatalf("job redelivered after ack: %v", got)
	}
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t)
	e.queue("q", nil)
	r := e.want(e.call("POST", "/v1/jobs", "", jobBody("q")), 401)
	if r.Header.Get("WWW-Authenticate") == "" || r.errCode() != "unauthenticated" {
		t.Errorf("401 response: %v %s", r.Header, r.body)
	}
	e.want(e.call("POST", "/v1/jobs", "wrong-key-0123456789", jobBody("q")), 401)
	e.want(e.call("POST", "/v1/jobs", "", jobBody("q"), "Authorization", "Basic abc"), 401)
	e.want(e.call("GET", "/healthz", "", nil), 200) // probes need no credentials
}

func TestAuthorizationByRole(t *testing.T) {
	e := newEnv(t)
	e.queue("q", nil)
	pullBody := map[string]any{"worker": "w"}
	if e.call("POST", "/v1/queues/q/pull", prodA, pullBody).StatusCode != 403 {
		t.Error("producer pulled jobs")
	}
	if e.call("POST", "/v1/jobs", workKey, jobBody("q")).StatusCode != 403 {
		t.Error("worker submitted jobs")
	}
	for _, c := range []struct{ method, path string }{
		{"POST", "/v1/queues"}, {"GET", "/v1/queues"}, {"DELETE", "/v1/queues/q"}, {"GET", "/v1/queues/q/dlq"},
	} {
		for _, k := range []string{prodA, workKey} {
			if got := e.call(c.method, c.path, k, map[string]any{}).StatusCode; got != 403 {
				t.Errorf("%s %s with non-admin key = %d, want 403", c.method, c.path, got)
			}
		}
	}
}

func TestTenantIsolation(t *testing.T) {
	e := newEnv(t)
	e.queue("q", nil)
	id := e.want(e.submit(prodA, jobBody("q")), 201).json()["job_id"].(string)

	// Tenant b cannot see or cancel tenant a's job, and cannot tell it exists.
	if r := e.call("GET", "/v1/jobs/"+id, prodB, nil); r.StatusCode != 404 || r.errCode() != "job_not_found" {
		t.Errorf("cross-tenant get: %d %s", r.StatusCode, r.body)
	}
	e.want(e.call("DELETE", "/v1/jobs/"+id, prodB, nil), 404)
	e.want(e.call("GET", "/v1/jobs/"+id, prodA, nil), 200)
	e.want(e.call("GET", "/v1/jobs/"+id, workKey, nil), 200)

	// A body cannot claim a tenant: unknown fields are rejected outright.
	bad := jobBody("q")
	bad["tenant"] = "b"
	e.want(e.submit(prodA, bad), 400)

	e.want(e.call("DELETE", "/v1/jobs/"+id, prodA, nil), 204)
	e.want(e.call("GET", "/v1/jobs/"+id, prodA, nil), 404)
}

func TestIdempotencyKey(t *testing.T) {
	e := newEnv(t)
	e.queue("payments", nil)
	b := jobBody("payments")
	first := e.want(e.submit(prodA, b, "Idempotency-Key", "payment-123"), 201).json()
	// The client timed out and retries: it must get the same job, not a new one.
	again := e.want(e.submit(prodA, b, "Idempotency-Key", "payment-123"), 200).json()
	if again["job_id"] != first["job_id"] || again["duplicate"] != true {
		t.Fatalf("retry: %v vs %v", again, first)
	}
	// Same key from another tenant is a different logical request.
	e.want(e.submit(prodB, b, "Idempotency-Key", "payment-123"), 201)
	// Body field works too, and must not contradict the header.
	b2 := jobBody("payments")
	b2["idempotency_key"] = "payment-123"
	if got := e.want(e.submit(prodA, b2), 200).json(); got["job_id"] != first["job_id"] {
		t.Fatalf("body key: %v", got)
	}
	e.want(e.submit(prodA, b2, "Idempotency-Key", "different"), 400)
	if n := len(e.pull("payments", "w", nil)); n != 2 {
		t.Fatalf("%d jobs exist, want 2 (one per tenant)", n)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, func(o *Options, _ *broker.Config) { o.MaxBodyBytes = 2048 })
	e.queue("q", nil)
	cases := []struct {
		name   string
		body   any
		status int
		code   string
		ct     string
	}{
		{"unknown field", `{"queue":"q","type":"t","idempotency_ky":"x"}`, 400, "invalid_json", ""},
		{"trailing data", `{"queue":"q","type":"t"} {"x":1}`, 400, "invalid_json", ""},
		{"empty body", ``, 400, "invalid_json", ""},
		{"not json", `{nope`, 400, "invalid_json", ""},
		{"missing queue", `{"type":"t"}`, 400, "invalid_queue", ""},
		{"bad queue name", `{"queue":"Bad.Name","type":"t"}`, 400, "invalid_queue", ""},
		{"missing type", `{"queue":"q"}`, 400, "invalid_job", ""},
		{"priority range", `{"queue":"q","type":"t","priority":11}`, 400, "invalid_job", ""},
		{"payload not json", `{"queue":"q","type":"t","payload":}`, 400, "invalid_json", ""},
		{"body too large", `{"queue":"q","type":"t","payload":"` + strings.Repeat("x", 4096) + `"}`, 413, "payload_too_large", ""},
		{"wrong content type", `{"queue":"q","type":"t"}`, 415, "unsupported_media_type", "text/plain"},
		{"unknown queue", `{"queue":"ghost","type":"t"}`, 404, "queue_not_found", ""},
	}
	for _, c := range cases {
		hdr := []string{}
		if c.ct != "" {
			hdr = []string{"Content-Type", c.ct}
		}
		r := e.call("POST", "/v1/jobs", prodA, c.body, hdr...)
		if r.StatusCode != c.status || r.errCode() != c.code {
			t.Errorf("%s: got %d %q (%s), want %d %q", c.name, r.StatusCode, r.errCode(), r.body, c.status, c.code)
		}
	}
}

func TestErrorEnvelopeCarriesRequestID(t *testing.T) {
	e := newEnv(t)
	r := e.call("GET", "/v1/jobs/nope", prodA, nil, "X-Request-ID", "trace-me-42")
	if got := r.Header.Get("X-Request-ID"); got != "trace-me-42" {
		t.Errorf("request id not echoed: %q", got)
	}
	errObj := r.json()["error"].(map[string]any)
	if errObj["request_id"] != "trace-me-42" || errObj["code"] == "" || errObj["message"] == "" {
		t.Errorf("envelope: %v", errObj)
	}
	r = e.call("GET", "/v1/jobs/nope", prodA, nil, "X-Request-ID", "bad id with spaces")
	if id := r.Header.Get("X-Request-ID"); id == "" || strings.ContainsAny(id, " \n") {
		t.Errorf("unsafe request id was not replaced: %q", id)
	}
	if r := e.call("GET", "/v1/nonexistent", prodA, nil); r.StatusCode != 404 || r.errCode() != "not_found" {
		t.Errorf("unknown route: %d %s", r.StatusCode, r.body)
	}
}

func TestRetryThenDeadLetterThenReplay(t *testing.T) {
	e := newEnv(t)
	e.queue("flaky", map[string]any{"partitions": 1, "retry": map[string]any{
		"max_attempts": 2, "base_delay": "10ms", "max_delay": "20ms", "jitter": "none"}})
	id := e.want(e.submit(prodA, jobBody("flaky")), 201).json()["job_id"].(string)

	j := e.pull("flaky", "w1", nil)[0]
	out := e.want(e.call("POST", "/v1/jobs/"+id+"/fail", workKey,
		map[string]any{"attempt": 1, "error": "smtp timeout"}), 200).json()
	if out["outcome"] != "retry" {
		t.Fatalf("outcome %v", out)
	}
	_ = j
	again := e.pull("flaky", "w2", map[string]any{"wait": "2s"})
	if len(again) != 1 || again[0]["attempt"].(float64) != 2 {
		t.Fatalf("retry delivery: %v", again)
	}
	out = e.want(e.call("POST", "/v1/jobs/"+id+"/fail", workKey,
		map[string]any{"attempt": 2, "error": "smtp timeout again"}), 200).json()
	if out["outcome"] != "dead" {
		t.Fatalf("outcome %v", out)
	}

	dlq := e.want(e.call("GET", "/v1/queues/flaky/dlq", adminKey, nil), 200).json()["dead_letters"].([]any)
	if len(dlq) != 1 {
		t.Fatalf("dlq: %v", dlq)
	}
	d := dlq[0].(map[string]any)
	if d["job_id"] != id || d["reason"] != "max_attempts_exceeded" || d["last_error"] != "smtp timeout again" ||
		d["worker_id"] != "w2" || d["attempts"].(float64) != 2 || d["original_queue"] != "flaky" {
		t.Fatalf("dlq metadata: %v", d)
	}

	e.want(e.call("POST", "/v1/queues/flaky/dlq/"+id+"/replay", adminKey, nil), 200)
	if got := e.pull("flaky", "w3", nil); len(got) != 1 || got[0]["attempt"].(float64) != 1 {
		t.Fatalf("replayed job: %v", got)
	}
}

func TestNonRetryableFailureAndDLQDeleteScopedToQueue(t *testing.T) {
	e := newEnv(t)
	e.queue("a", map[string]any{"partitions": 1})
	e.queue("b", map[string]any{"partitions": 1})
	id := e.want(e.submit(prodA, jobBody("a")), 201).json()["job_id"].(string)
	e.pull("a", "w", nil)
	out := e.want(e.call("POST", "/v1/jobs/"+id+"/fail", workKey,
		map[string]any{"attempt": 1, "error": "malformed", "retryable": false}), 200).json()
	if out["outcome"] != "dead" {
		t.Fatalf("%v", out)
	}
	e.want(e.call("DELETE", "/v1/queues/b/dlq/"+id, adminKey, nil), 404) // wrong queue
	e.want(e.call("DELETE", "/v1/queues/a/dlq/"+id, adminKey, nil), 204)
	if d := e.want(e.call("GET", "/v1/queues/a/dlq", adminKey, nil), 200).json()["dead_letters"].([]any); len(d) != 0 {
		t.Fatalf("dlq not empty: %v", d)
	}
}

func TestVisibilityTimeoutAndFencingOverHTTP(t *testing.T) {
	e := newEnv(t)
	e.queue("q", map[string]any{"partitions": 1})
	id := e.want(e.submit(prodA, jobBody("q")), 201).json()["job_id"].(string)
	e.pull("q", "slow-worker", map[string]any{"visibility": "100ms"})
	time.Sleep(200 * time.Millisecond) // lease lapses; the worker is presumed dead

	second := e.pull("q", "healthy-worker", nil)
	if len(second) != 1 || second[0]["attempt"].(float64) != 2 {
		t.Fatalf("redelivery: %v", second)
	}
	r := e.want(e.call("POST", "/v1/jobs/"+id+"/ack", workKey, map[string]any{"attempt": 1}), 409)
	if r.errCode() != "stale_lease" {
		t.Errorf("code %q", r.errCode())
	}
	e.want(e.call("POST", "/v1/jobs/"+id+"/renew", workKey, map[string]any{"attempt": 1}), 409)
	e.want(e.call("POST", "/v1/jobs/"+id+"/ack", workKey, map[string]any{"attempt": 2}), 204)
}

func TestRenewExtendsLease(t *testing.T) {
	e := newEnv(t)
	e.queue("q", map[string]any{"partitions": 1})
	id := e.want(e.submit(prodA, jobBody("q")), 201).json()["job_id"].(string)
	e.pull("q", "w", map[string]any{"visibility": "150ms"})
	for i := 0; i < 4; i++ { // 4 x 100ms, well past the original 150ms
		time.Sleep(100 * time.Millisecond)
		e.want(e.call("POST", "/v1/jobs/"+id+"/renew", workKey, map[string]any{"attempt": 1, "extend": "150ms"}), 200)
	}
	if got := e.pull("q", "other", nil); len(got) != 0 {
		t.Fatalf("job redelivered despite heartbeats: %v", got)
	}
	e.want(e.call("POST", "/v1/jobs/"+id+"/ack", workKey, map[string]any{"attempt": 1}), 204)
}

func TestScheduledJob(t *testing.T) {
	e := newEnv(t)
	e.queue("q", map[string]any{"partitions": 1})
	b := jobBody("q")
	b["run_at"] = time.Now().Add(300 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	if st := e.want(e.submit(prodA, b), 201).json()["state"]; st != "scheduled" {
		t.Fatalf("state %v", st)
	}
	if got := e.pull("q", "w", nil); len(got) != 0 {
		t.Fatal("scheduled job delivered early")
	}
	if got := e.pull("q", "w", map[string]any{"wait": "3s"}); len(got) != 1 {
		t.Fatalf("scheduled job never delivered: %v", got)
	}
}

func TestTenantRateLimit(t *testing.T) {
	clk := clock.NewFake(time.Now())
	e := newEnv(t, func(o *Options, _ *broker.Config) { o.Clock = clk; o.TenantRate = 1; o.TenantBurst = 2 })
	e.queue("q", nil)
	e.want(e.submit(prodA, jobBody("q")), 201)
	e.want(e.submit(prodA, jobBody("q")), 201)
	r := e.want(e.submit(prodA, jobBody("q")), 429)
	if r.errCode() != "rate_limited" || r.Header.Get("Retry-After") != "1" {
		t.Errorf("429: code=%q retry-after=%q", r.errCode(), r.Header.Get("Retry-After"))
	}
	e.want(e.submit(prodB, jobBody("q")), 201) // noisy neighbour does not affect tenant b
	clk.Advance(time.Second)
	e.want(e.submit(prodA, jobBody("q")), 201)
}

func TestGlobalRateLimitDoesNotChargeRejectedTenants(t *testing.T) {
	clk := clock.NewFake(time.Now())
	e := newEnv(t, func(o *Options, _ *broker.Config) {
		o.Clock = clk
		o.GlobalRate, o.GlobalBurst = 1, 1
		o.TenantRate, o.TenantBurst = 1, 1
	})
	e.queue("q", nil)
	e.want(e.submit(prodA, jobBody("q")), 201)
	if r := e.want(e.submit(prodB, jobBody("q")), 429); r.errCode() != "rate_limited" {
		t.Fatal(r.body)
	}
	clk.Advance(time.Second) // global refills; b's tenant token must have been refunded
	e.want(e.submit(prodB, jobBody("q")), 201)
}

func TestBackpressureQueueFull(t *testing.T) {
	e := newEnv(t)
	e.queue("small", map[string]any{"partitions": 1, "max_depth": 4})
	for i := 0; i < 4; i++ {
		e.want(e.submit(prodA, jobBody("small")), 201)
	}
	r := e.want(e.submit(prodA, jobBody("small")), 429)
	if r.errCode() != "queue_full" || r.Header.Get("Retry-After") == "" {
		t.Fatalf("429: %s %v", r.body, r.Header)
	}
	// Draining frees capacity again.
	j := e.pull("small", "w", map[string]any{"max": 1})[0]
	e.want(e.call("POST", "/v1/jobs/"+j["job_id"].(string)+"/ack", workKey, map[string]any{"attempt": 1}), 204)
	e.want(e.submit(prodA, jobBody("small")), 201)
}

func TestConsumeRateLimitReturns429(t *testing.T) {
	e := newEnv(t)
	e.queue("slow", map[string]any{"partitions": 1, "consume_rate": 0.5, "consume_burst": 1})
	for i := 0; i < 3; i++ {
		e.want(e.submit(prodA, jobBody("slow")), 201)
	}
	if got := e.pull("slow", "w", nil); len(got) != 1 {
		t.Fatalf("burst delivery: %d", len(got))
	}
	r := e.want(e.call("POST", "/v1/queues/slow/pull", workKey, map[string]any{"worker": "w"}), 429)
	if r.Header.Get("Retry-After") == "" {
		t.Error("no Retry-After on consume limit")
	}
}

func TestLongPollWakesOnSubmit(t *testing.T) {
	e := newEnv(t)
	e.queue("q", nil)
	got := make(chan []map[string]any, 1)
	go func() { got <- e.pull("q", "w", map[string]any{"wait": "10s"}) }()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	e.want(e.submit(prodA, jobBody("q")), 201)
	select {
	case jobs := <-got:
		if len(jobs) != 1 || time.Since(start) > 2*time.Second {
			t.Fatalf("jobs=%d after %v", len(jobs), time.Since(start))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not wake")
	}
}

func TestGracefulDrainReleasesLongPolls(t *testing.T) {
	e := newEnv(t)
	e.queue("q", nil)
	if e.call("GET", "/readyz", "", nil).StatusCode != 200 {
		t.Fatal("not ready before drain")
	}
	done := make(chan []map[string]any, 1)
	go func() { done <- e.pull("q", "w", map[string]any{"wait": "30s"}) }()
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	e.srv.BeginDrain()
	select {
	case jobs := <-done:
		if len(jobs) != 0 || time.Since(start) > 2*time.Second {
			t.Fatalf("jobs=%d after %v", len(jobs), time.Since(start))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not release the long poll; shutdown would stall for 30s")
	}
	r := e.call("GET", "/readyz", "", nil)
	if r.StatusCode != 503 || r.json()["status"] != "draining" {
		t.Fatalf("readyz while draining: %d %s", r.StatusCode, r.body)
	}
	e.want(e.call("GET", "/healthz", "", nil), 200) // still alive, just not accepting new traffic
}

func TestLoadSheddingWhenTooManyPollers(t *testing.T) {
	e := newEnv(t, func(o *Options, _ *broker.Config) { o.MaxPullers = 1 })
	e.queue("q", nil)
	held := make(chan struct{})
	go func() { e.pull("q", "w1", map[string]any{"wait": "3s"}); close(held) }()
	time.Sleep(150 * time.Millisecond)
	r := e.want(e.call("POST", "/v1/queues/q/pull", workKey, map[string]any{"worker": "w2"}), 503)
	if r.errCode() != "overloaded" || r.Header.Get("Retry-After") == "" {
		t.Fatalf("shed response: %s %v", r.body, r.Header)
	}
	e.srv.BeginDrain()
	<-held
}

func TestQueueAdministration(t *testing.T) {
	e := newEnv(t)
	e.queue("orders", map[string]any{"partitions": 3, "default_visibility": "45s"})
	e.want(e.call("POST", "/v1/queues", adminKey, map[string]any{"name": "orders"}), 409)
	e.want(e.call("POST", "/v1/queues", adminKey, map[string]any{"name": "Bad Name"}), 400)
	e.want(e.call("POST", "/v1/queues", adminKey, map[string]any{"name": "x", "partitions": 9999}), 400)
	e.want(e.call("POST", "/v1/queues", adminKey, map[string]any{"name": "x", "default_visibility": 30}), 400) // numbers are ambiguous

	list := e.want(e.call("GET", "/v1/queues", adminKey, nil), 200).json()["queues"].([]any)
	if len(list) != 1 {
		t.Fatalf("list: %v", list)
	}
	e.want(e.submit(prodA, jobBody("orders")), 201)
	st := e.want(e.call("GET", "/v1/queues/orders", adminKey, nil), 200).json()
	total := st["total"].(map[string]any)
	if total["ready"].(float64) != 1 || total["enqueued"].(float64) != 1 || len(st["partitions"].([]any)) != 3 {
		t.Fatalf("stats: %v", st)
	}
	if st["config"].(map[string]any)["default_visibility"] != "45s" {
		t.Fatalf("config: %v", st["config"])
	}
	e.want(e.call("DELETE", "/v1/queues/orders", adminKey, nil), 204)
	e.want(e.call("GET", "/v1/queues/orders", adminKey, nil), 404)
	if r := e.submit(prodA, jobBody("orders")); r.StatusCode != 404 || r.errCode() != "queue_not_found" {
		t.Fatalf("submit to deleted queue: %d %s", r.StatusCode, r.body)
	}
}

func TestMetricsExposition(t *testing.T) {
	e := newEnv(t)
	e.queue("m", map[string]any{"partitions": 2})
	e.want(e.submit(prodA, jobBody("m")), 201)
	e.want(e.submit(prodA, map[string]any{"queue": "m"}), 400)
	e.pull("m", "w", nil)

	res, err := http.Get(e.admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := string(raw)
	for _, want := range []string{
		`http_requests_total{route="submit_job",method="POST",code="201"} 1`,
		`http_requests_total{route="submit_job",method="POST",code="400"} 1`,
		`http_request_duration_seconds_bucket{route="submit_job",le="+Inf"} 2`,
		`jobqueue_pull_requests_total{queue="m",result="jobs"} 1`,
		`jobqueue_queue_delay_seconds_count{queue="m"} 1`,
		`jobqueue_partition_jobs{queue="m",partition="0",state="in_flight"}`,
		`jobqueue_enqueued_total{queue="m",partition=`,
		`jobqueue_leased_total{queue="m",partition=`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	// Metrics must not be served on the public listener.
	if r := e.call("GET", "/metrics", adminKey, nil); r.StatusCode != 404 {
		t.Errorf("/metrics on public port = %d", r.StatusCode)
	}
}

func TestLogsNeverContainSecretsOrPayloads(t *testing.T) {
	e := newEnv(t)
	e.queue("q", nil)
	b := jobBody("q")
	b["payload"] = map[string]any{"card_number": "4111-1111-SECRET-PAYLOAD"}
	id := e.want(e.submit(prodA, b), 201).json()["job_id"].(string)
	e.pull("q", "w", nil)
	e.want(e.call("POST", "/v1/jobs/"+id+"/fail", workKey, map[string]any{"attempt": 1, "error": "x", "retryable": false}), 200)
	e.call("GET", "/v1/jobs/"+id, "wrong-key-0123456789", nil)

	logs := e.logs.String()
	for _, secret := range []string{"SECRET-PAYLOAD", prodA, adminKey, workKey, "wrong-key-0123456789"} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs leaked %q", secret)
		}
	}
	for _, want := range []string{`"audit":true`, `"action":"create_queue"`, `"action":"auth_failed"`,
		`"request_id"`, `"route":"submit_job"`, `"tenant":"a"`, `"job_id"`, `"queue":"q"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %s", want)
		}
	}
}

func TestTraceparentIsLogged(t *testing.T) {
	e := newEnv(t)
	e.call("GET", "/healthz", "", nil, "traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if !strings.Contains(e.logs.String(), `"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`) {
		t.Errorf("trace id not logged:\n%s", e.logs.String())
	}
}

func TestPanicRecovery(t *testing.T) {
	e := newEnv(t)
	h := e.srv.outer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("panic response: %d %s (must be 500 without internals)", rec.Code, rec.Body)
	}
	if !strings.Contains(e.logs.String(), "panic in handler") {
		t.Error("panic not logged")
	}
}

func TestJobsSurviveServerRestart(t *testing.T) {
	dir := t.TempDir()
	withDir := func(o *Options, c *broker.Config) { c.DataDir = dir }
	e := newEnv(t, withDir)
	e.queue("durable", map[string]any{"partitions": 2})
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, e.want(e.submit(prodA, jobBody("durable"), "Idempotency-Key", fmt.Sprint("k", i)), 201).json()["job_id"].(string))
	}
	e.ts.Close()
	e.admin.Close()
	if err := e.b.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := newEnv(t, withDir)
	got := e2.pull("durable", "w", nil)
	if len(got) != 5 {
		t.Fatalf("recovered %d of 5 jobs", len(got))
	}
	// A client retrying across the restart still gets deduplicated.
	r := e2.want(e2.submit(prodA, jobBody("durable"), "Idempotency-Key", "k0"), 200).json()
	if r["duplicate"] != true {
		t.Fatalf("idempotency lost across restart: %v", r)
	}
	_ = ids
}
