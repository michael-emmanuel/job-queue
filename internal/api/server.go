// Package api is the HTTP edge: authentication, authorization, request
// validation, rate limiting, load shedding, structured errors, request
// tracing metadata, metrics and access/audit logging. It contains no queue
// logic; it translates between HTTP and the broker.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/ratelimit"
)

// Options configures a Server.
type Options struct {
	// Broker is the single-node queue engine. Set Backend instead to front a
	// cluster; if both are set, Backend wins.
	Broker  *broker.Broker
	Backend Backend
	Auth    *Authenticator
	Logger  *slog.Logger
	Metrics *observability.Registry
	Clock   clock.Clock

	// GlobalRate limits submissions across all tenants (jobs/s, 0 = off).
	GlobalRate  float64
	GlobalBurst int
	// TenantRate limits submissions per tenant (jobs/s, 0 = off). Each
	// tenant gets its own bucket, so one tenant exhausting its budget never
	// consumes another's: that isolation is the point of a per-tenant limit.
	TenantRate  float64
	TenantBurst int

	MaxBodyBytes   int64         // default 1 MiB
	RequestTimeout time.Duration // default 15s; long polls get MaxLongPoll on top
	MaxLongPoll    time.Duration // default 30s
	MaxInflight    int           // concurrent non-poll requests before shedding (default 1024)
	MaxPullers     int           // concurrent long polls before shedding (default 4096)
}

type limitClass int

const (
	classNormal limitClass = iota
	classPull
	classNone
)

type metrics struct {
	requests     *observability.CounterVec
	duration     *observability.HistogramVec
	inflight     *observability.GaugeVec
	rejected     *observability.CounterVec
	pulls        *observability.CounterVec
	queueDelay   *observability.HistogramVec
	authFailures *observability.CounterVec
}

// Server serves the HTTP API.
type Server struct {
	o        Options
	be       Backend
	log      *slog.Logger
	global   *ratelimit.Bucket
	tenants  *ratelimit.Keyed
	sem      [2]chan struct{}
	m        metrics
	drainCtx context.Context
	drain    context.CancelFunc
}

// New builds a Server, filling defaults.
func New(o Options) *Server {
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.Logger == nil {
		o.Logger = observability.Discard()
	}
	if o.Metrics == nil {
		o.Metrics = observability.NewRegistry()
	}
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 1 << 20
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 15 * time.Second
	}
	if o.MaxLongPoll <= 0 {
		o.MaxLongPoll = 30 * time.Second
	}
	if o.MaxInflight <= 0 {
		o.MaxInflight = 1024
	}
	if o.MaxPullers <= 0 {
		o.MaxPullers = 4096
	}
	if o.Backend == nil {
		o.Backend = BrokerBackend(o.Broker)
	}
	s := &Server{o: o, be: o.Backend, log: o.Logger}
	s.global = ratelimit.NewBucket(o.Clock, o.GlobalRate, o.GlobalBurst)
	s.tenants = ratelimit.NewKeyed(func() *ratelimit.Bucket {
		return ratelimit.NewBucket(o.Clock, o.TenantRate, o.TenantBurst)
	})
	s.sem[classNormal] = make(chan struct{}, o.MaxInflight)
	s.sem[classPull] = make(chan struct{}, o.MaxPullers)
	s.drainCtx, s.drain = context.WithCancel(context.Background())

	r := o.Metrics
	s.m = metrics{
		requests:     r.NewCounterVec("http_requests_total", "HTTP requests.", "route", "method", "code"),
		duration:     r.NewHistogramVec("http_request_duration_seconds", "HTTP request latency.", observability.DefBuckets, "route"),
		inflight:     r.NewGaugeVec("http_inflight_requests", "Requests currently being served.", "class"),
		rejected:     r.NewCounterVec("http_rejected_total", "Requests refused to protect the system.", "reason"),
		pulls:        r.NewCounterVec("jobqueue_pull_requests_total", "Worker pull requests by outcome.", "queue", "result"),
		queueDelay:   r.NewHistogramVec("jobqueue_queue_delay_seconds", "Time from enqueue to first delivery.", observability.DefBuckets, "queue"),
		authFailures: r.NewCounterVec("http_auth_failures_total", "Rejected credentials.", "reason"),
	}
	r.AddCollector(o.Backend.Collect)
	return s
}

// BeginDrain starts graceful shutdown: readiness turns red so load balancers
// stop sending traffic, and long polls return empty immediately instead of
// holding the shutdown for up to MaxLongPoll. Call it before
// http.Server.Shutdown, which then waits only for requests that are doing
// real work.
func (s *Server) BeginDrain() { s.drain() }

func (s *Server) draining() bool { return s.drainCtx.Err() != nil }

// ---- request context ----

type infoKey struct{}

// reqInfo is filled in as a request is handled and read by the access log.
// A request is served by one goroutine, so it needs no locking.
type reqInfo struct {
	id, traceID, route   string
	tenant, queue, jobID string
	status, bytesWritten int
	wroteHeader          bool
	rw                   http.ResponseWriter
}

func infoFrom(ctx context.Context) *reqInfo {
	if ri, ok := ctx.Value(infoKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{} // detached: tests calling handlers directly
}

type statusRecorder struct {
	http.ResponseWriter
	ri *reqInfo
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.ri.wroteHeader {
		s.ri.status, s.ri.wroteHeader = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.ri.wroteHeader {
		s.ri.status, s.ri.wroteHeader = http.StatusOK, true
	}
	n, err := s.ResponseWriter.Write(b)
	s.ri.bytesWritten += n
	return n, err
}

// Unwrap lets http.ResponseController reach the real connection.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// ---- middleware ----

// outer is the outermost layer: request ID, trace ID, logger injection,
// panic recovery, metrics and the access log.
func (s *Server) outer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ri := &reqInfo{route: "unmatched", status: http.StatusOK}
		ri.id = r.Header.Get("X-Request-ID")
		if !observability.ValidRequestID(ri.id) {
			ri.id = observability.NewRequestID()
		}
		w.Header().Set("X-Request-ID", ri.id)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		lg := s.log.With("request_id", ri.id)
		if tid, ok := observability.ParseTraceparent(r.Header.Get("traceparent")); ok {
			ri.traceID = tid
			lg = lg.With("trace_id", tid)
		}
		ctx := context.WithValue(observability.WithLogger(r.Context(), lg), infoKey{}, ri)
		rec := &statusRecorder{ResponseWriter: w, ri: ri}

		defer func() {
			if rcv := recover(); rcv != nil {
				if rcv == http.ErrAbortHandler {
					panic(rcv)
				}
				lg.Error("panic in handler", "panic", rcv, "stack", string(debug.Stack()))
				if !ri.wroteHeader {
					s.writeError(rec, r.WithContext(ctx), errors.New("panic"))
				}
			}
			s.finish(r, ri, time.Since(start), lg)
		}()
		next.ServeHTTP(rec, r.WithContext(ctx))
	})
}

func (s *Server) finish(r *http.Request, ri *reqInfo, d time.Duration, lg *slog.Logger) {
	s.m.requests.With(ri.route, r.Method, strconv.Itoa(ri.status)).Inc()
	s.m.duration.With(ri.route).Observe(d.Seconds())
	level := slog.LevelInfo
	switch {
	case ri.status >= 500:
		level = slog.LevelError
	case ri.status >= 400:
		level = slog.LevelWarn
	case ri.route == "healthz" || ri.route == "readyz":
		level = slog.LevelDebug
	}
	attrs := []any{"method", r.Method, "route", ri.route, "status", ri.status,
		"bytes", ri.bytesWritten, "duration_ms", float64(d.Microseconds()) / 1000}
	if ri.tenant != "" {
		attrs = append(attrs, "tenant", ri.tenant)
	}
	if ri.queue != "" {
		attrs = append(attrs, "queue", ri.queue)
	}
	if ri.jobID != "" {
		attrs = append(attrs, "job_id", ri.jobID)
	}
	lg.Log(r.Context(), level, "request", attrs...)
}

type handlerFunc func(w http.ResponseWriter, r *http.Request, p *Principal) error

func (s *Server) acquire(c limitClass) bool {
	select {
	case s.sem[c] <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) release(c limitClass) { <-s.sem[c] }

// handle registers an authenticated, authorised, load-shed, time-bounded route.
func (s *Server) handle(mux *http.ServeMux, pattern, name string, class limitClass, roles []Role, h handlerFunc) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		ri := infoFrom(r.Context())
		ri.route = name
		lg := observability.L(r.Context())

		p, err := s.o.Auth.Authenticate(r)
		if err != nil {
			s.m.authFailures.With("invalid_credentials").Inc()
			lg.Warn("audit", "audit", true, "action", "auth_failed", "route", name, "remote", r.RemoteAddr)
			w.Header().Set("WWW-Authenticate", `Bearer realm="jobqueue"`)
			s.writeError(w, r, newErr(http.StatusUnauthorized, "unauthenticated", "missing or invalid credentials"))
			return
		}
		ri.tenant = p.Tenant
		if len(roles) > 0 && !p.Has(roles...) {
			s.m.authFailures.With("forbidden").Inc()
			lg.Warn("audit", "audit", true, "action", "forbidden", "route", name, "key_id", p.KeyID, "tenant", p.Tenant)
			s.writeError(w, r, newErr(http.StatusForbidden, "forbidden", "this credential may not perform this operation"))
			return
		}

		timeout := s.o.RequestTimeout
		if class != classNone {
			if !s.acquire(class) {
				// Shedding load at the door is cheaper than queueing it:
				// an overloaded server that accepts everything serves nothing.
				s.m.rejected.With("overloaded").Inc()
				e := newErr(http.StatusServiceUnavailable, "overloaded", "too many concurrent requests")
				e.retryAfter = time.Second
				s.writeError(w, r, e)
				return
			}
			g := s.m.inflight.With(map[limitClass]string{classNormal: "normal", classPull: "pull"}[class])
			g.Add(1)
			defer func() { g.Add(-1); s.release(class) }()
		}
		if class == classPull {
			timeout += s.o.MaxLongPoll
		}
		// Per-request write deadline instead of a server-wide WriteTimeout,
		// which would kill legitimate long polls.
		rc := http.NewResponseController(w)
		if err := rc.SetWriteDeadline(time.Now().Add(timeout + 5*time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			lg.Debug("set write deadline", "err", err)
		}
		r.Body = http.MaxBytesReader(w, r.Body, s.o.MaxBodyBytes)
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		r = r.WithContext(ctx)

		if err := h(w, r, p); err != nil {
			s.writeError(w, r, err)
		}
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status, b = http.StatusInternalServerError, []byte(`{"error":{"code":"internal","message":"encoding response"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	w.Write(append(b, '\n'))
}

type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	ae, ok := toAPIError(err)
	if !ok {
		observability.L(r.Context()).Error("internal error", "err", err)
		ae = newErr(http.StatusInternalServerError, "internal", "internal error")
	}
	switch ae.code {
	case "rate_limited", "queue_full":
		s.m.rejected.With(ae.code).Inc()
	}
	if ae.retryAfter > 0 {
		w.Header().Set("Retry-After", retryAfterSeconds(ae.retryAfter))
	}
	var body errorBody
	body.Error.Code, body.Error.Message, body.Error.RequestID = ae.code, ae.msg, infoFrom(r.Context()).id
	s.writeJSON(w, ae.status, body)
}

func (s *Server) audit(r *http.Request, p *Principal, action string, attrs ...any) {
	base := []any{"audit", true, "action", action, "key_id", p.KeyID, "tenant", p.Tenant, "remote", r.RemoteAddr}
	observability.L(r.Context()).Info("audit", append(base, attrs...)...)
}

// ---- routing ----

// Handler returns the public API handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	prod, work, admin := []Role{RoleProducer}, []Role{RoleWorker}, []Role{RoleAdmin}
	both := []Role{RoleProducer, RoleWorker}

	s.handle(mux, "POST /v1/jobs", "submit_job", classNormal, prod, s.submitJob)
	s.handle(mux, "GET /v1/jobs/{id}", "get_job", classNormal, both, s.getJob)
	s.handle(mux, "DELETE /v1/jobs/{id}", "cancel_job", classNormal, prod, s.cancelJob)
	s.handle(mux, "POST /v1/queues/{queue}/pull", "pull", classPull, work, s.pull)
	s.handle(mux, "POST /v1/jobs/{id}/ack", "ack", classNormal, work, s.ack)
	s.handle(mux, "POST /v1/jobs/{id}/fail", "fail", classNormal, work, s.fail)
	s.handle(mux, "POST /v1/jobs/{id}/renew", "renew", classNormal, work, s.renew)

	s.handle(mux, "POST /v1/queues", "create_queue", classNormal, admin, s.createQueue)
	s.handle(mux, "GET /v1/queues", "list_queues", classNormal, admin, s.listQueues)
	s.handle(mux, "GET /v1/queues/{queue}", "get_queue", classNormal, admin, s.getQueue)
	s.handle(mux, "DELETE /v1/queues/{queue}", "delete_queue", classNormal, admin, s.deleteQueue)
	s.handle(mux, "GET /v1/queues/{queue}/dlq", "list_dlq", classNormal, admin, s.listDLQ)
	s.handle(mux, "POST /v1/queues/{queue}/dlq/{id}/replay", "replay_dlq", classNormal, admin, s.replayDLQ)
	s.handle(mux, "DELETE /v1/queues/{queue}/dlq/{id}", "delete_dlq", classNormal, admin, s.deleteDLQ)

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, r, newErr(http.StatusNotFound, "not_found", "no such endpoint"))
	})
	return s.outer(mux)
}

// AdminHandler returns the operations handler (metrics, probes, optional
// pprof). Serve it on a separate, non-public listener: metrics and profiles
// describe internals that clients have no business seeing.
func (s *Server) AdminHandler(enablePprof bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", s.o.Metrics.Handler())
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	if enablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return s.outer(mux)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	infoFrom(r.Context()).route = "healthz"
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is the load-balancer signal: not ready while draining or when any
// partition log has failed. Details are logged, not returned, because the
// endpoint is unauthenticated.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	infoFrom(r.Context()).route = "readyz"
	if s.draining() {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	if err := s.be.Ready(); err != nil {
		observability.L(r.Context()).Error("not ready", "err", err)
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
