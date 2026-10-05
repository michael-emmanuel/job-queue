// Command worker runs a pool of job handlers against the HTTP API.
//
// The built-in "demo" handler is driven by each job's payload so scenarios
// can be scripted without writing code:
//
//	{"sleep_ms": 200}                 pretend to work for 200ms
//	{"fail": true}                    return a retryable error
//	{"fail_until_attempt": 3}         fail until the 3rd delivery
//	{"poison": true}                  return a non-retryable error (DLQ)
//	{"effect": "invoice-42"}          perform an IDEMPOTENT side effect
//	{"crash_after_effect": true}      exit abruptly after the effect, before the ack
//
// Real deployments embed pkg/client's Worker in their own binary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr, nil); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type demoPayload struct {
	SleepMS          int    `json:"sleep_ms"`
	Fail             bool   `json:"fail"`
	FailUntilAttempt int    `json:"fail_until_attempt"`
	Poison           bool   `json:"poison"`
	Effect           string `json:"effect"`
	CrashAfterEffect bool   `json:"crash_after_effect"`
}

var safeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

type demo struct {
	log        *slog.Logger
	effectsDir string
	allowCrash bool
	exit       func(int)

	logAttempts bool
	mu          sync.Mutex
	retryTimes  []time.Time // arrival times of second deliveries, for the retry histogram
}

// retryHistogram prints when second deliveries arrived, in 100ms buckets
// relative to the first one. Retries bunched into one bucket are a retry
// storm; retries spread across buckets are what jitter is for.
func (d *demo) retryHistogram(w io.Writer) {
	d.mu.Lock()
	ts := append([]time.Time(nil), d.retryTimes...)
	d.mu.Unlock()
	if len(ts) == 0 {
		return
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	first := ts[0]
	buckets := map[int]int{}
	for _, t := range ts {
		buckets[int(t.Sub(first)/(100*time.Millisecond))]++
	}
	span := ts[len(ts)-1].Sub(first)
	// The central 90% (5th to 95th percentile) is robust to a few early or
	// late stragglers, unlike the raw span.
	core := ts[len(ts)*95/100-1].Sub(ts[len(ts)*5/100])
	fmt.Fprintf(w, "RETRY ARRIVALS: %d second deliveries over %dms; 90%% of them within %dms\n", len(ts), span.Milliseconds(), core.Milliseconds())
	keys := make([]int, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "  +%4dms %s %d\n", k*100, strings.Repeat("#", min(buckets[k], 60)), buckets[k])
	}
}

// applyEffect performs a side effect exactly once per key, however many times
// the job is delivered: the file is created with O_EXCL, which fails if it
// already exists. This is how at-least-once delivery plus an idempotent
// handler produces exactly-once EFFECTS.
func (d *demo) applyEffect(key, jobID string, attempt int) error {
	if d.effectsDir == "" {
		return errors.New("effect requested but -effects-dir is not set")
	}
	path := filepath.Join(d.effectsDir, safeName.ReplaceAllString(key, "_"))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	switch {
	case err == nil:
		fmt.Fprintf(f, "applied by job %s attempt %d\n", jobID, attempt)
		f.Close()
		d.log.Info("EFFECT APPLIED", "effect", key, "job_id", jobID, "attempt", attempt)
	case errors.Is(err, os.ErrExist):
		d.log.Info("DUPLICATE DELIVERY DETECTED: effect already applied, skipping", "effect", key, "job_id", jobID, "attempt", attempt)
	default:
		return err
	}
	return nil
}

func (d *demo) handle(ctx context.Context, j client.Job) error {
	if d.logAttempts {
		d.log.Info("attempt", "job_id", j.ID, "attempt", j.Attempt)
	}
	if j.Attempt == 2 {
		d.mu.Lock()
		d.retryTimes = append(d.retryTimes, time.Now())
		d.mu.Unlock()
	}
	var p demoPayload
	if len(j.Payload) > 0 {
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return client.NonRetryable(fmt.Errorf("malformed payload: %w", err))
		}
	}
	if p.Poison {
		return client.NonRetryable(errors.New("poison message"))
	}
	if p.SleepMS > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.Fail || j.Attempt < p.FailUntilAttempt {
		return fmt.Errorf("simulated failure on attempt %d", j.Attempt)
	}
	if p.Effect != "" {
		if err := d.applyEffect(p.Effect, j.ID, j.Attempt); err != nil {
			return err
		}
	}
	if p.CrashAfterEffect && d.allowCrash {
		d.log.Warn("CRASHING after the side effect and before the ack", "job_id", j.ID, "attempt", j.Attempt)
		d.exit(137)
	}
	return nil
}

func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer, ready func(admin string)) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	api := fs.String("api", "http://127.0.0.1:8080", "API base URL")
	queue := fs.String("queue", "", "queue to consume (required)")
	group := fs.String("group", "", "consumer group (cluster mode)")
	host, _ := os.Hostname()
	name := fs.String("name", host, "worker name")
	conc := fs.Int("concurrency", 4, "simultaneous jobs")
	vis := fs.Duration("visibility", 30*time.Second, "lease length")
	poll := fs.Duration("long-poll", 20*time.Second, "long-poll wait")
	grace := fs.Duration("shutdown-grace", 30*time.Second, "time to let running jobs finish on shutdown")
	admin := fs.String("admin-listen", "", "address for /metrics and /healthz (empty = disabled)")
	effects := fs.String("effects-dir", "", "directory for idempotent side effects (demo handler)")
	allowCrash := fs.Bool("allow-crash", false, "let payloads crash the process (fault injection)")
	logAttempts := fs.Bool("log-attempts", false, "log every delivery attempt")
	reportRetries := fs.Bool("report-retries", false, "on exit, print a histogram of when second deliveries arrived")
	level := fs.String("log-level", "info", "log level")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token := getenv("JOBQUEUE_TOKEN")
	if f := getenv("JOBQUEUE_TOKEN_FILE"); f != "" && token == "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		token = strings.TrimSpace(string(b))
	}
	switch {
	case *queue == "":
		return errors.New("-queue is required")
	case token == "":
		return errors.New("set JOBQUEUE_TOKEN (or JOBQUEUE_TOKEN_FILE) to a worker API key")
	}
	if *effects != "" {
		if err := os.MkdirAll(*effects, 0o755); err != nil {
			return err
		}
	}
	logger := observability.NewLogger(stderr, *level, true).With("worker", *name)
	d := &demo{log: logger, effectsDir: *effects, allowCrash: *allowCrash, exit: os.Exit, logAttempts: *logAttempts}
	w := client.NewWorker(client.WorkerConfig{
		Client: client.New(*api, token), Queue: *queue, Group: *group, Name: *name, Concurrency: *conc,
		Visibility: *vis, LongPoll: *poll, ShutdownGrace: *grace, Logger: logger,
	}, d.handle)

	var srv *http.Server
	if *admin != "" {
		reg := observability.NewRegistry()
		reg.AddCollector(func(o *observability.Writer) {
			s := w.Stats()
			l := []string{"worker", *name, "queue", *queue}
			o.Gauge("jobqueue_worker_active_jobs", "Jobs being processed now.", float64(s.Active), l...)
			o.Gauge("jobqueue_worker_concurrency", "Configured concurrency.", float64(*conc), l...)
			o.Gauge("jobqueue_worker_utilization", "Handler busy time over concurrency x uptime.", s.Utilization, l...)
			o.Counter("jobqueue_worker_succeeded_total", "Jobs acknowledged.", float64(s.Succeeded), l...)
			o.Counter("jobqueue_worker_failed_total", "Handler failures reported.", float64(s.Failed), l...)
			o.Counter("jobqueue_worker_lease_lost_total", "Jobs abandoned after losing the lease.", float64(s.LeaseLost), l...)
			o.Counter("jobqueue_worker_renewals_total", "Lease renewals sent.", float64(s.Renewals), l...)
			o.Counter("jobqueue_worker_pull_errors_total", "Failed pulls.", float64(s.PullErrors), l...)
		})
		reg.AddCollector(observability.RuntimeCollector(""))
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", reg.Handler())
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
		srv = &http.Server{Addr: *admin, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		ln, err := listen(*admin)
		if err != nil {
			return err
		}
		go srv.Serve(ln)
		if ready != nil {
			ready(ln.Addr().String())
		}
	}
	logger.Info("worker started", "queue", *queue, "group", *group, "concurrency", *conc)
	err := w.Run(ctx) // returns after the graceful drain
	if srv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}
	if *reportRetries {
		d.retryHistogram(os.Stdout)
	}
	s := w.Stats()
	logger.Info("worker stopped", "succeeded", s.Succeeded, "failed", s.Failed, "lease_lost", s.LeaseLost)
	return err
}
