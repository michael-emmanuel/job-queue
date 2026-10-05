// Package loadgen is an open-loop load generator for the job queue.
//
// Open loop means requests are issued on a fixed schedule regardless of how
// fast the server answers, and each request's latency is measured from the
// moment it WAS SUPPOSED to be sent, not from when a free client got around
// to sending it. A closed-loop tool (send, wait, send) quietly slows down
// when the server does, so it never records the long waits real arrivals
// would have suffered: "coordinated omission". Reported tail latencies from
// such tools are far too optimistic exactly when it matters.
package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config describes a run.
type Config struct {
	API   string
	Token string // producer/worker key (or admin)
	Queue string

	Rates         []float64 // target submissions/second, one stage each
	StageDuration time.Duration
	PayloadBytes  int
	Concurrency   int  // max in-flight submissions
	Idempotency   bool // attach a unique idempotency key to every job (realistic; costs dedupe memory)

	Consumers      int           // embedded consumers; 0 = produce only
	ConsumerBatch  int           // jobs per pull (default 50)
	Group          string        // consumer group, cluster mode
	DrainTimeout   time.Duration // how long to wait for the queue to empty after each stage
	AdminToken     string        // for queue depth sampling (optional)
	MetricsURLs    []string      // /metrics endpoints to sample for CPU, memory, worker utilization
	SampleInterval time.Duration // default 500ms
}

// Environment records where a result came from; numbers without it are not
// reproducible and should not be trusted.
type Environment struct {
	GoVersion  string `json:"go_version"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	NumCPU     int    `json:"num_cpu"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	StartedAt  string `json:"started_at"`
}

// Percentiles in milliseconds.
type Percentiles struct {
	P50, P95, P99, Max float64
	Count              int
}

// Resource is what the system under test consumed during a stage.
type Resource struct {
	Target      string  `json:"target"`
	CPUCores    float64 `json:"cpu_cores"`      // average cores used (CPU seconds / wall seconds)
	MaxAllocMB  float64 `json:"max_alloc_mb"`   // peak Go heap in use
	Utilization float64 `json:"worker_util"`    // mean jobqueue_worker_utilization, if exposed
	Goroutines  float64 `json:"max_goroutines"` // peak
}

// StageResult is the outcome of one rate stage.
type StageResult struct {
	TargetRate   float64     `json:"target_rate"`
	DurationSec  float64     `json:"duration_sec"`
	Intended     int         `json:"intended"`      // requests the schedule called for
	Sent         int         `json:"sent"`          // requests actually issued
	OK           int         `json:"ok"`            // accepted (201/200)
	Rejected     int         `json:"rejected"`      // 429
	Unavailable  int         `json:"unavailable"`   // 503/504
	OtherErrors  int         `json:"other_errors"`  // transport errors and unexpected statuses
	NotSent      int         `json:"not_sent"`      // intended but never issued before the stage ended (generator saturated)
	AchievedRate float64     `json:"achieved_rate"` // OK per second
	Submit       Percentiles `json:"submit_ms"`     // from intended send time to response
	EndToEnd     Percentiles `json:"end_to_end_ms"` // from intended send time to acknowledgement by a consumer
	Acked        int         `json:"acked"`
	MaxDepth     int         `json:"max_queue_depth"`
	DrainSec     float64     `json:"drain_sec"`
	Drained      bool        `json:"drained"`
	Resources    []Resource  `json:"resources,omitempty"`
}

// Result is a complete run.
type Result struct {
	Env    Environment    `json:"environment"`
	Config map[string]any `json:"config"`
	Stages []StageResult  `json:"stages"`
}

// Percentile returns the p-th percentile (0-100) of sorted using the
// nearest-rank method.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p/100*float64(len(sorted)) + 0.999999999)
	return sorted[min(max(rank, 1), len(sorted))-1]
}

func summarize(samples []float64) Percentiles {
	if len(samples) == 0 {
		return Percentiles{}
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	return Percentiles{P50: Percentile(s, 50), P95: Percentile(s, 95), P99: Percentile(s, 99), Max: s[len(s)-1], Count: len(s)}
}

type runner struct {
	cfg    Config
	http   *http.Client
	pad    string
	seq    atomic.Uint64
	runID  string
	submit *recorder
	e2e    *recorder
	acked  atomic.Int64
}

type recorder struct {
	mu sync.Mutex
	v  []float64
}

func (r *recorder) add(ms float64) { r.mu.Lock(); r.v = append(r.v, ms); r.mu.Unlock() }
func (r *recorder) take() []float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.v
	r.v = nil
	return v
}

// Run executes every stage in order and returns the results.
func Run(ctx context.Context, cfg Config, progress io.Writer) (Result, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 256
	}
	if cfg.ConsumerBatch <= 0 {
		cfg.ConsumerBatch = 50
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 500 * time.Millisecond
	}
	if cfg.StageDuration <= 0 {
		cfg.StageDuration = 10 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 60 * time.Second
	}
	if progress == nil {
		progress = io.Discard
	}
	r := &runner{cfg: cfg, runID: strconv.FormatInt(time.Now().UnixNano(), 36), submit: &recorder{}, e2e: &recorder{}}
	r.pad = strings.Repeat("x", max(cfg.PayloadBytes, 0))
	r.http = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        cfg.Concurrency + cfg.Consumers*20 + 64,
		MaxIdleConnsPerHost: cfg.Concurrency + cfg.Consumers*20 + 64,
		IdleConnTimeout:     90 * time.Second,
	}}
	res := Result{
		Env: Environment{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(),
			GOMAXPROCS: runtime.GOMAXPROCS(0), StartedAt: time.Now().UTC().Format(time.RFC3339)},
		Config: map[string]any{"queue": cfg.Queue, "rates": cfg.Rates, "stage_duration": cfg.StageDuration.String(),
			"payload_bytes": cfg.PayloadBytes, "concurrency": cfg.Concurrency, "consumers": cfg.Consumers,
			"idempotency_keys": cfg.Idempotency, "consumer_batch": cfg.ConsumerBatch},
	}

	cctx, stopConsumers := context.WithCancel(ctx)
	var cwg sync.WaitGroup
	for i := 0; i < cfg.Consumers; i++ {
		cwg.Add(1)
		go func(i int) { defer cwg.Done(); r.consume(cctx, fmt.Sprintf("lt-%s-%d", r.runID, i)) }(i)
	}
	defer func() { stopConsumers(); cwg.Wait() }()

	for _, rate := range cfg.Rates {
		if ctx.Err() != nil {
			break
		}
		fmt.Fprintf(progress, "stage: %.0f jobs/s for %v ...\n", rate, cfg.StageDuration)
		st := r.stage(ctx, rate)
		res.Stages = append(res.Stages, st)
		fmt.Fprintf(progress, "  achieved %.0f/s  ok=%d 429=%d 5xx=%d err=%d not_sent=%d  submit p50/p95/p99 = %.1f/%.1f/%.1f ms  e2e p99 = %.1f ms  drained=%v\n",
			st.AchievedRate, st.OK, st.Rejected, st.Unavailable, st.OtherErrors, st.NotSent,
			st.Submit.P50, st.Submit.P95, st.Submit.P99, st.EndToEnd.P99, st.Drained)
	}
	return res, ctx.Err()
}

func (r *runner) stage(ctx context.Context, rate float64) StageResult {
	st := StageResult{TargetRate: rate}
	r.submit.take()
	r.e2e.take()
	ackedBefore := r.acked.Load()

	sampler := r.startSampler(ctx)
	stageStart := time.Now()
	total := int(rate * r.cfg.StageDuration.Seconds())
	st.Intended = total

	type counters struct{ ok, rej, unav, other, skipped atomic.Int64 }
	var c counters
	var over atomic.Bool // set when the stage's time is up
	work := make(chan time.Time, min(total+1, 2_000_000))
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for intended := range work {
				if over.Load() {
					// The stage has ended. A request still queued now was never
					// issued in time; sending it late would stretch the stage
					// and inflate the counts, so discard and report it.
					c.skipped.Add(1)
					continue
				}
				code, err := r.submitOne(ctx, intended)
				ms := float64(time.Since(intended).Microseconds()) / 1000
				switch {
				case err != nil:
					c.other.Add(1)
				case code == 201 || code == 200:
					c.ok.Add(1)
					r.submit.add(ms)
				case code == 429:
					c.rej.Add(1)
				case code == 503 || code == 504:
					c.unav.Add(1)
				default:
					c.other.Add(1)
				}
			}
		}()
	}

	// Dispatcher: emit the intended send time of request k at start + k/rate.
	tick := time.NewTicker(time.Millisecond)
	emitted := 0
dispatch:
	for emitted < total {
		select {
		case <-ctx.Done():
			break dispatch
		case now := <-tick.C:
			due := min(int(now.Sub(stageStart).Seconds()*rate)+1, total) // request k is due at k/rate, so by time t exactly floor(t*rate)+1 are due
			for ; emitted < due; emitted++ {
				work <- stageStart.Add(time.Duration(float64(emitted) / rate * float64(time.Second)))
			}
		}
	}
	tick.Stop()
	// Requests still queued when the schedule ends were never issued before
	// the deadline: the generator, not the server, was the bottleneck.
	deadline := stageStart.Add(r.cfg.StageDuration + 2*time.Second)
	for len(work) > 0 && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if len(work) > 0 { // only when time ran out with work still queued; a normal drain must not discard anything
		over.Store(true)
	}
	close(work)
	wg.Wait()
	st.NotSent = int(c.skipped.Load())
	st.Sent = int(c.ok.Load() + c.rej.Load() + c.unav.Load() + c.other.Load())
	st.OK, st.Rejected, st.Unavailable, st.OtherErrors = int(c.ok.Load()), int(c.rej.Load()), int(c.unav.Load()), int(c.other.Load())
	st.DurationSec = r.cfg.StageDuration.Seconds()
	st.AchievedRate = float64(st.OK) / r.cfg.StageDuration.Seconds()
	st.Submit = summarize(r.submit.take())

	if r.cfg.Consumers > 0 {
		drainStart := time.Now()
		st.Drained = r.waitDrained(ctx, st.OK, ackedBefore)
		st.DrainSec = time.Since(drainStart).Seconds()
		st.Acked = int(r.acked.Load() - ackedBefore)
	}
	st.EndToEnd = summarize(r.e2e.take())
	st.MaxDepth, st.Resources = sampler.stop()
	return st
}

func (r *runner) submitOne(ctx context.Context, intended time.Time) (int, error) {
	n := r.seq.Add(1)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, `{"queue":%q,"type":"load","payload":{"t":%d,"pad":%q}`, r.cfg.Queue, intended.UnixNano(), r.pad)
	if r.cfg.Idempotency {
		fmt.Fprintf(&buf, `,"idempotency_key":"lt-%s-%d"`, r.runID, n)
	}
	buf.WriteByte('}')
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.API+"/v1/jobs", &buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.cfg.Token)
	res, err := r.http.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return res.StatusCode, nil
}

// consume pulls and acknowledges, recording submit-to-ack latency.
func (r *runner) consume(ctx context.Context, name string) {
	acks := make(chan struct{}, 32)
	for ctx.Err() == nil {
		body := fmt.Sprintf(`{"worker":%q,"max":%d,"visibility":"60s","wait":"1s"`, name, r.cfg.ConsumerBatch)
		if r.cfg.Group != "" {
			body += fmt.Sprintf(`,"group":%q`, r.cfg.Group)
		}
		body += "}"
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.API+"/v1/queues/"+r.cfg.Queue+"/pull", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+r.cfg.Token)
		res, err := r.http.Do(req)
		if err != nil {
			if ctx.Err() == nil {
				time.Sleep(50 * time.Millisecond)
			}
			continue
		}
		var out struct {
			Jobs []struct {
				ID      string `json:"job_id"`
				Attempt int    `json:"attempt"`
				Payload struct {
					T int64 `json:"t"`
				} `json:"payload"`
			} `json:"jobs"`
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		json.Unmarshal(raw, &out)
		var wg sync.WaitGroup
		for _, j := range out.Jobs {
			acks <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-acks }()
				a, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.API+"/v1/jobs/"+j.ID+"/ack",
					strings.NewReader(fmt.Sprintf(`{"attempt":%d}`, j.Attempt)))
				a.Header.Set("Content-Type", "application/json")
				a.Header.Set("Authorization", "Bearer "+r.cfg.Token)
				ar, err := r.http.Do(a)
				if err != nil {
					return
				}
				io.Copy(io.Discard, ar.Body)
				ar.Body.Close()
				if ar.StatusCode == 204 {
					r.acked.Add(1)
					if j.Payload.T > 0 {
						r.e2e.add(float64(time.Now().UnixNano()-j.Payload.T) / 1e6)
					}
				}
			}()
		}
		wg.Wait()
	}
}

func (r *runner) waitDrained(ctx context.Context, want int, ackedBefore int64) bool {
	deadline := time.Now().Add(r.cfg.DrainTimeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if int(r.acked.Load()-ackedBefore) >= want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// ---- resource sampling ----

type sample struct {
	cpu, alloc, gor, util, nutil float64
	at                           time.Time // when the scrape was taken
}

type sampler struct {
	r      *runner
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	first  map[string]sample
	peak   map[string]sample
	depth  int
}

func (r *runner) startSampler(ctx context.Context) *sampler {
	sctx, cancel := context.WithCancel(ctx)
	s := &sampler{r: r, cancel: cancel, done: make(chan struct{}), first: map[string]sample{}, peak: map[string]sample{}}
	s.scrape(sctx)
	go func() {
		defer close(s.done)
		t := time.NewTicker(r.cfg.SampleInterval)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				s.scrape(sctx)
			}
		}
	}()
	return s
}

func (s *sampler) scrape(ctx context.Context) {
	cfg := s.r.cfg
	if cfg.AdminToken != "" {
		if d, ok := s.r.depth(ctx); ok {
			s.mu.Lock()
			s.depth = max(s.depth, d)
			s.mu.Unlock()
		}
	}
	for _, u := range cfg.MetricsURLs {
		m, err := ScrapeMetrics(ctx, s.r.http, u)
		if err != nil {
			continue
		}
		s.mu.Lock()
		if _, ok := s.first[u]; !ok {
			s.first[u] = m
		}
		p := s.peak[u]
		p.alloc, p.gor = max(p.alloc, m.alloc), max(p.gor, m.gor)
		p.cpu, p.util, p.nutil, p.at = m.cpu, m.util, m.nutil, m.at // latest values
		s.peak[u] = p
		s.mu.Unlock()
	}
}

func (s *sampler) stop() (int, []Resource) {
	s.scrape(context.Background())
	s.cancel()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Resource
	for _, u := range s.r.cfg.MetricsURLs {
		f, ok := s.first[u]
		if !ok {
			continue
		}
		p := s.peak[u]
		// CPU is averaged over the interval actually sampled (first to last
		// scrape), which includes the drain, so it is a whole-stage figure.
		cores := 0.0
		if span := p.at.Sub(f.at).Seconds(); span > 0 {
			cores = (p.cpu - f.cpu) / span
		}
		res := Resource{Target: u, CPUCores: cores, MaxAllocMB: p.alloc / (1 << 20), Goroutines: p.gor}
		if p.nutil > 0 {
			res.Utilization = p.util / p.nutil
		}
		out = append(out, res)
	}
	return s.depth, out
}

func (r *runner) depth(ctx context.Context) (int, bool) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.API+"/v1/queues/"+r.cfg.Queue, nil)
	req.Header.Set("Authorization", "Bearer "+r.cfg.AdminToken)
	res, err := r.http.Do(req)
	if err != nil {
		return 0, false
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var generic struct {
		Total map[string]float64 `json:"total"`
	}
	if res.StatusCode != 200 || json.Unmarshal(raw, &generic) != nil {
		return 0, false
	}
	t := generic.Total
	return int(t["ready"] + t["scheduled"] + t["in_flight"]), true
}

// ScrapeMetrics fetches a Prometheus endpoint and extracts the few series the
// load report needs: process CPU seconds, Go heap in use, goroutines and mean
// worker utilization.
func ScrapeMetrics(ctx context.Context, hc *http.Client, url string) (sample, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	res, err := hc.Do(req)
	if err != nil {
		return sample{}, err
	}
	defer res.Body.Close()
	m := parseMetrics(res.Body)
	m.at = time.Now()
	return m, nil
}

func parseMetrics(r io.Reader) sample {
	var s sample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			continue
		}
		switch {
		case name == "process_cpu_seconds_total":
			s.cpu = v
		case name == "go_memstats_alloc_bytes":
			s.alloc = v
		case name == "go_goroutines":
			s.gor = v
		case strings.HasPrefix(name, "jobqueue_worker_utilization"):
			s.util += v
			s.nutil++
		}
	}
	return s
}
