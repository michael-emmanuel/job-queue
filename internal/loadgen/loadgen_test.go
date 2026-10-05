package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/api"
	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
)

func TestPercentileNearestRank(t *testing.T) {
	s := make([]float64, 100)
	for i := range s {
		s[i] = float64(i + 1)
	}
	for p, want := range map[float64]float64{0: 1, 1: 1, 50: 50, 95: 95, 99: 99, 100: 100} {
		if got := Percentile(s, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if Percentile(nil, 99) != 0 || Percentile([]float64{7}, 99) != 7 {
		t.Error("edge cases")
	}
	// A single slow outlier must show at p100 and, with 100 samples, at p99... but not p50.
	s[99] = 10_000
	if Percentile(s, 50) != 50 || Percentile(s, 100) != 10_000 {
		t.Error("outlier handling")
	}
}

func TestParseMetrics(t *testing.T) {
	m := parseMetrics(strings.NewReader(`# HELP x
process_cpu_seconds_total 12.5
go_memstats_alloc_bytes 1048576
go_goroutines 42
jobqueue_worker_utilization{worker="a",queue="q"} 0.5
jobqueue_worker_utilization{worker="b",queue="q"} 0.7
garbage line
`))
	if m.cpu != 12.5 || m.alloc != 1048576 || m.gor != 42 || m.util/m.nutil < 0.59 || m.util/m.nutil > 0.61 {
		t.Fatalf("%+v", m)
	}
}

func TestPacingMatchesTheTargetRate(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(201)
	}))
	defer srv.Close()
	res, err := Run(context.Background(), Config{API: srv.URL, Token: "x", Queue: "q", Rates: []float64{200}, StageDuration: 2 * time.Second, Concurrency: 16}, nil)
	if err != nil {
		t.Fatal(err)
	}
	st := res.Stages[0]
	if st.Intended != 400 || st.OK < 380 || st.OK > 400 || st.NotSent != 0 {
		t.Fatalf("%+v", st)
	}
	if st.AchievedRate < 190 || st.AchievedRate > 201 {
		t.Fatalf("achieved %.1f/s for a 200/s target", st.AchievedRate)
	}
	if res.Env.NumCPU == 0 || res.Env.GoVersion == "" {
		t.Fatal("environment not recorded")
	}
}

// The property that makes the tool trustworthy. The server handles ONE request
// at a time and takes 20ms, so it can sustain 50/s. We offer 100/s for 2s.
// A closed-loop tool would measure ~20ms per request and report a healthy
// system. In reality arrivals queue up: later requests wait behind a growing
// backlog, so their latency from the INTENDED send time must grow far beyond 20ms.
func TestLatencyIsMeasuredFromTheIntendedSendTime(t *testing.T) {
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		time.Sleep(20 * time.Millisecond)
		mu.Unlock()
		w.WriteHeader(201)
	}))
	defer srv.Close()
	res, _ := Run(context.Background(), Config{API: srv.URL, Token: "x", Queue: "q", Rates: []float64{100}, StageDuration: 2 * time.Second, Concurrency: 4}, nil)
	st := res.Stages[0]
	if st.Submit.P50 < 100 || st.Submit.P99 < 500 {
		t.Fatalf("submit p50=%.0fms p99=%.0fms: the overload was hidden (coordinated omission). A server limited to 50/s offered 100/s must show a growing backlog.",
			st.Submit.P50, st.Submit.P99)
	}
	t.Logf("offered 100/s to a 50/s server: p50=%.0fms p99=%.0fms (a closed-loop tool would say ~20ms)", st.Submit.P50, st.Submit.P99)
}

func TestSaturatedGeneratorReportsNotSentInsteadOfLying(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { <-block; w.WriteHeader(201) }))
	defer srv.Close()
	defer close(block)
	done := make(chan Result, 1)
	go func() {
		r, _ := Run(context.Background(), Config{API: srv.URL, Token: "x", Queue: "q", Rates: []float64{200}, StageDuration: time.Second, Concurrency: 2}, nil)
		done <- r
	}()
	time.Sleep(4 * time.Second)
	block <- struct{}{} // release the two in-flight requests so the stage can finish
	block <- struct{}{}
	select {
	case r := <-done:
		st := r.Stages[0]
		if st.NotSent < 150 {
			t.Fatalf("not_sent=%d; with 2 clients stuck, most of the 200 intended requests were never issued and that must be reported", st.NotSent)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not finish")
	}
}

func TestStatusClassification(t *testing.T) {
	var i atomic.Int64
	codes := []int{201, 200, 429, 503, 504, 400}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(codes[int(i.Add(1)-1)%len(codes)])
	}))
	defer srv.Close()
	res, _ := Run(context.Background(), Config{API: srv.URL, Token: "x", Queue: "q", Rates: []float64{60}, StageDuration: time.Second, Concurrency: 8}, nil)
	st := res.Stages[0]
	if st.OK != 20 || st.Rejected != 10 || st.Unavailable != 20 || st.OtherErrors != 10 {
		t.Fatalf("%+v", st)
	}
}

// End to end against the real API and broker: every accepted job is consumed
// and acknowledged, and end-to-end latency is recorded.
func TestEndToEndWithRealBroker(t *testing.T) {
	b, err := broker.Open(broker.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.CreateQueue("load", broker.QueueConfig{Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	auth, _ := api.ParseKeys("admin-key-0123456789:ops:admin")
	reg := observability.NewRegistry()
	reg.AddCollector(observability.RuntimeCollector(""))
	srv := api.New(api.Options{Broker: b, Auth: auth, Logger: observability.Discard(), Metrics: reg, MaxLongPoll: 2 * time.Second})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	adm := httptest.NewServer(srv.AdminHandler(false))
	defer adm.Close()

	res, err := Run(context.Background(), Config{API: ts.URL, Token: "admin-key-0123456789", AdminToken: "admin-key-0123456789",
		Queue: "load", Rates: []float64{100, 300}, StageDuration: time.Second, PayloadBytes: 256, Concurrency: 32,
		Consumers: 3, Idempotency: true, DrainTimeout: 15 * time.Second, MetricsURLs: []string{adm.URL + "/metrics"},
		SampleInterval: 100 * time.Millisecond}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range res.Stages {
		if st.OK != st.Intended || st.Acked != st.OK || !st.Drained {
			t.Fatalf("stage %.0f/s: ok=%d intended=%d acked=%d drained=%v", st.TargetRate, st.OK, st.Intended, st.Acked, st.Drained)
		}
		if st.EndToEnd.Count != st.Acked || st.EndToEnd.P50 <= 0 || st.EndToEnd.P99 < st.EndToEnd.P50 {
			t.Fatalf("end-to-end latency: %+v", st.EndToEnd)
		}
		if len(st.Resources) != 1 || st.Resources[0].MaxAllocMB <= 0 || st.Resources[0].Goroutines <= 0 {
			t.Fatalf("resources not sampled: %+v", st.Resources)
		}
	}
}

// Regression: a request is due at k/rate, so the generator must emit it then.
// An off-by-one that emitted it one period late added 1/rate of fake latency
// to every request (100ms at 10/s), which made low rates look slow and made
// latency appear to fall as load rose.
func TestInstantServerShowsNoGeneratorInducedLatency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(201) }))
	defer srv.Close()
	for _, rate := range []float64{10, 50} {
		res, _ := Run(context.Background(), Config{API: srv.URL, Token: "x", Queue: "q", Rates: []float64{rate}, StageDuration: 2 * time.Second, Concurrency: 8}, nil)
		st := res.Stages[0]
		if st.Submit.P50 > 15 || st.Submit.P99 > 40 {
			t.Fatalf("rate %.0f/s against an instant server: p50=%.1fms p99=%.1fms; the generator is adding its own latency", rate, st.Submit.P50, st.Submit.P99)
		}
	}
}

// CPU must be averaged over the interval that was actually sampled. Dividing
// CPU time accumulated over submit+drain by the submit duration alone
// inflated it and produced per-process figures summing to more than the
// machine has.
func TestCPUIsAveragedOverTheSampledInterval(t *testing.T) {
	var cpu atomic.Int64 // hundredths of a CPU-second, advanced 0.5 core-seconds per wall second
	start := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			cpu.Store(int64(time.Since(start).Seconds() * 0.5 * 100))
			w.Write([]byte("process_cpu_seconds_total " + strconvF(float64(cpu.Load())/100) + "\ngo_memstats_alloc_bytes 1000000\ngo_goroutines 5\n"))
			return
		}
		w.WriteHeader(201)
	}))
	defer srv.Close()
	res, _ := Run(context.Background(), Config{API: srv.URL, Token: "x", Queue: "q", Rates: []float64{50}, StageDuration: 2 * time.Second,
		Concurrency: 4, MetricsURLs: []string{srv.URL + "/metrics"}, SampleInterval: 100 * time.Millisecond}, nil)
	got := res.Stages[0].Resources[0].CPUCores
	if got < 0.4 || got > 0.6 {
		t.Fatalf("cpu = %.2f cores, want about 0.5", got)
	}
}

func strconvF(f float64) string { return strconv.FormatFloat(f, 'f', 2, 64) }
