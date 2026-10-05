package observability

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b bytes.Buffer
	if err := r.Expose(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestCounterGaugeAndLabels(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("http_requests_total", "Requests.", "route", "code")
	c.With("submit", "201").Inc()
	c.With("submit", "201").Add(2)
	c.With("pull", "200").Inc()
	g := r.NewGaugeVec("inflight", "In flight.", "class")
	g.With("api").Add(5)
	g.With("api").Add(-2)

	out := render(t, r)
	for _, want := range []string{
		"# TYPE http_requests_total counter",
		`http_requests_total{route="submit",code="201"} 3`,
		`http_requests_total{route="pull",code="200"} 1`,
		"# TYPE inflight gauge",
		`inflight{class="api"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Count(out, "# HELP http_requests_total") != 1 {
		t.Error("HELP/TYPE must appear once per family")
	}
}

func TestHistogramIsCumulative(t *testing.T) {
	r := NewRegistry()
	h := r.NewHistogramVec("lat_seconds", "Latency.", []float64{0.1, 1}, "route")
	for _, v := range []float64{0.05, 0.05, 0.5, 5} {
		h.With("x").Observe(v)
	}
	out := render(t, r)
	for _, want := range []string{
		`lat_seconds_bucket{route="x",le="0.1"} 2`,
		`lat_seconds_bucket{route="x",le="1"} 3`,
		`lat_seconds_bucket{route="x",le="+Inf"} 4`,
		`lat_seconds_sum{route="x"} 5.6`,
		`lat_seconds_count{route="x"} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	r := NewRegistry()
	r.NewCounterVec("c_total", "c", "v").With("a\"b\\c\nd").Inc()
	if out := render(t, r); !strings.Contains(out, `c_total{v="a\"b\\c\nd"} 1`) {
		t.Errorf("label not escaped:\n%s", out)
	}
}

func TestCollectorsGroupSamplesUnderOneFamily(t *testing.T) {
	r := NewRegistry()
	r.AddCollector(func(w *Writer) {
		w.Gauge("queue_depth", "Depth.", 3, "queue", "a", "partition", "0")
		w.Gauge("queue_depth", "Depth.", 9, "queue", "a", "partition", "1")
	})
	out := render(t, r)
	if strings.Count(out, "# TYPE queue_depth") != 1 || !strings.Contains(out, `queue_depth{queue="a",partition="1"} 9`) {
		t.Errorf("bad exposition:\n%s", out)
	}
}

func TestConcurrentUpdatesAreSafe(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("n_total", "n", "k")
	h := r.NewHistogramVec("h", "h", DefBuckets, "k")
	g := r.NewGaugeVec("g", "g")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.With("a").Inc()
				h.With("a").Observe(0.01)
				g.With().Add(1)
				if j%100 == 0 {
					render(t, r)
				}
			}
		}()
	}
	wg.Wait()
	if c.With("a").Value() != 8000 || g.With().Value() != 8000 {
		t.Fatalf("lost updates: %d %v", c.With("a").Value(), g.With().Value())
	}
}

func TestRuntimeCollectorEmitsCoreMetrics(t *testing.T) {
	r := NewRegistry()
	r.AddCollector(RuntimeCollector(t.TempDir()))
	out := render(t, r)
	for _, want := range []string{"go_goroutines", "go_gc_cycles_total", "go_memstats_alloc_bytes", "data_disk_free_bytes"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
}

func TestRequestIDValidation(t *testing.T) {
	good := []string{"abc-123", "A_b.c", strings.Repeat("a", 64)}
	bad := []string{"", "has space", "new\nline", "quote\"", strings.Repeat("a", 65), "ünï"}
	for _, s := range good {
		if !ValidRequestID(s) {
			t.Errorf("%q rejected", s)
		}
	}
	for _, s := range bad {
		if ValidRequestID(s) {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestParseTraceparent(t *testing.T) {
	id, ok := ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if !ok || id != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("%q %v", id, ok)
	}
	for _, bad := range []string{"", "garbage", "00-short-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}
