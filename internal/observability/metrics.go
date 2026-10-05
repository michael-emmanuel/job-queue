// Package observability provides Prometheus-compatible metrics and
// structured-logging helpers using only the standard library.
//
// Metrics are written by hand in the Prometheus text exposition format
// (version 0.0.4) so the repository has no third-party dependencies. A
// production deployment would likely swap this for the official client
// library; the surface here (counter, gauge, histogram, label vectors) is
// intentionally the same shape.
package observability

import (
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Collector emits samples at scrape time. It is how state that already lives
// elsewhere (queue depth inside a partition) is exposed without duplicating
// it in a second, possibly stale, counter.
type Collector func(w *Writer)

// Registry holds metric vectors and collectors.
type Registry struct {
	mu         sync.Mutex
	vecs       []interface{ collect(*Writer) }
	collectors []Collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// AddCollector registers a scrape-time collector.
func (r *Registry) AddCollector(c Collector) {
	r.mu.Lock()
	r.collectors = append(r.collectors, c)
	r.mu.Unlock()
}

func (r *Registry) add(v interface{ collect(*Writer) }) {
	r.mu.Lock()
	r.vecs = append(r.vecs, v)
	r.mu.Unlock()
}

// ---- exposition ----

type family struct {
	name, help, typ string
	samples         []string
}

// Writer accumulates samples grouped into metric families.
type Writer struct{ fams map[string]*family }

func (w *Writer) fam(name, help, typ string) *family {
	if w.fams == nil {
		w.fams = map[string]*family{}
	}
	f, ok := w.fams[name]
	if !ok {
		f = &family{name: name, help: help, typ: typ}
		w.fams[name] = f
	}
	return f
}

// Gauge emits one gauge sample. labels are alternating names and values.
func (w *Writer) Gauge(name, help string, v float64, labels ...string) {
	f := w.fam(name, help, "gauge")
	f.samples = append(f.samples, name+formatLabels(labels)+" "+formatValue(v))
}

// Counter emits one counter sample. Counter names should end in _total.
func (w *Writer) Counter(name, help string, v float64, labels ...string) {
	f := w.fam(name, help, "counter")
	f.samples = append(f.samples, name+formatLabels(labels)+" "+formatValue(v))
}

func (w *Writer) histogram(name, help string, bounds []float64, cum []uint64, sum float64, count uint64, labels []string) {
	f := w.fam(name, help, "histogram")
	for i, b := range bounds {
		f.samples = append(f.samples, name+"_bucket"+formatLabels(labels, "le", formatValue(b))+" "+strconv.FormatUint(cum[i], 10))
	}
	f.samples = append(f.samples,
		name+"_bucket"+formatLabels(labels, "le", "+Inf")+" "+strconv.FormatUint(count, 10),
		name+"_sum"+formatLabels(labels)+" "+formatValue(sum),
		name+"_count"+formatLabels(labels)+" "+strconv.FormatUint(count, 10))
}

func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func formatLabels(labels []string, extra ...string) string {
	all := append(append([]string(nil), labels...), extra...)
	if len(all) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(all); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(all[i])
		b.WriteString(`="`)
		b.WriteString(labelEscaper.Replace(all[i+1]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// Expose renders every metric in exposition format.
func (r *Registry) Expose(out io.Writer) error {
	r.mu.Lock()
	vecs := append([]interface{ collect(*Writer) }(nil), r.vecs...)
	cols := append([]Collector(nil), r.collectors...)
	r.mu.Unlock()

	w := &Writer{}
	for _, v := range vecs {
		v.collect(w)
	}
	for _, c := range cols {
		c(w)
	}
	names := make([]string, 0, len(w.fams))
	for n := range w.fams {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		f := w.fams[n]
		b.WriteString("# HELP " + f.name + " " + f.help + "\n# TYPE " + f.name + " " + f.typ + "\n")
		if f.typ != "histogram" { // histogram sample order is meaningful; keep emission order
			sort.Strings(f.samples)
		}
		for _, s := range f.samples {
			b.WriteString(s + "\n")
		}
	}
	_, err := io.WriteString(out, b.String())
	return err
}

// Handler serves the registry for Prometheus scraping.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = r.Expose(w)
	})
}

// ---- label vectors ----

type labelSet[T any] struct {
	mu     sync.RWMutex
	m      map[string]*entry[T]
	labels []string
	newT   func() *T
}

type entry[T any] struct {
	values []string
	v      *T
}

func newLabelSet[T any](labels []string, mk func() *T) *labelSet[T] {
	return &labelSet[T]{m: map[string]*entry[T]{}, labels: labels, newT: mk}
}

func (s *labelSet[T]) with(values []string) *T {
	if len(values) != len(s.labels) {
		panic("observability: wrong number of label values")
	}
	key := strings.Join(values, "\xff")
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	if ok {
		return e.v
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok = s.m[key]; ok {
		return e.v
	}
	e = &entry[T]{values: append([]string(nil), values...), v: s.newT()}
	s.m[key] = e
	return e.v
}

func (s *labelSet[T]) each(fn func(labels []string, v *T)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.m {
		pairs := make([]string, 0, 2*len(s.labels))
		for i, l := range s.labels {
			pairs = append(pairs, l, e.values[i])
		}
		fn(pairs, e.v)
	}
}

// Counter is a monotonically increasing count.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// CounterVec is a family of counters keyed by label values.
type CounterVec struct {
	name, help string
	set        *labelSet[Counter]
}

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, set: newLabelSet(labels, func() *Counter { return &Counter{} })}
	r.add(c)
	return c
}

// With returns the counter for the given label values.
func (c *CounterVec) With(values ...string) *Counter { return c.set.with(values) }

func (c *CounterVec) collect(w *Writer) {
	c.set.each(func(l []string, v *Counter) { w.Counter(c.name, c.help, float64(v.Value()), l...) })
}

// Gauge is a value that can go up and down.
type Gauge struct{ bits atomic.Uint64 }

// Set stores v.
func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }

// Add adds d (which may be negative).
func (g *Gauge) Add(d float64) {
	for {
		old := g.bits.Load()
		if g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+d)) {
			return
		}
	}
}

// Value returns the current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

// GaugeVec is a family of gauges.
type GaugeVec struct {
	name, help string
	set        *labelSet[Gauge]
}

// NewGaugeVec registers a gauge family.
func (r *Registry) NewGaugeVec(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{name: name, help: help, set: newLabelSet(labels, func() *Gauge { return &Gauge{} })}
	r.add(g)
	return g
}

// With returns the gauge for the given label values.
func (g *GaugeVec) With(values ...string) *Gauge { return g.set.with(values) }

func (g *GaugeVec) collect(w *Writer) {
	g.set.each(func(l []string, v *Gauge) { w.Gauge(g.name, g.help, v.Value(), l...) })
}

// DefBuckets suit request latencies from sub-millisecond to tens of seconds.
var DefBuckets = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// Histogram counts observations into fixed buckets.
type Histogram struct {
	bounds  []float64
	buckets []atomic.Uint64 // per-bucket (non-cumulative); last slot is +Inf
	sumBits atomic.Uint64
	count   atomic.Uint64
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // first bound >= v
	h.buckets[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		if h.sumBits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}

// HistogramVec is a family of histograms.
type HistogramVec struct {
	name, help string
	bounds     []float64
	set        *labelSet[Histogram]
}

// NewHistogramVec registers a histogram family. buckets must be ascending.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if !sort.Float64sAreSorted(buckets) {
		panic("observability: histogram buckets must be sorted")
	}
	h := &HistogramVec{name: name, help: help, bounds: buckets}
	h.set = newLabelSet(labels, func() *Histogram {
		return &Histogram{bounds: buckets, buckets: make([]atomic.Uint64, len(buckets)+1)}
	})
	r.add(h)
	return h
}

// With returns the histogram for the given label values.
func (h *HistogramVec) With(values ...string) *Histogram { return h.set.with(values) }

func (h *HistogramVec) collect(w *Writer) {
	h.set.each(func(l []string, v *Histogram) {
		cum := make([]uint64, len(h.bounds))
		var run uint64
		for i := range h.bounds {
			run += v.buckets[i].Load()
			cum[i] = run
		}
		w.histogram(h.name, h.help, h.bounds, cum, math.Float64frombits(v.sumBits.Load()), v.count.Load(), l)
	})
}
