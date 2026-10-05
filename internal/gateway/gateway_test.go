package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/api"
	"github.com/michael-emmanuel/jobqueue/internal/cluster"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
)

const (
	adminKey = "admin-key-0123456789"
	prodKey  = "prod-key-01234567890"
	workKey  = "work-key-012345678901"
	secret   = "cluster-secret-0123456789"
)

// swap lets an httptest server exist (and have a URL) before its handler does.
type swap struct {
	mu sync.RWMutex
	h  http.Handler
}

func (s *swap) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	h := s.h
	s.mu.RUnlock()
	if h == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	h.ServeHTTP(w, r)
}

func (s *swap) set(h http.Handler) { s.mu.Lock(); s.h = h; s.mu.Unlock() }

type stack struct {
	t       *testing.T
	coord   *cluster.Coordinator
	nodes   map[string]*cluster.Node
	servers map[string]*httptest.Server
	apiSrv  *httptest.Server
	cancel  context.CancelFunc
	names   []string
}

// newStack starts a coordinator, three nodes and an API gateway, all talking
// real HTTP on loopback with real clocks and short timeouts.
func newStack(t *testing.T) *stack {
	t.Helper()
	st := &stack{t: t, nodes: map[string]*cluster.Node{}, servers: map[string]*httptest.Server{}, names: []string{"n1", "n2", "n3"}}
	swaps := map[string]*swap{}
	peers := map[string]string{}
	for _, n := range append([]string{"coord"}, st.names...) {
		swaps[n] = &swap{}
		srv := httptest.NewServer(swaps[n])
		st.servers[n] = srv
		peers[n] = srv.URL
	}
	ctx, cancel := context.WithCancel(context.Background())
	st.cancel = cancel

	coord, err := cluster.NewCoordinator(cluster.CoordConfig{
		Dir: t.TempDir(), Transport: cluster.NewHTTPTransport("coord", peers, secret),
		FailureTimeout: 800 * time.Millisecond, LeaseDuration: 1200 * time.Millisecond, RPCTimeout: 400 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	st.coord = coord
	swaps["coord"].set(cluster.RPCHandler(coord.Handler(), secret, nil))
	go coord.Run(ctx, 100*time.Millisecond)

	for _, n := range st.names {
		node := cluster.NewNode(cluster.NodeConfig{Name: n, Dir: t.TempDir(), Coord: "coord",
			Transport: cluster.NewHTTPTransport(n, peers, secret), RPCTimeout: 400 * time.Millisecond,
			ReplHeartbeat: 30 * time.Millisecond})
		st.nodes[n] = node
		swaps[n].set(cluster.RPCHandler(node.Handler(), secret, nil))
		go node.Run(ctx, 100*time.Millisecond)
	}

	client := cluster.NewClient(cluster.ClientConfig{Name: "gw", Coord: "coord",
		Transport: cluster.NewHTTPTransport("gw", peers, secret), RetryDelay: 50 * time.Millisecond,
		Retries: 40, RPCTimeout: time.Second, GroupSession: 5 * time.Second})
	auth, err := api.ParseKeys(adminKey + ":ops:admin," + prodKey + ":acme:producer," + workKey + ":workers:worker")
	if err != nil {
		t.Fatal(err)
	}
	srv := api.New(api.Options{Backend: New(client, nil), Auth: auth, Logger: observability.Discard(),
		RequestTimeout: 20 * time.Second, MaxLongPoll: 5 * time.Second})
	st.apiSrv = httptest.NewServer(srv.Handler())

	t.Cleanup(func() {
		cancel()
		st.apiSrv.Close()
		for n, node := range st.nodes {
			node.Close()
			st.servers[n].Close()
		}
		st.servers["coord"].Close()
	})
	st.eventually("three nodes alive", 10*time.Second, func() bool { return len(coord.Stats().AliveNodes) == 3 })
	return st
}

func (st *stack) eventually(what string, d time.Duration, cond func() bool) {
	st.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			st.t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type reply struct {
	status int
	hdr    http.Header
	m      map[string]any
}

func (st *stack) call(method, path, key string, body any, hdr ...string) reply {
	st.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, st.apiSrv.URL+path, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		st.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var m map[string]any
	json.Unmarshal(raw, &m)
	return reply{res.StatusCode, res.Header, m}
}

func (r reply) code() string {
	e, _ := r.m["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func (st *stack) createQueue(name string, extra map[string]any) {
	st.t.Helper()
	cfg := map[string]any{"name": name, "partitions": 3, "replication_factor": 3,
		"retry": map[string]any{"max_attempts": 3, "base_delay": "100ms", "max_delay": "200ms", "jitter": "none"}}
	for k, v := range extra {
		cfg[k] = v
	}
	if r := st.call("POST", "/v1/queues", adminKey, cfg); r.status != 201 {
		st.t.Fatalf("create queue: %d %v", r.status, r.m)
	}
	st.eventually("every partition has a leader", 15*time.Second, func() bool {
		for _, q := range st.coord.Metadata().Queues {
			for _, p := range q.Partitions {
				if p.Leader == "" {
					return false
				}
			}
		}
		return true
	})
}

// submit retries across elections, as a real producer with an idempotency key would.
func (st *stack) submit(queue, key string) string {
	st.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		r := st.call("POST", "/v1/jobs", prodKey, map[string]any{"queue": queue, "type": "t", "idempotency_key": key,
			"payload": map[string]any{"k": key}})
		if r.status == 201 || r.status == 200 {
			return r.m["job_id"].(string)
		}
		if time.Now().After(deadline) || r.status == 400 || r.status == 404 {
			st.t.Fatalf("submit %s: %d %v", key, r.status, r.m)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// drain pulls as group members until every wanted ID was seen, acking each.
func (st *stack) drain(queue, group string, workers []string, want map[string]bool) map[string]int {
	st.t.Helper()
	seen := map[string]int{}
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < len(want) && time.Now().Before(deadline) {
		for _, w := range workers {
			r := st.call("POST", "/v1/queues/"+queue+"/pull", workKey,
				map[string]any{"worker": w, "group": group, "max": 10, "visibility": "20s"})
			if r.status != 200 {
				continue
			}
			for _, j := range r.m["jobs"].([]any) {
				jm := j.(map[string]any)
				id := jm["job_id"].(string)
				seen[id]++
				a := st.call("POST", "/v1/jobs/"+id+"/ack", workKey, map[string]any{"attempt": int(jm["attempt"].(float64))})
				if a.status != 204 && a.status != 409 {
					st.t.Fatalf("ack %s: %d %v", id, a.status, a.m)
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return seen
}

func TestGatewayEndToEndOverHTTP(t *testing.T) {
	st := newStack(t)
	st.createQueue("orders", nil)

	want := map[string]bool{}
	for i := 0; i < 25; i++ {
		want[st.submit("orders", fmt.Sprint("o", i))] = true
	}
	// A client retry is deduplicated through the whole stack.
	dup := st.call("POST", "/v1/jobs", prodKey, map[string]any{"queue": "orders", "type": "t", "idempotency_key": "o3"})
	if dup.status != 200 || dup.m["duplicate"] != true {
		t.Fatalf("retry: %d %v", dup.status, dup.m)
	}
	seen := st.drain("orders", "workers", []string{"w1", "w2"}, want)
	if len(seen) != 25 {
		t.Fatalf("processed %d of 25", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %s delivered %d times", id, n)
		}
	}
	stats := st.call("GET", "/v1/queues/orders", adminKey, nil)
	total := stats.m["total"].(map[string]any)
	if total["acked"].(float64) != 25 || total["ready"].(float64) != 0 || len(stats.m["partitions"].([]any)) != 3 {
		t.Fatalf("stats: %v", stats.m)
	}
	if stats.m["config"].(map[string]any)["replication_factor"].(float64) != 3 {
		t.Fatalf("config: %v", stats.m["config"])
	}
}

func TestGatewaySurvivesLosingANodeUnderTraffic(t *testing.T) {
	st := newStack(t)
	st.createQueue("q", nil)
	want := map[string]bool{}
	for i := 0; i < 20; i++ {
		want[st.submit("q", fmt.Sprint("a", i))] = true
	}
	victim := st.coord.Metadata().Queues["q"].Partitions[0].Leader
	st.nodes[victim].Kill()
	st.servers[victim].CloseClientConnections()
	st.servers[victim].Close()

	// Producers keep going; requests that land during the election are retried.
	for i := 0; i < 20; i++ {
		want[st.submit("q", fmt.Sprint("b", i))] = true
	}
	st.eventually("all partitions led by survivors", 15*time.Second, func() bool {
		for _, p := range st.coord.Metadata().Queues["q"].Partitions {
			if p.Leader == "" || p.Leader == victim {
				return false
			}
		}
		return true
	})
	seen := st.drain("q", "g", []string{"w1", "w2", "w3"}, want)
	for id := range want {
		if seen[id] == 0 {
			t.Fatalf("job %s was acknowledged to a producer but lost in the failover", id)
		}
	}
	t.Logf("%d acknowledged jobs, all present after losing node %s", len(want), victim)
}

func TestGatewayPropagatesBackpressureAndRateLimitsFromTheCluster(t *testing.T) {
	st := newStack(t)
	st.createQueue("tiny", map[string]any{"partitions": 1, "max_depth": 3})
	for i := 0; i < 3; i++ {
		st.submit("tiny", fmt.Sprint("k", i))
	}
	r := st.call("POST", "/v1/jobs", prodKey, map[string]any{"queue": "tiny", "type": "t", "idempotency_key": "overflow"})
	if r.status != 429 || r.code() != "queue_full" || r.hdr.Get("Retry-After") == "" {
		t.Fatalf("queue_full over RPC: %d %v %v", r.status, r.code(), r.hdr)
	}

	st.createQueue("slow", map[string]any{"partitions": 1, "enqueue_rate": 1, "enqueue_burst": 2})
	st.submit("slow", "x1")
	st.submit("slow", "x2")
	r = st.call("POST", "/v1/jobs", prodKey, map[string]any{"queue": "slow", "type": "t", "idempotency_key": "x3"})
	if r.status != 429 || r.code() != "rate_limited" {
		t.Fatalf("gateway rate limit: %d %v", r.status, r.m)
	}
}

func TestGatewayReportsUnavailableNotCorruptionWhenQuorumIsLost(t *testing.T) {
	st := newStack(t)
	st.createQueue("q", map[string]any{"partitions": 1})
	st.submit("q", "pre")
	leader := st.coord.Metadata().Queues["q"].Partitions[0].Leader
	for _, n := range st.names {
		if n != leader {
			st.nodes[n].Kill()
			st.servers[n].CloseClientConnections()
			st.servers[n].Close()
		}
	}
	start := time.Now()
	r := st.call("POST", "/v1/jobs", prodKey, map[string]any{"queue": "q", "type": "t", "idempotency_key": "nope"})
	if r.status != 503 && r.status != 504 {
		t.Fatalf("write without a quorum returned %d %v; it must be refused, never acknowledged", r.status, r.m)
	}
	if c := r.code(); c != "unavailable" && c != "timeout" {
		t.Fatalf("error code %q", c)
	}
	took := time.Since(start)
	t.Logf("without a quorum: HTTP %d %s after %v", r.status, r.code(), took.Round(time.Millisecond))
	if took > 8*time.Second {
		t.Fatalf("the gateway held the request for %v; ambiguous retries must be bounded", took)
	}
}

func TestGatewayMetricsAndReadiness(t *testing.T) {
	st := newStack(t)
	st.createQueue("q", nil)
	if r := st.call("GET", "/readyz", "", nil); r.status != 200 {
		t.Fatalf("readyz: %d %v", r.status, r.m)
	}
	reg := observability.NewRegistry()
	reg.AddCollector(st.coord.Collect)
	var b strings.Builder
	reg.Expose(&b)
	for _, want := range []string{"jobqueue_cluster_alive_nodes 3", "jobqueue_leaderless_partitions 0"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
}
