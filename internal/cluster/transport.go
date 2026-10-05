// Package cluster turns single-node partitions into a replicated,
// failure-tolerant system: a coordinator that places partitions, detects
// failures and runs elections; nodes that host replicas and serve the leader
// side; a client that routes requests; and consumer-group coordination.
//
// Every interaction between those parts is an RPC over the Transport
// interface. Tests run them over Network, an in-memory transport that can cut
// links, drop and duplicate messages and add latency; the cluster binaries
// use an HTTP transport. Messages are JSON-encoded even in memory so no
// component can accidentally share state with another.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

// Cluster-level errors.
var (
	ErrNotLeader       = replication.ErrNotLeader
	ErrNotHosted       = errors.New("cluster: partition is not hosted on this node")
	ErrStaleGeneration = errors.New("cluster: stale consumer-group generation; rejoin the group")
	ErrUnknownMember   = errors.New("cluster: unknown group member; rejoin the group")
	ErrUnreachable     = errors.New("cluster: node unreachable")
	ErrNoCoordinator   = errors.New("cluster: coordinator unavailable")
	ErrNoQuorum        = errors.New("cluster: could not reach a majority of replicas")
	ErrInternal        = errors.New("cluster: internal error")
	ErrUnauthorized    = errors.New("cluster: unauthorized RPC")
)

var codes = map[string]error{
	"not_leader":       ErrNotLeader,
	"not_hosted":       ErrNotHosted,
	"stale_generation": ErrStaleGeneration,
	"unknown_member":   ErrUnknownMember,
	"no_quorum":        ErrNoQuorum,
	"job_not_found":    partition.ErrNotFound,
	"stale_lease":      partition.ErrStaleLease,
	"queue_full":       partition.ErrQueueFull,
	"invalid_job":      partition.ErrInvalidJob,
	"closed":           partition.ErrClosed,
	"queue_not_found":  broker.ErrQueueNotFound,
	"queue_exists":     broker.ErrQueueExists,
	"invalid_queue":    broker.ErrInvalidQueue,
	"invalid_id":       broker.ErrInvalidID,
	"leadership_lost":  replication.ErrLeadershipLost,
	"deadline":         context.DeadlineExceeded,
	"unreachable":      ErrUnreachable,
	"unauthorized":     ErrUnauthorized,
}

// RPCError is an error that crossed a process boundary. errors.Is works on it
// for every sentinel in the codes table, so callers can write the same checks
// they would for a local call.
type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"` // e.g. the current leader for not_leader
}

func (e *RPCError) Error() string { return e.Code + ": " + e.Message }

// Is lets errors.Is match the original sentinel.
func (e *RPCError) Is(target error) bool { return codes[e.Code] == target }

// LeaderHint returns the leader named in a not_leader error, if any.
func LeaderHint(err error) string {
	var re *RPCError
	if errors.As(err, &re) {
		return re.Hint
	}
	return ""
}

func encodeError(err error, hint string) *RPCError {
	for code, sentinel := range codes {
		if errors.Is(err, sentinel) {
			return &RPCError{Code: code, Message: err.Error(), Hint: hint}
		}
	}
	return &RPCError{Code: "internal", Message: err.Error()}
}

// Handler serves RPCs for one named endpoint.
type Handler func(ctx context.Context, from, method string, body json.RawMessage) (any, error)

// Call performs an RPC over any replication.Transport and decodes the result.
// It exists so call sites read like function calls.
func Call[Req, Resp any](ctx context.Context, t replication.Transport, to, method string, req Req) (Resp, error) {
	var resp Resp
	err := t.Call(ctx, to, method, req, &resp)
	return resp, err
}

// ---- in-memory network with fault injection ----

// Network is an in-memory Transport for tests and demos. Faults are
// directional and can change while traffic is flowing.
type Network struct {
	mu       sync.Mutex
	handlers map[string]Handler
	down     map[string]bool
	cut      map[[2]string]bool
	loss     float64
	dup      float64
	latency  time.Duration
	rng      *rand.Rand

	Calls atomic.Int64 // RPCs attempted, for assertions
}

// NewNetwork returns a healthy network. seed makes loss/duplication reproducible.
func NewNetwork(seed uint64) *Network {
	return &Network{handlers: map[string]Handler{}, down: map[string]bool{}, cut: map[[2]string]bool{},
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

// Register attaches a handler under name (replacing any previous one).
func (n *Network) Register(name string, h Handler) {
	n.mu.Lock()
	n.handlers[name] = h
	n.mu.Unlock()
}

// Unregister removes an endpoint, as if its process had exited.
func (n *Network) Unregister(name string) {
	n.mu.Lock()
	delete(n.handlers, name)
	n.mu.Unlock()
}

// SetDown isolates a node completely: nothing in or out.
func (n *Network) SetDown(name string, v bool) { n.mu.Lock(); n.down[name] = v; n.mu.Unlock() }

// Cut blocks the directed link from -> to.
func (n *Network) Cut(from, to string, v bool) {
	n.mu.Lock()
	n.cut[[2]string{from, to}] = v
	n.mu.Unlock()
}

// Split cuts every link between the two groups, in both directions: a clean
// network partition.
func (n *Network) Split(a, b []string, v bool) {
	for _, x := range a {
		for _, y := range b {
			n.Cut(x, y, v)
			n.Cut(y, x, v)
		}
	}
}

// SetLoss makes each message fail with probability p.
func (n *Network) SetLoss(p float64) { n.mu.Lock(); n.loss = p; n.mu.Unlock() }

// SetDuplicate delivers each message twice with probability p.
func (n *Network) SetDuplicate(p float64) { n.mu.Lock(); n.dup = p; n.mu.Unlock() }

// SetLatency adds a fixed delay to every call.
func (n *Network) SetLatency(d time.Duration) { n.mu.Lock(); n.latency = d; n.mu.Unlock() }

// Endpoint returns the Transport as seen from node `from`.
func (n *Network) Endpoint(from string) replication.Transport { return endpoint{n, from} }

type endpoint struct {
	n    *Network
	from string
}

func (e endpoint) Call(ctx context.Context, to, method string, req, resp any) error {
	n := e.n
	n.Calls.Add(1)
	n.mu.Lock()
	h := n.handlers[to]
	blocked := h == nil || n.down[e.from] || n.down[to] || n.cut[[2]string{e.from, to}]
	lost := n.loss > 0 && n.rng.Float64() < n.loss
	dup := n.dup > 0 && n.rng.Float64() < n.dup
	lat := n.latency
	n.mu.Unlock()
	if blocked || lost {
		return fmt.Errorf("%w: %s -> %s", ErrUnreachable, e.from, to)
	}
	if lat > 0 {
		select {
		case <-time.After(lat):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	deliver := func() (json.RawMessage, error) {
		out, herr := h(ctx, e.from, method, append(json.RawMessage(nil), body...))
		if herr != nil {
			var re *RPCError
			if errors.As(herr, &re) {
				return nil, re
			}
			hint := ""
			if errors.Is(herr, ErrNotLeader) {
				hint = LeaderHint(herr)
			}
			return nil, encodeError(herr, hint)
		}
		return json.Marshal(out)
	}
	if dup {
		go deliver() // the network delivered a second copy; its result is discarded
	}
	raw, err := deliver()
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	return json.Unmarshal(raw, resp)
}

// notLeader builds a not_leader error carrying a hint.
func notLeader(hint string) error {
	return &RPCError{Code: "not_leader", Message: ErrNotLeader.Error(), Hint: hint}
}
