package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

// ---- fault injection for real processes ----

// FaultConfig describes injected network faults. It is how the cluster
// binaries simulate a network partition without root or iptables: each
// process is told which peers it must not talk to.
type FaultConfig struct {
	Drop      float64  `json:"drop"`       // probability an outbound call fails
	DelayMS   int      `json:"delay_ms"`   // added to every outbound call
	BlockTo   []string `json:"block_to"`   // outbound calls to these peers fail
	BlockFrom []string `json:"block_from"` // inbound calls from these peers are refused
}

// Faults holds the current fault configuration and is safe for concurrent use.
type Faults struct {
	mu  sync.Mutex
	cfg FaultConfig
	to  map[string]bool
	frm map[string]bool
	rng *rand.Rand
}

// NewFaults returns a Faults with no faults active.
func NewFaults(seed uint64) *Faults {
	return &Faults{rng: rand.New(rand.NewPCG(seed, 1)), to: map[string]bool{}, frm: map[string]bool{}}
}

// Set replaces the active configuration.
func (f *Faults) Set(c FaultConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfg = c
	f.to, f.frm = map[string]bool{}, map[string]bool{}
	for _, n := range c.BlockTo {
		f.to[n] = true
	}
	for _, n := range c.BlockFrom {
		f.frm[n] = true
	}
}

// Get returns the active configuration.
func (f *Faults) Get() FaultConfig { f.mu.Lock(); defer f.mu.Unlock(); return f.cfg }

func (f *Faults) outbound(to string) (fail bool, delay time.Duration) {
	if f == nil {
		return false, 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.to[to] || (f.cfg.Drop > 0 && f.rng.Float64() < f.cfg.Drop), time.Duration(f.cfg.DelayMS) * time.Millisecond
}

func (f *Faults) refuseFrom(from string) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frm[from]
}

// Handler serves GET (read) and PUT (replace) of the fault configuration.
// Expose it only on a private admin listener and only when explicitly enabled.
func (f *Faults) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
		case http.MethodPut, http.MethodPost:
			var c FaultConfig
			if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&c); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			f.Set(c)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(f.Get())
	})
}

// faultyTransport applies Faults to outbound calls.
type faultyTransport struct {
	inner replication.Transport
	f     *Faults
}

// WithFaults wraps a Transport so that f's outbound faults apply to it.
func WithFaults(t replication.Transport, f *Faults) replication.Transport {
	return &faultyTransport{inner: t, f: f}
}

func (t *faultyTransport) Call(ctx context.Context, to, method string, req, resp any) error {
	fail, delay := t.f.outbound(to)
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		return fmt.Errorf("%w: injected fault to %s", ErrUnreachable, to)
	}
	return t.inner.Call(ctx, to, method, req, resp)
}

// ---- HTTP transport ----

// HTTPTransport sends RPCs as JSON over HTTP: POST {peer}/rpc/{method}.
// Connections are pooled and reused: replication and heartbeats are a steady
// stream of small requests between the same few peers, and a new TCP
// connection per call would dominate their cost.
type HTTPTransport struct {
	self   string
	secret string
	client *http.Client

	mu    sync.RWMutex
	peers map[string]string
}

// NewHTTPTransport creates a transport for node `self`. peers maps endpoint
// names to base URLs. secret is a shared cluster secret sent as a bearer
// token and checked by RPCHandler.
func NewHTTPTransport(self string, peers map[string]string, secret string) *HTTPTransport {
	return NewHTTPTransportTLS(self, peers, secret, nil)
}

// NewHTTPTransportTLS is NewHTTPTransport with a custom TLS configuration
// (for example a private CA pool) used when peers are https URLs.
func NewHTTPTransportTLS(self string, peers map[string]string, secret string, tlsCfg *tls.Config) *HTTPTransport {
	t := &HTTPTransport{self: self, secret: secret, peers: map[string]string{}}
	for k, v := range peers {
		t.peers[k] = strings.TrimRight(v, "/")
	}
	t.client = &http.Client{Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		TLSClientConfig:       tlsCfg,
	}}
	return t
}

// SetPeer adds or replaces a peer address.
func (t *HTTPTransport) SetPeer(name, url string) {
	t.mu.Lock()
	t.peers[name] = strings.TrimRight(url, "/")
	t.mu.Unlock()
}

// Call implements replication.Transport.
func (t *HTTPTransport) Call(ctx context.Context, to, method string, req, resp any) error {
	t.mu.RLock()
	base, ok := t.peers[to]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: unknown peer %q", ErrUnreachable, to)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/rpc/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("X-Cluster-From", t.self)
	if t.secret != "" {
		hr.Header.Set("Authorization", "Bearer "+t.secret)
	}
	res, err := t.client.Do(hr)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // our own deadline or cancellation: the outcome is unknown, not "unreachable"
		}
		return fmt.Errorf("%w: %s: %v", ErrUnreachable, to, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: reading reply from %s: %v", ErrUnreachable, to, err)
	}
	if res.StatusCode != http.StatusOK {
		var re RPCError
		if json.Unmarshal(raw, &re) == nil && re.Code != "" {
			return &re
		}
		return &RPCError{Code: "internal", Message: fmt.Sprintf("%s returned HTTP %d", to, res.StatusCode)}
	}
	if resp == nil {
		return nil
	}
	return json.Unmarshal(raw, resp)
}

// RPCHandler serves a Handler over HTTP. Requests must carry the cluster
// secret (compared in constant time) when one is configured. faults may be nil.
func RPCHandler(h Handler, secret string, faults *Faults) http.Handler {
	want := sha256.Sum256([]byte(secret))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/rpc/") {
			http.NotFound(w, r)
			return
		}
		if secret != "" {
			tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			got := sha256.Sum256([]byte(tok))
			if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
				writeRPCError(w, http.StatusUnauthorized, &RPCError{Code: "unauthorized", Message: "bad cluster secret"})
				return
			}
		}
		from := r.Header.Get("X-Cluster-From")
		if faults.refuseFrom(from) {
			writeRPCError(w, http.StatusServiceUnavailable, &RPCError{Code: "unreachable", Message: "injected fault: inbound from " + from + " refused"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 32<<20))
		if err != nil {
			writeRPCError(w, http.StatusRequestEntityTooLarge, &RPCError{Code: "internal", Message: "request too large"})
			return
		}
		out, herr := h(r.Context(), from, strings.TrimPrefix(r.URL.Path, "/rpc/"), body)
		if herr != nil {
			var re *RPCError
			if !errors.As(herr, &re) {
				hint := ""
				if errors.Is(herr, ErrNotLeader) {
					hint = LeaderHint(herr)
				}
				re = encodeError(herr, hint)
			}
			status := http.StatusUnprocessableEntity
			if re.Code == "internal" {
				status = http.StatusInternalServerError
			}
			writeRPCError(w, status, re)
			return
		}
		b, err := json.Marshal(out)
		if err != nil {
			writeRPCError(w, http.StatusInternalServerError, &RPCError{Code: "internal", Message: "encoding reply"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
}

func writeRPCError(w http.ResponseWriter, status int, re *RPCError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(re)
}
