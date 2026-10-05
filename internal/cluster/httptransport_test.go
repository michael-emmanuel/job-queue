package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/partition"
)

func rpcServer(t *testing.T, h Handler, secret string, f *Faults) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(RPCHandler(h, secret, f))
	t.Cleanup(srv.Close)
	return srv
}

func echo(_ context.Context, from, method string, body json.RawMessage) (any, error) {
	var v map[string]any
	json.Unmarshal(body, &v)
	v["from"], v["method"] = from, method
	return v, nil
}

func TestHTTPRoundTrip(t *testing.T) {
	srv := rpcServer(t, echo, "s3cret-s3cret", nil)
	tr := NewHTTPTransport("n1", map[string]string{"n2": srv.URL}, "s3cret-s3cret")
	var out map[string]any
	if err := tr.Call(bg, "n2", "Node.Echo", map[string]any{"x": 1}, &out); err != nil {
		t.Fatal(err)
	}
	if out["x"].(float64) != 1 || out["from"] != "n1" || out["method"] != "Node.Echo" {
		t.Fatalf("reply: %v", out)
	}
}

func TestHTTPErrorsKeepTheirIdentity(t *testing.T) {
	cases := []struct {
		err  error
		is   error
		hint string
	}{
		{partition.ErrNotFound, partition.ErrNotFound, ""},
		{partition.ErrStaleLease, partition.ErrStaleLease, ""},
		{ErrStaleGeneration, ErrStaleGeneration, ""},
		{notLeader("n3"), ErrNotLeader, "n3"},
		{ErrNotHosted, ErrNotHosted, ""},
	}
	for _, c := range cases {
		c := c
		srv := rpcServer(t, func(context.Context, string, string, json.RawMessage) (any, error) { return nil, c.err }, "", nil)
		tr := NewHTTPTransport("a", map[string]string{"b": srv.URL}, "")
		err := tr.Call(bg, "b", "X.Y", struct{}{}, nil)
		if !errors.Is(err, c.is) {
			t.Errorf("%v came back as %v: errors.Is lost across HTTP", c.err, err)
		}
		if LeaderHint(err) != c.hint {
			t.Errorf("hint = %q, want %q", LeaderHint(err), c.hint)
		}
	}
	srv := rpcServer(t, func(context.Context, string, string, json.RawMessage) (any, error) {
		return nil, errors.New("disk path /secret/location exploded")
	}, "", nil)
	tr := NewHTTPTransport("a", map[string]string{"b": srv.URL}, "")
	err := tr.Call(bg, "b", "X.Y", struct{}{}, nil)
	var re *RPCError
	if !errors.As(err, &re) || re.Code != "internal" {
		t.Fatalf("unknown error = %v", err)
	}
}

func TestHTTPAuthentication(t *testing.T) {
	srv := rpcServer(t, echo, "right-secret-0123", nil)
	for _, secret := range []string{"", "wrong-secret-0123"} {
		tr := NewHTTPTransport("n1", map[string]string{"n2": srv.URL}, secret)
		err := tr.Call(bg, "n2", "Node.Echo", map[string]any{}, &map[string]any{})
		if !errors.Is(err, ErrUnauthorized) {
			t.Errorf("secret %q: err = %v, want ErrUnauthorized", secret, err)
		}
		if errors.Is(err, ErrUnreachable) {
			t.Error("an auth failure must not look like an unreachable node (it would be retried forever)")
		}
	}
	// A browser-style request without the header is also refused.
	res, _ := http.Post(srv.URL+"/rpc/Node.Echo", "application/json", bytes.NewReader([]byte("{}")))
	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("anonymous POST = %d", res.StatusCode)
	}
}

func TestHTTPUnreachableVersusTimeout(t *testing.T) {
	srv := rpcServer(t, func(ctx context.Context, _, _ string, _ json.RawMessage) (any, error) {
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
		return struct{}{}, nil
	}, "", nil)
	tr := NewHTTPTransport("a", map[string]string{"b": srv.URL, "dead": "http://127.0.0.1:1"}, "")

	ctx, cancel := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancel()
	err := tr.Call(ctx, "b", "Slow", struct{}{}, nil)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrUnreachable) {
		t.Fatalf("slow peer: %v; a timeout is AMBIGUOUS (the request may have run) and must not be reported as unreachable", err)
	}
	err = tr.Call(bg, "dead", "Any", struct{}{}, nil)
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("refused connection: %v, want ErrUnreachable (provably not applied)", err)
	}
	if err := tr.Call(bg, "nobody", "Any", struct{}{}, nil); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("unknown peer: %v", err)
	}
}

func TestFaultInjectionBlocksOutboundAndInbound(t *testing.T) {
	fb := NewFaults(1)
	srv := rpcServer(t, echo, "", fb)
	fa := NewFaults(2)
	tr := WithFaults(NewHTTPTransport("a", map[string]string{"b": srv.URL}, ""), fa)
	call := func() error { return tr.Call(bg, "b", "E", map[string]any{}, &map[string]any{}) }

	if err := call(); err != nil {
		t.Fatal(err)
	}
	fa.Set(FaultConfig{BlockTo: []string{"b"}})
	if err := call(); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("outbound block: %v", err)
	}
	fa.Set(FaultConfig{})
	fb.Set(FaultConfig{BlockFrom: []string{"a"}})
	if err := call(); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("inbound block: %v", err)
	}
	fb.Set(FaultConfig{})
	fa.Set(FaultConfig{Drop: 1})
	if err := call(); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("drop=1: %v", err)
	}
	fa.Set(FaultConfig{DelayMS: 80})
	start := time.Now()
	if err := call(); err != nil || time.Since(start) < 70*time.Millisecond {
		t.Fatalf("delay: %v after %v", err, time.Since(start))
	}
}

func TestFaultAdminEndpoint(t *testing.T) {
	f := NewFaults(1)
	adm := httptest.NewServer(f.Handler())
	defer adm.Close()
	req, _ := http.NewRequest(http.MethodPut, adm.URL, bytes.NewReader([]byte(`{"drop":0.5,"block_to":["n2","n3"]}`)))
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("%v %v", res, err)
	}
	if c := f.Get(); c.Drop != 0.5 || len(c.BlockTo) != 2 {
		t.Fatalf("config: %+v", c)
	}
	if fail, _ := f.outbound("n3"); !fail {
		t.Fatal("block_to not applied")
	}
	res, _ = http.DefaultClient.Do(mustReq(http.MethodDelete, adm.URL))
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("DELETE = %d", res.StatusCode)
	}
}

func mustReq(method, url string) *http.Request {
	r, _ := http.NewRequest(method, url, nil)
	return r
}
