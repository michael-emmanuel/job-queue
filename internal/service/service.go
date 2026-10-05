// Package service holds the plumbing shared by the cluster binaries:
// flag and environment handling, the cluster secret, peer addressing, the
// private admin listener, and ordered graceful shutdown.
package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/cluster"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
)

// Common is the configuration every cluster process shares.
type Common struct {
	Name   string // this process's endpoint name; peers address it by this
	Listen string // RPC / API listener
	Admin  string // metrics, probes, optional pprof and fault injection (keep private)

	PeersSpec string            // "name=url,name=url"
	Peers     map[string]string // parsed
	Secret    string            // shared cluster secret, from env or file only

	InsecureNoSecret bool
	TLSCert, TLSKey  string
	CAFile           string // extra root CAs for talking to https peers

	EnableFaults bool
	EnablePprof  bool

	LogLevel string
	LogJSON  bool
	Grace    time.Duration
}

func envOr(getenv func(string) string, key, def string) string {
	if v := getenv("JOBQUEUE_" + key); v != "" {
		return v
	}
	return def
}

// Register adds the common flags to fs. Environment variables (JOBQUEUE_*)
// provide defaults; flags override them.
func (c *Common) Register(fs *flag.FlagSet, getenv func(string) string, defaultListen string) {
	fs.StringVar(&c.Name, "name", envOr(getenv, "NAME", ""), "this process's endpoint name, used by peers (env JOBQUEUE_NAME)")
	fs.StringVar(&c.Listen, "listen", envOr(getenv, "LISTEN", defaultListen), "listen address (env JOBQUEUE_LISTEN)")
	fs.StringVar(&c.Admin, "admin-listen", envOr(getenv, "ADMIN_LISTEN", "127.0.0.1:0"), "admin listen address for /metrics, /healthz, /readyz; keep private (env JOBQUEUE_ADMIN_LISTEN)")
	fs.StringVar(&c.PeersSpec, "peers", envOr(getenv, "PEERS", ""), "peer addresses as name=url,name=url (env JOBQUEUE_PEERS)")
	fs.BoolVar(&c.InsecureNoSecret, "insecure-no-cluster-auth", false, "do not authenticate cluster RPCs; local development only")
	fs.StringVar(&c.TLSCert, "tls-cert", envOr(getenv, "TLS_CERT", ""), "TLS certificate for the listener (env JOBQUEUE_TLS_CERT)")
	fs.StringVar(&c.TLSKey, "tls-key", envOr(getenv, "TLS_KEY", ""), "TLS key for the listener (env JOBQUEUE_TLS_KEY)")
	fs.StringVar(&c.CAFile, "ca-file", envOr(getenv, "CA_FILE", ""), "extra CA bundle for https peers (env JOBQUEUE_CA_FILE)")
	fs.BoolVar(&c.EnableFaults, "enable-fault-injection", false, "serve /faults on the admin listener (testing only)")
	fs.BoolVar(&c.EnablePprof, "enable-pprof", false, "serve /debug/pprof on the admin listener")
	fs.StringVar(&c.LogLevel, "log-level", envOr(getenv, "LOG_LEVEL", "info"), "debug, info, warn or error")
	fs.BoolVar(&c.LogJSON, "log-json", true, "JSON logs")
	fs.DurationVar(&c.Grace, "shutdown-grace", 15*time.Second, "time allowed for in-flight requests on shutdown")
}

// Finish resolves the secret and peers and validates. Call after flag parsing.
func (c *Common) Finish(getenv func(string) string) error {
	c.Secret = getenv("JOBQUEUE_CLUSTER_SECRET")
	if f := getenv("JOBQUEUE_CLUSTER_SECRET_FILE"); f != "" && c.Secret == "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("reading JOBQUEUE_CLUSTER_SECRET_FILE: %w", err)
		}
		c.Secret = strings.TrimSpace(string(b))
	}
	c.Peers = map[string]string{}
	for _, kv := range strings.Split(c.PeersSpec, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		name, url, ok := strings.Cut(kv, "=")
		if !ok || name == "" || !strings.HasPrefix(url, "http") {
			return fmt.Errorf("bad -peers entry %q: want name=http://host:port", kv)
		}
		c.Peers[name] = url
	}
	switch {
	case c.Name == "":
		return errors.New("-name is required")
	case c.Secret == "" && !c.InsecureNoSecret:
		return errors.New("refusing to start without cluster authentication: set JOBQUEUE_CLUSTER_SECRET (or _FILE), or pass -insecure-no-cluster-auth for local development")
	case c.Secret != "" && len(c.Secret) < 16:
		return errors.New("JOBQUEUE_CLUSTER_SECRET must be at least 16 characters")
	case c.Secret != "" && c.InsecureNoSecret:
		return errors.New("-insecure-no-cluster-auth conflicts with a configured secret")
	case (c.TLSCert == "") != (c.TLSKey == ""):
		return errors.New("-tls-cert and -tls-key must be set together")
	}
	return nil
}

// Logger builds the process logger.
func (c *Common) Logger(w io.Writer) *slog.Logger {
	return observability.NewLogger(w, c.LogLevel, c.LogJSON).With("service", c.Name)
}

// Transport builds the HTTP transport, wrapped with fault injection when enabled.
func (c *Common) Transport(faults *cluster.Faults) (*cluster.HTTPTransport, error) {
	var tlsCfg *tls.Config
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("no certificates found in -ca-file")
		}
		tlsCfg = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return cluster.NewHTTPTransportTLS(c.Name, c.Peers, c.Secret, tlsCfg), nil
}

// AdminHandler builds the private operations handler.
func (c *Common) AdminHandler(reg *observability.Registry, ready func() error, faults *cluster.Faults) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", reg.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := ready(); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n"))
	})
	if c.EnableFaults {
		mux.Handle("/faults", faults.Handler())
	}
	if c.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	}
	return mux
}

// Listener is a bound HTTP server.
type Listener struct {
	Name string
	Srv  *http.Server
	Ln   net.Listener
	TLS  bool
}

// Bind creates an HTTP server with sane limits on addr.
func (c *Common) Bind(name, addr string, h http.Handler, tlsOn bool, logger *slog.Logger) (*Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	if tlsOn {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &Listener{Name: name, Srv: srv, Ln: ln, TLS: tlsOn}, nil
}

// Run serves every listener until ctx is cancelled or one fails, then shuts
// down in order: beginDrain (stop advertising readiness, release long
// polls), stop the HTTP servers (in-flight requests finish), and finally
// closeState (flush and close storage). Closing state last means no request
// can race a closing log.
func (c *Common) Run(ctx context.Context, logger *slog.Logger, ls []*Listener, beginDrain func(), closeState func() error) error {
	errc := make(chan error, len(ls))
	for _, l := range ls {
		go func() {
			if l.TLS {
				errc <- l.Srv.ServeTLS(l.Ln, c.TLSCert, c.TLSKey)
			} else {
				errc <- l.Srv.Serve(l.Ln)
			}
		}()
	}
	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("server failed: %w", err)
		}
	}
	if beginDrain != nil {
		beginDrain()
	}
	sctx, cancel := context.WithTimeout(context.Background(), c.Grace)
	defer cancel()
	for _, l := range ls {
		if err := l.Srv.Shutdown(sctx); err != nil {
			logger.Error("shutdown", "server", l.Name, "err", err)
			l.Srv.Close()
		}
	}
	if closeState != nil {
		if err := closeState(); err != nil {
			logger.Error("closing state", "err", err)
			if runErr == nil {
				runErr = err
			}
		}
	}
	logger.Info("stopped")
	return runErr
}
