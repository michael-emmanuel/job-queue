// Command broker runs a single-node job queue: the broker engine plus its
// HTTP API. The distribution milestone splits the stateless gateway from
// stateful broker nodes; until then this one process is both.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/api"
	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/config"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr, nil); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run starts the server and blocks until ctx is cancelled and shutdown has
// completed. ready, if non-nil, receives the bound addresses once listening.
func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer, ready func(api, admin string)) error {
	cfg, err := config.Load(args, getenv, stderr)
	if err != nil {
		return err
	}
	logger := observability.NewLogger(stderr, cfg.LogLevel, cfg.LogJSON)

	var auth *api.Authenticator
	if cfg.InsecureNoAuth {
		auth = api.InsecureAuthenticator()
		logger.Warn("AUTHENTICATION DISABLED: every caller is an admin; never use this outside local development")
	} else if auth, err = api.ParseKeys(cfg.APIKeys); err != nil {
		return err
	}
	if host, _, err := net.SplitHostPort(cfg.AdminAddr); err == nil {
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			logger.Warn("admin listener is not bound to loopback; metrics and pprof are unauthenticated, restrict access at the network layer", "addr", cfg.AdminAddr)
		}
	}

	policy := map[string]storage.SyncPolicy{"always": storage.SyncAlways, "periodic": storage.SyncPeriodic, "none": storage.SyncNone}[cfg.Sync]
	b, err := broker.Open(broker.Config{
		DataDir: cfg.DataDir, Sync: policy, SyncEvery: cfg.SyncEvery,
		SegmentBytes: cfg.SegmentBytes, CompactBytes: cfg.CompactBytes,
		AutoCreate: cfg.AutoCreateQueues, DefaultQueue: broker.QueueConfig{Partitions: cfg.DefaultPartitions},
		Logger: logger,
	})
	if err != nil {
		return err
	}

	reg := observability.NewRegistry()
	reg.AddCollector(observability.RuntimeCollector(cfg.DataDir))
	srv := api.New(api.Options{
		Broker: b, Auth: auth, Logger: logger, Metrics: reg,
		GlobalRate: cfg.GlobalRate, GlobalBurst: cfg.GlobalBurst,
		TenantRate: cfg.TenantRate, TenantBurst: cfg.TenantBurst,
		MaxBodyBytes: cfg.MaxBodyBytes, RequestTimeout: cfg.RequestTimeout,
		MaxLongPoll: cfg.MaxLongPoll, MaxInflight: cfg.MaxInflight, MaxPullers: cfg.MaxPullers,
	})

	stdlog := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	newServer := func(h http.Handler) *http.Server {
		return &http.Server{
			Handler:           h,
			ReadHeaderTimeout: 5 * time.Second, // slowloris protection
			ReadTimeout:       30 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxHeaderBytes:    64 << 10,
			ErrorLog:          stdlog,
			// No server-wide WriteTimeout: long polls would hit it. The API
			// sets a per-request write deadline instead.
		}
	}
	apiSrv := newServer(srv.Handler())
	adminSrv := newServer(srv.AdminHandler(cfg.EnablePprof))
	if cfg.TLSCertFile != "" {
		apiSrv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	apiLn, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		b.Close()
		return err
	}
	adminLn, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		apiLn.Close()
		b.Close()
		return err
	}
	logger.Info("listening", "api", apiLn.Addr().String(), "admin", adminLn.Addr().String(),
		"tls", cfg.TLSCertFile != "", "data_dir", cfg.DataDir, "sync", cfg.Sync)
	if ready != nil {
		ready(apiLn.Addr().String(), adminLn.Addr().String())
	}

	errc := make(chan error, 2)
	go func() {
		if cfg.TLSCertFile != "" {
			errc <- apiSrv.ServeTLS(apiLn, cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			errc <- apiSrv.Serve(apiLn)
		}
	}()
	go func() { errc <- adminSrv.Serve(adminLn) }()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("server failed: %w", err)
		}
	}

	// Shutdown order matters:
	//  1. Drain: readiness goes red and long polls return, so the load
	//     balancer stops routing here and nothing idles through the grace period.
	//  2. Stop the HTTP servers: in-flight requests finish (or the grace period ends).
	//  3. Only then close the broker, so no request can race a closing log;
	//     Close flushes and fsyncs every partition.
	srv.BeginDrain()
	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := apiSrv.Shutdown(shutCtx); err != nil {
		logger.Error("api shutdown", "err", err)
		apiSrv.Close()
	}
	if err := adminSrv.Shutdown(shutCtx); err != nil {
		logger.Error("admin shutdown", "err", err)
		adminSrv.Close()
	}
	if err := b.Close(); err != nil {
		logger.Error("broker close", "err", err)
		if runErr == nil {
			runErr = err
		}
	}
	logger.Info("stopped")
	return runErr
}
