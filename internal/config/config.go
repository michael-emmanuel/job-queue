// Package config loads process configuration from flags and environment.
//
// Precedence: flag > environment > default. Secrets (API keys) are accepted
// only through the environment or a file, never as flags, because command
// lines are visible to every user on the host via the process list.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Config is the validated process configuration.
type Config struct {
	Addr      string // public API listener
	AdminAddr string // metrics, probes, pprof (keep private)
	DataDir   string

	TLSCertFile, TLSKeyFile string

	// APIKeys is the raw "key:tenant:roles,..." spec, from the environment or a file.
	APIKeys        string
	InsecureNoAuth bool

	LogLevel string
	LogJSON  bool

	Sync         string // always | periodic | none
	SyncEvery    time.Duration
	SegmentBytes int64
	CompactBytes int64

	AutoCreateQueues  bool
	DefaultPartitions int

	GlobalRate  float64
	GlobalBurst int
	TenantRate  float64
	TenantBurst int

	MaxBodyBytes   int64
	RequestTimeout time.Duration
	MaxLongPoll    time.Duration
	MaxInflight    int
	MaxPullers     int
	ShutdownGrace  time.Duration
	EnablePprof    bool
}

func envOr(getenv func(string) string, key, def string) string {
	if v := getenv("JOBQUEUE_" + key); v != "" {
		return v
	}
	return def
}

// Load parses args (without the program name).
func Load(args []string, getenv func(string) string, stderr io.Writer) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&c.Addr, "addr", envOr(getenv, "ADDR", ":8080"), "public API listen address (env JOBQUEUE_ADDR)")
	fs.StringVar(&c.AdminAddr, "admin-addr", envOr(getenv, "ADMIN_ADDR", "127.0.0.1:9090"), "metrics/probe/pprof listen address; keep it private (env JOBQUEUE_ADMIN_ADDR)")
	fs.StringVar(&c.DataDir, "data-dir", envOr(getenv, "DATA_DIR", "./data"), "directory for queue metadata and logs; empty = in-memory, nothing persisted (env JOBQUEUE_DATA_DIR)")
	fs.StringVar(&c.TLSCertFile, "tls-cert", envOr(getenv, "TLS_CERT", ""), "TLS certificate file (env JOBQUEUE_TLS_CERT)")
	fs.StringVar(&c.TLSKeyFile, "tls-key", envOr(getenv, "TLS_KEY", ""), "TLS private key file (env JOBQUEUE_TLS_KEY)")
	fs.BoolVar(&c.InsecureNoAuth, "insecure-no-auth", false, "disable authentication; local development only")
	fs.StringVar(&c.LogLevel, "log-level", envOr(getenv, "LOG_LEVEL", "info"), "debug, info, warn or error")
	fs.BoolVar(&c.LogJSON, "log-json", true, "JSON logs (false for human-readable text)")
	fs.StringVar(&c.Sync, "sync", "always", "WAL fsync policy: always, periodic or none")
	fs.DurationVar(&c.SyncEvery, "sync-every", 50*time.Millisecond, "fsync interval for -sync=periodic")
	fs.Int64Var(&c.SegmentBytes, "segment-bytes", 64<<20, "WAL segment size")
	fs.Int64Var(&c.CompactBytes, "compact-bytes", 64<<20, "compact a partition log once it exceeds this size (0 disables)")
	fs.BoolVar(&c.AutoCreateQueues, "auto-create-queues", false, "create unknown queues on first use (development convenience)")
	fs.IntVar(&c.DefaultPartitions, "default-partitions", 4, "partitions for auto-created queues")
	fs.Float64Var(&c.GlobalRate, "global-rate", 0, "submissions/second across all tenants (0 = unlimited)")
	fs.IntVar(&c.GlobalBurst, "global-burst", 0, "burst for -global-rate (0 = one second of rate)")
	fs.Float64Var(&c.TenantRate, "tenant-rate", 0, "submissions/second per tenant (0 = unlimited)")
	fs.IntVar(&c.TenantBurst, "tenant-burst", 0, "burst for -tenant-rate (0 = one second of rate)")
	fs.Int64Var(&c.MaxBodyBytes, "max-body-bytes", 1<<20, "maximum request body size")
	fs.DurationVar(&c.RequestTimeout, "request-timeout", 15*time.Second, "per-request deadline")
	fs.DurationVar(&c.MaxLongPoll, "max-long-poll", 30*time.Second, "maximum worker long-poll wait")
	fs.IntVar(&c.MaxInflight, "max-inflight", 1024, "concurrent requests before shedding load")
	fs.IntVar(&c.MaxPullers, "max-pullers", 4096, "concurrent long polls before shedding load")
	fs.DurationVar(&c.ShutdownGrace, "shutdown-grace", 30*time.Second, "time allowed for in-flight requests on shutdown")
	fs.BoolVar(&c.EnablePprof, "enable-pprof", false, "serve /debug/pprof on the admin listener")
	if err := fs.Parse(args); err != nil {
		return c, err
	}

	c.APIKeys = getenv("JOBQUEUE_API_KEYS")
	if f := getenv("JOBQUEUE_API_KEYS_FILE"); f != "" && c.APIKeys == "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return c, fmt.Errorf("reading JOBQUEUE_API_KEYS_FILE: %w", err)
		}
		c.APIKeys = strings.TrimSpace(string(b))
	}
	return c, c.validate()
}

func (c Config) validate() error {
	switch {
	case c.APIKeys == "" && !c.InsecureNoAuth:
		return errors.New("refusing to start without authentication: set JOBQUEUE_API_KEYS (or _FILE), or pass -insecure-no-auth for local development")
	case c.APIKeys != "" && c.InsecureNoAuth:
		return errors.New("-insecure-no-auth conflicts with configured API keys")
	case (c.TLSCertFile == "") != (c.TLSKeyFile == ""):
		return errors.New("-tls-cert and -tls-key must be set together")
	case c.Sync != "always" && c.Sync != "periodic" && c.Sync != "none":
		return fmt.Errorf("-sync must be always, periodic or none, got %q", c.Sync)
	case c.DefaultPartitions < 1 || c.DefaultPartitions > 256:
		return errors.New("-default-partitions must be between 1 and 256")
	case c.GlobalRate < 0 || c.TenantRate < 0:
		return errors.New("rates must not be negative")
	case c.MaxBodyBytes < 1024:
		return errors.New("-max-body-bytes must be at least 1024")
	case c.Addr == "":
		return errors.New("-addr must not be empty")
	}
	return nil
}
