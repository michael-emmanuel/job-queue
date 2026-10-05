// Command gateway runs the public HTTP API in front of a cluster. It is
// stateless: it caches cluster metadata and routes each request to the right
// partition leader, so any number can run behind a load balancer.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/api"
	"github.com/michael-emmanuel/jobqueue/internal/cluster"
	"github.com/michael-emmanuel/jobqueue/internal/gateway"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/service"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr, nil); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer, ready func(api, admin string)) error {
	var c service.Common
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c.Register(fs, getenv, ":8080")
	coord := fs.String("coord", "coord", "coordinator endpoint name (must appear in -peers)")
	insecure := fs.Bool("insecure-no-auth", false, "disable API authentication; local development only")
	globalRate := fs.Float64("global-rate", 0, "submissions/second across all tenants (0 = unlimited)")
	tenantRate := fs.Float64("tenant-rate", 0, "submissions/second per tenant (0 = unlimited)")
	maxBody := fs.Int64("max-body-bytes", 1<<20, "maximum request body size")
	reqTimeout := fs.Duration("request-timeout", 15*time.Second, "per-request deadline")
	maxPoll := fs.Duration("max-long-poll", 30*time.Second, "maximum worker long-poll wait")
	maxInflight := fs.Int("max-inflight", 1024, "concurrent requests before shedding load")
	maxPullers := fs.Int("max-pullers", 4096, "concurrent long polls before shedding load")
	session := fs.Duration("group-session", 10*time.Second, "consumer-group session timeout for pulls that name a group")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c.Name == "" {
		c.Name = "gateway"
	}
	if err := c.Finish(getenv); err != nil {
		return err
	}
	if _, ok := c.Peers[*coord]; !ok {
		return fmt.Errorf("-peers must include the coordinator %q", *coord)
	}
	keys := getenv("JOBQUEUE_API_KEYS")
	if f := getenv("JOBQUEUE_API_KEYS_FILE"); f != "" && keys == "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("reading JOBQUEUE_API_KEYS_FILE: %w", err)
		}
		keys = strings.TrimSpace(string(b))
	}
	var auth *api.Authenticator
	logger := c.Logger(stderr)
	switch {
	case *insecure && keys != "":
		return fmt.Errorf("-insecure-no-auth conflicts with configured API keys")
	case *insecure:
		auth = api.InsecureAuthenticator()
		logger.Warn("API AUTHENTICATION DISABLED: every caller is an admin; never use this outside local development")
	case keys == "":
		return fmt.Errorf("refusing to start without API authentication: set JOBQUEUE_API_KEYS (or _FILE), or pass -insecure-no-auth for local development")
	default:
		var err error
		if auth, err = api.ParseKeys(keys); err != nil {
			return err
		}
	}

	faults := cluster.NewFaults(1)
	tr, err := c.Transport(faults)
	if err != nil {
		return err
	}
	client := cluster.NewClient(cluster.ClientConfig{Name: c.Name, Coord: *coord,
		Transport: cluster.WithFaults(tr, faults), GroupSession: *session})
	be := gateway.New(client, nil)
	reg := observability.NewRegistry()
	reg.AddCollector(observability.RuntimeCollector(""))
	srv := api.New(api.Options{Backend: be, Auth: auth, Logger: logger, Metrics: reg,
		GlobalRate: *globalRate, TenantRate: *tenantRate, MaxBodyBytes: *maxBody, RequestTimeout: *reqTimeout,
		MaxLongPoll: *maxPoll, MaxInflight: *maxInflight, MaxPullers: *maxPullers})

	pub, err := c.Bind("api", c.Listen, srv.Handler(), c.TLSCert != "", logger)
	if err != nil {
		return err
	}
	adm, err := c.Bind("admin", c.Admin, c.AdminHandler(reg, be.Ready, faults), false, logger)
	if err != nil {
		pub.Ln.Close()
		return err
	}
	logger.Info("gateway listening", "api", pub.Ln.Addr().String(), "admin", adm.Ln.Addr().String(), "tls", c.TLSCert != "")
	if ready != nil {
		ready(pub.Ln.Addr().String(), adm.Ln.Addr().String())
	}
	return c.Run(ctx, logger, []*service.Listener{pub, adm}, srv.BeginDrain, nil)
}
