// Command coordinator runs the cluster control plane: partition placement,
// failure detection, leader election and consumer-group coordination.
//
// It is a single process and therefore a single point of failure; see
// docs/design-notes-m3.md for what that does and does not put at risk.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/cluster"
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

func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer, ready func(rpc, admin string)) error {
	var c service.Common
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c.Register(fs, getenv, "127.0.0.1:7000")
	dir := fs.String("dir", "./coordinator-data", "state directory (epochs, queues, group generations)")
	failure := fs.Duration("failure-timeout", 3*time.Second, "declare a node dead after this long without a heartbeat")
	lease := fs.Duration("lease", 5*time.Second, "leader lease; failover takes about max(failure-timeout, lease)")
	tick := fs.Duration("tick", 500*time.Millisecond, "control-plane tick interval")
	rf := fs.Int("default-rf", 3, "default replication factor")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c.Name == "" {
		c.Name = "coord"
	}
	if err := c.Finish(getenv); err != nil {
		return err
	}
	if *lease <= *failure/2 {
		return fmt.Errorf("-lease (%v) should comfortably exceed half of -failure-timeout (%v)", *lease, *failure)
	}
	logger := c.Logger(stderr)
	faults := cluster.NewFaults(1)
	tr, err := c.Transport(faults)
	if err != nil {
		return err
	}
	coord, err := cluster.NewCoordinator(cluster.CoordConfig{
		Dir: *dir, Transport: cluster.WithFaults(tr, faults), FailureTimeout: *failure,
		LeaseDuration: *lease, DefaultRF: *rf, Logger: logger,
	})
	if err != nil {
		return err
	}
	reg := observability.NewRegistry()
	reg.AddCollector(coord.Collect)
	reg.AddCollector(observability.RuntimeCollector(*dir))

	rpc, err := c.Bind("rpc", c.Listen, cluster.RPCHandler(coord.Handler(), c.Secret, faults), c.TLSCert != "", logger)
	if err != nil {
		return err
	}
	adminMux := http.NewServeMux()
	adminMux.Handle("/", c.AdminHandler(reg, func() error { return nil }, faults))
	adminMux.HandleFunc("GET /groups", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(coord.Groups())
	})
	// Plain-text views for operators and demos.
	adminMux.HandleFunc("GET /leaders", func(w http.ResponseWriter, _ *http.Request) {
		m := coord.Metadata()
		names := make([]string, 0, len(m.Queues))
		for n := range m.Queues {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			for _, p := range m.Queues[n].Partitions {
				fmt.Fprintf(w, "%s/%d leader=%s epoch=%d replicas=%s\n", n, p.Index, p.Leader, p.Epoch, strings.Join(p.Replicas, ","))
			}
		}
	})
	adminMux.HandleFunc("GET /groups.txt", func(w http.ResponseWriter, _ *http.Request) {
		for _, g := range coord.Groups() {
			members := make([]string, 0, len(g.Assignment))
			for m := range g.Assignment {
				members = append(members, m)
			}
			sort.Strings(members)
			for _, m := range members {
				ps := make([]string, len(g.Assignment[m]))
				for i, p := range g.Assignment[m] {
					ps[i] = strconv.Itoa(p)
				}
				fmt.Fprintf(w, "%s/%s generation=%d member=%s partitions=%s\n", g.Queue, g.Group, g.Generation, m, strings.Join(ps, ","))
			}
		}
	})
	adminMux.HandleFunc("GET /metadata", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(coord.Metadata())
	})
	adm, err := c.Bind("admin", c.Admin, adminMux, false, logger)
	if err != nil {
		rpc.Ln.Close()
		return err
	}
	logger.Info("coordinator listening", "rpc", rpc.Ln.Addr().String(), "admin", adm.Ln.Addr().String(),
		"failure_timeout", *failure, "lease", *lease, "peers", len(c.Peers))
	if ready != nil {
		ready(rpc.Ln.Addr().String(), adm.Ln.Addr().String())
	}
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { coord.Run(loopCtx, *tick); close(done) }()
	err = c.Run(ctx, logger, []*service.Listener{rpc, adm}, cancel, func() error { <-done; return nil })
	return err
}
