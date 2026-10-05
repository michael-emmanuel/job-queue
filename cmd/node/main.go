// Command node runs a broker node: it hosts partition replicas, serves the
// leader side of the partitions it leads, and replicates to its peers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
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
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c.Register(fs, getenv, "127.0.0.1:7100")
	dir := fs.String("dir", "./node-data", "directory for replica logs")
	coord := fs.String("coord", "coord", "coordinator endpoint name (must appear in -peers)")
	tick := fs.Duration("tick", 500*time.Millisecond, "heartbeat interval to the coordinator")
	repl := fs.Duration("repl-heartbeat", 50*time.Millisecond, "idle replication heartbeat")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := c.Finish(getenv); err != nil {
		return err
	}
	if _, ok := c.Peers[*coord]; !ok {
		return fmt.Errorf("-peers must include the coordinator %q", *coord)
	}
	logger := c.Logger(stderr)
	faults := cluster.NewFaults(1)
	tr, err := c.Transport(faults)
	if err != nil {
		return err
	}
	node := cluster.NewNode(cluster.NodeConfig{Name: c.Name, Dir: *dir, Coord: *coord,
		Transport: cluster.WithFaults(tr, faults), ReplHeartbeat: *repl, Logger: logger})
	reg := observability.NewRegistry()
	reg.AddCollector(node.Collect)
	reg.AddCollector(observability.RuntimeCollector(*dir))

	// Ready once the coordinator has answered recently: a node that cannot
	// reach the control plane cannot renew leases or learn its assignments.
	readyFn := func() error {
		if time.Since(node.LastHeartbeat()) > 4**tick {
			return errors.New("no recent coordinator heartbeat")
		}
		return nil
	}
	rpc, err := c.Bind("rpc", c.Listen, cluster.RPCHandler(node.Handler(), c.Secret, faults), c.TLSCert != "", logger)
	if err != nil {
		return err
	}
	adm, err := c.Bind("admin", c.Admin, c.AdminHandler(reg, readyFn, faults), false, logger)
	if err != nil {
		rpc.Ln.Close()
		return err
	}
	logger.Info("node listening", "rpc", rpc.Ln.Addr().String(), "admin", adm.Ln.Addr().String(), "dir", *dir)
	if ready != nil {
		ready(rpc.Ln.Addr().String(), adm.Ln.Addr().String())
	}
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { node.Run(loopCtx, *tick); close(done) }()
	return c.Run(ctx, logger, []*service.Listener{rpc, adm}, cancel, func() error {
		<-done
		return node.Close() // flushes and fsyncs every replica log
	})
}
