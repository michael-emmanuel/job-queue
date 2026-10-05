// Command loadtest drives an open-loop, staged load against the HTTP API and
// reports throughput, latency percentiles, queue depth and resource use.
//
// It never invents numbers: every result carries the environment it ran in,
// and the tool reports requests it could not issue (generator saturated)
// instead of silently dropping them. See docs/performance.md for how to read
// the output and what the single-machine numbers do and do not mean.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/loadgen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	api := fs.String("api", "http://127.0.0.1:8080", "API base URL")
	queue := fs.String("queue", "loadtest", "queue to use")
	rates := fs.String("rates", "10,100,1000", "comma-separated target jobs/second, one stage each")
	dur := fs.Duration("stage-duration", 10*time.Second, "length of each stage")
	payload := fs.Int("payload-bytes", 256, "padding bytes in each job payload")
	conc := fs.Int("concurrency", 256, "max in-flight submissions")
	consumers := fs.Int("consumers", 4, "embedded consumers (0 = produce only)")
	batch := fs.Int("consumer-batch", 50, "jobs per pull")
	group := fs.String("group", "", "consumer group (cluster mode)")
	drain := fs.Duration("drain-timeout", 60*time.Second, "wait this long for the queue to empty after each stage")
	metrics := fs.String("metrics-url", "", "comma-separated /metrics URLs to sample CPU, memory, worker utilization")
	idem := fs.Bool("idempotency", true, "attach a unique idempotency key to each job")
	create := fs.Bool("create-queue", false, "create the queue first (needs JOBQUEUE_ADMIN_TOKEN)")
	partitions := fs.Int("partitions", 4, "partitions when creating the queue")
	rf := fs.Int("replication-factor", 0, "replication factor when creating the queue (cluster)")
	ack := fs.String("ack", "", "ack level when creating the queue: quorum, leader or all (cluster)")
	maxDepth := fs.Int("max-depth", 5_000_000, "max_depth when creating the queue")
	out := fs.String("out", "", "write full results as JSON to this file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token, admin := getenv("JOBQUEUE_TOKEN"), getenv("JOBQUEUE_ADMIN_TOKEN")
	if token == "" {
		token = admin
	}
	if token == "" {
		return errors.New("set JOBQUEUE_TOKEN (a producer+worker key; an admin key works)")
	}
	var rs []float64
	for _, f := range strings.Split(*rates, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil || v <= 0 {
			return fmt.Errorf("bad rate %q", f)
		}
		rs = append(rs, v)
	}
	if *create {
		if admin == "" {
			return errors.New("-create-queue needs JOBQUEUE_ADMIN_TOKEN")
		}
		cfg := map[string]any{"name": *queue, "partitions": *partitions, "max_depth": *maxDepth}
		if *rf > 0 {
			cfg["replication_factor"] = *rf
		}
		if *ack != "" {
			cfg["ack"] = *ack
		}
		if err := createQueue(ctx, *api, admin, cfg); err != nil {
			return err
		}
	}
	var urls []string
	for _, u := range strings.Split(*metrics, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	res, err := loadgen.Run(ctx, loadgen.Config{API: strings.TrimRight(*api, "/"), Token: token, AdminToken: admin, Queue: *queue,
		Rates: rs, StageDuration: *dur, PayloadBytes: *payload, Concurrency: *conc, Consumers: *consumers,
		ConsumerBatch: *batch, Group: *group, DrainTimeout: *drain, MetricsURLs: urls, Idempotency: *idem}, stderr)
	printReport(stdout, res)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if werr := os.WriteFile(*out, b, 0o644); werr != nil {
			return werr
		}
	}
	return err
}

func createQueue(ctx context.Context, api, token string, cfg map[string]any) error {
	b, _ := json.Marshal(cfg)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(api, "/")+"/v1/queues", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 201 && res.StatusCode != 409 { // 409: already exists, fine for reruns
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("creating queue: HTTP %d %s", res.StatusCode, raw)
	}
	return nil
}

func printReport(w io.Writer, r loadgen.Result) {
	fmt.Fprintf(w, "\nEnvironment: %s %s/%s, %d CPU(s), GOMAXPROCS=%d, started %s\n",
		r.Env.GoVersion, r.Env.GOOS, r.Env.GOARCH, r.Env.NumCPU, r.Env.GOMAXPROCS, r.Env.StartedAt)
	fmt.Fprintf(w, "Config: %v\n\n", r.Config)
	fmt.Fprintf(w, "%8s %9s %7s %6s %6s %8s | %8s %8s %8s | %9s %9s | %8s %6s\n",
		"target/s", "achieved", "ok", "429", "5xx", "not_sent", "sub p50", "sub p95", "sub p99", "e2e p50", "e2e p99", "max depth", "drain")
	for _, s := range r.Stages {
		fmt.Fprintf(w, "%8.0f %9.0f %7d %6d %6d %8d | %6.1fms %6.1fms %6.1fms | %7.1fms %7.1fms | %8d %5.1fs\n",
			s.TargetRate, s.AchievedRate, s.OK, s.Rejected, s.Unavailable+s.OtherErrors, s.NotSent,
			s.Submit.P50, s.Submit.P95, s.Submit.P99, s.EndToEnd.P50, s.EndToEnd.P99, s.MaxDepth, s.DrainSec)
		for _, rs := range s.Resources {
			fmt.Fprintf(w, "           resources %-34s cpu=%.2f cores  heap=%.0fMB  goroutines=%.0f\n", rs.Target, rs.CPUCores, rs.MaxAllocMB, rs.Goroutines)
		}
	}
}
