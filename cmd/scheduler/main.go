// Command scheduler submits recurring jobs defined in a JSON file. Run several
// replicas for availability: slot-derived idempotency keys prevent duplicates.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/scheduler"
	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) error {
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	fs.SetOutput(stderr)
	api := fs.String("api", "http://127.0.0.1:8080", "API base URL")
	cfgPath := fs.String("config", "", "schedule file (JSON, required)")
	tick := fs.Duration("tick", time.Second, "how often to check schedules")
	level := fs.String("log-level", "info", "log level")
	if err := fs.Parse(args); err != nil {
		return err
	}
	token := getenv("JOBQUEUE_TOKEN")
	if f := getenv("JOBQUEUE_TOKEN_FILE"); f != "" && token == "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		token = strings.TrimSpace(string(b))
	}
	if *cfgPath == "" || token == "" {
		return errors.New("-config and JOBQUEUE_TOKEN (a producer key) are required")
	}
	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		return err
	}
	var cfg scheduler.Config
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("parsing %s: %w", *cfgPath, err)
	}
	logger := observability.NewLogger(stderr, *level, true)
	s, err := scheduler.New(client.New(*api, token), cfg, nil, logger)
	if err != nil {
		return err
	}
	logger.Info("scheduler started", "schedules", len(cfg.Schedules))
	s.Run(ctx, *tick)
	logger.Info("scheduler stopped", "submitted", s.Submitted, "deduplicated", s.Deduplicated)
	return nil
}
