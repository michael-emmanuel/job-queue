// Command jqctl is a small operator CLI for the job queue API.
//
//	jqctl queue-create NAME [flags]     create a queue
//	jqctl queue-stats NAME              one-line counts
//	jqctl partitions NAME               per-partition counts (skew)
//	jqctl submit -queue Q [flags]       submit one or many jobs
//	jqctl get ID                        job status
//	jqctl dlq list|replay|delete Q [ID]
//	jqctl wait-empty NAME [-timeout D]  block until the queue is drained
//
// Environment: JOBQUEUE_API (default http://127.0.0.1:8080) and JOBQUEUE_TOKEN.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, out, errw io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: jqctl <queue-create|queue-stats|partitions|submit|get|dlq|wait-empty> ...")
	}
	api := getenv("JOBQUEUE_API")
	if api == "" {
		api = "http://127.0.0.1:8080"
	}
	token := getenv("JOBQUEUE_TOKEN")
	if token == "" {
		return errors.New("set JOBQUEUE_TOKEN")
	}
	c := client.New(api, token)
	cmd, rest := args[0], args[1:]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(errw)

	switch cmd {
	case "queue-create":
		parts := fs.Int("partitions", 0, "partitions")
		rf := fs.Int("rf", 0, "replication factor (cluster)")
		ack := fs.String("ack", "", "quorum|leader|all (cluster)")
		maxDepth := fs.Int("max-depth", 0, "max_depth")
		attempts := fs.Int("attempts", 0, "max delivery attempts")
		base := fs.String("base-delay", "", "retry base delay")
		maxd := fs.String("max-delay", "", "retry max delay")
		jitter := fs.String("jitter", "", "none|equal|full")
		vis := fs.String("visibility", "", "default visibility timeout")
		enq := fs.Float64("enqueue-rate", 0, "queue enqueue rate limit (jobs/s)")
		name, flags := splitName(rest)
		if err := fs.Parse(flags); err != nil {
			return err
		}
		q := client.QueueConfig{Name: name, Partitions: *parts, ReplicationFactor: *rf, Ack: *ack,
			MaxDepth: *maxDepth, DefaultVisibility: *vis, EnqueueRate: *enq}
		if *attempts > 0 || *base != "" || *maxd != "" || *jitter != "" {
			q.Retry = &struct {
				MaxAttempts int    `json:"max_attempts,omitempty"`
				BaseDelay   string `json:"base_delay,omitempty"`
				MaxDelay    string `json:"max_delay,omitempty"`
				Jitter      string `json:"jitter,omitempty"`
			}{*attempts, *base, *maxd, *jitter}
		}
		if err := c.CreateQueue(ctx, q); err != nil {
			return err
		}
		fmt.Fprintf(out, "queue %s created\n", name)
	case "queue-stats", "partitions":
		jsonOut := fs.Bool("json", false, "raw JSON")
		name, flags := splitName(rest)
		if err := fs.Parse(flags); err != nil {
			return err
		}
		s, err := c.QueueStats(ctx, name)
		if err != nil {
			return err
		}
		if *jsonOut {
			return json.NewEncoder(out).Encode(s)
		}
		if cmd == "partitions" {
			for i, p := range s.Partitions {
				fmt.Fprintf(out, "partition %d: enqueued=%d ready=%d in_flight=%d\n", i, p.Enqueued, p.Ready, p.InFlight)
			}
			return nil
		}
		t := s.Total
		fmt.Fprintf(out, "ready=%d scheduled=%d in_flight=%d dead=%d enqueued=%d acked=%d retried=%d dead_lettered=%d lease_expired=%d rejected=%d\n",
			t.Ready, t.Scheduled, t.InFlight, t.Dead, t.Enqueued, t.Acked, t.Retried, t.DeadLettered, t.LeaseExpired, t.Rejected)
	case "submit":
		queue := fs.String("queue", "", "queue")
		typ := fs.String("type", "demo", "job type")
		payload := fs.String("payload", "{}", "JSON payload")
		key := fs.String("key", "", "idempotency key (with -count>1 the index is appended)")
		pkey := fs.String("partition-key", "", "partition key")
		count := fs.Int("count", 1, "number of jobs")
		prio := fs.Int("priority", 0, "priority 1-10")
		noRetry := fs.Bool("no-retry", false, "do not retry rejected requests (shows raw backpressure)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *noRetry {
			c.Retry = client.RetryPolicy{MaxAttempts: 1}
		}
		if *queue == "" {
			return errors.New("-queue is required")
		}
		var raw json.RawMessage
		if err := json.Unmarshal([]byte(*payload), &raw); err != nil {
			return fmt.Errorf("-payload is not valid JSON: %w", err)
		}
		var accepted, dup, rejected, failed int
		var last client.SubmitResult
		for i := 0; i < *count; i++ {
			k := *key
			if k != "" && *count > 1 {
				k = fmt.Sprintf("%s-%d", k, i)
			}
			res, err := c.Submit(ctx, client.SubmitRequest{Queue: *queue, Type: *typ, Payload: raw, Priority: *prio,
				PartitionKey: *pkey, IdempotencyKey: k})
			var ae *client.APIError
			switch {
			case err == nil && res.Duplicate:
				dup++
				last = res
			case err == nil:
				accepted++
				last = res
			case errors.As(err, &ae) && ae.Rejected():
				rejected++
			default:
				failed++
			}
		}
		if *count == 1 && last.JobID == "" {
			// A single submission that was not accepted must fail the command;
			// scripts rely on the exit status to know whether to retry.
			return fmt.Errorf("submit not accepted (rejected=%d failed=%d)", rejected, failed)
		}
		if *count == 1 {
			fmt.Fprintf(out, "%s duplicate=%v\n", last.JobID, last.Duplicate)
		} else {
			fmt.Fprintf(out, "accepted=%d duplicate=%d rejected=%d failed=%d\n", accepted, dup, rejected, failed)
		}
	case "get":
		j, err := c.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(j)
	case "dlq":
		if len(rest) < 2 {
			return errors.New("usage: jqctl dlq list|replay|delete QUEUE [ID]")
		}
		sub, queue := rest[0], rest[1]
		switch sub {
		case "list":
			d, err := c.DeadLetters(ctx, queue, 100)
			if err != nil {
				return err
			}
			for _, e := range d {
				fmt.Fprintf(out, "%s reason=%s attempts=%d worker=%s error=%q\n", e.JobID, e.Reason, e.Attempts, e.WorkerID, e.LastError)
			}
			fmt.Fprintf(out, "%d dead-lettered\n", len(d))
		case "replay", "delete":
			if len(rest) < 3 {
				return errors.New("an ID is required")
			}
			var err error
			if sub == "replay" {
				err = c.ReplayDead(ctx, queue, rest[2])
			} else {
				err = c.DeleteDead(ctx, queue, rest[2])
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "%s %sed\n", rest[2], strings.TrimSuffix(sub, "e"))
		default:
			return fmt.Errorf("unknown dlq command %q", sub)
		}
	case "wait-empty":
		timeout := fs.Duration("timeout", time.Minute, "give up after")
		name, flags := splitName(rest)
		if err := fs.Parse(flags); err != nil {
			return err
		}
		deadline := time.Now().Add(*timeout)
		for {
			s, err := c.QueueStats(ctx, name)
			if err == nil && s.Total.Ready+s.Total.Scheduled+s.Total.InFlight == 0 {
				fmt.Fprintln(out, "queue is empty")
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("queue %s not empty after %v (%+v)", name, *timeout, s.Total)
			}
			time.Sleep(200 * time.Millisecond)
		}
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	return nil
}

// splitName takes the NAME, which must come first ("queue-create q1 -partitions 3").
func splitName(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
}
