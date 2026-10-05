package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func start(t *testing.T, dir string, extra ...string) (api, admin string, stop func() error, logs *lockedBuf) {
	t.Helper()
	logs = &lockedBuf{}
	ctx, cancel := context.WithCancel(context.Background())
	env := map[string]string{"JOBQUEUE_API_KEYS": "admin-key-0123456789:ops:admin,prod-key-01234567890:acme:producer,work-key-012345678901:w:worker"}
	args := append([]string{"-addr", "127.0.0.1:0", "-admin-addr", "127.0.0.1:0", "-data-dir", dir, "-log-level", "debug"}, extra...)
	readyc := make(chan [2]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, args, func(k string) string { return env[k] }, logs, func(a, b string) { readyc <- [2]string{a, b} })
	}()
	select {
	case addrs := <-readyc:
		api, admin = "http://"+addrs[0], "http://"+addrs[1]
	case err := <-done:
		t.Fatalf("run exited early: %v\n%s", err, logs)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not start")
	}
	return api, admin, func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			t.Fatal("shutdown did not complete")
			return nil
		}
	}, logs
}

func call(t *testing.T, method, url, key string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var m map[string]any
	json.Unmarshal(raw, &m)
	return res.StatusCode, m
}

// The whole binary, wired as in production: submit, restart the process
// (graceful), and find the job still there.
func TestBinaryEndToEndWithGracefulRestart(t *testing.T) {
	dir := t.TempDir()
	api, admin, stop, _ := start(t, dir)

	if code, _ := call(t, "POST", api+"/v1/queues", "admin-key-0123456789", map[string]any{"name": "e2e", "partitions": 2}); code != 201 {
		t.Fatalf("create queue: %d", code)
	}
	code, m := call(t, "POST", api+"/v1/jobs", "prod-key-01234567890", map[string]any{"queue": "e2e", "type": "t", "payload": map[string]any{"n": 1}})
	if code != 201 {
		t.Fatalf("submit: %d %v", code, m)
	}
	id := m["job_id"].(string)

	res, err := http.Get(admin + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, want := range []string{"go_goroutines", "process_cpu_seconds_total", "data_disk_free_bytes", `jobqueue_enqueued_total{queue="e2e"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("admin metrics missing %s", want)
		}
	}
	if err := stop(); err != nil {
		t.Fatalf("graceful shutdown returned %v", err)
	}

	api2, _, stop2, logs := start(t, dir)
	code, m = call(t, "POST", api2+"/v1/queues/e2e/pull", "work-key-012345678901", map[string]any{"worker": "w1"})
	jobs, _ := m["jobs"].([]any)
	if code != 200 || len(jobs) != 1 || jobs[0].(map[string]any)["job_id"] != id {
		t.Fatalf("job lost across restart: %d %v", code, m)
	}
	if err := stop2(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), `"msg":"stopped"`) {
		t.Error("no clean stop logged")
	}
}

func TestShutdownIsPromptWithLongPollsOpen(t *testing.T) {
	api, _, stop, _ := start(t, "", "-shutdown-grace", "10s")
	call(t, "POST", api+"/v1/queues", "admin-key-0123456789", map[string]any{"name": "idle"})
	done := make(chan struct{})
	go func() {
		call(t, "POST", api+"/v1/queues/idle/pull", "work-key-012345678901", map[string]any{"worker": "w", "wait": "25s"})
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	begin := time.Now()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("shutdown took %v with an open long poll", d)
	}
	<-done
}

func TestRefusesToStartInsecureByDefault(t *testing.T) {
	err := run(context.Background(), []string{"-data-dir", ""}, func(string) string { return "" }, io.Discard, nil)
	if err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("err = %v", err)
	}
}
