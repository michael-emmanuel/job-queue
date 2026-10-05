package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/ratelimit"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// Config configures a Broker.
type Config struct {
	// DataDir holds queue metadata and partition logs. Empty means fully
	// in-memory with no persistence (tests, benchmarks).
	DataDir      string
	Sync         storage.SyncPolicy
	SyncEvery    time.Duration
	SegmentBytes int64
	// CompactBytes enables automatic log compaction per partition (0 = off).
	CompactBytes int64

	// AutoCreate creates unknown queues on first use with DefaultQueue.
	// Convenient for development; in production queues should be created
	// explicitly so a typo in a queue name cannot silently create one.
	AutoCreate   bool
	DefaultQueue QueueConfig

	Clock  clock.Clock
	Logger *slog.Logger
}

// Broker owns every queue on this node.
type Broker struct {
	cfg Config

	mu     sync.RWMutex
	queues map[string]*Queue
	closed bool
}

type metaFile struct {
	Version int                    `json:"version"`
	Queues  map[string]queueRecord `json:"queues"`
}

type queueRecord struct {
	Config    QueueConfig `json:"config"`
	CreatedAt time.Time   `json:"created_at"`
}

// Open loads queue metadata from DataDir and recovers every partition.
func Open(cfg Config) (*Broker, error) {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = observability.Discard()
	}
	cfg.DefaultQueue = cfg.DefaultQueue.withDefaults(DefaultQueueConfig())
	if err := cfg.DefaultQueue.validate(); err != nil {
		return nil, fmt.Errorf("broker: default queue config: %w", err)
	}
	b := &Broker{cfg: cfg, queues: map[string]*Queue{}}
	if cfg.DataDir == "" {
		return b, nil
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	meta, err := b.loadMeta()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(meta.Queues))
	for n := range meta.Queues {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		q, err := b.openQueue(n, meta.Queues[n].Config)
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("broker: recovering queue %q: %w", n, err)
		}
		q.created = meta.Queues[n].CreatedAt
		b.queues[n] = q
	}
	cfg.Logger.Info("broker opened", "queues", len(b.queues), "data_dir", cfg.DataDir)
	return b, nil
}

func (b *Broker) metaPath() string { return filepath.Join(b.cfg.DataDir, "queues.json") }

func (b *Broker) queueDir(name string) string {
	return filepath.Join(b.cfg.DataDir, "queues", name)
}

func (b *Broker) loadMeta() (metaFile, error) {
	m := metaFile{Version: 1, Queues: map[string]queueRecord{}}
	data, err := os.ReadFile(b.metaPath())
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, fmt.Errorf("broker: corrupt %s: %w", b.metaPath(), err)
	}
	return m, nil
}

// saveMeta atomically replaces the metadata file: write temp, fsync, rename,
// fsync the directory. A crash leaves either the old or the new file.
func (b *Broker) saveMeta(m metaFile) error {
	if b.cfg.DataDir == "" {
		return nil
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := b.metaPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(tmp)
		return werr
	}
	if err := os.Rename(tmp, b.metaPath()); err != nil {
		return err
	}
	d, err := os.Open(b.cfg.DataDir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (b *Broker) snapshotMetaLocked(extra map[string]queueRecord, without string) metaFile {
	m := metaFile{Version: 1, Queues: map[string]queueRecord{}}
	for n, q := range b.queues {
		if n != without {
			m.Queues[n] = queueRecord{Config: q.cfg, CreatedAt: q.created}
		}
	}
	for n, r := range extra {
		m.Queues[n] = r
	}
	return m
}

// openQueue opens every partition of a queue and starts its Run goroutines.
func (b *Broker) openQueue(name string, cfg QueueConfig) (*Queue, error) {
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		name:   name,
		cfg:    cfg,
		cancel: cancel,
		enqLim: ratelimit.NewBucket(b.cfg.Clock, cfg.EnqueueRate, cfg.EnqueueBurst),
		conLim: ratelimit.NewBucket(b.cfg.Clock, cfg.ConsumeRate, cfg.ConsumeBurst),
	}
	for i := 0; i < cfg.Partitions; i++ {
		var lg storage.Log
		if b.cfg.DataDir == "" {
			lg = storage.NewMemLog()
		} else {
			lg, err = storage.Open(storage.Options{
				Dir:          filepath.Join(b.queueDir(name), fmt.Sprintf("p%03d", i)),
				SegmentBytes: b.cfg.SegmentBytes,
				Sync:         b.cfg.Sync,
				SyncEvery:    b.cfg.SyncEvery,
			})
			if err != nil {
				break
			}
		}
		var pc partition.Config
		if pc, err = cfg.PartitionConfig(name, i); err != nil {
			lg.Close()
			break
		}
		pc.Log, pc.Clock, pc.CompactMinBytes, pc.OnReady = lg, b.cfg.Clock, b.cfg.CompactBytes, q.sig.fire
		pc.Logger = b.cfg.Logger.With("queue", name)
		var p *partition.Partition
		p, err = partition.Open(pc)
		if err != nil {
			lg.Close()
			break
		}
		q.parts = append(q.parts, p)
	}
	if err != nil {
		cancel()
		for _, p := range q.parts {
			p.Close()
		}
		return nil, err
	}
	for _, p := range q.parts {
		q.wg.Add(1)
		go func(p *partition.Partition) {
			defer q.wg.Done()
			p.Run(ctx) // returns when the queue is closed
		}(p)
	}
	return q, nil
}

// CreateQueue creates a queue. Unset config fields take the broker defaults.
func (b *Broker) CreateQueue(name string, cfg QueueConfig) (QueueConfig, error) {
	if !ValidName(name) {
		return QueueConfig{}, fmt.Errorf("%w: name must match %s", ErrInvalidQueue, nameRE)
	}
	cfg = cfg.withDefaults(b.cfg.DefaultQueue)
	if err := cfg.validate(); err != nil {
		return QueueConfig{}, fmt.Errorf("%w: %v", ErrInvalidQueue, err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return QueueConfig{}, ErrClosed
	}
	if _, ok := b.queues[name]; ok {
		return QueueConfig{}, ErrQueueExists
	}
	// Metadata is persisted BEFORE any partition files exist, so a crash can
	// leave a registered queue with no data (harmless: recovered empty) but
	// never data belonging to an unregistered queue.
	rec := queueRecord{Config: cfg, CreatedAt: b.cfg.Clock.Now()}
	if err := b.saveMeta(b.snapshotMetaLocked(map[string]queueRecord{name: rec}, "")); err != nil {
		return QueueConfig{}, err
	}
	if b.cfg.DataDir != "" {
		// A directory with this name that is not in the metadata is the
		// leftover of a deleted queue; reusing it would resurrect its jobs.
		if err := os.RemoveAll(b.queueDir(name)); err != nil {
			return QueueConfig{}, err
		}
	}
	q, err := b.openQueue(name, cfg)
	if err != nil {
		_ = b.saveMeta(b.snapshotMetaLocked(nil, name))
		return QueueConfig{}, err
	}
	q.created = rec.CreatedAt
	b.queues[name] = q
	b.cfg.Logger.Info("queue created", "queue", name, "partitions", cfg.Partitions)
	return cfg, nil
}

// DeleteQueue removes a queue and all of its data. Pending long-polls
// return an error.
func (b *Broker) DeleteQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	q, ok := b.queues[name]
	if !ok {
		return ErrQueueNotFound
	}
	// Deregister durably first; removing the directory afterwards is cleanup
	// that CreateQueue repeats if a crash interrupts it.
	if err := b.saveMeta(b.snapshotMetaLocked(nil, name)); err != nil {
		return err
	}
	delete(b.queues, name)
	err := q.close()
	if b.cfg.DataDir != "" {
		if rerr := os.RemoveAll(b.queueDir(name)); err == nil {
			err = rerr
		}
	}
	b.cfg.Logger.Info("queue deleted", "queue", name)
	return err
}

// Queue returns a queue by name, creating it if AutoCreate is on.
func (b *Broker) Queue(name string) (*Queue, error) {
	b.mu.RLock()
	q, ok := b.queues[name]
	closed := b.closed
	b.mu.RUnlock()
	switch {
	case closed:
		return nil, ErrClosed
	case ok:
		return q, nil
	case !ValidName(name):
		return nil, fmt.Errorf("%w: %q", ErrQueueNotFound, name)
	case !b.cfg.AutoCreate:
		return nil, ErrQueueNotFound
	}
	if _, err := b.CreateQueue(name, QueueConfig{}); err != nil && !errors.Is(err, ErrQueueExists) {
		return nil, err
	}
	return b.Queue(name)
}

// QueueInfo is a listing entry.
type QueueInfo struct {
	Name   string      `json:"name"`
	Config QueueConfig `json:"config"`
}

// ListQueues returns all queues sorted by name.
func (b *Broker) ListQueues() []QueueInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]QueueInfo, 0, len(b.queues))
	for n, q := range b.queues {
		out = append(out, QueueInfo{Name: n, Config: q.cfg})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (b *Broker) queueList() []*Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// locate resolves a job ID to its partition. It never auto-creates queues:
// an ID can only exist if its queue does.
func (b *Broker) locate(id string) (*partition.Partition, Ref, error) {
	ref, err := parseID(id)
	if err != nil {
		return nil, ref, err
	}
	b.mu.RLock()
	q, ok := b.queues[ref.Queue]
	closed := b.closed
	b.mu.RUnlock()
	switch {
	case closed:
		return nil, ref, ErrClosed
	case !ok:
		return nil, ref, ErrQueueNotFound
	case ref.Partition >= len(q.parts):
		return nil, ref, ErrInvalidID
	}
	return q.parts[ref.Partition], ref, nil
}

// Locate reports which queue and partition own a job ID without touching it.
func (b *Broker) Locate(id string) (Ref, error) {
	_, ref, err := b.locate(id)
	return ref, err
}

// Get returns a live or dead-lettered job. Completed jobs are not retained.
func (b *Broker) Get(id string) (job.Job, error) {
	p, _, err := b.locate(id)
	if err != nil {
		return job.Job{}, err
	}
	return p.Get(id)
}

// Ack completes a job.
func (b *Broker) Ack(ctx context.Context, id string, attempt int) error {
	p, _, err := b.locate(id)
	if err != nil {
		return err
	}
	return p.Ack(ctx, id, attempt)
}

// Fail reports a processing failure.
func (b *Broker) Fail(ctx context.Context, id string, attempt int, f partition.Failure) (partition.Outcome, error) {
	p, _, err := b.locate(id)
	if err != nil {
		return "", err
	}
	return p.Fail(ctx, id, attempt, f)
}

// Renew extends a lease.
func (b *Broker) Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error) {
	p, _, err := b.locate(id)
	if err != nil {
		return time.Time{}, err
	}
	return p.Renew(ctx, id, attempt, extend)
}

// Cancel removes a live job.
func (b *Broker) Cancel(ctx context.Context, id string) error {
	p, _, err := b.locate(id)
	if err != nil {
		return err
	}
	return p.Cancel(ctx, id)
}

// ReplayDead moves a dead-lettered job back to ready.
func (b *Broker) ReplayDead(ctx context.Context, id string) error {
	p, _, err := b.locate(id)
	if err != nil {
		return err
	}
	return p.ReplayDead(ctx, id)
}

// DeleteDead permanently removes a dead-lettered job.
func (b *Broker) DeleteDead(ctx context.Context, id string) error {
	p, _, err := b.locate(id)
	if err != nil {
		return err
	}
	return p.DeleteDead(ctx, id)
}

// Ready reports whether the broker can serve traffic: it is open and no
// partition log has failed.
func (b *Broker) Ready() error {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	for _, q := range b.queueList() {
		for i, p := range q.parts {
			if err := p.Healthy(); err != nil {
				return fmt.Errorf("partition %s/%d: %w", q.name, i, err)
			}
		}
	}
	return nil
}

// Close stops every queue and makes all logs durable. It is safe to call
// once the HTTP server has stopped accepting requests.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	qs := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		qs = append(qs, q)
	}
	b.mu.Unlock()
	var first error
	for _, q := range qs {
		if err := q.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Collect emits per-queue and per-partition metrics.
func (b *Broker) Collect(w *observability.Writer) {
	for _, q := range b.queueList() {
		s := q.Stats()
		w.Gauge("jobqueue_queue_partitions", "Partitions in the queue.", float64(len(s.Partitions)), "queue", q.name)
		for i, ps := range s.Partitions {
			l := []string{"queue", q.name, "partition", fmt.Sprint(i)}
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(ps.Ready), append(l, "state", "ready")...)
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(ps.Scheduled), append(l, "state", "scheduled")...)
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(ps.InFlight), append(l, "state", "in_flight")...)
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(ps.Dead), append(l, "state", "dead")...)
			w.Gauge("jobqueue_partition_log_bytes", "Bytes held by the partition log.", float64(ps.LogBytes), l...)
			w.Counter("jobqueue_enqueued_total", "Jobs accepted.", float64(ps.Enqueued), l...)
			w.Counter("jobqueue_duplicates_total", "Enqueues suppressed by an idempotency key.", float64(ps.Duplicates), l...)
			w.Counter("jobqueue_rejected_total", "Enqueues rejected because the partition was full.", float64(ps.Rejected), l...)
			w.Counter("jobqueue_leased_total", "Deliveries to workers (attempts).", float64(ps.Leased), l...)
			w.Counter("jobqueue_acked_total", "Jobs completed.", float64(ps.Acked), l...)
			w.Counter("jobqueue_retried_total", "Failures scheduled for retry.", float64(ps.Retried), l...)
			w.Counter("jobqueue_dead_lettered_total", "Jobs moved to the DLQ.", float64(ps.DeadLettered), l...)
			w.Counter("jobqueue_lease_expired_total", "Visibility timeouts that fired.", float64(ps.Expired), l...)
			w.Counter("jobqueue_compactions_total", "Log compactions.", float64(ps.Compactions), l...)
		}
	}
}
