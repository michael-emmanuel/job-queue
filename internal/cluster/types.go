package cluster

import (
	"fmt"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
)

// Key identifies a partition: "queue/index".
func Key(queue string, idx int) string { return fmt.Sprintf("%s/%d", queue, idx) }

// PartitionMeta is the coordinator's record of one partition.
type PartitionMeta struct {
	Index    int      `json:"index"`
	Replicas []string `json:"replicas"` // placement; fixed at creation, replicas[0] is the preferred leader
	Epoch    uint64   `json:"epoch"`    // highest epoch ever issued for this partition
	Leader   string   `json:"leader"`   // "" while an election is pending
}

// QueueMeta is the coordinator's record of one queue.
type QueueMeta struct {
	Name       string             `json:"name"`
	Config     broker.QueueConfig `json:"config"`
	Partitions []PartitionMeta    `json:"partitions"`
}

// Metadata is what clients and tools see.
type Metadata struct {
	Version uint64                `json:"version"`
	Queues  map[string]*QueueMeta `json:"queues"`
	Alive   []string              `json:"alive"`
}

// LedPartition is a node's claim that it leads a partition.
type LedPartition struct {
	Key   string `json:"key"`
	Epoch uint64 `json:"epoch"`
}

// HostedPartition tells a node to keep a replica of a partition.
type HostedPartition struct {
	Key      string             `json:"key"`
	Queue    string             `json:"queue"`
	Index    int                `json:"index"`
	Replicas []string           `json:"replicas"`
	Config   broker.QueueConfig `json:"config"`
}

// HeartbeatReq is a node's periodic report to the coordinator.
type HeartbeatReq struct {
	Node string         `json:"node"`
	Led  []LedPartition `json:"led"`
}

// HeartbeatResp is the coordinator's reply: what to host, which leases were
// renewed (as DURATIONS, so no clock needs to agree with another), and the
// current consumer-group generations.
type HeartbeatResp struct {
	Hosted []HostedPartition        `json:"hosted"`
	Lease  map[string]time.Duration `json:"lease"`
	Groups map[string]uint64        `json:"groups"`
}

// CreateQueueReq creates a queue.
type CreateQueueReq struct {
	Name   string             `json:"name"`
	Config broker.QueueConfig `json:"config"`
}

// PromoteReq tells a node (already promised) to start leading.
type PromoteReq struct {
	Key      string        `json:"key"`
	Epoch    uint64        `json:"epoch"`
	Replicas []string      `json:"replicas"`
	LeaseFor time.Duration `json:"lease_for"`
}

// Group RPCs.
type (
	JoinGroupReq struct {
		Queue, Group, Member string
		SessionTimeout       time.Duration
	}
	GroupHeartbeatReq struct {
		Queue, Group, Member string
	}
	GroupResp struct {
		Generation uint64 `json:"generation"`
		Assigned   []int  `json:"assigned"`
	}
)

// Node RPC payloads. Every request names the partition it targets.
type (
	EnqueueReq struct {
		Key string  `json:"key"`
		Job job.Job `json:"job"`
	}
	EnqueueResp struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	PullReq struct {
		Key        string        `json:"key"`
		Group      string        `json:"group"`
		Member     string        `json:"member"`
		Generation uint64        `json:"generation"`
		Worker     string        `json:"worker"`
		Max        int           `json:"max"`
		Visibility time.Duration `json:"visibility"`
		Wait       time.Duration `json:"wait"`
	}
	PullResp struct {
		Jobs []job.Job `json:"jobs"`
	}
	AckReq struct {
		Key     string `json:"key"`
		ID      string `json:"id"`
		Attempt int    `json:"attempt"`
	}
	FailReq struct {
		Key          string `json:"key"`
		ID           string `json:"id"`
		Attempt      int    `json:"attempt"`
		Err          string `json:"err"`
		NonRetryable bool   `json:"non_retryable"`
	}
	FailResp struct {
		Outcome string `json:"outcome"`
	}
	RenewReq struct {
		Key     string        `json:"key"`
		ID      string        `json:"id"`
		Attempt int           `json:"attempt"`
		Extend  time.Duration `json:"extend"`
	}
	RenewResp struct {
		Deadline time.Time `json:"deadline"`
	}
	JobReq struct {
		Key string `json:"key"`
		ID  string `json:"id"`
	}
	JobResp struct {
		Job job.Job `json:"job"`
	}
	StatsResp struct {
		Stats partition.Stats `json:"stats"`
	}
	DeadReq struct {
		Key   string `json:"key"`
		Limit int    `json:"limit"`
	}
	DeadResp struct {
		Dead []job.DeadLetter `json:"dead"`
	}
)
