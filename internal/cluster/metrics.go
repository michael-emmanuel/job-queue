package cluster

import (
	"fmt"
	"sort"

	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

// Collect emits replication and queue metrics for every partition this node
// hosts. Register it with Registry.AddCollector.
func (n *Node) Collect(w *observability.Writer) {
	for _, st := range n.Status() {
		l := []string{"node", n.cfg.Name, "partition", st.Key}
		leader := 0.0
		if st.Info.Role == replication.Leader {
			leader = 1
		}
		w.Gauge("jobqueue_replica_is_leader", "1 if this node leads the partition.", leader, l...)
		w.Gauge("jobqueue_replica_epoch", "Highest epoch promised by this replica.", float64(st.Info.Epoch), l...)
		w.Gauge("jobqueue_replica_last_index", "Last log index on this replica.", float64(st.Info.LastIndex), l...)
		w.Gauge("jobqueue_replica_durable_index", "Highest fsynced log index.", float64(st.Info.Durable), l...)
		w.Gauge("jobqueue_replica_commit_index", "Highest index known committed.", float64(st.Info.Commit), l...)
		followers := make([]string, 0, len(st.Info.Lag))
		for f := range st.Info.Lag {
			followers = append(followers, f)
		}
		sort.Strings(followers)
		for _, f := range followers {
			w.Gauge("jobqueue_replica_lag_entries", "Entries a follower is behind its leader.",
				float64(st.Info.Lag[f]), append(l, "follower", f)...)
		}
		if qs := st.QueueStats; qs != nil {
			queue, idx := splitKey(st.Key)
			ql := []string{"queue", queue, "partition", fmt.Sprint(idx)}
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(qs.Ready), append(ql, "state", "ready")...)
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(qs.Scheduled), append(ql, "state", "scheduled")...)
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(qs.InFlight), append(ql, "state", "in_flight")...)
			w.Gauge("jobqueue_partition_jobs", "Jobs by state.", float64(qs.Dead), append(ql, "state", "dead")...)
		}
	}
}

// Collect emits control-plane metrics.
func (c *Coordinator) Collect(w *observability.Writer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Clock.Now()
	w.Gauge("jobqueue_cluster_alive_nodes", "Nodes with a recent heartbeat.", float64(len(c.aliveNodesLocked(now))))
	w.Counter("jobqueue_leader_elections_total", "Successful partition leader elections (leader changes).", float64(c.elections))
	w.Counter("jobqueue_failed_elections_total", "Elections that could not reach a majority or promote a leader.", float64(c.failedElections))
	w.Counter("jobqueue_group_rebalances_total", "Consumer-group rebalances.", float64(c.groups.Rebalances))
	leaderless := 0
	for _, q := range c.state.Queues {
		for _, p := range q.Partitions {
			if p.Leader == "" {
				leaderless++
			}
			w.Gauge("jobqueue_partition_epoch", "Highest epoch issued for the partition.", float64(p.Epoch),
				"queue", q.Name, "partition", fmt.Sprint(p.Index))
		}
	}
	w.Gauge("jobqueue_leaderless_partitions", "Partitions with no leader: unavailable until elected.", float64(leaderless))
	for _, v := range c.groups.Views() {
		w.Gauge("jobqueue_group_members", "Members in a consumer group.", float64(len(v.Assignment)), "queue", v.Queue, "group", v.Group)
		w.Gauge("jobqueue_group_generation", "Current consumer-group generation.", float64(v.Generation), "queue", v.Queue, "group", v.Group)
	}
}
