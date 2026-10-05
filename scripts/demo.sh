#!/usr/bin/env bash
# Reproducible demonstrations. Each demo starts its own local cluster
# (scripts/cluster.sh), provokes a behaviour, and CHECKS that the system
# behaved as documented in docs/demo.md. A demo that does not behave exits
# non-zero instead of printing a reassuring story.
#
#   scripts/demo.sh <name>     one of: normal worker-failure duplicate retry-storm dlq
#                              leader-failure replica-failure rebalance backpressure
#                              hot-partition load shutdown net-partition freeze
#   scripts/demo.sh all
set -uo pipefail
set +m  # no job-control chatter when workers are killed on purpose
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=$ROOT/bin
CL=$ROOT/scripts/cluster.sh
STATE=$ROOT/.cluster
FAILS=0
declare -A WPID

say()  { printf '\n==== %s ====\n' "$*"; }
step() { printf -- '-- %s\n' "$*"; }
ok()   { printf '  PASS: %s\n' "$*"; }
bad()  { printf '  FAIL: %s\n' "$*"; FAILS=$((FAILS+1)); }
expect_eq() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1: got '$2', want '$3'"; fi; }
expect_ge() { if [ "${2:-0}" -ge "$3" ] 2>/dev/null; then ok "$1 ($2 >= $3)"; else bad "$1: got '${2:-}', want >= $3"; fi; }
expect_le() { if [ "${2:-0}" -le "$3" ] 2>/dev/null; then ok "$1 ($2 <= $3)"; else bad "$1: got '${2:-}', want <= $3"; fi; }
field() { echo "$1" | tr ' ' '\n' | grep "^$2=" | head -1 | cut -d= -f2; }
now() { date +%s.%N; }
since() { echo "$(now) - $1" | bc | sed 's/^\./0./'; }

begin() {
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...) || exit 1
  "$CL" clean >/dev/null 2>&1
  # Short timers keep the demos quick; production defaults are 3s/5s.
  export FAILURE_TIMEOUT=2s LEASE=3s GATEWAY_FLAGS="${GATEWAY_FLAGS:--group-session 4s}"
  "$CL" up | tail -1
  # shellcheck disable=SC1091
  source "$STATE/env"
  export JOBQUEUE_API=http://127.0.0.1:8080 JOBQUEUE_TOKEN=$ADMIN_KEY
}
end() {
  for n in "${!WPID[@]}"; do disown "${WPID[$n]}" 2>/dev/null; kill -KILL "${WPID[$n]}" 2>/dev/null; done; WPID=()
  "$CL" down >/dev/null 2>&1
}
ctl() { "$BIN/jqctl" "$@"; }
stats() { ctl queue-stats "$1"; }

# start_worker NAME QUEUE [worker flags...]
start_worker() {
  local name=$1 queue=$2; shift 2
  JOBQUEUE_TOKEN=$WORK_KEY "$BIN/worker" -api "$JOBQUEUE_API" -queue "$queue" -name "$name" -long-poll 2s -log-level info "$@" \
    >>"$STATE/logs/worker-$name.log" 2>&1 &
  WPID[$name]=$!
}
hard_kill() { # kill -9 without shell job-control chatter
  local n=$1; disown "${WPID[$n]}" 2>/dev/null; kill -KILL "${WPID[$n]}" 2>/dev/null; unset "WPID[$n]"
}
stop_worker() { # graceful
  local name=$1; kill -TERM "${WPID[$name]}" 2>/dev/null; wait "${WPID[$name]}" 2>/dev/null; local rc=$?; unset "WPID[$name]"; return $rc
}
leaders() { curl -s 127.0.0.1:9000/leaders; }
leader_of() { leaders | grep "^$1/$2 " | sed 's/.*leader=\([^ ]*\).*/\1/'; }
admin_port() { case $1 in n1) echo 9101;; n2) echo 9102;; n3) echo 9103;; esac; }

# ---------------------------------------------------------------------------
demo_normal() {
  say "Demo 1: normal processing"
  begin
  ctl queue-create q1 -partitions 6 -rf 3 >/dev/null
  for w in w1 w2 w3; do start_worker $w q1 -group g -concurrency 4; done
  sleep 1.5
  step "submit 60 jobs"
  ctl submit -queue q1 -count 60 -key demo1 -payload '{"sleep_ms":10}'
  ctl wait-empty q1 -timeout 30s
  s=$(stats q1); echo "$s"
  expect_eq "all jobs acknowledged" "$(field "$s" acked)" 60
  expect_eq "no retries" "$(field "$s" retried)" 0
  expect_eq "nothing dead-lettered" "$(field "$s" dead)" 0
  step "work was spread across partitions (each partition has its own leader node)"
  leaders
  ctl partitions q1
  end
}

demo_worker_failure() {
  say "Demo 2: worker failure (visibility timeout and redelivery)"
  begin
  ctl queue-create q2 -partitions 3 -rf 3 >/dev/null
  start_worker w1 q2 -group g -concurrency 4 -visibility 3s
  start_worker w2 q2 -group g -concurrency 4 -visibility 3s
  sleep 1.5
  ctl submit -queue q2 -count 16 -key demo2 -payload '{"sleep_ms":1500}'
  sleep 1
  step "kill -9 worker w2 while it is holding jobs"
  hard_kill w2
  ctl wait-empty q2 -timeout 40s
  s=$(stats q2); echo "$s"
  expect_eq "every job completed despite the crash" "$(field "$s" acked)" 16
  expect_ge "leases expired and jobs were redelivered" "$(field "$s" lease_expired)" 1
  expect_eq "nothing was lost to the DLQ" "$(field "$s" dead)" 0
  end
}

demo_duplicate() {
  say "Demo 3: duplicate delivery and the idempotent handler"
  begin
  ctl queue-create q3 -partitions 1 -rf 3 >/dev/null
  mkdir -p "$STATE/effects"; rm -f "$STATE/effects"/*
  start_worker crasher q3 -concurrency 1 -visibility 3s -allow-crash -effects-dir "$STATE/effects"
  sleep 1
  step "submit one job: perform the side effect, then crash BEFORE acknowledging"
  ctl submit -queue q3 -key invoice-42 -payload '{"effect":"invoice-42","crash_after_effect":true}'
  wait "${WPID[crasher]}" 2>/dev/null; unset 'WPID[crasher]'
  step "worker crashed; the queue cannot know the side effect already happened"
  stats q3
  step "a healthy worker takes over after the visibility timeout"
  start_worker healthy q3 -concurrency 1 -visibility 3s -effects-dir "$STATE/effects"
  ctl wait-empty q3 -timeout 30s
  s=$(stats q3); echo "$s"
  expect_eq "job completed" "$(field "$s" acked)" 1
  expect_ge "the lease expired and the job was delivered again" "$(field "$s" lease_expired)" 1
  expect_eq "side effect applied exactly once" "$(ls "$STATE/effects" | wc -l | tr -d ' ')" 1
  if grep -q "DUPLICATE DELIVERY DETECTED" "$STATE/logs/worker-healthy.log"; then ok "the redelivery was detected and the effect skipped"; else bad "no duplicate detection logged"; fi
  grep -h "EFFECT APPLIED\|DUPLICATE DELIVERY\|CRASHING" "$STATE"/logs/worker-crasher.log "$STATE"/logs/worker-healthy.log | sed 's/^/    /' | cut -c1-170
  end
}

demo_retry_storm() {
  say "Demo 4: retry-storm prevention (jitter)"
  begin
  run_storm() { # queue jitter
    ctl queue-create "$1" -partitions 3 -rf 3 -attempts 3 -base-delay 4s -max-delay 8s -jitter "$2" >/dev/null
    start_worker "storm-$2" "$1" -concurrency 300 -visibility 20s -report-retries
    sleep 1.5
    # 300 jobs that all fail together, as if a shared dependency just went down.
    ctl submit -queue "$1" -count 300 -key "$1" -payload '{"sleep_ms":1000,"fail_until_attempt":2}' >/dev/null
    ctl wait-empty "$1" -timeout 60s >/dev/null
    stop_worker "storm-$2"
    grep -A40 "RETRY ARRIVALS" "$STATE/logs/worker-storm-$2.log" | grep -v '^{' | head -30
  }
  step "without jitter: every failed job retries after exactly the same backoff"
  run_storm storm-none none
  none_span=$(grep "RETRY ARRIVALS" "$STATE/logs/worker-storm-none.log" | tail -1 | sed 's/.*within \([0-9]*\)ms.*/\1/')
  step "with equal jitter: retries are spread across the window"
  run_storm storm-equal equal
  eq_span=$(grep "RETRY ARRIVALS" "$STATE/logs/worker-storm-equal.log" | tail -1 | sed 's/.*within \([0-9]*\)ms.*/\1/')
  echo "90% of the retries arrived within ${none_span}ms without jitter and within ${eq_span}ms with jitter"
  expect_le "without jitter the retries bunch together" "$none_span" 1000
  expect_ge "with jitter they are spread over at least twice the window" "$eq_span" $(( none_span * 2 ))
  end
}

demo_dlq() {
  say "Demo 5: dead-letter queue"
  begin
  ctl queue-create q5 -partitions 2 -rf 3 -attempts 3 -base-delay 200ms -max-delay 400ms -jitter none >/dev/null
  start_worker w1 q5 -group g -concurrency 4
  sleep 1.5
  ctl submit -queue q5 -key poison -payload '{"poison":true}'
  ctl submit -queue q5 -key flaky -payload '{"fail":true}'
  ctl submit -queue q5 -count 3 -key fine -payload '{}'
  ctl wait-empty q5 -timeout 30s >/dev/null
  step "dead-letter queue"
  d=$(ctl dlq list q5); echo "$d"
  expect_eq "two jobs dead-lettered" "$(echo "$d" | tail -1 | cut -d' ' -f1)" 2
  echo "$d" | grep -q "non_retryable_error attempts=1" && ok "poison job went straight to the DLQ after 1 attempt" || bad "poison job not found with 1 attempt"
  echo "$d" | grep -q "max_attempts_exceeded attempts=3" && ok "failing job exhausted its 3 attempts" || bad "failing job not found with 3 attempts"
  s=$(stats q5); expect_eq "the three healthy jobs completed" "$(field "$s" acked)" 3
  pid=$(echo "$d" | grep non_retryable | cut -d' ' -f1); fid=$(echo "$d" | grep max_attempts | cut -d' ' -f1)
  step "replay the failing job (its bug is not fixed, so it returns to the DLQ), delete the poison one"
  ctl dlq replay q5 "$fid"; ctl dlq delete q5 "$pid"
  sleep 4
  expect_eq "after replay the unfixed job is dead-lettered again" "$(ctl dlq list q5 | tail -1 | cut -d' ' -f1)" 1
  end
}

demo_leader_failure() {
  say "Demo 6: partition leader failure"
  begin
  ctl queue-create q6 -partitions 3 -rf 3 >/dev/null
  start_worker w1 q6 -group g -concurrency 4; sleep 1.5
  echo "before: $(ctl submit -queue q6 -count 30 -key before)"
  step "leaders before the failure"; leaders
  victim=$(leader_of q6 0)
  step "kill -9 $victim (leader of q6/0)"
  t0=$(now); "$CL" kill "$victim"
  until ctl queue-stats q6 >/dev/null 2>&1; do sleep 0.1; [ "$(echo "$(now) - $t0 > 30" | bc)" = 1 ] && break; done
  failover=$(since "$t0")
  echo "every partition answering again after ${failover}s (failure timeout 2s, lease 3s)"
  step "leaders after"; leaders
  expect_eq "the dead node no longer leads anything" "$(leaders | grep -c "leader=$victim")" 0
  echo "after:  $(ctl submit -queue q6 -count 30 -key after)"
  ctl wait-empty q6 -timeout 40s >/dev/null
  s=$(stats q6); echo "$s"
  expect_eq "all 60 jobs acknowledged (30 before the failure, 30 after)" "$(field "$s" acked)" 60
  end
}

demo_replica_failure() {
  say "Demo 7: replica (follower) failure and catch-up"
  begin
  ctl queue-create q7 -partitions 1 -rf 3 >/dev/null
  sleep 2
  leader=$(leader_of q7 0)
  follower=$(leaders | grep '^q7/0' | sed 's/.*replicas=//' | tr ',' '\n' | grep -v "^$leader$" | head -1)
  step "leader of q7/0 is $leader; killing follower $follower"
  "$CL" kill "$follower"
  t0=$(now)
  out=$(ctl submit -queue q7 -count 100 -key whiledown -no-retry)
  echo "writes with a follower down: $out ($(since "$t0")s)"
  expect_eq "writes keep being acknowledged by the remaining quorum" "$out" "accepted=100 duplicate=0 rejected=0 failed=0"
  lag=$(curl -s 127.0.0.1:$(admin_port "$leader")/metrics | grep 'jobqueue_replica_lag_entries' | grep "follower=\"$follower\"" | awk '{print $2}')
  echo "replication lag the leader reports for $follower: $lag entries"
  expect_ge "lag is visible in metrics" "${lag%.*}" 100
  step "restart $follower; it replays its log and catches up from the leader"
  "$CL" restart "$follower" >/dev/null; sleep 0.5
  for _ in $(seq 100); do
    lag=$(curl -s 127.0.0.1:$(admin_port "$leader")/metrics | grep 'jobqueue_replica_lag_entries' | grep "follower=\"$follower\"" | awk '{print $2}')
    [ "${lag%.*}" = "0" ] && break; sleep 0.2
  done
  expect_eq "lag drained to zero" "${lag%.*}" 0
  end
}

demo_rebalance() {
  say "Demo 8: consumer-group rebalance"
  begin
  ctl queue-create q8 -partitions 6 -rf 3 >/dev/null
  for w in w1 w2 w3; do start_worker $w q8 -group g -concurrency 2; done
  sleep 3
  step "three workers share six partitions"; curl -s 127.0.0.1:9000/groups.txt
  expect_eq "each worker owns two partitions" "$(curl -s 127.0.0.1:9000/groups.txt | grep -c 'partitions=[0-9],[0-9]$')" 3
  gen1=$(curl -s 127.0.0.1:9000/groups.txt | head -1 | sed 's/.*generation=\([0-9]*\).*/\1/')
  step "kill -9 w3: after its session (4s) lapses its partitions move to the survivors"
  hard_kill w3
  for _ in $(seq 60); do curl -s 127.0.0.1:9000/groups.txt | grep -q member=w3 || break; sleep 0.5; done
  curl -s 127.0.0.1:9000/groups.txt
  gen2=$(curl -s 127.0.0.1:9000/groups.txt | head -1 | sed 's/.*generation=\([0-9]*\).*/\1/')
  expect_eq "survivors own three partitions each" "$(curl -s 127.0.0.1:9000/groups.txt | grep -c 'partitions=[0-9],[0-9],[0-9]$')" 2
  expect_ge "the generation advanced (members that missed it are fenced)" "$gen2" $((gen1+1))
  ctl submit -queue q8 -count 30 -key after >/dev/null
  ctl wait-empty q8 -timeout 30s >/dev/null
  s=$(stats q8); expect_eq "all jobs processed by the two survivors" "$(field "$s" acked)" 30
  end
}

demo_backpressure() {
  say "Demo 9: backpressure"
  begin
  ctl queue-create q9 -partitions 1 -rf 3 -max-depth 200 >/dev/null
  step "no consumers; producers submit 400 jobs into a queue bounded at 200"
  out=$(ctl submit -queue q9 -count 400 -key flood -no-retry); echo "$out"
  expect_eq "exactly the bound was accepted" "$(field "$out" accepted)" 200
  expect_eq "the rest were rejected with 429, not buffered" "$(field "$out" rejected)" 200
  s=$(stats q9); echo "$s"
  expect_eq "queue depth stayed at the bound" "$(field "$s" ready)" 200
  step "a consumer arrives and drains; producers are admitted again"
  start_worker w1 q9 -concurrency 8
  ctl wait-empty q9 -timeout 40s >/dev/null
  out=$(ctl submit -queue q9 -count 50 -key again -no-retry); echo "$out"
  expect_eq "after draining, submissions are accepted" "$(field "$out" accepted)" 50
  end
}

demo_hot_partition() {
  say "Demo 10: hot partition"
  begin
  ctl queue-create q10 -partitions 6 -rf 3 -max-depth 600 >/dev/null
  step "one merchant sends 240 jobs (all one partition key); 60 jobs come from many others"
  hot=$(ctl submit -queue q10 -count 240 -partition-key merchant-hot -key hot -no-retry); echo "hot merchant: $hot"
  rest=$(ctl submit -queue q10 -count 60 -key others -no-retry); echo "everyone else: $rest"
  ctl partitions q10
  s=$(stats q10)
  total=$(field "$s" ready)
  echo "queue holds $total jobs against a configured limit of 600; partition 5 is full while the others have room"
  expect_eq "the hot partition rejected its overflow (limit 100 per partition)" "$(field "$hot" accepted)" 100
  expect_ge "most other traffic (the five unaffected partitions) was accepted" "$(field "$rest" accepted)" 40
  expect_ge "but other keys that hash to the hot partition were rejected too: its blast radius is the whole partition" "$(field "$rest" rejected)" 1
  expect_le "the queue as a whole is far from its limit" "$total" 200
  end
}

demo_load() {
  say "Demo 11: high-volume load test"
  begin
  (cd "$ROOT" && JOBQUEUE_ADMIN_TOKEN=$ADMIN_KEY "$BIN/loadtest" -api "$JOBQUEUE_API" -create-queue -queue load11 -partitions 6 -replication-factor 3 \
    -group g -rates 100,500,1000 -stage-duration 6s -consumers 4 \
    -metrics-url "http://127.0.0.1:9080/metrics,http://127.0.0.1:9101/metrics,http://127.0.0.1:9102/metrics,http://127.0.0.1:9103/metrics" 2>/dev/null) | tee "$STATE/load.out"
  ok_stage=$(grep -c '^ *100 ' "$STATE/load.out")
  expect_ge "the 100 jobs/s stage was reported" "$ok_stage" 1
  end
}

demo_shutdown() {
  say "Demo 12: graceful shutdown"
  begin
  ctl queue-create q12 -partitions 2 -rf 3 >/dev/null
  step "worker: SIGTERM while 4 jobs are mid-flight (3s each)"
  start_worker w1 q12 -concurrency 4 -shutdown-grace 20s
  sleep 1; ctl submit -queue q12 -count 4 -key sd -payload '{"sleep_ms":3000}' >/dev/null
  sleep 1; t0=$(now)
  stop_worker w1; rc=$?
  took=$(since "$t0")
  echo "worker exited with status $rc after ${took}s"
  expect_eq "exit status 0" "$rc" 0
  s=$(stats q12); echo "$s"
  expect_eq "all 4 in-flight jobs finished and were acknowledged" "$(field "$s" acked)" 4
  grep -h "worker stopped" "$STATE/logs/worker-w1.log" | sed 's/.*"succeeded"/  succeeded/' | cut -c1-100
  step "gateway: SIGTERM with a 25s long poll open must not wait it out"
  curl -s -m 30 -X POST "$JOBQUEUE_API/v1/queues/q12/pull" -H "Authorization: Bearer $WORK_KEY" -H 'Content-Type: application/json' \
    -d '{"worker":"lp","max":1,"wait":"25s"}' >"$STATE/longpoll.out" &
  lp=$!; sleep 1; t0=$(now)
  gwpid=$(cat "$STATE/pids/gateway.pid"); kill -TERM "$gwpid"
  while kill -0 "$gwpid" 2>/dev/null; do sleep 0.1; [ "$(echo "$(now) - $t0 > 10" | bc)" = 1 ] && break; done
  took=$(since "$t0"); wait $lp 2>/dev/null
  echo "gateway stopped after ${took}s; the long poll returned: $(cat "$STATE/longpoll.out")"
  expect_le "gateway shut down promptly (seconds, not the 25s poll)" "${took%.*}" 4
  rm -f "$STATE/pids/gateway.pid"
  step "node: SIGTERM n2, restart, data intact"
  before=$(stats q12 2>/dev/null)
  "$CL" restart n2 >/dev/null; sleep 4
  "$CL" restart gateway >/dev/null; sleep 2
  after=$(stats q12); expect_eq "acked count unchanged across the node restart" "$(field "$after" acked)" 4
  end
}

demo_net_partition() {
  say "Extra: network partition (leader isolated from the cluster)"
  begin
  ctl queue-create qp -partitions 3 -rf 3 >/dev/null
  start_worker w1 qp -group g -concurrency 4; sleep 1.5
  ctl submit -queue qp -count 20 -key pre >/dev/null
  victim=$(leader_of qp 0)
  others=$(echo n1 n2 n3 | tr ' ' '\n' | grep -v "^$victim$" | sed 's/.*/"&"/' | paste -sd, -)
  step "isolate $victim from its peers and the coordinator (both directions)"
  "$CL" faults "$victim" "{\"block_to\":[$others,\"coord\"],\"block_from\":[$others,\"coord\"]}" >/dev/null
  for o in $(echo n1 n2 n3 | tr ' ' '\n' | grep -v "^$victim$"); do "$CL" faults "$o" "{\"block_to\":[\"$victim\"],\"block_from\":[\"$victim\"]}" >/dev/null; done
  "$CL" faults coord "{\"block_to\":[\"$victim\"],\"block_from\":[\"$victim\"]}" >/dev/null
  step "producers keep submitting (with idempotency keys and retries) while the majority side elects a new leader"
  acc=0; for i in $(seq 40); do
    for try in 1 2 3 4 5 6; do
      r=$(ctl submit -queue qp -key "during-$i" -no-retry 2>&1) && { acc=$((acc+1)); break; }; sleep 0.5
    done
  done
  echo "accepted $acc of 40 submissions made during the partition"
  step "leaders now"; leaders
  expect_eq "the isolated node lost leadership" "$(leaders | grep -c "leader=$victim")" 0
  step "heal the partition"
  for s in coord n1 n2 n3; do "$CL" faults $s '{}' >/dev/null; done
  ctl wait-empty qp -timeout 60s >/dev/null
  s=$(stats qp); echo "$s"
  expect_eq "every accepted job (20 + $acc) was processed exactly once" "$(field "$s" acked)" $((20+acc))
  grep -h "deposed\|stepping down\|lease expired" "$STATE/logs/$victim.log" | tail -2 | cut -c1-200 | sed 's/^/    node log: /'
  end
}

demo_freeze() {
  say "Extra: frozen leader (SIGSTOP), the case leases exist for"
  begin
  ctl queue-create qf -partitions 3 -rf 3 >/dev/null
  start_worker w1 qf -group g -concurrency 4; sleep 1.5
  ctl submit -queue qf -count 20 -key pre >/dev/null
  victim=$(leader_of qf 0)
  step "freeze $victim for 9s: alive as a process, silent as a node"
  "$CL" pause "$victim"
  acc=0; for i in $(seq 30); do
    for try in 1 2 3 4 5 6; do ctl submit -queue qf -key "during-$i" -no-retry >/dev/null 2>&1 && { acc=$((acc+1)); break; }; sleep 0.5; done
  done
  sleep 1
  step "resume $victim"; "$CL" resume "$victim"; sleep 3
  leaders
  expect_eq "the thawed node does not lead what it used to" "$(leaders | grep -c "leader=$victim")" 0
  ctl wait-empty qf -timeout 60s >/dev/null
  s=$(stats qf); echo "$s"
  expect_eq "every accepted job processed exactly once, no split brain" "$(field "$s" acked)" $((20+acc))
  grep -h "lease expired\|deposed\|stepping down" "$STATE/logs/$victim.log" | tail -2 | cut -c1-200 | sed 's/^/    node log: /'
  end
}

ALL=(normal worker_failure duplicate retry_storm dlq leader_failure replica_failure rebalance backpressure hot_partition load shutdown net_partition freeze)
name=${1:-}; name=${name//-/_}
trap 'end >/dev/null 2>&1' EXIT
if [ "$name" = all ]; then for d in "${ALL[@]}"; do "demo_$d"; done
elif declare -F "demo_$name" >/dev/null; then "demo_$name"
else sed -n '2,12p' "$0"; exit 2; fi
echo
if [ $FAILS -eq 0 ]; then echo "ALL CHECKS PASSED"; else echo "$FAILS CHECK(S) FAILED"; exit 1; fi
