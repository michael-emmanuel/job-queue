#!/usr/bin/env bash
# Local multi-process cluster: 1 coordinator, 3 broker nodes, 1 gateway.
# Everything runs as ordinary processes on loopback so failures can be
# injected for real: kill -9, SIGSTOP/SIGCONT (a frozen process), and
# network partitions between processes via the /faults endpoint.
#
#   scripts/cluster.sh up | down | status | env
#   scripts/cluster.sh kill|pause|resume|restart <coord|n1|n2|n3|gateway>
#   scripts/cluster.sh faults <svc> '<json>'      e.g. '{"block_to":["n2"]}'
#   scripts/cluster.sh logs <svc>
#   scripts/cluster.sh clean
#
# The keys and secret below are generated per run into .cluster/env; they are
# for local use only and are never committed (.cluster/ is git-ignored).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
STATE=${CLUSTER_DIR:-$ROOT/.cluster}
BIN=$ROOT/bin
NODES=(n1 n2 n3)

declare -A RPC=( [coord]=7000 [n1]=7101 [n2]=7102 [n3]=7103 )
declare -A ADMIN=( [coord]=9000 [n1]=9101 [n2]=9102 [n3]=9103 [gateway]=9080 )
API_PORT=${API_PORT:-8080}

peers() {
  echo "coord=http://127.0.0.1:${RPC[coord]},n1=http://127.0.0.1:${RPC[n1]},n2=http://127.0.0.1:${RPC[n2]},n3=http://127.0.0.1:${RPC[n3]}"
}

build() {
  mkdir -p "$BIN"
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
}

ensure_env() {
  mkdir -p "$STATE/logs" "$STATE/pids"
  if [ ! -f "$STATE/env" ]; then
    rnd() { head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24; }
    umask 077
    cat > "$STATE/env" <<EOT
export JOBQUEUE_CLUSTER_SECRET=cs-$(rnd)
export ADMIN_KEY=ak-$(rnd)
export PROD_KEY=pk-$(rnd)
export WORK_KEY=wk-$(rnd)
EOT
  fi
  # shellcheck disable=SC1091
  source "$STATE/env"
  export JOBQUEUE_API_KEYS="$ADMIN_KEY:ops:admin,$PROD_KEY:acme:producer,$WORK_KEY:workers:worker"
}

pidfile() { echo "$STATE/pids/$1.pid"; }
alive() { [ -f "$(pidfile "$1")" ] && kill -0 "$(cat "$(pidfile "$1")")" 2>/dev/null; }

start() {
  local svc=$1; ensure_env
  if alive "$svc"; then echo "$svc already running"; return; fi
  local log="$STATE/logs/$svc.log" common=(-enable-fault-injection -log-json=true)
  case $svc in
    coord)
      "$BIN/coordinator" "${common[@]}" -name coord -listen 127.0.0.1:${RPC[coord]} -admin-listen 127.0.0.1:${ADMIN[coord]} \
        -peers "$(peers)" -dir "$STATE/coord" -failure-timeout "${FAILURE_TIMEOUT:-3s}" -lease "${LEASE:-5s}" -tick 250ms >>"$log" 2>&1 & ;;
    n1|n2|n3)
      "$BIN/node" "${common[@]}" -name "$svc" -listen 127.0.0.1:${RPC[$svc]} -admin-listen 127.0.0.1:${ADMIN[$svc]} \
        -peers "$(peers)" -dir "$STATE/$svc" -tick 250ms ${NODE_FLAGS:-} >>"$log" 2>&1 & ;;
    gateway)
      "$BIN/gateway" "${common[@]}" -name gateway -listen 127.0.0.1:$API_PORT -admin-listen 127.0.0.1:${ADMIN[gateway]} \
        -peers "$(peers)" ${GATEWAY_FLAGS:-} >>"$log" 2>&1 & ;;
    *) echo "unknown service $svc" >&2; exit 2;;
  esac
  echo $! > "$(pidfile "$svc")"
}

wait_http() { # url, seconds
  for _ in $(seq $(( $2 * 10 ))); do curl -sf "$1" >/dev/null 2>&1 && return 0; sleep 0.1; done
  return 1
}

stop() { # SIGTERM then wait; SIGKILL as a last resort
  local svc=$1
  alive "$svc" || { rm -f "$(pidfile "$svc")"; return 0; }
  local pid; pid=$(cat "$(pidfile "$svc")")
  kill -CONT "$pid" 2>/dev/null || true
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -0 "$pid" 2>/dev/null && { echo "$svc did not stop; killing"; kill -KILL "$pid"; }
  rm -f "$(pidfile "$svc")"
}

cmd=${1:-}; svc=${2:-}
case $cmd in
  up)
    build; ensure_env
    start coord; wait_http "http://127.0.0.1:${ADMIN[coord]}/healthz" 10 || { echo "coordinator failed"; tail "$STATE/logs/coord.log"; exit 1; }
    for n in "${NODES[@]}"; do start "$n"; done
    for n in "${NODES[@]}"; do wait_http "http://127.0.0.1:${ADMIN[$n]}/readyz" 15 || { echo "$n not ready"; tail "$STATE/logs/$n.log"; exit 1; }; done
    start gateway
    wait_http "http://127.0.0.1:${ADMIN[gateway]}/readyz" 15 || { echo "gateway not ready"; tail "$STATE/logs/gateway.log"; exit 1; }
    echo "cluster up: API http://127.0.0.1:$API_PORT   (source $STATE/env for keys)" ;;
  down)
    stop gateway; for n in "${NODES[@]}"; do stop "$n"; done; stop coord; echo "cluster stopped" ;;
  status)
    for s in coord n1 n2 n3 gateway; do
      if alive "$s"; then st=$(ps -o stat= -p "$(cat "$(pidfile "$s")")" | tr -d ' '); echo "$s running (pid $(cat "$(pidfile "$s")"), state $st)"; else echo "$s stopped"; fi
    done ;;
  env) ensure_env; sed 's/^export /export /' "$STATE/env"; echo "export API=http://127.0.0.1:$API_PORT" ;;
  kill)    kill -KILL "$(cat "$(pidfile "$svc")")"; echo "$svc killed (SIGKILL)" ;;
  pause)   kill -STOP "$(cat "$(pidfile "$svc")")"; echo "$svc paused (SIGSTOP)" ;;
  resume)  kill -CONT "$(cat "$(pidfile "$svc")")"; echo "$svc resumed" ;;
  restart) stop "$svc"; start "$svc"; echo "$svc restarted" ;;
  faults)  curl -s -X PUT "http://127.0.0.1:${ADMIN[$svc]}/faults" -d "$3"; echo ;;
  logs)    tail -n "${3:-40}" "$STATE/logs/$svc.log" ;;
  clean)   "$0" down 2>/dev/null || true; rm -rf "$STATE"; echo "cleaned" ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
