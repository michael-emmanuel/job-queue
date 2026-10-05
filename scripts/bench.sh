#!/usr/bin/env bash
# Reproducible benchmark. Brings up the system, runs a staged open-loop load,
# prints a table and writes JSON to bench-results/. Nothing is faked: the
# report includes the machine it ran on. Compare numbers only across runs on
# the same hardware.
#
#   scripts/bench.sh standalone [sync]      single-node broker; sync = always|periodic|none
#   scripts/bench.sh cluster [ack]          coordinator + 3 nodes + gateway; ack = quorum|leader|all
#
# Environment: RATES (default 10,100,1000,10000), STAGE (default 10s),
#              CONSUMERS (default 4), PAYLOAD (default 256), PARTITIONS (default 4)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
MODE=${1:-standalone}; OPT=${2:-}
RATES=${RATES:-10,100,1000,10000}; STAGE=${STAGE:-10s}
CONSUMERS=${CONSUMERS:-4}; PAYLOAD=${PAYLOAD:-256}; PARTS=${PARTITIONS:-4}
OUT=$ROOT/bench-results; mkdir -p "$OUT"
BIN=$ROOT/bin; (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
STAMP=$(date +%Y%m%d-%H%M%S)

if [ "$MODE" = standalone ]; then
  SYNC=${OPT:-always}; DATA=$(mktemp -d)
  export JOBQUEUE_API_KEYS="adminkey-0123456789:ops:admin"
  "$BIN/broker" -addr 127.0.0.1:18080 -admin-addr 127.0.0.1:19090 -data-dir "$DATA" -sync "$SYNC" -log-level warn >"$DATA/broker.log" 2>&1 &
  PID=$!; trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA"' EXIT
  for _ in $(seq 100); do curl -sf http://127.0.0.1:18080/healthz >/dev/null && break; sleep 0.1; done
  export JOBQUEUE_ADMIN_TOKEN=adminkey-0123456789
  EXTRA=(-api http://127.0.0.1:18080 -metrics-url http://127.0.0.1:19090/metrics)
  LABEL="standalone-sync-$SYNC"
else
  ACK=${OPT:-quorum}
  "$ROOT/scripts/cluster.sh" clean >/dev/null 2>&1 || true
  "$ROOT/scripts/cluster.sh" up >/dev/null
  trap '"$ROOT/scripts/cluster.sh" down >/dev/null 2>&1' EXIT
  # shellcheck disable=SC1091
  source "$ROOT/.cluster/env"
  export JOBQUEUE_ADMIN_TOKEN=$ADMIN_KEY
  EXTRA=(-api http://127.0.0.1:8080 -replication-factor 3 -ack "$ACK" -group bench
    -metrics-url "http://127.0.0.1:9080/metrics,http://127.0.0.1:9101/metrics,http://127.0.0.1:9102/metrics,http://127.0.0.1:9103/metrics,http://127.0.0.1:9000/metrics")
  LABEL="cluster-ack-$ACK"
fi

echo "== $LABEL: rates=$RATES stage=$STAGE consumers=$CONSUMERS payload=${PAYLOAD}B partitions=$PARTS =="
"$BIN/loadtest" "${EXTRA[@]}" -create-queue -queue "bench-$STAMP" -partitions "$PARTS" -rates "$RATES" \
  -stage-duration "$STAGE" -consumers "$CONSUMERS" -payload-bytes "$PAYLOAD" -out "$OUT/$LABEL-$STAMP.json"
echo "results: $OUT/$LABEL-$STAMP.json"
