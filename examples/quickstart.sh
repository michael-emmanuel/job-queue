#!/usr/bin/env bash
# Walks through the job lifecycle against a running broker (see `make run`).
# Keys below match the development keys in the Makefile; do not reuse them.
set -euo pipefail

API=${API:-http://127.0.0.1:8080}
ADMIN=admin-key-0123456789
PROD=prod-key-01234567890
WORK=work-key-012345678901

req() { # req METHOD PATH KEY [JSON] [extra curl args...]
  local method=$1 path=$2 key=$3 body=${4:-}
  shift 4 || shift $#
  if [ -n "$body" ]; then
    curl -sS -X "$method" "$API$path" -H "Authorization: Bearer $key" -H 'Content-Type: application/json' -d "$body" "$@"
  else
    curl -sS -X "$method" "$API$path" -H "Authorization: Bearer $key" "$@"
  fi
}
field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" | head -n1; }
num()   { sed -n "s/.*\"$1\":\([0-9]*\).*/\1/p" | head -n1; }

echo "1. create a queue (admin)"
req POST /v1/queues $ADMIN '{"name":"email","partitions":4,"default_visibility":"30s","retry":{"max_attempts":3,"base_delay":"500ms","max_delay":"5s","jitter":"equal"}}' || true
echo

echo "2. submit a job with an idempotency key (producer)"
BODY='{"queue":"email","type":"send_email","payload":{"user_id":"123","template":"welcome"},"priority":5}'
R=$(req POST /v1/jobs $PROD "$BODY" -H 'Idempotency-Key: signup-123')
echo "$R"; ID=$(echo "$R" | field job_id)

echo "3. retry the same request: same job, no duplicate"
req POST /v1/jobs $PROD "$BODY" -H 'Idempotency-Key: signup-123'

echo "4. a worker pulls (long poll up to 5s)"
P=$(req POST /v1/queues/email/pull $WORK '{"worker":"worker-1","max":1,"visibility":"30s","wait":"5s"}')
echo "$P"; ATT=$(echo "$P" | num attempt)

echo "5. worker reports a retryable failure"
req POST "/v1/jobs/$ID/fail" $WORK "{\"attempt\":$ATT,\"error\":\"smtp timeout\"}"

echo "6. job is retried after backoff; pull again"
P=$(req POST /v1/queues/email/pull $WORK '{"worker":"worker-2","max":1,"wait":"5s"}')
echo "$P"; ATT=$(echo "$P" | num attempt)

echo "7. ack with a stale attempt is fenced out (409), the current one succeeds (204)"
req POST "/v1/jobs/$ID/ack" $WORK '{"attempt":1}' -w ' -> HTTP %{http_code}\n'
req POST "/v1/jobs/$ID/ack" $WORK "{\"attempt\":$ATT}" -w 'HTTP %{http_code}\n'

echo "8. queue statistics (admin)"
req GET /v1/queues/email $ADMIN
