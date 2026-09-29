#!/usr/bin/env bash
# End-to-end check against `docker compose up`: enqueue a job over HTTP and
# wait for a worker container to finish it. Used by CI.
set -euo pipefail
API=${API:-http://localhost:8000}

for _ in $(seq 60); do
  curl -fsS "$API/healthz" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$API/healthz"; echo

job=$(curl -fsS -X POST "$API/jobs" -H 'content-type: application/json' \
  -d '{"task": "echo", "payload": {"hello": "compose"}, "idempotency_key": "smoke-1"}')
id=$(echo "$job" | jq -r .id)
echo "enqueued job $id"

# Same idempotency key -> same job id, HTTP 200.
dup_status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/jobs" \
  -H 'content-type: application/json' -d '{"task": "echo", "idempotency_key": "smoke-1"}')
test "$dup_status" = "200"

for _ in $(seq 60); do
  state=$(curl -fsS "$API/jobs/$id" | jq -r .state)
  [ "$state" = "succeeded" ] && break
  sleep 1
done
curl -fsS "$API/jobs/$id"; echo
test "$state" = "succeeded"
test "$(curl -fsS "$API/jobs/$id" | jq -c .result)" = '{"echo":{"hello":"compose"}}'

curl -fsS "$API/metrics" | grep 'pgqueue_jobs_completed_total{queue="default"} 1'
echo "compose smoke test passed"
