#!/usr/bin/env bash
# Stop the running agentlake Flink jobs, recording where each one can resume.
#
#     ./stream/flink/stop.sh          # stop, remember the resume point
#     ./stream/flink/stop.sh --forget # stop and discard it (next start is a reset)
#
# Cancelling a Flink job normally throws its state away, and a job restarted
# with no state reads from earliest-offset -- straight into an Iceberg table
# that already holds those rows. The cluster therefore runs with
# `execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION`
# (docker-compose.yml), so the last completed checkpoint outlives the job.
#
# This script reads each job's latest completed checkpoint path out of the REST
# API *before* cancelling and writes it to stream/flink/.resume/<pipeline>, which
# is what `submit.sh --resume` reads back. Nothing here is clever: the checkpoint
# is Flink's, the path is Flink's, this only remembers it so a human does not
# have to.
#
# Free the task slots and then verify:
#     ./stream/flink/stop.sh && ./stream/flink/submit.sh --verify
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

REST="http://localhost:${FLINK_REST_PORT:-8082}"
RESUME_DIR="stream/flink/.resume"

forget=false
[[ "${1:-}" == "--forget" ]] && forget=true

running=$(curl -fsS "$REST/jobs/overview" | python3 -c '
import sys, json
for job in json.load(sys.stdin)["jobs"]:
    if job["state"] == "RUNNING":
        print(job["jid"], job["name"])
')

if [[ -z "$running" ]]; then
  echo "no running jobs"
  # --forget still has work to do here: the common case for it is a warehouse
  # reset *after* the jobs were already stopped, and without this the stale
  # resume point would keep blocking a fresh submit.
  if $forget && [[ -d "$RESUME_DIR" ]]; then
    rm -f "$RESUME_DIR"/*
    echo "resume points discarded (--forget)"
  fi
  exit 0
fi

mkdir -p "$RESUME_DIR"

while read -r jid name; do
  # Read the resume point BEFORE cancelling: once the job is gone its
  # checkpoint listing goes with it, even though the files on S3 remain.
  path=$(curl -fsS "$REST/jobs/$jid/checkpoints" | python3 -c '
import sys, json
latest = (json.load(sys.stdin).get("latest") or {}).get("completed") or {}
print(latest.get("external_path") or "")
' 2>/dev/null || echo "")

  curl -fsS -X PATCH "$REST/jobs/$jid?mode=cancel" >/dev/null
  echo "cancelled $name ($jid)"

  if $forget; then
    rm -f "$RESUME_DIR/$name"
    echo "  resume point discarded (--forget)"
  elif [[ -n "$path" ]]; then
    printf '%s\n' "$path" >"$RESUME_DIR/$name"
    echo "  resume from: $path"
  else
    # No completed checkpoint yet -- a job stopped within its first 30s has
    # committed nothing to Iceberg either, so starting over is correct.
    rm -f "$RESUME_DIR/$name"
    echo "  no completed checkpoint yet; nothing committed, so no resume point"
  fi
done <<<"$running"

echo
echo "resume with:  ./stream/flink/submit.sh --resume"
