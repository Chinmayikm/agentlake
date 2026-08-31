#!/usr/bin/env bash
# Submit a Flink SQL job file from stream/flink/jobs/ to the running cluster.
#
#     ./stream/flink/submit.sh 01_raw_sink.sql
#     ./stream/flink/submit.sh                  # submits every job, in order
#     ./stream/flink/submit.sh --resume         # ...from the last stop.sh point
#     ./stream/flink/submit.sh --verify         # runs stream/flink/verify/*.sql
#     ./stream/flink/submit.sh --shell          # interactive sql-client
#
# --resume restarts each job from the retained checkpoint stop.sh recorded,
# which is the difference between continuing the stream and replaying it. A
# plain submit starts from earliest-offset; against a warehouse that already
# holds those rows that means duplicates, so this script refuses to do it
# silently when a resume point exists. See ADR-004 #11.
#
# The SQL client runs as a throwaway container (`docker compose run --rm`) on
# the same image and classpath as the cluster, with stream/flink/jobs mounted at
# /sql. It is in its own `sql` profile so `--profile streaming up -d` doesn't
# start it as a long-lived service. Both profiles are passed explicitly: `run`
# enables the target's own profile, but the client's `depends_on:
# flink-jobmanager` points into the `streaming` profile, and compose rejects a
# depends_on that names a service outside the active profile set -- even under
# --no-deps, which only stops it from *starting* the dependency.
#
# INSERT INTO statements submit detached: sql-client prints a job id and exits,
# leaving the job running on the JobManager. Watch it at http://localhost:8082.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

run_client() {
  docker compose --profile streaming --profile sql run --rm --no-deps flink-sql-client "$@"
}

if [[ "${1:-}" == "--shell" ]]; then
  # No -f: an interactive session against the same catalog, which is how the
  # verification queries in ADR-004 are run.
  exec docker compose --profile streaming --profile sql run --rm --no-deps -it flink-sql-client ./bin/sql-client.sh
fi

if [[ "${1:-}" == "--verify" ]]; then
  shift
  # The verification queries are batch jobs, and batch jobs need a task slot.
  # The TaskManager has two, and the two streaming jobs hold both -- so with
  # them running a query does not fail, it *queues*, silently, forever. Check
  # first and say so, rather than letting it look like a hung terminal.
  free=$(curl -fsS "http://localhost:${FLINK_REST_PORT:-8082}/overview" \
         | python3 -c 'import sys,json; print(json.load(sys.stdin)["slots-available"])' 2>/dev/null || echo 0)
  if [[ "$free" -lt 1 ]]; then
    cat >&2 <<'MSG'
No free task slots: the streaming jobs are holding both.

Verification queries are bounded batch jobs and need a slot of their own. Stop
the streaming jobs first, then re-run:

    ./stream/flink/stop.sh
    ./stream/flink/submit.sh --verify
MSG
    exit 1
  fi
  queries=("$@")
  if [[ ${#queries[@]} -eq 0 ]]; then
    mapfile -t queries < <(cd stream/flink/verify && ls -1 [0-9]*.sql)
  fi
  for q in "${queries[@]}"; do
    echo "==> $q"
    run_client ./bin/sql-client.sh -f "/verify/$q"
  done
  exit 0
fi

resume=false
if [[ "${1:-}" == "--resume" ]]; then
  resume=true
  shift
fi

jobs=("$@")
if [[ ${#jobs[@]} -eq 0 ]]; then
  # Numeric prefixes are the ordering: raw sink first, so the aggregate job
  # never starts against a warehouse with no raw table.
  mapfile -t jobs < <(cd stream/flink/jobs && ls -1 *.sql)
fi

# A job's resume point is filed under its pipeline.name, which the SQL declares
# itself -- so the mapping lives in one place and cannot drift from the file
# name.
pipeline_name() {
  sed -n "s/^SET 'pipeline.name' = '\(.*\)';/\1/p" "stream/flink/jobs/$1" | head -1
}

for job in "${jobs[@]}"; do
  if [[ ! -f "stream/flink/jobs/$job" ]]; then
    echo "no such job: stream/flink/jobs/$job" >&2
    exit 1
  fi

  name=$(pipeline_name "$job")
  point="stream/flink/.resume/$name"

  if $resume; then
    if [[ ! -s "$point" ]]; then
      echo "no resume point for '$name' (expected $point)." >&2
      echo "Either it was never stopped with stop.sh, or it was stopped with" >&2
      echo "--forget. Reset the warehouse and submit without --resume:" >&2
      echo "    python -m stream.flink.create_tables --recreate" >&2
      exit 1
    fi
    echo "==> resuming $job from $(cat "$point")"
    run_client ./bin/sql-client.sh \
      -D "execution.savepoint.path=$(cat "$point")" \
      -f "/sql/$job"
  else
    if [[ -s "$point" ]]; then
      # The whole failure mode this guards: a plain submit here replays from
      # earliest into a table that already holds those rows, and nothing
      # complains -- the duplicates only show up in a COUNT much later.
      cat >&2 <<MSG
Refusing to submit $job from earliest-offset: a resume point exists.

    $point
    $(cat "$point")

'$name' was stopped with state retained. Starting it fresh would replay every
event it has already committed to Iceberg, duplicating them. Either:

    ./stream/flink/submit.sh --resume            # continue where it stopped
    python -m stream.flink.create_tables --recreate \\
      && ./stream/flink/stop.sh --forget         # deliberate reset, then submit
MSG
      exit 1
    fi
    echo "==> submitting $job"
    run_client ./bin/sql-client.sh -f "/sql/$job"
  fi
done
