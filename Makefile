.PHONY: gateway traces \
        flink-jars stream-up stream-down flink-tables flink-jobs flink-resume \
        flink-stop flink-verify flink-shell traffic \
        hot-up hot-down ch-tables ch-verify ch-freshness ch-panels ch-sample

gateway:
	.venv/bin/uvicorn services.gateway.app:create_app --factory --reload --port 8100

traces:
	.venv/bin/python3 scripts/consume_tree.py

# --- cold path: Kafka -> Flink SQL -> Iceberg (ADR-004) ---------------------
#
# Order for a cold start:
#   make flink-jars      # once: ~172 MB of connector jars into stream/flink/lib
#   make stream-up       # minio + iceberg-rest + flink jm/tm (+ kafka, sr)
#   make flink-tables    # create the Iceberg tables (day(ts) hidden partition)
#   make flink-jobs      # submit both SQL jobs -- they run until cancelled
#   make traffic         # 600 spans through the real SDK
#
# Verification queries are batch jobs and need a task slot, and the two
# streaming jobs hold both. So: make flink-stop, then make flink-verify.
#
# make flink-stop records where each job can resume; make flink-resume picks it
# back up. A plain `make flink-jobs` after a stop is refused, because it would
# replay already-committed rows -- see ADR-004 #11.

flink-jars:
	bash scripts/fetch_flink_jars.sh

stream-up:
	docker compose --profile streaming up -d

stream-down:
	docker compose --profile streaming down

flink-tables:
	.venv/bin/python3 -m stream.flink.create_tables

flink-jobs:
	./stream/flink/submit.sh

flink-resume:
	./stream/flink/submit.sh --resume

flink-stop:
	./stream/flink/stop.sh

flink-verify:
	./stream/flink/submit.sh --verify

flink-shell:
	./stream/flink/submit.sh --shell

traffic:
	.venv/bin/python3 scripts/gen_traffic.py --events 600

# --- hot path: Kafka -> ClickHouse -> Grafana (ADR-005) --------------------
#
# Order for a cold start:
#   make hot-up          # clickhouse + grafana (+ kafka, sr)
#   make ch-tables       # apply stream/clickhouse/sql/*, in filename order
#   make traffic         # 600 spans through the real SDK
#   make ch-verify       # rows vs distinct span_ids vs topic offsets
#
# Then http://localhost:3000 -- both dashboards are already there, provisioned
# from dashboards/. Anonymous read-only; admin/admin at /login to edit.
#
# Unlike `make flink-jobs`, there is nothing to refuse here: this consumer's
# offsets live in the broker under group 'clickhouse-hotpath', so a restart
# resumes by itself and cannot replay into a populated table (ADR-005 #7).
# A deliberate reset is:
#   .venv/bin/python3 -m stream.clickhouse.bootstrap --recreate
#
# Run this profile OR the streaming one, not both -- each is sized to fit
# beside the spine, not beside the other.

hot-up:
	docker compose --profile hotpath up -d

hot-down:
	docker compose --profile hotpath down

ch-tables:
	.venv/bin/python3 -m stream.clickhouse.bootstrap

ch-verify:
	.venv/bin/python3 scripts/hot_path_verify.py counts

# NFR-2: emit -> queryable, p95 <= 5s. Needs Kafka; emits through the real SDK.
ch-freshness:
	.venv/bin/python3 scripts/hot_path_verify.py freshness --probes 50

# NFR-5: every dashboard panel < 1s. Reads the queries out of dashboards/json/
# so it cannot drift into timing something the dashboards do not run.
ch-panels:
	.venv/bin/python3 scripts/hot_path_verify.py panels

ch-sample:
	.venv/bin/python3 scripts/hot_path_verify.py sample

# Flink dashboard: http://localhost:8082   (8081 is Schema Registry)
# MinIO console:   http://localhost:9001
# Iceberg REST:    http://localhost:8181/v1/config
# Grafana:         http://localhost:3000   (anonymous Viewer; admin/admin to edit)
# ClickHouse:      http://localhost:8123   (HTTP; native 9000 is container-only)
#
#   curl -s localhost:8123 --data-binary \
#     "SELECT event_type, uniqExact(span_id) FROM agentlake.trace_events_rt GROUP BY event_type"

# Non-streaming:
#   curl -s localhost:8100/v1/chat -H 'content-type: application/json' \
#     -d '{"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]}' | jq
#
# Streaming (SSE):
#   curl -N -s localhost:8100/v1/chat -H 'content-type: application/json' \
#     -d '{"model_alias": "fast", "stream": true, "messages": [{"role": "user", "content": "hi"}]}'
#
# Health / stats:
#   curl -s localhost:8100/v1/health | jq
#   curl -s localhost:8100/v1/stats | jq
