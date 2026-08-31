.PHONY: gateway traces \
        flink-jars stream-up stream-down flink-tables flink-jobs flink-resume \
        flink-stop flink-verify flink-shell traffic

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

# Flink dashboard: http://localhost:8082   (8081 is Schema Registry)
# MinIO console:   http://localhost:9001
# Iceberg REST:    http://localhost:8181/v1/config

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
