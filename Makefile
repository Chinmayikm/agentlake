.PHONY: gateway traces \
        flink-jars stream-up stream-down flink-tables flink-jobs flink-resume \
        flink-stop flink-verify flink-shell traffic \
        hot-up hot-down ch-tables ch-verify ch-freshness ch-panels ch-sample \
        analytics-build analytics-up analytics-down trino-shell \
        dbt-build dbt-docs quality \
        lineage-up lineage-down lineage \
        analytics-verify analytics-session analytics-crosscheck seed \
        cdc-up cdc-down cdc-connector cdc-psql cdc-slot cdc-topic \
        cdc-table cdc-land cdc-seed

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

# --- analytics: Iceberg -> Trino -> dbt marts (ADR-006) --------------------
#
# Order for a cold start. STOP THE COLD PATH FIRST -- Trino's JVM takes the
# Flink cluster's place in the memory budget, and the analytics slice needs
# neither Flink nor Kafka (it reads Iceberg through the same REST catalog):
#
#   make flink-stop                                  # if the jobs are running
#   docker compose stop flink-jobmanager flink-taskmanager kafka schema-registry
#   make analytics-build   # once: builds the dbt/GE/dbt-ol toolbox image
#   make analytics-up      # trino (+ minio, iceberg-rest)
#   make dbt-build         # dbt deps && dbt build -- 7 models, 71 tests
#   make quality           # the Great Expectations gate; non-zero on failure
#   make analytics-verify  # marts vs staging vs raw, with the numbers
#
# dbt and Great Expectations run INSIDE a container, and that is forced rather
# than chosen: this repo is Python 3.14, dbt-core has no 3.14 support
# (dbt-labs/dbt-core#12098) and great-expectations 1.22.0 declares <3.14.
# See ADR-006 #2.

analytics-build:
	docker compose build dbt

# `trino` is NAMED rather than relying on the profile alone, and that is worth
# 745 MiB. kafka and schema-registry carry no `profiles:` key, so they start
# under every bare `docker compose up` -- including `--profile analytics up -d`.
# The analytics slice does not use them: it reads Iceberg through the REST
# catalog and never touches the topic. Naming trino brings up exactly its
# dependency chain (iceberg-rest -> minio-init -> minio) and nothing else.
analytics-up:
	docker compose --profile analytics up -d trino
	$(MAKE) --no-print-directory analytics-wait

# Trino accepts queries ~30s after the container starts, but the first query to
# touch the Iceberg catalog also pays plugin init, the S3 client build and a
# catalog round trip -- measured 28.2s cold against 0.4s warm. dbt-trino exposes
# no request-timeout setting (the trino client's fixed 30s applies), so an
# unwarmed build intermittently fails its first model with a read timeout.
# Paying it here, deliberately, is ADR-000 #3's warmup argument again.
analytics-wait:
	.venv/bin/python3 scripts/analytics_verify.py wait

analytics-down:
	docker compose --profile analytics down

trino-shell:
	docker compose exec trino trino --catalog lake --schema analytics

dbt-build:
	docker compose run --rm dbt dbt deps
	docker compose run --rm dbt dbt build

# NFR: the quality gate exits non-zero when a BLOCKING expectation fails.
# Warn-severity expectations are reported and never fail the build -- see the
# blocking-vs-warn rule at the top of quality/checkpoint.py.
quality:
	docker compose run --rm --entrypoint python dbt /quality/checkpoint.py

analytics-verify:
	.venv/bin/python3 scripts/analytics_verify.py counts

analytics-session:
	.venv/bin/python3 scripts/analytics_verify.py session

# Trino's EXACT p95 against ClickHouse's approximate quantile(0.95). Needs the
# hot path too -- start ONLY clickhouse beside the analytics slice, not the
# whole hotpath profile, and stop it again afterwards:
#
#   docker compose up -d clickhouse && make analytics-crosscheck
#   docker compose stop clickhouse
analytics-crosscheck:
	.venv/bin/python3 scripts/analytics_verify.py crosscheck

# Synthetic spans written straight into Iceberg through Trino, for a warehouse
# with no Kafka and no Flink in front of it -- which is what CI has. REFUSES a
# table that already holds rows unless --force.
seed:
	.venv/bin/python3 scripts/seed_iceberg.py seed --events 600 --seed 7

# --- cdc: Postgres -> Debezium -> Kafka -> Iceberg (ADR-007) ---------------
#
# Unlike `make analytics-up`, this one does NOT name its services: the cdc slice
# genuinely needs kafka and schema-registry, so a bare profile bring-up starting
# them too is right rather than wasteful.
#
# TWO PHASES, and the split is a memory decision measured rather than guessed.
# Capture (metadata-db + connect) costs ~535 MiB and landing needs the analytics
# slice's 853; both at once plus kafka leaves too little for dbt's 512 MiB
# toolbox on a 3.9 GB box. Kafka keeps the topic between them, so stopping the
# capture side loses nothing:
#
#   PHASE 1 -- capture
#     make cdc-up          # metadata-db -> metadata-init -> connect
#     make cdc-connector   # idempotent PUT; snapshot.mode=initial fires here
#     make cdc-topic       # what Debezium actually emitted
#     ...make changes with `make cdc-psql`...
#     docker compose stop connect metadata-db
#
#   PHASE 2 -- land and model (Flink must be stopped; Trino takes its place)
#     make analytics-up
#     make cdc-table       # once: creates lake.cdc.prompt_versions
#     make cdc-land        # drain the topic into Iceberg, resumably
#     make dbt-build && make quality && make analytics-verify

cdc-up:
	docker compose --profile cdc up -d

cdc-down:
	docker compose --profile cdc down

cdc-connector:
	.venv/bin/python3 scripts/register_connector.py

cdc-psql:
	docker compose exec metadata-db psql -U agentlake -d agentlake

# The classic production failure mode, in one query (ADR-007 #5). restart_lsn is
# what actually pins WAL on disk; confirmed_flush_lsn is only how far the
# consumer has acknowledged, and it catches up FIRST. Watching the wrong one
# reports "caught up" while megabytes are still retained.
cdc-slot:
	docker compose exec -T metadata-db psql -U agentlake -d agentlake -c "\
	SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, \
	       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal \
	  FROM pg_replication_slots;"

cdc-topic:
	.venv/bin/python3 scripts/cdc_verify.py topic

cdc-table:
	.venv/bin/python3 scripts/cdc_land.py create-table

cdc-land:
	.venv/bin/python3 scripts/cdc_land.py land

# Synthetic changelog rows for a warehouse with no Kafka in front of it, which
# is what CI has. Refuses a populated table without --force, exactly like
# `make seed`.
cdc-seed:
	.venv/bin/python3 scripts/cdc_land.py seed

# --- lineage: OpenLineage -> Marquez (ADR-006 #7) --------------------------
#
# Its own profile because these three containers are ~320 MiB that only the
# lineage run needs:
#
#   make lineage-up && make lineage      # then http://localhost:3001
#   make lineage-down
#
# `make lineage` does two things. dbt-ol runs the build and post-processes
# target/manifest.json into OpenLineage events -- so it only ever sees dbt
# nodes, and the graph would start at lake.raw.trace_events. emit_flink_lineage
# adds the Kafka -> Iceberg edge in front of it, DECLARED from the job SQL
# rather than captured from a running Flink job. See ADR-006 #7.

# Named, for the same reason analytics-up names trino: a bare profile bring-up
# would also start kafka and schema-registry, which lineage does not use.
lineage-up:
	docker compose --profile lineage up -d marquez marquez-web

lineage-down:
	docker compose --profile lineage stop marquez marquez-web marquez-db

lineage:
	.venv/bin/python3 scripts/emit_flink_lineage.py
	docker compose run --rm \
		-e OPENLINEAGE_URL=http://marquez:5000 \
		-e OPENLINEAGE_NAMESPACE=agentlake \
		dbt dbt-ol build

# Flink dashboard: http://localhost:8082   (8081 is Schema Registry)
# MinIO console:   http://localhost:9001
# Iceberg REST:    http://localhost:8181/v1/config
# Grafana:         http://localhost:3000   (anonymous Viewer; admin/admin to edit)
# ClickHouse:      http://localhost:8123   (HTTP; native 9000 is container-only)
# Trino:           http://localhost:8085   (8080 stays free -- see ADR-006 #1)
# Marquez API:     http://localhost:5000/api/v1/namespaces
# Marquez UI:      http://localhost:3001   (grafana owns 3000)
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
