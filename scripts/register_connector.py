"""Register the Debezium Postgres connector with Kafka Connect.

Run from the repo root with the cdc profile up::

    python scripts/register_connector.py
    python scripts/register_connector.py --dry-run     # print the config
    python scripts/register_connector.py --delete      # destructive; see below

Why this is a script and not a compose one-shot
-----------------------------------------------
The schema migrations are a compose one-shot (``metadata-init``) because
``connect`` must not start until they have run. Registering the connector is the
other way round: it needs Connect to already be answering, and it is the thing a
developer re-runs by hand after editing a config key. That is the same shape as
``stream/flink/create_tables.py`` and ``stream/clickhouse/bootstrap.py`` -- a
host-side ``httpx`` migrator -- and it is written the same way, down to the
``created``/``exists`` output and the server's own body being attached to any
error.

Idempotent, structurally rather than by checking first: ``PUT
/connectors/<name>/config`` creates the connector if it is absent and updates it
in place if it is present, and the response body is identical either way. There
is no window where the connector is deleted and not yet recreated, which a
``DELETE`` + ``POST`` pair would have -- and a deleted connector loses its
committed offsets, which is a re-snapshot.

Why raw HTTP and not the Debezium CLI
-------------------------------------
There isn't one. Connect's control plane *is* a REST API, ``httpx`` is already a
dependency, and the config below is a flat JSON object -- the same argument
ADR-004 #2 makes for POSTing an Iceberg ``CreateTableRequest`` rather than
adding pyiceberg.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

import httpx
from dotenv import load_dotenv

DEFAULT_CONNECT_URL = os.environ.get("AGENTLAKE_CONNECT", "http://localhost:8083")

CONNECTOR_NAME = "agentlake-metadata"

# The topic every captured table lands under: <prefix>.<schema>.<table> by
# default, reduced to <prefix>.<table> by the RegexRouter below.
TOPIC_PREFIX = "cdc.metadata"

CAPTURED_TABLES = (
    "public.prompt_versions",
    "public.golden_examples",
    "public.eval_runs",
    "public.eval_results",
)


def connector_config(password: str) -> dict[str, str]:
    """The connector config, with every non-obvious key explained where it sits.

    Flat string->string, which is what Connect's ``/config`` endpoint takes --
    the nested ``{"name": ..., "config": {...}}`` form is the ``POST /connectors``
    body and is not interchangeable.
    """
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        # One task, and this is not tunable: the Postgres connector reads a
        # single replication slot and Debezium hard-caps it at 1. Stating it
        # stops anyone raising it and wondering why nothing changed.
        "tasks.max": "1",
        "database.hostname": "metadata-db",
        "database.port": "5432",
        "database.user": "debezium",
        "database.password": password,
        "database.dbname": "agentlake",
        "topic.prefix": TOPIC_PREFIX,
        "table.include.list": ",".join(CAPTURED_TABLES),
        # pgoutput: Postgres' own logical decoding output plugin, in-tree since
        # PG10. The alternatives (decoderbufs, wal2json) are extensions that
        # would have to be compiled into the postgres image -- pgoutput needs
        # nothing installed, which is why postgres:16-alpine works unmodified.
        "plugin.name": "pgoutput",
        # Named, so ADR-007 #5's pg_replication_slots queries have a stable
        # thing to point at. Debezium's default is "debezium", which is fine
        # until a second connector exists.
        "slot.name": "agentlake_slot",
        # The publication is created by metadata/sql/06_publication.sql, and
        # `disabled` is what makes that the single source of truth: the
        # connector refuses to start against a missing publication rather than
        # quietly creating a different one. `all_tables` (the default) would
        # also need a superuser connection and would widen itself silently as
        # tables are added.
        "publication.name": "agentlake_cdc",
        "publication.autocreate.mode": "disabled",
        # Read the tables once at startup (op='r'), then stream. Without this
        # the changelog would begin at "whatever changes next", and the seeded
        # v1/v2/v3 would never appear.
        "snapshot.mode": "initial",
        # Deletes must propagate. `true` IS the default; set explicitly because
        # it is a requirement of this design rather than an inherited default,
        # and because a future Debezium changing its mind should not silently
        # change ours. A delete produces two records -- the op='d' envelope
        # carrying the full `before` image (which is what REPLICA IDENTITY FULL
        # on prompt_versions buys), then a null-valued tombstone for log
        # compaction. scripts/cdc_land.py resolves state from the first and
        # counts the second; see ADR-007 #4.
        "tombstones.on.delete": "true",
        # numeric(12,6) otherwise arrives as a base64-encoded unscaled value
        # plus a scale, which is correct, lossless and unreadable. `string`
        # keeps full precision and is what a JSON consumer can actually use.
        "decimal.handling.mode": "string",
        # `adaptive`, the default, renders a Postgres `timestamp` as
        # MICROseconds and a `timestamptz` as an ISO-8601 string -- two
        # different Python types out of one column type, decided by a schema the
        # payload does not carry (schemas.enable=false). `connect` renders every
        # temporal type as milliseconds, always a number. This is the single
        # most expensive thing to discover by reading rows.
        "time.precision.mode": "connect",
        # The slot's confirmed_flush_lsn only advances when the connector
        # acknowledges a record, and it only receives records for tables in the
        # publication. On a database where the captured tables are idle but
        # something else is writing, the slot pins WAL indefinitely -- the
        # classic production failure mode, and ADR-007 #5 measures it. The
        # heartbeat gives Debezium something to acknowledge on a timer.
        "heartbeat.interval.ms": "10000",
        # <prefix>.public.<table> -> <prefix>.<table>. `public` carries no
        # information here (there is one schema) and would be in the name of
        # every topic, every dbt source and every ADR line forever.
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        # The prefix is escaped, not interpolated raw: it contains a dot, and an
        # unescaped one is a regex wildcard that would also match "cdcXmetadata".
        "transforms.route.regex": f"^{re.escape(TOPIC_PREFIX)}\\.public\\.(.*)$",
        "transforms.route.replacement": f"{TOPIC_PREFIX}.$1",
    }


def _password() -> str:
    """The password metadata/sql/01_role_debezium.sql set on the role.

    Read from .env the same way docker-compose.yml interpolates it, with the
    same default -- if these two disagree the connector authenticates fine
    against Connect and then fails its task with "password authentication
    failed", which reads like a Debezium problem and is not one. load_dotenv()
    never overrides an already-exported variable, matching
    services/gateway/app.py.
    """
    load_dotenv()
    return os.environ.get("DEBEZIUM_PASSWORD", "debezium-dev-secret")


def _raise_for(response: httpx.Response, what: str) -> None:
    if response.is_error:
        raise RuntimeError(f"{what}: HTTP {response.status_code}\n{response.text}")


def wait_for_connect(client: httpx.Client, seconds: float) -> None:
    """Block until Connect answers /connectors.

    The compose healthcheck already covers `up -d`, but this script is also run
    straight after `docker compose start connect` (ADR-007 #5's restart test),
    where nothing has waited. Paying initialisation deliberately rather than
    charging it to the first real request is the same call ADR-000 #3 and
    ADR-006 #8 make.
    """
    deadline = time.monotonic() + seconds
    last = "no attempt"
    while time.monotonic() < deadline:
        try:
            response = client.get("/connectors", timeout=5.0)
        except httpx.HTTPError as exc:
            last = f"{type(exc).__name__}: {exc}"
        else:
            if not response.is_error:
                return
            last = f"HTTP {response.status_code}"
        time.sleep(2.0)
    raise RuntimeError(f"Kafka Connect did not answer within {seconds:.0f}s: {last}")


def print_status(client: httpx.Client) -> int:
    """Print the connector's state and its task's state. Returns an exit code.

    Registration returning 200 means Connect accepted the config, not that the
    connector works: a bad password or a missing publication surfaces a moment
    later as a FAILED task, with the stack trace only in the status endpoint.
    Reporting "registered" without looking would be reporting the wrong fact.
    """
    for _ in range(15):
        response = client.get(f"/connectors/{CONNECTOR_NAME}/status")
        if response.is_error:
            time.sleep(1.0)
            continue
        status = response.json()
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        tasks = status.get("tasks", [])
        if connector_state == "RUNNING" and tasks:
            break
        time.sleep(1.0)
    else:
        print(f"{CONNECTOR_NAME}: never reported RUNNING with a task", file=sys.stderr)
        return 1

    print(f"connector  {CONNECTOR_NAME}  {connector_state}")
    failed = False
    for task in tasks:
        state = task.get("state", "UNKNOWN")
        print(f"task {task.get('id')}     {state}")
        if state != "RUNNING":
            failed = True
            trace = task.get("trace", "")
            if trace:
                print(trace.strip()[:2000], file=sys.stderr)
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_CONNECT_URL,
        help=f"Kafka Connect REST base URL (default: {DEFAULT_CONNECT_URL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the connector config instead of sending it",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="DELETE the connector. Destructive: it drops the connector's "
        "committed offsets, so the next registration re-snapshots from "
        "scratch and re-emits every row as op='r'. The replication slot "
        "survives and must be dropped by hand if that is what you meant.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="seconds to wait for Connect to answer (default: 120)",
    )
    args = parser.parse_args(argv)

    config = connector_config(_password())

    if args.dry_run:
        redacted = dict(config)
        redacted["database.password"] = "***"
        print(json.dumps({"name": CONNECTOR_NAME, "config": redacted}, indent=2))
        return 0

    with httpx.Client(base_url=args.url.rstrip("/"), timeout=30.0) as client:
        wait_for_connect(client, args.timeout)

        if args.delete:
            response = client.delete(f"/connectors/{CONNECTOR_NAME}")
            if response.status_code == httpx.codes.NOT_FOUND:
                print(f"absent   {CONNECTOR_NAME}")
                return 0
            _raise_for(response, f"delete {CONNECTOR_NAME}")
            print(f"deleted  {CONNECTOR_NAME}")
            return 0

        existed = client.get(f"/connectors/{CONNECTOR_NAME}").status_code != 404
        response = client.put(f"/connectors/{CONNECTOR_NAME}/config", json=config)
        _raise_for(response, f"register {CONNECTOR_NAME}")
        print(f"{'updated ' if existed else 'created '} {CONNECTOR_NAME}")

        exit_code = print_status(client)

    print(f"\ntopics: {TOPIC_PREFIX}.<table> for {len(CAPTURED_TABLES)} captured table(s)")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
