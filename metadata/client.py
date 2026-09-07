"""Connecting to the metadata database.

The counterpart of `analytics/trino_client.py`: that module owns how anything
in this repo reaches Trino, this one owns how anything reaches Postgres. Both
exist so a connection detail lives in one place rather than being re-derived at
each call site -- which is the same mistake that made every dense retrieval
return nothing for months (ADR-008 #1).

Unlike `TrinoClient`, this is not a hand-rolled HTTP client. Trino speaks
HTTP+JSON, so `httpx` was already enough and a dependency would have bought
nothing (ADR-004 #2's argument against pyiceberg). Postgres speaks its own
binary wire protocol over a stateful connection with its own type system, so
"just use httpx" has no meaning here: psycopg is the dependency, and it is the
first real database driver in the repo.

psycopg is imported lazily inside connect(), not at module import time -- the
same pattern as services.sdk._get_kafka(), services.rag.embed's fastembed, and
QdrantStore._get_client(), so importing this module stays free for anything
that only wants DEFAULT_* or the DSN helper.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from psycopg import Connection

# Matches docker-compose.yml's metadata-db service. 5433 on the host because
# 5432 is deliberately left free for a locally installed Postgres (ADR-007);
# inside the compose network the connector uses metadata-db:5432.
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5433
DEFAULT_USER = "agentlake"
DEFAULT_PASSWORD = "agentlake-dev-secret"
DEFAULT_DBNAME = "agentlake"


def default_dsn() -> str:
    """AGENTLAKE_METADATA_DSN wins outright; otherwise assemble from the same
    METADATA_DB_* variables docker-compose.yml reads, so a compose override and
    a client override cannot disagree.

    The whole-DSN escape hatch exists because CI and a managed database are
    both cases where the parts are not independently known -- a connection
    string is handed over whole or not at all.
    """
    dsn = os.environ.get("AGENTLAKE_METADATA_DSN")
    if dsn:
        return dsn
    host = os.environ.get("METADATA_DB_HOST", DEFAULT_HOST)
    port = os.environ.get("METADATA_DB_PORT", str(DEFAULT_PORT))
    user = os.environ.get("METADATA_DB_USER", DEFAULT_USER)
    password = os.environ.get("METADATA_DB_PASSWORD", DEFAULT_PASSWORD)
    dbname = os.environ.get("METADATA_DB_NAME", DEFAULT_DBNAME)
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def redacted_dsn(dsn: str | None = None) -> str:
    """The DSN with its password replaced, for printing.

    Every script here prints what it connected to, and a password in a terminal
    scrollback or a CI log is a leak that no later commit can undo.
    """
    dsn = dsn or default_dsn()
    if "://" not in dsn or "@" not in dsn:
        return dsn
    scheme, rest = dsn.split("://", 1)
    credentials, target = rest.rsplit("@", 1)
    user = credentials.split(":", 1)[0]
    return f"{scheme}://{user}:***@{target}"


class MetadataUnavailableError(RuntimeError):
    """Raised instead of psycopg's own error, so callers get one exception type
    and a message that names the thing to start."""


def connect(dsn: str | None = None, **kwargs: Any) -> Connection:
    """An open connection, autocommit off (psycopg's default).

    Callers use `with connect() as conn:` -- psycopg commits on a clean exit
    and rolls back on an exception, which is what makes the eval harness's
    per-example write either land whole or not at all.
    """
    import psycopg

    dsn = dsn or default_dsn()
    try:
        return psycopg.connect(dsn, **kwargs)
    except psycopg.OperationalError as exc:
        raise MetadataUnavailableError(
            f"cannot reach the metadata database at {redacted_dsn(dsn)} "
            f"-- is `make cdc-up` running? ({exc})"
        ) from exc
