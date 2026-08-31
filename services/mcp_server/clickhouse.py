"""ClickHouse client for the MCP server's get_trace/query_metrics tools.

Talks to the HTTP interface on 8123 with httpx, which is already a dependency.
clickhouse-connect would be a new one (plus optional pyarrow) to do the same
thing with a typed columnar result nothing here wants -- the same argument
ADR-004 #2 made against pyiceberg, and the same one
stream/clickhouse/bootstrap.py follows. See ADR-005 #1.

Shaped after services/rag/qdrant_store.py: a dataclass whose url is resolved at
instantiation rather than at import (so tests and callers can point it
somewhere else), a client built lazily on first use, and httpx imported at
module scope only because it is already imported by services/rag/fetch.py --
there is no heavyweight driver here to defer.

Every caller value reaches ClickHouse as a bound HTTP parameter, never
interpolated into the SQL string. See query() and ADR-005 #5.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import httpx

DEFAULT_URL = "http://localhost:8123"
DEFAULT_DATABASE = "agentlake"
DEFAULT_TIMEOUT = 10.0


def default_url() -> str:
    return os.environ.get("AGENTLAKE_CLICKHOUSE", DEFAULT_URL)


class ClickHouseUnavailable(RuntimeError):
    """The store could not be reached, or refused the query.

    Deliberately one exception for both: from a tool's point of view "ClickHouse
    is not running" and "ClickHouse rejected this SQL" are the same event -- no
    answer is available and none may be invented. The tools turn this into a
    structured {"error": ...}, which ADR-003 #3 fixed as the permanent contract
    for an unreachable store.
    """


@dataclass
class ClickHouseClient:
    url: str = field(default_factory=default_url)
    database: str = DEFAULT_DATABASE
    timeout: float = DEFAULT_TIMEOUT
    _client: object | None = field(default=None, init=False, repr=False)

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(base_url=self.url.rstrip("/"), timeout=self.timeout)
        assert isinstance(self._client, httpx.Client)
        return self._client

    def query(self, sql: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """Run one SELECT and return its rows as dicts.

        `params` are bound, not formatted: each key is passed as a `param_<name>`
        query-string argument and referenced in the SQL as `{name:Type}`, so
        ClickHouse does the substitution server-side with the type it was told.
        A trace_id containing a quote is a trace_id that does not match anything,
        not a syntax error and not an injection.

        FORMAT JSONEachRow because it streams one object per line and, unlike
        FORMAT JSON, does not wrap the result in a meta/statistics envelope this
        caller would only unwrap again.
        """
        client = self._get_client()
        query_params = {f"param_{k}": v for k, v in (params or {}).items()}
        query_params["database"] = self.database
        try:
            response = client.post(
                "/", content=f"{sql}\nFORMAT JSONEachRow".encode(), params=query_params
            )
        except httpx.HTTPError as exc:
            raise ClickHouseUnavailable(f"{self.url}: {exc}") from exc
        if response.is_error:
            raise ClickHouseUnavailable(
                f"{self.url}: HTTP {response.status_code}: {response.text.strip()[:400]}"
            )
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]

    def ping(self) -> bool:
        """Cheap round trip, for warmup. Raises ClickHouseUnavailable if the
        store is not there -- server.warmup() is what swallows that.
        """
        return self.query("SELECT 1 AS ok") == [{"ok": 1}]

    def close(self) -> None:
        if self._client is not None:
            assert isinstance(self._client, httpx.Client)
            self._client.close()
            self._client = None

    def __enter__(self) -> ClickHouseClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
