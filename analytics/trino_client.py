"""A small Trino client over the HTTP protocol, using httpx.

Why not trino-python-client
---------------------------
``httpx`` is already a dependency, and this repo runs Python 3.14 while the
analytics toolchain does not (ADR-006 #2) -- so a host-side script that imported
the official client would either drag the 3.12 constraint onto the venv or force
the verification scripts into the container, where they could not also reach
ClickHouse on the host. The Trino client protocol is a POST and a polling loop,
which is small enough to write down.

This is the same argument ``stream/flink/create_tables.py`` makes against
pyiceberg and ``stream/clickhouse/bootstrap.py`` makes against
clickhouse-connect, and it lands the same way: one dependency the repo already
has, against a documented HTTP interface.

The protocol, in full
---------------------
``POST /v1/statement`` with the SQL as the body returns a JSON envelope. The
envelope carries at most a slice of the result plus a ``nextUri``; you GET that,
and keep GETting, until an envelope arrives without one. Results accumulate
across those responses, ``columns`` appears on the first envelope that has any
data, and ``error`` can appear at any point -- including after rows have already
been returned, because Trino streams.

A 503 with no body means "not ready, retry this exact URI", which is Trino's
documented backpressure signal rather than a failure.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import httpx


def default_url() -> str:
    """Host-side Trino. 8085, not 8080 -- see docker-compose.yml."""
    return os.environ.get("AGENTLAKE_TRINO", "http://localhost:8085")


class TrinoError(RuntimeError):
    """A query that Trino rejected or failed. Carries Trino's own message."""


@dataclass
class TrinoResult:
    """One completed query."""

    columns: list[str]
    rows: list[list[Any]]
    #: Trino's own execution statistics, as reported on the final envelope.
    stats: dict[str, Any] = field(default_factory=dict)
    #: Wall-clock milliseconds for the whole POST + polling loop, which is what
    #: a caller timing a query actually experiences.
    elapsed_ms: float = 0.0

    def scalar(self) -> Any:
        """The single value of a single-row, single-column result."""
        if len(self.rows) != 1 or len(self.rows[0]) != 1:
            raise TrinoError(
                f"expected exactly one value, got {len(self.rows)} rows "
                f"x {len(self.columns)} columns"
            )
        return self.rows[0][0]

    def dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, row, strict=True)) for row in self.rows]


@dataclass
class TrinoClient:
    """Executes statements against Trino's HTTP protocol."""

    url: str = field(default_factory=default_url)
    user: str = "agentlake"
    catalog: str = "lake"
    schema: str | None = None
    timeout: float = 120.0

    def _headers(self) -> dict[str, str]:
        headers = {"X-Trino-User": self.user, "X-Trino-Catalog": self.catalog}
        if self.schema:
            headers["X-Trino-Schema"] = self.schema
        return headers

    def execute(self, sql: str) -> TrinoResult:
        """Run one statement to completion and return every row.

        Every row, deliberately: this client exists for verification queries
        that produce counts and handfuls of rows, and a streaming interface
        would be more machinery than any caller here wants. A query that
        returns a million rows should not use it.
        """
        started = time.perf_counter()
        columns: list[str] = []
        rows: list[list[Any]] = []
        stats: dict[str, Any] = {}

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                f"{self.url.rstrip('/')}/v1/statement",
                content=sql.encode("utf-8"),
                headers=self._headers(),
            )
            response.raise_for_status()
            envelope = response.json()

            while True:
                # An error can arrive on any envelope, including after rows --
                # Trino streams results, so a query can fail halfway through
                # having already returned data. Checking only the first
                # response would report a partial result as a complete one.
                if "error" in envelope:
                    error = envelope["error"]
                    raise TrinoError(
                        f"{error.get('errorName', 'ERROR')}: {error.get('message', '')}\n\n{sql}"
                    )
                if envelope.get("columns") and not columns:
                    columns = [column["name"] for column in envelope["columns"]]
                rows.extend(envelope.get("data") or [])
                stats = envelope.get("stats", stats)

                next_uri = envelope.get("nextUri")
                if not next_uri:
                    break

                envelope = self._poll(client, next_uri)

        return TrinoResult(
            columns=columns,
            rows=rows,
            stats=stats,
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
        )

    def _poll(self, client: httpx.Client, uri: str) -> dict[str, Any]:
        """GET one nextUri, honouring Trino's 503-means-retry contract."""
        for attempt in range(60):
            response = client.get(uri, headers=self._headers())
            if response.status_code == httpx.codes.SERVICE_UNAVAILABLE:
                # Documented backpressure, not a failure. Trino asks the client
                # to re-issue the same URI; a plain raise_for_status() here
                # turns a busy coordinator into a spurious query failure.
                time.sleep(min(0.1 * (attempt + 1), 2.0))
                continue
            response.raise_for_status()
            return response.json()
        raise TrinoError(f"gave up polling {uri} after 60 attempts")

    def wait_until_ready(self, seconds: float = 120.0) -> None:
        """Block until the coordinator reports ACTIVE.

        ``docker compose up -d`` returns as soon as the container is started,
        and Trino spends ~30s loading plugins before it will accept a query --
        so scripting `up` followed by a query without this is a race that fails
        intermittently and looks like a config error.
        """
        deadline = time.monotonic() + seconds
        last = "no response"
        while time.monotonic() < deadline:
            try:
                info = httpx.get(f"{self.url.rstrip('/')}/v1/info", timeout=5.0).json()
                if not info.get("starting", True):
                    return
                last = f"starting (up {info.get('uptime')})"
            except httpx.HTTPError as exc:
                last = type(exc).__name__
            time.sleep(2.0)
        raise TrinoError(f"trino at {self.url} not ready within {seconds:.0f}s: {last}")
