#!/usr/bin/env python3
"""Fail if this branch breaks Avro BACKWARD compatibility.

Compares the working-tree copy of contracts/trace_event_v1.avsc against the copy
on a base git ref, using a live Schema Registry as the judge -- the same engine
that will reject the write in production, rather than a reimplementation of
Avro's resolution rules.

    1. read the base ref's schema out of git
    2. register it, and pin the subject to BACKWARD
    3. ask the registry whether this branch's schema is compatible with it

Standard library only: no jq, no pip install, so it runs identically on a CI
runner and on a laptop before dependencies are set up.

Usage::

    # against a live local stack (docker compose up -d kafka schema-registry)
    python3 scripts/check_schema_compat.py --wait 90

    # locally, without touching the real subject on your dev registry
    python3 scripts/check_schema_compat.py --subject citest-compat --reset

Exit codes: 0 compatible (or nothing to check), 1 incompatible, 2 operational
failure (registry unreachable, bad ref).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
DEFAULT_REGISTRY = "http://localhost:8081"
DEFAULT_SUBJECT = "traces.events.v1-value"
DEFAULT_SCHEMA = "contracts/trace_event_v1.avsc"
DEFAULT_BASE = "origin/main"

EXIT_OK = 0
EXIT_INCOMPATIBLE = 1
EXIT_ERROR = 2


def log(message: str) -> None:
    print(message, flush=True)


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def request(
    method: str, url: str, payload: dict[str, Any] | None = None, timeout: float = 15.0
) -> tuple[int, Any]:
    """Return (status, parsed body). HTTP errors are returned, not raised.

    The registry puts its most useful diagnostics in the body of 4xx responses,
    so swallowing them via HTTPError would throw away the reason for a failure.
    """
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", CONTENT_TYPE)
    req.add_header("Accept", CONTENT_TYPE)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode()
            status = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        status = exc.code

    try:
        return status, json.loads(body)
    except json.JSONDecodeError:
        return status, body


def wait_for_registry(registry: str, seconds: float) -> bool:
    """Poll /subjects until the registry answers, or the budget runs out."""
    deadline = time.monotonic() + seconds
    attempt = 0
    while True:
        attempt += 1
        try:
            status, _ = request("GET", f"{registry}/subjects", timeout=5.0)
            if status == 200:
                log(f"registry ready at {registry} (attempt {attempt})")
                return True
        except OSError:
            pass  # not listening yet
        if time.monotonic() >= deadline:
            return False
        time.sleep(2.0)


# ---------------------------------------------------------------------------
# git
# ---------------------------------------------------------------------------


def ref_exists(ref: str) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def schema_at_ref(ref: str, path: str) -> str | None:
    """The file's contents at `ref`, or None if it does not exist there."""
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"], capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    return result.stdout


def canonical(schema_text: str, source: str) -> str:
    """Compact the schema so whitespace differences never look like changes."""
    try:
        return json.dumps(json.loads(schema_text), separators=(",", ":"))
    except json.JSONDecodeError as exc:
        fail(f"{source} is not valid JSON: {exc}")
        raise SystemExit(EXIT_ERROR) from exc


# ---------------------------------------------------------------------------
# Schema Registry operations
# ---------------------------------------------------------------------------


def reset_subject(registry: str, subject: str) -> None:
    """Soft- then hard-delete, so repeat local runs start from nothing."""
    for url in (
        f"{registry}/subjects/{subject}",
        f"{registry}/subjects/{subject}?permanent=true",
    ):
        status, _ = request("DELETE", url)
        if status not in (200, 404):
            log(f"note: delete returned {status} for {url}")
    log(f"reset subject {subject}")


def register(registry: str, subject: str, schema: str) -> None:
    status, body = request(
        "POST",
        f"{registry}/subjects/{subject}/versions",
        {"schema": schema, "schemaType": "AVRO"},
    )
    if status != 200:
        fail(f"could not register the base schema to {subject}: HTTP {status}\n{body}")
        raise SystemExit(EXIT_ERROR)
    log(f"registered base schema to {subject} (id {body.get('id')})")


def set_compatibility(registry: str, subject: str, level: str) -> None:
    status, body = request(
        "PUT", f"{registry}/config/{subject}", {"compatibility": level}
    )
    if status != 200:
        fail(f"could not set {level} on {subject}: HTTP {status}\n{body}")
        raise SystemExit(EXIT_ERROR)
    log(f"compatibility for {subject} set to {level}")


def check_compatibility(registry: str, subject: str, schema: str) -> tuple[bool, Any]:
    # verbose=true makes the registry name the offending field instead of just
    # saying no. That message is the whole value of this job's output.
    status, body = request(
        "POST",
        f"{registry}/compatibility/subjects/{subject}/versions/latest?verbose=true",
        {"schema": schema, "schemaType": "AVRO"},
    )
    if status != 200:
        fail(f"compatibility check failed: HTTP {status}\n{body}")
        raise SystemExit(EXIT_ERROR)
    return bool(body.get("is_compatible")), body


# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument(
        "--subject",
        default=DEFAULT_SUBJECT,
        help="pass a scratch name locally to leave the real subject alone",
    )
    parser.add_argument(
        "--base", default=DEFAULT_BASE, help="git ref to compare against"
    )
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="path to the .avsc")
    parser.add_argument(
        "--wait", type=float, default=0.0, help="seconds to wait for the registry"
    )
    parser.add_argument(
        "--reset", action="store_true", help="delete the subject before registering"
    )
    args = parser.parse_args()

    # (b) the base ref's copy of the schema
    if not ref_exists(args.base):
        fail(f"git ref {args.base!r} does not resolve; fetch it first")
        return EXIT_ERROR

    base_text = schema_at_ref(args.base, args.schema)
    if base_text is None:
        log(f"NOTICE: {args.schema} does not exist on {args.base} yet -- nothing to")
        log("compare against. Skipping the compatibility check.")
        return EXIT_OK

    try:
        with open(args.schema, encoding="utf-8") as fh:
            head_text = fh.read()
    except OSError as exc:
        fail(f"could not read {args.schema}: {exc}")
        return EXIT_ERROR

    base_schema = canonical(base_text, f"{args.base}:{args.schema}")
    head_schema = canonical(head_text, args.schema)

    log(f"base    : {args.base}:{args.schema}")
    log(f"proposed: {args.schema}")
    if base_schema == head_schema:
        log("schemas are identical; the check below should be a formality.")

    if args.wait and not wait_for_registry(args.registry, args.wait):
        fail(f"registry at {args.registry} not ready after {args.wait:.0f}s")
        return EXIT_ERROR

    # (c) register the base version and pin the subject to BACKWARD
    if args.reset:
        reset_subject(args.registry, args.subject)
    register(args.registry, args.subject, base_schema)
    set_compatibility(args.registry, args.subject, "BACKWARD")

    # (d) ask the registry about the proposed schema
    compatible, body = check_compatibility(args.registry, args.subject, head_schema)
    if compatible:
        log(f"OK: proposed schema is BACKWARD compatible with {args.base}")
        return EXIT_OK

    messages = body.get("messages")
    if messages:
        # These already embed the old schema, so dumping the whole body as well
        # would repeat a multi-KB blob in the CI log for no extra information.
        detail = "Registry said:\n" + "\n".join(f"  - {m}" for m in messages)
    else:
        detail = f"Registry response:\n{json.dumps(body, indent=2)}"

    fail(
        f"{args.schema} is NOT BACKWARD compatible with {args.base}.\n"
        f"Subject: {args.subject}\n"
        f"{detail}\n\n"
        "A consumer built on the old schema could not read data written with the\n"
        "new one. See CLAUDE.md: never break Avro BACKWARD compatibility."
    )
    return EXIT_INCOMPATIBLE


if __name__ == "__main__":
    sys.exit(main())
