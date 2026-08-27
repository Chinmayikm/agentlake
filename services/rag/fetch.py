"""Pulls raw docs onto disk per-project, via a pluggable FetchStrategy.

Kafka publishes rendered HTML; Flink/Iceberg publish markdown in their repos
at a versioned path -- genuinely different publishing mechanisms, so this is
one protocol with two implementations keyed by `strategy` in sources.yaml,
not one scraper trying to handle both. See ADR-002 #5.

Fetch always runs -- it's cheap (a few MB) -- so there's no conditional-HTTP
bookkeeping here. Idempotency lives one layer up, in cli.py's ingest command,
which compares FetchedFile.content_hash against Store.content_hash_for() and
only pays for chunk+embed+store on changed/new docs. See ADR-002 #6.
"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

import yaml

DEFAULT_SOURCES_PATH = Path(__file__).parent / "sources.yaml"
DEFAULT_RAW_DIR = Path(__file__).parent / "data" / "raw"


@dataclass(frozen=True, slots=True)
class ProjectSpec:
    name: str
    version: str
    strategy: str
    config: dict[str, Any]


@dataclass(frozen=True, slots=True)
class FetchedFile:
    source_path: str
    local_path: Path
    content_hash: str


class FetchStrategy(Protocol):
    def __call__(self, spec: ProjectSpec, dest: Path) -> list[FetchedFile]: ...


def load_sources(path: str | Path = DEFAULT_SOURCES_PATH) -> list[ProjectSpec]:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return [
        ProjectSpec(
            name=name,
            version=str(cfg["version"]),
            strategy=cfg["strategy"],
            config={k: v for k, v in cfg.items() if k not in ("version", "strategy")},
        )
        for name, cfg in raw["projects"].items()
    ]


def load_corpus_version(path: str | Path = DEFAULT_SOURCES_PATH) -> str:
    """sources.yaml's top-level version -- stamped onto every point QdrantStore
    writes, so a bumped pin without a re-ingest shows up as a corpus_version
    mismatch (empty/reduced search results) rather than silently serving
    whatever the collection happened to hold last. See ADR-002's Qdrant
    corpus-version staleness guard section.
    """
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return str(raw["version"])


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch_rendered_html(spec: ProjectSpec, dest: Path) -> list[FetchedFile]:
    import httpx

    dest.mkdir(parents=True, exist_ok=True)
    fetched = []
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        for url in spec.config["urls"]:
            response = client.get(url)
            response.raise_for_status()
            filename = Path(urlparse(url).path).name or "index.html"
            local_path = dest / filename
            local_path.write_bytes(response.content)
            fetched.append(
                FetchedFile(
                    source_path=url, local_path=local_path, content_hash=_sha256(response.content)
                )
            )
    return fetched


def _run_git(args: list[str], cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def fetch_git_sparse_checkout(spec: ProjectSpec, dest: Path) -> list[FetchedFile]:
    repo = spec.config["repo"]
    ref = spec.config["ref"]
    sparse_path = spec.config["sparse_path"]

    dest.mkdir(parents=True, exist_ok=True)
    if not (dest / ".git").exists():
        _run_git(
            ["clone", "--depth", "1", "--branch", ref, "--filter=blob:none", "--sparse", repo, "."],
            cwd=dest,
        )
        _run_git(["sparse-checkout", "set", sparse_path], cwd=dest)
    else:
        _run_git(["fetch", "--depth", "1", "origin", ref], cwd=dest)
        _run_git(["checkout", "FETCH_HEAD"], cwd=dest)

    fetched = []
    root = dest / sparse_path
    for local_path in sorted(root.rglob("*")):
        if not local_path.is_file():
            continue
        data = local_path.read_bytes()
        source_path = str(local_path.relative_to(dest))
        fetched.append(
            FetchedFile(source_path=source_path, local_path=local_path, content_hash=_sha256(data))
        )
    return fetched


DEFAULT_STRATEGIES: dict[str, FetchStrategy] = {
    "rendered_html": fetch_rendered_html,
    "git_sparse_checkout": fetch_git_sparse_checkout,
}


def fetch_all(
    specs: list[ProjectSpec],
    dest_root: Path = DEFAULT_RAW_DIR,
    strategies: dict[str, FetchStrategy] | None = None,
) -> dict[str, list[FetchedFile]]:
    strategies = strategies or DEFAULT_STRATEGIES
    results: dict[str, list[FetchedFile]] = {}
    for spec in specs:
        strategy = strategies[spec.strategy]
        dest = dest_root / spec.name / spec.version
        results[spec.name] = strategy(spec, dest)
    return results
