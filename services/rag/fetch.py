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


def _sparse_pattern(path: str) -> str:
    """Non-cone sparse-checkout uses gitignore-style patterns: anchor at repo
    root (leading /) and, for a directory (no file suffix), a trailing / so
    it recurses -- otherwise it matches only an entry literally named `path`.
    """
    pattern = path if path.startswith("/") else f"/{path}"
    if not Path(path).suffix and not pattern.endswith("/"):
        pattern += "/"
    return pattern


def fetch_git_sparse_checkout(spec: ProjectSpec, dest: Path) -> list[FetchedFile]:
    """`ref` may be a branch, tag, OR a full commit SHA -- `git clone --branch`
    only accepts the first two, so this uses init+fetch instead, which GitHub
    (and most modern git hosts) will resolve any of the three against.

    `sparse_path` may be a single path or a list -- some projects split docs
    into multiple sibling directories (concepts/deployment/ops), others need
    individual files pulled out of an otherwise-irrelevant flat directory
    (Kafka's docs/design.html, docs/configuration.html, docs/ops.html, not
    its entire docs/). Cone mode (git's default) silently promotes a file
    pattern to its whole containing directory -- --no-cone is what actually
    respects file-level patterns.
    """
    repo = spec.config["repo"]
    ref = spec.config["ref"]
    sparse_paths = spec.config["sparse_path"]
    if isinstance(sparse_paths, str):
        sparse_paths = [sparse_paths]

    dest.mkdir(parents=True, exist_ok=True)
    if not (dest / ".git").exists():
        _run_git(["init"], cwd=dest)
        _run_git(["remote", "add", "origin", repo], cwd=dest)
        _run_git(["sparse-checkout", "init", "--no-cone"], cwd=dest)
        _run_git(["sparse-checkout", "set", *(_sparse_pattern(p) for p in sparse_paths)], cwd=dest)
    _run_git(["fetch", "--depth", "1", "--filter=blob:none", "origin", ref], cwd=dest)
    _run_git(["checkout", "FETCH_HEAD"], cwd=dest)

    fetched = []
    for sparse_path in sparse_paths:
        root = dest / sparse_path
        candidates = [root] if root.is_file() else sorted(root.rglob("*"))
        for local_path in candidates:
            # docs trees ship images/assets alongside content; chunk.py only reads text
            if not local_path.is_file() or local_path.suffix not in (".md", ".html", ".htm"):
                continue
            data = local_path.read_bytes()
            source_path = str(local_path.relative_to(dest))
            content_hash = _sha256(data)
            fetched.append(
                FetchedFile(
                    source_path=source_path, local_path=local_path, content_hash=content_hash
                )
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
