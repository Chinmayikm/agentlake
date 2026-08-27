# ADR-002: RAG ingestion pipeline design decisions

- **Status:** Accepted
- **Date:** 2026-08-26
- **Context:** `services/rag/` -- fetches Kafka/Flink/Iceberg docs and turns
  them into a queryable corpus (`fetch -> chunk -> embed -> store ->
  retrieve(query, k)`) for a future eval harness / agent, retrieved via
  hybrid (dense + BM25) search and observed through the telemetry SDK's
  `RETRIEVAL` span. Wiring `retrieve()` into an actual agent turn belongs to
  `services/agent/`, not yet built, and is out of scope here -- this ADR
  covers the pipeline itself.

---

## 1. Qdrant is the production Store; sqlite+numpy is the Store protocol's test fake

**Decision.** `services/rag/store.py` defines a `Store` protocol (structural,
`typing.Protocol`) -- `upsert_document`, `content_hash_for`,
`replace_chunks`, `get_chunk`, `search`, `close`. Two implementations exist:
`CorpusStore` (sqlite + numpy brute-force cosine, in the same file) and
`QdrantStore` (`services/rag/qdrant_store.py`). Real ingest/retrieve default
to `QdrantStore`; every test uses `CorpusStore` -- real stdlib `sqlite3`
against `tmp_path`, no server, sub-millisecond, hermetic by construction.

**Why Qdrant in production, when the brute-force math still holds.** The
scale math from the original design (low thousands of chunks, a few MB of
vectors, a matmul is sub-millisecond) is still true, and it's exactly why
`CorpusStore` remains a good *test* fake. But production retrieval needs two
things a Python-process-local numpy array doesn't give for free:

- **Payload filtering at the database layer.** `project`/`corpus_version`
  filters (see #3 below) run inside Qdrant's query, not as a Python
  post-filter over an array that's already been loaded whole into the
  process -- the distinction that matters once retrieval is called from a
  service process (the future `services/agent/`) rather than a one-shot CLI.
- **A real service boundary.** ADR-001 already treats the gateway as its own
  service rather than a library every caller links in-process; the
  production vector store gets the same treatment here -- one more
  docker-compose service (`qdrant`, `qdrant/qdrant:v1.12.5`, 512m
  `mem_limit`, named volume `qdrant_data`, port 6333), not a database file
  living inside the CLI process's working directory.

**Why not rip out the sqlite implementation instead of keeping it as a
fake.** `CorpusStore` is exactly what a fast, hermetic, dependency-free unit
test wants -- no docker, no network, sub-millisecond. Every `services/rag`
test (chunk, embed, fusion, retrieve, cli) runs against it. Only
`QdrantStore` needs a running `qdrant` container, so it stays untested by
default (see #3's testing note), same as Kafka.

**Point id / schema.** `chunk_id`/`doc_id` are 16-hex-char sha1 prefixes (64
bits) -- Qdrant point ids accept an unsigned int directly
(`int(hex_id, 16)`), so no UUID remapping is needed. One collection
(`docs_v1`) holds both document markers (`payload.kind = "document"`, zero
vector, never returned by `search()`) and chunks (`payload.kind = "chunk"`,
a real embedding) -- one collection keeps `corpus_version` filtering and
collection lifecycle in one place instead of two.

---

## 2. Local embedding model (fastembed/ONNX), not the gateway, and not sentence-transformers

**Decision.** `services/rag/embed.py`'s `FastEmbedEmbedder` runs
`BAAI/bge-small-en-v1.5` (384-dim, ONNX) in-process via `fastembed`, imported
lazily inside `embed()` -- not at module import time, same pattern as
`services.sdk._get_kafka()`.

**fastembed accepted as a substitution for sentence-transformers.** Both
wrap the same class of local embedding models; fastembed's ONNX runtime has
no PyTorch dependency, which matters directly against the 4GB WSL cap
(`sentence-transformers` pulls in a full `torch` install). Model choice
(`BAAI/bge-small-en-v1.5`, 384-dim) and behavior (local, keyless, lazily
imported) are unchanged from what a `sentence-transformers`-backed
`Embedder` implementation would provide -- this is a dependency substitution
behind the same `Embedder` protocol, not a design change.

**Why this doesn't conflict with ADR-001.** ADR-001 makes `services/gateway`
the only thing allowed to hold `ANTHROPIC_API_KEY` or call an LLM provider.
Anthropic has no embeddings endpoint, so embeddings were never actually a
gateway question -- this is a separate, local, keyless capability, not a
bypass of the "single door" rule.

**Verified before implementation.** `fastembed`/`onnxruntime` wheel
availability for Python 3.14 was the one open risk flagged during planning.
Confirmed via `pip install --dry-run`: both resolve cleanly
(`onnxruntime==1.29.0`, `fastembed==0.8.0`), so the pure-numpy hashing-trick
fallback behind the same `Embedder` protocol was not needed.

**Injectable seam.** Every function that needs an embedder takes
`embedder: Embedder | None = None` and only builds `FastEmbedEmbedder` when
`None`. Tests always inject `FakeEmbedder` (`tests/conftest.py`, a
deterministic hash-of-text projection) -- no model download, no network, in
the default suite. `test_import_does_not_pull_in_fastembed`
(`tests/test_rag_embed.py`) subprocess-checks that `import services.rag`
never puts `fastembed`/`onnxruntime` in `sys.modules`, mirroring the SDK's
`test_import_does_not_pull_in_confluent_kafka`. The same pattern covers
Qdrant: `test_import_does_not_pull_in_qdrant_client`
(`tests/test_rag_qdrant_store.py`).

---

## 3. Hybrid retrieval: dense + BM25, fused with reciprocal rank fusion

**Decision.** `retrieve(query, k, mode=...)` supports three modes:
`"dense"` (embedding cosine similarity via `Store.search()`), `"bm25"`
(`services/rag/bm25.py`'s `BM25Index`), and `"hybrid"` (the default) --
both rankings pulled at a wider candidate pool (`max(20, k*5)`) and merged
via `services/rag/fusion.py`'s `reciprocal_rank_fusion(rankings, k=60)`.
`mode` is recorded on the emitted `RETRIEVAL` span (see #4).

**Why hybrid, not dense-only.** Dense embeddings and BM25 fail in different,
complementary ways -- dense catches semantic/paraphrase matches BM25 misses
entirely; BM25 catches exact identifier/config-key matches (`log.retention.
hours`) that a small embedding model can blur. The Day-5 A/B eval this
pipeline exists for needs all three modes queryable independently
(`mode="dense"` vs `"bm25"` vs `"hybrid"`) to actually measure the
difference -- a dense-only pipeline can't produce that comparison at all.

**Why RRF over score normalization.** Cosine similarity (roughly `[-1, 1]`)
and BM25 scores (unbounded, corpus-dependent) live on incomparable scales.
RRF sidesteps normalizing either: it only uses rank position
(`1/(k+rank)`), so fusing them needs no calibration step that could silently
drift as the corpus grows. `k=60` is the default from the original RRF
paper (Cormack et al., 2009); exposed as `retrieve(..., rrf_k=...)` since
the Day-5 eval may want to sweep it.

**BM25 index: separate, persisted artifact, not inside the vector store.**
`BM25Index` (`services/rag/bm25.py`) is a pickled `rank_bm25.BM25Okapi`
wrapper, persisted at `services/rag/data/bm25_index.pkl` (gitignored, same
status as `corpus.db`). `rank_bm25` has no incremental-update API -- the
index rebuilds lazily from the full tokenized corpus on the next `search()`
after any `replace_chunks()` call, which is sub-millisecond at this corpus's
scale. It lives outside whichever `Store` backend is active (sqlite fake or
Qdrant) because it's local ingest state, not something either backend's
service needs to host.

**Testing.** `reciprocal_rank_fusion()` gets a hand-computed unit test
(`tests/test_rag_fusion.py`) -- two small rankings, expected fused scores
worked out by hand from the `1/(k+rank)` formula, asserted with
`pytest.approx`. `BM25Index` and hybrid `retrieve()` are tested against
`CorpusStore` + `FakeEmbedder`, no real Qdrant or model needed.

---

## 4. Every retrieve() call emits a RETRIEVAL span

**Decision.** `retrieve()` wraps its body in
`with span("RETRIEVAL", "vector_search", index="docs-v1", top_k=k, mode=mode) as rspan:`,
matching the shape already stubbed in `services/demo_sdk.py`. Before
returning, `rspan.set(query=..., project=..., hits=len(results),
top_chunk_ids=..., top_scores=...)` -- `query` truncated to 200 chars
(mirrors `telemetry._MAX_ERROR_MESSAGE`'s reasoning: an attribute is a
`map<string,string>` field, not a place for unbounded text).

**Why.** This is an observability platform. Its own retrieval path being the
one unobservable thing in it would be the platform failing its own premise
-- every other cross-cutting call in this codebase (gateway's `LLM_CALL`,
the SDK's own dogfooding in `demo_sdk.py`) goes through a span; retrieval
doesn't get an exception.

**Testing.** `tests/test_rag_retrieve.py` asserts on emitted spans using the
SDK's list-collector pattern (`tests/conftest.py`'s `events` fixture) --
one `RETRIEVAL` event per `retrieve()` call, `mode`/`top_k` attributes
present and correct, `hits` matching the returned list length.

---

## 5. Pluggable per-project fetch strategy, not one scraper

**Decision.** `services/rag/sources.yaml` (config-not-code, mirrors
`services/gateway/models.yaml`'s versioned-table shape) declares each
project's pinned version and a `strategy` key, plus a top-level `version`
stamp used as `corpus_version` (see #1's Qdrant payload and the guard
below). `services/rag/fetch.py` implements one `FetchStrategy` per
publishing mechanism: `fetch_git_sparse_checkout` (a pinned commit SHA plus
one or more `sparse_path` entries -- individual files for a flat doc tree,
whole directories where a project genuinely splits into concepts/deployment/
ops) and `fetch_rendered_html` (for a future doc source that only publishes
rendered pages with no git-accessible source; not used by any currently
pinned project).

**Why.** All three currently pinned projects (Kafka, Flink, Iceberg) publish
their docs in git, so all three use `fetch_git_sparse_checkout` -- the real
per-project difference is file-level vs. directory-level `sparse_path`
patterns, not the fetch mechanism. Kafka's and Iceberg's doc trees are flat
(no concepts/config/ops subdirectories), so individual files are pinned
rather than whole directories; Flink's `docs/content/docs/` genuinely splits
into `concepts/`, `deployment/`, and `ops/`, so those three directories are
pinned whole. `fetch_rendered_html` stays in the protocol for the day a doc
source shows up with no git repo to sparse-checkout from. Adding a fourth
doc source later is a YAML entry plus, at most, one new strategy function.

**Resolved: pins are final commit SHAs, not placeholders.** `sources.yaml`
pins each project by exact commit SHA -- Kafka `771b9576...` (tag `3.8.0`),
Flink `ea37edb7...` (branch `release-1.20`), Iceberg `5f7c992c...` (tag
`apache-iceberg-1.7.0`) -- verified against the real repo trees before this
PR and confirmed by the full corpus ingest (see "Ingest run notes" below:
94 docs, 1,413 chunks). `test_fetch_rendered_html_live`
(`tests/test_rag_fetch.py`, `@pytest.mark.slow`) now builds its own
synthetic `ProjectSpec` rather than pulling Kafka's from `load_sources()`
-- Kafka's real spec stopped having a `urls` key once it moved to
`git_sparse_checkout`, which had left this slow-marked (so not caught by
the default `pytest -q` run) test silently broken until this pass. It
exercises `fetch_rendered_html` as a general capability now, not the real
pinned corpus, since no currently pinned project uses that strategy.

---

## 6. Idempotency via content-hash comparison, not conditional HTTP

**Decision.** `fetch_all()` always re-fetches (cheap -- a few MB of
HTML/markdown per run). `cli.py`'s `ingest` command computes a sha256
`content_hash` per fetched file and compares it against
`Store.content_hash_for(doc_id)`; only new/changed docs pay for the
expensive chunk+embed+store step. Changed docs get `replace_chunks()`
(delete-then-insert by `doc_id`) on both the vector store and the BM25
index.

**Why.** ETag/If-Modified-Since bookkeeping is one more thing to get subtly
wrong for a benefit this corpus size doesn't need -- the fetch itself is
already cheap, so gating only the expensive step is enough.

---

## 7. Deterministic chunk/doc ids via hashing

**Decision.** `services/rag/chunk.py`'s `doc_id_for(project, version,
source_path)` and each `Chunk.chunk_id` are sha1 hashes of their identifying
fields, not random or sequence-generated. `QdrantStore` reuses these
directly as point ids (`int(hex_id, 16)`, see #1).

**Why.** Re-ingesting unchanged content produces byte-identical `doc_id`s and
`chunk_id`s, which is what makes `replace_chunks()`'s delete-then-insert (on
either `Store` backend, and on the BM25 index) safe to run repeatedly and
gives a future eval harness stable citation keys.

---

## Corpus-version staleness guard (Qdrant)

`QdrantStore.search()` always filters on `payload.corpus_version ==
self.corpus_version` (constructed from `fetch.load_corpus_version()`,
i.e. `sources.yaml`'s top-level `version`). A point written under a
different `corpus_version` -- the embedding model changed, `sources.yaml`'s
version was bumped, a re-ingest hasn't run yet -- never matches the filter,
so a version mismatch surfaces as fewer-or-zero results rather than
silently blending stale or dimension-mismatched vectors into a ranking.

---

## Ingest run notes (first real corpus load, 2026-08-27)

**Thrashing incident + mitigation.** The first real ingest against the full
Kafka/Flink/Iceberg corpus thrashed the 8GB laptop (WSL capped at 4GB) --
high iowait, stalled partway through. Root cause: embedding is
memory-dominant at this machine's scale, not CPU-dominant, so a large
`fastembed.embed()` call materializes activations for the whole batch at
once. Mitigation, all in `services/rag`: `FastEmbedEmbedder.batch_size`
(`embed.py`) capped at 8 to bound peak RAM per call; `kafka`/`schema-registry`
deliberately left stopped during ingest to free memory for embedding (Qdrant
alone is what's needed); and `_ingest()` (`cli.py`) made resume-aware via
`Store.existing_chunk_ids()`/`upsert_chunks()` -- a doc's `content_hash` is
only written after every one of its chunks is confirmed present, so a killed
run re-detects that doc as incomplete and embeds only what's still missing,
instead of redoing the whole document.

**Qdrant WAL finding.** An unclean hard reboot mid-ingest dropped
`points_count` from 108 to 66. Confirmed this is *not* a bad volume mount
(the same failure class as the Kafka `log.dirs` incident): `docker inspect`
shows the compose mount lands exactly on `/qdrant/storage`, the path Qdrant
actually writes to, and a full `docker compose down`/`up` cycle (container
removed and recreated, not just restarted) round-tripped 1571 points
unchanged. The real cause is `wal_config`/`optimizer_config.flush_interval_sec:
5` -- writes are batched and flushed to disk every 5s, not fsynced per
request, so an abrupt power-cut (not a graceful `docker stop`, which lets
Qdrant flush on SIGTERM) can legitimately lose whatever was in the last
unflushed window. Data loss on unclean shutdown, not a misconfigured path.

**Retrieval behavior, empirically.** Querying the real corpus for the exact
config key `taskmanager.memory.process.size`: dense's top-3 missed
`deployment/memory/mem_migration.md` (an old->new config key mapping table)
entirely, surfacing only semantically-related-but-generic memory docs
instead; BM25 ranked `mem_migration.md` #1 on the literal match, and hybrid
promoted it to #2 -- concrete support for #3's "why hybrid" argument. The
reverse case also showed up: for "iceberg schema evolution", BM25's #3 was
an off-topic Flink checkpointing doc that merely shares vocabulary, which
hybrid's RRF fusion against dense's ranking corrected back out.

---

## Future (out of scope here)

Wiring `retrieve()` into an actual agent turn inside `services/agent/`, once
that package exists -- `retrieve()` already emits its own `RETRIEVAL` span
per call (see #4), so no further `services/sdk` changes are needed there.
