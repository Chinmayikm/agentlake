# Retrieval A/B — dense vs BM25 vs hybrid

Measured by `make eval-ab` (`python -m eval ab`) over the **whole 85-example
golden set**, not the 25-example CI subset. Retrieval only: one `retrieve()`
call per (question, mode), no agent and no LLM, so this costs nothing and is
the one number in ADR-008 that covers the full set.

## Summary

| | hit@5 | vs dense |
|---|---|---|
| `dense` | **0.9146** | — |
| `hybrid` (the production default) | **0.8659** | **-0.0488** |
| `bm25` | **0.7073** | -0.2073 |

**Two findings, and the second one is uncomfortable.**

**1. Dense retrieval was returning nothing at all, and hybrid was silently
BM25-only.** `services/rag/retrieve.py` built `QdrantStore()` without a
`corpus_version`, which is a search filter, so every dense query matched zero
points — and RRF over (empty, bm25) is bm25. The before/after tables below are
that bug measured rather than described: dense at `0.0000`, and hybrid's top-5
byte-identical to bm25's for **82/82** examples before, **0/82** after. See
ADR-008 §1. Reproduce the "before" row at any time with
`python -m eval ab --corpus-version unknown`.

**2. With dense repaired, `hybrid` — the production default that ADR-002 §3
chose — is 4.9 points WORSE than dense alone.**

That is a real result and it is not what ADR-002 predicted. The per-project
split says why:

| | dense | bm25 | hybrid |
|---|---|---|---|
| kafka (3 documents) | **1.0000** | 0.3333 | 0.7778 |
| flink (81 documents) | 0.8333 | 0.8750 | **0.9167** |
| iceberg (10 documents) | 0.9032 | 0.9032 | 0.9032 |

**Fusion helps exactly where BM25 is competitive and hurts where it is not.**
On Flink, BM25 beats dense and hybrid beats both — this is ADR-002 §3's
argument, confirmed. On Kafka, BM25 scores 0.3333 against dense's 1.0000, and
RRF's equal weighting drags a perfect ranking down to 0.7778. The likely cause
is corpus shape rather than anything about Kafka: the three Kafka pages are
huge (`ops.html` alone is 182 of 1477 chunks) while Flink contributes 81
documents, so a Kafka question's vocabulary has low IDF inside Kafka's own
pages and BM25 surfaces rarer-term Flink chunks instead.

**The obvious confound, tested and rejected.** These questions were authored
from prose sections, so the natural objection is that the set is mostly
paraphrase — dense's home ground — and under-represents the exact-identifier
lookups BM25 is for. The by-type table below refutes it: dense wins on `config`
questions too (0.8696 vs BM25's 0.6957), which is precisely the category that
names `log.retention.hours` and `write.target-file-size-bytes` literally. The
split that matters is by project, not by question type.

**What this does not settle.** hit@5 here asks whether the right *file*
appeared, and Kafka has only three files to choose between, so its dense
`1.0000` is an easier target than Flink's. A section-level metric would be
harder and is deliberately deferred (ADR-008 §3: keying on sections would make
trace-derived and observation-derived hit@k compute different numbers and
destroy the cross-check between them). This measures retrieval in isolation;
whether the delta survives into end-to-end answer quality is what the judge
scores, and is not answered here.

**Recommendation, not yet applied.** `mode="hybrid"` remains the default in
`services/rag` and in `search_docs`. Changing it is a design decision, not an
eval output, and it should not be made on one corpus's numbers — but the case
for revisiting it, or for weighting RRF by per-project BM25 quality, is now
measured rather than argued.

---

### Before the fix — `retrieve()` built `QdrantStore()`, so corpus_version defaulted to `unknown`

`corpus_version = unknown`, k = 5, 82 answerable examples (3 unanswerable examples excluded -- they have no expected source, so every mode scores 0 and only the denominator moves).

| mode | hit@5 | kafka | flink | iceberg | wall |
|---|---|---|---|---|---|
| `dense` | **0.0000** | 0.0000 | 0.0000 | 0.0000 | 11.0s |
| `bm25` | **0.7073** | 0.3333 | 0.8750 | 0.9032 | 2.5s |
| `hybrid` | **0.7073** | 0.3333 | 0.8750 | 0.9032 | 11.3s |

| mode | conceptual (n=36) | config (n=23) | factual (n=23) |
|---|---|---|---|
| `dense` | 0.0000 | 0.0000 | 0.0000 |
| `bm25` | 0.6667 | 0.6957 | 0.7826 |
| `hybrid` | 0.6667 | 0.6957 | 0.7826 |

**hybrid - dense = +0.7073**
hybrid's top-5 is byte-identical to bm25's for **82/82** examples.

### Where the modes disagree

| example | dense | bm25 | hybrid | question |
|---|---|---|---|---|
| `flink-backpressure-001` | - | hit | hit | The web UI shows High back pressure on my Source. What is a... |
| `flink-backpressure-002` | - | hit | hit | Which metrics tell me whether a subtask is back pressured, ... |
| `flink-checkpoint-002` | - | hit | hit | I cancelled my job and the checkpoints disappeared. Is that... |
| `flink-checkpoint-003` | - | hit | hit | What does unaligned checkpointing change, and when is it wo... |
| `flink-checkpoint-004` | - | hit | hit | If I switch checkpointing to at-least-once, does my map-and... |
| `flink-failover-001` | - | hit | hit | When one task fails, does Flink have to restart the whole job? |
| `flink-memory-001` | - | hit | hit | What is the difference between taskmanager.memory.process.s... |
| `flink-production-001` | - | hit | hit | Why should I set max parallelism explicitly, and what value... |
| `flink-production-002` | - | hit | hit | Why does every operator need an explicit uid before I rely ... |
| `flink-restart-001` | - | hit | hit | If I enable checkpointing but never configure a restart str... |
| `flink-restart-002` | - | hit | hit | Why is an exponential delay restart strategy recommended ov... |
| `flink-savepoint-001` | - | hit | hit | I want to move a job from RocksDB to the heap state backend... |
| ... | | | | 46 more |

### After the fix — `default_store()`, corpus_version = the ingested pin

`corpus_version = 2026-08-27-pinned`, k = 5, 82 answerable examples (3 unanswerable examples excluded -- they have no expected source, so every mode scores 0 and only the denominator moves).

| mode | hit@5 | kafka | flink | iceberg | wall |
|---|---|---|---|---|---|
| `dense` | **0.9146** | 1.0000 | 0.8333 | 0.9032 | 12.3s |
| `bm25` | **0.7073** | 0.3333 | 0.8750 | 0.9032 | 2.6s |
| `hybrid` | **0.8659** | 0.7778 | 0.9167 | 0.9032 | 8.0s |

| mode | conceptual (n=36) | config (n=23) | factual (n=23) |
|---|---|---|---|
| `dense` | 0.9444 | 0.8696 | 0.9130 |
| `bm25` | 0.6667 | 0.6957 | 0.7826 |
| `hybrid` | 0.8889 | 0.8261 | 0.8696 |

**hybrid - dense = -0.0488**
hybrid's top-5 is byte-identical to bm25's for **0/82** examples.

### Where the modes disagree

| example | dense | bm25 | hybrid | question |
|---|---|---|---|---|
| `flink-checkpoint-001` | hit | - | - | What is the difference between a checkpoint and a savepoint? |
| `flink-checkpoint-004` | - | hit | hit | If I switch checkpointing to at-least-once, does my map-and... |
| `flink-checkpoint-005` | hit | - | hit | Which configuration option turns checkpointing on? |
| `flink-memory-001` | - | hit | hit | What is the difference between taskmanager.memory.process.s... |
| `flink-savepoint-001` | - | hit | - | I want to move a job from RocksDB to the heap state backend... |
| `flink-state-003` | hit | - | hit | Which configuration option selects the state backend? |
| `flink-window-001` | - | hit | hit | What kinds of windows does Flink distinguish? |
| `iceberg-branching-001` | - | hit | hit | How can I validate data quality before it becomes visible i... |
| `iceberg-flink-001` | - | hit | - | Which catalog types can the Flink Iceberg connector use? |
| `iceberg-reliability-004` | hit | - | hit | Why is planning an Iceberg scan cheaper than planning a Hiv... |
| `iceberg-schemas-001` | hit | - | - | Which numeric type promotions does Iceberg allow? |
| `kafka-compaction-001` | hit | - | hit | If log compaction removes a message, do the offsets after i... |
| ... | | | | 17 more |


---

## Reproducing

```
docker compose --profile rag up -d qdrant     # the only thing that must run
make eval-ab                                  # after the fix
python -m eval ab --corpus-version unknown    # before the fix
```

Wall time is ~25s for all three modes over 82 examples, dominated by the
embedding model. The store, embedder and BM25 index are constructed once and
threaded through every call -- letting `retrieve()` build its own defaults
would load fastembed's ONNX model on each of the ~246 calls, which is ADR-003
§6's observer-effect lesson in its third instance.
