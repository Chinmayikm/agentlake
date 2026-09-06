# ADR-008: The evaluation harness — and what it found on the way

- **Status:** Accepted
- **Date:** 2026-09-05
- **Context:** `eval/`, `services/agent/prompts/`, `metadata/sql/08_eval_harness.sql`,
  `scripts/load_prompts.py`, `metadata/client.py`, the `eval-preflight` /
  `eval-gate` CI jobs, and one line in `services/rag/qdrant_store.py` that
  turned out to matter more than the rest of it.

Every slice so far measures *what happened*: spans on a topic, facts in
Iceberg, percentiles in ClickHouse, a dimension joined through CDC. Nothing
measured whether the agent's **answers were any good**. ADR-007 built
`golden_examples`, `eval_runs` and `eval_results` and said plainly: *"The eval
harness itself is not built here."* This is that harness.

It was supposed to be the quality layer. It began by discovering that two of
the things it was going to measure did not work.

---

## 1. Dense retrieval had been returning nothing, and hybrid was BM25 wearing a different name

`QdrantStore.search()` filters on `corpus_version` — the stale-index guard
ADR-002 introduced deliberately, so a version mismatch surfaces as zero results
rather than as silently blended stale vectors. That guard worked exactly as
designed. The problem was who it was guarding.

```python
# services/rag/retrieve.py                # services/rag/cli.py
def _default_store():                     def _default_store():
    return QdrantStore()                      return QdrantStore(
                                                  corpus_version=load_corpus_version())
```

`QdrantStore.corpus_version` defaults to `"unknown"`. Ingest wrote
`"2026-08-27-pinned"`. So **the library path matched zero points on every dense
query**, and because reciprocal rank fusion over `(empty, bm25)` is `bm25`,
`mode="hybrid"` was BM25-only and looked entirely healthy from outside.

That is the path `services/mcp_server`'s `search_docs` takes. Every agent turn
since ADR-003 retrieved through BM25 alone, and `mode="dense"` returned nothing
at all. ADR-002's empirical hybrid-vs-dense notes are unaffected — they were
measured through the CLI, which was on the correct side of the split.

**Two call sites that had to agree, and did not.** The fix is one factory:

```python
def default_store() -> QdrantStore:
    return QdrantStore(corpus_version=load_corpus_version())
```

`retrieve.py`, `cli.py` and `mcp_server`'s `warmup()` all go through it, and a
source-text test asserts that no other module constructs a `QdrantStore`
directly. The dataclass default stays `"unknown"` on purpose: a
`default_factory` that reads YAML off disk inside a constructor is hidden I/O,
and `"unknown"` is the honest value for a store nobody configured. The fix is
that nothing constructs an unconfigured one.

### What made it invisible, and what now makes it visible

The RETRIEVAL span recorded `top_k`, `mode`, `hits`, `top_chunk_ids`,
`top_scores` — everything except the one thing that would have answered *"why
zero?"*. It now records `corpus_version` and `top_source_paths`. The first
makes "retrieval returned nothing" answerable from the trace alone; the second
says WHICH documents came back, so reading a trace no longer means resolving
every chunk id against the store — asking the store a question the trace was
supposed to answer.

`python -m services.rag diagnose` (`make rag-preflight`) is the permanent
guard, and its fingerprint needs no labelled data at all:

```
                              dense empty   hybrid identical to bm25
    QdrantStore()                   6/6              6/6
    default_store()                 0/6              0/6
```

Dense returning nothing for *every* probe is not a bad index, it is no index.
Hybrid's top-k being byte-identical to BM25's for *every* probe means fusion
had nothing to fuse. The harness runs the same assertion before it will spend
money, because a corpus_version mismatch would otherwise produce a full run of
legitimate-looking zeros that reads as a retrieval regression.

---

## 2. `prompt_version` was a label on requests that were byte-identical

ADR-007 §6 stamped `prompt_version` onto AGENT_STEP and LLM_CALL spans, made
`fct_cost_by_prompt` a real join, and populated two Grafana panels. All of that
was true. What was not true was that the versions differed.

`ChatRequest` had no `system` field, `services/agent` never sent one, and
`prompt_versions.template_text` was read by nothing (grep confirms). v1, v2 and
v3 produced identical bytes on the wire. The dimension was a join key
describing a difference that did not exist — and the eval's whole prompt A/B
would have compared a label to itself.

**Decision.** `ChatRequest.system: str | None`, forwarded verbatim into
`messages.create(system=...)`. This is ADR-003 §5's argument for `tools`,
unchanged: a Messages API parameter, not a new class of capability, so
`cost_usd`, `price_table_version` stamping and error mapping all work untouched
— asserted by a test that a system prompt does not disturb costing.

**A field, not a header** — and the contrast with `X-Prompt-Version` sitting
beside it is the whole point. That one is *telemetry about the caller*: the
gateway only stamps it on a span it already opens, so putting it in the body
would imply the gateway does something with it. `system` is an *instruction to
the model*: it changes what the provider is asked. ADR-007 §6 drew this
distinction to justify a header; the same distinction justifies a field here.

**Prompts are files.** `services/agent/prompts/v1.md` … `v5.md`, plain
markdown, no front-matter — `load_prompt()` returns exactly the bytes on disk,
because an eval score is only reproducible if "what was the prompt" has a
byte-exact answer. Descriptive metadata lives in a `params.yaml` sidecar
instead (config-not-code, the same shape as `models.yaml` and `sources.yaml`).

**v4 is v3's text verbatim**, and the bump records a change in *delivery*
rather than wording: the same words now actually reach the model. Same words
plus different behaviour is a different system, so it gets a different version.
**v5 removes exactly the three instructions the harness scores** — search
first, cite sources, admit when the corpus does not answer — so a red gate is
attributable to a stated cause rather than to noise, and `params.yaml` marks it
`degraded: true` so nobody ships it by accident.

`scripts/load_prompts.py` publishes the files into `prompt_versions`, one
direction only: nothing reads `template_text` back into the agent, because
ADR-007 §6 is right that an agent refusing to run over a missing metadata row
would make the telemetry a dependency of the thing it observes.

Two things running it taught, neither of which was in the design:

- **`ON CONFLICT ... DO UPDATE` needs a `WHERE`.** Without one, every bring-up
  writes a WAL record for rows nobody touched, and Debezium emits an `op='u'`
  for each — at which point the changelog stops being a record of change.
- **`params_json` had to merge, not replace.** The first version silently
  discarded 07_seed.sql's `style` / `cite_sources` / `tool_hint` keys. Those
  are another writer's data, and this script does not own them; `||` keeps
  both. Same instinct as `cdc_land.py` ignoring an unknown key rather than
  crashing on it.

---

## 3. The golden set, and the authoring-bias rule

85 examples across the three pinned projects; a stratified 25 tagged `ci`.
One YAML file per project, so `project` comes from the file and cannot disagree
with the content.

**`expected_sources` are `source_path` prefixes, OR-ed.** A prefix is either an
exact file or a directory — one mechanism, no globs. OR rather than AND because
the question is whether retrieval surfaced *any* of the right places;
requiring all of them would measure how redundantly the corpus covers a topic
rather than how well the retriever finds it.

**No section-level keying in v1, and the reason is structural.** The RETRIEVAL
span carries `top_source_paths` but not section breadcrumbs, which are long
enough to blow the attribute budget. If the dataset keyed on sections, the
trace-derived and observation-derived hit@k would compute *different numbers* —
destroying the cross-check that is the only reason for having two sources.
Path-only makes them provably identical. Section keying is a v2 question.

**The authoring-bias rule, enforced rather than promised.** Every question was
written *from* a named source file and heading, and only then was retrieval run
against it. Questions written from retrieval results are biased toward being
retrievable and inflate hit@k by construction. `written_from` is a required
field, and a test asserts both that it names a real corpus document and that
that document is among the example's `expected_sources` — an example written
from one file but scored against another is either a typo or a question that
does not test what it claims to.

**`corpus_paths.txt` is generated and committed**, so the contract test that
every prefix resolves runs with no Qdrant. It caught a real parser bug on its
first run — the reader was keeping the project column as part of the path, so
all 85 examples "failed" at once. A check that fails loudly on its own bug is
doing its job.

**Three deliberately unanswerable examples.** "Say plainly when the corpus does
not answer the question" is an instruction v4 gives and v5 removes; without
these, the degraded prompt would only look terser. They are excluded from the
hit@k denominator — they have no source to hit — and included in faithfulness,
where refusing correctly is a 5.

---

## 4. Two published retrieval numbers, and one of them is uncomfortable

`make eval-ab` runs retrieval only — no agent, no LLM, no money — over all 82
answerable examples, so it is the one measurement here that covers the full set
rather than the CI subset. Full report: `docs/eval/retrieval_ab.md`.

| | hit@5 | vs dense |
|---|---|---|
| `dense` | **0.9146** | — |
| `hybrid` (the production default) | **0.8659** | **−0.0488** |
| `bm25` | **0.7073** | −0.2073 |

**Hybrid, which ADR-002 §3 chose as the default, is 4.9 points worse than dense
alone.** The per-project split says why:

| | dense | bm25 | hybrid |
|---|---|---|---|
| kafka (3 documents) | **1.0000** | 0.3333 | 0.7778 |
| flink (81 documents) | 0.8333 | 0.8750 | **0.9167** |
| iceberg (10 documents) | 0.9032 | 0.9032 | 0.9032 |

Fusion helps exactly where BM25 is competitive and hurts where it is not. On
Flink, BM25 beats dense and hybrid beats both — ADR-002 §3's argument,
confirmed. On Kafka, BM25 scores 0.3333 against dense's 1.0000 and RRF's equal
weighting drags a perfect ranking down. The likely cause is corpus shape rather
than anything about Kafka: the three Kafka pages are huge (`ops.html` alone is
182 of 1477 chunks) while Flink contributes 81 documents, so a Kafka question's
vocabulary has low IDF inside Kafka's own pages and BM25 surfaces rarer-term
Flink chunks instead.

**The obvious objection, tested and rejected.** These questions were authored
from prose, so the natural complaint is that the set is mostly paraphrase —
dense's home ground — and under-represents exact-identifier lookups. The
by-type table refutes it: dense also wins on `config` questions (0.8696 against
BM25's 0.6957), the category that names `log.retention.hours` and
`write.target-file-size-bytes` literally. The split that matters is by project,
not by question type.

**`mode="hybrid"` is left as the default.** Changing it is a design decision,
not an eval output, and one corpus's numbers are not enough to make it. The
case for revisiting it — or for weighting RRF by per-project BM25 quality — is
now measured rather than argued, which is the thing the harness exists to
provide.

---

## 5. The judge: two calls, and what blinding cannot be

**Two calls per example, and the split is structural rather than cautious.**
Faithfulness is graded against the *retrieved chunks* and must not see the
reference answer, or it grades agreement-with-reference instead of
groundedness. Answer quality is graded against the *reference* and must not see
the chunks, or an answer that is chunk-supported but wrong earns credit. One
call means one context holding both, and a model cannot unsee either. Both
prompt builders are tested for what they must **not** contain.

Rubrics live in `eval/rubric/*.md` and a test asserts the file's text appears
verbatim in the prompt — otherwise a rubric edit changes nothing and the file
becomes documentation of something that is not happening.

**Model pinning.** `JUDGE_ALIAS = "quality"`. `eval_runs.judge_model` records
the *provider model id*, not the alias, because a silently re-pointed alias is
exactly what makes two "identical" runs incomparable and is invisible from the
alias alone.

**No temperature**, and stated precisely: not "temperature 0" but *provider
default sampling* — the parameter was removed from the Messages API on
current-generation models. The judge is therefore nondeterministic, which is
*why* §7's thresholds are measured from repeat runs rather than assumed to be
zero.

### Bias controls — three, and one honest limit

- **Position randomisation** on the quality call: whether the reference block
  precedes or follows the candidate, seeded per *example* rather than drawn
  from one stream, so a resumed run presents the same example in the same order
  the full run did. Recorded as `judge_order` on the row, because a control
  nobody can check afterwards is a claim rather than a control.
- **Blinding**: the judge never sees the prompt version, model alias, run id or
  any other system identity — only the question, the passages or the reference,
  the rubric, and the answer.
- **Length control**: Pearson *r* between each score and `answer_len_tokens`,
  reported and flagged, never gated. At n=75 the 95% CI on *r* is about ±0.22,
  so only |r| above ~0.23 is distinguishable from zero; above 0.4 the rubric is
  buying length. At n=25 it is noise, and is reported only for full runs.

**The limit, stated rather than glossed.** Full A/B blinding is not achievable
with one system and one human reference: the judge must be *told* which is the
reference, or "grade the candidate against the reference" has no meaning. What
can honestly be controlled is position. True blind pairwise comparison needs
two candidate systems — v4's answer against v5's, labelled A and B — and is
named here and deferred.

### Parsing, and never a default score

Exactly one fenced JSON block, `{"score": <int 1-5>, "rationale": "…"}`. The
parser takes the **last** object, because a model that thinks out loud leaves
an example object earlier in the text and taking the first would grade the
example. A **fractional score is rejected, not rounded** — a judge emitting 3.7
reports precision it does not have, which is exactly why
`eval_results.faithfulness` is a `smallint`, and rounding here would defeat
that choice silently.

One retry, with an appended repair instruction rather than the same prompt: an
identical retry re-rolls the same failure mode and buys nothing but a second
bill. Then `score = NULL` and `judge_parse_ok = false`. **Never a default.** A
fabricated 3 is indistinguishable from a real one in `eval_results` and would
move the gated faithfulness mean with nothing raising — the lie ADR-003 §3
forbids, in a new place.

---

## 6. Metrics: three decisions that decide what the numbers mean

**hit@k is scored on the FIRST `search_docs` call**, not the union of every
call. A metric an agent can brute-force by searching five times measures the
agent, not the retriever — and first-call-only is what makes `make eval-ab`,
which issues exactly one retrieval per question, comparable to a full run.
`any_call_hit` is reported separately and gated on never.

**"The agent never searched" is `None`, not `False`.** That is a different
failure from the retriever missing, and collapsing them would make a prompt
change that stops the agent searching look like a retrieval regression — which
is precisely the v4-to-v5 case the gate has to *diagnose*, not merely detect.
The headline `hit_at_5_rate` counts a `None` as a miss, because that is what a
user experiences end to end; `search_rate` and `hit_at_5_given_search` are
reported as its two components. Which convention applies is explicit at every
call site rather than a default, so no reader has to guess which question a
number answers.

**`citation_ok` is a smoke detector and is documented as one.** It is string
overlap: it cannot tell a citation from a coincidence, cannot check that the
cited source *supports* the claim (that is faithfulness's job), and scores
False for a correct answer that cites in prose — a false negative asserted in a
test so it is a known property rather than a surprise. It is reported so a
*collapse* in it is visible, not so any particular value is believed.

---

## 7. The threshold math, and the part that is weak

`make eval-baseline` runs the 25-example CI subset twice, identically. For two
observations,

$$s = \frac{|x_1 - x_2|}{\sqrt 2}, \qquad T = \max(2s,\ \text{floor})$$

**The floors are derived from the discreteness of the measurement.** The judge
emits integers, so one example flipping by one point moves a 25-example mean by
exactly 1/25 = 0.04:

- `faithfulness_mean` → **0.20**, five single-point flips: the smallest change
  that cannot be a handful of borderline 3-vs-4 calls.
- `hit_at_5_rate` → **0.08** = 2/25: the smallest movement that cannot be one
  flaky retrieval.

A test asserts both are multiples of 1/25 — a floor that was not would fire, or
fail to fire, on fractions of an example, which do not exist.

**And σ̂ from n = 2 is weak, which is the honest part.** One degree of freedom
puts the χ² 95% interval on σ at roughly [0.45 s, 32 s] — an order of magnitude
either way. **The floors are doing the work; the 2s term is a sanity check, not
a calibration.** The rule that follows is written into the gate's own output:
if 2s exceeds twice the floor, do *not* widen the threshold — that reading
means the harness is too noisy to gate at this n, and the answer is more
examples, not more tolerance.

**Why 25 fixed examples is defensible at all.** Binomial sampling error at
n = 25 would be about 0.08 on a hit rate near 0.8 — the size of the floor. It
does not apply, because the *same* 25 questions run every time; there is no
resampling. The only run-to-run variance is the agent's and the judge's
nondeterminism, and that is exactly what the two baseline runs measure.

**Gated** (one-sided; only a regression fails): `faithfulness_mean`,
`hit_at_5_rate`. An improvement is reported and prompts a deliberate
re-baseline, because a gate that auto-adopted every improvement could not tell
a real gain from a lucky run. **Failing on a separate exit path**: config drift
(exit 2) and harness failures (exit 3) — "faithfulness dropped 0.3" and "you
compared v4 against v5" and "the gateway fell over" need different responses,
and reporting any of them as another sends someone hunting a regression that is
not there. Everything else is **reported, never gated** — the same
BLOCKING/WARN rule ADR-006 §6 wrote down.

---

## 8. Spending money on purpose

The harness is the one part of this repo whose job is to spend, so the fence is
four independent layers: an explicit `--yes` or an interactive confirmation; a
hard refusal if `pytest` is in `sys.modules`; a gateway health check (and the
harness never holds `ANTHROPIC_API_KEY` itself, so it *cannot* reach a provider
even if the rest failed — ADR-001 §1 unchanged); and CI never invoking
`make eval` at all.

`eval/budget.py` keeps a gitignored ledger. Cost comes from the gateway's
`/v1/stats`, which is computed from `models.yaml` and the provider's own token
counts and is the only figure in this repo entitled to be called the cost. The
ledger persists it **because `/v1/stats` does not**: gateway stats are
process-lifetime, so a cap enforced against them alone would silently reset
every time the gateway restarted.

Every paid target refuses before spending if the projection would breach the
cap, checks after every example, and appends what it actually cost. Writing the
tests found a real bug: the baseline was taken on the first *post-example*
poll, making example 1's spend invisible to the cap. It is now taken before the
loop.

**Results are committed per example, not batched.** A 25-example run that dies
at 24 must not throw away what it paid for — which is what
`UNIQUE (eval_run_id, golden_example_id)` in migration 08 turns from a weak
guard into a working upsert. Without a constraint to conflict on, `ON CONFLICT`
is not a weak guard: it is a syntax error. That is the same lesson ADR-007 §7
recorded for the seed's `ON CONFLICT DO NOTHING`, met again in a new place.

A failed example becomes a row with `NULL` scores and an `error`, and
`RunSummary.failures` is loud, because averaging a failure in as a zero reports
a harness fault as a quality regression.

**Retries are the harness's job, not the agent's.** A 429 does *not* arrive as
a `tool_result`: `HttpGatewayClient` calls `raise_for_status()` and
`run_turn`'s `try/except` wraps only tool execution, so a rate limit propagates
out of the turn. ADR-003 §2 put retry policy explicitly out of scope for the
agent; the harness honours `Retry-After` with bounded exponential backoff, and
does not retry a 4xx that is not a 429 — a 400 will be a 400 again, and
retrying it three times spends three times as much on the same mistake.

---

## 9. CI posture

Two jobs, and the split is forced rather than stylistic: `secrets` is not
usable in a job-level `if:` (only `github`, `needs`, `vars` and `inputs` are)
but *is* usable in a step-level `env:`, so `eval-preflight` converts "does the
key exist" into a boolean output that `eval-gate` can gate on. `-n "$KEY"`
never echoes the key.

**A fork PR gets a loud green notice, never a red X.** Failing a contributor's
PR because they cannot hold the maintainer's API key would punish them for the
repo's configuration. The cost is stated rather than hidden: eval paths *can*
change without being gated, and §10 says so.

CI runs the **real** Qdrant and the real ingest. A sqlite `CorpusStore` would
be faster, and was rejected: it is exact brute-force cosine where Qdrant is
approximate HNSW, so its top-5 can differ — CI would need its own baseline and
neither number would be production's — and it has no `corpus_version` filter,
which would leave §1's bug class untested in the one place it should be caught.
What *is* cached is the slow, network-flaky half: the three sparse clones. A
separate loud step asserts the corpus is complete, because a partial ingest
otherwise shows up as a lower hit rate and reads as a quality regression.

CI also starts Postgres, which **partially closes ADR-007 §10's "CI runs
neither Postgres nor Debezium"** — Postgres now yes, Debezium still no. It does
not start Kafka: span emits fail and are swallowed (ADR-000), so the CI run is
untraced, which is exactly why observation-derived hit@k is the default rather
than the fallback.

`services/gateway/` is in the path filter deliberately — a `models.yaml` edit
or a passthrough change alters what the agent sends, and leaving it out would
be the blind spot §10 would have to confess to.

---

## 10. What the gate does not catch

> The gate scores 25 fixed questions from a three-project pinned corpus. It
> says nothing about latency, multi-turn behaviour, adversarial input, or any
> question outside the set. It cannot detect a regression the judge shares: a
> change that makes both the agent and the judge more credulous *raises*
> faithfulness. The reference answers were written by the same person who wrote
> the prompts, so "answer quality" means "agrees with the author", and a
> reference that is itself wrong makes a correct answer score 2. hit@5 asks
> whether the right *file* appeared — never the right passage, and never
> whether the answer used it; on Kafka there are only three files to choose
> between, so that project's numbers are an easier target than Flink's.
> `citation_ok` is string overlap and is reported precisely because it should
> not be trusted. The thresholds are calibrated from two runs, so a regression
> smaller than ~0.2 faithfulness points or ~2 of 25 retrievals is invisible by
> construction. And the gate does not run at all on fork PRs, or when a change
> arrives through a path outside its filter — a regression can ship untested
> for reasons that have nothing to do with its size.

---

## 11. Deliberately not built

- **Landing `eval_runs` / `eval_results` into Iceberg.** The connector already
  captures all four tables, so the rows reach Kafka with no configuration
  change — verified: writing the golden set produced
  `cdc.metadata.golden_examples` records carrying the new `example_key` column.
  A lander and a mart over them is ADR-007 §10's "copy of `cdc_land.py` rather
  than a design", and remains so.
- **Eval scores on a Grafana panel.** Scores are rows in Postgres, not spans.
  Emitting one as a span would need a new symbol in the Avro `EventType` enum,
  which the Flink reader schema would reject; stamping it on an `AGENT_STEP`
  would label something that is not an agent step. Neither is worth doing to
  make a chart fit a datasource. `quality.json`'s placeholder panels now say
  that, instead of saying the harness does not exist.
- **A section-level hit@k.** §3 explains why: it would break the equivalence
  between the two hit@k sources.
- **Pairwise blind judging.** §5. It needs two candidate systems.

---

## 12. Verification log

*(Filled in as each measurement is taken. Free measurements first; every paid
figure is the 25-example CI subset unless it says otherwise, and the full
85-example run is deferred to a future budget top-up.)*

### 0. The bug, before and after

```
$ python -m services.rag diagnose        # QdrantStore()      -> before
dense returned nothing : 6/6
hybrid identical to bm25: 6/6
FAIL  dense returned 0 results for all 6 probes
FAIL  hybrid's top-5 was identical to bm25's for all 6 probes

$ make rag-preflight                     # default_store()    -> after
dense returned nothing : 0/6
hybrid identical to bm25: 0/6
PASS  dense is alive and hybrid fuses two real rankings
```

### 1. Retrieval A/B — free, all 82 answerable examples

`docs/eval/retrieval_ab.md`. Before the fix: `dense = 0.0000`, hybrid
byte-identical to bm25 for **82/82**. After: dense **0.9146**, hybrid
**0.8659**, bm25 **0.7073**, and hybrid identical to bm25 for **0/82**.
See §4 for what that means.

### 2. The golden set

```
$ make eval-validate
85 examples, 25 in the CI subset

  project    flink=25 (ci 9)  iceberg=32 (ci 8)  kafka=28 (ci 8)
  type       conceptual=37 (ci 8)  config=23 (ci 8)  factual=25 (ci 9)
  difficulty easy=17 (ci 8)  hard=19 (ci 8)  medium=49 (ci 9)
  answerable yes=82  no=3

OK -- every expected_source resolves against 94 corpus documents
```

Every axis is 9/8/8 in the CI subset, no topic contributes more than two
examples, and one unanswerable example is included.

### 3. Prompts and the golden set through CDC — free

```
$ make prompts-load
    updated  v1 … v4        inserted  v5
$ make prompts-load          # again
  unchanged  v1 … v5         0 inserted, 0 updated, 5 unchanged

$ make eval-load
85 inserted, 0 updated, 0 unchanged
$ make eval-load             # again
0 inserted, 0 updated, 85 unchanged
```

Both loaders are genuine no-ops on a second run — no WAL, and therefore no
spurious CDC update. Debezium carried the rows through unchanged:

```
cdc.metadata.golden_examples, offset 87:
{"after":{"id":103,"question":"How do I configure a Kafka cluster with the
 Strimzi Kubernetes operator?", …, "example_key":"kafka-unanswerable-001"},
 "op":"c","source":{"lsn":28840752, …}}
```

The `example_key` column added by migration 08 flows through with no connector
change — ADR-007's `FOR TABLE` publication working as designed, and its "no
schema gate on the CDC topics" caveat visible in the same record.

### 4. The fences — free

```
$ python -m eval run --subset ci --limit 3 --dry-run --yes
budget: $0.0000 spent, this run is estimated at $0.2250, projected $0.2250 of $4.25
pilot: 3 examples, estimated $0.2250 of real API spend.
dry run: everything checked, nothing spent.
```

Gateway reachable, dense retrieval answering, budget within cap. With the
ledger primed to $4.20:

```
$ python -m eval run --subset ci --yes --dry-run   # exit 2
REFUSED before spending anything: eval-ci is estimated at $1.8750, and $4.2000
is already spent, so it would reach $6.0750 against a $4.25 cap.
Not starting a run the arithmetic says cannot finish.
```

### 5. Tests and lint

```
$ python -m pytest -q
653 passed, 1 deselected

$ ruff check services/ tests/ stream/ scripts/ analytics/ quality/ metadata/ eval/
All checks passed!
```

Zero of those tests make an API call, open a database connection, or reach
Qdrant — asserted for the harness by a source-text contract.

### 6. Still to measure

- The pilot's measured per-example cost, and the projection it licenses.
- Two baseline runs, σ̂, and the derived thresholds → `eval/baseline.json`.
- Judge-vs-human agreement over 30 hand labels → `docs/eval/agreement.md`
  (blocked on a human labelling session).
- The gate demonstrated red on v5 and green on the revert.
- `docs/img/quality_prompt_version.png`, once v4 and v5 traffic exists.
