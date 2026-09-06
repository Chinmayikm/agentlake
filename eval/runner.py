"""Wiring the real objects, and the fence around the one command that spends.

`eval/harness.py` knows nothing about gateways, subprocesses or databases -- it
takes them as arguments. This module is where the real ones get built, which
makes it the only place the expensive-step fence has to live.

**The fence has four independent layers**, because any one of them can be
bypassed by accident:

1. An explicit `--yes`, or an interactive confirmation, before the first call.
2. A hard refusal if pytest is imported. Tests reach `run_eval` through its
   injected seams; the CLI path can never be reached from the suite.
3. A gateway health check -- and the harness never holds ANTHROPIC_API_KEY
   itself, so it CANNOT call a provider even if everything else failed
   (ADR-001 #1 unchanged).
4. CI never invokes `make eval`; only `make eval-ci`, from a secret-gated job.
"""

from __future__ import annotations

import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

from eval.budget import BudgetGuard, Ledger, gateway_total_cost
from eval.dataset import GoldenExample
from eval.judge import JUDGE_ALIAS, JUDGE_PROMPT_VERSION
from eval.judge import judge as judge_examples


class ExpensiveStepRefused(RuntimeError):
    """A paid run that was not authorised, or cannot safely start."""


def assert_not_under_pytest() -> None:
    """A paid run started from a test is a bug, not a use case.

    `eval/harness.py` is fully injectable, so the suite exercises everything
    through fakes; nothing in it needs this CLI path. Refusing outright is
    cheaper than trusting that no future test ever calls main().
    """
    if "pytest" in sys.modules:
        raise ExpensiveStepRefused(
            "refusing to start a paid eval run under pytest. The harness is "
            "injectable -- test it through run_eval(sink=ListSink(), ...) instead."
        )


def confirm(label: str, n: int, estimate_usd: float, *, yes: bool) -> None:
    print(f"\n{label}: {n} examples, estimated ${estimate_usd:.4f} of real API spend.")
    if yes:
        return
    if not sys.stdin.isatty():
        raise ExpensiveStepRefused(
            "refusing to spend money without confirmation. Pass --yes (or set "
            "AGENTLAKE_EVAL_CONFIRM=1) if you mean it."
        )
    if input("proceed? [y/N] ").strip().lower() not in {"y", "yes"}:
        raise ExpensiveStepRefused("cancelled at the confirmation prompt.")


def require_gateway() -> None:
    """The gateway must be up before anything is scheduled.

    Discovering it is down on example 1 wastes nothing, but discovering it on
    example 12 of a paid run wastes eleven examples' worth of wall clock and
    leaves a half-run in the database.
    """
    import os

    import httpx

    from services.agent.gateway_client import DEFAULT_GATEWAY_URL

    url = os.environ.get("AGENTLAKE_GATEWAY", DEFAULT_GATEWAY_URL).rstrip("/")
    try:
        response = httpx.get(f"{url}/v1/health", timeout=5.0)
        response.raise_for_status()
    except Exception as exc:
        raise ExpensiveStepRefused(
            f"the inference gateway is not reachable at {url} ({exc}). "
            f"Start it with `make gateway` -- the harness never holds "
            f"ANTHROPIC_API_KEY itself (ADR-001 #1)."
        ) from None


def require_retrieval() -> None:
    """Dense retrieval must actually be returning rows.

    The permanent guard for ADR-008 #1. Without it a corpus_version mismatch
    would produce a full run of legitimate-looking zeros, and the hit rate
    would be read as a retrieval regression rather than as a broken store.
    """
    from services.rag.bm25 import BM25Index
    from services.rag.embed import FastEmbedEmbedder
    from services.rag.qdrant_store import default_store
    from services.rag.retrieve import retrieve

    store = default_store()
    hits = retrieve(
        "kafka exactly once",
        1,
        mode="dense",
        store=store,
        embedder=FastEmbedEmbedder(),
        bm25_index=BM25Index.load(),
    )
    if not hits:
        raise ExpensiveStepRefused(
            f"dense retrieval returned nothing for corpus_version "
            f"{store.corpus_version!r}. Refusing to spend money measuring a "
            f"retriever that is not answering -- see `make rag-preflight` and "
            f"ADR-008 #1."
        )


@dataclass(slots=True)
class GatewayChat:
    """A `ChatFn` for the judge: one gateway call, no tools, a system prompt.

    Judge calls go through the same door as everything else (ADR-001 #1), so
    judging is traced and costed, and `X-Prompt-Version: judge-v1` keeps judge
    spend separable from agent spend on the ADR-007 panels.
    """

    gateway: Any
    session_id: str

    async def __call__(self, messages: list[dict[str, Any]], *, system: str) -> Any:
        from services.gateway.chat import ChatRequest

        return await self.gateway.chat(
            ChatRequest(messages=messages, model_alias=JUDGE_ALIAS, system=system),
            session_id=self.session_id,
            prompt_version=JUDGE_PROMPT_VERSION,
        )


def make_judge_fn(gateway: Any, session_id: str):
    chat = GatewayChat(gateway=gateway, session_id=session_id)

    async def judge_fn(**kwargs):
        return await judge_examples(chat, **kwargs)

    return judge_fn


async def execute(
    examples: list[GoldenExample],
    *,
    label: str,
    subset: str,
    prompt_version: str,
    model_alias: str,
    max_steps: int,
    concurrency: int,
    seed: int,
    k: int,
    estimate_per_example: float,
    with_judge: bool,
    yes: bool,
    cap_usd: float,
    notes: str = "",
    dry_run: bool = False,
) -> Any:
    """Build everything real, run, and record. Returns the RunSummary."""
    from eval.dataset import DATASET_VERSION
    from eval.db import PostgresSink
    from eval.harness import DEFAULT_TOOL_TIMEOUT, run_eval
    from services.agent.gateway_client import HttpGatewayClient
    from services.agent.loop import run_turn
    from services.agent.mcp_client import StdioToolExecutor
    from services.gateway.pricing import load_price_table
    from services.mcp_server.schemas import SEARCH_DOCS_SCHEMA
    from services.rag.fetch import load_corpus_version

    assert_not_under_pytest()

    estimate = estimate_per_example * len(examples)
    ledger = Ledger.load()
    guard = BudgetGuard(ledger=ledger, label=label, estimate_usd=estimate, cap_usd=cap_usd)
    guard.preflight()
    confirm(label, len(examples), estimate, yes=yes)

    require_gateway()
    require_retrieval()

    if dry_run:
        print("dry run: everything checked, nothing spent.")
        return None

    # Read k/mode out of the tool schema the model is actually offered, so the
    # recorded config cannot drift from what was on the table. The model still
    # chooses per call -- the run summary prints the observed distribution,
    # which is the honest answer to "what was the retriever configured with".
    properties = SEARCH_DOCS_SCHEMA["properties"]
    retriever_config = {
        "k": properties["k"].get("default"),
        "mode": properties["mode"].get("default"),
        "index": "docs-v1",
        "store": "qdrant",
        "embedder": "fastembed/BAAI/bge-small-en-v1.5",
        "note": "tool-schema defaults; the model may override k/mode per call",
    }

    sink = PostgresSink(DATASET_VERSION)
    gateway = HttpGatewayClient()
    price_table = load_price_table()

    try:
        async with AsyncExitStack() as stack:
            executors = [
                await stack.enter_async_context(StdioToolExecutor(f"eval-w{i}"))
                for i in range(max(1, concurrency))
            ]
            summary = await run_eval(
                examples,
                run_turn=run_turn,
                gateway=gateway,
                tool_executors=executors,
                sink=sink,
                judge_fn=make_judge_fn(gateway, "eval-judge") if with_judge else None,
                prompt_version=prompt_version,
                corpus_version=load_corpus_version(),
                dataset_version=DATASET_VERSION,
                subset=subset,
                prompt_version_id=sink.prompt_version_id(prompt_version),
                judge_model=price_table.get(JUDGE_ALIAS).provider_model_id,
                retriever_config=retriever_config,
                model_alias=model_alias,
                max_steps=max_steps,
                tool_timeout=DEFAULT_TOOL_TIMEOUT,
                k=k,
                seed=seed,
                budget=guard,
                poll_cost=gateway_total_cost,
                notes=notes,
            )
    finally:
        await gateway.aclose()
        guard.commit(note=f"{len(examples)} examples, subset={subset}")
        sink.close()

    return summary
