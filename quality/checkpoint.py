"""The Great Expectations gate over the marts.

    make quality

Run inside the toolbox container (``docker compose run --rm dbt python
/quality/checkpoint.py``), because great-expectations 1.22.0 declares
Requires-Python ``>=3.10,<3.14`` and this repo runs 3.14 -- see ADR-006 #2.

Exit code is the whole point: **0 if every blocking expectation held, 1
otherwise**, so this is usable as a merge gate rather than a report someone
reads. Warn-severity expectations are evaluated, printed and never affect the
exit code.

What is blocking and what is a warning
--------------------------------------
The rule is one sentence, and it is the same one ``severity: warn`` follows in
the dbt schema files:

    BLOCKING  = an invariant that can only be false if the pipeline is broken.
    WARN      = a property that depends on when traffic last ran, or on data
                shape that is legitimately variable.

So "fct_sessions has rows", "no null session_id", "cost is not negative",
"span_count >= turn_count" all block -- none of them can go false while the
pipeline is healthy. Freshness does not block: this is a laptop lakehouse whose
source table only advances when someone runs `make traffic` with the Flink jobs
up, so a stale mart is the normal state on a Tuesday and gating on it would
train everyone to ignore the gate. The null rate of `model` does not block
either, because model is NULL for every non-LLM span by contract (ADR-004's
verification log calls that group real, not a defect).

Why these checks and not the dbt tests
--------------------------------------
The split is deliberate and is forced as much as chosen -- see ADR-006 #6:

    GE  owns single-table contracts on the finished marts: row counts,
        null rates, value bounds, uniqueness, freshness, within-row
        comparisons.
    dbt owns everything relational: `relationships` between models,
        reconciliation of a mart against its own staging source, the
        streaming-vs-batch aggregate comparison.

That is not a philosophical preference. GE's Trino backend supports **table
assets only** -- there is no query asset -- so a GE expectation cannot express a
join, and dbt tests are ordinary SELECTs that can. Each tool got the half it can
actually do.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from dataclasses import dataclass
from typing import Any

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context.types.base import ProgressBarsConfig

TRINO_HOST = os.environ.get("AGENTLAKE_TRINO_HOST", "trino")
TRINO_PORT = os.environ.get("AGENTLAKE_TRINO_PORT", "8080")
CATALOG = "lake"
SCHEMA = "analytics"

CONNECTION_STRING = f"trino://agentlake@{TRINO_HOST}:{TRINO_PORT}/{CATALOG}"

#: Marks an expectation as non-blocking. GE carries `meta` through to the result
#: unchanged, which is what lets severity be read back off the result rather
#: than tracked in a parallel structure that could disagree with it.
WARN = {"severity": "warn"}
BLOCK = {"severity": "blocking"}

#: How stale a mart may be before the freshness check complains. Generous on
#: purpose: it is a warning, and the question it answers is "has anyone run
#: traffic this week", not "is the pipeline down".
FRESHNESS_HOURS = float(os.environ.get("AGENTLAKE_FRESHNESS_HOURS", "168"))

#: The marts, and every expectation over them. build_suites() asserts that it
#: produces exactly these, in this order -- without that this constant is a
#: comment that can silently disagree with the code below it.
MARTS = (
    "fct_sessions",
    "fct_model_costs",
    "fct_tool_reliability",
    "fct_cost_by_prompt",
)


@dataclass(frozen=True)
class MartSuite:
    """One mart and its expectations."""

    table: str
    expectations: list[Any]


def freshness_floor() -> dt.datetime:
    """The oldest ``last_span_ts`` the freshness warning tolerates."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None) - dt.timedelta(hours=FRESHNESS_HOURS)


def build_suites() -> list[MartSuite]:
    """Every expectation, in one place, so the gate can be read as a list."""
    suites = [
        MartSuite(
            table="fct_sessions",
            expectations=[
                # --- blocking ---
                gxe.ExpectTableRowCountToBeBetween(min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToNotBeNull(column="session_id", meta=BLOCK),
                gxe.ExpectColumnValuesToBeUnique(column="session_id", meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(
                    column="total_cost_usd", min_value=0, meta=BLOCK
                ),
                gxe.ExpectColumnValuesToBeBetween(column="total_tokens", min_value=0, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="turn_count", min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="duration_ms", min_value=0, meta=BLOCK),
                # Referential sanity WITHIN the row -- the cross-table kind is a
                # dbt `relationships` test, because a GE table asset cannot join.
                gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="span_count", column_B="turn_count", or_equal=True, meta=BLOCK
                ),
                # --- warn ---
                gxe.ExpectColumnMaxToBeBetween(
                    column="last_span_ts", min_value=freshness_floor(), meta=WARN
                ),
            ],
        ),
        MartSuite(
            table="fct_model_costs",
            expectations=[
                # --- blocking ---
                gxe.ExpectTableRowCountToBeBetween(min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToNotBeNull(column="event_day", meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="cost_usd", min_value=0, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="calls", min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="total_tokens", min_value=0, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(
                    column="latency_p95_ms", min_value=0, max_value=600_000, meta=BLOCK
                ),
                # p95 is a member of the sample, so the max cannot be below it.
                # This is the cheapest available check that the percentile macro
                # indexes the sorted array correctly.
                gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="latency_max_ms",
                    column_B="latency_p95_ms",
                    or_equal=True,
                    meta=BLOCK,
                ),
                # --- warn ---
                # model is NULL for every non-LLM span by contract. This mart
                # filters to LLM_CALL so it should be populated, but a null here
                # is a data-shape question, not a broken pipeline.
                gxe.ExpectColumnValuesToNotBeNull(column="model", mostly=0.99, meta=WARN),
            ],
        ),
        MartSuite(
            table="fct_tool_reliability",
            expectations=[
                # --- blocking ---
                gxe.ExpectTableRowCountToBeBetween(min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToNotBeNull(column="tool_name", meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(
                    column="error_rate", min_value=0, max_value=1, meta=BLOCK
                ),
                gxe.ExpectColumnValuesToBeBetween(
                    column="timeout_rate", min_value=0, max_value=1, meta=BLOCK
                ),
                gxe.ExpectColumnValuesToBeBetween(column="calls", min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(
                    column="latency_p95_ms", min_value=0, max_value=600_000, meta=BLOCK
                ),
                # Every timeout is also an error: the SDK's except-block forces
                # status='error' before it records error_class.
                gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="error_count", column_B="timeout_count", or_equal=True, meta=BLOCK
                ),
            ],
        ),
        MartSuite(
            table="fct_cost_by_prompt",
            expectations=[
                # --- blocking ---
                gxe.ExpectTableRowCountToBeBetween(min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToNotBeNull(column="event_day", meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="calls", min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="turns", min_value=1, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="cost_usd", min_value=0, meta=BLOCK),
                gxe.ExpectColumnValuesToBeBetween(column="total_tokens", min_value=0, meta=BLOCK),
                # A CASE with exactly three arms produced this column, so a
                # fourth value is the model having changed, not the data.
                gxe.ExpectColumnValuesToBeInSet(
                    column="prompt_attribution",
                    value_set=["known", "unversioned", "unknown"],
                    meta=BLOCK,
                ),
                # turns is count(distinct trace_id) over the very rows calls
                # counts, so this is arithmetic rather than a hope.
                gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="calls", column_B="turns", or_equal=True, meta=BLOCK
                ),
                # --- warn ---
                # A span naming a prompt version the dimension does not hold is
                # news, not a broken build: it is the normal state between a row
                # being created in Postgres and `make cdc-land` next running.
                # "Depends on when it last ran" is ADR-006 #6's WARN half
                # verbatim -- the same argument freshness gets. NOT a threshold
                # on how many rows are 'unversioned', either: that share only
                # falls as new traffic arrives after the gateway change, so
                # gating on it would measure how recently someone ran traffic.
                gxe.ExpectColumnValuesToNotBeInSet(
                    column="prompt_attribution", value_set=["unknown"], meta=WARN
                ),
            ],
        ),
    ]
    # MARTS is a documentation constant and this is what stops it becoming a
    # stale one: a mart added below and not added there (or vice versa) fails
    # here, at the top of the gate, rather than being noticed by nobody.
    covered = tuple(suite.table for suite in suites)
    if covered != MARTS:
        raise RuntimeError(f"build_suites() covers {covered}, but MARTS says {MARTS}")
    return suites


def _severity(config: dict[str, Any]) -> str:
    return str((config.get("meta") or {}).get("severity", "blocking"))


def _label(config: dict[str, Any]) -> str:
    kind = str(config.get("type", config.get("expectation_type", "?")))
    kwargs = config.get("kwargs") or {}
    column = kwargs.get("column") or kwargs.get("column_A")
    return f"{kind}({column})" if column else kind


def main() -> int:
    context = gx.get_context(mode="ephemeral")

    # GE draws a tqdm bar per metric batch. Redirected to a file -- which is
    # what CI and `make quality | tee` both do -- each refresh becomes its own
    # carriage-return-laden line and 62 metrics bury the result table in a few
    # thousand characters of noise. TQDM_DISABLE does not reach these; GE
    # constructs them from its own config, so this is the switch that works.
    context.variables.progress_bars = ProgressBarsConfig(
        globally=False, metric_calculations=False
    )

    datasource = context.data_sources.add_sql(
        name="lake_analytics", connection_string=CONNECTION_STRING
    )

    validation_definitions = []
    for mart in build_suites():
        asset = datasource.add_table_asset(
            name=mart.table, table_name=mart.table, schema_name=SCHEMA
        )
        batch_definition = asset.add_batch_definition_whole_table(f"{mart.table}_all")

        suite = context.suites.add(gx.ExpectationSuite(name=mart.table))
        for expectation in mart.expectations:
            suite.add_expectation(expectation)

        validation_definitions.append(
            context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"validate_{mart.table}", data=batch_definition, suite=suite
                )
            )
        )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="agentlake_marts", validation_definitions=validation_definitions)
    )

    print(f"great expectations {gx.__version__} -> {CONNECTION_STRING} schema={SCHEMA}")
    print(
        f"freshness floor {freshness_floor():%Y-%m-%d %H:%M:%S} "
        f"({FRESHNESS_HOURS:.0f}h, warn only)\n"
    )

    result = checkpoint.run()

    blocking_failures: list[str] = []
    warn_failures: list[str] = []
    checks = 0

    for validation in result.run_results.values():
        print(f"--- {SCHEMA}.{validation.suite_name} ---")
        for outcome in validation.results:
            config = outcome.expectation_config.to_json_dict()
            severity = _severity(config)
            label = _label(config)
            checks += 1
            if outcome.success:
                mark = "ok  "
            elif severity == "warn":
                mark = "WARN"
                warn_failures.append(f"{validation.suite_name}.{label}")
            else:
                mark = "FAIL"
                blocking_failures.append(f"{validation.suite_name}.{label}")
            print(f"  {mark}  [{severity:8}] {label}")
        print()

    passed = checks - len(blocking_failures) - len(warn_failures)
    print(
        f"{checks} expectations: {passed} ok, "
        f"{len(blocking_failures)} blocking failures, {len(warn_failures)} warnings"
    )
    for failure in warn_failures:
        print(f"  WARN  {failure}")
    for failure in blocking_failures:
        print(f"  FAIL  {failure}")

    if blocking_failures:
        print("\nFAIL -- a blocking expectation did not hold.")
        return 1
    print("\nPASS -- every blocking expectation held.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
