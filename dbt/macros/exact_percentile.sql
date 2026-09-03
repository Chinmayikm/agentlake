{#
    Exact nearest-rank percentile, computed from every individual value.

    This macro is the point of the whole batch layer. ADR-004 #6 recorded that
    Flink 1.20's SQL dialect has no percentile aggregate at all, so
    lake.curated.agg_model_5m carries sums and a max, and percentiles were
    deferred "downstream from lake.raw.trace_events, which holds every
    individual latency_ms". ADR-005 #4 discharged that for the hot path with
    ClickHouse's quantile(), which is APPROXIMATE -- it samples. This is the
    other half, and it is exact.

    Deliberately NOT approx_percentile. Trino has one, it is cheaper, and it is
    the right choice at scale -- but exactness is the reason a mart reads from
    the raw table instead of from the 5-minute aggregate. Using an approximation
    here would make lake.raw.trace_events pointless for this question and would
    leave the repo with two approximate p95s and no exact one to check them
    against. scripts/analytics_verify.py crosscheck compares this number against
    ClickHouse's, and the comparison only means something because this side is
    exact.

    Where that stops being true: array_agg materialises every value of the group
    in the coordinator's heap. At the current grain -- one model or one tool per
    day, order 10K doubles, ~80 KB -- that is nothing against an 800 MB heap. At
    a million values per group it is still fine; at a hundred million it is not,
    and the answer there is approx_percentile with the loss of exactness stated.

    Nearest rank, 1-based: index = ceil(q * n), which is the same definition
    ClickHouse's quantileExact uses, so the two are comparable by construction.
    array_agg includes NULLs in Trino, hence the FILTER; count(column) already
    excludes them, so the two agree on n.
#}
{% macro exact_percentile(column, quantile) -%}
case
    when count({{ column }}) = 0 then null
    else element_at(
        array_sort(array_agg({{ column }}) filter (where {{ column }} is not null)),
        greatest(1, cast(ceil({{ quantile }} * count({{ column }})) as integer))
    )
end
{%- endmacro %}
