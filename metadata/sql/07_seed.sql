-- Seed rows, so the connector's initial snapshot has something to carry and so
-- the join in lake.analytics.fct_cost_by_prompt has a dimension on day one.
--
-- Three genuinely different prompts, not three copies with a bumped version:
-- the point of versioning a prompt is that the versions behave differently, and
-- a seed where they do not makes every downstream comparison meaningless. v3 is
-- what services/agent's DEFAULT_PROMPT_VERSION emits.
--
-- ON CONFLICT DO NOTHING against the unique index on `version`, so re-applying
-- is a no-op -- which matters because metadata-init runs on every bring-up, and
-- because an INSERT that fired twice would show up in the changelog as two
-- creates for one logical row.

INSERT INTO prompt_versions (name, version, template_text, params_json) VALUES
(
    'agent-system',
    'v1',
    'You are a helpful assistant. Answer the user''s question.',
    '{"style": "terse", "cite_sources": false}'::jsonb
),
(
    'agent-system',
    'v2',
    'You are a helpful assistant with access to tools. Use search_docs before '
    'answering questions about Kafka, Flink or Iceberg. Answer the user''s question.',
    '{"style": "terse", "cite_sources": false, "tool_hint": true}'::jsonb
),
(
    'agent-system',
    'v3',
    'You are agentlake''s documentation assistant. You have tools: search_docs for '
    'the pinned Kafka/Flink/Iceberg corpus, get_trace and query_metrics for this '
    'platform''s own telemetry. Search before answering anything factual. Cite the '
    'source of every claim you make, and say plainly when the corpus does not '
    'answer the question rather than filling the gap.',
    '{"style": "cited", "cite_sources": true, "tool_hint": true, "refuse_unsourced": true}'::jsonb
)
ON CONFLICT (version) DO NOTHING;

-- Placeholders. The eval harness (roadmap) writes the real set; these exist so
-- the table is not empty when the connector snapshots it, and so the shape of
-- expected_sources_json and tags is visible in the topic from the first record.
INSERT INTO golden_examples
    (question, expected_answer, expected_sources_json, tags, dataset_version)
VALUES
(
    'What does Kafka''s exactly-once semantics actually guarantee?',
    NULL,
    '[{"source": "kafka-docs", "anchor": "exactly-once"}]'::jsonb,
    ARRAY['kafka', 'semantics'],
    'seed-v0'
),
(
    'Why can Flink SQL not declare an Iceberg hidden partition?',
    NULL,
    '[{"source": "iceberg-docs", "anchor": "flink-ddl"}]'::jsonb,
    ARRAY['flink', 'iceberg', 'partitioning'],
    'seed-v0'
),
(
    'What is a watermark, and what does source idleness change about it?',
    NULL,
    '[{"source": "flink-docs", "anchor": "watermarks"}]'::jsonb,
    ARRAY['flink', 'watermarks'],
    'seed-v0'
)
ON CONFLICT (question, dataset_version) DO NOTHING;
