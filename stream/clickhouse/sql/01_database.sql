-- The hot path's database. Everything else in this directory lives here.
--
-- Named for the project rather than for a layer (raw/curated, the way the
-- Iceberg side is split) because ClickHouse holds exactly one layer: recent
-- spans. The layering vocabulary belongs to the lake.
CREATE DATABASE IF NOT EXISTS agentlake
