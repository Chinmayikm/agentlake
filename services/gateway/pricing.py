"""Model aliases and cost math, loaded from services/gateway/models.yaml.

cost_usd is computed from this table only -- see ADR-001. The table's version
string is stamped into every LLM_CALL span's attributes, so a cost figure is
always traceable to the price table that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_MODELS_PATH = Path(__file__).parent / "models.yaml"


@dataclass(frozen=True, slots=True)
class ModelConfig:
    alias: str
    provider_model_id: str
    input_price_per_mtok: float
    output_price_per_mtok: float


@dataclass(frozen=True, slots=True)
class PriceTable:
    version: str
    models: dict[str, ModelConfig]  # keyed by alias

    def get(self, alias: str) -> ModelConfig | None:
        return self.models.get(alias)


def load_price_table(path: str | Path = DEFAULT_MODELS_PATH) -> PriceTable:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    models = {
        alias: ModelConfig(
            alias=alias,
            provider_model_id=cfg["provider_model_id"],
            input_price_per_mtok=float(cfg["input_price_per_mtok"]),
            output_price_per_mtok=float(cfg["output_price_per_mtok"]),
        )
        for alias, cfg in raw["aliases"].items()
    }
    return PriceTable(version=raw["version"], models=models)


def cost_usd(model: ModelConfig, prompt_tokens: int, completion_tokens: int) -> float:
    return (
        prompt_tokens * model.input_price_per_mtok
        + completion_tokens * model.output_price_per_mtok
    ) / 1_000_000
