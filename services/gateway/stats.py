"""Process-lifetime counters for GET /v1/stats.

No lock: asyncio is single-threaded cooperative scheduling, and nothing here
awaits between a counter's read and write, so plain int/float increments are
safe. Reset on process restart -- these are not persisted telemetry, which is
what services.sdk / Kafka is for.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ModelStats:
    requests: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0


@dataclass
class GatewayStats:
    requests: int = 0
    errors: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_cost_usd: float = 0.0
    by_model: dict[str, ModelStats] = field(default_factory=dict)

    def record_success(
        self, alias: str, prompt_tokens: int, completion_tokens: int, cost: float
    ) -> None:
        self.requests += 1
        self.total_prompt_tokens += prompt_tokens
        self.total_completion_tokens += completion_tokens
        self.total_cost_usd += cost

        model_stats = self.by_model.setdefault(alias, ModelStats())
        model_stats.requests += 1
        model_stats.prompt_tokens += prompt_tokens
        model_stats.completion_tokens += completion_tokens
        model_stats.cost_usd += cost

    def record_error(self, alias: str) -> None:
        self.requests += 1
        self.errors += 1
        self.by_model.setdefault(alias, ModelStats()).requests += 1

    def to_dict(self) -> dict:
        return {
            "requests": self.requests,
            "errors": self.errors,
            "total_prompt_tokens": self.total_prompt_tokens,
            "total_completion_tokens": self.total_completion_tokens,
            "total_cost_usd": self.total_cost_usd,
            "by_model": {
                alias: {
                    "requests": s.requests,
                    "prompt_tokens": s.prompt_tokens,
                    "completion_tokens": s.completion_tokens,
                    "cost_usd": s.cost_usd,
                }
                for alias, s in self.by_model.items()
            },
        }
