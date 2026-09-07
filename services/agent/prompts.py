"""Prompt templates, loaded from `services/agent/prompts/<version>.md`.

**The repo is the source of truth; Postgres is the published dimension.**
`scripts/load_prompts.py` pushes these files into `prompt_versions`, never the
other way round, and `services/agent` never reads the metadata database --
ADR-007 #6's rule that an agent refusing to run because a metadata row was
missing would make the telemetry a dependency of the thing it observes.

Until ADR-008, `prompt_version` was a pure telemetry LABEL: no `system` prompt
was sent at all, and `prompt_versions.template_text` was read by nothing, so
two "different prompts" produced byte-identical requests. The files here are
what makes the label describe something.
"""

from __future__ import annotations

from functools import cache, lru_cache
from pathlib import Path

PROMPTS_DIR = Path(__file__).parent / "prompts"
PARAMS_PATH = PROMPTS_DIR / "params.yaml"
_SUFFIX = ".md"


class UnknownPromptVersion(ValueError):
    """Raised for a version with no file, naming the ones that do exist."""


def available_versions() -> list[str]:
    return sorted(p.stem for p in PROMPTS_DIR.glob(f"*{_SUFFIX}"))


@cache
def load_prompt(version: str) -> str:
    """The system prompt text for `version`.

    Cached, because the agent loop reads it once per gateway call and a prompt
    file does not change inside a process. Trailing whitespace is stripped so
    an editor adding a final newline cannot change the bytes sent to the
    provider -- and therefore cannot change a measured eval score.
    """
    path = PROMPTS_DIR / f"{version}{_SUFFIX}"
    if not path.is_file():
        raise UnknownPromptVersion(
            f"no prompt file for version {version!r}; available: {available_versions()}"
        )
    return path.read_text(encoding="utf-8").strip()


@lru_cache(maxsize=1)
def load_params() -> dict[str, dict[str, object]]:
    """Descriptive metadata per version, from the params.yaml sidecar.

    A sidecar and not front-matter, so `load_prompt()` returns exactly the
    bytes on disk: an eval score is only reproducible if "what was the prompt"
    has a byte-exact answer, and a file that has to be parsed before it is sent
    does not have one.

    Only scripts/load_prompts.py reads this -- nothing at runtime depends on it,
    so a missing entry is an empty dict rather than an error.
    """
    import yaml

    if not PARAMS_PATH.is_file():
        return {}
    return yaml.safe_load(PARAMS_PATH.read_text(encoding="utf-8")) or {}
