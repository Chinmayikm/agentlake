"""The spend ledger and the hard stop.

Every paid target prints an estimate before it starts, refuses to start if the
estimate would breach the cap, checks after every example, and appends what it
actually cost. The point is that "how much has this cost so far" is a question
with an answer, not a feeling.

**Cost comes from the gateway, not from an estimate.** `GET /v1/stats` is
computed by `services/gateway/pricing.py` from `models.yaml` and the provider's
own token counts (ADR-001 #2), so it is the only figure in this repo entitled
to be called the cost. The delta across a run is what that run cost.

**The ledger persists it, because /v1/stats does not.** Gateway stats are
process-lifetime and reset on restart, so a cap enforced against them alone
would silently reset every time the gateway was restarted -- which, during a
development session, is often.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

LEDGER_PATH = Path(__file__).parent / ".spend.json"

#: Hard stop. A run crossing this aborts mid-flight, flushes what it completed,
#: and prints the ledger.
DEFAULT_CAP_USD = 4.25

#: What the owner authorised in total. The gap between this and the cap is
#: deliberate headroom: an abort at the cap must still leave room for the
#: in-flight example to finish being recorded.
AUTHORISED_USD = 4.69


class BudgetExceeded(RuntimeError):
    """Raised when a run would cross, or has crossed, the cap."""


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    label: str
    cost_usd: float
    at: str
    note: str = ""


@dataclass(slots=True)
class Ledger:
    entries: list[LedgerEntry] = field(default_factory=list)
    path: Path = LEDGER_PATH

    @property
    def spent(self) -> float:
        return sum(e.cost_usd for e in self.entries)

    @classmethod
    def load(cls, path: Path = LEDGER_PATH) -> Ledger:
        if not path.is_file():
            return cls(path=path)
        raw = json.loads(path.read_text(encoding="utf-8"))
        return cls(entries=[LedgerEntry(**e) for e in raw.get("entries", [])], path=path)

    def save(self) -> None:
        self.path.write_text(
            json.dumps({"entries": [asdict(e) for e in self.entries]}, indent=2) + "\n",
            encoding="utf-8",
        )

    def record(self, label: str, cost_usd: float, note: str = "") -> None:
        self.entries.append(
            LedgerEntry(
                label=label,
                cost_usd=round(cost_usd, 6),
                at=time.strftime("%Y-%m-%dT%H:%M:%S"),
                note=note,
            )
        )
        self.save()

    def render(self, cap: float = DEFAULT_CAP_USD) -> str:
        lines = ["", "SPEND LEDGER", "-" * 58]
        for entry in self.entries:
            note = f"  ({entry.note})" if entry.note else ""
            lines.append(f"  {entry.at}  ${entry.cost_usd:7.4f}  {entry.label}{note}")
        if not self.entries:
            lines.append("  (nothing spent yet)")
        lines += [
            "-" * 58,
            f"  spent      ${self.spent:7.4f}",
            f"  cap        ${cap:7.4f}   (abort)",
            f"  authorised ${AUTHORISED_USD:7.4f}",
            f"  remaining  ${cap - self.spent:7.4f}",
            "",
        ]
        return "\n".join(lines)


@dataclass(slots=True)
class BudgetGuard:
    """Pre-flight refusal, in-run abort, post-run record.

    `estimate_usd` is the caller's projection for the whole run. It is used ONLY
    to refuse before spending anything -- every number that is recorded comes
    from the gateway.
    """

    ledger: Ledger
    label: str
    estimate_usd: float
    cap_usd: float = DEFAULT_CAP_USD
    _baseline: float | None = None
    _latest: float = 0.0

    def preflight(self) -> None:
        projected = self.ledger.spent + self.estimate_usd
        print(
            f"budget: ${self.ledger.spent:.4f} spent, this run is estimated at "
            f"${self.estimate_usd:.4f}, projected ${projected:.4f} of ${self.cap_usd:.2f}"
        )
        if projected > self.cap_usd:
            raise BudgetExceeded(
                f"REFUSED before spending anything: {self.label} is estimated at "
                f"${self.estimate_usd:.4f}, and ${self.ledger.spent:.4f} is already spent, "
                f"so it would reach ${projected:.4f} against a ${self.cap_usd:.2f} cap.\n"
                f"Not starting a run the arithmetic says cannot finish."
                + self.ledger.render(self.cap_usd)
            )

    def observe(self, gateway_total_usd: float) -> float:
        """Feed the gateway's lifetime total in; get this run's cost back.

        The FIRST observation sets the baseline rather than counting as spend,
        because the gateway process may already have served other traffic --
        `make gateway` is usually running before the harness starts.
        """
        if self._baseline is None:
            self._baseline = gateway_total_usd
        self._latest = max(0.0, gateway_total_usd - self._baseline)
        return self._latest

    def check(self) -> None:
        if self.ledger.spent + self._latest > self.cap_usd:
            raise BudgetExceeded(
                f"ABORTED mid-run: ${self.ledger.spent + self._latest:.4f} spent against a "
                f"${self.cap_usd:.2f} cap. Completed examples are already committed."
                + self.ledger.render(self.cap_usd)
            )

    @property
    def run_cost(self) -> float:
        return self._latest

    def commit(self, note: str = "") -> None:
        self.ledger.record(self.label, self._latest, note)
        delta = self._latest - self.estimate_usd
        print(
            f"budget: {self.label} cost ${self._latest:.4f} "
            f"(estimated ${self.estimate_usd:.4f}, {delta:+.4f})"
        )
        print(self.ledger.render(self.cap_usd))


def gateway_total_cost(base_url: str | None = None) -> float:
    """The gateway's lifetime total_cost_usd.

    Returns 0.0 if the gateway is unreachable rather than raising: the caller
    is usually mid-run and losing the run to a stats hiccup would be worse than
    a momentarily stale ledger. The pre-flight check, which runs when nothing
    has been spent, is where an unreachable gateway is fatal.
    """
    import httpx

    url = (base_url or os.environ.get("AGENTLAKE_GATEWAY", "http://localhost:8100")).rstrip("/")
    try:
        response = httpx.get(f"{url}/v1/stats", timeout=5.0)
        response.raise_for_status()
        return float(response.json().get("total_cost_usd", 0.0))
    except Exception:
        return 0.0
