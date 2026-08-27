from pathlib import Path

import pytest

from services.rag.fetch import FetchedFile, ProjectSpec, fetch_all, load_sources


class FakeFetchStrategy:
    """Writes canned bytes to dest, no network/git -- proves fetch_all's wiring."""

    def __init__(self, content: bytes = b"canned content") -> None:
        self.content = content
        self.calls: list[Path] = []

    def __call__(self, spec: ProjectSpec, dest: Path) -> list[FetchedFile]:
        self.calls.append(dest)
        dest.mkdir(parents=True, exist_ok=True)
        local_path = dest / "doc.md"
        local_path.write_bytes(self.content)
        return [
            FetchedFile(
                source_path="doc.md",
                local_path=local_path,
                content_hash="deadbeef",
            )
        ]


def test_load_sources_parses_all_three_projects() -> None:
    specs = load_sources()
    names = {s.name for s in specs}
    assert names == {"kafka", "flink", "iceberg"}
    assert all(s.strategy for s in specs)


def test_fetch_all_calls_matching_strategy_per_spec(tmp_path: Path) -> None:
    specs = [ProjectSpec(name="fake", version="1.0", strategy="fake_strategy", config={})]
    strategy = FakeFetchStrategy()

    results = fetch_all(specs, dest_root=tmp_path, strategies={"fake_strategy": strategy})

    assert strategy.calls == [tmp_path / "fake" / "1.0"]
    assert results["fake"][0].source_path == "doc.md"
    assert results["fake"][0].content_hash == "deadbeef"


def test_fetch_all_raises_on_unknown_strategy(tmp_path: Path) -> None:
    specs = [ProjectSpec(name="fake", version="1.0", strategy="nope", config={})]
    with pytest.raises(KeyError):
        fetch_all(specs, dest_root=tmp_path, strategies={})


@pytest.mark.slow
def test_fetch_rendered_html_live(tmp_path: Path) -> None:
    """Hits the real Kafka docs URL. Skipped by default; run with `pytest -m slow`."""
    from services.rag.fetch import fetch_rendered_html

    specs = load_sources()
    kafka_spec = next(s for s in specs if s.name == "kafka")
    fetched = fetch_rendered_html(kafka_spec, tmp_path)
    assert fetched
    assert all(f.local_path.exists() for f in fetched)
