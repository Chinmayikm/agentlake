from pathlib import Path

from services.rag.chunk import chunk_html, chunk_markdown, doc_id_for

FIXTURES = Path(__file__).parent / "fixtures" / "rag"


def test_chunk_markdown_tracks_heading_path() -> None:
    text = (FIXTURES / "sample_flink.md").read_text()
    chunks = chunk_markdown(text, project="flink", version="1.20", source_path="sample_flink.md")

    sections = {c.section for c in chunks}
    assert "Flink Documentation" in sections
    assert "Flink Documentation > Checkpointing > Checkpoint Storage" in sections
    assert "Flink Documentation > Watermarks" in sections


def test_chunk_html_tracks_heading_path() -> None:
    html = (FIXTURES / "sample_kafka.html").read_text()
    chunks = chunk_html(html, project="kafka", version="3.8", source_path="sample_kafka.html")

    sections = {c.section for c in chunks}
    assert "Kafka Documentation" in sections
    assert "Kafka Documentation > Log Compaction" in sections
    assert "Kafka Documentation > Broker Configs > log.retention.hours" in sections


def test_chunk_respects_max_chars_with_overlap() -> None:
    text = "# Heading\n\n" + ("word " * 500)
    chunks = chunk_markdown(
        text, project="kafka", version="3.8", source_path="long.md", max_chars=200, overlap_chars=20
    )
    assert len(chunks) > 1
    assert all(len(c.text) <= 200 for c in chunks)


def test_chunk_ids_are_deterministic() -> None:
    text = (FIXTURES / "sample_iceberg.md").read_text()
    first = chunk_markdown(text, project="iceberg", version="1.7", source_path="sample_iceberg.md")
    second = chunk_markdown(text, project="iceberg", version="1.7", source_path="sample_iceberg.md")

    assert [c.chunk_id for c in first] == [c.chunk_id for c in second]
    assert all(c.doc_id == doc_id_for("iceberg", "1.7", "sample_iceberg.md") for c in first)


def test_chunk_ids_differ_across_source_paths() -> None:
    text = "# Same heading\n\nsame body text"
    a = chunk_markdown(text, project="kafka", version="3.8", source_path="a.md")
    b = chunk_markdown(text, project="kafka", version="3.8", source_path="b.md")
    assert a[0].chunk_id != b[0].chunk_id
