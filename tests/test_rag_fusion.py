import pytest

from services.rag.fusion import reciprocal_rank_fusion


def test_reciprocal_rank_fusion_hand_computed() -> None:
    """Two rankings, k=60. Expected scores worked out by hand from
    1/(k+rank), 1-indexed rank -- not derived from the implementation.
    """
    dense = [("a", 0.9), ("b", 0.8), ("c", 0.7)]
    bm25 = [("c", 5.0), ("a", 3.0), ("d", 1.0)]

    fused = reciprocal_rank_fusion([dense, bm25], k=60)
    scores = dict(fused)

    assert scores["a"] == pytest.approx(1 / 61 + 1 / 62)
    assert scores["b"] == pytest.approx(1 / 62)
    assert scores["c"] == pytest.approx(1 / 63 + 1 / 61)
    assert scores["d"] == pytest.approx(1 / 63)

    assert [item_id for item_id, _ in fused] == ["a", "c", "b", "d"]


def test_reciprocal_rank_fusion_ignores_input_scores() -> None:
    """Only rank *position* (list order) matters, never the attached score --
    that's what lets dense cosine and BM25 scores (incomparable scales)
    combine without normalization. x ranks 1st in a, 2nd in b; y ranks 2nd in
    a, 1st in b -- so despite wildly different raw scores, both end up with
    the same rank-1-plus-rank-2 sum and must tie.
    """
    ranking_a = [("x", 1000.0), ("y", 0.001)]
    ranking_b = [("y", 1000.0), ("x", 0.001)]

    fused = reciprocal_rank_fusion([ranking_a, ranking_b], k=60)
    scores = dict(fused)

    assert scores["x"] == scores["y"]


def test_reciprocal_rank_fusion_single_ranking_preserves_order() -> None:
    ranking = [("a", 1.0), ("b", 0.5), ("c", 0.1)]
    fused = reciprocal_rank_fusion([ranking], k=60)
    assert [item_id for item_id, _ in fused] == ["a", "b", "c"]


def test_reciprocal_rank_fusion_empty_rankings() -> None:
    assert reciprocal_rank_fusion([], k=60) == []
    assert reciprocal_rank_fusion([[], []], k=60) == []
