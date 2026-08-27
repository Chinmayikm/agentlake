"""Reciprocal rank fusion -- merges independently-ranked hit lists into one.

RRF(d) = sum over rankings containing d of 1 / (k + rank(d)), 1-indexed rank.
A higher-ranked hit in any input ranking contributes more; a hit missing from
a ranking simply contributes 0 from it. k=60 is the default from the original
RRF paper (Cormack et al., 2009) -- large enough that a single rank-1 hit
doesn't dominate a hit that ranks well across both lists.
"""

from __future__ import annotations

DEFAULT_RRF_K = 60


def reciprocal_rank_fusion(
    rankings: list[list[tuple[str, float]]], k: int = DEFAULT_RRF_K
) -> list[tuple[str, float]]:
    """Each ranking is an ordered [(id, score), ...] list, best-first.

    Input scores are ignored -- only rank position matters, which is the
    point of RRF: it fuses lists whose scores live on incomparable scales
    (cosine similarity vs. BM25) without needing to normalize either.
    """
    fused: dict[str, float] = {}
    for ranking in rankings:
        for rank, (item_id, _score) in enumerate(ranking, start=1):
            fused[item_id] = fused.get(item_id, 0.0) + 1.0 / (k + rank)
    return sorted(fused.items(), key=lambda pair: -pair[1])
