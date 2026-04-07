"""
neighbor_mix_v1 — bridge-v2 exploratory signal (pure logic; no DB).

Executable spec: behavior is defined here and in ``tests/test_bridge_neighbor_mix.py``.

Inputs
------
- ``anchor_work_id``: which work to score.
- ``vectors_by_work``: raw embedding components per ``work_id`` (same convention as
  ``clustering.ClusteringInput``: arbitrary L2, not assumed pre-normalized).
- ``cluster_by_work``: cluster label per ``work_id`` (e.g. ``"c000"`` from k-means).
- ``k``: size of the neighbor set (neighbors are the ``k`` *other* works with highest cosine
  similarity to the anchor).

Cosine similarity (reproducibility)
---------------------------------
- Similarity is **standard cosine** on **raw** vectors:
  ``dot(a,b) / (||a||_2 * ||b||_2)``.
- This equals the dot product of **L2-normalized** copies of ``a`` and ``b``; we compute
  via the explicit formula so behavior does not depend on an intermediate normalize-then-dot
  order for non-unit vectors.
- If ``||anchor||_2 < norm_eps`` or ``||neighbor||_2 < norm_eps``, that vector is **invalid**
  for similarity (anchor invalid ⇒ ineligible; neighbor invalid ⇒ skipped for ranking).
- If two vectors differ in **length** (corrupted / wrong embedding row), the pair is **invalid**
  for similarity (``None``); we never truncate with ``zip`` to a shorter length.

Self exclusion
--------------
- The anchor is **never** a neighbor of itself.

Neighbor ranking and tie-breaking
---------------------------------
- Candidates are all ``work_id != anchor_work_id`` with valid vectors and cluster labels
  present in both maps.
- Sort by ascending key ``(-cosine_similarity, work_id)``: higher cosine first; equal cosine
  ⇒ smaller ``work_id`` wins (deterministic).
- Take the first ``k`` as the neighbor set. If fewer than ``k`` candidates exist ⇒ **ineligible**.

Scoring formula (when eligible)
-------------------------------
- Let ``C`` be the anchor's cluster id.
- ``mix_score = (number of neighbors whose cluster id ≠ C) / k`` (in ``[0, 1]``).

Eligibility
-----------
All must hold:

1. ``anchor_work_id`` in ``vectors_by_work`` and ``cluster_by_work``.
2. Anchor L2 norm ≥ ``norm_eps``.
3. At least ``k`` candidate neighbors after excluding self, invalid norms, and works missing
   from either map.

Output
------
``NeighborMixV1Result`` with ``eligible``, ``mix_score`` (``None`` if not eligible),
``neighbor_work_ids`` (the ``k`` chosen ids, empty if not eligible), and counts for debugging.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import fsum, sqrt
from typing import Any

from pipeline.clustering import ClusteringInput


_NORM_EPS_DEFAULT = 1e-12

# Neighbor count for neighbor_mix_v1 in ranking runs (persisted in ranking_runs.config_json).
NEIGHBOR_MIX_V1_DEFAULT_K = 15


@dataclass(frozen=True)
class NeighborMixV1Result:
    eligible: bool
    mix_score: float | None
    neighbor_work_ids: tuple[int, ...]
    anchor_cluster_id: str | None
    foreign_neighbor_count: int | None
    """Count of neighbors with cluster ≠ anchor cluster; ``None`` if not eligible."""


def _l2_norm(vec: tuple[float, ...]) -> float:
    return sqrt(fsum(float(x) * float(x) for x in vec))


def _cosine_similarity_raw(a: tuple[float, ...], b: tuple[float, ...]) -> float | None:
    """Cosine similarity on raw vectors; ``None`` if lengths differ or either L2 norm is zero."""
    if len(a) != len(b):
        return None
    dot_ab = fsum(float(x) * float(y) for x, y in zip(a, b, strict=True))
    na = _l2_norm(a)
    nb = _l2_norm(b)
    if na <= 0.0 or nb <= 0.0:
        return None
    return dot_ab / (na * nb)


def neighbor_mix_v1(
    anchor_work_id: int,
    vectors_by_work: Mapping[int, tuple[float, ...]],
    cluster_by_work: Mapping[int, str],
    k: int,
    *,
    norm_eps: float = _NORM_EPS_DEFAULT,
) -> NeighborMixV1Result:
    if k <= 0:
        raise ValueError("k must be positive.")

    if anchor_work_id not in vectors_by_work or anchor_work_id not in cluster_by_work:
        return NeighborMixV1Result(
            eligible=False,
            mix_score=None,
            neighbor_work_ids=(),
            anchor_cluster_id=None,
            foreign_neighbor_count=None,
        )

    anchor_vec = vectors_by_work[anchor_work_id]
    anchor_cluster = cluster_by_work[anchor_work_id]
    if _l2_norm(anchor_vec) < norm_eps:
        return NeighborMixV1Result(
            eligible=False,
            mix_score=None,
            neighbor_work_ids=(),
            anchor_cluster_id=anchor_cluster,
            foreign_neighbor_count=None,
        )

    scored: list[tuple[float, int]] = []
    for wid, vec in vectors_by_work.items():
        if wid == anchor_work_id:
            continue
        if wid not in cluster_by_work:
            continue
        if _l2_norm(vec) < norm_eps:
            continue
        sim = _cosine_similarity_raw(anchor_vec, vec)
        if sim is None:
            continue
        scored.append((sim, wid))

    scored.sort(key=lambda t: (-t[0], t[1]))
    top = scored[:k]
    if len(top) < k:
        return NeighborMixV1Result(
            eligible=False,
            mix_score=None,
            neighbor_work_ids=(),
            anchor_cluster_id=anchor_cluster,
            foreign_neighbor_count=None,
        )

    neighbor_ids = tuple(wid for _, wid in top)
    foreign = sum(1 for wid in neighbor_ids if cluster_by_work[wid] != anchor_cluster)
    mix = foreign / k
    return NeighborMixV1Result(
        eligible=True,
        mix_score=mix,
        neighbor_work_ids=neighbor_ids,
        anchor_cluster_id=anchor_cluster,
        foreign_neighbor_count=foreign,
    )


def compute_neighbor_mix_v1_by_work(
    inputs: Sequence[ClusteringInput],
    cluster_by_work: Mapping[int, str],
    k: int,
    *,
    norm_eps: float = _NORM_EPS_DEFAULT,
) -> dict[int, NeighborMixV1Result]:
    """Run ``neighbor_mix_v1`` for every ``work_id`` in ``inputs``."""
    vectors_by_work = {inp.work_id: inp.vector for inp in inputs}
    return {
        wid: neighbor_mix_v1(wid, vectors_by_work, cluster_by_work, k, norm_eps=norm_eps)
        for wid in vectors_by_work
    }


def neighbor_mix_v1_json_payload(result: NeighborMixV1Result, *, k: int) -> dict[str, Any]:
    """JSON-serializable payload for ``paper_scores.bridge_signal_json`` (debug / future API)."""
    payload: dict[str, Any] = {
        "signal_version": "neighbor_mix_v1",
        "k": k,
        "eligible": result.eligible,
    }
    if result.anchor_cluster_id is not None:
        payload["anchor_cluster_id"] = result.anchor_cluster_id
    if result.eligible:
        if result.mix_score is not None:
            payload["mix_score"] = result.mix_score
        payload["neighbor_work_ids"] = list(result.neighbor_work_ids)
        if result.foreign_neighbor_count is not None:
            payload["foreign_neighbor_count"] = result.foreign_neighbor_count
    return payload
