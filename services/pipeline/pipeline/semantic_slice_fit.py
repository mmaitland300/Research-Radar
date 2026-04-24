"""
Emerging-only semantic v1: slice-fit score from title+abstract embeddings.

Definition: mean embedding vector over all included works in the snapshot that have
``embedding_version`` rows; per work, cosine similarity to that centroid, mapped to [0, 1]
via (cos_sim + 1) / 2. Cheap, deterministic, snapshot-scoped — not a universal relevance score.
"""

from __future__ import annotations

import math

from pipeline.clustering import ClusteringInput


def compute_semantic_slice_fit_by_work(rows: list[ClusteringInput]) -> dict[int, float]:
    if not rows:
        return {}
    dim = len(rows[0].vector)
    n = len(rows)
    centroid = tuple(
        sum(float(rows[i].vector[d]) for i in range(n)) / float(n) for d in range(dim)
    )
    nc = math.sqrt(sum(x * x for x in centroid))
    if nc < 1e-10:
        return {r.work_id: 0.5 for r in rows}
    nc += 1e-12
    out: dict[int, float] = {}
    for r in rows:
        v = r.vector
        nv = math.sqrt(sum(float(x) * float(x) for x in v)) + 1e-12
        dot = sum(float(v[d]) * float(centroid[d]) for d in range(dim))
        cos_sim = dot / (nv * nc)
        out[r.work_id] = max(0.0, min(1.0, (cos_sim + 1.0) / 2.0))
    return out
