from __future__ import annotations

from dataclasses import dataclass
from math import fsum
from typing import Sequence


@dataclass(frozen=True)
class ClusteringInput:
    work_id: int
    vector: tuple[float, ...]


@dataclass(frozen=True)
class ClusterAssignment:
    work_id: int
    cluster_id: str


def _squared_l2_distance(a: Sequence[float], b: Sequence[float]) -> float:
    return fsum((float(x) - float(y)) ** 2 for x, y in zip(a, b))


def _mean_vector(vectors: list[tuple[float, ...]]) -> tuple[float, ...]:
    width = len(vectors[0])
    return tuple(fsum(float(v[i]) for v in vectors) / len(vectors) for i in range(width))


def cluster_inputs_kmeans(
    inputs: list[ClusteringInput],
    *,
    cluster_count: int,
    max_iterations: int = 20,
) -> list[ClusterAssignment]:
    if cluster_count <= 0:
        raise ValueError("cluster_count must be positive.")
    if max_iterations <= 0:
        raise ValueError("max_iterations must be positive.")
    if not inputs:
        return []

    ordered = sorted(inputs, key=lambda item: item.work_id)
    vector_width = len(ordered[0].vector)
    if vector_width <= 0:
        raise ValueError("Clustering vectors must be non-empty.")
    for item in ordered:
        if len(item.vector) != vector_width:
            raise ValueError("All vectors must have the same dimension.")

    k = min(cluster_count, len(ordered))
    centroids: list[tuple[float, ...]] = [ordered[idx].vector for idx in range(k)]
    assignments: dict[int, int] = {}

    for _ in range(max_iterations):
        next_assignments: dict[int, int] = {}
        for item in ordered:
            best_idx = min(
                range(k),
                key=lambda idx: (_squared_l2_distance(item.vector, centroids[idx]), idx),
            )
            next_assignments[item.work_id] = best_idx

        if next_assignments == assignments:
            break
        assignments = next_assignments

        for idx in range(k):
            members = [item.vector for item in ordered if assignments[item.work_id] == idx]
            if members:
                centroids[idx] = _mean_vector(members)

    return [
        ClusterAssignment(work_id=item.work_id, cluster_id=f"c{assignments[item.work_id]:03d}")
        for item in ordered
    ]


_BRIDGE_RATIO_EPS = 1e-12


def compute_bridge_boundary_scores(
    inputs: list[ClusteringInput],
    assignments: dict[int, str],
    *,
    epsilon: float = _BRIDGE_RATIO_EPS,
) -> dict[int, float | None]:
    """
    Prototype structural bridge signal: ratio of squared L2 distance to the assigned cluster
    centroid vs distance to the nearest other centroid (clamped to [0, 1]). Higher when the
    paper sits near a boundary between two centroids. Requires at least two clusters with
    embedded members. Uses the same squared L2 notion as kmeans-l2-v0.
    """
    if epsilon <= 0:
        raise ValueError("epsilon must be positive.")
    vectors_by_work = {item.work_id: item.vector for item in inputs}
    cluster_members: dict[str, list[int]] = {}
    for work_id, cluster_id in assignments.items():
        if work_id not in vectors_by_work:
            continue
        cluster_members.setdefault(cluster_id, []).append(work_id)

    if len(cluster_members) < 2:
        return {wid: None for wid in assignments}

    centroids: dict[str, tuple[float, ...]] = {}
    for cluster_id, member_ids in cluster_members.items():
        vecs = [vectors_by_work[i] for i in member_ids]
        centroids[cluster_id] = _mean_vector(vecs)

    out: dict[int, float | None] = {}
    for work_id, cluster_id in assignments.items():
        if work_id not in vectors_by_work:
            out[work_id] = None
            continue
        if cluster_id not in centroids:
            out[work_id] = None
            continue
        vec = vectors_by_work[work_id]
        d1 = _squared_l2_distance(vec, centroids[cluster_id])
        other_dists = [
            _squared_l2_distance(vec, centroids[cid]) for cid in centroids if cid != cluster_id
        ]
        if not other_dists:
            out[work_id] = None
            continue
        d2 = min(other_dists)
        ratio = d1 / max(d2, epsilon)
        if ratio <= 0:
            out[work_id] = 0.0
        elif ratio >= 1:
            out[work_id] = 1.0
        else:
            out[work_id] = ratio
    return out

