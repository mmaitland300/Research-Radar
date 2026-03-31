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

