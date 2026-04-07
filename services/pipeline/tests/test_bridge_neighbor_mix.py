"""Executable spec for ``neighbor_mix_v1`` (see ``bridge_neighbor_mix`` module docstring)."""

from __future__ import annotations

import math

import pytest

from pipeline.bridge_neighbor_mix import NeighborMixV1Result, neighbor_mix_v1


def _v(*xs: float) -> tuple[float, ...]:
    return tuple(xs)


def test_all_neighbors_same_cluster_mix_zero() -> None:
    """Anchor and k neighbors share cluster A ⇒ foreign count 0 ⇒ mix 0."""
    vectors = {
        1: _v(1.0, 0.0),
        2: _v(0.99, 0.01),
        3: _v(0.98, 0.02),
    }
    clusters = {1: "c0", 2: "c0", 3: "c0"}
    r = neighbor_mix_v1(1, vectors, clusters, k=2)
    assert r.eligible is True
    assert r.mix_score == 0.0
    assert r.foreign_neighbor_count == 0
    assert r.anchor_cluster_id == "c0"
    assert len(r.neighbor_work_ids) == 2


def test_all_neighbors_foreign_cluster_mix_one() -> None:
    """Anchor in c0; both neighbors in c1 ⇒ 2/2 foreign ⇒ mix 1."""
    vectors = {
        1: _v(1.0, 0.0),
        2: _v(0.0, 1.0),
        3: _v(0.0, 0.99),
    }
    clusters = {1: "c0", 2: "c1", 3: "c1"}
    r = neighbor_mix_v1(1, vectors, clusters, k=2)
    assert r.eligible is True
    assert r.mix_score == 1.0
    assert r.foreign_neighbor_count == 2


def test_half_foreign_mix_half() -> None:
    vectors = {
        10: _v(1.0, 0.0, 0.0),
        20: _v(0.99, 0.1, 0.0),
        30: _v(0.0, 1.0, 0.0),
        40: _v(0.0, 0.99, 0.0),
    }
    clusters = {10: "a", 20: "a", 30: "b", 40: "b"}
    r = neighbor_mix_v1(10, vectors, clusters, k=2)
    assert r.eligible is True
    assert r.mix_score == 0.5
    assert r.foreign_neighbor_count == 1
    assert set(r.neighbor_work_ids) == {20, 30}
    assert 20 in r.neighbor_work_ids


def test_tie_break_equal_cosine_smaller_work_id_first() -> None:
    """Equal cosine to two neighbors ⇒ lower work_id appears in top-k first when k=1."""
    vectors = {
        1: _v(1.0, 0.0),
        2: _v(0.0, 1.0),
        3: _v(0.0, -1.0),
    }
    clusters = {1: "c0", 2: "c1", 3: "c1"}
    cos_12 = 0.0
    cos_13 = 0.0
    assert math.isclose(cos_12, cos_13)
    r = neighbor_mix_v1(1, vectors, clusters, k=1)
    assert r.eligible is True
    assert r.neighbor_work_ids == (2,)


def test_self_excluded() -> None:
    vectors = {1: _v(1.0, 0.0)}
    clusters = {1: "c0"}
    r = neighbor_mix_v1(1, vectors, clusters, k=1)
    assert r.eligible is False
    assert r.mix_score is None


def test_ineligible_anchor_missing_cluster() -> None:
    vectors = {1: _v(1.0, 0.0), 2: _v(1.0, 0.1)}
    clusters = {2: "c0"}
    r = neighbor_mix_v1(1, vectors, clusters, k=1)
    assert r.eligible is False


def test_ineligible_anchor_zero_norm() -> None:
    vectors = {1: _v(0.0, 0.0), 2: _v(1.0, 0.0)}
    clusters = {1: "c0", 2: "c0"}
    r = neighbor_mix_v1(1, vectors, clusters, k=1, norm_eps=1e-9)
    assert r.eligible is False
    assert r.anchor_cluster_id == "c0"


def test_neighbor_with_tiny_norm_skipped() -> None:
    vectors = {
        1: _v(1.0, 0.0),
        2: _v(0.0, 0.0),
        3: _v(0.99, 0.01),
    }
    clusters = {1: "c0", 2: "c0", 3: "c1"}
    r = neighbor_mix_v1(1, vectors, clusters, k=1, norm_eps=1e-9)
    assert r.eligible is True
    assert r.neighbor_work_ids == (3,)


def test_cosine_on_raw_matches_normalized_dot() -> None:
    a = _v(3.0, 4.0)
    b = _v(5.0, 12.0)
    from pipeline.bridge_neighbor_mix import _cosine_similarity_raw

    raw = _cosine_similarity_raw(a, b)
    na = math.sqrt(9 + 16)
    nb = math.sqrt(25 + 144)
    a_n = tuple(x / na for x in a)
    b_n = tuple(x / nb for x in b)
    dot_n = sum(x * y for x, y in zip(a_n, b_n))
    assert raw is not None
    assert math.isclose(raw, dot_n, rel_tol=1e-12)


def test_k_positive_required() -> None:
    with pytest.raises(ValueError, match="k must be positive"):
        neighbor_mix_v1(1, {1: _v(1.0)}, {1: "c0"}, k=0)


def test_deterministic_full_ordering() -> None:
    """Fixed small pool: neighbor order and mix_score stable across repeated calls."""
    vectors = {i: _v(float(i), 1.0) for i in range(1, 8)}
    clusters = {i: ("c0" if i % 2 == 0 else "c1") for i in range(1, 8)}
    r1 = neighbor_mix_v1(1, vectors, clusters, k=3)
    r2 = neighbor_mix_v1(1, vectors, clusters, k=3)
    assert r1 == r2
    assert r1.eligible is True
