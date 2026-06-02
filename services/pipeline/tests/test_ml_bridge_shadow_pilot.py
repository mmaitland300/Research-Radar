"""Tests for the bridge shadow pilot module."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from pipeline.ml_bridge_shadow_pilot import (
    ARTIFACT_TYPE,
    FAMILY,
    PILOT_VERSION,
    TOP_K,
    WORKSHEET_ARTIFACT_TYPE,
    MLBridgeShadowPilotError,
    _add_hybrid_ranks,
    _build_worksheet_rows,
    _comparison_tables,
    _disagreement_buckets,
    _execute_select,
    _load_frozen_scorer,
    _rank_pct_from_pairs,
    _score_candidates,
    _score_with_frozen_model,
    build_ml_bridge_shadow_pilot_payload,
    markdown_from_shadow_pilot,
)

V2_ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer_v2"
V2_SCORER_VERSION = "ml-offline-bridge-recommendable-scorer-v2"


# ---------------------------------------------------------------------------
# Fake DB helpers
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, rows: list) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()) -> "_FakeCursor":
        self.executed.append((sql, params))
        return self

    def fetchall(self) -> list:
        return list(self.rows)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeConn:
    """Fake psycopg connection with two query results: candidates, then embeddings."""

    def __init__(self, candidate_rows: list, embedding_rows: list) -> None:
        self._results = [candidate_rows, embedding_rows]
        self._call_idx = 0
        self._txn_started = False

    def cursor(self, row_factory: object | None = None) -> _FakeCursor:
        rows = self._results[min(self._call_idx, len(self._results) - 1)]
        self._call_idx += 1
        return _FakeCursor(rows)

    def transaction(self) -> "_FakeTxn":
        return _FakeTxn()

    def execute(self, sql: str, params: tuple = ()) -> None:
        pass

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeTxn:
    def __enter__(self) -> "_FakeTxn":
        return self

    def __exit__(self, *args: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DIM = 4  # tiny embeddings for tests


def _frozen_scorer(dim: int = DIM) -> dict[str, Any]:
    return {
        "scaler_mean": [0.0] * dim,
        "scaler_scale": [1.0] * dim,
        "coef": [0.1] * dim,
        "intercept": 0.0,
        "embedding_dimensions": dim,
        "embedding_version": "shadow-generalization-text-embedding-v1",
    }


def _v2_artifact(dim: int = DIM) -> dict[str, Any]:
    return {
        "artifact_type": V2_ARTIFACT_TYPE,
        "scorer_version": V2_SCORER_VERSION,
        "frozen_scorer": {
            "scorer_version": V2_SCORER_VERSION,
            "target": "bridge_recommendable",
            "feature_source": "embeddings.vector",
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "embedding_dimensions": dim,
            "scaler_mean": [0.0] * dim,
            "scaler_scale": [1.0] * dim,
            "coef": [0.1] * dim,
            "intercept": 0.0,
            "classes": [0, 1],
        },
    }


def _candidate_row(i: int, *, bridge_score: float = 0.4) -> dict[str, Any]:
    return {
        "work_id_int": 1000 + i,
        "openalex_id": f"https://openalex.org/W{1000 + i}",
        "title": f"Paper {i}",
        "bridge_score": bridge_score,
        "final_score": 1.0 - i * 0.01,  # descending
        "current_family_rank": i,
    }


def _make_n_candidates(n: int, *, bridge_score: float = 0.4) -> list[dict[str, Any]]:
    return [_candidate_row(i + 1, bridge_score=bridge_score) for i in range(n)]


def _make_embeddings(candidates: list[dict[str, Any]], dim: int = DIM) -> dict[int, list[float]]:
    return {int(r["work_id_int"]): [0.5] * dim for r in candidates}


# ---------------------------------------------------------------------------
# SQL guard
# ---------------------------------------------------------------------------

def test_execute_select_allows_select() -> None:
    cur = _FakeCursor([])
    _execute_select(cur, "SELECT 1")
    assert cur.executed


def test_execute_select_rejects_insert() -> None:
    cur = _FakeCursor([])
    with pytest.raises(MLBridgeShadowPilotError, match="SELECT"):
        _execute_select(cur, "INSERT INTO foo VALUES (1)")


def test_execute_select_rejects_update() -> None:
    cur = _FakeCursor([])
    with pytest.raises(MLBridgeShadowPilotError, match="SELECT"):
        _execute_select(cur, "UPDATE foo SET bar = 1")


# ---------------------------------------------------------------------------
# Frozen scorer loading
# ---------------------------------------------------------------------------

def test_load_frozen_scorer_ok() -> None:
    artifact = _v2_artifact(dim=8)
    result = _load_frozen_scorer(artifact)
    assert result["embedding_dimensions"] == 8
    assert len(result["coef"]) == 8


def test_load_frozen_scorer_wrong_artifact_type() -> None:
    artifact = _v2_artifact()
    artifact["artifact_type"] = "wrong"
    with pytest.raises(MLBridgeShadowPilotError, match="artifact_type"):
        _load_frozen_scorer(artifact)


def test_load_frozen_scorer_wrong_scorer_version() -> None:
    artifact = _v2_artifact()
    artifact["scorer_version"] = "wrong-version"
    with pytest.raises(MLBridgeShadowPilotError, match="scorer_version"):
        _load_frozen_scorer(artifact)


def test_load_frozen_scorer_missing_frozen() -> None:
    artifact = _v2_artifact()
    del artifact["frozen_scorer"]
    with pytest.raises(MLBridgeShadowPilotError, match="frozen_scorer"):
        _load_frozen_scorer(artifact)


def test_load_frozen_scorer_dimension_mismatch() -> None:
    artifact = _v2_artifact()
    artifact["frozen_scorer"]["coef"] = [0.1] * 3   # wrong length vs mean/scale (4)
    with pytest.raises(MLBridgeShadowPilotError, match="dimension"):
        _load_frozen_scorer(artifact)


# ---------------------------------------------------------------------------
# Frozen model inference
# ---------------------------------------------------------------------------

def test_score_with_frozen_model_zero_vector() -> None:
    frozen = _frozen_scorer(dim=4)
    prob = _score_with_frozen_model(
        [0.0] * 4,
        scaler_mean=frozen["scaler_mean"],
        scaler_scale=frozen["scaler_scale"],
        coef=frozen["coef"],
        intercept=0.0,
    )
    assert prob == pytest.approx(0.5, abs=1e-6)  # sigmoid(0) = 0.5


def test_score_with_frozen_model_positive_logit() -> None:
    frozen = _frozen_scorer(dim=2)
    # coef=[1,0], intercept=2 → logit=2 → prob > 0.5
    prob = _score_with_frozen_model(
        [1.0, 0.0],
        scaler_mean=[0.0, 0.0],
        scaler_scale=[1.0, 1.0],
        coef=[1.0, 0.0],
        intercept=2.0,
    )
    assert prob > 0.5


def test_score_with_frozen_model_wrong_dim() -> None:
    frozen = _frozen_scorer(dim=4)
    with pytest.raises(MLBridgeShadowPilotError, match="dimension"):
        _score_with_frozen_model(
            [0.5] * 3,  # wrong dim
            scaler_mean=frozen["scaler_mean"],
            scaler_scale=frozen["scaler_scale"],
            coef=frozen["coef"],
            intercept=0.0,
        )


# ---------------------------------------------------------------------------
# Rank percentile helper
# ---------------------------------------------------------------------------

def test_rank_pct_basic() -> None:
    pairs = [("a", 10.0), ("b", 5.0), ("c", 1.0)]
    result = _rank_pct_from_pairs(pairs)
    assert result["a"] > result["b"] > result["c"]


def test_rank_pct_ties() -> None:
    pairs = [("a", 5.0), ("b", 5.0), ("c", 1.0)]
    result = _rank_pct_from_pairs(pairs)
    assert result["a"] == result["b"]
    assert result["a"] > result["c"]


# ---------------------------------------------------------------------------
# Scoring candidates
# ---------------------------------------------------------------------------

def test_score_candidates_ok() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(10)
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    assert len(scored) == 10
    for row in scored:
        assert 0.0 <= row["ml_probability"] <= 1.0
        assert "work_id_token" in row


def test_score_candidates_missing_embedding() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(5)
    embeddings = {}  # no embeddings
    with pytest.raises(MLBridgeShadowPilotError, match="embedding"):
        _score_candidates(candidates, embeddings, frozen=frozen)


# ---------------------------------------------------------------------------
# Hybrid ranks
# ---------------------------------------------------------------------------

def test_add_hybrid_ranks_all_covered() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(20)
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    ranks = [r["hybrid_rank"] for r in scored]
    assert sorted(ranks) == list(range(1, 21))  # all 1..20 present
    for row in scored:
        assert row["hybrid_score"] is not None


def test_add_hybrid_ranks_null_bridge_score() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(10, bridge_score=None)  # type: ignore[arg-type]
    for c in candidates:
        c["bridge_score"] = None
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    assert all(r["hybrid_score"] is None for r in scored)
    assert all(r["bridge_score_rank_pct"] is None for r in scored)


# ---------------------------------------------------------------------------
# Comparison tables
# ---------------------------------------------------------------------------

def test_comparison_tables_stable_when_identical() -> None:
    frozen = _frozen_scorer()
    # Force all 528 to have the same ML prob so current rank == hybrid rank
    candidates = _make_n_candidates(30)
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    k = 10
    comp = _comparison_tables(scored, k=k)
    # With identical ML probs and same bridge_score, hybrid order = current order.
    # Stable + promoted = k (hybrid top-k); demoted == promoted (symmetric churn).
    assert comp["promoted_count"] + comp["stable_count"] == k
    assert comp["demoted_count"] == comp["promoted_count"]


def test_comparison_tables_structure() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(30)
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    comp = _comparison_tables(scored, k=10)
    assert "promoted" in comp
    assert "demoted" in comp
    assert "stable" in comp
    assert isinstance(comp["promoted"], list)


# ---------------------------------------------------------------------------
# Worksheet builder
# ---------------------------------------------------------------------------

def test_build_worksheet_deduplicates() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(30)
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    comp = _comparison_tables(scored, k=10)
    buckets = _disagreement_buckets(scored)
    rows = _build_worksheet_rows(comp, buckets)
    work_ids = [r["work_id"] for r in rows]
    assert len(work_ids) == len(set(work_ids))  # no duplicates


def test_build_worksheet_has_blank_labels() -> None:
    frozen = _frozen_scorer()
    candidates = _make_n_candidates(30)
    embeddings = _make_embeddings(candidates)
    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    comp = _comparison_tables(scored, k=10)
    buckets = _disagreement_buckets(scored)
    rows = _build_worksheet_rows(comp, buckets)
    for row in rows:
        assert row["bridge_like_label"] == ""
        assert row["relevance_label"] == ""


# ---------------------------------------------------------------------------
# Happy path: full payload build with mocked DB
# ---------------------------------------------------------------------------

def test_happy_path_payload_structure(tmp_path: Path) -> None:
    N = 30
    candidates = _make_n_candidates(N)
    embedding_rows = [
        {"work_id": int(r["work_id_int"]), "vector": json.dumps([0.5] * DIM)}
        for r in candidates
    ]
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_v2_artifact()), encoding="utf-8")

    fake_conn = _FakeConn(candidates, embedding_rows)

    with patch(
        "pipeline.ml_bridge_shadow_pilot.EXPECTED_CANDIDATE_COUNT", N
    ), patch(
        "pipeline.ml_bridge_shadow_pilot.MIN_BRIDGE_SCORE_COVERAGE", 0.0
    ), patch("psycopg.connect", return_value=fake_conn):
        payload, ws_rows = build_ml_bridge_shadow_pilot_payload(
            v2_scorer_path=scorer_file,
            ranking_run_id="rank-test1234ab",
            database_url="postgresql://test",
        )

    assert payload["artifact_type"] == ARTIFACT_TYPE
    assert payload["pilot_version"] == PILOT_VERSION
    assert payload["ranking_run_id"] == "rank-test1234ab"
    assert payload["candidate_count"] == N
    assert "top_20_comparison" in payload
    assert "disagreement_buckets" in payload
    assert isinstance(payload["all_candidates"], list)
    assert len(payload["all_candidates"]) == N


def test_happy_path_markdown_structure(tmp_path: Path) -> None:
    N = 30
    candidates = _make_n_candidates(N)
    embedding_rows = [
        {"work_id": int(r["work_id_int"]), "vector": json.dumps([0.5] * DIM)}
        for r in candidates
    ]
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_v2_artifact()), encoding="utf-8")

    fake_conn = _FakeConn(candidates, embedding_rows)

    with patch(
        "pipeline.ml_bridge_shadow_pilot.EXPECTED_CANDIDATE_COUNT", N
    ), patch(
        "pipeline.ml_bridge_shadow_pilot.MIN_BRIDGE_SCORE_COVERAGE", 0.0
    ), patch("psycopg.connect", return_value=fake_conn):
        payload, _ = build_ml_bridge_shadow_pilot_payload(
            v2_scorer_path=scorer_file,
            ranking_run_id="rank-test1234ab",
            database_url="postgresql://test",
        )

    md = markdown_from_shadow_pilot(payload)
    assert "Bridge shadow pilot" in md
    assert "Promoted" in md
    assert "Demoted" in md
    assert "Caveats" in md


def test_empty_ranking_run_raises(tmp_path: Path) -> None:
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_v2_artifact()), encoding="utf-8")
    with pytest.raises(MLBridgeShadowPilotError, match="non-empty"):
        build_ml_bridge_shadow_pilot_payload(
            v2_scorer_path=scorer_file,
            ranking_run_id="  ",
            database_url="postgresql://test",
        )


def test_low_bridge_score_coverage_raises(tmp_path: Path) -> None:
    N = 10
    candidates = [
        {**_candidate_row(i + 1), "bridge_score": None}  # all null
        for i in range(N)
    ]
    embedding_rows = [
        {"work_id": int(r["work_id_int"]), "vector": json.dumps([0.5] * DIM)}
        for r in candidates
    ]
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_v2_artifact()), encoding="utf-8")

    fake_conn = _FakeConn(candidates, embedding_rows)
    with patch("psycopg.connect", return_value=fake_conn):
        with pytest.raises(MLBridgeShadowPilotError, match="coverage"):
            build_ml_bridge_shadow_pilot_payload(
                v2_scorer_path=scorer_file,
                ranking_run_id="rank-test",
                database_url="postgresql://test",
            )
