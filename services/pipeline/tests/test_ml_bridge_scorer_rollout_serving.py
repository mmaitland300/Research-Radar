"""Tests for bounded Bridge scorer serving helper."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline import ml_bridge_scorer_rollout_serving as serving


def _candidate(idx: int, *, bridge_score: float | None = None, token: str | None = None) -> dict:
    work_token = token or f"W{idx:03d}"
    return {
        "work_id_int": idx,
        "work_id_token": work_token,
        "openalex_id": f"https://openalex.org/{work_token}",
        "bridge_score": bridge_score if bridge_score is not None else idx / 100,
        "final_score": 1.0 - idx / 1000,
        "current_family_rank": idx,
        "ml_probability": 0.0,
        "ml_rank_pct": 0.0,
        "hybrid_score": -999.0,
        "hybrid_rank": 999,
    }


def _frozen_scorer(c_value: float = 0.001) -> dict:
    return {
        "C": c_value,
        "embedding_version": "shadow-generalization-text-embedding-v1",
        "scaler_mean": [0.0],
        "scaler_scale": [1.0],
        "coef": [1.0],
        "intercept": 0.0,
    }


def test_full_pool_is_scored_not_only_top20(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidates = [_candidate(i) for i in range(1, 26)]
    seen: dict[str, int] = {}

    monkeypatch.setattr(
        serving,
        "_load_serving_plan_and_frozen_scorer",
        lambda **_kwargs: (
            {"pinned_run_context": {"candidate_count": 25}},
            _frozen_scorer(),
        ),
    )
    monkeypatch.setattr(serving, "_fetch_bridge_candidates_from_db", lambda *_args, **_kwargs: candidates)

    def fake_score(pool, **_kwargs):
        seen["pool_size"] = len(pool)
        return {str(row["work_id_token"]): idx / 100 for idx, row in enumerate(pool, start=1)}

    monkeypatch.setattr(serving, "_score_pool_ml_probabilities", fake_score)

    rows, metadata = serving.rank_bridge_recommendations_with_scorer(
        database_url="postgresql://example",
        repo_root=tmp_path,
        limit=3,
    )

    assert seen["pool_size"] == 25
    assert len(rows) == 3
    assert metadata["scored_candidate_count"] == 25
    assert metadata["returned_count"] == 3
    assert len(serving.map_bridge_scorer_rows_to_paper_ids(rows, limit=3)) == 3


def test_rank_percentiles_are_recomputed_and_sort_uses_hybrid_then_token() -> None:
    candidates = [
        _candidate(2, bridge_score=0.5, token="W002"),
        _candidate(1, bridge_score=0.5, token="W001"),
        _candidate(3, bridge_score=0.1, token="W003"),
    ]
    ml_probs = {"W001": 0.9, "W002": 0.9, "W003": 0.1}

    rows = serving._rank_bridge_candidates(candidates, ml_prob_by_token=ml_probs)

    assert [row["work_id_token"] for row in rows] == ["W001", "W002", "W003"]
    assert rows[0]["hybrid_score"] == rows[1]["hybrid_score"]
    assert rows[0]["hybrid_rank"] == 1
    assert rows[0]["v3_ml_probability"] == 0.9
    assert rows[0]["hybrid_score"] != -999.0


def test_missing_bridge_score_raises_controlled_error() -> None:
    candidates = [_candidate(1, bridge_score=None), _candidate(2)]
    candidates[0]["bridge_score"] = None

    with pytest.raises(serving.MLBridgeScorerRolloutServingError, match="bridge_score"):
        serving._rank_bridge_candidates(candidates, ml_prob_by_token={"W001": 0.9, "W002": 0.1})


def test_selected_frozen_scorer_c_001_is_required(tmp_path: Path) -> None:
    with pytest.raises(serving.MLBridgeScorerRolloutServingError, match="selected_frozen_scorer.C"):
        serving._validate_frozen_scorer(
            {
                "selected_frozen_coefficient_C": 0.001,
                "selected_frozen_scorer": _frozen_scorer(c_value=1.0),
            },
            path=tmp_path / "sensitivity.json",
        )


def test_serving_plan_artifact_sha256_is_enforced(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"ok": true}\n', encoding="utf-8")

    serving._validate_sha256(
        artifact,
        serving.sha256_file(artifact),
        label="test artifact",
    )
    with pytest.raises(serving.MLBridgeScorerRolloutServingError, match="SHA256 mismatch"):
        serving._validate_sha256(
            artifact,
            "0" * 64,
            label="test artifact",
        )


def test_historical_shadow_pilot_fields_are_ignored() -> None:
    candidates = [
        _candidate(1, bridge_score=0.1, token="W001"),
        _candidate(2, bridge_score=0.9, token="W002"),
    ]
    candidates[0]["ml_probability"] = 0.0
    candidates[0]["ml_rank_pct"] = 0.0
    candidates[0]["hybrid_score"] = 0.0
    candidates[0]["hybrid_rank"] = 99

    rows = serving._rank_bridge_candidates(
        candidates,
        ml_prob_by_token={"W001": 0.95, "W002": 0.05},
    )

    row_w1 = next(row for row in rows if row["work_id_token"] == "W001")
    assert row_w1["v3_ml_probability"] == 0.95
    assert row_w1["v3_ml_rank_pct"] > 0
    assert row_w1["hybrid_score"] > 0
    assert row_w1["hybrid_rank"] != 99


def test_execute_select_rejects_write_sql() -> None:
    class Cursor:
        def execute(self, *_args, **_kwargs):  # pragma: no cover - should not be called
            raise AssertionError("write SQL should be rejected before execution")

    with pytest.raises(serving.MLBridgeScorerRolloutServingError, match="write/DDL"):
        serving._execute_select(Cursor(), "SELECT 1; UPDATE paper_scores SET final_score = 0")
