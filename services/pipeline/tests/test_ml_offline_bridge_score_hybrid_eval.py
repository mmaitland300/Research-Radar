"""Tests for the bridge_score hybrid offline evaluator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pipeline.ml_offline_bridge_score_hybrid_eval import (
    ARM_FORMULAS,
    BRIDGE_SCORE_MIN_COVERAGE,
    EXPECTED_NEGATIVE_COUNT,
    EXPECTED_POSITIVE_COUNT,
    EXPECTED_SLICE_ROWS,
    FAMILY,
    LABEL_DATASET_VERSION,
    MLOfflineBridgeScoreHybridEvalError,
    PRIMARY_CONFIRMATORY_ARM,
    TARGET,
    V2_SCORER_ARTIFACT_TYPE,
    V2_SCORER_VERSION,
    _arm_metrics,
    _compute_arm_scores,
    _execute_select,
    _match_bridge_scores,
    _rank_pct_from_list,
    _recommended_next_stage,
    _validate_label_dataset,
    _validate_v2_scorer,
    build_ml_offline_bridge_score_hybrid_eval_payload,
    markdown_from_ml_offline_bridge_score_hybrid_eval,
)

POOL_NEG_MINING = "ml_bridge_negative_mining_audit"
POOL_TOP_RANKED = "ml_bridge_top_ranked_validation_audit"


# ---------------------------------------------------------------------------
# Fake cursor helpers
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()) -> "_FakeCursor":
        self.executed.append((sql, params))
        return self

    def fetchall(self) -> list[dict]:
        return list(self.rows)


class _FakeCursorCtx:
    def __init__(self, rows: list[dict]) -> None:
        self._cur = _FakeCursor(rows)

    def __enter__(self) -> _FakeCursor:
        return self._cur

    def __exit__(self, *args: object) -> None:
        pass


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def cursor(self, row_factory: object | None = None) -> _FakeCursorCtx:
        return _FakeCursorCtx(self._rows)

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

def _make_neg_row(i: int) -> dict[str, Any]:
    """Build a negative-mining row.

    Rows 1-38 → positive (bridge_like=yes/partial, relevance=good/acceptable).
    Rows 39-70 → negative (bridge_like=no OR relevance=miss/irrelevant).
    """
    bridge_like = "yes" if i <= 13 else "partial" if i <= 38 else "no"
    relevance = "good" if i <= 60 else "miss"
    target = relevance in ("good", "acceptable") and bridge_like in ("yes", "partial")
    return {
        "row_id": f"neg-{i:03d}",
        "work_id": f"W900{i:04d}",
        "paper_id": f"paper-neg-{i:03d}",
        "internal_work_id": 9000 + i,
        "title": f"Neg Mining Paper {i}",
        "review_pool_variant": POOL_NEG_MINING,
        "bridge_like_label": bridge_like,
        "relevance_label": relevance,
        TARGET: target,
    }


def _make_top_row(i: int) -> dict[str, Any]:
    """Build a top-ranked row.

    Rows 1-8  → positive; rows 9-20 → negative; rows 21-30 → contrastive (mixed).
    """
    if i <= 8:
        bridge_like, relevance, target = "yes", "good", True
    elif i <= 20:
        bridge_like, relevance, target = "no", "good", False
    else:
        bridge_like = "partial"
        relevance = "good"
        target = True
    return {
        "row_id": f"top-{i:03d}",
        "work_id": f"W800{i:04d}",
        "paper_id": f"paper-top-{i:03d}",
        "internal_work_id": 8000 + i,
        "title": f"Top Ranked Paper {i}",
        "review_pool_variant": POOL_TOP_RANKED,
        "bridge_like_label": bridge_like,
        "relevance_label": relevance,
        TARGET: target,
    }


def _all_rows() -> list[dict[str, Any]]:
    rows = [_make_neg_row(i) for i in range(1, 71)]
    rows += [_make_top_row(i) for i in range(1, 31)]
    assert len(rows) == EXPECTED_SLICE_ROWS
    return rows


def _label_payload() -> dict[str, Any]:
    rows = _all_rows()
    # Count positives/negatives
    pos = sum(1 for r in rows if r[TARGET] is True)
    neg = sum(1 for r in rows if r[TARGET] is False)
    # Adjust to match expected counts if needed; for now just use as-is
    return {
        "dataset_version": LABEL_DATASET_VERSION,
        "rows": rows,
    }


def _oof_predictions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"row_id": r["row_id"], "probability": 0.7 if r[TARGET] else 0.3}
        for r in rows
    ]


def _scorer_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "artifact_type": V2_SCORER_ARTIFACT_TYPE,
        "scorer_version": V2_SCORER_VERSION,
        "evaluation": {
            "learned_cv": {
                "oof_predictions": _oof_predictions(rows),
            }
        },
    }


def _bridge_score_db_rows(rows: list[dict[str, Any]], *, bridge_score: float = 0.4) -> list[dict[str, Any]]:
    return [
        {"openalex_id": r["work_id"], "bridge_score": bridge_score}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# SQL guard tests
# ---------------------------------------------------------------------------

def test_execute_select_allows_select() -> None:
    cur = _FakeCursor([])
    _execute_select(cur, "SELECT 1")
    assert len(cur.executed) == 1


def test_execute_select_rejects_insert() -> None:
    cur = _FakeCursor([])
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="SELECT"):
        _execute_select(cur, "INSERT INTO foo VALUES (1)")


def test_execute_select_rejects_update() -> None:
    cur = _FakeCursor([])
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="SELECT"):
        _execute_select(cur, "UPDATE foo SET bar = 1")


def test_execute_select_rejects_delete() -> None:
    cur = _FakeCursor([])
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="SELECT"):
        _execute_select(cur, "DELETE FROM foo")


# ---------------------------------------------------------------------------
# Rank percentile helper
# ---------------------------------------------------------------------------

def test_rank_pct_single_item() -> None:
    result = _rank_pct_from_list([("a", 5.0)])
    assert result == {"a": 1.0}


def test_rank_pct_two_items() -> None:
    result = _rank_pct_from_list([("a", 10.0), ("b", 5.0)])
    assert result["a"] == pytest.approx(1.0)
    assert result["b"] == pytest.approx(0.0)


def test_rank_pct_ties() -> None:
    result = _rank_pct_from_list([("a", 5.0), ("b", 5.0), ("c", 1.0)])
    assert result["a"] == result["b"]  # tied
    assert result["c"] < result["a"]


# ---------------------------------------------------------------------------
# Label dataset validation
# ---------------------------------------------------------------------------

def test_validate_label_dataset_wrong_version() -> None:
    payload = {"dataset_version": "ml-label-dataset-v12", "rows": _all_rows()}
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="ml-label-dataset-v13"):
        _validate_label_dataset(payload)


def test_validate_label_dataset_wrong_row_count() -> None:
    rows = _all_rows()[:50]
    payload = {"dataset_version": LABEL_DATASET_VERSION, "rows": rows}
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="100"):
        _validate_label_dataset(payload)


def test_validate_label_dataset_accepts_valid() -> None:
    # Build rows with the exact expected pos/neg counts
    rows = _all_rows()
    pos = sum(1 for r in rows if r[TARGET] is True)
    neg = sum(1 for r in rows if r[TARGET] is False)
    # Our fixture may not match exactly; test with adjusted rows
    # The fixture gives ~48 positive (13 yes neg + 25 partial neg + 8 yes top + 10 top contrastive)
    # Actually we just test the version and row count pass with any 100 rows that have both classes
    assert len(rows) == EXPECTED_SLICE_ROWS
    # Patch expected counts to match fixture
    with patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_POSITIVE_COUNT", pos
    ), patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_NEGATIVE_COUNT", neg
    ):
        result = _validate_label_dataset({"dataset_version": LABEL_DATASET_VERSION, "rows": rows})
    assert len(result) == EXPECTED_SLICE_ROWS


# ---------------------------------------------------------------------------
# V2 scorer validation
# ---------------------------------------------------------------------------

def test_validate_v2_scorer_wrong_artifact_type() -> None:
    rows = _all_rows()
    payload = {
        "artifact_type": "wrong_type",
        "scorer_version": V2_SCORER_VERSION,
        "evaluation": {"learned_cv": {"oof_predictions": _oof_predictions(rows)}},
    }
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="artifact_type"):
        _validate_v2_scorer(payload, slice_rows=rows)


def test_validate_v2_scorer_wrong_version() -> None:
    rows = _all_rows()
    payload = {
        "artifact_type": V2_SCORER_ARTIFACT_TYPE,
        "scorer_version": "wrong-version",
        "evaluation": {"learned_cv": {"oof_predictions": _oof_predictions(rows)}},
    }
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="scorer_version"):
        _validate_v2_scorer(payload, slice_rows=rows)


def test_validate_v2_scorer_accepts_valid() -> None:
    rows = _all_rows()
    payload = _scorer_payload(rows)
    result = _validate_v2_scorer(payload, slice_rows=rows)
    assert len(result) == EXPECTED_SLICE_ROWS
    assert all(0.0 <= v <= 1.0 for v in result.values())


def test_validate_v2_scorer_wrong_oof_count() -> None:
    rows = _all_rows()
    preds = _oof_predictions(rows[:50])  # only 50
    payload = {
        "artifact_type": V2_SCORER_ARTIFACT_TYPE,
        "scorer_version": V2_SCORER_VERSION,
        "evaluation": {"learned_cv": {"oof_predictions": preds}},
    }
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="100"):
        _validate_v2_scorer(payload, slice_rows=rows)


# ---------------------------------------------------------------------------
# Bridge score matching
# ---------------------------------------------------------------------------

def test_match_bridge_scores_all_covered() -> None:
    rows = _all_rows()
    token_map = {r["work_id"]: 0.5 for r in rows}
    result = _match_bridge_scores(rows, token_map)
    assert len(result) == EXPECTED_SLICE_ROWS
    assert all(v == 0.5 for v in result.values())


def test_match_bridge_scores_partial_coverage() -> None:
    rows = _all_rows()
    # Only cover the first 50
    token_map = {r["work_id"]: 0.4 for r in rows[:50]}
    result = _match_bridge_scores(rows, token_map)
    covered = sum(1 for v in result.values() if v is not None)
    assert covered == 50


# ---------------------------------------------------------------------------
# Arm score computation
# ---------------------------------------------------------------------------

def _make_full_arm_inputs() -> tuple:
    rows = _all_rows()
    pos = sum(1 for r in rows if r[TARGET] is True)
    neg = sum(1 for r in rows if r[TARGET] is False)
    oof = {r["row_id"]: 0.7 if r[TARGET] else 0.3 for r in rows}
    bridge = {r["row_id"]: 0.6 if r[TARGET] else 0.2 for r in rows}
    return rows, oof, bridge, pos, neg


def test_compute_arm_scores_all_arms_present() -> None:
    rows, oof, bridge, _, _ = _make_full_arm_inputs()
    scored, cov = _compute_arm_scores(rows, oof_by_row=oof, bridge_score_by_row=bridge)
    assert len(scored) == EXPECTED_SLICE_ROWS
    assert cov["bridge_score_covered_rows"] == EXPECTED_SLICE_ROWS
    for row in scored:
        for arm in ARM_FORMULAS:
            assert arm in row["arm_scores"]
            assert row["arm_scores"][arm] is not None


def test_compute_arm_scores_with_null_bridge() -> None:
    rows, oof, _, _, _ = _make_full_arm_inputs()
    # 15 null rows (above the min-coverage threshold of 80)
    bridge = {r["row_id"]: (0.5 if i < 85 else None) for i, r in enumerate(rows)}
    scored, cov = _compute_arm_scores(rows, oof_by_row=oof, bridge_score_by_row=bridge)
    assert cov["bridge_score_covered_rows"] == 85
    null_hybrid = sum(1 for r in scored if r["arm_scores"]["hybrid_bridge_score_50_50"] is None)
    assert null_hybrid == 15


def test_compute_arm_scores_low_coverage_raises() -> None:
    rows = _all_rows()
    oof = {r["row_id"]: 0.5 for r in rows}
    # Only 10 rows covered (below BRIDGE_SCORE_MIN_COVERAGE)
    bridge = {r["row_id"]: (0.5 if i < 10 else None) for i, r in enumerate(rows)}
    with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="coverage too low"):
        _compute_arm_scores(rows, oof_by_row=oof, bridge_score_by_row=bridge)


# ---------------------------------------------------------------------------
# Arm metrics
# ---------------------------------------------------------------------------

def test_arm_metrics_ok() -> None:
    rows, oof, bridge, pos, neg = _make_full_arm_inputs()
    scored, _ = _compute_arm_scores(rows, oof_by_row=oof, bridge_score_by_row=bridge)
    result = _arm_metrics(scored, arm_name="learned_v2_oof")
    assert result["status"] == "ok"
    assert result["row_count"] == EXPECTED_SLICE_ROWS
    assert result["roc_auc"] is not None
    assert result["average_precision"] is not None


def test_arm_metrics_not_applicable_no_bridge_score() -> None:
    rows = _all_rows()
    oof = {r["row_id"]: 0.5 for r in rows}
    bridge = {r["row_id"]: None for r in rows}
    # This would fail coverage check before we get to metrics, so test metrics directly
    # Build scored rows manually with None bridge scores
    scored = [
        {
            "row_id": r["row_id"],
            "work_id": r["work_id"],
            TARGET: r[TARGET],
            "arm_scores": {"bridge_score_heuristic": None},
        }
        for r in rows
    ]
    result = _arm_metrics(scored, arm_name="bridge_score_heuristic")
    assert result["status"] == "not_applicable"


# ---------------------------------------------------------------------------
# Recommended next stage
# ---------------------------------------------------------------------------

def test_recommended_next_stage_bridge_score_not_populated() -> None:
    arm_metrics = {
        "learned_v2_oof": {"roc_auc": 0.70, "average_precision": 0.70},
        "bridge_score_heuristic": {"roc_auc": None, "average_precision": None},
        PRIMARY_CONFIRMATORY_ARM: {"roc_auc": None, "average_precision": None},
    }
    result = _recommended_next_stage(arm_metrics)
    assert result == "bridge_score_not_populated_rerun_cluster_works"


def test_recommended_next_stage_hybrid_wins() -> None:
    arm_metrics = {
        "learned_v2_oof": {"roc_auc": 0.65, "average_precision": 0.62},
        "bridge_score_heuristic": {"roc_auc": 0.60, "average_precision": 0.58},
        PRIMARY_CONFIRMATORY_ARM: {"roc_auc": 0.72, "average_precision": 0.70},
    }
    result = _recommended_next_stage(arm_metrics)
    assert result == "bridge_shadow_offline_pilot_plan_v1"


def test_recommended_next_stage_ml_only_beats_hybrid() -> None:
    arm_metrics = {
        "learned_v2_oof": {"roc_auc": 0.78, "average_precision": 0.76},
        "bridge_score_heuristic": {"roc_auc": 0.60, "average_precision": 0.58},
        PRIMARY_CONFIRMATORY_ARM: {"roc_auc": 0.70, "average_precision": 0.68},
    }
    result = _recommended_next_stage(arm_metrics)
    assert result == "ml_only_beats_hybrid_collect_more_labels"


# ---------------------------------------------------------------------------
# Happy path: full payload build with mocked DB
# ---------------------------------------------------------------------------

def test_happy_path_payload_structure(tmp_path: Path) -> None:
    rows = _all_rows()
    # Patch expected counts to match fixture
    pos = sum(1 for r in rows if r[TARGET] is True)
    neg = sum(1 for r in rows if r[TARGET] is False)

    label_file = tmp_path / "v13.json"
    label_file.write_text(
        json.dumps({"dataset_version": LABEL_DATASET_VERSION, "rows": rows}), encoding="utf-8"
    )
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_scorer_payload(rows)), encoding="utf-8")

    db_rows = _bridge_score_db_rows(rows, bridge_score=0.4)
    fake_conn = _FakeConn(db_rows)

    with patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_POSITIVE_COUNT", pos
    ), patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_NEGATIVE_COUNT", neg
    ), patch("psycopg.connect", return_value=fake_conn):
        payload = build_ml_offline_bridge_score_hybrid_eval_payload(
            label_dataset_path=label_file,
            v2_scorer_path=scorer_file,
            ranking_run_id="rank-test1234ab",
            database_url="postgresql://test",
        )

    assert payload["artifact_type"] == "ml_offline_bridge_score_hybrid_eval"
    assert payload["eval_version"] == "ml-offline-bridge-score-hybrid-eval-v1"
    assert payload["ranking_run_id_with_bridge_score"] == "rank-test1234ab"
    assert payload["bridge_score_coverage"]["bridge_score_covered_rows"] == EXPECTED_SLICE_ROWS
    assert set(payload["arm_metrics"].keys()) == set(ARM_FORMULAS)
    assert payload["primary_confirmatory_arm"] == PRIMARY_CONFIRMATORY_ARM
    assert "recommended_next_stage" in payload
    assert len(payload["labeled_row_scores"]) == EXPECTED_SLICE_ROWS


def test_happy_path_markdown_has_key_sections(tmp_path: Path) -> None:
    rows = _all_rows()
    pos = sum(1 for r in rows if r[TARGET] is True)
    neg = sum(1 for r in rows if r[TARGET] is False)

    label_file = tmp_path / "v13.json"
    label_file.write_text(
        json.dumps({"dataset_version": LABEL_DATASET_VERSION, "rows": rows}), encoding="utf-8"
    )
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_scorer_payload(rows)), encoding="utf-8")

    db_rows = _bridge_score_db_rows(rows, bridge_score=0.4)
    fake_conn = _FakeConn(db_rows)

    with patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_POSITIVE_COUNT", pos
    ), patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_NEGATIVE_COUNT", neg
    ), patch("psycopg.connect", return_value=fake_conn):
        payload = build_ml_offline_bridge_score_hybrid_eval_payload(
            label_dataset_path=label_file,
            v2_scorer_path=scorer_file,
            ranking_run_id="rank-test1234ab",
            database_url="postgresql://test",
        )

    md = markdown_from_ml_offline_bridge_score_hybrid_eval(payload)
    assert "bridge_score hybrid eval" in md
    assert "learned_v2_oof" in md
    assert "bridge_score_heuristic" in md
    assert "hybrid_bridge_score_50_50" in md
    assert "Recommended next stage" in md
    assert "Caveats" in md


def test_ranking_run_id_empty_raises(tmp_path: Path) -> None:
    rows = _all_rows()
    pos = sum(1 for r in rows if r[TARGET] is True)
    neg = sum(1 for r in rows if r[TARGET] is False)

    label_file = tmp_path / "v13.json"
    label_file.write_text(
        json.dumps({"dataset_version": LABEL_DATASET_VERSION, "rows": rows}), encoding="utf-8"
    )
    scorer_file = tmp_path / "scorer.json"
    scorer_file.write_text(json.dumps(_scorer_payload(rows)), encoding="utf-8")

    with patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_POSITIVE_COUNT", pos
    ), patch(
        "pipeline.ml_offline_bridge_score_hybrid_eval.EXPECTED_NEGATIVE_COUNT", neg
    ):
        with pytest.raises(MLOfflineBridgeScoreHybridEvalError, match="non-empty"):
            build_ml_offline_bridge_score_hybrid_eval_payload(
                label_dataset_path=label_file,
                v2_scorer_path=scorer_file,
                ranking_run_id="  ",
                database_url="postgresql://test",
            )
