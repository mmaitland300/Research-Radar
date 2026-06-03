"""Tests for ml_offline_bridge_hybrid_eval_v3."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_offline_bridge_hybrid_eval_v3 import (
    ARTIFACT_TYPE,
    BRIDGE_SCORE_MIN_COVERAGE,
    EVAL_VERSION,
    EXPECTED_SHADOW_NEGATIVE,
    EXPECTED_SHADOW_POSITIVE,
    EXPECTED_SHADOW_ROWS,
    MLOfflineBridgeHybridEvalV3Error,
    SENSITIVITY_ARTIFACT_VERSION,
    TARGET,
    _build_scored_rows,
    _recommended_next_stage,
    _validate_prerequisite_sensitivity_artifact,
    build_ml_offline_bridge_hybrid_eval_v3_payload,
    markdown_from_ml_offline_bridge_hybrid_eval_v3,
)

POOL = "ml_bridge_shadow_pilot_audit"
BUCKETS = (
    ["promoted_by_hybrid"] * 20
    + ["demoted_by_hybrid"] * 20
    + ["high_ml_low_bridge_score"] * 10
    + ["high_bridge_score_low_ml"] * 10
)


def _shadow_row(idx: int, *, bucket: str, label: bool, bridge_score: float | None) -> dict:
    token = f"W{1000000000 + idx}"
    return {
        "row_id": f"row-{idx}",
        "work_id": token,
        "split": "audit_only",
        "family": "bridge",
        "review_pool_variant": POOL,
        "ranking_run_id": "rank-5a7efa5ca3",
        TARGET: label,
        "sample_reason": bucket,
        "bridge_shadow_pilot_context": {
            "disagreement_bucket": bucket,
            "bridge_score": bridge_score,
        },
    }


def _make_shadow_rows() -> list[dict]:
    rows: list[dict] = []
    idx = 0
    for bucket, n, pos, neg in (
        ("promoted_by_hybrid", 20, 14, 6),
        ("demoted_by_hybrid", 20, 8, 12),
        ("high_ml_low_bridge_score", 10, 10, 0),
        ("high_bridge_score_low_ml", 10, 2, 8),
    ):
        for i in range(pos):
            rows.append(_shadow_row(idx, bucket=bucket, label=True, bridge_score=0.9 - idx * 0.01))
            idx += 1
        for i in range(neg):
            rows.append(_shadow_row(idx, bucket=bucket, label=False, bridge_score=0.4 + idx * 0.01))
            idx += 1
    return rows


def _sensitivity_payload(rows: list[dict]) -> dict:
    oof = [
        {
            "work_id": r["work_id"],
            "probability": 0.9 if r[TARGET] else 0.1,
            "review_pool_variant": POOL,
            "ranking_run_id": "rank-5a7efa5ca3",
            "disagreement_bucket": r["bridge_shadow_pilot_context"]["disagreement_bucket"],
            "label": r[TARGET],
        }
        for r in rows
    ]
    return {
        "artifact_type": "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity",
        "artifact_version": SENSITIVITY_ARTIFACT_VERSION,
        "ready_for_offline_hybrid_eval": True,
        "selected_frozen_coefficient_C": 0.001,
        "selected_frozen_scorer": {
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "C": 0.001,
        },
        "regularization_sweep": [
            {
                "C": 0.001,
                "learned_cv_primary_deduped": {"oof_predictions_deduped": oof},
            }
        ],
    }


def _readiness_payload(label_sha: str) -> dict:
    return {
        "provenance": {
            "label_dataset_version": "ml-label-dataset-v14",
            "label_dataset_sha256": label_sha,
        },
        "groups": [
            {
                "ranking_run_id": "rank-5a7efa5ca3",
                "family": "bridge",
                "target": TARGET,
                "total_labeled_rows": EXPECTED_SHADOW_ROWS,
                "positive_count": EXPECTED_SHADOW_POSITIVE,
                "negative_count": EXPECTED_SHADOW_NEGATIVE,
                "paper_scores_joinable_count": EXPECTED_SHADOW_ROWS,
            }
        ],
    }


def _embeddings_payload() -> dict:
    return {
        "metadata": {
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "embedding_version": "shadow-generalization-text-embedding-v1",
        }
    }


def _write_fixture(tmp_path: Path) -> dict[str, Path]:
    rows = _make_shadow_rows()
    label_path = tmp_path / "v14.json"
    label_path.write_text(
        json.dumps({"dataset_version": "ml-label-dataset-v14", "rows": rows}),
        encoding="utf-8",
    )
    label_sha = "abc123"
    readiness_path = tmp_path / "readiness.json"
    readiness_path.write_text(json.dumps(_readiness_payload(label_sha)), encoding="utf-8")
    sens_path = tmp_path / "sensitivity.json"
    sens_path.write_text(json.dumps(_sensitivity_payload(rows)), encoding="utf-8")
    emb_path = tmp_path / "embeddings.json"
    emb_path.write_text(json.dumps(_embeddings_payload()), encoding="utf-8")
    return {
        "label": label_path,
        "readiness": readiness_path,
        "sensitivity": sens_path,
        "embeddings": emb_path,
        "label_sha": label_sha,
    }


def test_prerequisite_rejects_not_ready() -> None:
    payload = {
        "artifact_type": "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity",
        "artifact_version": SENSITIVITY_ARTIFACT_VERSION,
        "ready_for_offline_hybrid_eval": False,
        "selected_frozen_coefficient_C": 0.001,
        "selected_frozen_scorer": {"C": 0.001},
    }
    with pytest.raises(MLOfflineBridgeHybridEvalV3Error, match="ready_for_offline_hybrid_eval"):
        _validate_prerequisite_sensitivity_artifact(payload, path=Path("x.json"))


def test_prerequisite_rejects_missing_frozen_scorer() -> None:
    payload = {
        "artifact_type": "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity",
        "artifact_version": SENSITIVITY_ARTIFACT_VERSION,
        "ready_for_offline_hybrid_eval": True,
        "selected_frozen_coefficient_C": 0.001,
    }
    with pytest.raises(MLOfflineBridgeHybridEvalV3Error, match="selected_frozen_scorer"):
        _validate_prerequisite_sensitivity_artifact(payload, path=Path("x.json"))


def test_bridge_score_coverage_aborts() -> None:
    rows = _make_shadow_rows()
    for r in rows[:10]:
        r["bridge_shadow_pilot_context"]["bridge_score"] = None
    oof = {r["work_id"]: 0.5 for r in rows}
    with pytest.raises(MLOfflineBridgeHybridEvalV3Error, match="bridge_score coverage too low"):
        _build_scored_rows(rows, oof_by_work_id=oof)


def test_three_alphas_produce_distinct_hybrid_scores(tmp_path: Path) -> None:
    paths = _write_fixture(tmp_path)
    with patch(
        "pipeline.ml_offline_bridge_hybrid_eval_v3.sha256_file",
        return_value="abc123",
    ):
        payload = build_ml_offline_bridge_hybrid_eval_v3_payload(
            sensitivity_artifact_path=paths["sensitivity"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
        )
    alpha_keys = ["hybrid_alpha_0_3", "hybrid_alpha_0_5", "hybrid_alpha_0_7"]
    hybrid_scores = []
    for key in alpha_keys:
        hybrid = payload["alpha_results"][key]["arm_metrics"]["hybrid"]
        assert hybrid["status"] == "ok"
        hybrid_scores.append(hybrid["roc_auc"])
    assert len(set(hybrid_scores)) >= 2


def test_targeted_readout_keys_present(tmp_path: Path) -> None:
    paths = _write_fixture(tmp_path)
    with patch(
        "pipeline.ml_offline_bridge_hybrid_eval_v3.sha256_file",
        return_value="abc123",
    ):
        payload = build_ml_offline_bridge_hybrid_eval_v3_payload(
            sensitivity_artifact_path=paths["sensitivity"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
        )
    targeted = payload["primary_summary"]["targeted_readouts"]
    for bucket in (
        "high_bridge_score_low_ml",
        "promoted_by_hybrid",
        "demoted_by_hybrid",
        "high_ml_low_bridge_score",
    ):
        assert bucket in targeted
    for bucket in ("high_bridge_score_low_ml", "promoted_by_hybrid", "demoted_by_hybrid"):
        for arm in ("pure_ml", "pure_bridge", "hybrid"):
            assert "verdict" in targeted[bucket][arm]


def test_recommended_next_stage_rescues_branch() -> None:
    targeted = {
        "high_bridge_score_low_ml": {"hybrid": {"verdict": "rescues_high_bridge_positives"}},
        "promoted_by_hybrid": {"hybrid": {"verdict": "maintains_hybrid_promotion"}},
        "demoted_by_hybrid": {"hybrid": {"verdict": "separates_demotions"}},
    }
    lift = {"hybrid_hurts_ml_precision": False}
    stage, rescue, _ = _recommended_next_stage(targeted=targeted, hybrid_lift=lift)
    assert stage == "authorize_bridge_hybrid_serving_controlled_rollout_eval"
    assert rescue is True


def test_recommended_next_stage_collect_more_branch() -> None:
    targeted = {
        "high_bridge_score_low_ml": {"hybrid": {"verdict": "partial"}},
        "promoted_by_hybrid": {"hybrid": {"verdict": "partial"}},
        "demoted_by_hybrid": {"hybrid": {"verdict": "separates_demotions"}},
    }
    lift = {"hybrid_hurts_ml_precision": False}
    stage, rescue, _ = _recommended_next_stage(targeted=targeted, hybrid_lift=lift)
    assert stage == "collect_more_high_bridge_score_low_ml_labels_before_serving"
    assert rescue is False


def test_markdown_contains_required_sections(tmp_path: Path) -> None:
    paths = _write_fixture(tmp_path)
    with patch(
        "pipeline.ml_offline_bridge_hybrid_eval_v3.sha256_file",
        return_value="abc123",
    ):
        payload = build_ml_offline_bridge_hybrid_eval_v3_payload(
            sensitivity_artifact_path=paths["sensitivity"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
        )
    md = markdown_from_ml_offline_bridge_hybrid_eval_v3(payload)
    assert "SHA256" in md
    assert "Arm comparison" in md
    assert "Targeted readout verdicts" in md
    assert "recommended_next_stage" in md


def test_happy_path_payload_structure(tmp_path: Path) -> None:
    paths = _write_fixture(tmp_path)
    with patch(
        "pipeline.ml_offline_bridge_hybrid_eval_v3.sha256_file",
        return_value="abc123",
    ):
        payload = build_ml_offline_bridge_hybrid_eval_v3_payload(
            sensitivity_artifact_path=paths["sensitivity"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
        )
    assert payload["artifact_type"] == ARTIFACT_TYPE
    assert payload["eval_version"] == EVAL_VERSION
    assert payload["bridge_score_coverage"]["bridge_score_covered_rows"] >= BRIDGE_SCORE_MIN_COVERAGE
    assert "hybrid_rescue_confirmed" in payload


def test_module_has_no_psycopg_import() -> None:
    import pipeline.ml_offline_bridge_hybrid_eval_v3 as mod

    source = Path(mod.__file__).read_text(encoding="utf-8")
    assert "psycopg" not in source
