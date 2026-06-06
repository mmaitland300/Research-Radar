"""Tests for Bridge rank-pct hybrid controlled rollout replay."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_bridge_rank_pct_hybrid_controlled_rollout_eval import (
    ARTIFACT_TYPE,
    ARTIFACT_VERSION,
    HIGH_RISK_ML_PROB_THRESHOLD,
    HIGH_RISK_ML_RANK_PCT_THRESHOLD,
    MLBridgeRankPctHybridControlledRolloutEvalError,
    _alpha_key,
    _apply_label_overlay,
    _group_summary,
    _label_overlay_by_work_id,
    _recommended_next_stage_for_comparison,
    _risk_readouts,
    _validate_rank_pct_eval_artifact,
    _validate_shadow_pilot_artifact,
    build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload,
    markdown_from_ml_bridge_rank_pct_hybrid_controlled_rollout_eval,
)
from pipeline.ml_offline_bridge_hybrid_eval_v3 import MLOfflineBridgeHybridEvalV3Error


def _candidate(idx: int, *, current_rank: int, bridge_score: float) -> dict:
    token = f"W{idx}"
    return {
        "work_id_int": idx,
        "openalex_id": f"https://openalex.org/{token}",
        "work_id_token": token,
        "title": f"Paper {idx}",
        "bridge_score": bridge_score,
        "current_family_rank": current_rank,
        "ml_probability": 0.123,
        "ml_rank_pct": 0.0,
        "bridge_score_rank_pct": 0.0,
        "hybrid_score": 0.0,
        "hybrid_rank": 99,
    }


def _candidates() -> list[dict]:
    return [
        _candidate(1, current_rank=1, bridge_score=0.60),
        _candidate(2, current_rank=2, bridge_score=0.50),
        _candidate(3, current_rank=3, bridge_score=0.40),
        _candidate(4, current_rank=4, bridge_score=0.99),
        _candidate(5, current_rank=5, bridge_score=0.98),
        _candidate(6, current_rank=6, bridge_score=0.20),
        _candidate(7, current_rank=7, bridge_score=0.10),
        _candidate(8, current_rank=8, bridge_score=0.05),
    ]


def _fake_ml_probs() -> dict[str, float]:
    return {
        "W1": 0.95,
        "W2": 0.20,
        "W3": 0.30,
        "W4": 0.90,
        "W5": 0.10,
        "W6": 0.80,
        "W7": 0.05,
        "W8": 0.01,
    }


def _shadow_pilot_payload(*, ranking_run_id: str = "rank-5a7efa5ca3", candidates: list[dict] | None = None) -> dict:
    rows = candidates or _candidates()
    return {
        "artifact_type": "ml_bridge_shadow_pilot",
        "pilot_version": "ml-bridge-shadow-pilot-v1",
        "ranking_run_id": ranking_run_id,
        "candidate_count": len(rows),
        "all_candidates": rows,
    }


def _sensitivity_payload(*, ready: bool = True, version: str | None = None) -> dict:
    return {
        "artifact_type": "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity",
        "artifact_version": version or "ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1",
        "ready_for_offline_hybrid_eval": ready,
        "selected_frozen_coefficient_C": 0.001,
        "selected_frozen_scorer": {
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "C": 0.001,
            "scaler_mean": [0.0],
            "scaler_scale": [1.0],
            "coef": [1.0],
            "intercept": 0.0,
        },
    }


def _rank_pct_eval_payload(**overrides: object) -> dict:
    payload = {
        "artifact_type": "ml_offline_bridge_hybrid_rank_pct_eval_v3",
        "eval_version": "ml-offline-bridge-hybrid-rank-pct-eval-v3-v1",
        "rank_percentile_scope": "full_bridge_candidate_pool",
        "recommended_next_stage": "authorize_bridge_hybrid_serving_controlled_rollout_eval",
        "hybrid_rescue_confirmed": True,
    }
    payload.update(overrides)
    return payload


def _label_row(token: str, *, pool: str, label: bool | None) -> dict:
    return {
        "row_id": f"{pool}-{token}",
        "work_id": token,
        "split": "audit_only",
        "family": "bridge",
        "review_pool_variant": pool,
        "ranking_run_id": "rank-5a7efa5ca3",
        "bridge_recommendable": label,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v14",
        "rows": [
            _label_row("W1", pool="ml_bridge_shadow_pilot_audit", label=True),
            _label_row("W2", pool="ml_bridge_negative_mining_audit", label=False),
            _label_row("W2", pool="ml_bridge_top_ranked_validation_audit", label=True),
            _label_row("W3", pool="ml_bridge_shadow_pilot_audit", label=True),
            _label_row("W4", pool="ml_bridge_shadow_pilot_audit", label=False),
            _label_row("W6", pool="ml_bridge_shadow_pilot_audit", label=True),
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
                "target": "bridge_recommendable",
                "total_labeled_rows": 60,
                "positive_count": 34,
                "negative_count": 26,
                "paper_scores_joinable_count": 60,
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


def _write_inputs(tmp_path: Path) -> dict[str, Path]:
    shadow = tmp_path / "shadow.json"
    shadow.write_text(json.dumps(_shadow_pilot_payload()), encoding="utf-8")
    sens = tmp_path / "sensitivity.json"
    sens.write_text(json.dumps(_sensitivity_payload()), encoding="utf-8")
    rank_pct = tmp_path / "rank_pct.json"
    rank_pct.write_text(json.dumps(_rank_pct_eval_payload()), encoding="utf-8")
    label = tmp_path / "labels.json"
    label.write_text(json.dumps(_label_payload()), encoding="utf-8")
    readiness = tmp_path / "readiness.json"
    readiness.write_text(json.dumps(_readiness_payload(sha256_file(label))), encoding="utf-8")
    embeddings = tmp_path / "embeddings.json"
    embeddings.write_text(json.dumps(_embeddings_payload()), encoding="utf-8")
    return {
        "shadow": shadow,
        "sensitivity": sens,
        "rank_pct": rank_pct,
        "label": label,
        "readiness": readiness,
        "embeddings": embeddings,
    }


def _build_payload(tmp_path: Path) -> dict:
    paths = _write_inputs(tmp_path)
    with patch(
        "pipeline.ml_bridge_rank_pct_hybrid_controlled_rollout_eval._score_pool_ml_probabilities",
        return_value=_fake_ml_probs(),
    ) as scorer:
        payload = build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload(
            shadow_pilot_artifact_path=paths["shadow"],
            sensitivity_artifact_path=paths["sensitivity"],
            rank_pct_eval_artifact_path=paths["rank_pct"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
            database_url="postgres://example",
            expected_candidate_count=8,
            top_k=3,
        )
    assert scorer.called
    return payload


def test_prerequisite_artifacts_reject_wrong_version_or_not_ready(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    paths["sensitivity"].write_text(json.dumps(_sensitivity_payload(ready=False)), encoding="utf-8")
    with pytest.raises(MLOfflineBridgeHybridEvalV3Error, match="ready_for_offline_hybrid_eval"):
        build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload(
            shadow_pilot_artifact_path=paths["shadow"],
            sensitivity_artifact_path=paths["sensitivity"],
            rank_pct_eval_artifact_path=paths["rank_pct"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
            database_url="postgres://example",
            expected_candidate_count=8,
            top_k=3,
        )

    with pytest.raises(MLBridgeRankPctHybridControlledRolloutEvalError, match="eval_version"):
        _validate_rank_pct_eval_artifact(
            _rank_pct_eval_payload(eval_version="wrong"),
            path=Path("rank_pct.json"),
        )


def test_readiness_matrix_sha_must_match_label_dataset(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    paths["readiness"].write_text(json.dumps(_readiness_payload("wrong-sha")), encoding="utf-8")
    with patch(
        "pipeline.ml_bridge_rank_pct_hybrid_controlled_rollout_eval._score_pool_ml_probabilities",
        return_value=_fake_ml_probs(),
    ), pytest.raises(MLOfflineBridgeHybridEvalV3Error, match="label_dataset_sha256"):
        build_ml_bridge_rank_pct_hybrid_controlled_rollout_eval_payload(
            shadow_pilot_artifact_path=paths["shadow"],
            sensitivity_artifact_path=paths["sensitivity"],
            rank_pct_eval_artifact_path=paths["rank_pct"],
            label_dataset_path=paths["label"],
            readiness_matrix_path=paths["readiness"],
            embeddings_provenance_path=paths["embeddings"],
            database_url="postgres://example",
            expected_candidate_count=8,
            top_k=3,
        )


def test_label_overlay_uses_work_id_and_shadow_top_negative_priority() -> None:
    overlay = _label_overlay_by_work_id(
        {
            "dataset_version": "ml-label-dataset-v14",
            "rows": [
                _label_row("W4415316343", pool="ml_bridge_negative_mining_audit", label=True),
                _label_row("W4415316343", pool="ml_bridge_top_ranked_validation_audit", label=True),
                _label_row("W4415316343", pool="ml_bridge_shadow_pilot_audit", label=False),
            ],
        }
    )
    assert overlay["W4415316343"]["bridge_recommendable"] is False
    assert overlay["W4415316343"]["label_source_review_pool_variant"] == "ml_bridge_shadow_pilot_audit"


def test_recomputes_rank_percentiles_and_ignores_stale_shadow_pilot_fields(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path)
    assert payload["artifact_type"] == ARTIFACT_TYPE
    assert payload["artifact_version"] == ARTIFACT_VERSION
    row_w4 = next(row for row in payload["candidate_scores"] if row["work_id_token"] == "W4")
    assert row_w4["historical_shadow_pilot_hybrid_rank"] == 99
    assert row_w4["hybrid_rank_alpha_0_5"] == 1
    assert payload["scoring"]["stale_shadow_pilot_fields_ignored"]["recomputed_hybrid_rank_diff_count"] > 0
    assert payload["scoring"]["stale_shadow_pilot_fields_ignored"][
        "example_promoted_recomputed_ml_probability_diff"
    ] is not None


def test_rejects_missing_bridge_score_and_wrong_ranking_run() -> None:
    candidates = _candidates()
    candidates[0]["bridge_score"] = None
    with pytest.raises(MLBridgeRankPctHybridControlledRolloutEvalError, match="bridge_score coverage"):
        _validate_shadow_pilot_artifact(
            _shadow_pilot_payload(candidates=candidates),
            path=Path("shadow.json"),
            expected_candidate_count=8,
        )
    with pytest.raises(MLBridgeRankPctHybridControlledRolloutEvalError, match="ranking_run_id"):
        _validate_shadow_pilot_artifact(
            _shadow_pilot_payload(ranking_run_id="wrong"),
            path=Path("shadow.json"),
            expected_candidate_count=8,
        )


def test_current_proposed_promoted_demoted_stable_sets_are_correct(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path)
    comparison = payload["top20_comparison_by_alpha"]["alpha_0_5"]
    assert {row["work_id_token"] for row in comparison["current_top20"]} == {"W1", "W2", "W3"}
    assert {row["work_id_token"] for row in comparison["proposed_top20"]} == {"W1", "W4", "W5"}
    assert {row["work_id_token"] for row in comparison["stable"]} == {"W1"}
    assert {row["work_id_token"] for row in comparison["promoted"]} == {"W4", "W5"}
    assert {row["work_id_token"] for row in comparison["demoted"]} == {"W2", "W3"}
    assert comparison["churn_count"] == 2
    assert comparison["churn_fraction"] == 2 / 3


def test_label_overlay_counts_positives_negatives_and_unlabeled() -> None:
    rows = [{"bridge_recommendable": True}, {"bridge_recommendable": False}, {"bridge_recommendable": None}]
    summary = _group_summary(rows)
    assert summary["labeled_positive_count"] == 1
    assert summary["labeled_negative_count"] == 1
    assert summary["unlabeled_count"] == 1
    assert summary["labeled_precision"] == 0.5


def test_promoted_unlabeled_high_risk_thresholds_work() -> None:
    promoted = [
        {
            "work_id_token": "W1",
            "work_id_int": 1,
            "title": "x",
            "current_family_rank": 9,
            "bridge_recommendable": None,
            "v3_ml_probability": HIGH_RISK_ML_PROB_THRESHOLD - 0.01,
            "v3_ml_rank_pct": 0.5,
            "bridge_score_raw": 1.0,
            "bridge_score_rank_pct": 1.0,
            "hybrid_rank_alpha_0_5": 1,
            "hybrid_rank_pct_score_alpha_0_5": 1.0,
        }
    ]
    risk = _risk_readouts(promoted_rows=promoted, demoted_rows=[], stable_rows=[], alpha=0.5)
    assert risk["promoted_unlabeled_high_risk_count"] == 1
    assert risk["risk_thresholds"]["promoted_unlabeled_high_risk_v3_ml_rank_pct_lt"] == HIGH_RISK_ML_RANK_PCT_THRESHOLD


def test_competitive_vs_clear_loss_demotion_classification() -> None:
    demoted = [
        {"work_id_token": "W1", "work_id_int": 1, "title": "a", "current_family_rank": 1, "bridge_recommendable": True, "v3_ml_probability": 0.9, "v3_ml_rank_pct": 0.9, "bridge_score_raw": 0.1, "bridge_score_rank_pct": 0.1, "hybrid_rank_alpha_0_5": 5, "hybrid_rank_pct_score_alpha_0_5": 0.5},
        {"work_id_token": "W2", "work_id_int": 2, "title": "b", "current_family_rank": 2, "bridge_recommendable": True, "v3_ml_probability": 0.1, "v3_ml_rank_pct": 0.1, "bridge_score_raw": 0.1, "bridge_score_rank_pct": 0.1, "hybrid_rank_alpha_0_5": 6, "hybrid_rank_pct_score_alpha_0_5": 0.1},
        {"work_id_token": "W3", "work_id_int": 3, "title": "c", "current_family_rank": 3, "bridge_recommendable": False, "v3_ml_probability": 0.5, "v3_ml_rank_pct": 0.5, "bridge_score_raw": 0.1, "bridge_score_rank_pct": 0.1, "hybrid_rank_alpha_0_5": 7, "hybrid_rank_pct_score_alpha_0_5": 0.2},
    ]
    risk = _risk_readouts(promoted_rows=[], demoted_rows=demoted, stable_rows=[], alpha=0.5)
    classes = {row["work_id_token"]: row["demotion_classification"] for row in risk["demoted_labeled_positives"]}
    assert classes == {"W1": "competitive", "W2": "clear_loss"}


def test_decision_logic_uses_primary_alpha_only_shape() -> None:
    comparison = {
        "group_summaries": {
            "current_top20": {"labeled_precision": 0.5},
            "proposed_top20": {"labeled_precision": 0.75},
        },
        "risk_readouts": {
            "promoted_labeled_negatives_count": 0,
            "promoted_unlabeled_high_risk_count": 0,
            "demoted_labeled_positive_clear_loss_count": 0,
        },
    }
    stage, _, passed = _recommended_next_stage_for_comparison(comparison)
    assert stage == "draft_bridge_rank_pct_hybrid_serving_plan_v1"
    assert passed is True
    comparison["risk_readouts"]["promoted_labeled_negatives_count"] = 1
    stage, _, passed = _recommended_next_stage_for_comparison(comparison)
    assert stage == "collect_bridge_rollout_review_labels_before_serving"
    assert passed is False


def test_markdown_contains_promoted_demoted_and_caveats(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path)
    md = markdown_from_ml_bridge_rank_pct_hybrid_controlled_rollout_eval(payload)
    assert "SHA256" in md
    assert "Arm Comparison" in md
    assert "Verdict Table" in md
    assert "Promoted" in md
    assert "Demoted" in md
    assert "Caveats" in md


def test_module_has_no_api_web_or_shadow_runs_imports() -> None:
    import pipeline.ml_bridge_rank_pct_hybrid_controlled_rollout_eval as mod

    source = Path(mod.__file__).read_text(encoding="utf-8")
    assert "apps.api" not in source
    assert "apps.web" not in source
    assert "shadow-runs" not in source
