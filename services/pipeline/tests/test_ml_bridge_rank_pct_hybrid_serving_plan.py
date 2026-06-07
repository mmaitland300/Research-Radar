"""Tests for Bridge rank-pct hybrid serving plan artifact."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_bridge_rank_pct_hybrid_serving_plan import (
    ARTIFACT_TYPE,
    PLAN_VERSION,
    MLBridgeRankPctHybridServingPlanError,
    build_ml_bridge_rank_pct_hybrid_serving_plan_payload,
    markdown_from_ml_bridge_rank_pct_hybrid_serving_plan,
)


EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"


def _shadow_payload() -> dict:
    return {
        "artifact_type": "ml_bridge_shadow_pilot",
        "pilot_version": "ml-bridge-shadow-pilot-v1",
        "ranking_run_id": "rank-5a7efa5ca3",
        "embedding_version": EMBEDDING_VERSION,
        "candidate_count": 528,
        "bridge_score_coverage": {
            "non_null_count": 528,
            "total_count": 528,
            "coverage_fraction": 1.0,
        },
    }


def _controlled_payload(shadow_path: Path, **overrides: object) -> dict:
    payload = {
        "artifact_type": "ml_bridge_rank_pct_hybrid_controlled_rollout_eval",
        "artifact_version": "ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1",
        "controlled_rollout_eval_ready": True,
        "recommended_next_stage": "draft_bridge_rank_pct_hybrid_serving_plan_v1",
        "ranking_run_id": "rank-5a7efa5ca3",
        "selected_frozen_coefficient_C": 0.001,
        "primary_alpha": 0.5,
        "exploratory_alpha": 0.7,
        "rank_percentile_scope": "full_bridge_candidate_pool",
        "inputs": [
            {
                "name": "shadow_pilot_artifact",
                "path": shadow_path.as_posix(),
                "sha256": sha256_file(shadow_path),
            }
        ],
        "prerequisite_checks": {
            "candidate_count": 528,
            "bridge_score_coverage": "528/528",
        },
        "primary_alpha_0_5_summary": {
            "alpha": 0.5,
            "churn_count": 20,
            "churn_fraction": 1.0,
            "group_summaries": {
                "current_top20": {
                    "row_count": 20,
                    "labeled_count": 20,
                    "labeled_positive_count": 8,
                    "labeled_negative_count": 12,
                    "unlabeled_count": 0,
                    "labeled_precision": 0.4,
                },
                "proposed_top20": {
                    "row_count": 20,
                    "labeled_count": 13,
                    "labeled_positive_count": 13,
                    "labeled_negative_count": 0,
                    "unlabeled_count": 7,
                    "labeled_precision": 1.0,
                },
            },
            "risk_readouts": {
                "promoted_labeled_negatives_count": 0,
                "promoted_unlabeled_high_risk_count": 0,
                "demoted_labeled_positive_clear_loss_count": 0,
                "demoted_labeled_positive_competitive_count": 8,
            },
            "top20_quality_delta_labeled_only": 0.6,
        },
    }
    payload.update(overrides)
    return payload


def _rank_pct_payload(**overrides: object) -> dict:
    payload = {
        "artifact_type": "ml_offline_bridge_hybrid_rank_pct_eval_v3",
        "eval_version": "ml-offline-bridge-hybrid-rank-pct-eval-v3-v1",
        "rank_percentile_scope": "full_bridge_candidate_pool",
        "recommended_next_stage": "authorize_bridge_hybrid_serving_controlled_rollout_eval",
        "selected_frozen_coefficient_C": 0.001,
        "pool_candidate_count": 528,
        "primary_hybrid_alpha": 0.5,
    }
    payload.update(overrides)
    return payload


def _linear_hybrid_payload(**overrides: object) -> dict:
    payload = {
        "artifact_type": "ml_offline_bridge_hybrid_eval_v3",
        "eval_version": "ml-offline-bridge-hybrid-eval-v3-v1",
        "recommended_next_stage": "do_not_authorize_bridge_hybrid_serving_recheck_alpha_or_formula",
        "selected_frozen_coefficient_C": 0.001,
        "primary_hybrid_alpha": 0.5,
        "target": "bridge_recommendable",
    }
    payload.update(overrides)
    return payload


def _sensitivity_payload(**overrides: object) -> dict:
    payload = {
        "artifact_type": "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity",
        "artifact_version": "ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1",
        "ready_for_offline_hybrid_eval": True,
        "selected_frozen_coefficient_C": 0.001,
        "selected_frozen_scorer": {
            "embedding_version": EMBEDDING_VERSION,
            "C": 0.001,
            "scaler_mean": [0.0],
            "scaler_scale": [1.0],
            "coef": [1.0],
            "intercept": 0.0,
        },
    }
    payload.update(overrides)
    return payload


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v14",
        "rows": [],
    }


def _readiness_payload(label_sha: str) -> dict:
    return {
        "artifact_type": "ml_label_readiness_matrix",
        "provenance": {
            "label_dataset_version": "ml-label-dataset-v14",
            "label_dataset_sha256": label_sha,
        },
        "groups": [],
    }


def _embeddings_payload() -> dict:
    return {
        "metadata": {
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "embedding_version": EMBEDDING_VERSION,
        },
        "coverage": {
            "embedded_work_count": 528,
            "missing_embedding_count": 0,
            "snapshot_work_count": 528,
        },
    }


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_inputs(tmp_path: Path) -> dict[str, Path]:
    shadow = tmp_path / "shadow.json"
    _write_json(shadow, _shadow_payload())
    controlled = tmp_path / "controlled.json"
    _write_json(controlled, _controlled_payload(shadow))
    rank_pct = tmp_path / "rank_pct.json"
    _write_json(rank_pct, _rank_pct_payload())
    linear = tmp_path / "linear.json"
    _write_json(linear, _linear_hybrid_payload())
    sensitivity = tmp_path / "sensitivity.json"
    _write_json(sensitivity, _sensitivity_payload())
    label = tmp_path / "labels.json"
    _write_json(label, _label_payload())
    readiness = tmp_path / "readiness.json"
    _write_json(readiness, _readiness_payload(sha256_file(label)))
    embeddings = tmp_path / "embeddings.json"
    _write_json(embeddings, _embeddings_payload())
    return {
        "controlled": controlled,
        "rank_pct": rank_pct,
        "linear": linear,
        "sensitivity": sensitivity,
        "label": label,
        "readiness": readiness,
        "embeddings": embeddings,
        "shadow": shadow,
    }


def _build(paths: dict[str, Path]) -> dict:
    return build_ml_bridge_rank_pct_hybrid_serving_plan_payload(
        controlled_rollout_eval_path=paths["controlled"],
        rank_pct_eval_artifact_path=paths["rank_pct"],
        linear_hybrid_eval_v3_path=paths["linear"],
        sensitivity_artifact_path=paths["sensitivity"],
        label_dataset_path=paths["label"],
        readiness_matrix_path=paths["readiness"],
        embeddings_provenance_path=paths["embeddings"],
    )


def _rewrite_controlled(paths: dict[str, Path], payload: dict) -> None:
    _write_json(paths["controlled"], payload)


def test_rejects_controlled_rollout_eval_wrong_next_stage(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _rewrite_controlled(
        paths,
        _controlled_payload(paths["shadow"], recommended_next_stage="collect_more_labels"),
    )
    with pytest.raises(MLBridgeRankPctHybridServingPlanError, match="recommended_next_stage"):
        _build(paths)


def test_rejects_controlled_rollout_eval_not_ready(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _rewrite_controlled(
        paths,
        _controlled_payload(paths["shadow"], controlled_rollout_eval_ready=False),
    )
    with pytest.raises(MLBridgeRankPctHybridServingPlanError, match="controlled_rollout_eval_ready"):
        _build(paths)


@pytest.mark.parametrize(
    "risk_key",
    [
        "promoted_labeled_negatives_count",
        "promoted_unlabeled_high_risk_count",
        "demoted_labeled_positive_clear_loss_count",
    ],
)
def test_rejects_primary_alpha_risk_readout_counts(tmp_path: Path, risk_key: str) -> None:
    paths = _write_inputs(tmp_path)
    controlled = _controlled_payload(paths["shadow"])
    controlled["primary_alpha_0_5_summary"]["risk_readouts"][risk_key] = 1
    _rewrite_controlled(paths, controlled)
    with pytest.raises(MLBridgeRankPctHybridServingPlanError, match=risk_key):
        _build(paths)


def test_rejects_rank_pct_eval_wrong_next_stage(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _write_json(
        paths["rank_pct"],
        _rank_pct_payload(recommended_next_stage="collect_more_rank_pct_labels"),
    )
    with pytest.raises(MLBridgeRankPctHybridServingPlanError, match="recommended_next_stage"):
        _build(paths)


def test_rejects_sensitivity_artifact_not_ready(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _write_json(paths["sensitivity"], _sensitivity_payload(ready_for_offline_hybrid_eval=False))
    with pytest.raises(MLBridgeRankPctHybridServingPlanError, match="ready_for_offline_hybrid_eval"):
        _build(paths)


def test_verifies_selected_frozen_coefficient_c(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _write_json(paths["sensitivity"], _sensitivity_payload(selected_frozen_coefficient_C=1.0))
    with pytest.raises(MLBridgeRankPctHybridServingPlanError, match="selected_frozen_coefficient_C"):
        _build(paths)


def test_serving_scope_and_pinned_context_fields(tmp_path: Path) -> None:
    payload = _build(_write_inputs(tmp_path))
    assert payload["artifact_type"] == ARTIFACT_TYPE
    assert payload["plan_version"] == PLAN_VERSION
    assert payload["preconditions"]["linear_hybrid_eval_included_as_negative_guardrail"] is True
    assert payload["serving_scope"]["route"] == "/api/v1/recommendations/ranked"
    assert payload["serving_scope"]["family"] == "bridge"
    assert payload["serving_scope"]["ranking_run_id"] == "rank-5a7efa5ca3"
    assert payload["serving_scope"]["limit"] == 20
    assert payload["serving_scope"]["no_emerging_changes"] is True
    assert payload["serving_scope"]["no_undercited_changes"] is True
    assert payload["serving_scope"]["scoring_scope"]["score_only_top20"] is False
    assert payload["pinned_run_context"]["ranking_run_id"] == "rank-5a7efa5ca3"
    assert payload["pinned_run_context"]["embedding_version"] == EMBEDDING_VERSION
    assert payload["pinned_run_context"]["candidate_count"] == 528
    assert payload["pinned_run_context"]["bridge_score_coverage"]["as_text"] == "528/528"


def test_default_fail_closed_env_contract_is_documented(tmp_path: Path) -> None:
    payload = _build(_write_inputs(tmp_path))
    env = payload["required_future_env"]
    assert env["variables"]["ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED"].startswith("missing/false")
    assert env["variables"]["ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_PERCENT"].startswith("missing/zero")
    assert env["variables"]["ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP"].startswith("missing/zero")
    assert env["variables"]["ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST"].startswith("missing/empty")
    assert env["example_pinned_ranking_run_env"] == "ML_BRIDGE_SCORER_V1_RANKING_RUN_ID=rank-5a7efa5ca3"
    assert payload["bridge_gate_contract"]["do_not_extend_env_prefix"] == "ML_SHADOW_SCORER_V1_"


def test_ranked_ranking_mode_future_contract_includes_bridge_mode(tmp_path: Path) -> None:
    payload = _build(_write_inputs(tmp_path))
    modes = payload["api_response_contract"]["RankedRankingMode_future_values"]
    assert modes == [
        "materialized_heuristic",
        "bounded_ml_scorer",
        "bounded_bridge_ml_scorer",
    ]
    assert payload["api_response_contract"]["bridge_ranking_mode"] == "bounded_bridge_ml_scorer"
    assert payload["api_response_contract"]["do_not_reuse_bounded_ml_scorer_for_bridge"] is True


def test_recommended_next_stage_and_markdown(tmp_path: Path) -> None:
    payload = _build(_write_inputs(tmp_path))
    assert payload["recommended_next_stage"] == "implement_bridge_rank_pct_hybrid_serving_gate_v1"
    md = markdown_from_ml_bridge_rank_pct_hybrid_serving_plan(payload)
    assert "Caveats" in md
    assert "recommended_next_stage" in md
    assert "implement_bridge_rank_pct_hybrid_serving_gate_v1" in md
    assert "Web copy must not call Bridge \"validated.\"" in md
