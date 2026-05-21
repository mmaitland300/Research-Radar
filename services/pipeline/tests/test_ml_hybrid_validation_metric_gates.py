"""Tests for fresh-surface hybrid validation metric gates v1."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_hybrid_validation_metric_gates import (
    GATES_VERSION,
    MLHybridValidationMetricGatesError,
    build_ml_hybrid_validation_metric_gates_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


EXPECTED_FORMULAS = [
    ("heuristic_final_score_baseline", "final_score"),
    ("holdout_embedding_probability_baseline", "audit_embedding_probability_work"),
    (
        "hybrid_rank_mean_50_50",
        "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
    ),
    (
        "hybrid_rank_mean_75_25_heuristic",
        "0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)",
    ),
    (
        "hybrid_rank_mean_25_75_heuristic",
        "0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)",
    ),
]


def _pr(precision: float) -> dict:
    return {
        "precision": precision,
        "recall": 0.1,
        "labeled_work_count": 143,
        "positive_count": 54,
        "negative_count": 89,
        "top_k_labeled_positive_count": int(round(precision * 10)),
        "top_k_labeled_negative_count": 10 - int(round(precision * 10)),
    }


def _metrics(
    arm_id: str,
    roc_auc: float,
    average_precision: float,
    *,
    p5: float = 0.6,
    p10: float = 0.5,
    p20: float = 0.5,
) -> dict:
    return {
        "arm_id": arm_id,
        "roc_auc_mann_whitney": roc_auc,
        "average_precision": average_precision,
        "precision_recall_at_k": {"5": _pr(p5), "10": _pr(p10), "20": _pr(p20)},
        "positive_work_count": 54,
        "negative_work_count": 89,
        "scored_labeled_work_count": 143,
        "labeled_eval_subset_work_count": 143,
    }


def _validation_payload() -> dict:
    heuristic = _metrics(
        "heuristic_final_score_baseline",
        0.8252184769038702,
        0.649431822568543,
        p5=0.6,
        p10=0.5,
        p20=0.5,
    )
    holdout = _metrics(
        "holdout_embedding_probability_baseline",
        0.850187265917603,
        0.7513413290429989,
        p5=0.6,
        p10=0.7,
        p20=0.85,
    )
    primary = _metrics(
        "hybrid_rank_mean_50_50",
        0.9103204327923429,
        0.8616964696673468,
        p5=1.0,
        p10=1.0,
        p20=0.95,
    )
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_validation_on_fresh_surface",
            "validation_version": "ml-hybrid-validation-on-fresh-surface-v1",
            "candidate_pool_work_set_sha256": "fresh-sha",
        },
        "recommended_next_stage": "run_hybrid_validation_metric_gates_v1",
        "candidate_eval_coverage": {
            "candidate_pool_work_count": 358,
            "candidate_pool_work_set_sha256": "fresh-sha",
            "confirmatory_metric_work_count": 143,
            "confirmatory_positive_work_count": 54,
            "confirmatory_negative_work_count": 89,
        },
        "pre_registered_arms_executed": [
            {"arm_id": arm_id, "score_formula": formula} for arm_id, formula in EXPECTED_FORMULAS
        ],
        "arm_metrics": {
            "heuristic_final_score_baseline": heuristic,
            "holdout_embedding_probability_baseline": holdout,
            "hybrid_rank_mean_50_50": primary,
            "hybrid_rank_mean_75_25_heuristic": _metrics(
                "hybrid_rank_mean_75_25_heuristic", 0.89, 0.80, p5=0.8, p10=0.9, p20=0.8
            ),
            "hybrid_rank_mean_25_75_heuristic": _metrics(
                "hybrid_rank_mean_25_75_heuristic", 0.88, 0.79, p5=0.8, p10=0.8, p20=0.75
            ),
        },
        "comparisons_vs_heuristic": {
            "hybrid_rank_mean_50_50": {
                "delta_roc_auc": 0.08510195588847269,
                "delta_average_precision": 0.21226464709880377,
                "material_lift_passed_against_heuristic": True,
            }
        },
        "confirmatory_decision_inputs": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "primary_arm_comparison_vs_heuristic": {
                "delta_roc_auc": 0.08510195588847269,
                "delta_average_precision": 0.21226464709880377,
                "delta_precision_at_5": 0.4,
                "delta_precision_at_10": 0.5,
                "delta_precision_at_20": 0.44999999999999996,
                "material_lift_passed_against_heuristic": True,
            },
            "confirmatory_metrics_ready_for_gates": True,
            "best_arm_selection_is_exploratory_only": True,
            "confirmatory_validation_passed": False,
            "confirmatory_validation_passed_reason": "metric_gates_not_run",
            "best_arm_by_roc_auc": {"arm_id": "hybrid_rank_mean_50_50", "roc_auc_mann_whitney": 0.9103204327923429},
            "best_arm_by_average_precision": {"arm_id": "hybrid_rank_mean_50_50", "average_precision": 0.8616964696673468},
        },
        "summary": {
            "confirmatory_validation_passed": False,
            "confirmatory_validation_passed_reason": "metric_gates_not_run",
            "best_arm_selection_is_exploratory_only": True,
            "best_arm_by_roc_auc": {"arm_id": "hybrid_rank_mean_50_50", "roc_auc_mann_whitney": 0.9103204327923429},
            "best_arm_by_average_precision": {"arm_id": "hybrid_rank_mean_50_50", "average_precision": 0.8616964696673468},
        },
        "label_join_summary": {
            "confirmatory_labeled_work_count": 143,
            "confirmatory_positive_work_count": 54,
            "confirmatory_negative_work_count": 89,
        },
        "leakage_report": {
            "old_217_overlap_excluded_from_confirmatory_metrics": True,
            "confirmatory_rows_with_previous_eval_overlap_count": 0,
            "supervised_fit_used": False,
            "eval_label_weight_tuning_used": False,
            "scorer_refit_used": False,
        },
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
    }


def _surface_payload() -> dict:
    threshold_check = {
        "minimum_candidate_work_count": {"observed": 143, "threshold": 100, "passed": True},
        "minimum_confirmatory_labeled_work_count": {"observed": 143, "threshold": 100, "passed": True},
        "minimum_confirmatory_label_coverage_rate": {"observed": 1.0, "threshold": 0.6, "passed": True},
        "minimum_confirmatory_positive_work_count": {"observed": 54, "threshold": 50, "passed": True},
        "minimum_confirmatory_negative_work_count": {"observed": 89, "threshold": 20, "passed": True},
        "minimum_distinct_negative_work_count": {"observed": 89, "threshold": 20, "passed": True},
    }
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "status": "materialized_ready",
            "label_dataset_version": "ml-label-dataset-v10",
            "expected_label_dataset_version": "ml-label-dataset-v10",
        },
        "ready_for_hybrid_validation_scoring": True,
        "candidate_pool": {
            "candidate_work_count": 358,
            "candidate_work_set_sha256": "fresh-sha",
        },
        "threshold_check": threshold_check,
    }


def _policy_payload(*, include_thresholds: bool = True) -> dict:
    policy = {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": "old-sha",
        },
        "frozen_hybrid_arms": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
        },
    }
    if include_thresholds:
        policy["gate_linkage"] = {
            "material_lift_thresholds": {
                "delta_roc_auc_gte": 0.03,
                "or_delta_average_precision_gte": 0.02,
            }
        }
    return policy


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
        },
        "targets": {"good_or_acceptable": {"production_eligible": False}},
        "production_default_authorized": False,
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment_spec",
            "spec_version": "ml-hybrid-scorer-offline-experiment-v1-spec",
        },
        "pre_registered_hybrid_arms": [
            {"arm_id": arm_id, "score_formula": formula} for arm_id, formula in EXPECTED_FORMULAS
        ],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    validation: dict | None = None,
    surface: dict | None = None,
    policy: dict | None = None,
    plan: dict | None = None,
    spec: dict | None = None,
    prior_gates: dict | None = None,
) -> dict[str, Path]:
    out = {
        "hybrid_validation_on_fresh_surface_path": _write_json(tmp_path, "validation.json", validation or _validation_payload()),
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "plan.json", plan or _production_plan_payload()),
    }
    if spec is not None:
        out["hybrid_experiment_spec_path"] = _write_json(tmp_path, "spec.json", spec)
    if prior_gates is not None:
        out["hybrid_scorer_metric_gates_path"] = _write_json(tmp_path, "prior-gates.json", prior_gates)
    return out


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_hybrid_validation_metric_gates_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-20T00:00:00Z",
    )


def _gate(payload: dict, gate_id: str) -> dict:
    return next(gate for gate in payload["gates"] if gate["gate_id"] == gate_id)


def test_happy_path_primary_lift_passes_and_recommends_shadow_spec(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_hybrid_validation_metric_gates"
    assert payload["metadata"]["gates_version"] == GATES_VERSION
    assert payload["primary_confirmatory_arm"] == "hybrid_rank_mean_50_50"
    assert payload["primary_hybrid_material_lift_passed"] is True
    assert payload["confirmatory_validation_passed"] is True
    assert payload["recommended_next_stage"] == "draft_ml_shadow_scorer_v1_spec"
    assert _gate(payload, "G06_primary_material_lift_vs_heuristic")["status"] == "pass"


def test_fails_if_fresh_surface_not_materialized_ready(tmp_path: Path) -> None:
    surface = _surface_payload()
    surface["metadata"]["status"] = "materialized_needs_labels"

    with pytest.raises(MLHybridValidationMetricGatesError, match="materialized_ready"):
        _build(tmp_path, surface=surface)


def test_fails_if_ready_for_hybrid_validation_scoring_false(tmp_path: Path) -> None:
    surface = _surface_payload()
    surface["ready_for_hybrid_validation_scoring"] = False

    with pytest.raises(MLHybridValidationMetricGatesError, match="ready_for_hybrid_validation_scoring"):
        _build(tmp_path, surface=surface)


def test_fails_if_candidate_sha_equals_disallowed_old_217_sha(tmp_path: Path) -> None:
    validation = _validation_payload()
    surface = _surface_payload()
    policy = _policy_payload()
    validation["metadata"]["candidate_pool_work_set_sha256"] = "old-sha"
    validation["candidate_eval_coverage"]["candidate_pool_work_set_sha256"] = "old-sha"
    surface["candidate_pool"]["candidate_work_set_sha256"] = "old-sha"

    payload = _build(tmp_path, validation=validation, surface=surface, policy=policy)

    assert _gate(payload, "G03_leakage_and_freshness")["status"] == "fail"
    assert payload["confirmatory_validation_passed"] is False


def test_fails_if_primary_arm_is_not_frozen_50_50(tmp_path: Path) -> None:
    validation = _validation_payload()
    validation["confirmatory_decision_inputs"]["primary_confirmatory_arm"] = "hybrid_rank_mean_25_75_heuristic"

    with pytest.raises(MLHybridValidationMetricGatesError, match="primary_confirmatory_arm"):
        _build(tmp_path, validation=validation)


def test_fails_if_primary_material_lift_does_not_clear_threshold(tmp_path: Path) -> None:
    validation = _validation_payload()
    validation["arm_metrics"]["hybrid_rank_mean_50_50"]["roc_auc_mann_whitney"] = 0.84
    validation["arm_metrics"]["hybrid_rank_mean_50_50"]["average_precision"] = 0.66

    payload = _build(tmp_path, validation=validation)

    assert payload["primary_hybrid_material_lift_passed"] is False
    assert payload["confirmatory_validation_passed"] is False
    assert _gate(payload, "G06_primary_material_lift_vs_heuristic")["status"] == "fail"
    assert payload["recommended_next_stage"] == "collect_labels_features_or_new_eval_surface"


def test_uses_policy_thresholds_when_present_and_falls_back_when_absent(tmp_path: Path) -> None:
    strict_policy = _policy_payload()
    strict_policy["gate_linkage"]["material_lift_thresholds"]["delta_roc_auc_gte"] = 0.2
    strict_policy["gate_linkage"]["material_lift_thresholds"]["or_delta_average_precision_gte"] = 0.3
    strict_payload = _build(tmp_path / "strict", policy=strict_policy)

    fallback_payload = _build(tmp_path / "fallback", policy=_policy_payload(include_thresholds=False))

    assert strict_payload["metadata"]["thresholds"]["minimum_primary_delta_roc_auc_for_material_lift"] == 0.2
    assert strict_payload["primary_hybrid_material_lift_passed"] is False
    assert fallback_payload["metadata"]["thresholds"]["minimum_primary_delta_roc_auc_for_material_lift"] == 0.03
    assert fallback_payload["metadata"]["thresholds"]["minimum_primary_delta_average_precision_for_material_lift"] == 0.02
    assert fallback_payload["primary_hybrid_material_lift_passed"] is True


def test_fails_if_leakage_tuning_or_refit_flags_are_unsafe(tmp_path: Path) -> None:
    validation = _validation_payload()
    validation["leakage_report"]["eval_label_weight_tuning_used"] = True

    payload = _build(tmp_path, validation=validation)

    assert _gate(payload, "G03_leakage_and_freshness")["status"] == "fail"
    assert payload["confirmatory_validation_passed"] is False


def test_fails_if_best_arm_selection_not_exploratory(tmp_path: Path) -> None:
    validation = _validation_payload()
    validation["confirmatory_decision_inputs"]["best_arm_selection_is_exploratory_only"] = False

    with pytest.raises(MLHybridValidationMetricGatesError, match="best_arm_selection_is_exploratory_only"):
        _build(tmp_path, validation=validation)


def test_optional_experiment_spec_mismatch_fails(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["pre_registered_hybrid_arms"][2]["score_formula"] = "changed"

    with pytest.raises(MLHybridValidationMetricGatesError, match="pre-registered arms"):
        build_ml_hybrid_validation_metric_gates_payload(
            **_paths(tmp_path, spec=spec),
            repo_root=tmp_path,
            generated_at="2026-05-20T00:00:00Z",
        )


def test_confirmatory_pass_removes_not_complete_shadow_blocker(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["confirmatory_validation_passed"] is True
    assert "confirmatory_validation_not_complete" not in payload["shadow_blockers"]
    assert payload["shadow_and_production_blockers"]["confirmatory_validation_not_complete"] is False


def test_shadow_and_production_always_false(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False


def test_cli_writes_json_and_markdown_with_primary_flag(tmp_path: Path) -> None:
    paths = _paths(tmp_path, spec=_spec_payload(), prior_gates={"metadata": {"gates_version": "x"}})
    out_json = tmp_path / "gates.json"
    out_md = tmp_path / "gates.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-hybrid-validation-metric-gates",
        "--hybrid-validation-on-fresh-surface",
        str(paths["hybrid_validation_on_fresh_surface_path"]),
        "--fresh-eval-surface",
        str(paths["fresh_eval_surface_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--hybrid-experiment-spec",
        str(paths["hybrid_experiment_spec_path"]),
        "--hybrid-scorer-metric-gates",
        str(paths["hybrid_scorer_metric_gates_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["confirmatory_validation_passed"] is True
    assert "Hybrid Validation Metric Gates" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_hybrid_validation_metric_gates.py").read_text(
        encoding="utf-8"
    ).lower()
    test_source = Path(__file__).read_text(encoding="utf-8").lower()
    import_lines = "\n".join(
        line.strip()
        for line in (module_source + "\n" + test_source).splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    assert "psycopg" not in import_lines
    assert "postgres" not in import_lines
    assert "sklearn" not in import_lines
    assert "openai" not in import_lines
    assert "openalex" not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-hybrid-validation-metric-gates"')
    end = cli_source.index("ml_fresh_eval_labeling_plan_hybrid_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
