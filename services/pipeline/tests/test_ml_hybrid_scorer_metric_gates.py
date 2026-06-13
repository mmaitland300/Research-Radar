"""Tests for hybrid scorer metric gates v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_hybrid_scorer_metric_gates import (
    GATES_VERSION,
    MLHybridScorerMetricGatesError,
    build_ml_hybrid_scorer_metric_gates_payload,
)


EXPECTED_ARMS = [
    "heuristic_final_score_baseline",
    "holdout_embedding_probability_baseline",
    "hybrid_rank_mean_50_50",
    "hybrid_rank_mean_75_25_heuristic",
    "hybrid_rank_mean_25_75_heuristic",
]


def _pr_entry(precision: float = 1.0) -> dict:
    return {
        "precision": precision,
        "recall": 0.1,
        "reason": None,
        "labeled_work_count": 100,
        "positive_count": 90,
        "negative_count": 10,
    }


def _metrics(arm_id: str, roc_auc: float, average_precision: float, p10: float = 1.0) -> dict:
    return {
        "arm_id": arm_id,
        "roc_auc_mann_whitney": roc_auc,
        "average_precision": average_precision,
        "precision_recall_at_k": {
            "5": _pr_entry(1.0),
            "10": _pr_entry(p10),
            "20": _pr_entry(1.0),
        },
    }


def _experiment_payload() -> dict:
    arm_metrics = {
        "heuristic_final_score_baseline": _metrics("heuristic_final_score_baseline", 0.80, 0.95),
        "holdout_embedding_probability_baseline": _metrics("holdout_embedding_probability_baseline", 0.805, 0.955),
        "hybrid_rank_mean_50_50": _metrics("hybrid_rank_mean_50_50", 0.8425, 0.965),
        "hybrid_rank_mean_75_25_heuristic": _metrics("hybrid_rank_mean_75_25_heuristic", 0.81, 0.955),
        "hybrid_rank_mean_25_75_heuristic": _metrics("hybrid_rank_mean_25_75_heuristic", 0.82, 0.96),
    }
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment",
            "experiment_version": "ml-hybrid-scorer-offline-experiment-v1",
            "source_scoring_version": "ml-offline-production-candidate-scoring-v3",
            "source_metric_gates_version": "ml-offline-production-candidate-metric-gates-v3",
            "eval_work_set_sha256": "eval-sha",
        },
        "candidate_eval_coverage": {
            "candidate_pool_work_count": 100,
            "labeled_eval_metric_work_count": 100,
            "positive_work_prevalence": 0.90,
        },
        "pre_registered_arms_executed": [{"arm_id": arm_id} for arm_id in EXPECTED_ARMS],
        "arm_metrics": arm_metrics,
        "comparisons_vs_heuristic": {
            "holdout_embedding_probability_baseline": {
                "delta_roc_auc": 0.005,
                "delta_average_precision": 0.005,
                "delta_precision_at_10": 0.0,
                "material_lift_passed_against_heuristic": False,
            },
            "hybrid_rank_mean_50_50": {
                "delta_roc_auc": 0.0425,
                "delta_average_precision": 0.015,
                "delta_precision_at_10": 0.0,
                "material_lift_passed_against_heuristic": True,
            },
            "hybrid_rank_mean_75_25_heuristic": {
                "delta_roc_auc": 0.01,
                "delta_average_precision": 0.005,
                "delta_precision_at_10": 0.0,
                "material_lift_passed_against_heuristic": False,
            },
            "hybrid_rank_mean_25_75_heuristic": {
                "delta_roc_auc": 0.02,
                "delta_average_precision": 0.01,
                "delta_precision_at_10": 0.0,
                "material_lift_passed_against_heuristic": False,
            },
        },
        "summary": {
            "best_arm_selection_is_exploratory_only": True,
            "hybrid_material_lift_passed": True,
            "recommended_next_stage": "create_hybrid_scorer_metric_gates_v1",
        },
        "leakage_report": {
            "supervised_fit_used": False,
            "eval_label_weight_tuning_used": False,
        },
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment_spec",
            "spec_version": "ml-hybrid-scorer-offline-experiment-v1-spec",
            "eval_work_set_sha256": "eval-sha",
        },
        "future_gate_contract": {"best_arm_on_seen_eval_is_exploratory_only": True},
        "pre_registered_hybrid_arms": [{"arm_id": arm_id} for arm_id in EXPECTED_ARMS],
    }


def _product_candidate_gates_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v3",
            "eval_work_set_sha256": "eval-sha",
        },
        "independent_learned_validation_passed": True,
    }


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
        },
        "targets": {"good_or_acceptable": {"production_eligible": False}},
        "production_default_authorized": False,
    }


def _assignment_payload() -> dict:
    return {
        "metadata": {
            "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
            "eval_work_set_sha256": "eval-sha",
        },
        "leakage_report": {"global_zero_assertion": True},
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    experiment: dict | None = None,
    spec: dict | None = None,
    product_gates: dict | None = None,
    plan: dict | None = None,
    assignment: dict | None = None,
) -> dict[str, Path]:
    return {
        "hybrid_experiment_path": _write_json(tmp_path, "experiment.json", experiment or _experiment_payload()),
        "experiment_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "production_candidate_metric_gates_path": _write_json(
            tmp_path, "product-gates.json", product_gates or _product_candidate_gates_payload()
        ),
        "production_readiness_plan_path": _write_json(tmp_path, "plan.json", plan or _production_plan_payload()),
        "holdout_assignment_path": _write_json(tmp_path, "assignment.json", assignment or _assignment_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_hybrid_scorer_metric_gates_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def _gate(payload: dict, gate_id: str) -> dict:
    return next(gate for gate in payload["gates"] if gate["gate_id"] == gate_id)


def test_happy_path_hybrid_rank_mean_50_50_lift(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_hybrid_scorer_metric_gates"
    assert payload["metadata"]["gates_version"] == GATES_VERSION
    assert payload["hybrid_material_lift_passed"] is True
    assert payload["hybrid_arms_passing_material_lift"] == ["hybrid_rank_mean_50_50"]
    assert payload["recommended_next_stage"] == "create_fresh_eval_surface_for_hybrid_validation_v1"
    assert _gate(payload, "G04_hybrid_material_lift_vs_heuristic")["status"] == "pass"


def test_g04_false_when_no_hybrid_arm_clears_lift(tmp_path: Path) -> None:
    experiment = _experiment_payload()
    for arm_id in (
        "hybrid_rank_mean_50_50",
        "hybrid_rank_mean_75_25_heuristic",
        "hybrid_rank_mean_25_75_heuristic",
    ):
        experiment["comparisons_vs_heuristic"][arm_id]["delta_roc_auc"] = 0.01
        experiment["comparisons_vs_heuristic"][arm_id]["delta_average_precision"] = 0.01
        experiment["comparisons_vs_heuristic"][arm_id]["material_lift_passed_against_heuristic"] = False
    experiment["summary"]["hybrid_material_lift_passed"] = False

    payload = _build(tmp_path, experiment=experiment)

    assert payload["hybrid_material_lift_passed"] is False
    assert payload["hybrid_arms_passing_material_lift"] == []
    assert payload["recommended_next_stage"] == "collect_labels_or_features_or_new_eval_surface"
    assert _gate(payload, "G04_hybrid_material_lift_vs_heuristic")["status"] == "fail"


def test_baselines_do_not_trigger_hybrid_material_lift(tmp_path: Path) -> None:
    experiment = _experiment_payload()
    experiment["comparisons_vs_heuristic"]["holdout_embedding_probability_baseline"] = {
        "delta_roc_auc": 0.20,
        "delta_average_precision": 0.20,
        "material_lift_passed_against_heuristic": True,
    }
    for arm_id in (
        "hybrid_rank_mean_50_50",
        "hybrid_rank_mean_75_25_heuristic",
        "hybrid_rank_mean_25_75_heuristic",
    ):
        experiment["comparisons_vs_heuristic"][arm_id]["delta_roc_auc"] = 0.0
        experiment["comparisons_vs_heuristic"][arm_id]["delta_average_precision"] = 0.0
        experiment["comparisons_vs_heuristic"][arm_id]["material_lift_passed_against_heuristic"] = False

    payload = _build(tmp_path, experiment=experiment)

    assert payload["hybrid_material_lift_passed"] is False


def test_best_hybrid_arm_by_roc_auc_uses_hybrid_arms_only(tmp_path: Path) -> None:
    experiment = _experiment_payload()
    experiment["arm_metrics"]["holdout_embedding_probability_baseline"]["roc_auc_mann_whitney"] = 0.99

    payload = _build(tmp_path, experiment=experiment)

    assert payload["best_hybrid_arm_by_roc_auc"]["arm_id"] == "hybrid_rank_mean_50_50"


def test_g05_fails_if_best_arm_selection_not_exploratory(tmp_path: Path) -> None:
    experiment = _experiment_payload()
    experiment["summary"]["best_arm_selection_is_exploratory_only"] = False

    payload = _build(tmp_path, experiment=experiment)

    assert _gate(payload, "G05_best_arm_exploratory_only")["status"] == "fail"


def test_shadow_prod_and_confirmatory_are_always_false(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["confirmatory_validation_passed"] is False


def test_rejects_wrong_experiment_version(tmp_path: Path) -> None:
    experiment = _experiment_payload()
    experiment["metadata"]["experiment_version"] = "wrong"

    with pytest.raises(MLHybridScorerMetricGatesError, match="experiment_version"):
        _build(tmp_path, experiment=experiment)


def test_rejects_wrong_spec_version(tmp_path: Path) -> None:
    spec = _spec_payload()
    spec["metadata"]["spec_version"] = "wrong"

    with pytest.raises(MLHybridScorerMetricGatesError, match="spec_version"):
        _build(tmp_path, spec=spec)


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "gates.json"
    out_md = tmp_path / "gates.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-hybrid-scorer-metric-gates",
        "--hybrid-experiment",
        str(paths["hybrid_experiment_path"]),
        "--experiment-spec",
        str(paths["experiment_spec_path"]),
        "--production-candidate-metric-gates",
        str(paths["production_candidate_metric_gates_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--holdout-assignment",
        str(paths["holdout_assignment_path"]),
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
    assert data["metadata"]["gates_version"] == GATES_VERSION
    assert "Three-Decision Table" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_hybrid_scorer_metric_gates.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-hybrid-scorer-metric-gates"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
