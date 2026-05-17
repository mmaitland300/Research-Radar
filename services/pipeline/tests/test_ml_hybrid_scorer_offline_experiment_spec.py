"""Tests for hybrid scorer offline experiment spec v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_hybrid_scorer_offline_experiment_spec import (
    MLHybridScorerOfflineExperimentSpecError,
    SPEC_VERSION,
    build_ml_hybrid_scorer_offline_experiment_spec_payload,
    markdown_from_ml_hybrid_scorer_offline_experiment_spec,
    write_ml_hybrid_scorer_offline_experiment_spec,
)


def _scoring_payload(*, eval_sha: str = "eval-sha") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v3",
            "scoring_mode": "heuristic_and_holdout_embedding_scorer",
            "target": "good_or_acceptable",
            "ranking_run_id": "rank-fixture",
            "family": "emerging",
            "eval_work_set_sha256": eval_sha,
        },
        "candidate_pool_summary": {
            "candidate_unique_canonical_work_count": 4,
            "candidate_pool_work_set_sha256": eval_sha,
        },
        "label_join_summary": {
            "labeled_eval_subset_work_count": 4,
            "labeled_eval_subset_positive_work_count": 3,
            "labeled_eval_subset_negative_work_count": 1,
        },
        "leakage_report": {
            "train_rows_used_in_metrics": 0,
            "train_works_used_in_metrics": 0,
        },
        "scoring_mode_details": {"eval_only": True},
        "heuristic_metrics": {
            "roc_auc_mann_whitney": 0.8035087719298246,
            "average_precision": 0.9578865940621812,
            "precision_recall_at_k": {
                "5": {"precision": 1.0},
                "10": {"precision": 1.0},
                "20": {"precision": 1.0},
            },
        },
        "learned_or_embedding_metrics": {
            "eval_only": True,
            "metrics": {
                "roc_auc_mann_whitney": 0.8046783625730994,
                "average_precision": 0.9665979798704442,
                "precision_recall_at_k": {
                    "5": {"precision": 1.0},
                    "10": {"precision": 1.0},
                    "20": {"precision": 1.0},
                },
            },
            "comparison_to_heuristic": {
                "delta_roc_auc": 0.0011695906432748204,
                "delta_average_precision": 0.008711385808262917,
                "delta_precision_at_5": 0.0,
                "delta_precision_at_10": 0.0,
                "delta_precision_at_20": 0.0,
            },
        },
    }


def _gates_payload(
    *,
    eval_sha: str = "eval-sha",
    next_stage: str = "create_hybrid_scorer_offline_experiment_v1",
    material_lift_passed: bool = False,
    independent: bool = True,
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v3",
            "eval_work_set_sha256": eval_sha,
            "thresholds": {
                "minimum_delta_roc_auc_for_material_lift": 0.03,
                "minimum_delta_average_precision_for_material_lift": 0.02,
            },
        },
        "product_candidate_heuristic_gates_passed": True,
        "held_out_learned_validity_passed": True,
        "heuristic_non_regression_passed": True,
        "independent_learned_validation_passed": independent,
        "material_lift_passed": material_lift_passed,
        "recommended_next_stage": next_stage,
    }


def _assignment_payload(*, eval_sha: str = "eval-sha") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_assignment",
            "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
            "eval_work_set_sha256": eval_sha,
            "eval_work_count": 4,
        },
        "leakage_report": {
            "global_zero_assertion": True,
            "train_eval_work_overlap_count": 0,
        },
    }


def _split_policy_payload() -> dict:
    return {"metadata": {"artifact_type": "ml_label_split_policy", "policy_version": "ml-label-split-policy-v1"}}


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
        },
        "targets": {"good_or_acceptable": {"production_eligible": False}},
        "production_default_authorized": False,
    }


def _holdout_policy_payload(*, eval_sha: str = "eval-sha") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_policy",
            "policy_version": "ml-learned-scorer-holdout-policy-v1",
        },
        "dataset_inventory": {"product_candidate_eval_work_set_sha256": eval_sha},
        "primary_holdout_strategy": {"eval_work_set_definition": {"eval_work_set_sha256": eval_sha}},
    }


def _scorer_payload(*, eval_sha: str = "eval-sha") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_audit_embedding_scorer",
            "scorer_version": "ml-offline-audit-embedding-scorer-v2",
            "fit_mode": "holdout_bound_train_only",
            "eval_work_set_sha256": eval_sha,
        }
    }


def _label_payload() -> dict:
    return {"dataset_version": "ml-label-dataset-v8", "rows": []}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    scoring: dict | None = None,
    gates: dict | None = None,
    assignment: dict | None = None,
    holdout_policy: dict | None = None,
    scorer: dict | None = None,
    include_optional: bool = False,
) -> dict[str, Path]:
    paths = {
        "production_candidate_scoring_path": _write_json(tmp_path, "scoring-v3.json", scoring or _scoring_payload()),
        "production_candidate_metric_gates_path": _write_json(
            tmp_path,
            "metric-gates-v3.json",
            gates or _gates_payload(),
        ),
        "holdout_assignment_path": _write_json(
            tmp_path,
            "assignment.json",
            assignment or _assignment_payload(),
        ),
        "split_policy_path": _write_json(tmp_path, "split-policy.json", _split_policy_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "plan.json", _production_plan_payload()),
    }
    if include_optional or holdout_policy is not None:
        paths["holdout_policy_path"] = _write_json(
            tmp_path,
            "holdout-policy.json",
            holdout_policy or _holdout_policy_payload(),
        )
    if include_optional or scorer is not None:
        paths["audit_embedding_scorer_export_path"] = _write_json(
            tmp_path,
            "scorer-v2.json",
            scorer or _scorer_payload(),
        )
    if include_optional:
        paths["label_dataset_path"] = _write_json(tmp_path, "labels.json", _label_payload())
    return paths


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_hybrid_scorer_offline_experiment_spec_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def test_happy_path_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "hybrid-spec.json"
    out_md = tmp_path / "hybrid-spec.md"
    payload = write_ml_hybrid_scorer_offline_experiment_spec(
        **_paths(tmp_path, include_optional=True),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=tmp_path,
    )

    written = json.loads(out_json.read_text(encoding="utf-8"))
    md = out_md.read_text(encoding="utf-8")
    assert written["metadata"]["artifact_type"] == "ml_hybrid_scorer_offline_experiment_spec"
    assert written["metadata"]["spec_version"] == SPEC_VERSION
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert "Why Hybrid Is Next" in md
    assert "Not Shadow / Not Production" in md


def test_validates_gates_v3_recommended_next_stage(tmp_path: Path) -> None:
    gates = _gates_payload(next_stage="draft_ml_shadow_scorer_v1_spec")

    with pytest.raises(MLHybridScorerOfflineExperimentSpecError, match="recommended_next_stage"):
        _build(tmp_path, gates=gates)


def test_rejects_material_lift_passed_true(tmp_path: Path) -> None:
    gates = _gates_payload(material_lift_passed=True)

    with pytest.raises(MLHybridScorerOfflineExperimentSpecError, match="material_lift_passed"):
        _build(tmp_path, gates=gates)


def test_rejects_independent_validation_false(tmp_path: Path) -> None:
    gates = _gates_payload(independent=False)

    with pytest.raises(MLHybridScorerOfflineExperimentSpecError, match="independent_learned_validation_passed"):
        _build(tmp_path, gates=gates)


@pytest.mark.parametrize("mismatch", ["gates", "assignment"])
def test_rejects_scoring_gates_assignment_eval_sha_mismatch(tmp_path: Path, mismatch: str) -> None:
    gates = _gates_payload(eval_sha="wrong-sha") if mismatch == "gates" else None
    assignment = _assignment_payload(eval_sha="wrong-sha") if mismatch == "assignment" else None

    with pytest.raises(MLHybridScorerOfflineExperimentSpecError, match="eval_work_set_sha256"):
        _build(tmp_path, gates=gates, assignment=assignment)


def test_validates_optional_scorer_and_holdout_policy_when_supplied(tmp_path: Path) -> None:
    payload = _build(tmp_path, include_optional=True)

    input_names = {item["name"] for item in payload["metadata"]["inputs"]}
    assert {"holdout_policy", "audit_embedding_scorer_export", "label_dataset"}.issubset(input_names)
    assert payload["metadata"]["holdout_policy_version"] == "ml-learned-scorer-holdout-policy-v1"
    assert payload["metadata"]["audit_embedding_scorer_version"] == "ml-offline-audit-embedding-scorer-v2"


@pytest.mark.parametrize("optional_name", ["holdout_policy", "scorer"])
def test_rejects_optional_eval_sha_mismatch(tmp_path: Path, optional_name: str) -> None:
    holdout_policy = _holdout_policy_payload(eval_sha="wrong-sha") if optional_name == "holdout_policy" else None
    scorer = _scorer_payload(eval_sha="wrong-sha") if optional_name == "scorer" else None

    with pytest.raises(MLHybridScorerOfflineExperimentSpecError, match="eval_work_set_sha256"):
        _build(tmp_path, holdout_policy=holdout_policy, scorer=scorer)


def test_computes_material_lift_gaps_correctly(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    gaps = payload["evidence_summary"]["material_lift_gaps"]

    assert gaps["roc_auc_gap_to_material_lift"] == pytest.approx(0.03 - 0.0011695906432748204)
    assert gaps["average_precision_gap_to_material_lift"] == pytest.approx(0.02 - 0.008711385808262917)


def test_all_pre_registered_hybrid_arms_present(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    arm_ids = {arm["arm_id"] for arm in payload["pre_registered_hybrid_arms"]}

    assert arm_ids == {
        "heuristic_final_score_baseline",
        "holdout_embedding_probability_baseline",
        "hybrid_rank_mean_50_50",
        "hybrid_rank_mean_75_25_heuristic",
        "hybrid_rank_mean_25_75_heuristic",
    }


def test_feature_policy_rank_percentile_full_candidate_pool(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    transforms = payload["feature_policy"]["allowed_label_blind_transforms"]
    rank = next(item for item in transforms if item["name"] == "rank_percentile")

    assert rank["scope"] == "full candidate pool"
    assert rank["same_pool_as_scoring_v3"] is True


def test_forbidden_tuning_and_post_hoc_selection_are_in_json_and_markdown(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    md = markdown_from_ml_hybrid_scorer_offline_experiment_spec(payload)
    forbidden = " ".join(payload["forbidden_designs"] + payload["feature_policy"]["forbidden_features_or_methods"])

    assert "supervised hybrid on eval labels" in forbidden
    assert "post-hoc transforms chosen using eval-label performance" in forbidden
    assert "picking the best pre-registered arm" in forbidden
    assert "supervised hybrid on eval labels" in md
    assert "post-hoc transforms" in md


def test_future_gate_contract_marks_seen_eval_best_arm_exploratory(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["future_gate_contract"]["best_arm_on_seen_eval_is_exploratory_only"] is True


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path, include_optional=True)
    out_json = tmp_path / "hybrid-cli.json"
    out_md = tmp_path / "hybrid-cli.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-hybrid-scorer-offline-experiment-spec",
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--production-candidate-metric-gates",
        str(paths["production_candidate_metric_gates_path"]),
        "--holdout-assignment",
        str(paths["holdout_assignment_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--holdout-policy",
        str(paths["holdout_policy_path"]),
        "--audit-embedding-scorer-export",
        str(paths["audit_embedding_scorer_export_path"]),
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
    assert data["metadata"]["spec_version"] == SPEC_VERSION
    assert "Future Gates Sketch" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_hybrid_scorer_offline_experiment_spec.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "openai" not in module_source
    assert "openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-hybrid-scorer-offline-experiment-spec"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
