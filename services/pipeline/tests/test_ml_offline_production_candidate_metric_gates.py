"""Tests for product-candidate offline metric gates v1."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_offline_production_candidate_metric_gates import (
    GATES_VERSION,
    GATES_VERSION_V2,
    MLOfflineProductionCandidateMetricGatesError,
    build_ml_offline_production_candidate_metric_gates_payload,
    markdown_from_ml_offline_production_candidate_metric_gates,
)
from pipeline.ml_label_dataset import sha256_file


def _scoring_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v1",
            "ranking_run_id": "rank-ee2ba6c816",
            "family": "emerging",
            "target": "good_or_acceptable",
            "scoring_mode": "heuristic_and_coverage_only",
        },
        "candidate_pool_definition": {
            "source": "existing paper_scores rows joined to works",
            "ranking_run_id": "rank-ee2ba6c816",
            "family": "emerging",
            "no_new_ranking_run": True,
            "postgres_write_allowed": False,
        },
        "candidate_pool_summary": {
            "paper_scores_row_count": 217,
            "candidate_unique_internal_work_count": 217,
            "candidate_unique_canonical_work_count": 217,
            "candidate_rows_without_canonical_work_id": 0,
        },
        "label_join_summary": {
            "explicit_target_observation_count": 427,
            "joined_labeled_observation_count": 286,
            "joined_labeled_unique_work_count": 217,
            "labeled_eval_subset_work_count": 217,
            "labeled_eval_subset_positive_work_count": 190,
            "labeled_eval_subset_negative_work_count": 27,
            "candidate_work_labeled_coverage_rate": 1.0,
            "candidate_work_unlabeled_count": 0,
        },
        "embedding_join_summary": {
            "embedding_rows_available": 427,
            "labeled_candidate_observation_count": 286,
            "labeled_candidate_unique_work_count": 217,
            "missing_embedding_count": 0,
            "missing_embedding_row_ids": [],
        },
        "scoring_mode_details": {
            "scoring_mode": "heuristic_and_coverage_only",
            "learned_product_scores_produced": False,
            "reason": "no frozen full-fit audit scorer export exists",
        },
        "heuristic_metrics": {
            "metric_level": "canonical_work_labeled_eval_subset",
            "scored_labeled_work_count": 217,
            "positive_work_count": 190,
            "negative_work_count": 27,
            "roc_auc_mann_whitney": 0.8035087719298246,
            "roc_auc_reason": None,
            "average_precision": 0.9578865940621812,
            "average_precision_reason": None,
            "precision_recall_at_k": {
                "10": {
                    "precision": 1.0,
                    "recall": 0.05263157894736842,
                    "reason": None,
                }
            },
        },
        "learned_or_embedding_metrics": {
            "metrics": None,
            "reason": "no frozen full-fit audit scorer export exists",
            "learned_product_scores_produced": False,
            "audit_embedding_scorer_export_present": False,
        },
        "blockers_to_shadow": [
            "product-candidate metric gates not yet evaluated",
            "no ml-shadow-scorer-v1 contract exists",
            "production default blocked by readiness plan",
            "no production model artifact exists",
        ],
    }


def _scorer_payload(*, product_candidate_pool_used_for_training: bool = False) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_audit_embedding_scorer",
            "scorer_version": "ml-offline-audit-embedding-scorer-v1",
            "target": "good_or_acceptable",
            "fit_mode": "full_fit_audit_corpus",
            "label_dataset_sha256": "label-sha",
            "embedding_artifact_sha256": "embedding-sha",
            "embedding_dimensions": 2,
        },
        "scorer": {
            "scaler": {"feature_count": 2, "mean": [0.0, 0.0], "scale": [1.0, 1.0]},
            "classifier": {
                "coefficients_standardized_space": [1.0, -1.0],
                "intercept_standardized_space": 0.0,
            },
        },
        "policy_compliance": {
            "product_candidate_pool_used_for_training": product_candidate_pool_used_for_training,
            "shadow_scoring_authorized": False,
            "production_artifact_written": False,
        },
    }


def _scoring_payload_v2(scorer_sha: str) -> dict:
    payload = copy.deepcopy(_scoring_payload())
    payload["metadata"].update(
        {
            "experiment_version": "ml-offline-production-candidate-scoring-v2",
            "scoring_mode": "heuristic_and_audit_embedding_scorer",
            "inputs": [
                {"name": "label_dataset", "path": "label.json", "sha256": "label-sha"},
                {"name": "embeddings", "path": "embeddings.json", "sha256": "embedding-sha"},
                {
                    "name": "audit_embedding_scorer_export",
                    "path": "scorer.json",
                    "sha256": scorer_sha,
                },
            ],
        }
    )
    payload["scoring_mode_details"] = {
        "scoring_mode": "heuristic_and_audit_embedding_scorer",
        "learned_product_scores_produced": True,
        "audit_embedding_scorer_export_present": True,
        "audit_embedding_scorer_version": "ml-offline-audit-embedding-scorer-v1",
        "audit_embedding_scorer_sha256": scorer_sha,
        "product_candidate_rows_used_for_training": 0,
        "learned_score_aggregation_policy": "max_probability",
        "learned_metric_thresholds": {
            "minimum_learned_roc_auc": 0.70,
            "minimum_learned_average_precision": 0.85,
            "minimum_learned_precision_at_10": 0.80,
        },
        "learned_metric_thresholds_satisfied": True,
    }
    payload["learned_or_embedding_metrics"] = {
        "metrics": {
            "metric_level": "canonical_work_labeled_eval_subset",
            "score_name": "audit_embedding_probability_work",
            "labeled_eval_subset_work_count": 217,
            "scored_labeled_work_count": 217,
            "positive_work_count": 190,
            "negative_work_count": 27,
            "roc_auc_mann_whitney": 1.0,
            "roc_auc_reason": None,
            "average_precision": 1.0,
            "average_precision_reason": None,
            "precision_recall_at_k": {
                "5": {"precision": 1.0, "recall": 0.02631578947368421, "reason": None},
                "10": {"precision": 1.0, "recall": 0.05263157894736842, "reason": None},
                "20": {"precision": 1.0, "recall": 0.10526315789473684, "reason": None},
            },
        },
        "comparison_to_heuristic": {
            "delta_roc_auc": 0.19649122807017538,
            "delta_average_precision": 0.04211340593781876,
            "delta_precision_at_10": 0.0,
            "side_by_side": {
                "roc_auc_mann_whitney": {
                    "heuristic_final_score": 0.8035087719298246,
                    "audit_embedding_probability_work": 1.0,
                }
            },
        },
        "learned_product_scores_produced": True,
        "audit_embedding_scorer_export_present": True,
        "aggregation_policy": "max_probability",
        "learned_metric_thresholds_satisfied": True,
    }
    payload["blockers_to_shadow"] = [
        "product-candidate learned metric gates not yet evaluated",
        "no ml-shadow-scorer-v1 contract exists",
        "production default blocked by readiness plan",
        "no production model artifact exists",
    ]
    return payload


def _offline_metric_gates_payload() -> dict:
    return {
        "audit_ranker_gates_passed": True,
        "recommended_next_stage": "proceed_to_production_candidate_offline_scoring",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "metadata": {
            "artifact_type": "ml_offline_metric_gates",
            "gates_version": "ml-offline-metric-gates-v1",
        },
    }


def _split_policy_payload() -> dict:
    return {"metadata": {"artifact_type": "ml_label_split_policy", "policy_version": "ml-label-split-policy-v1"}}


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
            "overall_status": "research_only",
        },
        "targets": {
            "good_or_acceptable": {
                "status": "primary_candidate",
                "allowed_next_stage": "offline_ranker_research_only",
                "production_eligible": False,
            },
            "surprising_or_useful": {"status": "deferred", "production_eligible": False},
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, scoring: dict | None = None) -> dict[str, Path]:
    return {
        "production_candidate_scoring_path": _write_json(tmp_path, "scoring.json", scoring or _scoring_payload()),
        "offline_metric_gates_path": _write_json(tmp_path, "offline-gates.json", _offline_metric_gates_payload()),
        "split_policy_path": _write_json(tmp_path, "split-policy.json", _split_policy_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "production-plan.json", _production_plan_payload()),
    }


def _build(tmp_path: Path, scoring: dict | None = None) -> dict:
    return build_ml_offline_production_candidate_metric_gates_payload(
        **_paths(tmp_path, scoring),
        repo_root=tmp_path,
        generated_at="2026-05-16T00:00:00Z",
    )


def _paths_v2(
    tmp_path: Path,
    *,
    scoring: dict | None = None,
    scorer: dict | None = None,
    include_prior_v1: bool = False,
) -> dict[str, Path]:
    scorer_path = _write_json(tmp_path, "scorer.json", scorer or _scorer_payload())
    scorer_sha = sha256_file(scorer_path)
    paths = _paths(tmp_path, scoring or _scoring_payload_v2(scorer_sha))
    paths["audit_embedding_scorer_export_path"] = scorer_path
    if include_prior_v1:
        paths["production_candidate_metric_gates_v1_path"] = _write_json(
            tmp_path,
            "metric-gates-v1.json",
            {
                "metadata": {
                    "artifact_type": "ml_offline_production_candidate_metric_gates",
                    "gates_version": "ml-offline-production-candidate-metric-gates-v1",
                },
                "product_candidate_heuristic_gates_passed": True,
            },
        )
    return paths


def _build_v2(
    tmp_path: Path,
    *,
    scoring: dict | None = None,
    scorer: dict | None = None,
    include_prior_v1: bool = False,
) -> dict:
    return build_ml_offline_production_candidate_metric_gates_payload(
        **_paths_v2(tmp_path, scoring=scoring, scorer=scorer, include_prior_v1=include_prior_v1),
        gates_version=GATES_VERSION_V2,
        repo_root=tmp_path,
        generated_at="2026-05-16T00:00:00Z",
    )


def _gate(payload: dict, gate_id: str) -> dict:
    return next(gate for gate in payload["gates"] if gate["gate_id"] == gate_id)


def test_happy_path_current_style_artifact_passes_heuristic_gates_and_blocks_learned_shadow(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["gates_version"] == GATES_VERSION
    assert payload["product_candidate_heuristic_gates_passed"] is True
    assert payload["learned_scorer_product_candidate_gates_passed"] is False
    assert payload["recommended_next_stage"] == "create_frozen_audit_embedding_scorer_export_v1"
    assert _gate(payload, "G10_learned_scorer_status")["status"] == "not_evaluated"
    assert _gate(payload, "G13_positive_prevalence_advisory")["status"] == "advisory_warn"
    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False


def test_low_label_coverage_fails_g04(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["label_join_summary"]["candidate_work_labeled_coverage_rate"] = 0.79
    payload = _build(tmp_path, scoring)

    assert _gate(payload, "G04_label_coverage")["status"] == "fail"
    assert payload["product_candidate_heuristic_gates_passed"] is False
    assert payload["recommended_next_stage"] == "blocked_pending_product_candidate_heuristic_gate_failures"


def test_low_negative_coverage_fails_g05(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["label_join_summary"]["labeled_eval_subset_negative_work_count"] = 19
    payload = _build(tmp_path, scoring)

    assert _gate(payload, "G05_negative_coverage")["status"] == "fail"
    assert payload["product_candidate_heuristic_gates_passed"] is False


def test_missing_embeddings_over_threshold_fails_g06(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["embedding_join_summary"]["missing_embedding_count"] = 16
    payload = _build(tmp_path, scoring)

    assert _gate(payload, "G06_embedding_coverage")["status"] == "fail"
    assert payload["coverage_summary"]["missing_embedding_rate"] == pytest.approx(16 / 286)


def test_low_roc_auc_fails_g07(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["heuristic_metrics"]["roc_auc_mann_whitney"] = 0.69
    payload = _build(tmp_path, scoring)

    assert _gate(payload, "G07_heuristic_roc_auc")["status"] == "fail"
    assert payload["product_candidate_heuristic_gates_passed"] is False


def test_low_average_precision_fails_g08(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["heuristic_metrics"]["average_precision"] = 0.84
    payload = _build(tmp_path, scoring)

    assert _gate(payload, "G08_heuristic_average_precision")["status"] == "fail"
    assert payload["product_candidate_heuristic_gates_passed"] is False


@pytest.mark.parametrize("precision", [None, 0.79])
def test_missing_or_low_precision_at_10_fails_g09(tmp_path: Path, precision: float | None) -> None:
    scoring = _scoring_payload()
    scoring["heuristic_metrics"]["precision_recall_at_k"]["10"]["precision"] = precision
    if precision is None:
        scoring["heuristic_metrics"]["precision_recall_at_k"]["10"]["reason"] = "requires at least 10 labeled works"
    payload = _build(tmp_path, scoring)

    assert _gate(payload, "G09_top_k_precision")["status"] == "fail"
    assert payload["product_candidate_heuristic_gates_passed"] is False


def test_g10_not_evaluated_does_not_fail_heuristic_gates(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert _gate(payload, "G10_learned_scorer_status")["status"] == "not_evaluated"
    assert payload["product_candidate_heuristic_gates_passed"] is True
    assert "G10_learned_scorer_status:not_evaluated" in payload["blocked_reasons"]


def test_positive_prevalence_advisory_does_not_fail_heuristic_gates(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    advisory = _gate(payload, "G13_positive_prevalence_advisory")
    assert advisory["status"] == "advisory_warn"
    assert advisory["observed_value"]["positive_work_prevalence"] == pytest.approx(190 / 217)
    assert payload["product_candidate_heuristic_gates_passed"] is True


def test_shadow_scoring_and_production_default_are_always_false(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert set(payload["shadow_blockers"]) >= {
        "learned_scorer_not_evaluated",
        "missing_ml_shadow_scorer_v1",
        "production_default_blocked",
        "no_production_model_artifact",
    }
    assert _gate(payload, "G11_shadow_blockers_documented")["status"] == "pass"


def test_v2_happy_path_passes_learned_application_and_blocks_shadow(tmp_path: Path) -> None:
    payload = _build_v2(tmp_path, include_prior_v1=True)

    assert payload["metadata"]["gates_version"] == GATES_VERSION_V2
    assert payload["metadata"]["thresholds_version"] == "ml-offline-production-candidate-metric-gates-v2-thresholds"
    assert payload["product_candidate_heuristic_gates_passed"] is True
    assert payload["learned_scorer_application_gates_passed"] is True
    assert payload["independent_learned_validation_passed"] is False
    assert payload["recommended_next_stage"] == "create_learned_scorer_holdout_policy_v1"
    assert _gate(payload, "G10_learned_scorer_application")["status"] == "pass"
    assert _gate(payload, "G11_scorer_provenance")["status"] == "pass"
    assert _gate(payload, "G12_independent_validation_status")["status"] == "not_evaluated"
    assert _gate(payload, "G15_positive_prevalence_advisory")["status"] == "advisory_warn"
    assert _gate(payload, "G16_near_perfect_learned_metrics_advisory")["status"] == "advisory_warn"
    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["learned_metric_summary"]["roc_auc_mann_whitney"] == 1.0
    assert payload["comparison_to_heuristic"]["delta_roc_auc"] == pytest.approx(0.19649122807017538)
    assert any(item["name"] == "production_candidate_metric_gates_v1" for item in payload["metadata"]["inputs"])
    assert (
        payload["metadata"]["strategic_framing"]["passing_v2_gates_authorizes_only"]
        == "create_learned_scorer_holdout_policy_v1"
    )


@pytest.mark.parametrize(
    ("metric_path", "value"),
    [
        ("roc_auc_mann_whitney", 0.69),
        ("average_precision", 0.84),
        ("precision_recall_at_k.10.precision", 0.79),
    ],
)
def test_v2_low_learned_metric_fails_g10(tmp_path: Path, metric_path: str, value: float) -> None:
    scorer_path = _write_json(tmp_path, "scorer.json", _scorer_payload())
    scoring = _scoring_payload_v2(sha256_file(scorer_path))
    current = scoring["learned_or_embedding_metrics"]["metrics"]
    parts = metric_path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value

    payload = build_ml_offline_production_candidate_metric_gates_payload(
        **_paths(tmp_path, scoring),
        audit_embedding_scorer_export_path=scorer_path,
        gates_version=GATES_VERSION_V2,
        repo_root=tmp_path,
        generated_at="2026-05-16T00:00:00Z",
    )

    assert _gate(payload, "G10_learned_scorer_application")["status"] == "fail"
    assert payload["learned_scorer_application_gates_passed"] is False
    assert payload["recommended_next_stage"] == "revisit_scorer_export_or_features"


def test_v2_rejects_scorer_sha_mismatch(tmp_path: Path) -> None:
    scorer_path = _write_json(tmp_path, "scorer.json", _scorer_payload())
    scoring = _scoring_payload_v2("wrong-sha")

    with pytest.raises(MLOfflineProductionCandidateMetricGatesError, match="scorer sha256"):
        build_ml_offline_production_candidate_metric_gates_payload(
            **_paths(tmp_path, scoring),
            audit_embedding_scorer_export_path=scorer_path,
            gates_version=GATES_VERSION_V2,
            repo_root=tmp_path,
            generated_at="2026-05-16T00:00:00Z",
        )


def test_v2_rejects_scorer_trained_on_product_candidate_pool(tmp_path: Path) -> None:
    scorer = _scorer_payload(product_candidate_pool_used_for_training=True)
    scorer_path = _write_json(tmp_path, "scorer.json", scorer)
    scoring = _scoring_payload_v2(sha256_file(scorer_path))

    with pytest.raises(MLOfflineProductionCandidateMetricGatesError, match="product_candidate_pool_used_for_training"):
        build_ml_offline_production_candidate_metric_gates_payload(
            **_paths(tmp_path, scoring),
            audit_embedding_scorer_export_path=scorer_path,
            gates_version=GATES_VERSION_V2,
            repo_root=tmp_path,
            generated_at="2026-05-16T00:00:00Z",
        )


def test_v2_advisories_do_not_fail_pass_gates(tmp_path: Path) -> None:
    payload = _build_v2(tmp_path)

    assert _gate(payload, "G15_positive_prevalence_advisory")["status"] == "advisory_warn"
    assert _gate(payload, "G16_near_perfect_learned_metrics_advisory")["status"] == "advisory_warn"
    assert payload["product_candidate_heuristic_gates_passed"] is True
    assert payload["learned_scorer_application_gates_passed"] is True


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "product-candidate-gates.json"
    out_md = tmp_path / "product-candidate-gates.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-production-candidate-metric-gates",
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--offline-metric-gates",
        str(paths["offline_metric_gates_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
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
    assert data["metadata"]["artifact_type"] == "ml_offline_production_candidate_metric_gates"
    assert data["recommended_next_stage"] == "create_frozen_audit_embedding_scorer_export_v1"
    assert "Not Shadow / Not Production" in out_md.read_text(encoding="utf-8")


def test_cli_v2_requires_scorer_export_but_v1_does_not(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "product-candidate-gates.json"
    out_md = tmp_path / "product-candidate-gates.md"

    import pipeline.cli as cli_main

    base_argv = [
        "pipeline.cli",
        "ml-offline-production-candidate-metric-gates",
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--offline-metric-gates",
        str(paths["offline_metric_gates_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", base_argv):
        cli_main.main()

    with patch.object(
        sys,
        "argv",
        base_argv + ["--gates-version", "ml-offline-production-candidate-metric-gates-v2"],
    ):
        with pytest.raises(SystemExit) as excinfo:
            cli_main.main()
    assert excinfo.value.code == 2


def test_cli_v2_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths_v2(tmp_path, include_prior_v1=True)
    out_json = tmp_path / "product-candidate-gates-v2.json"
    out_md = tmp_path / "product-candidate-gates-v2.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-production-candidate-metric-gates",
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--offline-metric-gates",
        str(paths["offline_metric_gates_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--audit-embedding-scorer-export",
        str(paths["audit_embedding_scorer_export_path"]),
        "--production-candidate-metric-gates-v1",
        str(paths["production_candidate_metric_gates_v1_path"]),
        "--gates-version",
        "ml-offline-production-candidate-metric-gates-v2",
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
    assert data["learned_scorer_application_gates_passed"] is True
    assert data["recommended_next_stage"] == "create_learned_scorer_holdout_policy_v1"
    md = out_md.read_text(encoding="utf-8")
    assert "Independent Validation Status" in md
    assert "not shadow scoring" in md.lower()


def test_markdown_contains_required_sections(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    md = markdown_from_ml_offline_production_candidate_metric_gates(payload)

    assert "Executive Summary" in md
    assert "Gate Checklist" in md
    assert "Product-Candidate Coverage Summary" in md
    assert "Heuristic Metric Summary" in md
    assert "Learned Scorer Status" in md
    assert "Positive Prevalence Advisory" in md
    assert "Recommended Next Stage" in md


def test_v2_markdown_contains_required_sections(tmp_path: Path) -> None:
    payload = _build_v2(tmp_path)
    md = markdown_from_ml_offline_production_candidate_metric_gates(payload)

    assert "Executive Summary" in md
    assert "Gate Checklist" in md
    assert "Coverage" in md
    assert "Heuristic Summary" in md
    assert "Learned Application Summary" in md
    assert "Independent Validation Status" in md
    assert "Advisories" in md
    assert "Not Shadow / Not Production" in md
    assert "Recommended Next Stage" in md
    assert "not independent validation" in md


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_offline_production_candidate_metric_gates.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "openai" not in module_source
    assert "openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-offline-production-candidate-metric-gates"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
