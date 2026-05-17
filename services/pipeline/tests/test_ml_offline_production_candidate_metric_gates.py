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
    build_ml_offline_production_candidate_metric_gates_payload,
    markdown_from_ml_offline_production_candidate_metric_gates,
)


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
