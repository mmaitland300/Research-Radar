"""Tests for offline metric gate artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

from cli_parser_source import read_cli_parser_source
from pipeline.ml_offline_metric_gates import (
    GATES_VERSION,
    build_ml_offline_metric_gates_payload,
    markdown_from_ml_offline_metric_gates,
)


def _metric(mean: float, std: float = 0.02, n: int = 5) -> dict:
    return {"mean": mean, "std": std, "n": n}


def _ranker_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_ranker_experiment",
            "experiment_version": "ml-offline-ranker-experiment-v1",
            "target": "good_or_acceptable",
            "effective_cv_folds": 5,
        },
        "policy_compliance": {
            "grouped_split_used": True,
            "row_level_random_split_used": False,
            "production_artifact_written": False,
            "leakage_checks_passed": True,
            "allowed_target_verified": True,
            "forbidden_targets_verified": True,
        },
        "dataset_inventory": {
            "eligible_observations": 400,
            "unique_eligible_canonical_work_count": 350,
            "duplicate_observation_pressure": 50,
            "review_pool_variant_counts": {"ml_blind_snapshot_audit": 120, "ml_transfer_gap_audit": 280},
            "target_class_counts": {
                "observation_level": {"positive": 280, "negative": 120},
                "work_group_reporting_level": {
                    "any_positive": {"positive": 230, "negative": 120},
                    "conflicting_target_work_group_count": 2,
                },
            },
        },
        "leakage_report": {
            "global_leakage_work_overlap_count": 0,
            "global_zero_assertion": True,
            "per_fold": [{"fold_index": i, "leakage_work_overlap_count": 0} for i in range(1, 6)],
        },
        "models": {
            "embedding_logistic": {
                "aggregate": {
                    "folds_evaluated": 5,
                    "folds_skipped": 0,
                    "observation_metrics_mean_std": {
                        "balanced_accuracy": _metric(0.82, 0.04),
                        "roc_auc": _metric(0.88, 0.03),
                        "average_precision": _metric(0.91, 0.02),
                    },
                }
            },
            "majority_class": {
                "aggregate": {
                    "observation_metrics_mean_std": {
                        "balanced_accuracy": _metric(0.50),
                    }
                }
            },
        },
    }


def _split_policy_payload() -> dict:
    return {
        "metadata": {"artifact_type": "ml_label_split_policy", "policy_version": "ml-label-split-policy-v1"},
        "allowed_targets_for_v1_split": ["good_or_acceptable"],
        "forbidden_targets": ["surprising_or_useful"],
    }


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


def _transfer_payload() -> dict:
    return {
        "metadata": {"artifact_type": "ml_text_transfer_readiness", "readiness_version": "ml-text-transfer-readiness-v8"},
        "heuristic_readiness_flags": {
            "good_or_acceptable": {"production_ready": {"value": False}},
            "surprising_or_useful": {"external_blind_transfer_weak": {"value": True}},
        },
        "cross_pool_synthesis": {
            "good_or_acceptable": {"source_transfer": {}},
            "surprising_or_useful": {"source_transfer": {}},
        },
        "production_recommender_missing_gates": ["shadow/flagged experiment"],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _build(tmp_path: Path, ranker: dict | None = None, *, with_transfer: bool = True) -> dict:
    kwargs = {
        "ranker_experiment_path": _write_json(tmp_path, "ranker.json", ranker or _ranker_payload()),
        "split_policy_path": _write_json(tmp_path, "policy.json", _split_policy_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "plan.json", _production_plan_payload()),
        "repo_root": tmp_path,
        "generated_at": "2026-05-16T00:00:00Z",
    }
    if with_transfer:
        kwargs["transfer_readiness_path"] = _write_json(tmp_path, "transfer.json", _transfer_payload())
    return build_ml_offline_metric_gates_payload(**kwargs)


def _gate(payload: dict, gate_id: str) -> dict:
    return next(gate for gate in payload["gates"] if gate["gate_id"] == gate_id)


def test_happy_path_all_required_audit_gates_pass_with_advisory_transfer(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    assert payload["metadata"]["gates_version"] == GATES_VERSION
    assert payload["metadata"]["experiment_scope"] == "audit_pool_offline_ranker"
    assert payload["audit_ranker_gates_passed"] is True
    assert payload["recommended_next_stage"] == "proceed_to_production_candidate_offline_scoring"
    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert _gate(payload, "G12_product_candidate_experiment_required")["status"] == "not_evaluated"
    assert _gate(payload, "G14_transfer_readiness_advisory")["status"] == "advisory_warn"
    assert "G14_transfer_readiness_advisory" not in payload["blocked_reasons"]


def test_leakage_nonzero_fails_g03_and_blocks(tmp_path: Path) -> None:
    ranker = _ranker_payload()
    ranker["leakage_report"]["global_leakage_work_overlap_count"] = 1
    ranker["leakage_report"]["global_zero_assertion"] = False
    ranker["leakage_report"]["per_fold"][0]["leakage_work_overlap_count"] = 1
    payload = _build(tmp_path, ranker)
    assert _gate(payload, "G03_leakage_zero")["status"] == "fail"
    assert payload["recommended_next_stage"] == "blocked_pending_audit_gate_failures"
    assert "G03_leakage_zero" in payload["blocked_reasons"]


def test_ba_margin_below_threshold_fails_g06(tmp_path: Path) -> None:
    ranker = _ranker_payload()
    ranker["models"]["embedding_logistic"]["aggregate"]["observation_metrics_mean_std"]["balanced_accuracy"]["mean"] = 0.60
    payload = _build(tmp_path, ranker)
    assert _gate(payload, "G06_majority_baseline_margin")["status"] == "fail"
    assert payload["recommended_next_stage"] == "blocked_pending_audit_gate_failures"


def test_high_fold_std_fails_g09(tmp_path: Path) -> None:
    ranker = _ranker_payload()
    ranker["models"]["embedding_logistic"]["aggregate"]["observation_metrics_mean_std"]["balanced_accuracy"]["std"] = 0.12
    payload = _build(tmp_path, ranker)
    assert _gate(payload, "G09_fold_stability")["status"] == "fail"
    assert payload["recommended_next_stage"] == "blocked_pending_audit_gate_failures"


def test_duplicate_pressure_mismatch_fails_g10(tmp_path: Path) -> None:
    ranker = _ranker_payload()
    ranker["dataset_inventory"]["duplicate_observation_pressure"] = 49
    payload = _build(tmp_path, ranker)
    assert _gate(payload, "G10_duplicate_pressure_reported")["status"] == "fail"


def test_only_g05_failure_recommends_more_labeling(tmp_path: Path) -> None:
    ranker = _ranker_payload()
    ranker["dataset_inventory"]["eligible_observations"] = 299
    ranker["dataset_inventory"]["unique_eligible_canonical_work_count"] = 249
    ranker["dataset_inventory"]["duplicate_observation_pressure"] = 50
    payload = _build(tmp_path, ranker)
    assert _gate(payload, "G05_class_balance_floor")["status"] == "fail"
    assert payload["audit_ranker_gates_passed"] is False
    assert payload["recommended_next_stage"] == "continue_labeling_rubric"


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "gates.json"
    out_md = tmp_path / "gates.md"
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-metric-gates",
        "--ranker-experiment",
        str(_write_json(tmp_path, "ranker.json", _ranker_payload())),
        "--split-policy",
        str(_write_json(tmp_path, "policy.json", _split_policy_payload())),
        "--production-readiness-plan",
        str(_write_json(tmp_path, "plan.json", _production_plan_payload())),
        "--transfer-readiness",
        str(_write_json(tmp_path, "transfer.json", _transfer_payload())),
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
    assert data["metadata"]["artifact_type"] == "ml_offline_metric_gates"
    assert "Not Ship / Not Shadow Yet" in out_md.read_text(encoding="utf-8")


def test_markdown_contains_product_candidate_requirements(tmp_path: Path) -> None:
    md = markdown_from_ml_offline_metric_gates(_build(tmp_path))
    assert "Product-Candidate Experiment Requirements" in md
    assert "Production defaults remain blocked" in md


def test_no_db_network_ml_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_offline_metric_gates.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "openai" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-offline-metric-gates"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
