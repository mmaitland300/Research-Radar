"""Tests for fresh eval surface policy for hybrid validation v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_fresh_eval_surface_policy_hybrid import (
    MLFreshEvalSurfacePolicyHybridError,
    POLICY_VERSION,
    build_ml_fresh_eval_surface_policy_hybrid_payload,
)


def _hybrid_metric_gates_payload(
    *,
    recommended_next_stage: str = "create_fresh_eval_surface_for_hybrid_validation_v1",
    confirmatory_validation_passed: bool = False,
    shadow_allowed: bool = False,
    prod_allowed: bool = False,
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_metric_gates",
            "gates_version": "ml-hybrid-scorer-metric-gates-v1",
            "eval_work_set_sha256": "eval-sha",
        },
        "hybrid_material_lift_passed": True,
        "confirmatory_validation_passed": confirmatory_validation_passed,
        "recommended_next_stage": recommended_next_stage,
        "shadow_scoring_allowed": shadow_allowed,
        "production_default_allowed": prod_allowed,
    }


def _hybrid_experiment_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment",
            "experiment_version": "ml-hybrid-scorer-offline-experiment-v1",
            "eval_work_set_sha256": "eval-sha",
        },
        "summary": {
            "best_arm_selection_is_exploratory_only": True,
            "hybrid_material_lift_passed": True,
        },
    }


def _hybrid_spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment_spec",
            "spec_version": "ml-hybrid-scorer-offline-experiment-v1-spec",
            "eval_work_set_sha256": "eval-sha",
        },
    }


def _scoring_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v3",
            "ranking_run_id": "rank-ee2ba6c816",
            "family": "emerging",
            "eval_work_set_sha256": "eval-sha",
        },
        "candidate_pool_summary": {
            "candidate_unique_canonical_work_count": 217,
            "candidate_pool_work_set_sha256": "eval-sha",
        },
        "candidate_pool_definition": {
            "ranking_run_id": "rank-ee2ba6c816",
            "family": "emerging",
        },
    }


def _assignment_payload() -> dict:
    return {
        "metadata": {
            "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
            "eval_work_set_sha256": "eval-sha",
            "eval_work_count": 217,
            "ranking_run_id": "rank-ee2ba6c816",
            "family": "emerging",
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    gates: dict | None = None,
    experiment: dict | None = None,
    spec: dict | None = None,
    scoring: dict | None = None,
    assignment: dict | None = None,
) -> dict[str, Path]:
    conflict = tmp_path / "conflict-policy.md"
    conflict.write_text("# Conflict Policy\n\nNo silent conflict merge.\n", encoding="utf-8")
    return {
        "hybrid_metric_gates_path": _write_json(tmp_path, "hybrid-gates.json", gates or _hybrid_metric_gates_payload()),
        "hybrid_experiment_path": _write_json(tmp_path, "hybrid-experiment.json", experiment or _hybrid_experiment_payload()),
        "hybrid_experiment_spec_path": _write_json(tmp_path, "hybrid-spec.json", spec or _hybrid_spec_payload()),
        "production_candidate_scoring_path": _write_json(tmp_path, "scoring.json", scoring or _scoring_payload()),
        "holdout_assignment_path": _write_json(tmp_path, "assignment.json", assignment or _assignment_payload()),
        "conflict_policy_path": conflict,
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_fresh_eval_surface_policy_hybrid_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def test_happy_path_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "policy.json"
    out_md = tmp_path / "policy.md"

    from pipeline.ml_fresh_eval_surface_policy_hybrid import write_ml_fresh_eval_surface_policy_hybrid

    payload = write_ml_fresh_eval_surface_policy_hybrid(
        **paths,
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=tmp_path,
    )

    assert payload["metadata"]["artifact_type"] == "ml_fresh_eval_surface_policy_hybrid"
    assert payload["metadata"]["policy_version"] == POLICY_VERSION
    assert json.loads(out_json.read_text(encoding="utf-8"))["metadata"]["policy_version"] == POLICY_VERSION
    assert "old 217-work surface cannot be used as confirmatory evidence" in out_md.read_text(encoding="utf-8")


def test_rejects_hybrid_gates_wrong_recommended_next_stage(tmp_path: Path) -> None:
    gates = _hybrid_metric_gates_payload(recommended_next_stage="draft_ml_shadow_scorer_v1_spec")

    with pytest.raises(MLFreshEvalSurfacePolicyHybridError, match="recommended_next_stage"):
        _build(tmp_path, gates=gates)


def test_rejects_hybrid_gates_confirmatory_validation_true(tmp_path: Path) -> None:
    gates = _hybrid_metric_gates_payload(confirmatory_validation_passed=True)

    with pytest.raises(MLFreshEvalSurfacePolicyHybridError, match="confirmatory_validation_passed"):
        _build(tmp_path, gates=gates)


def test_rejects_hybrid_gates_shadow_or_prod_allowed(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalSurfacePolicyHybridError, match="shadow_scoring_allowed"):
        _build(tmp_path, gates=_hybrid_metric_gates_payload(shadow_allowed=True))

    with pytest.raises(MLFreshEvalSurfacePolicyHybridError, match="production_default_allowed"):
        _build(tmp_path, gates=_hybrid_metric_gates_payload(prod_allowed=True))


def test_disallowed_surface_includes_scoring_eval_sha_and_candidate_count(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    surface = payload["disallowed_surfaces"][0]

    assert surface["surface_id"] == "product_candidate_eval_surface_rank-ee2ba6c816_emerging_v3"
    assert surface["eval_work_set_sha256"] == "eval-sha"
    assert surface["candidate_work_count"] == 217
    assert surface["confirmatory_use"] == "disallowed"


def test_policy_assertions_freeze_old_surface_and_primary_arm(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["policy_assertions"]["old_217_surface_confirmatory_reuse_allowed"] is False
    assert payload["policy_assertions"]["frozen_primary_hybrid_arm"] == "hybrid_rank_mean_50_50"
    assert payload["frozen_hybrid_arms"]["primary_confirmatory_arm"] == "hybrid_rank_mean_50_50"
    assert payload["frozen_hybrid_arms"]["secondary_reporting_arm"] == "hybrid_rank_mean_25_75_heuristic"


def test_minimum_label_thresholds_are_present(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    thresholds = payload["label_policy"]["minimum_confirmatory_label_thresholds"]

    assert thresholds["minimum_candidate_work_count"] == 100
    assert thresholds["minimum_confirmatory_labeled_work_count"] == 100
    assert thresholds["minimum_confirmatory_positive_work_count"] == 50
    assert thresholds["minimum_confirmatory_negative_work_count"] == 20
    assert thresholds["minimum_confirmatory_label_coverage_rate"] == 0.60
    assert thresholds["minimum_distinct_negative_work_count"] == 20


def test_blocked_actions_include_shadow_and_production_default(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert "shadow_scoring" in payload["blocked_actions"]
    assert "production_default_change" in payload["blocked_actions"]
    assert "using old 217-work surface for confirmatory hybrid validation" in payload["blocked_actions"]


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "policy.json"
    out_md = tmp_path / "policy.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-eval-surface-policy-hybrid",
        "--hybrid-metric-gates",
        str(paths["hybrid_metric_gates_path"]),
        "--hybrid-experiment",
        str(paths["hybrid_experiment_path"]),
        "--hybrid-experiment-spec",
        str(paths["hybrid_experiment_spec_path"]),
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--holdout-assignment",
        str(paths["holdout_assignment_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["metadata"]["policy_version"] == POLICY_VERSION
    assert "Disallowed 217-Work Surface Details" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_eval_surface_policy_hybrid.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-fresh-eval-surface-policy-hybrid"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
