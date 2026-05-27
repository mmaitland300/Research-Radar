"""Tests for ml-shadow-scorer-v1 generalization audit gates."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_generalization_audit_gates as gates_module
from pipeline.ml_shadow_scorer_generalization_audit_gates import (
    FORMULA_ID,
    MLShadowScorerGeneralizationAuditGatesError,
    build_ml_shadow_scorer_generalization_audit_gates_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _patch_small_constants(monkeypatch: pytest.MonkeyPatch) -> str:
    work_ids = ["W000000001", "W000000002", "W000000003", "W000000004"]
    candidate_sha = gates_module._work_set_sha256(work_ids)
    monkeypatch.setattr(gates_module, "EXPECTED_POOL_SIZE", 4)
    monkeypatch.setattr(gates_module, "EXPECTED_CONFIRMATORY_COUNT", 2)
    monkeypatch.setattr(gates_module, "EXPECTED_POOL_ONLY_NON_METRIC_COUNT", 2)
    monkeypatch.setattr(gates_module, "EXPECTED_POSITIVE_COUNT", 1)
    monkeypatch.setattr(gates_module, "EXPECTED_NEGATIVE_COUNT", 1)
    monkeypatch.setattr(gates_module, "EXPECTED_OLD_217_OVERLAP_COUNT", 1)
    monkeypatch.setattr(gates_module, "EXPECTED_FIRST_SURFACE_OVERLAP_COUNT", 1)
    monkeypatch.setattr(gates_module, "EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT", 2)
    monkeypatch.setattr(gates_module, "EXPECTED_CANDIDATE_POOL_SHA", candidate_sha)
    return candidate_sha


def _shadow_rows(candidate_sha: str) -> list[dict]:
    return [
        {
            "shadow_rank": 1,
            "canonical_openalex_work_id": "W000000001",
            "final_score": 0.9,
            "audit_embedding_probability_work": 0.8,
            "final_score_rank_pct": 1.0,
            "audit_embedding_probability_rank_pct": 1.0,
            "ml_shadow_scorer_v1_score": 1.0,
            "confirmatory_metric_eligible": True,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        {
            "shadow_rank": 2,
            "canonical_openalex_work_id": "W000000002",
            "final_score": 0.7,
            "audit_embedding_probability_work": 0.2,
            "final_score_rank_pct": 2.0 / 3.0,
            "audit_embedding_probability_rank_pct": 1.0 / 3.0,
            "ml_shadow_scorer_v1_score": 0.5,
            "confirmatory_metric_eligible": True,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        {
            "shadow_rank": 3,
            "canonical_openalex_work_id": "W000000003",
            "final_score": 0.3,
            "audit_embedding_probability_work": 0.6,
            "final_score_rank_pct": 1.0 / 3.0,
            "audit_embedding_probability_rank_pct": 2.0 / 3.0,
            "ml_shadow_scorer_v1_score": 0.5,
            "confirmatory_metric_eligible": False,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        {
            "shadow_rank": 4,
            "canonical_openalex_work_id": "W000000004",
            "final_score": 0.1,
            "audit_embedding_probability_work": 0.1,
            "final_score_rank_pct": 0.0,
            "audit_embedding_probability_rank_pct": 0.0,
            "ml_shadow_scorer_v1_score": 0.0,
            "confirmatory_metric_eligible": False,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
    ]


def _audit_payload(candidate_sha: str, *, material_lift: bool = True) -> dict:
    delta_roc = 0.04 if material_lift else 0.01
    delta_ap = 0.01 if material_lift else 0.0
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_surface_generalization_audit",
            "artifact_version": "ml-shadow-scorer-v1-second-surface-generalization-audit-v1",
            "ranking_run_id": gates_module.RANKING_RUN_ID,
            "family": gates_module.FAMILY,
            "corpus_snapshot_version": gates_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": gates_module.EMBEDDING_VERSION,
            "candidate_pool_work_set_sha256": candidate_sha,
        },
        "generalization_audit_executed": True,
        "generalization_audit_gates_passed": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "recommended_next_stage": "run_ml_shadow_scorer_v1_generalization_audit_gates_v1",
        "audit_scope": {"candidate_pool_work_count": 4, "confirmatory_metric_work_count": 2},
        "label_join_summary": {
            "joined_label_count": 2,
            "positive_count": 1,
            "negative_count": 1,
            "conflicting_target_work_group_count": 0,
        },
        "metric_coverage": {
            "candidate_pool_work_count": 4,
            "shadow_output_row_count": 4,
            "confirmatory_metric_work_count": 2,
            "pool_only_non_metric_row_count": 2,
            "prior_overlap_rows_scored_but_excluded_from_metric_denominator": True,
        },
        "leakage_report": {
            "labels_used_for_scoring": False,
            "scorer_refit_used": False,
            "supervised_fit_used": False,
            "eval_label_weight_tuning_used": False,
            "train_rows_used": 0,
            "old_217_overlap_excluded_from_confirmatory_metrics": True,
            "first_validated_surface_overlap_excluded_from_confirmatory_metrics": True,
            "old_217_overlap_count_in_full_pool": 1,
            "rank_9f4b2a2084_overlap_count_in_full_pool": 1,
            "combined_prior_surface_overlap_count_in_full_pool": 2,
        },
        "arm_metrics": {
            "heuristic_final_score_baseline": {"roc_auc_mann_whitney": 0.6, "average_precision": 0.65},
            FORMULA_ID: {"roc_auc_mann_whitney": 0.64, "average_precision": 0.66},
        },
        "comparisons_vs_heuristic": {
            FORMULA_ID: {
                "material_lift_observed": material_lift,
                "delta_roc_auc": delta_roc,
                "delta_average_precision": delta_ap,
                "delta_precision_at_5": -0.2,
                "delta_precision_at_10": -0.2,
                "delta_precision_at_20": -0.1,
            }
        },
        "shadow_and_production_blockers": {
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "shadow_output_rows": _shadow_rows(candidate_sha),
    }


def _discovery_payload(candidate_sha: str) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
        },
        "discovery_summary": {"status": "selected_ready_for_generalization_audit"},
        "readiness_for_generalization_audit": {"ready_for_generalization_audit_execution": True},
        "selected_second_surface": {
            "ranking_run_id": gates_module.RANKING_RUN_ID,
            "family": gates_module.FAMILY,
            "corpus_snapshot_version": gates_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": gates_module.EMBEDDING_VERSION,
            "candidate_pool_work_set_sha256": candidate_sha,
            "candidate_pool_work_count": 4,
            "confirmatory_metric_eligible_work_count": 2,
        },
    }


def _plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
        "generalization_gate_contract": {"passes_only_if": ["material lift passes"]},
    }


def _online_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
    }


def _fresh_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "gate_linkage": {"material_lift_thresholds": {"delta_roc_auc_gte": 0.03, "or_delta_average_precision_gte": 0.02}},
    }


def _production_plan_payload(*, blocked: bool = True) -> dict:
    return {
        "metadata": {"artifact_type": "ml_production_readiness_plan", "plan_version": "ml-production-readiness-plan-v1"},
        "overall_status": "research_only" if blocked else "production_ready",
        "production_default_authorized": False if blocked else True,
        "targets": {"good_or_acceptable": {"production_eligible": False if blocked else True}},
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    audit: dict | None = None,
    discovery: dict | None = None,
    plan: dict | None = None,
    online_policy: dict | None = None,
    fresh_policy: dict | None = None,
    production_plan: dict | None = None,
) -> dict[str, Path]:
    candidate_sha = _patch_small_constants(monkeypatch)
    return {
        "second_surface_generalization_audit_path": _write_json(tmp_path, "audit.json", audit or _audit_payload(candidate_sha)),
        "generalization_second_surface_path": _write_json(
            tmp_path, "discovery.json", discovery or _discovery_payload(candidate_sha)
        ),
        "generalization_audit_plan_path": _write_json(tmp_path, "plan.json", plan or _plan_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "online-policy.json", online_policy or _online_policy_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "fresh-policy.json", fresh_policy or _fresh_policy_payload()),
        "production_readiness_plan_path": _write_json(
            tmp_path, "production-plan.json", production_plan or _production_plan_payload()
        ),
    }


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **overrides: dict) -> dict:
    return build_ml_shadow_scorer_generalization_audit_gates_payload(
        **_paths(tmp_path, monkeypatch, **overrides),
        repo_root=tmp_path,
        generated_at="2026-05-26T00:00:00Z",
    )


def test_happy_path_gates_pass_on_small_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["generalization_audit_gates_passed"] is True
    assert payload["second_surface_generalization_passed"] is True
    assert payload["material_lift_gate_passed"] is True
    assert payload["disabled_by_default_runtime_implementation_next_stage_allowed"] is True
    assert payload["recommended_next_stage"] == "implement_online_shadow_runtime_disabled_by_default"
    assert all(gate["status"] == "pass" for gate in payload["gate_results"])


def test_rejects_wrong_audit_recommended_next_stage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    audit = _audit_payload(sha)
    audit["recommended_next_stage"] = "something_else"
    with pytest.raises(MLShadowScorerGeneralizationAuditGatesError, match="recommended_next_stage"):
        build_ml_shadow_scorer_generalization_audit_gates_payload(
            **_paths(tmp_path, monkeypatch, audit=audit), repo_root=tmp_path
        )


def test_rejects_identity_mismatch_across_audit_and_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sha = _patch_small_constants(monkeypatch)
    discovery = _discovery_payload(sha)
    discovery["selected_second_surface"]["ranking_run_id"] = "rank-other"
    with pytest.raises(MLShadowScorerGeneralizationAuditGatesError, match="identity ranking_run_id"):
        build_ml_shadow_scorer_generalization_audit_gates_payload(
            **_paths(tmp_path, monkeypatch, discovery=discovery), repo_root=tmp_path
        )


def test_rejects_incomplete_rows_or_missing_required_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    incomplete = _audit_payload(sha)
    incomplete["shadow_output_rows"] = incomplete["shadow_output_rows"][:3]
    with pytest.raises(MLShadowScorerGeneralizationAuditGatesError, match="shadow_output_rows length"):
        build_ml_shadow_scorer_generalization_audit_gates_payload(
            **_paths(tmp_path / "incomplete", monkeypatch, audit=incomplete), repo_root=tmp_path
        )

    missing = _audit_payload(sha)
    del missing["shadow_output_rows"][0]["final_score_rank_pct"]
    with pytest.raises(MLShadowScorerGeneralizationAuditGatesError, match="missing required fields"):
        build_ml_shadow_scorer_generalization_audit_gates_payload(
            **_paths(tmp_path / "missing", monkeypatch, audit=missing), repo_root=tmp_path
        )


def test_rejects_formula_mismatch_beyond_tolerance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    audit = _audit_payload(sha)
    audit["shadow_output_rows"][0]["ml_shadow_scorer_v1_score"] = 0.7
    with pytest.raises(MLShadowScorerGeneralizationAuditGatesError, match="formula replay mismatch"):
        build_ml_shadow_scorer_generalization_audit_gates_payload(
            **_paths(tmp_path, monkeypatch, audit=audit), repo_root=tmp_path
        )


def test_material_lift_failure_routes_to_failure_analysis(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = _patch_small_constants(monkeypatch)
    payload = build_ml_shadow_scorer_generalization_audit_gates_payload(
        **_paths(tmp_path, monkeypatch, audit=_audit_payload(sha, material_lift=False)),
        repo_root=tmp_path,
    )

    assert payload["material_lift_gate_passed"] is False
    assert payload["generalization_audit_gates_passed"] is False
    assert payload["recommended_next_stage"] == "write_second_surface_generalization_failure_analysis_v1"
    assert payload["shadow_and_production_blockers"]["missing_generalization_audit_gates"] is True


def test_precision_at_k_regression_alone_does_not_fail_gates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    advisory = payload["metric_gate_results"]["precision_at_k_advisory"]
    assert advisory["precision_at_5"] < 0
    assert advisory["precision_at_10"] < 0
    assert advisory["gate_effect"] == "reported_only_not_gate_failing"
    assert payload["generalization_audit_gates_passed"] is True


def test_g06_uses_executable_prior_overlap_count_checks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    g06 = next(gate for gate in payload["gate_results"] if gate["gate_id"] == "G06_prior_surface_exclusion")

    assert g06["passed"] is True
    assert g06["observed_value"]["confirmatory_metric_work_count"] == 2
    assert g06["observed_value"]["pool_only_non_metric_row_count"] == 2


def test_production_readiness_must_remain_blocked_research_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _build(tmp_path, monkeypatch, production_plan=_production_plan_payload(blocked=False))

    assert payload["generalization_audit_gates_passed"] is False
    assert payload["recommended_next_stage"] == "write_second_surface_generalization_failure_analysis_v1"
    g09 = next(gate for gate in payload["gate_results"] if gate["gate_id"] == "G09_production_readiness_still_separate")
    assert g09["passed"] is False


def test_g09_reports_metadata_overall_status_and_absent_default_authorization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    production_plan = _production_plan_payload()
    production_plan["metadata"]["overall_status"] = production_plan.pop("overall_status")
    production_plan.pop("production_default_authorized")

    payload = _build(tmp_path, monkeypatch, production_plan=production_plan)
    g09 = next(gate for gate in payload["gate_results"] if gate["gate_id"] == "G09_production_readiness_still_separate")
    observed = g09["observed_value"]

    assert g09["passed"] is True
    assert observed["overall_status"] == "research_only"
    assert observed["production_default_authorized"] is None
    assert observed["production_default_authorized"] is not True
    assert observed["good_or_acceptable_production_eligible"] is False
    assert observed["production_plan_blocked"] is True
    assert payload["generalization_audit_gates_passed"] is True


def test_runtime_prod_api_shadow_remain_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["runtime_implementation_authorized"] is False
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["api_web_changes_allowed"] is False


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "gates.json"
    out_md = tmp_path / "gates.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-generalization-audit-gates",
        "--second-surface-generalization-audit",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json"),
        "--generalization-second-surface",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.json"),
        "--generalization-audit-plan",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-audit-v1.json"),
        "--online-shadow-policy",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"),
        "--fresh-surface-policy",
        str(REPO_ROOT / "docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json"),
        "--production-readiness-plan",
        str(REPO_ROOT / "docs/audit/ml-production-readiness-plan-v1.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_generalization_audit_gates"
    assert payload["generalization_audit_gates_passed"] is True
    assert "implement_online_shadow_runtime_disabled_by_default" in result.stdout
    assert "Generalization Audit Gates" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_generalization_audit_gates.py").read_text(
        encoding="utf-8"
    )
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-generalization-audit-gates"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
