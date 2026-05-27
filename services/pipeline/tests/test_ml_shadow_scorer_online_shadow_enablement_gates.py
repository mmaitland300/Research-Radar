"""Tests for ml-shadow-scorer-v1 online shadow enablement gate definitions."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module
from pipeline.ml_shadow_scorer_online_shadow_enablement_gates import (
    FEATURE_FLAG,
    MLShadowScorerOnlineShadowEnablementGatesError,
    build_ml_shadow_scorer_online_shadow_enablement_gates_payload,
    build_ml_shadow_scorer_online_shadow_enablement_gates_run_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _identity() -> dict:
    return {
        "ranking_run_id": runtime_module.RANKING_RUN_ID,
        "family": runtime_module.FAMILY,
        "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
        "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
        "embedding_version": runtime_module.EMBEDDING_VERSION,
    }


def _verification_payload(*, passed: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_runtime_isolation_verification",
            "verification_version": "ml-shadow-scorer-v1-runtime-isolation-verification-v1",
            "runtime_feature_flag": FEATURE_FLAG,
            **_identity(),
        },
        "runtime_isolation_verification_passed": passed,
        "recommended_next_stage": "draft_online_shadow_execution_enablement_gates_v1",
        "runtime_execution_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "verification_summary": {"incomplete_coverage_cases_verified": 2},
        "verification_results": [
            {"gate_id": "V04_skip_on_incomplete_coverage", "status": "pass", "passed": True}
        ],
    }


def _runtime_payload(*, disabled: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_runtime_disabled",
            "runtime_version": "ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1",
            **_identity(),
        },
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": disabled,
        "runtime_default_state": "off" if disabled else "on",
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "last_disabled_run": {
            "status": "skipped_runtime_disabled",
            "shadow_row_count": 0,
            "writes_performed": False,
        },
        "runtime_contract": {
            "writes_performed": False,
            "skip_on_incomplete_coverage": True,
            "partial_scoring_allowed": False,
        },
    }


def _generalization_gates_payload(*, passed: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_gates",
            "gates_version": "ml-shadow-scorer-v1-generalization-audit-gates-v1",
            **_identity(),
        },
        "generalization_audit_gates_passed": passed,
        "second_surface_generalization_passed": passed,
        "material_lift_gate_passed": passed,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
    }


def _policy_payload(*, default_off: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "online_shadow_execution_policy_defined": True,
        "online_shadow_execution_enabled": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "runtime_isolation_policy": {
            "feature_flag": FEATURE_FLAG,
            "feature_flag_default": "off" if default_off else "on",
            "feature_flag_default_off": default_off,
        },
        "disable_and_rollback_policy": {"disable_switch_default": "off" if default_off else "on"},
        "separation_from_production_default_chain": {
            "future_online_shadow_gates_do_not_set_production_default_allowed": True,
            "production_default_allowed": False,
        },
        "allowed_write_scope": {
            "future_only_after_later_gates": True,
            "targets": ["isolated shadow/audit table"],
            "required_fields": ["run_id", "scorer_id"],
        },
        "forbidden_write_scope": ["ranking_runs", "paper_scores"],
        "observability_contract": {
            "component_coverage": True,
            "missing_learned_probability": True,
            "score_distributions": True,
            "top_k_overlap_with_heuristic": True,
            "rank_displacement": True,
            "family_counts": True,
            "output_completeness": True,
            "runtime_errors": True,
            "latency": True,
            "skipped_candidates_and_reasons": True,
            "skipped_ranking_run_records": True,
            "write_counts_by_isolated_target": True,
        },
        "future_runtime_verification_requirements": {
            "future_artifact": "ml-shadow-scorer-v1-runtime-isolation-verification",
            "must_prove": ["disable path tested", "skip-on-incomplete-coverage tested"],
        },
        "validation_snapshot_scope": {
            "ranking_run_id": "rank-9f4b2a2084",
            "candidate_pool_work_set_sha256": "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6",
        },
    }


def _production_plan_payload(*, blocked: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
            "overall_status": "research_only" if blocked else "ready",
        },
        "production_default_authorized": False if blocked else True,
        "targets": {"good_or_acceptable": {"production_eligible": False if blocked else True}},
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    verification: dict | None = None,
    runtime: dict | None = None,
    gates: dict | None = None,
    policy: dict | None = None,
    production: dict | None = None,
) -> dict[str, Path]:
    return {
        "runtime_isolation_verification_path": _write_json(tmp_path, "verification.json", verification or _verification_payload()),
        "online_shadow_runtime_path": _write_json(tmp_path, "runtime.json", runtime or _runtime_payload()),
        "generalization_audit_gates_path": _write_json(tmp_path, "gates.json", gates or _generalization_gates_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "production_readiness_plan_path": _write_json(tmp_path, "production.json", production or _production_plan_payload()),
    }


def _gate_by_id(payload: dict, gate_id: str) -> dict:
    return {gate["gate_id"]: gate for gate in payload["enablement_gate_contract"]}[gate_id]


def test_happy_path_writes_definition_only_gates(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
        **_paths(tmp_path),
        repo_root=tmp_path,
        generated_at="2026-05-26T00:00:00Z",
    )

    assert payload["online_shadow_enablement_gates_defined"] is True
    assert payload["online_shadow_enablement_gates_executed"] is False
    assert payload["all_prerequisite_gates_satisfied"] is True
    assert payload["recommended_next_stage"] == "run_ml_shadow_scorer_v1_online_shadow_enablement_gates_v1"
    assert payload["runtime_execution_authorized"] is False
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["shadow_scoring_allowed"] is False
    assert payload["production_default_allowed"] is False
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_online_shadow_enablement_gates"] is False
    assert blockers["missing_online_shadow_execution_authorization"] is True
    assert blockers["missing_production_readiness_authorization"] is True
    assert payload["evidence_summary"]["policy_scope_note"]["policy_used_as"] == (
        "default-off / write-scope / observability contract only"
    )


def test_rejects_runtime_isolation_verification_not_passed(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="runtime_isolation_verification_passed"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, verification=_verification_payload(passed=False)),
            repo_root=tmp_path,
        )


def test_rejects_runtime_not_disabled_by_default(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="runtime_disabled_by_default"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, runtime=_runtime_payload(disabled=False)),
            repo_root=tmp_path,
        )


def test_rejects_generalization_gates_not_passed(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="generalization_audit_gates_passed"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, gates=_generalization_gates_payload(passed=False)),
            repo_root=tmp_path,
        )


def test_rejects_policy_feature_flag_default_off_mismatch(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="feature_flag_default_off"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, policy=_policy_payload(default_off=False)),
            repo_root=tmp_path,
        )


def test_rejects_runtime_isolation_api_web_change_allowed(tmp_path: Path) -> None:
    verification = _verification_payload()
    verification["api_web_changes_allowed"] = True
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="api_web_changes_allowed"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, verification=verification),
            repo_root=tmp_path,
        )


def test_rejects_policy_production_default_allowed(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy["production_default_allowed"] = True
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="production_default_allowed"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, policy=policy),
            repo_root=tmp_path,
        )


def test_rejects_production_readiness_not_blocked(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="research_only"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, production=_production_plan_payload(blocked=False)),
            repo_root=tmp_path,
        )


def test_e01_to_e09_are_prerequisite_definitions_not_executed(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(**_paths(tmp_path), repo_root=tmp_path)
    gates = {gate["gate_id"]: gate for gate in payload["enablement_gate_contract"]}

    for index in range(1, 10):
        gate = gates[f"E{index:02d}_" + {
            1: "generalization_gates_passed",
            2: "runtime_disabled_by_default_implemented",
            3: "runtime_isolation_verification_passed",
            4: "feature_flag_default_off_and_disable_path_defined",
            5: "no_production_default_or_api_web_change",
            6: "shadow_write_isolation_requirement_documented_not_enabled",
            7: "observability_requirements_defined_for_future_online_run",
            8: "skip_on_incomplete_coverage_verified",
            9: "production_default_chain_remains_separate",
        }[index]]
        assert gate["definition_ready"] is True
        assert gate["prerequisite_evidence_present"] is True
        assert gate["enablement_gate_executed"] is False
        assert gate["decision"] == "definition_only_prerequisite_evidence_present"


def test_e10_records_enablement_decision_not_executed(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(**_paths(tmp_path), repo_root=tmp_path)
    e10 = {gate["gate_id"]: gate for gate in payload["enablement_gate_contract"]}[
        "E10_online_shadow_enablement_decision_not_executed"
    ]

    assert e10["enablement_gate_executed"] is False
    assert e10["decision"] == "online_shadow_enablement_decision_not_executed"
    assert e10["observed_evidence"]["online_shadow_execution_enabled"] is False
    assert e10["observed_evidence"]["runtime_execution_authorized"] is False


def test_e06_missing_write_scope_marks_prerequisite_missing(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy.pop("allowed_write_scope")
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
        **_paths(tmp_path, policy=policy),
        repo_root=tmp_path,
    )
    e06 = _gate_by_id(payload, "E06_shadow_write_isolation_requirement_documented_not_enabled")

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert e06["prerequisite_evidence_present"] is False
    assert e06["decision"] == "definition_only_missing_prerequisite_evidence"
    assert e06["observed_evidence"]["allowed_write_scope_present"] is False


def test_e07_missing_observability_contract_marks_prerequisite_missing(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy.pop("observability_contract")
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
        **_paths(tmp_path, policy=policy),
        repo_root=tmp_path,
    )
    e07 = _gate_by_id(payload, "E07_observability_requirements_defined_for_future_online_run")

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert e07["prerequisite_evidence_present"] is False
    assert e07["decision"] == "definition_only_missing_prerequisite_evidence"
    assert e07["observed_evidence"]["observability_contract_present"] is False


def test_e08_skip_on_incomplete_coverage_false_marks_prerequisite_missing(tmp_path: Path) -> None:
    runtime = _runtime_payload()
    runtime["runtime_contract"]["skip_on_incomplete_coverage"] = False
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
        **_paths(tmp_path, runtime=runtime),
        repo_root=tmp_path,
    )
    e08 = _gate_by_id(payload, "E08_skip_on_incomplete_coverage_verified")

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert e08["prerequisite_evidence_present"] is False
    assert e08["observed_evidence"]["runtime_contract_skip_on_incomplete_coverage"] is False


def test_e08_failed_v04_marks_prerequisite_missing(tmp_path: Path) -> None:
    verification = _verification_payload()
    verification["verification_results"][0]["status"] = "fail"
    verification["verification_results"][0]["passed"] = False
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
        **_paths(tmp_path, verification=verification),
        repo_root=tmp_path,
    )
    e08 = _gate_by_id(payload, "E08_skip_on_incomplete_coverage_verified")

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert e08["prerequisite_evidence_present"] is False
    assert e08["observed_evidence"]["verification_gate_v04_status"] == "fail"
    assert e08["observed_evidence"]["verification_gate_v04_passed"] is False


def test_rejects_identity_mismatch_across_runtime_verification_and_gates(tmp_path: Path) -> None:
    gates = copy.deepcopy(_generalization_gates_payload())
    gates["metadata"]["ranking_run_id"] = "rank-other"
    with pytest.raises(MLShadowScorerOnlineShadowEnablementGatesError, match="generalization gates metadata.ranking_run_id"):
        build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
            **_paths(tmp_path, gates=gates),
            repo_root=tmp_path,
        )


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "enablement-gates.json"
    out_md = tmp_path / "enablement-gates.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-enablement-gates",
        "--runtime-isolation-verification",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-runtime-isolation-verification-v1.json"),
        "--online-shadow-runtime",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json"),
        "--generalization-audit-gates",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json"),
        "--online-shadow-policy",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"),
        "--production-readiness-plan",
        str(REPO_ROOT / "docs/audit/ml-production-readiness-plan-v1.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_enablement_gates"
    assert payload["online_shadow_enablement_gates_defined"] is True
    assert payload["online_shadow_enablement_gates_executed"] is False
    assert payload["all_prerequisite_gates_satisfied"] is True
    assert payload["recommended_next_stage"] == "run_ml_shadow_scorer_v1_online_shadow_enablement_gates_v1"
    assert "True" in result.stdout
    assert "Online Shadow Enablement Gates" in out_md.read_text(encoding="utf-8")


def test_run_happy_path_writes_executed_json_markdown(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_run_payload(
        **_paths(tmp_path),
        repo_root=tmp_path,
        generated_at="2026-05-27T00:00:00Z",
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_enablement_gates_run"
    assert payload["metadata"]["gates_run_version"] == "ml-shadow-scorer-v1-online-shadow-enablement-gates-run-v1"
    assert payload["online_shadow_enablement_gates_defined"] is True
    assert payload["online_shadow_enablement_gates_executed"] is True
    assert payload["all_prerequisite_gates_satisfied"] is True
    assert payload["recommended_next_stage"] == "request_online_shadow_execution_authorization_v1"
    e01 = _gate_by_id({"enablement_gate_contract": payload["enablement_gate_results"]}, "E01_generalization_gates_passed")
    assert e01["enablement_gate_executed"] is True
    assert e01["decision"] == "passed"
    e10 = _gate_by_id(
        {"enablement_gate_contract": payload["enablement_gate_results"]},
        "E10_online_shadow_enablement_decision_not_executed",
    )
    assert e10["decision"] == "enablement_evaluation_only_not_authorized"
    assert e10["observed_evidence"]["all_prerequisite_gates_satisfied"] is True


def test_run_e06_missing_write_scope_writes_failed_artifact(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy.pop("allowed_write_scope")
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_run_payload(
        **_paths(tmp_path, policy=policy),
        repo_root=tmp_path,
    )
    e06 = _gate_by_id(
        {"enablement_gate_contract": payload["enablement_gate_results"]},
        "E06_shadow_write_isolation_requirement_documented_not_enabled",
    )

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert payload["recommended_next_stage"] == "harden_online_shadow_enablement_prerequisites_v1"
    assert e06["enablement_gate_executed"] is True
    assert e06["decision"] == "failed"
    assert e06["observed_evidence"]["allowed_write_scope_present"] is False


def test_run_e07_missing_observability_writes_failed_artifact(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy.pop("observability_contract")
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_run_payload(
        **_paths(tmp_path, policy=policy),
        repo_root=tmp_path,
    )
    e07 = _gate_by_id(
        {"enablement_gate_contract": payload["enablement_gate_results"]},
        "E07_observability_requirements_defined_for_future_online_run",
    )

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert payload["recommended_next_stage"] == "harden_online_shadow_enablement_prerequisites_v1"
    assert e07["decision"] == "failed"
    assert e07["observed_evidence"]["observability_contract_present"] is False


def test_run_e08_failed_v04_writes_failed_artifact(tmp_path: Path) -> None:
    verification = _verification_payload()
    verification["verification_results"][0]["status"] = "fail"
    verification["verification_results"][0]["passed"] = False
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_run_payload(
        **_paths(tmp_path, verification=verification),
        repo_root=tmp_path,
    )
    e08 = _gate_by_id(
        {"enablement_gate_contract": payload["enablement_gate_results"]},
        "E08_skip_on_incomplete_coverage_verified",
    )

    assert payload["all_prerequisite_gates_satisfied"] is False
    assert payload["recommended_next_stage"] == "harden_online_shadow_enablement_prerequisites_v1"
    assert e08["decision"] == "failed"
    assert e08["observed_evidence"]["verification_gate_v04_status"] == "fail"


def test_run_authorization_flags_remain_false_when_prerequisites_pass(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_run_payload(**_paths(tmp_path), repo_root=tmp_path)

    for key in (
        "online_shadow_execution_enabled",
        "shadow_scoring_allowed",
        "runtime_execution_authorized",
        "runtime_implementation_authorized",
        "production_default_allowed",
        "api_web_changes_allowed",
        "user_visible_ranking_changed",
    ):
        assert payload[key] is False
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_online_shadow_execution_authorization"] is True
    assert blockers["missing_production_readiness_authorization"] is True
    assert blockers["runtime_implementation_authorized"] is False


def test_run_cli_ingress_error_exits_nonzero_and_writes_no_artifact(tmp_path: Path) -> None:
    verification = _verification_payload()
    verification["api_web_changes_allowed"] = True
    paths = _paths(tmp_path, verification=verification)
    out_json = tmp_path / "run.json"
    out_md = tmp_path / "run.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-enablement-gates-run",
        "--runtime-isolation-verification",
        str(paths["runtime_isolation_verification_path"]),
        "--online-shadow-runtime",
        str(paths["online_shadow_runtime_path"]),
        "--generalization-audit-gates",
        str(paths["generalization_audit_gates_path"]),
        "--online-shadow-policy",
        str(paths["online_shadow_policy_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True)

    assert result.returncode != 0
    assert "api_web_changes_allowed" in result.stderr
    assert not out_json.exists()
    assert not out_md.exists()


def test_run_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "enablement-gates-run.json"
    out_md = tmp_path / "enablement-gates-run.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-enablement-gates-run",
        "--runtime-isolation-verification",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-runtime-isolation-verification-v1.json"),
        "--online-shadow-runtime",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json"),
        "--generalization-audit-gates",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json"),
        "--online-shadow-policy",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"),
        "--production-readiness-plan",
        str(REPO_ROOT / "docs/audit/ml-production-readiness-plan-v1.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_enablement_gates_run"
    assert payload["online_shadow_enablement_gates_executed"] is True
    assert payload["all_prerequisite_gates_satisfied"] is True
    assert payload["recommended_next_stage"] == "request_online_shadow_execution_authorization_v1"
    assert result.stdout.splitlines() == ["True", "request_online_shadow_execution_authorization_v1"]
    assert "Online Shadow Enablement Gates Run" in out_md.read_text(encoding="utf-8")


def test_new_module_has_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_enablement_gates.py").read_text(
        encoding="utf-8"
    )
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-enablement-gates"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
