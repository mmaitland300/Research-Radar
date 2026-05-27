"""Tests for ml-shadow-scorer-v1 runtime isolation verification."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module
from pipeline.ml_shadow_scorer_runtime_isolation_verification import (
    FEATURE_FLAG,
    MLShadowScorerRuntimeIsolationVerificationError,
    build_ml_shadow_scorer_runtime_isolation_verification_payload,
    source_guard_results,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _runtime_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_runtime_disabled",
            "runtime_version": "ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1",
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        },
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": True,
        "runtime_default_state": "off",
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "recommended_next_stage": "run_ml_shadow_scorer_v1_runtime_isolation_verification_v1",
        "last_disabled_run": {
            "status": "skipped_runtime_disabled",
            "shadow_row_count": 0,
            "writes_performed": False,
        },
        "shadow_and_production_blockers": {"missing_shadow_runtime_isolation_verification": True},
    }


def _gates_payload(*, passed: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_gates",
            "gates_version": "ml-shadow-scorer-v1-generalization-audit-gates-v1",
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
            "embedding_version": runtime_module.EMBEDDING_VERSION,
        },
        "generalization_audit_gates_passed": passed,
        "second_surface_generalization_passed": passed,
        "disabled_by_default_runtime_implementation_next_stage_allowed": passed,
        "recommended_next_stage": "implement_online_shadow_runtime_disabled_by_default",
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }


def _policy_payload(*, default_off: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_policy",
            "policy_version": "ml-shadow-scorer-v1-online-shadow-policy",
        },
        "runtime_implementation_authorized": False,
        "runtime_isolation_policy": {
            "feature_flag": FEATURE_FLAG,
            "feature_flag_default": "off" if default_off else "on",
            "feature_flag_default_off": default_off,
        },
        "disable_and_rollback_policy": {"disable_switch_default": "off" if default_off else "on"},
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    runtime: dict | None = None,
    gates: dict | None = None,
    policy: dict | None = None,
) -> dict[str, Path]:
    return {
        "online_shadow_runtime_path": _write_json(tmp_path, "runtime.json", runtime or _runtime_payload()),
        "generalization_audit_gates_path": _write_json(tmp_path, "gates.json", gates or _gates_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
    }


def test_happy_path_verification_passes(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_runtime_isolation_verification_payload(
        **_paths(tmp_path),
        repo_root=tmp_path,
        generated_at="2026-05-26T00:00:00Z",
    )

    assert payload["runtime_isolation_verification_passed"] is True
    assert payload["recommended_next_stage"] == "draft_online_shadow_execution_enablement_gates_v1"
    assert payload["verification_summary"]["flag_off_cases_verified"] == 6
    assert payload["verification_summary"]["flag_on_cases_verified"] == 3
    assert payload["source_guard_results"]["passed"] is True
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_shadow_runtime_isolation_verification"] is False
    assert blockers["missing_production_readiness_authorization"] is True
    assert blockers["online_shadow_execution_enabled"] is False
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["runtime_implementation_authorized"] is False


def test_fails_if_runtime_default_state_is_on(tmp_path: Path) -> None:
    runtime = _runtime_payload()
    runtime["runtime_default_state"] = "on"
    with pytest.raises(MLShadowScorerRuntimeIsolationVerificationError, match="runtime_default_state"):
        build_ml_shadow_scorer_runtime_isolation_verification_payload(
            **_paths(tmp_path, runtime=runtime),
            repo_root=tmp_path,
        )


@pytest.mark.parametrize("field,value", [("shadow_row_count", 1), ("writes_performed", True)])
def test_fails_if_disabled_run_has_rows_or_writes(tmp_path: Path, field: str, value: object) -> None:
    runtime = _runtime_payload()
    runtime["last_disabled_run"][field] = value
    with pytest.raises(MLShadowScorerRuntimeIsolationVerificationError, match=field):
        build_ml_shadow_scorer_runtime_isolation_verification_payload(
            **_paths(tmp_path, runtime=runtime),
            repo_root=tmp_path,
        )


def test_fails_if_gates_not_passed(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerRuntimeIsolationVerificationError, match="generalization_audit_gates_passed"):
        build_ml_shadow_scorer_runtime_isolation_verification_payload(
            **_paths(tmp_path, gates=_gates_payload(passed=False)),
            repo_root=tmp_path,
        )


def test_fails_if_policy_feature_flag_contract_mismatches(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerRuntimeIsolationVerificationError, match="feature flag default off"):
        build_ml_shadow_scorer_runtime_isolation_verification_payload(
            **_paths(tmp_path, policy=_policy_payload(default_off=False)),
            repo_root=tmp_path,
        )


def test_source_guard_catches_forbidden_imports(tmp_path: Path) -> None:
    module_path = tmp_path / "runtime.py"
    module_path.write_text("import psycopg\nfrom sklearn.linear_model import LogisticRegression\n", encoding="utf-8")

    result = source_guard_results(module_path, repo_root=tmp_path)

    assert result["passed"] is False
    assert result["forbidden_import_tokens_present"] == ["psycopg", "sklearn"]


def test_source_guard_catches_write_sql_strings(tmp_path: Path) -> None:
    module_path = tmp_path / "runtime.py"
    module_path.write_text('SQL = "INSERT INTO shadow_table VALUES (1)"\n', encoding="utf-8")

    result = source_guard_results(module_path, repo_root=tmp_path)

    assert result["passed"] is False
    assert result["write_sql_verbs_present"] == ["INSERT"]


def test_runtime_probe_gate_summaries_cover_required_behaviors(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_runtime_isolation_verification_payload(
        **_paths(tmp_path),
        repo_root=tmp_path,
    )
    gates = {result["gate_id"]: result for result in payload["verification_results"]}

    assert gates["V01_default_off_behavior"]["passed"] is True
    assert gates["V03_in_memory_scoring_only"]["passed"] is True
    assert gates["V04_skip_on_incomplete_coverage"]["passed"] is True
    assert gates["V05_label_field_rejection"]["passed"] is True
    assert gates["V06_identity_scope_rejection"]["passed"] is True
    assert gates["V10_runtime_isolation_verification_decision"]["passed"] is True
    probes = payload["runtime_probe_results"]
    assert probes["incomplete_coverage_results"]["missing_final_score"]["status"] == "skipped_incomplete_coverage"
    assert probes["label_field_rejection_results"]["reviewer_notes"]["status"] == "rejected_label_fields_present"
    assert probes["identity_rejection_results"]["wrong_candidate_pool_work_set_sha256"]["status"] == "rejected_identity_mismatch"


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "verification.json"
    out_md = tmp_path / "verification.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-runtime-isolation-verification",
        "--online-shadow-runtime",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json"),
        "--generalization-audit-gates",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json"),
        "--online-shadow-policy",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_runtime_isolation_verification"
    assert payload["runtime_isolation_verification_passed"] is True
    assert payload["recommended_next_stage"] == "draft_online_shadow_execution_enablement_gates_v1"
    assert "True" in result.stdout
    assert "Runtime Isolation Verification" in out_md.read_text(encoding="utf-8")


def test_verification_module_has_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_runtime_isolation_verification.py").read_text(
        encoding="utf-8"
    )
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-runtime-isolation-verification"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]


def test_rejects_identity_mismatch_between_runtime_and_gates(tmp_path: Path) -> None:
    gates = copy.deepcopy(_gates_payload())
    gates["metadata"]["ranking_run_id"] = "rank-other"
    with pytest.raises(MLShadowScorerRuntimeIsolationVerificationError, match="gates metadata.ranking_run_id"):
        build_ml_shadow_scorer_runtime_isolation_verification_payload(
            **_paths(tmp_path, gates=gates),
            repo_root=tmp_path,
        )
