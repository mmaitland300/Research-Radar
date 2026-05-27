"""Tests for ml-shadow-scorer-v1 Phase 1 no-write online shadow pilot plans."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan import (
    MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError,
    build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

POLICY_CONTRACT_KEYS = (
    "component_coverage",
    "missing_learned_probability",
    "score_distributions",
    "top_k_overlap_with_heuristic",
    "rank_displacement",
    "family_counts",
    "output_completeness",
    "runtime_errors",
    "latency",
    "skipped_candidates_and_reasons",
    "skipped_ranking_run_records",
    "write_counts_by_isolated_target",
)
RUN_LEVEL_FIELDS = (
    "status",
    "shadow_row_count",
    "writes_performed",
    "production_default_changed",
    "user_visible_ranking_changed",
    "api_web_changes_allowed",
    "runtime_feature_flag_value",
)


def _identity() -> dict:
    return {
        "ranking_run_id": runtime_module.RANKING_RUN_ID,
        "family": runtime_module.FAMILY,
        "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
        "corpus_snapshot_version": runtime_module.CORPUS_SNAPSHOT_VERSION,
        "embedding_version": runtime_module.EMBEDDING_VERSION,
    }


def _write_json(tmp_path: Path, relative_path: str, payload: dict) -> Path:
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _record(tmp_path: Path, name: str, relative_path: str, payload: dict | None = None) -> dict[str, str]:
    path = _write_json(tmp_path, relative_path, payload or {"name": name})
    return {"name": name, "path": relative_path, "sha256": sha256_file(path)}


def _grant_payload(tmp_path: Path) -> dict:
    metadata_inputs = [_record(tmp_path, "authorization_request", "request.json")]
    verified_request_inputs = [_record(tmp_path, "enablement_gates_run", "run.json")]
    verified_input_chain = [
        _record(tmp_path, "runtime_isolation_verification", "inputs/runtime_isolation_verification.json"),
        _record(tmp_path, "online_shadow_runtime", "inputs/online_shadow_runtime.json"),
        _record(tmp_path, "generalization_audit_gates", "inputs/generalization_audit_gates.json"),
        _record(tmp_path, "online_shadow_policy", "inputs/online_shadow_policy.json"),
        _record(tmp_path, "production_readiness_plan", "inputs/production_readiness_plan.json"),
    ]
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_execution_authorization_grant",
            "grant_version": "ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1",
            "inputs": metadata_inputs,
            "verified_request_inputs": verified_request_inputs,
            "verified_input_chain": verified_input_chain,
            **_identity(),
        },
        "grant_decision": {
            "decision": "granted",
            "owner": "Matt Maitland",
            "review_by": "2026-08-27",
            "expiry_date": "2026-08-27",
        },
        "authorization_granted": True,
        "online_shadow_execution_authorized": True,
        "missing_online_shadow_execution_authorization": False,
        "online_shadow_execution_enabled": False,
        "feature_flag_default_off": True,
        "flag_may_be_enabled_only_in_pilot_env": True,
        "runtime_execution_authorized": True,
        "shadow_scoring_allowed": True,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_production_readiness_authorization": True,
        "grant_scope": {
            "ranking_run_id": runtime_module.RANKING_RUN_ID,
            "family": runtime_module.FAMILY,
            "candidate_pool_work_set_sha256": runtime_module.CANDIDATE_POOL_WORK_SET_SHA256,
            "formula_id": runtime_module.FORMULA_ID,
            "scorer_id": runtime_module.SCORER_ID,
            "scope": "fixture",
            "basis": "fixture",
        },
        "pilot_authorization": {
            "runtime_execution_authorized": True,
            "shadow_scoring_allowed": True,
            "environments": "non-prod pilot only",
            "initial_ranking_run_ids": [runtime_module.RANKING_RUN_ID],
        },
        "pilot_bounds": {
            "non_prod_only": True,
            "one_approved_ranking_run_to_start": True,
            "flag_on_only_in_pilot_env": True,
            "manual_or_scheduled_jobs_only": True,
            "second_surface_identity_only": True,
            "no_fleet_wide_enable": True,
            "read_only_prod_inputs": True,
            "skip_incomplete_coverage": True,
        },
        "write_mode_policy": {
            "phase_1": "no_writes",
            "phase_1_writes_allowed": False,
            "phase_2": "isolated_audit_only_writes_after_phase1_and_write_mode_proof",
            "phase_2_requires_separate_authorization": True,
        },
        "required_observability": {
            "policy_contract": {key: True for key in POLICY_CONTRACT_KEYS},
            "run_level_fields": list(RUN_LEVEL_FIELDS),
        },
        "basis_artifacts": {"authorization_request": "request.json", "enablement_gates_run": "run.json"},
        "shadow_and_production_blockers": {
            "missing_generalization_audit_on_second_surface": False,
            "missing_generalization_audit_gates": False,
            "missing_online_shadow_implementation_disabled_by_default": False,
            "missing_shadow_runtime_isolation_verification": False,
            "missing_online_shadow_enablement_gates": False,
            "missing_online_shadow_execution_authorization": False,
            "missing_production_readiness_authorization": True,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": True,
            "production_default_allowed": False,
            "runtime_implementation_authorized": False,
            "runtime_execution_authorized": True,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "authorization_scope": "bounded_non_prod_pilot_only",
        },
    }


def _runtime_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_runtime_disabled",
            "runtime_version": "ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1",
            **_identity(),
        },
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": True,
        "runtime_default_state": "off",
        "runtime_feature_flag": runtime_module.FEATURE_FLAG,
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
    }


def _policy_payload() -> dict:
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
            "feature_flag": runtime_module.FEATURE_FLAG,
            "feature_flag_default": "off",
            "feature_flag_default_off": True,
        },
    }


def _paths(tmp_path: Path, *, grant: dict | None = None, runtime: dict | None = None, policy: dict | None = None) -> dict:
    return {
        "authorization_grant_path": _write_json(tmp_path, "grant.json", grant or _grant_payload(tmp_path)),
        "online_shadow_runtime_path": _write_json(tmp_path, "runtime.json", runtime or _runtime_payload()),
        "online_shadow_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
    }


def test_happy_path_writes_phase1_plan_semantics(tmp_path: Path) -> None:
    grant = _grant_payload(tmp_path)
    payload = build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
        **_paths(tmp_path, grant=grant),
        repo_root=tmp_path,
        generated_at="2026-05-27T00:00:00Z",
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan"
    assert payload["phase1_no_write_pilot_plan_defined"] is True
    assert payload["phase1_no_write_pilot_executed"] is False
    assert payload["online_shadow_execution_authorized"] is True
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["runtime_execution_authorization_scope"] == "bounded_non_prod_pilot_only"
    assert payload["shadow_scoring_allowed_scope"] == "bounded_non_prod_pilot_only"
    assert payload["writes_allowed"] is False
    assert payload["missing_online_shadow_execution_authorization"] is False
    assert payload["missing_production_readiness_authorization"] is True
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["runtime_execution_authorized"] is True
    assert blockers["shadow_scoring_allowed"] is True
    assert blockers["missing_online_shadow_execution_authorization"] is False
    assert blockers["phase1_no_write_pilot_executed"] is False
    assert payload["recommended_next_stage"] == "implement_online_shadow_phase1_no_write_pilot_runner_v1"


def test_blocker_authorization_scope_is_optional_but_validated_if_present(tmp_path: Path) -> None:
    grant = _grant_payload(tmp_path)
    del grant["shadow_and_production_blockers"]["authorization_scope"]
    payload = build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
        **_paths(tmp_path, grant=grant),
        repo_root=tmp_path,
    )
    assert "authorization_scope" not in payload["shadow_and_production_blockers"]

    grant = _grant_payload(tmp_path)
    grant["shadow_and_production_blockers"]["authorization_scope"] = "production"
    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError, match="authorization_scope"):
        build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
            **_paths(tmp_path, grant=grant),
            repo_root=tmp_path,
        )


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda grant: grant["grant_decision"].update({"decision": "denied"}), "grant_decision.decision"),
        (lambda grant: grant["grant_decision"].update({"review_by": "2026-08-28"}), "grant_decision.review_by"),
        (lambda grant: grant["write_mode_policy"].update({"phase_1_writes_allowed": True}), "phase_1_writes_allowed"),
        (lambda grant: grant.update({"online_shadow_execution_enabled": True}), "online_shadow_execution_enabled"),
        (lambda grant: grant.update({"production_default_allowed": True}), "production_default_allowed"),
        (lambda grant: grant.update({"api_web_changes_allowed": True}), "api_web_changes_allowed"),
        (lambda grant: grant.update({"user_visible_ranking_changed": True}), "user_visible_ranking_changed"),
        (
            lambda grant: grant["shadow_and_production_blockers"].update({"runtime_execution_authorized": False}),
            "runtime_execution_authorized",
        ),
    ],
)
def test_rejects_invalid_grant_states(tmp_path: Path, mutate, match: str) -> None:
    grant = _grant_payload(tmp_path)
    mutate(grant)
    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError, match=match):
        build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
            **_paths(tmp_path, grant=grant),
            repo_root=tmp_path,
        )


@pytest.mark.parametrize(
    "record_path",
    [
        ("metadata", "inputs", 0),
        ("metadata", "verified_request_inputs", 0),
        ("metadata", "verified_input_chain", 0),
    ],
)
def test_rejects_tampered_grant_input_chain(tmp_path: Path, record_path: tuple[str, str, int]) -> None:
    grant = _grant_payload(tmp_path)
    record = grant[record_path[0]][record_path[1]][record_path[2]]
    (tmp_path / record["path"]).write_text('{"tampered": true}\n', encoding="utf-8")
    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError, match="sha256 mismatch"):
        build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
            **_paths(tmp_path, grant=grant),
            repo_root=tmp_path,
        )


def test_rejects_runtime_not_disabled_by_default(tmp_path: Path) -> None:
    runtime = _runtime_payload()
    runtime["runtime_disabled_by_default"] = False
    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError, match="runtime_disabled_by_default"):
        build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
            **_paths(tmp_path, runtime=runtime),
            repo_root=tmp_path,
        )


def test_rejects_policy_feature_flag_default_on(tmp_path: Path) -> None:
    policy = _policy_payload()
    policy["runtime_isolation_policy"]["feature_flag_default_off"] = False
    with pytest.raises(MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError, match="feature_flag_default_off"):
        build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
            **_paths(tmp_path, policy=policy),
            repo_root=tmp_path,
        )


def test_observability_no_write_contract_and_rollback_are_inherited(tmp_path: Path) -> None:
    grant = _grant_payload(tmp_path)
    payload = build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
        **_paths(tmp_path, grant=grant),
        repo_root=tmp_path,
    )

    assert payload["observability_plan"]["inherited_from_grant"] == grant["required_observability"]
    assert set(payload["observability_plan"]["policy_contract"]) == set(POLICY_CONTRACT_KEYS)
    assert payload["observability_plan"]["run_level_fields"] == list(RUN_LEVEL_FIELDS)
    assert payload["observability_plan"]["phase_1_expectation"]["all_write_counts_must_be_zero"] is True
    assert payload["no_write_execution_contract"]["writes_allowed"] is False
    assert "ranking_runs" in payload["no_write_execution_contract"]["forbidden_write_targets"]
    assert payload["rollback_disable_drill"]["preflight_verify_flag_off_skips_runtime"] is True
    assert payload["rollback_disable_drill"]["postflight_set_flag_off_and_verify_skipped_runtime_disabled"] is True


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "plan.json"
    out_md = tmp_path / "plan.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase1-no-write-pilot-plan",
        "--authorization-grant",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json"),
        "--online-shadow-runtime",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json"),
        "--online-shadow-policy",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan"
    assert payload["phase1_no_write_pilot_plan_defined"] is True
    assert payload["phase1_no_write_pilot_executed"] is False
    assert payload["recommended_next_stage"] == "implement_online_shadow_phase1_no_write_pilot_runner_v1"
    assert result.stdout.splitlines() == ["True", "False", "implement_online_shadow_phase1_no_write_pilot_runner_v1"]
    assert "Online Shadow Phase 1 No-Write Pilot Plan" in out_md.read_text(encoding="utf-8")


def test_cli_has_no_database_url_and_module_has_no_forbidden_imports() -> None:
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase1-no-write-pilot-plan"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]

    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
