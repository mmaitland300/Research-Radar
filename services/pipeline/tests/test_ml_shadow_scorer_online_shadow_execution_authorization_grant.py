"""Tests for ml-shadow-scorer-v1 online shadow execution authorization grant artifacts."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_grant import (
    MLShadowScorerOnlineShadowExecutionAuthorizationGrantError,
    build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


PREREQUISITE_GATE_IDS = (
    "E01_generalization_gates_passed",
    "E02_runtime_disabled_by_default_implemented",
    "E03_runtime_isolation_verification_passed",
    "E04_feature_flag_default_off_and_disable_path_defined",
    "E05_no_production_default_or_api_web_change",
    "E06_shadow_write_isolation_requirement_documented_not_enabled",
    "E07_observability_requirements_defined_for_future_online_run",
    "E08_skip_on_incomplete_coverage_verified",
    "E09_production_default_chain_remains_separate",
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


def _upstream_chain(tmp_path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for name in (
        "runtime_isolation_verification",
        "online_shadow_runtime",
        "generalization_audit_gates",
        "online_shadow_policy",
        "production_readiness_plan",
    ):
        path = _write_json(tmp_path, f"inputs/{name}.json", {"name": name})
        records.append({"name": name, "path": f"inputs/{name}.json", "sha256": sha256_file(path), "verification_status": "confirmed"})
    return records


def _gate(gate_id: str, *, decision: str = "passed") -> dict:
    return {
        "gate_id": gate_id,
        "title": gate_id,
        "enablement_gate_executed": True,
        "decision": decision,
    }


def _run_payload(*, failed_gate_id: str | None = None) -> dict:
    results = [_gate(gate_id, decision="failed" if gate_id == failed_gate_id else "passed") for gate_id in PREREQUISITE_GATE_IDS]
    results.append(_gate("E10_online_shadow_enablement_decision_not_executed", decision="enablement_evaluation_only_not_authorized"))
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_enablement_gates_run",
            "gates_run_version": "ml-shadow-scorer-v1-online-shadow-enablement-gates-run-v1",
            **_identity(),
        },
        "online_shadow_enablement_gates_executed": True,
        "all_prerequisite_gates_satisfied": True,
        "enablement_gate_results": results,
    }


def _request_payload(
    tmp_path: Path,
    *,
    requested: bool = True,
    already_granted: bool = False,
    prerequisites: bool = True,
    failed_run_gate_id: str | None = None,
) -> dict:
    chain = _upstream_chain(tmp_path)
    run_path = _write_json(tmp_path, "run.json", _run_payload(failed_gate_id=failed_run_gate_id))
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_execution_authorization_request",
            "request_version": "ml-shadow-scorer-v1-online-shadow-execution-authorization-request-v1",
            "inputs": [{"name": "enablement_gates_run", "path": "run.json", "sha256": sha256_file(run_path)}],
            "verified_input_chain": chain,
            **_identity(),
        },
        "online_shadow_execution_authorization_requested": requested,
        "online_shadow_execution_authorized": already_granted,
        "authorization_granted": already_granted,
        "all_prerequisite_gates_satisfied": prerequisites,
        "recommended_next_stage": "record_online_shadow_execution_authorization_grant_v1",
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "shadow_and_production_blockers": {
            "missing_generalization_audit_on_second_surface": False,
            "missing_generalization_audit_gates": False,
            "missing_online_shadow_implementation_disabled_by_default": False,
            "missing_shadow_runtime_isolation_verification": False,
            "missing_online_shadow_enablement_gates": False,
            "missing_online_shadow_execution_authorization": True,
            "missing_production_readiness_authorization": True,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "runtime_implementation_authorized": False,
            "runtime_execution_authorized": False,
            "blockers_unchanged_by_request": True,
        },
    }


def _write_request(tmp_path: Path, payload: dict) -> Path:
    return _write_json(tmp_path, "request.json", payload)


def test_happy_path_records_bounded_pilot_grant(tmp_path: Path) -> None:
    request_path = _write_request(tmp_path, _request_payload(tmp_path))
    payload = build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload(
        authorization_request_path=request_path,
        repo_root=tmp_path,
        generated_at="2026-05-27T00:00:00Z",
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_execution_authorization_grant"
    assert payload["grant_decision"]["decision"] == "granted"
    assert payload["grant_decision"]["owner"] == "Matt Maitland"
    assert payload["grant_decision"]["review_by"] == "2026-08-27"
    assert payload["grant_decision"]["expiry_date"] == "2026-08-27"
    assert payload["authorization_granted"] is True
    assert payload["online_shadow_execution_authorized"] is True
    assert payload["missing_online_shadow_execution_authorization"] is False
    assert payload["shadow_and_production_blockers"]["missing_online_shadow_execution_authorization"] is False
    assert payload["shadow_and_production_blockers"]["missing_production_readiness_authorization"] is True
    assert payload["shadow_and_production_blockers"]["runtime_execution_authorized"] is True
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is True
    assert payload["shadow_and_production_blockers"]["authorization_scope"] == "bounded_non_prod_pilot_only"
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["write_mode_policy"]["phase_1"] == "no_writes"
    assert payload["write_mode_policy"]["phase_1_writes_allowed"] is False
    assert payload["pilot_authorization"]["initial_ranking_run_ids"] == [runtime_module.RANKING_RUN_ID]
    assert payload["runtime_implementation_authorized"] is False
    policy_contract = payload["required_observability"]["policy_contract"]
    for key in (
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
    ):
        assert policy_contract[key] is True
    for field in (
        "status",
        "shadow_row_count",
        "writes_performed",
        "production_default_changed",
        "user_visible_ranking_changed",
        "api_web_changes_allowed",
        "runtime_feature_flag_value",
    ):
        assert field in payload["required_observability"]["run_level_fields"]
    assert payload["recommended_next_stage"] == "prepare_online_shadow_phase1_no_write_pilot_plan_v1"


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"requested": False}, "online_shadow_execution_authorization_requested"),
        ({"already_granted": True}, "online_shadow_execution_authorized"),
        ({"prerequisites": False}, "all_prerequisite_gates_satisfied"),
    ],
)
def test_rejects_invalid_request_states(tmp_path: Path, kwargs: dict, match: str) -> None:
    request_path = _write_request(tmp_path, _request_payload(tmp_path, **kwargs))
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationGrantError, match=match):
        build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload(
            authorization_request_path=request_path,
            repo_root=tmp_path,
        )


def test_rejects_tampered_verified_chain_sha(tmp_path: Path) -> None:
    request = _request_payload(tmp_path)
    (tmp_path / request["metadata"]["verified_input_chain"][0]["path"]).write_text('{"tampered": true}\n', encoding="utf-8")
    request_path = _write_request(tmp_path, request)

    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationGrantError, match="sha256 mismatch"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload(
            authorization_request_path=request_path,
            repo_root=tmp_path,
        )


def test_rejects_stale_enablement_run_with_failed_gate(tmp_path: Path) -> None:
    request_path = _write_request(
        tmp_path,
        _request_payload(tmp_path, failed_run_gate_id="E06_shadow_write_isolation_requirement_documented_not_enabled"),
    )

    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationGrantError, match="E06_shadow_write_isolation"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_grant_payload(
            authorization_request_path=request_path,
            repo_root=tmp_path,
        )


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "grant.json"
    out_md = tmp_path / "grant.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-execution-authorization-grant",
        "--authorization-request",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-request-v1.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_execution_authorization_grant"
    assert payload["authorization_granted"] is True
    assert payload["online_shadow_execution_authorized"] is True
    assert payload["online_shadow_execution_enabled"] is False
    assert result.stdout.splitlines() == ["True", "True", "prepare_online_shadow_phase1_no_write_pilot_plan_v1"]
    assert "Online Shadow Execution Authorization Grant" in out_md.read_text(encoding="utf-8")


def test_cli_has_no_database_url_and_module_has_no_forbidden_imports() -> None:
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-execution-authorization-grant"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]

    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_execution_authorization_grant.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
