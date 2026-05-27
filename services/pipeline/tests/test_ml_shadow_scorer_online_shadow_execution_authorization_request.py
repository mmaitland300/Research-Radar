"""Tests for ml-shadow-scorer-v1 online shadow execution authorization request artifacts."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.ml_shadow_scorer_online_shadow_runtime as runtime_module
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_request import (
    MLShadowScorerOnlineShadowExecutionAuthorizationRequestError,
    build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload,
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


def _input_chain(tmp_path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for name in (
        "runtime_isolation_verification",
        "online_shadow_runtime",
        "generalization_audit_gates",
        "online_shadow_policy",
        "production_readiness_plan",
    ):
        path = _write_json(tmp_path, f"inputs/{name}.json", {"name": name})
        records.append({"name": name, "path": f"inputs/{name}.json", "sha256": sha256_file(path)})
    return records


def _gate(gate_id: str, *, decision: str = "passed") -> dict:
    return {
        "gate_id": gate_id,
        "title": gate_id.replace("_", " "),
        "enablement_gate_executed": True,
        "decision": decision,
        "expected_evidence": ["expected"],
        "observed_evidence": {"observed": True},
        "rationale": "fixture",
    }


def _run_payload(
    tmp_path: Path,
    *,
    all_satisfied: bool = True,
    failed_gate_id: str | None = None,
    include_results: bool = True,
    source_contract_failed: bool = False,
) -> dict:
    results = [_gate(gate_id, decision="failed" if gate_id == failed_gate_id else "passed") for gate_id in PREREQUISITE_GATE_IDS]
    results.append(
        _gate("E10_online_shadow_enablement_decision_not_executed", decision="enablement_evaluation_only_not_authorized")
    )
    payload = {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_online_shadow_enablement_gates_run",
            "gates_run_version": "ml-shadow-scorer-v1-online-shadow-enablement-gates-run-v1",
            "inputs": _input_chain(tmp_path),
            **_identity(),
        },
        "online_shadow_enablement_gates_defined": True,
        "online_shadow_enablement_gates_executed": True,
        "all_prerequisite_gates_satisfied": all_satisfied,
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
        },
        "source_enablement_gate_contract": [
            {
                "gate_id": "E01_generalization_gates_passed",
                "decision": "definition_only_missing_prerequisite_evidence" if source_contract_failed else "definition_only_prerequisite_evidence_present",
            }
        ],
    }
    if include_results:
        payload["enablement_gate_results"] = results
    return payload


def _write_run(tmp_path: Path, payload: dict) -> Path:
    return _write_json(tmp_path, "run.json", payload)


def test_happy_path_writes_request_json_md_semantics(tmp_path: Path) -> None:
    run_payload = _run_payload(tmp_path)
    run_path = _write_run(tmp_path, run_payload)
    payload = build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
        enablement_gates_run_path=run_path,
        repo_root=tmp_path,
        generated_at="2026-05-27T00:00:00Z",
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_execution_authorization_request"
    assert payload["online_shadow_execution_authorization_requested"] is True
    assert payload["online_shadow_execution_authorized"] is False
    assert payload["authorization_granted"] is False
    assert payload["shadow_and_production_blockers"]["missing_online_shadow_execution_authorization"] is True
    assert payload["shadow_and_production_blockers"]["blockers_unchanged_by_request"] is True
    assert payload["recommended_next_stage"] == "record_online_shadow_execution_authorization_grant_v1"
    assert all(record["verification_status"] == "confirmed" for record in payload["metadata"]["verified_input_chain"])


def test_rejects_run_with_all_prerequisite_gates_false(tmp_path: Path) -> None:
    run_path = _write_run(tmp_path, _run_payload(tmp_path, all_satisfied=False))
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationRequestError, match="all_prerequisite_gates_satisfied"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
            enablement_gates_run_path=run_path,
            repo_root=tmp_path,
        )


def test_rejects_failed_gate_in_enablement_gate_results(tmp_path: Path) -> None:
    run_path = _write_run(tmp_path, _run_payload(tmp_path, failed_gate_id="E06_shadow_write_isolation_requirement_documented_not_enabled"))
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationRequestError, match="E06_shadow_write_isolation"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
            enablement_gates_run_path=run_path,
            repo_root=tmp_path,
        )


def test_rejects_missing_enablement_gate_results(tmp_path: Path) -> None:
    run_path = _write_run(tmp_path, _run_payload(tmp_path, include_results=False))
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationRequestError, match="enablement_gate_results"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
            enablement_gates_run_path=run_path,
            repo_root=tmp_path,
        )


def test_rejects_metadata_input_hash_mismatch(tmp_path: Path) -> None:
    run_payload = _run_payload(tmp_path)
    (tmp_path / run_payload["metadata"]["inputs"][0]["path"]).write_text('{"tampered": true}\n', encoding="utf-8")
    run_path = _write_run(tmp_path, run_payload)
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationRequestError, match="sha256 mismatch"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
            enablement_gates_run_path=run_path,
            repo_root=tmp_path,
        )


def test_rejects_missing_metadata_input_file(tmp_path: Path) -> None:
    run_payload = _run_payload(tmp_path)
    (tmp_path / run_payload["metadata"]["inputs"][0]["path"]).unlink()
    run_path = _write_run(tmp_path, run_payload)
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationRequestError, match="missing on disk"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
            enablement_gates_run_path=run_path,
            repo_root=tmp_path,
        )


def test_ignores_source_enablement_gate_contract_for_pass_fail(tmp_path: Path) -> None:
    run_path = _write_run(tmp_path, _run_payload(tmp_path, source_contract_failed=True))
    payload = build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
        enablement_gates_run_path=run_path,
        repo_root=tmp_path,
    )

    assert payload["all_prerequisite_gates_satisfied"] is True
    assert payload["enablement_summary"]["failed_gate_ids"] == []
    assert payload["consumer_guidance"]["definition_provenance_note"].startswith("source_enablement_gate_contract")


def test_blockers_are_copied_and_authorization_flags_remain_false(tmp_path: Path) -> None:
    run_payload = _run_payload(tmp_path)
    run_blockers = copy.deepcopy(run_payload["shadow_and_production_blockers"])
    run_path = _write_run(tmp_path, run_payload)
    payload = build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
        enablement_gates_run_path=run_path,
        repo_root=tmp_path,
    )

    for key, value in run_blockers.items():
        assert payload["shadow_and_production_blockers"][key] == value
    assert payload["shadow_and_production_blockers"]["missing_online_shadow_execution_authorization"] is True
    assert payload["shadow_and_production_blockers"]["missing_production_readiness_authorization"] is True
    for key in (
        "online_shadow_execution_authorized",
        "authorization_granted",
        "online_shadow_execution_enabled",
        "shadow_scoring_allowed",
        "runtime_execution_authorized",
        "runtime_implementation_authorized",
        "production_default_allowed",
        "api_web_changes_allowed",
        "user_visible_ranking_changed",
    ):
        assert payload[key] is False


def test_rejects_run_with_authorization_flag_true(tmp_path: Path) -> None:
    run_payload = _run_payload(tmp_path)
    run_payload["runtime_execution_authorized"] = True
    run_path = _write_run(tmp_path, run_payload)
    with pytest.raises(MLShadowScorerOnlineShadowExecutionAuthorizationRequestError, match="runtime_execution_authorized"):
        build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
            enablement_gates_run_path=run_path,
            repo_root=tmp_path,
        )


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "request.json"
    out_md = tmp_path / "request.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-execution-authorization-request",
        "--enablement-gates-run",
        str(REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-enablement-gates-run-v1.json"),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_execution_authorization_request"
    assert payload["online_shadow_execution_authorization_requested"] is True
    assert payload["online_shadow_execution_authorized"] is False
    assert result.stdout.splitlines() == ["True", "False", "record_online_shadow_execution_authorization_grant_v1"]
    assert "NOT AUTHORIZED" in out_md.read_text(encoding="utf-8")


def test_new_cli_has_no_database_url_and_module_has_no_forbidden_imports() -> None:
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-execution-authorization-request"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]

    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_execution_authorization_request.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
