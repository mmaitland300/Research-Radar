"""Tests for Phase 2 isolated audit write authorization grant artifacts."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant import (
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError,
    build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant_payload,
    write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "execution_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
}

COPY_RELS = sorted(
    {
        *FIXTURE_RELS.values(),
        "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-request-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-enablement-gates-run-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-run-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
        "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
        "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
        "docs/audit/ml-shadow-scorer-v1-runtime-isolation-verification-v1.json",
        "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json",
        "docs/audit/ml-production-readiness-plan-v1.json",
    }
)


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in COPY_RELS:
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build(
    root: Path,
    *,
    request_path: Path | None = None,
    proof_path: Path | None = None,
    plan_path: Path | None = None,
    execution_grant_path: Path | None = None,
) -> dict[str, Any]:
    return build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant_payload(
        phase2_write_authorization_request_path=request_path or _fixture(root, "request"),
        phase2_write_mode_proof_path=proof_path or _fixture(root, "proof"),
        phase2_write_mode_plan_path=plan_path or _fixture(root, "plan"),
        execution_authorization_grant_path=execution_grant_path or _fixture(root, "execution_grant"),
        repo_root=root,
        generated_at="2026-05-28T21:00:00Z",
    )


def _shadow_runs_contents(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*")}


def test_happy_path_writes_json_markdown_and_authorizes_bounded_pilot(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/grant.json"
    out_md = root / "docs/audit/grant.md"
    payload = write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant(
        phase2_write_authorization_request_path=_fixture(root, "request"),
        phase2_write_mode_proof_path=_fixture(root, "proof"),
        phase2_write_mode_plan_path=_fixture(root, "plan"),
        execution_authorization_grant_path=_fixture(root, "execution_grant"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
    )
    persisted = _load(out_json)

    assert payload["phase2_isolated_audit_write_authorization_granted"] is True
    assert persisted["phase2_write_pilot_authorized"] is True
    assert persisted["phase2_writes_authorized"] is True
    assert persisted["online_shadow_execution_enabled"] is False
    assert persisted["writes_performed"] is False
    assert persisted["runtime_writes_performed"] is False
    assert persisted["isolated_artifact_tree_writes_performed"] is False
    assert persisted["recommended_next_stage"] == "run_online_shadow_phase2_isolated_audit_write_pilot_v1"
    assert "Phase 2 Isolated Audit Write Authorization Grant" in out_md.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("phase2_isolated_audit_write_authorization_requested", False, "authorization_requested"),
        ("phase2_isolated_audit_write_authorization_granted", True, "authorization_granted"),
    ],
)
def test_rejects_request_not_requested_or_already_granted(
    tmp_path: Path,
    field: str,
    value: bool,
    match: str,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    request = copy.deepcopy(_load(_fixture(root, "request")))
    request[field] = value
    request_path = _write_json(root / "docs/audit/request-bad.json", request)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match=match):
        _build(root, request_path=request_path)


def test_rejects_request_phase2_writes_authorized_true_before_grant(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    request = copy.deepcopy(_load(_fixture(root, "request")))
    request["phase2_writes_authorized"] = True
    request_path = _write_json(root / "docs/audit/request-writes-true.json", request)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match="phase2_writes_authorized"):
        _build(root, request_path=request_path)


def test_rejects_proof_not_passed_or_forbidden_write_target_nonzero(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    proof = copy.deepcopy(_load(_fixture(root, "proof")))
    proof["phase2_write_mode_proof_passed"] = False
    proof_path = _write_json(root / "docs/audit/proof-not-passed.json", proof)
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match="phase2_write_mode_proof_passed"):
        _build(root, proof_path=proof_path)

    proof = copy.deepcopy(_load(_fixture(root, "proof")))
    proof["write_count_verification"]["write_counts_by_isolated_target"]["paper_scores"] = 1
    proof_path = _write_json(root / "docs/audit/proof-forbidden-write.json", proof)
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match="forbidden write targets"):
        _build(root, proof_path=proof_path)


def test_rejects_plan_target_root_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    plan = copy.deepcopy(_load(_fixture(root, "plan")))
    plan["isolated_write_target"]["root_path"] = "docs/audit/wrong/"
    plan_path = _write_json(root / "docs/audit/plan-wrong-root.json", plan)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match="root_path"):
        _build(root, plan_path=plan_path)


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        (("grant_decision", "owner"), "Someone Else", "owner"),
        (("grant_decision", "review_by"), "2026-08-28", "review_by"),
    ],
)
def test_rejects_prior_execution_grant_owner_or_review_by_mismatch(
    tmp_path: Path,
    path: tuple[str, str],
    value: str,
    match: str,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    grant = copy.deepcopy(_load(_fixture(root, "execution_grant")))
    grant[path[0]][path[1]] = value
    grant_path = _write_json(root / "docs/audit/execution-grant-bad.json", grant)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match=match):
        _build(root, execution_grant_path=grant_path)


@pytest.mark.parametrize("artifact", ["request", "proof", "plan", "execution_grant"])
def test_rejects_identity_mismatch_across_sources(tmp_path: Path, artifact: str) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = copy.deepcopy(_load(_fixture(root, artifact)))
    if artifact == "execution_grant":
        payload["grant_scope"]["scorer_id"] = "wrong-scorer"
    else:
        payload["metadata"]["scorer_id"] = "wrong-scorer"
    path = _write_json(root / f"docs/audit/{artifact}-identity-bad.json", payload)

    kwargs = {
        "request_path": path if artifact == "request" else None,
        "proof_path": path if artifact == "proof" else None,
        "plan_path": path if artifact == "plan" else None,
        "execution_grant_path": path if artifact == "execution_grant" else None,
    }
    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match="identity"):
        _build(root, **kwargs)


def test_rejects_tampered_input_sha(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    request = copy.deepcopy(_load(_fixture(root, "request")))
    request["metadata"]["inputs"][0]["sha256"] = "0" * 64
    request_path = _write_json(root / "docs/audit/request-hash-bad.json", request)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError, match="sha256 mismatch"):
        _build(root, request_path=request_path)


def test_proof_summary_file_hashes_are_preserved(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    request = _load(_fixture(root, "request"))
    payload = _build(root)

    expected = {
        record["relative_path"]: record["sha256"]
        for record in request["proof_summary"]["isolated_file_writes"]["files_written"]
    }
    observed = {
        record["relative_path"]: record["sha256"]
        for record in payload["proof_summary"]["isolated_file_writes"]["files_written"]
    }
    assert observed == expected


def test_allowed_target_and_required_observability_are_inherited(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    execution_grant = _load(_fixture(root, "execution_grant"))
    payload = _build(root)

    assert payload["write_mode_boundaries"]["allowed_write_targets"] == ["isolated_audit_shadow_artifacts"]
    assert payload["phase2_write_pilot_authorization"]["approved_write_target_type"] == "isolated_audit_shadow_artifacts"
    assert payload["required_observability"] == execution_grant["required_observability"]


def test_rollback_revocation_and_prod_db_boundaries_are_closed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _build(root)

    assert payload["rollback_disable_policy"]["flag_off_first"] is True
    assert payload["rollback_disable_policy"]["disable_switch"] == "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED=off"
    assert payload["rollback_disable_policy"]["never_delete_phase2_proof_root"] is True
    assert payload["revocation_policy"]["review_by"] == "2026-08-27"
    assert payload["revocation_policy"]["revoke_by_superseding_grant"] is True
    assert payload["write_mode_boundaries"]["db_writes_allowed"] is False
    assert payload["write_mode_boundaries"]["db_ddl_allowed"] is False
    assert payload["write_mode_boundaries"]["production_api_web_changes_allowed"] is False
    assert payload["write_mode_boundaries"]["production_default_changes_allowed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["api_web_changes_allowed"] is False
    assert payload["user_visible_ranking_changed"] is False


def test_blockers_are_grant_specific_not_request_stale(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _build(root)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["missing_online_shadow_execution_authorization"] is False
    assert blockers["missing_phase2_isolated_audit_write_pilot_authorization"] is False
    assert blockers["missing_phase2_write_mode_isolation_proof"] is False
    assert blockers["phase2_write_pilot_authorized"] is True
    assert blockers["phase2_writes_authorized"] is True
    assert blockers["blockers_changed_by_grant"] == [
        "missing_phase2_isolated_audit_write_pilot_authorization",
        "phase2_write_pilot_authorized",
        "phase2_writes_authorized",
    ]
    assert "blockers_changed_by_request" not in blockers


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "grant.json"
    out_md = tmp_path / "grant.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-authorization-grant",
        "--phase2-write-authorization-request",
        str(REPO_ROOT / FIXTURE_RELS["request"]),
        "--phase2-write-mode-proof",
        str(REPO_ROOT / FIXTURE_RELS["proof"]),
        "--phase2-write-mode-plan",
        str(REPO_ROOT / FIXTURE_RELS["plan"]),
        "--execution-authorization-grant",
        str(REPO_ROOT / FIXTURE_RELS["execution_grant"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["phase2_isolated_audit_write_authorization_granted"] is True
    assert payload["phase2_write_pilot_authorized"] is True
    assert result.stdout.splitlines() == [
        "True",
        "True",
        "run_online_shadow_phase2_isolated_audit_write_pilot_v1",
    ]
    assert "Phase 2 Isolated Audit Write Authorization Grant" in out_md.read_text(encoding="utf-8")


def test_no_database_url_no_runtime_call_and_no_shadow_runs_files(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_contents(root)

    _build(root)

    assert _shadow_runs_contents(root) == before
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant.py"
    ).read_text(encoding="utf-8")
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-authorization-grant"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
