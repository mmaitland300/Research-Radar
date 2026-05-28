"""Tests for Phase 2 isolated audit write authorization request artifacts."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request import (
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError,
    build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "phase1_run": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-run-v1.json",
}

COPY_RELS = sorted(
    {
        *FIXTURE_RELS.values(),
        "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-request-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1.json",
        "docs/audit/ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1.json",
        "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
        "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
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
    proof_path: Path | None = None,
    plan_path: Path | None = None,
    grant_path: Path | None = None,
    review_path: Path | None = None,
) -> dict[str, Any]:
    return build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request_payload(
        phase2_write_mode_proof_path=proof_path or _fixture(root, "proof"),
        phase2_write_mode_plan_path=plan_path or _fixture(root, "plan"),
        authorization_grant_path=grant_path or _fixture(root, "grant"),
        phase1_no_write_pilot_review_path=review_path or _fixture(root, "review"),
        repo_root=root,
        generated_at="2026-05-28T20:00:00Z",
    )


def _shadow_runs_contents(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {
        str(path.relative_to(root)).replace("\\", "/")
        for path in shadow_root.rglob("*")
    }


def test_happy_path_builds_request_from_committed_fixtures(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _build(root)

    assert payload["metadata"]["artifact_type"] == (
        "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request"
    )
    assert payload["phase2_isolated_audit_write_authorization_requested"] is True
    assert payload["phase2_isolated_audit_write_authorization_granted"] is False
    assert payload["phase2_write_pilot_authorized"] is False
    assert payload["phase2_writes_authorized"] is False
    assert payload["missing_phase2_write_mode_isolation_proof"] is False
    assert payload["missing_phase2_isolated_audit_write_pilot_authorization"] is True
    assert payload["runtime_writes_performed"] is False
    assert payload["isolated_artifact_tree_writes_performed"] is False
    assert payload["writes_performed"] is False
    assert payload["recommended_next_stage"] == (
        "record_online_shadow_phase2_isolated_audit_write_authorization_grant_v1"
    )
    assert payload["proof_summary"]["joined_candidate_count"] == 528
    assert payload["requested_grant_scope"]["write_target_root"] == (
        "docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/"
    )


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("phase2_write_mode_proof_passed", False, "phase2_write_mode_proof_passed"),
        ("recommended_next_stage", "wrong_next_stage", "recommended_next_stage"),
    ],
)
def test_rejects_proof_not_passed_or_wrong_next_stage(
    tmp_path: Path,
    field: str,
    value: Any,
    match: str,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    proof = copy.deepcopy(_load(_fixture(root, "proof")))
    proof[field] = value
    proof_path = _write_json(root / "docs/audit/proof-bad.json", proof)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError, match=match):
        _build(root, proof_path=proof_path)


def test_rejects_proof_with_missing_phase2_write_mode_isolation_proof_true(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    proof = copy.deepcopy(_load(_fixture(root, "proof")))
    proof["missing_phase2_write_mode_isolation_proof"] = True
    proof_path = _write_json(root / "docs/audit/proof-missing.json", proof)

    with pytest.raises(
        MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError,
        match="missing_phase2_write_mode_isolation_proof",
    ):
        _build(root, proof_path=proof_path)


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        (("grant_decision", "review_by"), "2026-08-28", "review_by"),
        (("grant_decision", "expiry_date"), "2026-08-28", "expiry_date"),
        (("write_mode_policy", "phase_2"), "wrong_policy", "phase_2"),
    ],
)
def test_rejects_grant_review_dates_or_phase2_policy_mismatch(
    tmp_path: Path,
    path: tuple[str, str],
    value: str,
    match: str,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    grant = copy.deepcopy(_load(_fixture(root, "grant")))
    grant[path[0]][path[1]] = value
    grant_path = _write_json(root / "docs/audit/grant-bad.json", grant)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError, match=match):
        _build(root, grant_path=grant_path)


def test_rejects_grant_phase2_writes_authorized_true(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    grant = copy.deepcopy(_load(_fixture(root, "grant")))
    grant["phase2_writes_authorized"] = True
    grant_path = _write_json(root / "docs/audit/grant-phase2-true.json", grant)

    with pytest.raises(
        MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError,
        match="phase2_writes_authorized",
    ):
        _build(root, grant_path=grant_path)


def test_request_flags_and_new_pilot_blocker_remain_closed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = _build(root)

    assert payload["phase2_writes_authorized"] is False
    assert payload["phase2_write_pilot_authorized"] is False
    assert payload["missing_phase2_isolated_audit_write_pilot_authorization"] is True
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_phase2_write_mode_isolation_proof"] is False
    assert blockers["missing_phase2_isolated_audit_write_pilot_authorization"] is True
    assert blockers["phase2_writes_authorized"] is False
    assert blockers["blockers_unchanged_by_request"] is True


def test_proof_summary_includes_file_hashes_from_proof_fixture(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    proof = _load(_fixture(root, "proof"))
    payload = _build(root)

    expected = {
        record["relative_path"]: record["sha256"]
        for record in proof["isolated_file_writes"]["files_written"]
    }
    observed = {
        record["relative_path"]: record["sha256"]
        for record in payload["proof_summary"]["isolated_file_writes"]["files_written"]
    }
    assert observed == expected
    assert payload["proof_summary"]["write_counts_by_isolated_target"]["isolated_audit_shadow_artifacts"] > 0
    assert payload["proof_summary"]["write_counts_by_isolated_target"]["isolated_audit_shadow_tables"] == 0


def test_request_generation_creates_no_shadow_runs_files(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_contents(root)

    _build(root)

    assert _shadow_runs_contents(root) == before


def test_rejects_recorded_input_hash_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    proof = copy.deepcopy(_load(_fixture(root, "proof")))
    proof["metadata"]["inputs"][0]["sha256"] = "0" * 64
    proof_path = _write_json(root / "docs/audit/proof-hash-mismatch.json", proof)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError, match="sha256 mismatch"):
        _build(root, proof_path=proof_path)


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "request.json"
    out_md = tmp_path / "request.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-authorization-request",
        "--phase2-write-mode-proof",
        str(REPO_ROOT / FIXTURE_RELS["proof"]),
        "--phase2-write-mode-plan",
        str(REPO_ROOT / FIXTURE_RELS["plan"]),
        "--authorization-grant",
        str(REPO_ROOT / FIXTURE_RELS["grant"]),
        "--phase1-no-write-pilot-review",
        str(REPO_ROOT / FIXTURE_RELS["review"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["phase2_isolated_audit_write_authorization_requested"] is True
    assert payload["phase2_isolated_audit_write_authorization_granted"] is False
    assert result.stdout.splitlines() == [
        "True",
        "False",
        "record_online_shadow_phase2_isolated_audit_write_authorization_grant_v1",
    ]
    assert "Phase 2 Isolated Audit Write Authorization Request" in out_md.read_text(encoding="utf-8")


def test_no_database_url_no_runtime_call_and_uses_hash_helper() -> None:
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request.py"
    ).read_text(encoding="utf-8")
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
    assert "recorded_sha256_matches_text_artifact" in module_source

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-authorization-request"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]
