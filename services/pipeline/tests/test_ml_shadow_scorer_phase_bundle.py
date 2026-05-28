"""Tests for canonical online shadow phase bundles."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_phase_bundle import (
    MLShadowScorerPhaseBundleError,
    assemble_ml_shadow_scorer_phase_bundle_payload,
    verify_ml_shadow_scorer_phase_bundle,
    verify_ml_shadow_scorer_phase_bundle_payload,
    write_ml_shadow_scorer_phase_bundle,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "prior_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
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


def _build(root: Path, **overrides: Path) -> dict[str, Any]:
    return assemble_ml_shadow_scorer_phase_bundle_payload(
        phase2_write_mode_plan_path=overrides.get("plan_path", _fixture(root, "plan")),
        phase2_write_mode_proof_path=overrides.get("proof_path", _fixture(root, "proof")),
        phase2_write_authorization_request_path=overrides.get("request_path", _fixture(root, "request")),
        phase2_write_authorization_grant_path=overrides.get("grant_path", _fixture(root, "grant")),
        phase1_no_write_pilot_review_path=overrides.get("review_path", _fixture(root, "review")),
        prior_execution_authorization_grant_path=overrides.get("prior_grant_path", _fixture(root, "prior_grant")),
        online_shadow_policy_path=overrides.get("policy_path", _fixture(root, "policy")),
        repo_root=root,
        generated_at="2026-05-28T22:00:00Z",
    )


def _write_bundle(root: Path) -> Path:
    out_json = root / "docs/audit/bundles/phase2-v1/bundle.json"
    out_md = root / "docs/audit/bundles/phase2-v1/bundle.md"
    write_ml_shadow_scorer_phase_bundle(
        phase2_write_mode_plan_path=_fixture(root, "plan"),
        phase2_write_mode_proof_path=_fixture(root, "proof"),
        phase2_write_authorization_request_path=_fixture(root, "request"),
        phase2_write_authorization_grant_path=_fixture(root, "grant"),
        phase1_no_write_pilot_review_path=_fixture(root, "review"),
        prior_execution_authorization_grant_path=_fixture(root, "prior_grant"),
        online_shadow_policy_path=_fixture(root, "policy"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
    )
    return out_json


def _shadow_runs_files(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*") if path.is_file()}


def test_happy_path_assembles_bundle_json_markdown_from_committed_fixtures(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/bundles/phase2-v1/bundle.json"
    out_md = root / "docs/audit/bundles/phase2-v1/bundle.md"

    payload = write_ml_shadow_scorer_phase_bundle(
        phase2_write_mode_plan_path=_fixture(root, "plan"),
        phase2_write_mode_proof_path=_fixture(root, "proof"),
        phase2_write_authorization_request_path=_fixture(root, "request"),
        phase2_write_authorization_grant_path=_fixture(root, "grant"),
        phase1_no_write_pilot_review_path=_fixture(root, "review"),
        prior_execution_authorization_grant_path=_fixture(root, "prior_grant"),
        online_shadow_policy_path=_fixture(root, "policy"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
    )
    persisted = _load(out_json)

    assert persisted["metadata"]["artifact_type"] == "ml_shadow_scorer_phase_bundle"
    assert persisted["metadata"]["bundle_version"] == "online-shadow-phase2-v1"
    assert persisted["metadata"]["bundle_revision"] == 1
    assert len(persisted["metadata"]["legacy_artifacts_index"]) == 7
    assert persisted["authorization"]["phase2_write_pilot_authorized"] is True
    assert persisted["authorization"]["phase2_writes_authorized"] is True
    assert persisted["execution"]["phase2_write_pilot_executed"] is False
    assert persisted["review"]["phase2_write_pilot_reviewed"] is False
    assert payload["recommended_next_stage"] == "run_online_shadow_phase2_isolated_audit_write_pilot_v1"
    assert "Online Shadow Phase Bundle" in out_md.read_text(encoding="utf-8")


def test_verify_command_passes_on_assembled_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_bundle(root)

    result = verify_ml_shadow_scorer_phase_bundle(bundle_path=bundle_path, repo_root=root)

    assert result["verification_status"] == "passed"
    assert result["bundle_version"] == "online-shadow-phase2-v1"
    assert result["recommended_next_stage"] == "run_online_shadow_phase2_isolated_audit_write_pilot_v1"


def test_rejects_missing_referenced_artifact(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_bundle(root)
    _fixture(root, "plan").unlink()

    with pytest.raises(MLShadowScorerPhaseBundleError, match="missing"):
        verify_ml_shadow_scorer_phase_bundle(bundle_path=bundle_path, repo_root=root)


def test_rejects_tampered_referenced_sha(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _build(root)
    bundle["policy_ref"]["online_shadow_policy"]["sha256"] = "0" * 64

    with pytest.raises(MLShadowScorerPhaseBundleError, match="sha256 mismatch"):
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root)


def test_rejects_pinned_identity_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _build(root)
    bundle["metadata"]["pinned_identity"]["scorer_id"] = "wrong-scorer"

    with pytest.raises(MLShadowScorerPhaseBundleError, match="pinned_identity.scorer_id"):
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root)


def test_rejects_bundle_authorization_booleans_that_drift_from_grant_json(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _build(root)
    bundle["authorization"]["phase2_writes_authorized"] = False

    with pytest.raises(MLShadowScorerPhaseBundleError, match="authorization.phase2_writes_authorized"):
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root)


def test_rejects_missing_or_unresolvable_proof_summary_ref(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _build(root)
    bundle["evidence"]["proof_summary_ref"] = "phase2_write_authorization_grant.missing"

    with pytest.raises(MLShadowScorerPhaseBundleError, match="proof_summary_ref"):
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root)


def test_rejects_grant_not_authorized(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    grant = copy.deepcopy(_load(_fixture(root, "grant")))
    grant["phase2_write_pilot_authorized"] = False
    grant_path = _write_json(root / "docs/audit/grant-not-authorized.json", grant)

    with pytest.raises(MLShadowScorerPhaseBundleError, match="phase2_write_pilot_authorized"):
        _build(root, grant_path=grant_path)


def test_rejects_proof_not_passed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    proof = copy.deepcopy(_load(_fixture(root, "proof")))
    proof["phase2_write_mode_proof_passed"] = False
    proof_path = _write_json(root / "docs/audit/proof-not-passed.json", proof)

    with pytest.raises(MLShadowScorerPhaseBundleError, match="phase2_write_mode_proof_passed"):
        _build(root, proof_path=proof_path)


def test_rejects_phase1_review_not_accepted(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    review = copy.deepcopy(_load(_fixture(root, "review")))
    review["phase1_no_write_pilot_result_accepted"] = False
    review["review_decision"]["decision"] = "denied"
    review_path = _write_json(root / "docs/audit/review-not-accepted.json", review)

    with pytest.raises(MLShadowScorerPhaseBundleError, match="phase1_no_write_pilot_result_accepted"):
        _build(root, review_path=review_path)


def test_rejects_bundle_with_phase2_write_pilot_executed_true_before_pilot_pr(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _build(root)
    bundle["execution"]["phase2_write_pilot_executed"] = True

    with pytest.raises(MLShadowScorerPhaseBundleError, match="phase2_write_pilot_executed"):
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root)


def test_posture_recommended_next_stage_and_caveats_are_stable(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _build(root)
    posture = bundle["posture"]

    assert posture["online_shadow_execution_enabled"] is False
    assert posture["production_default_allowed"] is False
    assert posture["api_web_changes_allowed"] is False
    assert posture["user_visible_ranking_changed"] is False
    assert posture["missing_production_readiness_authorization"] is True
    assert bundle["recommended_next_stage"] == "run_online_shadow_phase2_isolated_audit_write_pilot_v1"
    assert len(bundle["caveats"]) >= 6
    assert "blockers_changed_by_request" not in bundle["shadow_and_production_blockers"]


def test_no_shadow_runs_files_are_created(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_files(root)

    _write_bundle(root)

    assert _shadow_runs_files(root) == before


def test_cli_smoke_for_assemble_and_verify(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/bundles/phase2-v1/bundle.json"
    out_md = root / "docs/audit/bundles/phase2-v1/bundle.md"
    assemble_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-phase-bundle-assemble",
        "--phase2-write-mode-plan",
        str(_fixture(root, "plan")),
        "--phase2-write-mode-proof",
        str(_fixture(root, "proof")),
        "--phase2-write-authorization-request",
        str(_fixture(root, "request")),
        "--phase2-write-authorization-grant",
        str(_fixture(root, "grant")),
        "--phase1-no-write-pilot-review",
        str(_fixture(root, "review")),
        "--prior-execution-authorization-grant",
        str(_fixture(root, "prior_grant")),
        "--online-shadow-policy",
        str(_fixture(root, "policy")),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(root),
    ]
    assemble = subprocess.run(assemble_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert assemble.stdout.splitlines() == [
        "online-shadow-phase2-v1",
        "1",
        "run_online_shadow_phase2_isolated_audit_write_pilot_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-phase-bundle-verify",
        "--bundle",
        str(out_json),
        "--repo-root",
        str(root),
    ]
    verify = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verify.stdout.splitlines() == [
        "passed",
        "online-shadow-phase2-v1",
        "run_online_shadow_phase2_isolated_audit_write_pilot_v1",
    ]


def test_no_forbidden_imports_and_no_database_url_on_bundle_cli() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_phase_bundle.py").read_text(encoding="utf-8")
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    assemble_start = cli_source.index('"ml-shadow-scorer-phase-bundle-assemble"')
    verify_start = cli_source.index('"ml-shadow-scorer-phase-bundle-verify"', assemble_start)
    next_command = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', verify_start)
    assert "--database-url" not in cli_source[assemble_start:verify_start]
    assert "--database-url" not in cli_source[verify_start:next_command]
