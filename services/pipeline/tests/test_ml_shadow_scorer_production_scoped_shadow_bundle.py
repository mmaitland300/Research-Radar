"""Tests for the production-scoped online shadow plan bundle."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_phase_bundle import verify_ml_shadow_scorer_phase_bundle
from pipeline.ml_shadow_scorer_production_readiness_bundle import (
    verify_ml_shadow_scorer_production_readiness_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    MLShadowScorerProductionScopedShadowBundleError,
    PLAN_SUBSECTIONS,
    apply_production_scoped_shadow_plan,
    apply_production_scoped_shadow_pilot_authorization_grant,
    apply_production_scoped_shadow_pilot_authorization_request,
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    prove_ml_shadow_scorer_production_scoped_shadow_bundle,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "production_readiness_bundle": "docs/audit/bundles/production-readiness-v1/bundle.json",
    "production_readiness_bundle_md": "docs/audit/bundles/production-readiness-v1/bundle.md",
    "production_readiness_criteria": "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json",
    "phase2_bundle": "docs/audit/bundles/phase2-v1/bundle.json",
    "phase2_bundle_md": "docs/audit/bundles/phase2-v1/bundle.md",
    "online_shadow_policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
    "execution_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "phase2_write_mode_plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "phase2_write_mode_proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "generalization_audit_gates": "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json",
    "production_readiness_plan": "docs/audit/ml-production-readiness-plan-v1.json",
    "production_readiness_plan_md": "docs/audit/ml-production-readiness-plan-v1.md",
    "phase2_write_authorization_request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "phase2_write_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "phase1_review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        if not src.exists():
            continue
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _shadow_runs_files(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*") if path.is_file()}


def _optional_kwargs(root: Path) -> dict[str, Path]:
    return {
        "execution_authorization_grant_path": _fixture(root, "execution_authorization_grant"),
        "phase2_write_mode_plan_path": _fixture(root, "phase2_write_mode_plan"),
        "phase2_write_mode_proof_path": _fixture(root, "phase2_write_mode_proof"),
        "generalization_audit_gates_path": _fixture(root, "generalization_audit_gates"),
    }


def _write_pre_plan_bundle(root: Path) -> Path:
    out_json = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    write_ml_shadow_scorer_production_scoped_shadow_bundle(
        production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
        **_optional_kwargs(root),
    )
    return out_json


def _write_plan_bundle(root: Path) -> Path:
    bundle_path = _write_pre_plan_bundle(root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    return bundle_path


def _write_proof_bundle(root: Path, *, pilot_run_id: str = "proof-pilot") -> Path:
    bundle_path = _write_plan_bundle(root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id=pilot_run_id,
        repo_root=root,
    )
    return bundle_path


def _write_pilot_request_bundle(root: Path, *, pilot_run_id: str = "proof-pilot") -> Path:
    bundle_path = _write_proof_bundle(root, pilot_run_id=pilot_run_id)
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    return bundle_path


def _write_pilot_grant_bundle(root: Path, *, pilot_run_id: str = "proof-pilot") -> Path:
    bundle_path = _write_pilot_request_bundle(root, pilot_run_id=pilot_run_id)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review=(
            "Owner reviewed the production-scoped proof, pilot request, and bounded pilot contract as equivalent authorization review."
        ),
        repo_root=root,
    )
    return bundle_path


def test_assemble_pre_plan_revision_zero_and_verify_not_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)
    payload = _load(bundle_path)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_plan_filed=False,
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_production_scoped_shadow_bundle"
    assert payload["metadata"]["bundle_revision"] == 0
    assert payload["plan"]["prod_scoped_shadow_plan_defined"] is False
    assert payload["recommended_next_stage"] == "begin_production_scoped_online_shadow_plan_v1"
    assert result["verification_mode"] == "pre_plan"


def test_apply_plan_revision_one_and_verify_plan_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)

    planned = plan_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        planner="Planner Name",
        plan_notes="plan notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_plan_filed=True,
    )

    assert planned["metadata"]["bundle_revision"] == 1
    assert planned["plan"]["prod_scoped_shadow_plan_defined"] is True
    assert planned["plan"]["plan_decision"]["planner"] == "Planner Name"
    assert planned["plan"]["plan_decision"]["plan_notes"] == "plan notes"
    assert planned["posture"]["missing_prod_scoped_shadow_proof"] is True
    assert planned["posture"]["prod_scoped_shadow_proof_authorized"] is False
    assert planned["authorization"]["prod_scoped_shadow_proof_authorized"] is False
    assert planned["recommended_next_stage"] == "implement_production_scoped_online_shadow_proof_v1"
    assert result["verification_mode"] == "post_plan"


def test_rejects_plan_apply_on_already_plan_filed_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)


def test_prove_from_revision_one_plan_and_verify_proof_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)

    proven = prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id="proof-pilot-001",
        prover="Proof Runner",
        proof_notes="proof notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_proof_filed=True,
    )

    assert proven["metadata"]["bundle_revision"] == 2
    assert proven["proof"]["prod_scoped_shadow_proof_filed"] is True
    assert proven["proof"]["proof_decision"]["decision"] == "proven"
    assert proven["proof"]["proof_decision"]["prover"] == "Proof Runner"
    assert proven["proof"]["proof_decision"]["proof_notes"] == "proof notes"
    assert proven["proof"]["proof_pass_fail"]["overall_passed"] is True
    assert proven["authorization"]["prod_scoped_shadow_proof_allowed_by_plan"] is True
    assert proven["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert proven["authorization"]["prod_scoped_shadow_proof_authorized"] is False
    assert proven["authorization"]["prod_scoped_shadow_pilot_authorized"] is False
    assert proven["posture"]["prod_scoped_shadow_proof_passed"] is True
    assert proven["posture"]["missing_prod_scoped_shadow_proof"] is False
    assert proven["posture"]["prod_scoped_shadow_pilot_authorized"] is False
    assert proven["recommended_next_stage"] == "request_production_scoped_online_shadow_pilot_authorization_v1"
    assert result["verification_mode"] == "post_proof"


def test_request_pilot_from_revision_two_proof_and_verify_request_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_proof_bundle(root, pilot_run_id="proof-before-request")
    before = _load(bundle_path)
    plan_before = before["plan"]
    proof_before = before["proof"]
    shadow_before = _shadow_runs_files(root)

    requested = request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Request Reviewer",
        request_notes="pilot request notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_pilot_request_filed=True,
    )

    assert requested["metadata"]["bundle_revision"] == 3
    assert requested["plan"] == plan_before
    assert requested["proof"] == proof_before
    assert requested["authorization"]["prod_scoped_shadow_pilot_authorization_requested"] is True
    assert requested["authorization"]["prod_scoped_shadow_pilot_authorized"] is False
    assert requested["authorization"]["request_decision"]["decision"] == "requested"
    assert requested["authorization"]["request_decision"]["requester"] == "Request Reviewer"
    assert requested["authorization"]["request_decision"]["request_notes"] == "pilot request notes"
    assert requested["authorization"]["requested_scope"]["authorization_scope"] == (
        "production_scoped_shadow_pilot_paperwork_only"
    )
    assert requested["posture"]["missing_prod_scoped_shadow_pilot_authorization"] is True
    assert requested["posture"]["prod_scoped_shadow_pilot_authorized"] is False
    assert requested["execution"]["prod_scoped_shadow_pilot_executed"] is False
    assert requested["shadow_and_production_blockers"]["missing_prod_scoped_shadow_pilot_authorization"] is True
    assert "missing_prod_scoped_shadow_pilot_authorization" in requested["shadow_and_production_blockers"][
        "blockers_introduced_by_pilot_request"
    ]
    assert requested["shadow_and_production_blockers"]["blockers_cleared_by_pilot_request"] == []
    assert requested["shadow_and_production_blockers"]["blockers_unchanged_by_pilot_request"] is True
    assert requested["recommended_next_stage"] == (
        "record_production_scoped_online_shadow_pilot_authorization_grant_v1"
    )
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_pilot_request"


def test_rejects_pilot_request_on_wrong_or_already_requested_revisions(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    pre_plan = _write_pre_plan_bundle(root)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=pre_plan, repo_root=root)

    plan = _write_plan_bundle(root)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=plan, repo_root=root)

    requested = _write_pilot_request_bundle(root, pilot_run_id="proof-already-requested")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=requested, repo_root=root)


def test_rejects_pilot_request_if_proof_not_passed_or_proof_missing(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_proof_bundle(root)
    payload = _load(bundle_path)
    payload["posture"]["prod_scoped_shadow_proof_passed"] = False
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="prod_scoped_shadow_proof_passed"):
        request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)

    payload = _load(_write_proof_bundle(root, pilot_run_id="proof-missing-blocker"))
    payload["posture"]["missing_prod_scoped_shadow_proof"] = True
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="missing_prod_scoped_shadow_proof"):
        request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)


def test_pilot_request_rejects_sha_tamper_and_preserves_proof_hashes(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_proof_bundle(root)
    payload = _load(bundle_path)
    original_proof = payload["proof"]
    payload["metadata"]["legacy_artifacts_index"][0]["sha256"] = "0" * 64
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="sha256 mismatch"):
        request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)

    bundle_path = _write_proof_bundle(root, pilot_run_id="proof-preserved")
    payload = _load(bundle_path)
    original_proof = payload["proof"]
    requested = request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    assert requested["proof"] == original_proof
    assert [row["sha256"] for row in requested["proof"]["write_evidence"]["files_written"]] == [
        row["sha256"] for row in original_proof["write_evidence"]["files_written"]
    ]


def test_post_pilot_request_verifier_rejects_pilot_authorized_or_bad_blockers(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root)
    payload = _load(bundle_path)
    payload["authorization"]["prod_scoped_shadow_pilot_authorized"] = True
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="prod_scoped_shadow_pilot_authorized"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_pilot_request_filed=True,
        )

    payload = _load(_write_pilot_request_bundle(root, pilot_run_id="proof-bad-blockers"))
    payload["shadow_and_production_blockers"]["blockers_cleared_by_pilot_request"] = [
        "missing_prod_scoped_shadow_pilot_authorization"
    ]
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="blockers_cleared_by_pilot_request"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_pilot_request_filed=True,
        )


def test_post_pilot_request_caveats_exclude_plan_caveats(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root)
    payload = _load(bundle_path)
    caveats = payload["caveats"]

    assert "Bundle pilot-request milestone only; grants no pilot authorization." in caveats
    assert "Accepted proof evidence is necessary but not sufficient for pilot execution." in caveats
    assert all("Future proof must clear missing_prod_scoped_shadow_proof" not in caveat for caveat in caveats)
    assert all("does not authorize production-scoped proof execution" not in caveat for caveat in caveats)


def test_grant_pilot_from_revision_three_request_and_verify_grant_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root, pilot_run_id="proof-before-grant")
    before = _load(bundle_path)
    plan_before = before["plan"]
    proof_before = before["proof"]
    shadow_before = _shadow_runs_files(root)

    granted = grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner="Grant Owner",
        second_reviewer="Second Reviewer",
        grant_notes="grant notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_pilot_grant_filed=True,
    )

    assert granted["metadata"]["bundle_revision"] == 4
    assert granted["plan"] == plan_before
    assert granted["proof"] == proof_before
    assert granted["authorization"]["prod_scoped_shadow_pilot_authorization_requested"] is True
    assert granted["authorization"]["prod_scoped_shadow_pilot_authorization_granted"] is True
    assert granted["authorization"]["prod_scoped_shadow_pilot_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert granted["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert granted["authorization"]["grant_decision"]["decision"] == "granted"
    assert granted["authorization"]["grant_decision"]["owner"] == "Grant Owner"
    assert granted["authorization"]["grant_decision"]["second_reviewer"] == "Second Reviewer"
    assert granted["authorization"]["grant_decision"]["grant_notes"] == "grant notes"
    assert granted["authorization"]["granted_scope"]["authorization_scope"] == (
        "production_scoped_shadow_pilot_authorization_only"
    )
    assert granted["posture"]["missing_prod_scoped_shadow_pilot_authorization"] is False
    assert granted["posture"]["prod_scoped_shadow_pilot_authorized"] is True
    assert granted["execution"]["prod_scoped_shadow_pilot_executed"] is False
    assert granted["shadow_and_production_blockers"]["missing_prod_scoped_shadow_pilot_authorization"] is False
    assert "missing_prod_scoped_shadow_pilot_authorization" in granted["shadow_and_production_blockers"][
        "blockers_cleared_by_pilot_grant"
    ]
    assert granted["shadow_and_production_blockers"]["blockers_introduced_by_pilot_grant"] == []
    assert granted["shadow_and_production_blockers"]["blockers_unchanged_by_pilot_grant"] is True
    assert granted["recommended_next_stage"] == "run_production_scoped_online_shadow_pilot_v1"
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_pilot_grant"


def test_rejects_pilot_grant_on_wrong_or_already_granted_revisions(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    for bundle_path in (
        _write_pre_plan_bundle(root),
        _write_plan_bundle(root),
        _write_proof_bundle(root, pilot_run_id="proof-grant-wrong-rev"),
    ):
        with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
            grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
                bundle_path=bundle_path,
                owner_documents_equivalent_review="owner equivalent review",
                repo_root=root,
            )

    granted = _write_pilot_grant_bundle(root, pilot_run_id="proof-already-granted")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=granted,
            owner_documents_equivalent_review="owner equivalent review",
            repo_root=root,
        )


def test_rejects_pilot_grant_when_missing_blocker_already_false(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root)
    payload = _load(bundle_path)
    payload["posture"]["missing_prod_scoped_shadow_pilot_authorization"] = False
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="missing_prod_scoped_shadow_pilot_authorization",
    ):
        grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent review",
            repo_root=root,
        )


def test_rejects_pilot_grant_without_independent_or_equivalent_review(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="requires second_reviewer"):
        grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="second_reviewer"):
        grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner="Same Reviewer",
            second_reviewer="Same Reviewer",
            repo_root=root,
        )


def test_accepts_pilot_grant_with_owner_equivalent_review_rationale(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root)

    granted = grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent review rationale",
        repo_root=root,
    )

    assert granted["authorization"]["grant_decision"]["owner_documents_equivalent_review"] == (
        "owner equivalent review rationale"
    )
    assert granted["authorization"]["prod_scoped_shadow_pilot_authorized"] is True


def test_pilot_grant_rejects_sha_tamper_and_preserves_proof_hashes(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_request_bundle(root)
    payload = _load(bundle_path)
    payload["metadata"]["legacy_artifacts_index"][0]["sha256"] = "0" * 64
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="sha256 mismatch"):
        grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent review",
            repo_root=root,
        )

    bundle_path = _write_pilot_request_bundle(root, pilot_run_id="proof-grant-preserved")
    payload = _load(bundle_path)
    original_proof = payload["proof"]
    granted = grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent review",
        repo_root=root,
    )
    assert granted["proof"] == original_proof
    assert [row["sha256"] for row in granted["proof"]["write_evidence"]["files_written"]] == [
        row["sha256"] for row in original_proof["write_evidence"]["files_written"]
    ]


def test_post_pilot_grant_verifier_rejects_bad_blockers_or_caveats(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root)
    payload = _load(bundle_path)
    payload["shadow_and_production_blockers"]["blockers_introduced_by_pilot_grant"] = [
        "missing_prod_scoped_shadow_pilot_authorization"
    ]
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="blockers_introduced_by_pilot_grant"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_pilot_grant_filed=True,
        )

    payload = _load(_write_pilot_grant_bundle(root, pilot_run_id="proof-grant-bad-caveat"))
    payload["caveats"].append("Bundle pilot-request milestone only; grants no pilot authorization.")
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="request-only"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_pilot_grant_filed=True,
        )


def test_post_pilot_grant_non_authorizations_hold(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pilot_grant_bundle(root)
    payload = _load(bundle_path)

    assert payload["posture"]["online_shadow_execution_enabled"] is False
    assert payload["posture"]["production_default_allowed"] is False
    assert payload["posture"]["api_web_changes_allowed"] is False
    assert payload["posture"]["user_visible_ranking_changed"] is False
    assert payload["execution"]["prod_scoped_shadow_pilot_executed"] is False
    assert payload["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert payload["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert payload["writes_performed"] is False
    assert payload["runtime_writes_performed"] is False


def test_rejects_proof_on_pre_plan_and_already_proven_bundles(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    pre_plan = _write_pre_plan_bundle(root)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        prove_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=pre_plan,
            pilot_run_id="proof-pre-plan",
            repo_root=root,
        )

    proven = _write_proof_bundle(root, pilot_run_id="proof-already")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        prove_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=proven,
            pilot_run_id="proof-again",
            repo_root=root,
        )


def test_rejects_missing_empty_or_nonzero_forbidden_write_counts_post_proof(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_proof_bundle(root)
    payload = _load(bundle_path)
    del payload["proof"]["write_evidence"]["forbidden_write_target_counts"]["ranking_runs"]
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="missing keys"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_proof_filed=True,
        )

    payload = _load(_write_proof_bundle(root, pilot_run_id="proof-empty-counts"))
    payload["proof"]["write_evidence"]["forbidden_write_target_counts"] = {}
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="non-empty object"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_proof_filed=True,
        )

    payload = _load(_write_proof_bundle(root, pilot_run_id="proof-nonzero-counts"))
    payload["proof"]["write_evidence"]["forbidden_write_target_counts"]["paper_scores"] = 1
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="paper_scores"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_proof_filed=True,
        )


def test_rejects_observability_and_rollback_failures_post_proof(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_proof_bundle(root)
    payload = _load(bundle_path)
    payload["proof"]["observability_evidence"]["observability_complete"] = False
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="observability_complete"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_proof_filed=True,
        )

    payload = _load(_write_proof_bundle(root, pilot_run_id="proof-rollback"))
    payload["proof"]["rollback_drill_evidence"]["flag_enablement_attempted"] = True
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="flag_enablement_attempted"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_proof_filed=True,
        )


def test_proof_write_evidence_and_posture_clear_only_proof_blocker(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_proof_bundle(root)
    payload = _load(bundle_path)
    write_evidence = payload["proof"]["write_evidence"]

    assert write_evidence["local_artifact_tree_writes_performed"] is True
    assert write_evidence["production_writes_performed"] is False
    assert write_evidence["committed_artifact_writes_performed"] is False
    assert write_evidence["runtime_writes_performed"] is False
    assert set(write_evidence["forbidden_write_target_counts"]) == set(FORBIDDEN_PROD_SCOPED_WRITE_TARGETS)
    assert all(count == 0 for count in write_evidence["forbidden_write_target_counts"].values())
    assert len(write_evidence["files_written"]) == 4
    assert payload["shadow_and_production_blockers"]["missing_prod_scoped_shadow_proof"] is False
    assert "missing_prod_scoped_shadow_proof" in payload["shadow_and_production_blockers"]["blockers_changed_by_proof"]
    for field in (
        "online_shadow_execution_enabled",
        "production_default_allowed",
        "api_web_changes_allowed",
        "user_visible_ranking_changed",
        "writes_performed",
        "runtime_writes_performed",
    ):
        assert payload["posture"][field] is False


def test_proof_rejects_fixture_labels_and_invalid_pilot_run_id(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)
    fixture = root / "fixture.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "ranking_run_id": "rank-83787b91ef",
                    "family": "emerging",
                    "candidate_pool_work_set_sha256": "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc",
                    "final_score_rank_pct": 0.5,
                    "audit_embedding_probability_rank_pct": 0.4,
                    "component_coverage": {},
                    "generated_at": "2026-05-29T00:00:00Z",
                    "input_hashes": {},
                    "label": "forbidden",
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="label fields"):
        prove_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            pilot_run_id="proof-label",
            fixture_input_path=fixture,
            repo_root=root,
        )
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        prove_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            pilot_run_id="../proof",
            repo_root=root,
        )


def test_rejects_if_production_readiness_bundle_not_grant_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    production_readiness = _load(_fixture(root, "production_readiness_bundle"))
    production_readiness["metadata"]["bundle_revision"] = 1
    production_readiness["authorization"]["production_readiness_authorization_granted"] = False
    production_readiness["posture"]["missing_production_readiness_authorization"] = True
    production_readiness["recommended_next_stage"] = "record_production_readiness_authorization_grant_v1"
    _write_json(_fixture(root, "production_readiness_bundle"), production_readiness)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _write_pre_plan_bundle(root)


def test_rejects_if_phase2_bundle_not_accepted(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    phase2_copy = root / "docs/audit/bundles/phase2-v1/not-accepted.json"
    phase2 = _load(_fixture(root, "phase2_bundle"))
    phase2["review"]["phase2_write_pilot_accepted"] = False
    phase2["review"]["review_decision"]["decision"] = "not_accepted"
    _write_json(phase2_copy, phase2)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        write_ml_shadow_scorer_production_scoped_shadow_bundle(
            production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
            phase_bundle_path=phase2_copy,
            online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
            output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json",
            markdown_output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md",
            repo_root=root,
            **_optional_kwargs(root),
        )


def test_rejects_sha_tamper_and_identity_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)
    payload = _load(bundle_path)
    payload["metadata"]["legacy_artifacts_index"][0]["sha256"] = "0" * 64
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="sha256 mismatch"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_plan_filed=False,
        )

    phase2_copy = root / "docs/audit/bundles/phase2-v1/identity-mismatch.json"
    phase2 = _load(_fixture(root, "phase2_bundle"))
    phase2["posture"]["pinned_identity"]["family"] = "mismatch"
    _write_json(phase2_copy, phase2)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        write_ml_shadow_scorer_production_scoped_shadow_bundle(
            production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
            phase_bundle_path=phase2_copy,
            online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
            output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/identity.json",
            markdown_output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/identity.md",
            repo_root=root,
            **_optional_kwargs(root),
        )


def test_plan_subsections_populated_and_caveats_do_not_imply_enablement(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)
    payload = _load(bundle_path)

    for subsection in PLAN_SUBSECTIONS:
        assert payload["plan"][subsection]
    assert payload["posture"]["online_shadow_execution_enabled"] is False
    assert payload["posture"]["production_default_allowed"] is False
    assert payload["posture"]["api_web_changes_allowed"] is False
    assert payload["posture"]["user_visible_ranking_changed"] is False
    caveat_text = " ".join(payload["caveats"]).lower()
    assert "authorizes proof execution" not in caveat_text
    assert "authorizes pilot" not in caveat_text
    assert "enables online shadow" not in caveat_text


def test_verify_rejects_missing_plan_subsection_post_plan(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)
    payload = _load(bundle_path)
    del payload["plan"]["observability_and_slo_plan"]
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="observability_and_slo_plan"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_plan_filed=True,
        )


def test_no_shadow_runs_writes_or_upstream_mutation(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_files(root)
    production_readiness_before = _fixture(root, "production_readiness_bundle").read_text(encoding="utf-8")
    phase2_before = _fixture(root, "phase2_bundle").read_text(encoding="utf-8")

    _write_plan_bundle(root)

    assert _shadow_runs_files(root) == before
    assert _fixture(root, "production_readiness_bundle").read_text(encoding="utf-8") == production_readiness_before
    assert _fixture(root, "phase2_bundle").read_text(encoding="utf-8") == phase2_before


def test_apply_plan_in_memory_requires_revision_zero() -> None:
    payload = {
        "metadata": {"bundle_revision": 1},
        "plan": {"prod_scoped_shadow_plan_defined": True},
    }
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        apply_production_scoped_shadow_plan(payload)


def test_apply_pilot_request_in_memory_requires_revision_two() -> None:
    payload = {
        "metadata": {"bundle_revision": 1},
        "proof": {"prod_scoped_shadow_proof_filed": False},
        "posture": {"prod_scoped_shadow_proof_passed": False},
    }
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        apply_production_scoped_shadow_pilot_authorization_request(payload)


def test_apply_pilot_grant_in_memory_requires_revision_three() -> None:
    payload = {
        "metadata": {"bundle_revision": 2},
        "authorization": {"prod_scoped_shadow_pilot_authorization_requested": False},
        "posture": {"missing_prod_scoped_shadow_pilot_authorization": False},
    }
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        apply_production_scoped_shadow_pilot_authorization_grant(
            payload,
            owner_documents_equivalent_review="owner equivalent review",
        )


def test_cli_smoke_assemble_plan_verify(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    assemble_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-assemble",
        "--production-readiness-bundle",
        str(_fixture(root, "production_readiness_bundle")),
        "--phase-bundle",
        str(_fixture(root, "phase2_bundle")),
        "--online-shadow-policy",
        str(_fixture(root, "online_shadow_policy")),
        "--execution-authorization-grant",
        str(_fixture(root, "execution_authorization_grant")),
        "--phase2-write-mode-plan",
        str(_fixture(root, "phase2_write_mode_plan")),
        "--phase2-write-mode-proof",
        str(_fixture(root, "phase2_write_mode_proof")),
        "--generalization-audit-gates",
        str(_fixture(root, "generalization_audit_gates")),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(root),
    ]
    assemble = subprocess.run(assemble_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert assemble.stdout.splitlines() == [
        "online-shadow-production-scoped-v1",
        "False",
        "begin_production_scoped_online_shadow_plan_v1",
    ]

    plan_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-plan",
        "--bundle",
        str(out_json),
        "--planner",
        "CLI Planner",
        "--plan-notes",
        "cli plan notes",
        "--repo-root",
        str(root),
    ]
    planned = subprocess.run(plan_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert planned.stdout.splitlines() == [
        "planned",
        "True",
        "implement_production_scoped_online_shadow_proof_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-plan-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_plan",
        "online-shadow-production-scoped-v1",
        "implement_production_scoped_online_shadow_proof_v1",
    ]

    prove_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-prove",
        "--bundle",
        str(out_json),
        "--pilot-run-id",
        "cli-proof-pilot",
        "--prover",
        "CLI Prover",
        "--proof-notes",
        "cli proof notes",
        "--repo-root",
        str(root),
    ]
    proven = subprocess.run(prove_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert proven.stdout.splitlines() == [
        "proven",
        "True",
        "request_production_scoped_online_shadow_pilot_authorization_v1",
    ]

    verify_proof_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-proof-filed",
        "--repo-root",
        str(root),
    ]
    verified_proof = subprocess.run(
        verify_proof_cmd,
        cwd=PACKAGE_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert verified_proof.stdout.splitlines() == [
        "passed",
        "post_proof",
        "online-shadow-production-scoped-v1",
        "request_production_scoped_online_shadow_pilot_authorization_v1",
    ]

    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-pilot",
        "--bundle",
        str(out_json),
        "--requester",
        "CLI Requester",
        "--request-notes",
        "cli request notes",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        "record_production_scoped_online_shadow_pilot_authorization_grant_v1",
    ]

    verify_request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-pilot-request-filed",
        "--repo-root",
        str(root),
    ]
    verified_request = subprocess.run(
        verify_request_cmd,
        cwd=PACKAGE_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert verified_request.stdout.splitlines() == [
        "passed",
        "post_pilot_request",
        "online-shadow-production-scoped-v1",
        "record_production_scoped_online_shadow_pilot_authorization_grant_v1",
    ]

    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-pilot",
        "--bundle",
        str(out_json),
        "--owner-documents-equivalent-review",
        "Owner reviewed the production-scoped proof, pilot request, and bounded pilot contract.",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        "run_production_scoped_online_shadow_pilot_v1",
    ]

    verify_grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-pilot-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified_grant = subprocess.run(
        verify_grant_cmd,
        cwd=PACKAGE_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert verified_grant.stdout.splitlines() == [
        "passed",
        "post_pilot_grant",
        "online-shadow-production-scoped-v1",
        "run_production_scoped_online_shadow_pilot_v1",
    ]


def test_cli_verify_rejects_conflicting_pilot_request_flags(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = _write_pilot_request_bundle(root, pilot_run_id="proof-cli-conflict")
    command = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-proof-filed",
        "--expect-pilot-request-filed",
        "--repo-root",
        str(root),
    ]

    result = subprocess.run(command, cwd=PACKAGE_ROOT, text=True, capture_output=True)

    assert result.returncode != 0
    assert "not allowed with argument" in result.stderr


def test_upstream_verifiers_still_pass(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    phase2_result = verify_ml_shadow_scorer_phase_bundle(
        bundle_path=_fixture(root, "phase2_bundle"),
        repo_root=root,
        expect_pilot_reviewed=True,
    )
    production_readiness_result = verify_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=_fixture(root, "production_readiness_bundle"),
        repo_root=root,
        expect_grant_filed=True,
    )

    assert phase2_result["verification_mode"] == "post_review"
    assert production_readiness_result["verification_mode"] == "post_grant"


def test_committed_bundle_fixture_matches_post_pilot_harness_review_if_present() -> None:
    committed = REPO_ROOT / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_pilot_harness_review_filed=True,
    )
    assert result["bundle_revision"] == 6
    assert result["recommended_next_stage"] == "run_production_scoped_online_shadow_pilot_v1"


def test_payload_verifier_infers_plan_mode(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)
    pre = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    post = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id="infer-proof",
        repo_root=root,
    )
    proof = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    request = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent review",
        repo_root=root,
    )
    grant = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)

    assert pre["verification_mode"] == "pre_plan"
    assert post["verification_mode"] == "post_plan"
    assert proof["verification_mode"] == "post_proof"
    assert request["verification_mode"] == "post_pilot_request"
    assert grant["verification_mode"] == "post_pilot_grant"


def test_no_forbidden_imports_or_database_url_on_bundle_cli() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_scoped_shadow_bundle.py").read_text(
        encoding="utf-8"
    )
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    assemble_start = cli_source.index('"ml-shadow-scorer-production-scoped-shadow-bundle-assemble"')
    next_command = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', assemble_start)
    assert "--database-url" not in cli_source[assemble_start:next_command]
    assert '"ml-shadow-scorer-production-scoped-shadow-bundle-request-pilot"' in cli_source[assemble_start:next_command]
    assert '"ml-shadow-scorer-production-scoped-shadow-bundle-grant-pilot"' in cli_source[assemble_start:next_command]
    assert '"ml-shadow-scorer-production-scoped-shadow-pilot-harness-run"' in cli_source[assemble_start:next_command]
    assert '"ml-shadow-scorer-production-scoped-shadow-pilot-harness-review"' in cli_source[assemble_start:next_command]
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
