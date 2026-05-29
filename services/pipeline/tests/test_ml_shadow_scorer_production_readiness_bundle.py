"""Tests for the production-readiness authorization request bundle."""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_phase_bundle import verify_ml_shadow_scorer_phase_bundle
from pipeline.ml_shadow_scorer_production_readiness_authorization_criteria import (
    verify_ml_shadow_scorer_production_readiness_authorization_criteria,
)
from pipeline.ml_shadow_scorer_production_readiness_bundle import (
    MLShadowScorerProductionReadinessBundleError,
    apply_production_readiness_authorization_grant,
    apply_production_readiness_authorization_request,
    assemble_ml_shadow_scorer_production_readiness_bundle_payload,
    grant_ml_shadow_scorer_production_readiness_bundle,
    request_ml_shadow_scorer_production_readiness_bundle,
    verify_ml_shadow_scorer_production_readiness_bundle,
    verify_ml_shadow_scorer_production_readiness_bundle_payload,
    write_ml_shadow_scorer_production_readiness_bundle,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "criteria": "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json",
    "phase2_bundle": "docs/audit/bundles/phase2-v1/bundle.json",
    "phase2_bundle_md": "docs/audit/bundles/phase2-v1/bundle.md",
    "production_bundle": "docs/audit/bundles/production-readiness-v1/bundle.json",
    "production_bundle_md": "docs/audit/bundles/production-readiness-v1/bundle.md",
    "generalization_audit_gates": "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json",
    "online_shadow_policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
    "execution_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "production_readiness_plan": "docs/audit/ml-production-readiness-plan-v1.json",
    "superseded_plan_md": "docs/audit/ml-production-readiness-plan-v1.md",
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "phase1_review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
}


def _copy_fixture_repo(tmp_path: Path, *, include_production_bundle: bool = False) -> Path:
    root = tmp_path / "fixture-repo"
    for key, rel in sorted(FIXTURE_RELS.items()):
        if key.startswith("production_bundle") and not include_production_bundle:
            continue
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
        "generalization_audit_gates_path": _fixture(root, "generalization_audit_gates"),
        "online_shadow_policy_path": _fixture(root, "online_shadow_policy"),
        "execution_authorization_grant_path": _fixture(root, "execution_authorization_grant"),
        "production_readiness_plan_path": _fixture(root, "production_readiness_plan"),
    }


def _write_pre_request_bundle(root: Path) -> Path:
    out_json = root / "docs/audit/bundles/production-readiness-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-readiness-v1/bundle.md"
    write_ml_shadow_scorer_production_readiness_bundle(
        production_readiness_criteria_path=_fixture(root, "criteria"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
        **_optional_kwargs(root),
    )
    return out_json


def _write_request_bundle(root: Path) -> Path:
    bundle_path = _write_pre_request_bundle(root)
    request_ml_shadow_scorer_production_readiness_bundle(bundle_path=bundle_path, repo_root=root)
    return bundle_path


def test_assemble_pre_request_bundle_and_verify_not_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_request_bundle(root)
    payload = _load(bundle_path)
    result = verify_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_request_filed=False,
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_production_readiness_bundle"
    assert payload["metadata"]["bundle_revision"] == 1
    assert payload["authorization"]["production_readiness_authorization_requested"] is False
    assert payload["authorization"]["production_readiness_authorization_granted"] is False
    assert payload["recommended_next_stage"] == "request_production_readiness_authorization_v1"
    assert result["verification_mode"] == "pre_request"


def test_apply_request_and_verify_post_request_same_bundle_path(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_request_bundle(root)

    requested = request_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        requester="Requester Name",
        request_notes="request notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_request_filed=True,
    )

    assert requested["authorization"]["production_readiness_authorization_requested"] is True
    assert requested["authorization"]["production_readiness_authorization_granted"] is False
    assert requested["authorization"]["request_decision"]["requester"] == "Requester Name"
    assert requested["authorization"]["request_decision"]["request_notes"] == "request notes"
    assert requested["posture"]["missing_production_readiness_authorization"] is True
    assert requested["recommended_next_stage"] == "record_production_readiness_authorization_grant_v1"
    assert result["verification_mode"] == "post_request"


def test_grant_from_revision_1_request_and_verify_post_grant(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_request_bundle(root)

    granted = grant_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        second_reviewer="Second Reviewer",
        grant_notes="grant notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_grant_filed=True,
    )

    assert granted["metadata"]["bundle_revision"] == 2
    assert granted["authorization"]["production_readiness_authorization_requested"] is True
    assert granted["authorization"]["production_readiness_authorization_granted"] is True
    assert granted["authorization"]["grant_decision"]["decision"] == "granted"
    assert granted["authorization"]["grant_decision"]["second_reviewer"] == "Second Reviewer"
    assert granted["posture"]["missing_production_readiness_authorization"] is False
    assert granted["recommended_next_stage"] == "begin_production_scoped_online_shadow_plan_v1"
    assert result["verification_mode"] == "post_grant"


def test_rejects_grant_on_pre_request_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_request_bundle(root)

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="production_readiness_authorization_requested"):
        grant_ml_shadow_scorer_production_readiness_bundle(
            bundle_path=bundle_path,
            second_reviewer="Second Reviewer",
            repo_root=root,
        )


def test_rejects_grant_on_already_granted_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_request_bundle(root)
    grant_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        second_reviewer="Second Reviewer",
        repo_root=root,
    )

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="bundle_revision"):
        grant_ml_shadow_scorer_production_readiness_bundle(
            bundle_path=bundle_path,
            second_reviewer="Another Reviewer",
            repo_root=root,
        )


def test_rejects_grant_if_missing_authorization_already_false_on_input(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_request_bundle(root)
    payload = _load(bundle_path)
    payload["posture"]["missing_production_readiness_authorization"] = False
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="missing_production_readiness_authorization"):
        grant_ml_shadow_scorer_production_readiness_bundle(
            bundle_path=bundle_path,
            second_reviewer="Second Reviewer",
            repo_root=root,
        )


def test_rejects_grant_without_review_basis_or_with_same_second_reviewer(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_request_bundle(root)

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="second_reviewer"):
        grant_ml_shadow_scorer_production_readiness_bundle(bundle_path=bundle_path, repo_root=root)
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="second_reviewer"):
        grant_ml_shadow_scorer_production_readiness_bundle(
            bundle_path=bundle_path,
            owner="Matt Maitland",
            second_reviewer="Matt Maitland",
            repo_root=root,
        )


def test_accepts_grant_with_owner_documented_equivalent_review(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_request_bundle(root)

    granted = grant_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="Owner reviewed criteria, Phase 2 evidence, and grant-time gate resolutions.",
        repo_root=root,
    )

    assert granted["authorization"]["production_readiness_authorization_granted"] is True
    assert granted["authorization"]["grant_decision"]["owner_documents_equivalent_review"].startswith("Owner reviewed")


def test_post_grant_rejects_unresolved_gates_and_required_grant_statuses(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_request_bundle(root)
    granted = grant_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        second_reviewer="Second Reviewer",
        repo_root=root,
    )

    missing_gate = copy.deepcopy(granted)
    missing_gate["evidence"]["gate_assessments"].pop("offline_metric_gate_required")
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="missing keys"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            missing_gate,
            repo_root=root,
            expect_grant_filed=True,
        )

    partial_gate = copy.deepcopy(granted)
    partial_gate["evidence"]["gate_assessments"]["label_volume_and_balance_gate_required"]["status"] = "partial"
    partial_gate["evidence"]["gate_assessments"]["label_volume_and_balance_gate_required"]["satisfies_criteria_detail"] = False
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="post-grant resolved"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            partial_gate,
            repo_root=root,
            expect_grant_filed=True,
        )

    incident_wrong = copy.deepcopy(granted)
    incident_wrong["evidence"]["gate_assessments"]["incident_response_and_revocation_plan_required"]["status"] = "satisfied_by_upstream"
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="incident_response"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            incident_wrong,
            repo_root=root,
            expect_grant_filed=True,
        )

    multi_wrong = copy.deepcopy(granted)
    multi_wrong["evidence"]["gate_assessments"]["multi_reviewer_adjudication_required"]["status"] = "satisfied_by_upstream"
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="multi_reviewer"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            multi_wrong,
            repo_root=root,
            expect_grant_filed=True,
        )


def test_posture_clears_only_missing_production_readiness_authorization(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    granted = grant_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=_write_request_bundle(root),
        second_reviewer="Second Reviewer",
        repo_root=root,
    )
    posture = granted["posture"]
    blockers = granted["shadow_and_production_blockers"]

    assert posture["missing_production_readiness_authorization"] is False
    assert blockers["missing_production_readiness_authorization"] is False
    assert blockers["blockers_changed_by_grant"] == ["missing_production_readiness_authorization"]
    assert posture["online_shadow_execution_enabled"] is False
    assert posture["production_default_allowed"] is False
    assert posture["api_web_changes_allowed"] is False
    assert posture["user_visible_ranking_changed"] is False
    assert granted["execution"]["production_readiness_execution_performed"] is False
    assert granted["execution"]["production_shadow_pilot_executed"] is False
    assert granted["writes_performed"] is False
    assert granted["runtime_writes_performed"] is False


def test_committed_fixture_matches_revision_2_post_grant_state(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path, include_production_bundle=True)
    result = verify_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=_fixture(root, "production_bundle"),
        repo_root=root,
        expect_grant_filed=True,
    )
    payload = _load(_fixture(root, "production_bundle"))

    assert payload["metadata"]["bundle_revision"] == 2
    assert payload["authorization"]["production_readiness_authorization_requested"] is True
    assert payload["authorization"]["production_readiness_authorization_granted"] is True
    assert payload["posture"]["missing_production_readiness_authorization"] is False
    assert result["recommended_next_stage"] == "begin_production_scoped_online_shadow_plan_v1"


def test_rejects_criteria_not_defined(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    criteria = _load(_fixture(root, "criteria"))
    criteria["production_readiness_criteria_defined"] = False
    _write_json(_fixture(root, "criteria"), criteria)

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="production_readiness_criteria_defined"):
        assemble_ml_shadow_scorer_production_readiness_bundle_payload(
            production_readiness_criteria_path=_fixture(root, "criteria"),
            phase_bundle_path=_fixture(root, "phase2_bundle"),
            repo_root=root,
            **_optional_kwargs(root),
        )


def test_rejects_phase2_not_accepted(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    phase2 = _load(_fixture(root, "phase2_bundle"))
    phase2["review"]["phase2_write_pilot_accepted"] = False
    phase2["review"]["review_decision"]["decision"] = "not_accepted"
    phase2["recommended_next_stage"] = "remediate_online_shadow_phase2_write_pilot_v1"
    _write_json(_fixture(root, "phase2_bundle"), phase2)
    criteria = _load(_fixture(root, "criteria"))
    new_phase_sha = _sha256(_fixture(root, "phase2_bundle"))
    criteria["metadata"]["inputs"][0]["sha256"] = new_phase_sha
    criteria["source_evidence"]["phase_bundle"]["sha256"] = new_phase_sha
    _write_json(_fixture(root, "criteria"), criteria)

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="phase2_write_pilot_accepted"):
        assemble_ml_shadow_scorer_production_readiness_bundle_payload(
            production_readiness_criteria_path=_fixture(root, "criteria"),
            phase_bundle_path=_fixture(root, "phase2_bundle"),
            repo_root=root,
            **_optional_kwargs(root),
        )


def test_rejects_sha_tamper(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = assemble_ml_shadow_scorer_production_readiness_bundle_payload(
        production_readiness_criteria_path=_fixture(root, "criteria"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        repo_root=root,
        **_optional_kwargs(root),
    )
    bundle["metadata"]["legacy_artifacts_index"][0]["sha256"] = "0" * 64

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="sha256 mismatch"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(bundle, repo_root=root)


def test_rejects_identity_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = assemble_ml_shadow_scorer_production_readiness_bundle_payload(
        production_readiness_criteria_path=_fixture(root, "criteria"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        repo_root=root,
        **_optional_kwargs(root),
    )
    bundle["metadata"]["pinned_identity"]["scorer_id"] = "wrong-scorer"

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="pinned_identity.scorer_id"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(bundle, repo_root=root)


def test_rejects_authorization_granted_true_on_request(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    pre_bundle = assemble_ml_shadow_scorer_production_readiness_bundle_payload(
        production_readiness_criteria_path=_fixture(root, "criteria"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        repo_root=root,
        **_optional_kwargs(root),
    )
    requested = apply_production_readiness_authorization_request(pre_bundle)
    requested["authorization"]["production_readiness_authorization_granted"] = True

    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="authorization_granted"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            requested,
            repo_root=root,
            expect_request_filed=True,
        )


def test_rejects_missing_gate_assessment_key_and_missing_rationale(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = apply_production_readiness_authorization_request(
        assemble_ml_shadow_scorer_production_readiness_bundle_payload(
            production_readiness_criteria_path=_fixture(root, "criteria"),
            phase_bundle_path=_fixture(root, "phase2_bundle"),
            repo_root=root,
            **_optional_kwargs(root),
        )
    )
    missing_gate = copy.deepcopy(bundle)
    missing_gate["evidence"]["gate_assessments"].pop("offline_metric_gate_required")
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="missing keys"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(missing_gate, repo_root=root)

    missing_rationale = copy.deepcopy(bundle)
    missing_rationale["evidence"]["gate_assessments"]["incident_response_and_revocation_plan_required"]["rationale"] = ""
    with pytest.raises(MLShadowScorerProductionReadinessBundleError, match="rationale"):
        verify_ml_shadow_scorer_production_readiness_bundle_payload(missing_rationale, repo_root=root)


def test_gate_status_partial_and_open_are_allowed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = apply_production_readiness_authorization_request(
        assemble_ml_shadow_scorer_production_readiness_bundle_payload(
            production_readiness_criteria_path=_fixture(root, "criteria"),
            phase_bundle_path=_fixture(root, "phase2_bundle"),
            repo_root=root,
            **_optional_kwargs(root),
        )
    )
    assessments = bundle["evidence"]["gate_assessments"]

    assert assessments["multi_reviewer_adjudication_required"]["status"] == "partial"
    assert assessments["incident_response_and_revocation_plan_required"]["status"] == "open_for_grant"
    verify_ml_shadow_scorer_production_readiness_bundle_payload(
        bundle,
        repo_root=root,
        expect_request_filed=True,
    )


def test_no_shadow_runs_files_created(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_files(root)
    criteria_before = _fixture(root, "criteria").read_text(encoding="utf-8")
    phase2_before = _fixture(root, "phase2_bundle").read_text(encoding="utf-8")

    bundle_path = _write_pre_request_bundle(root)
    request_ml_shadow_scorer_production_readiness_bundle(bundle_path=bundle_path, repo_root=root)
    grant_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=bundle_path,
        second_reviewer="Second Reviewer",
        repo_root=root,
    )

    assert _shadow_runs_files(root) == before
    assert _fixture(root, "criteria").read_text(encoding="utf-8") == criteria_before
    assert _fixture(root, "phase2_bundle").read_text(encoding="utf-8") == phase2_before


def test_cli_smoke_assemble_request_verify(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/bundles/production-readiness-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-readiness-v1/bundle.md"
    assemble_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-readiness-bundle-assemble",
        "--production-readiness-criteria",
        str(_fixture(root, "criteria")),
        "--phase-bundle",
        str(_fixture(root, "phase2_bundle")),
        "--generalization-audit-gates",
        str(_fixture(root, "generalization_audit_gates")),
        "--online-shadow-policy",
        str(_fixture(root, "online_shadow_policy")),
        "--execution-authorization-grant",
        str(_fixture(root, "execution_authorization_grant")),
        "--production-readiness-plan",
        str(_fixture(root, "production_readiness_plan")),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(root),
    ]
    assemble = subprocess.run(assemble_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert assemble.stdout.splitlines() == [
        "online-shadow-production-readiness-v1",
        "False",
        "request_production_readiness_authorization_v1",
    ]

    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-readiness-bundle-request",
        "--bundle",
        str(out_json),
        "--requester",
        "CLI Requester",
        "--request-notes",
        "cli notes",
        "--repo-root",
        str(root),
    ]
    request = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert request.stdout.splitlines() == [
        "requested",
        "True",
        "record_production_readiness_authorization_grant_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-readiness-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-request-filed",
        "--repo-root",
        str(root),
    ]
    verify = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verify.stdout.splitlines() == [
        "passed",
        "post_request",
        "online-shadow-production-readiness-v1",
        "record_production_readiness_authorization_grant_v1",
    ]

    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-readiness-bundle-grant",
        "--bundle",
        str(out_json),
        "--second-reviewer",
        "CLI Second Reviewer",
        "--grant-notes",
        "cli grant notes",
        "--repo-root",
        str(root),
    ]
    grant = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert grant.stdout.splitlines() == [
        "granted",
        "True",
        "begin_production_scoped_online_shadow_plan_v1",
    ]

    verify_grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-readiness-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-grant-filed",
        "--repo-root",
        str(root),
    ]
    verify_grant = subprocess.run(
        verify_grant_cmd,
        cwd=PACKAGE_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert verify_grant.stdout.splitlines() == [
        "passed",
        "post_grant",
        "online-shadow-production-readiness-v1",
        "begin_production_scoped_online_shadow_plan_v1",
    ]


def test_upstream_verifiers_still_pass(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    criteria_result = verify_ml_shadow_scorer_production_readiness_authorization_criteria(
        criteria_path=_fixture(root, "criteria"),
        repo_root=root,
    )
    phase2_result = verify_ml_shadow_scorer_phase_bundle(
        bundle_path=_fixture(root, "phase2_bundle"),
        repo_root=root,
        expect_pilot_reviewed=True,
    )

    assert criteria_result["verification_status"] == "passed"
    assert phase2_result["verification_mode"] == "post_review"


def test_no_forbidden_imports_or_database_url_on_bundle_cli() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_readiness_bundle.py").read_text(
        encoding="utf-8"
    )
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    assemble_start = cli_source.index('"ml-shadow-scorer-production-readiness-bundle-assemble"')
    next_command = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', assemble_start)
    assert "--database-url" not in cli_source[assemble_start:next_command]
