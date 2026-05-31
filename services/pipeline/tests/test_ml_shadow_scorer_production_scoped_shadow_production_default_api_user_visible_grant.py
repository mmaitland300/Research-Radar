"""Tests for granting production default/API/user-visible shadow authorization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_CAVEATS,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_AUTHORIZES_FOR_CHAIN_ONLY,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_TIME_BOUNDARIES,
    apply_production_scoped_shadow_production_default_api_user_visible_authorization_grant,
    grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    _copy_fixture_repo,
    _copy_template_repo,
    _load,
    _write_json,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _set_path(payload: dict[str, Any], dotted_path: str, value: Any) -> None:
    current: dict[str, Any] = payload
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def _shadow_runs_files(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*") if path.is_file()}


def _prepare_rev21_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 25:
        payload = bundle_module._without_controlled_production_recommendation_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 24:
        payload = bundle_module._without_production_default_api_user_visible_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 23:
        payload = bundle_module._without_production_default_api_user_visible_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] == 22:
        payload = bundle_module._without_production_default_api_user_visible_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] != 21:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 21 or 22, got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE:
        raise AssertionError("expected post production default/API/user-visible request next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev21_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("production-default-api-user-visible-grant-template"))
    _prepare_rev21_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _apply_grant(payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    grant_kwargs = {
        "owner_documents_equivalent_review": "owner equivalent production default grant review",
        **kwargs,
    }
    return apply_production_scoped_shadow_production_default_api_user_visible_authorization_grant(
        payload,
        **grant_kwargs,
    )


def test_happy_path_grant_from_revision_twenty_one_to_twenty_two(
    tmp_path: Path,
    rev21_template_root: Path,
) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    granted = grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner="Production Default Grant Owner",
        second_reviewer="Production Default Grant Reviewer",
        grant_notes="production default grant notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_production_default_api_user_visible_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 21
    assert granted["metadata"]["bundle_revision"] == 22
    assert granted["plan"] == before["plan"]
    assert granted["proof"] == before["proof"]
    assert granted["execution"] == before["execution"]
    assert granted["review"] == before["review"]
    assert granted["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert granted["authorization"]["production_default_api_user_visible_request_decision"] == before[
        "authorization"
    ]["production_default_api_user_visible_request_decision"]
    assert granted["authorization"]["production_default_api_user_visible_requested_scope"] == before[
        "authorization"
    ]["production_default_api_user_visible_requested_scope"]
    assert granted["authorization"]["prod_scoped_shadow_production_default_api_user_visible_authorization_requested"] is True
    assert granted["authorization"]["prod_scoped_shadow_production_default_api_user_visible_authorization_granted"] is True
    assert granted["authorization"]["prod_scoped_shadow_production_default_api_user_visible_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    grant_decision = granted["authorization"]["production_default_api_user_visible_grant_decision"]
    assert grant_decision["decision"] == "granted"
    assert grant_decision["owner"] == "Production Default Grant Owner"
    assert grant_decision["second_reviewer"] == "Production Default Grant Reviewer"
    assert grant_decision["grant_notes"] == "production default grant notes"
    granted_scope = granted["authorization"]["production_default_api_user_visible_granted_scope"]
    assert granted_scope["authorization_scope"] == PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE
    assert "runtime_feature_flag" not in granted_scope
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_AUTHORIZES_FOR_CHAIN_ONLY).issubset(
        granted_scope["authorizes_for_chain_only"]
    )
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED).issubset(
        granted_scope["explicitly_still_not_included"]
    )
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_TIME_BOUNDARIES).issubset(
        granted_scope["grant_time_production_default_api_user_visible_boundaries"]
    )
    assert granted["posture"]["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"] is False
    assert granted["posture"]["online_shadow_execution_enabled"] is False
    assert granted["posture"]["production_default_allowed"] is False
    assert granted["posture"]["api_web_changes_allowed"] is False
    assert granted["posture"]["user_visible_ranking_changed"] is False
    assert (
        granted["plan"]["production_default_api_user_visible_separation"]["production_default_allowed"] is False
    )
    assert granted["plan"]["feature_flag_iam_config_requirements"]["prod_scoped_flag_enablement_authorized_now"] is False
    assert granted["shadow_and_production_blockers"]["blockers_cleared_by_production_default_api_user_visible_grant"] == [
        "missing_prod_scoped_shadow_production_default_api_user_visible_authorization"
    ]
    assert granted["shadow_and_production_blockers"]["blockers_introduced_by_production_default_api_user_visible_grant"] == []
    assert granted["shadow_and_production_blockers"]["blockers_unchanged_by_production_default_api_user_visible_grant"] is True
    assert "blockers_changed_by_production_default_api_user_visible_grant" not in granted["shadow_and_production_blockers"]
    assert granted["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_CAVEATS).issubset(granted["caveats"])
    assert "production_default_api_user_visible_pilot_run" not in granted.get("execution", {})
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_production_default_api_user_visible_grant"


def test_compositional_verify_strips_rev_twenty_two_overlay(
    tmp_path: Path,
    rev21_template_root: Path,
) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent production default grant review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_production_default_api_user_visible_grant_payload(payload)
    assert stripped["metadata"]["bundle_revision"] == 21
    assert stripped["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED:
        assert item not in stripped["authorization"]["explicitly_not_included"]
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_production_default_api_user_visible_request_filed=True,
        verify_local_pilot_files=False,
    )


def test_wrong_bundle_revision_rejection(tmp_path: Path, rev21_template_root: Path) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 20
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent production default grant review",
            repo_root=root,
        )


def test_wrong_recommended_next_stage_rejection(tmp_path: Path, rev21_template_root: Path) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent production default grant review",
            repo_root=root,
        )


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        ("authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted", True),
        ("authorization.prod_scoped_shadow_production_default_api_user_visible_authorized", True),
    ],
)
def test_double_grant_rejection(
    tmp_path: Path,
    rev21_template_root: Path,
    field_path: str,
    value: bool,
) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent production default grant review",
            repo_root=root,
        )


def test_rejects_pre_existing_production_default_grant_slices(
    tmp_path: Path,
    rev21_template_root: Path,
) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["authorization"]["production_default_api_user_visible_grant_decision"] = {
        "decision": "granted",
        "owner": "stale",
    }
    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="production_default_api_user_visible_grant_decision",
    ):
        _apply_grant(payload)

    payload = _load(bundle_path)
    payload["authorization"].pop("production_default_api_user_visible_grant_decision", None)
    payload["authorization"]["production_default_api_user_visible_granted_scope"] = {"authorization_scope": "stale"}
    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="production_default_api_user_visible_granted_scope",
    ):
        _apply_grant(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "posture.online_shadow_execution_enabled",
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
    ],
)
def test_rejects_accidental_runtime_or_global_enablement_flags(
    tmp_path: Path,
    rev21_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _apply_grant(payload)


def test_cli_smoke(tmp_path: Path, rev21_template_root: Path) -> None:
    root = _copy_template_repo(rev21_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-production-default-api-user-visible",
        "--bundle",
        str(bundle_path),
        "--owner",
        "Matt Maitland",
        "--owner-documents-equivalent-review",
        "owner equivalent production default grant review",
        "--grant-notes",
        "cli production default grant",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-production-default-api-user-visible-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_production_default_api_user_visible_grant",
        "online-shadow-production-scoped-v1",
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_production_default_api_user_visible_grant_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 22:
        pytest.skip("committed production-scoped bundle is not revision 22 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_production_default_api_user_visible_grant_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 22
    assert result["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE
    for caveat in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_CAVEATS:
        assert caveat in payload["caveats"]
