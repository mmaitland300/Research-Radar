"""Tests for requesting controlled production recommendation authorization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_CAVEATS,
    CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_EXPLICITLY_NOT_INCLUDED,
    CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_FUTURE_GRANT_REQUIREMENTS,
    CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_ONLY_EXPLICITLY_NOT_INCLUDED,
    CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_SCOPE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    apply_production_scoped_shadow_controlled_production_recommendation_authorization_request,
    request_controlled_production_recommendation_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    REPO_ROOT,
    _copy_fixture_repo,
    _copy_template_repo,
    _load,
    _set_path,
    _write_json,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _prepare_rev24_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 27:
        payload = bundle_module._without_controlled_production_recommendation_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] == 26:
        payload = bundle_module._without_controlled_production_recommendation_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 25:
        payload = bundle_module._without_controlled_production_recommendation_request_payload(payload)
    if payload["metadata"]["bundle_revision"] != 24:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 24 after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE:
        raise AssertionError("expected post production default/API/user-visible pilot review accepted next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev24_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("controlled-production-recommendation-request-template"))
    _prepare_rev24_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _request(root: Path, bundle_path: Path) -> dict[str, Any]:
    return request_controlled_production_recommendation_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Controlled Requester",
        request_notes="request only, no output",
        repo_root=root,
    )


def test_happy_path_requests_revision_twenty_five_without_output_or_grant(
    tmp_path: Path,
    rev24_template_root: Path,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    requested = _request(root, bundle_path)
    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_controlled_production_recommendation_request_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 24
    assert requested["metadata"]["bundle_revision"] == 25
    assert requested["plan"] == before["plan"]
    assert requested["proof"] == before["proof"]
    assert requested["execution"] == before["execution"]
    assert requested["review"] == before["review"]
    assert requested["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    for path in (
        "request_decision",
        "requested_scope",
        "grant_decision",
        "granted_scope",
        "live_read_only_grant_decision",
        "live_read_only_granted_scope",
        "live_execution_request_decision",
        "live_execution_requested_scope",
        "live_execution_grant_decision",
        "live_execution_granted_scope",
        "flag_enablement_request_decision",
        "flag_enablement_requested_scope",
        "flag_enablement_grant_decision",
        "flag_enablement_granted_scope",
        "production_default_api_user_visible_request_decision",
        "production_default_api_user_visible_requested_scope",
        "production_default_api_user_visible_grant_decision",
        "production_default_api_user_visible_granted_scope",
    ):
        assert requested["authorization"][path] == before["authorization"][path]

    authorization = requested["authorization"]
    assert authorization["prod_scoped_shadow_production_default_api_user_visible_authorized"] is True
    assert authorization["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert authorization["prod_scoped_shadow_live_execution_authorized"] is True
    assert authorization["prod_scoped_shadow_execution_authorized"] is False
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorization_requested"] is True
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorization_granted"] is False
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorized"] is False
    decision = authorization["controlled_production_recommendation_request_decision"]
    assert decision["decision"] == "requested"
    assert decision["requester"] == "Controlled Requester"
    assert decision["request_notes"] == "request only, no output"
    scope = authorization["controlled_production_recommendation_requested_scope"]
    assert scope["authorization_scope"] == CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_SCOPE
    assert scope["future_grant_would_require"] == list(
        CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_FUTURE_GRANT_REQUIREMENTS
    )
    assert set(CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        scope["explicitly_not_included"]
    )
    assert scope["controlled_production_recommendation_output_requested_for_future_grant_only"] is True
    assert scope["real_recommendations_emitted_at_request_time"] is False
    assert scope["bridge_recommendations_included"] is False

    for section_name in ("posture", "shadow_and_production_blockers"):
        section = requested[section_name]
        assert section["prod_scoped_shadow_controlled_production_recommendation_authorization_requested"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_authorization_granted"] is False
        assert section["prod_scoped_shadow_controlled_production_recommendation_authorized"] is False
        assert section["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] is True
        assert section["prod_scoped_shadow_production_default_api_user_visible_authorized"] is True
        assert section["prod_scoped_shadow_flag_enablement_authorized"] is True
        assert section["prod_scoped_shadow_live_execution_authorized"] is True
        assert section["prod_scoped_shadow_execution_authorized"] is False
        assert section["online_shadow_execution_enabled"] is False
        assert section["production_default_allowed"] is False
        assert section["api_web_changes_allowed"] is False
        assert section["user_visible_ranking_changed"] is False
        assert section["writes_performed"] is False
        assert section["runtime_writes_performed"] is False

    blockers = requested["shadow_and_production_blockers"]
    assert blockers["blockers_introduced_by_controlled_production_recommendation_request"] == [
        "missing_prod_scoped_shadow_controlled_production_recommendation_authorization"
    ]
    assert blockers["blockers_cleared_by_controlled_production_recommendation_request"] == []
    assert blockers["blockers_unchanged_by_controlled_production_recommendation_request"] is True
    assert "blockers_changed_by_controlled_production_recommendation_request" not in blockers
    assert requested["writes_performed"] is False
    assert requested["runtime_writes_performed"] is False
    assert requested["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE
    assert set(CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_CAVEATS).issubset(requested["caveats"])
    assert verified["verification_mode"] == "post_controlled_production_recommendation_request"


def test_compositional_verify_strips_revision_twenty_five_overlay(
    tmp_path: Path,
    rev24_template_root: Path,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _request(root, bundle_path)
    payload = _load(bundle_path)

    stripped = bundle_module._without_controlled_production_recommendation_request_payload(payload)

    assert stripped["metadata"]["bundle_revision"] == 24
    assert stripped["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    assert "controlled_production_recommendation_request_decision" not in stripped["authorization"]
    assert "controlled_production_recommendation_requested_scope" not in stripped["authorization"]
    assert "missing_prod_scoped_shadow_controlled_production_recommendation_authorization" not in stripped["posture"]
    for item in CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_ONLY_EXPLICITLY_NOT_INCLUDED:
        assert item not in stripped["authorization"]["explicitly_not_included"]
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_production_default_api_user_visible_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )


def test_rejects_wrong_revision_wrong_stage_and_double_request(
    tmp_path: Path,
    rev24_template_root: Path,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 23
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        _request(root, bundle_path)

    root = _copy_template_repo(rev24_template_root, tmp_path / "wrong-stage")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        _request(root, bundle_path)

    root = _copy_template_repo(rev24_template_root, tmp_path / "double")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _request(root, bundle_path)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        _request(root, bundle_path)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.controlled_production_recommendation_request_decision",
        "authorization.controlled_production_recommendation_requested_scope",
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorization_requested",
    ],
)
def test_rejects_existing_request_payload(
    tmp_path: Path,
    rev24_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    if field_path.endswith("_decision"):
        _set_path(payload, field_path, {"decision": "requested"})
    elif field_path.endswith("_scope"):
        _set_path(payload, field_path, {"authorization_scope": CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_SCOPE})
    else:
        _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _request(root, bundle_path)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorization_granted",
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorized",
    ],
)
def test_rejects_grant_or_authorized_true_at_request_time(
    tmp_path: Path,
    rev24_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _request(root, bundle_path)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "posture.writes_performed",
        "posture.runtime_writes_performed",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
    ],
)
def test_rejects_forbidden_output_or_global_flags_true(
    tmp_path: Path,
    rev24_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _request(root, bundle_path)


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        ("review.prod_scoped_shadow_production_default_api_user_visible_pilot_accepted", False),
        ("review.production_default_api_user_visible_pilot_review_decision.decision", "not_accepted"),
        ("review.production_default_api_user_visible_pilot_review_decision.failed_review_checks", ["failed"]),
    ],
)
def test_prior_production_default_pilot_review_missing_or_rejected_fails_closed(
    tmp_path: Path,
    rev24_template_root: Path,
    field_path: str,
    value: Any,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _request(root, bundle_path)


@pytest.mark.parametrize(
    "execution_key",
    [
        "live_read_only_pilot_run",
        "live_execution_pilot_run",
        "flag_enablement_pilot_run",
        "production_default_api_user_visible_pilot_run",
    ],
)
def test_prior_upstream_run_slices_missing_fail_closed(
    tmp_path: Path,
    rev24_template_root: Path,
    execution_key: str,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["execution"].pop(execution_key)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _request(root, bundle_path)


def test_request_section_verifier_validates_decision_scope_and_exclusions(
    tmp_path: Path,
    rev24_template_root: Path,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    updated = apply_production_scoped_shadow_controlled_production_recommendation_authorization_request(
        payload,
        requester="Verifier",
        request_notes="verify request",
        generated_at="2026-05-30T23:30:00Z",
    )
    bundle_module._verify_controlled_production_recommendation_request_section(updated["authorization"])

    bad = _load(bundle_path)
    bad = apply_production_scoped_shadow_controlled_production_recommendation_authorization_request(
        bad,
        requester="Verifier",
        generated_at="2026-05-30T23:30:00Z",
    )
    bad["authorization"]["controlled_production_recommendation_requested_scope"]["explicitly_not_included"].remove(
        "bridge recommendations"
    )
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bridge recommendations"):
        bundle_module._verify_controlled_production_recommendation_request_section(bad["authorization"])


def test_cli_smoke_request_then_verify_revision_twenty_five(
    tmp_path: Path,
    rev24_template_root: Path,
) -> None:
    root = _copy_template_repo(rev24_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-controlled-production-recommendation",
        "--bundle",
        str(bundle_path),
        "--requester",
        "CLI Controlled Requester",
        "--request-notes",
        "cli request only",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-controlled-production-recommendation-request-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_controlled_production_recommendation_request",
        "online-shadow-production-scoped-v1",
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_controlled_production_recommendation_request_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 25:
        pytest.skip("committed production-scoped bundle is no longer revision 25")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_controlled_production_recommendation_request_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 25
    assert result["verification_mode"] == "post_controlled_production_recommendation_request"
