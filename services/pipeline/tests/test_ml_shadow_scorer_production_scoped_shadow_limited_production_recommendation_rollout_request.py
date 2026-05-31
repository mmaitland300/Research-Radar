"""Tests for requesting limited production recommendation rollout authorization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_CAVEATS,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_EXPLICITLY_NOT_INCLUDED,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_FUTURE_GRANT_REQUIREMENTS,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_ONLY_EXPLICITLY_NOT_INCLUDED,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_SCOPE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE,
    apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_request,
    request_limited_production_recommendation_rollout_ml_shadow_scorer_production_scoped_shadow_bundle,
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


def _prepare_rev28_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 29:
        payload = bundle_module._without_limited_production_recommendation_rollout_request_payload(payload)
    if payload["metadata"]["bundle_revision"] != 28:
        raise AssertionError(
            "expected committed production-scoped bundle revision 28 after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE:
        raise AssertionError("expected post controlled production recommendation pilot review accepted next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev28_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("limited-production-recommendation-rollout-request-template"))
    _prepare_rev28_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _request(root: Path, bundle_path: Path) -> dict[str, Any]:
    return request_limited_production_recommendation_rollout_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Limited Rollout Requester",
        request_notes="request only, grants nothing",
        generated_at="2026-05-31T13:00:00Z",
        repo_root=root,
    )


def test_happy_path_requests_revision_twenty_nine_without_grant_or_serving(
    tmp_path: Path,
    rev28_template_root: Path,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    prior_authorization_slices = {
        key: before["authorization"][key]
        for key in (
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
            "controlled_production_recommendation_request_decision",
            "controlled_production_recommendation_requested_scope",
            "controlled_production_recommendation_grant_decision",
            "controlled_production_recommendation_granted_scope",
        )
    }

    requested = _request(root, bundle_path)
    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_limited_production_recommendation_rollout_request_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 28
    assert requested["metadata"]["bundle_revision"] == 29
    assert requested["metadata"]["generated_at"] == "2026-05-31T13:00:00Z"
    assert requested["plan"] == before["plan"]
    assert requested["proof"] == before["proof"]
    assert requested["execution"] == before["execution"]
    assert requested["review"] == before["review"]
    assert requested["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    for key, before_value in prior_authorization_slices.items():
        assert requested["authorization"][key] == before_value

    authorization = requested["authorization"]
    assert authorization["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested"] is True
    assert authorization["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted"] is False
    assert authorization["prod_scoped_shadow_limited_production_recommendation_rollout_authorized"] is False
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorized"] is True
    assert authorization["prod_scoped_shadow_execution_authorized"] is False
    decision = authorization["limited_production_recommendation_rollout_request_decision"]
    assert decision["decision"] == "requested"
    assert decision["requester"] == "Limited Rollout Requester"
    assert decision["request_notes"] == "request only, grants nothing"
    scope = authorization["limited_production_recommendation_rollout_requested_scope"]
    assert scope["authorization_scope"] == LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_SCOPE
    assert scope["future_grant_would_require"] == list(
        LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_FUTURE_GRANT_REQUIREMENTS
    )
    assert set(LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        scope["explicitly_not_included"]
    )
    assert scope["limited_production_recommendation_rollout_output_requested_for_future_grant_only"] is True
    assert scope["real_production_recommendation_output_to_public_users_at_request_time"] is False
    assert scope["public_production_serving_enabled_at_request_time"] is False
    assert scope["bridge_recommendations_included"] is False

    for section_name in ("posture", "shadow_and_production_blockers"):
        section = requested[section_name]
        assert section["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested"] is True
        assert section["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted"] is False
        assert section["prod_scoped_shadow_limited_production_recommendation_rollout_authorized"] is False
        assert section["missing_prod_scoped_shadow_limited_production_recommendation_rollout_authorization"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_pilot_reviewed"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_pilot_accepted"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_authorized"] is True
        assert section["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] is False
        assert section["prod_scoped_shadow_execution_authorized"] is False
        assert section["online_shadow_execution_enabled"] is False
        assert section["production_default_allowed"] is False
        assert section["api_web_changes_allowed"] is False
        assert section["user_visible_ranking_changed"] is False
        assert section["writes_performed"] is False
        assert section["runtime_writes_performed"] is False

    blockers = requested["shadow_and_production_blockers"]
    assert blockers["blockers_introduced_by_limited_production_recommendation_rollout_request"] == [
        "missing_prod_scoped_shadow_limited_production_recommendation_rollout_authorization"
    ]
    assert blockers["blockers_cleared_by_limited_production_recommendation_rollout_request"] == []
    assert blockers["blockers_unchanged_by_limited_production_recommendation_rollout_request"] is True
    assert "blockers_changed_by_limited_production_recommendation_rollout_request" not in blockers
    assert requested["writes_performed"] is False
    assert requested["runtime_writes_performed"] is False
    assert requested["recommended_next_stage"] == POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE
    assert set(LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_CAVEATS).issubset(requested["caveats"])
    assert verified["verification_mode"] == "post_limited_production_recommendation_rollout_request"


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 27, "bundle_revision"),
        ("recommended_next_stage", "wrong_next_stage", "recommended_next_stage"),
    ],
)
def test_wrong_revision_or_next_stage_rejected(
    tmp_path: Path,
    rev28_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=message):
        _request(root, bundle_path)


def test_controlled_pilot_review_not_accepted_rejected(
    tmp_path: Path,
    rev28_template_root: Path,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    payload["review"]["prod_scoped_shadow_controlled_production_recommendation_pilot_accepted"] = False
    payload["review"]["controlled_production_recommendation_pilot_review_decision"]["decision"] = "not_accepted"
    payload["review"]["controlled_production_recommendation_pilot_review_decision"]["failed_review_checks"] = [
        "response_status_200"
    ]
    payload["review"]["controlled_production_recommendation_pilot_review_decision"]["checks"][
        "response_status_200"
    ] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="accepted|failed checks"):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_request(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested",
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted",
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorized",
        "authorization.limited_production_recommendation_rollout_request_decision",
        "authorization.limited_production_recommendation_rollout_requested_scope",
        "authorization.limited_production_recommendation_rollout_grant_decision",
    ],
)
def test_double_request_or_grant_payload_rejected(
    tmp_path: Path,
    rev28_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    if field_path.endswith("_decision"):
        _set_path(payload, field_path, {"decision": "requested"})
    elif field_path.endswith("_scope"):
        _set_path(payload, field_path, {"authorization_scope": LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_SCOPE})
    else:
        _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_request(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_execution_authorized",
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted",
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
    ],
)
def test_forbidden_output_or_global_flags_rejected(
    tmp_path: Path,
    rev28_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_request(payload)


def test_compositional_strip_verifies_as_revision_twenty_eight(
    tmp_path: Path,
    rev28_template_root: Path,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _request(root, bundle_path)
    payload = _load(bundle_path)

    stripped = bundle_module._without_limited_production_recommendation_rollout_request_payload(payload)

    assert stripped["metadata"]["bundle_revision"] == 28
    assert stripped["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    assert "limited_production_recommendation_rollout_request_decision" not in stripped["authorization"]
    assert "limited_production_recommendation_rollout_requested_scope" not in stripped["authorization"]
    assert "missing_prod_scoped_shadow_limited_production_recommendation_rollout_authorization" not in stripped["posture"]
    for item in LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_ONLY_EXPLICITLY_NOT_INCLUDED:
        assert item not in stripped["authorization"]["explicitly_not_included"]
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_controlled_production_recommendation_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["verification_mode"] == "post_controlled_production_recommendation_pilot_review"


def test_cli_smoke_request_then_verify_revision_twenty_nine(
    tmp_path: Path,
    rev28_template_root: Path,
) -> None:
    root = _copy_template_repo(rev28_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-limited-production-recommendation-rollout",
        "--bundle",
        str(bundle_path),
        "--requester",
        "CLI Limited Requester",
        "--request-notes",
        "cli limited rollout request notes",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-limited-production-recommendation-rollout-request-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_limited_production_recommendation_rollout_request",
        "online-shadow-production-scoped-v1",
        POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_limited_production_recommendation_rollout_request_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 29:
        pytest.skip("committed production-scoped bundle is not revision 29 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_limited_production_recommendation_rollout_request_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 29
    assert result["recommended_next_stage"] == POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE
