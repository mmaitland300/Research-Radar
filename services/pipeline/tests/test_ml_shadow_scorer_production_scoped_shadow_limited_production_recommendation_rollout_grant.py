"""Tests for granting limited production recommendation rollout authorization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_CAVEATS,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_SCOPE,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_STILL_NOT_INCLUDED,
    LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_TIME_BOUNDARIES,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_NEXT_STAGE,
    POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE,
    apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_grant,
    grant_limited_production_recommendation_rollout_ml_shadow_scorer_production_scoped_shadow_bundle,
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


def _prepare_rev29_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 30:
        payload = bundle_module._without_limited_production_recommendation_rollout_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] != 29:
        raise AssertionError(
            "expected committed production-scoped bundle revision 29 after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE:
        raise AssertionError("expected post limited production recommendation rollout request next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev29_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("limited-production-recommendation-rollout-grant-template"))
    _prepare_rev29_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _grant(root: Path, bundle_path: Path, **kwargs: Any) -> dict[str, Any]:
    grant_kwargs = {
        "owner_documents_equivalent_review": "owner equivalent limited rollout grant review",
        "generated_at": "2026-05-31T14:00:00Z",
        **kwargs,
    }
    return grant_limited_production_recommendation_rollout_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        **grant_kwargs,
    )


def test_happy_path_rev29_to_rev30_grant(tmp_path: Path, rev29_template_root: Path) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
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
            "limited_production_recommendation_rollout_request_decision",
            "limited_production_recommendation_rollout_requested_scope",
        )
    }

    granted = _grant(
        root,
        bundle_path,
        owner="Limited Rollout Grant Owner",
        second_reviewer="Limited Rollout Grant Reviewer",
        grant_notes="grant only, no rollout run",
    )
    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_limited_production_recommendation_rollout_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 29
    assert granted["metadata"]["bundle_revision"] == 30
    assert granted["metadata"]["generated_at"] == "2026-05-31T14:00:00Z"
    assert granted["plan"] == before["plan"]
    assert granted["proof"] == before["proof"]
    assert granted["execution"] == before["execution"]
    assert granted["review"] == before["review"]
    assert granted["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    for key, before_value in prior_authorization_slices.items():
        assert granted["authorization"][key] == before_value

    authorization = granted["authorization"]
    assert authorization["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested"] is True
    assert authorization["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted"] is True
    assert authorization["prod_scoped_shadow_limited_production_recommendation_rollout_authorized"] is True
    assert authorization["prod_scoped_shadow_execution_authorized"] is False
    grant_decision = authorization["limited_production_recommendation_rollout_grant_decision"]
    assert grant_decision["decision"] == "granted"
    assert grant_decision["owner"] == "Limited Rollout Grant Owner"
    assert grant_decision["second_reviewer"] == "Limited Rollout Grant Reviewer"
    assert grant_decision["grant_notes"] == "grant only, no rollout run"
    granted_scope = authorization["limited_production_recommendation_rollout_granted_scope"]
    assert granted_scope["authorization_scope"] == LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_SCOPE
    assert granted_scope["authorizes_for_chain_only"] == list(
        LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY
    )
    assert granted_scope["explicitly_still_not_included"] == list(
        LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_STILL_NOT_INCLUDED
    )
    assert granted_scope["grant_time_limited_production_recommendation_rollout_boundaries"] == list(
        LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_TIME_BOUNDARIES
    )
    assert granted_scope["route_path_allowlist"]
    assert granted_scope["monitoring_and_alerts"]
    assert granted_scope["limited_production_recommendation_rollout_output_authorized_for_chain_only"] is True
    assert granted_scope["bridge_recommendations_included"] is False
    assert granted_scope["rollout_run_performed_at_grant_time"] is False
    assert granted_scope["public_production_serving_enabled_at_grant_time"] is False
    assert granted_scope["real_production_recommendation_output_to_public_users_at_grant_time"] is False

    for section_name in ("posture", "shadow_and_production_blockers"):
        section = granted[section_name]
        assert section["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested"] is True
        assert section["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted"] is True
        assert section["prod_scoped_shadow_limited_production_recommendation_rollout_authorized"] is True
        assert section["missing_prod_scoped_shadow_limited_production_recommendation_rollout_authorization"] is False
        assert section["prod_scoped_shadow_controlled_production_recommendation_pilot_executed"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_pilot_passed"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_pilot_reviewed"] is True
        assert section["prod_scoped_shadow_controlled_production_recommendation_pilot_accepted"] is True
        assert section["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] is False
        assert section["prod_scoped_shadow_execution_authorized"] is False
        assert section["online_shadow_execution_enabled"] is False
        assert section["production_default_allowed"] is False
        assert section["api_web_changes_allowed"] is False
        assert section["user_visible_ranking_changed"] is False
        assert section["writes_performed"] is False
        assert section["runtime_writes_performed"] is False

    blockers = granted["shadow_and_production_blockers"]
    assert blockers["blockers_cleared_by_limited_production_recommendation_rollout_grant"] == [
        "missing_prod_scoped_shadow_limited_production_recommendation_rollout_authorization"
    ]
    assert blockers["blockers_introduced_by_limited_production_recommendation_rollout_grant"] == []
    assert blockers["blockers_unchanged_by_limited_production_recommendation_rollout_grant"] is True
    assert "blockers_changed_by_limited_production_recommendation_rollout_grant" not in blockers
    assert "limited_production_recommendation_rollout_run" not in granted["execution"]
    assert granted["writes_performed"] is False
    assert granted["runtime_writes_performed"] is False
    assert granted["recommended_next_stage"] == POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_NEXT_STAGE
    assert set(LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_CAVEATS).issubset(granted["caveats"])
    assert verified["verification_mode"] == "post_limited_production_recommendation_rollout_grant"


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 28, "bundle_revision"),
        ("recommended_next_stage", "wrong_next_stage", "recommended_next_stage"),
    ],
)
def test_wrong_revision_or_next_stage_rejected(
    tmp_path: Path,
    rev29_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=message):
        _grant(root, bundle_path)


def test_missing_request_rejected(tmp_path: Path, rev29_template_root: Path) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["authorization"]["prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested"] = False
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _grant(root, bundle_path)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted",
        "authorization.prod_scoped_shadow_limited_production_recommendation_rollout_authorized",
        "posture.prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted",
        "posture.prod_scoped_shadow_limited_production_recommendation_rollout_authorized",
        "shadow_and_production_blockers.prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted",
        "shadow_and_production_blockers.prod_scoped_shadow_limited_production_recommendation_rollout_authorized",
        "authorization.limited_production_recommendation_rollout_grant_decision",
        "authorization.limited_production_recommendation_rollout_granted_scope",
        "execution.limited_production_recommendation_rollout_run",
        "execution.prod_scoped_shadow_limited_production_recommendation_rollout_executed",
        "execution.prod_scoped_shadow_limited_production_recommendation_rollout_passed",
    ],
)
def test_double_grant_authorized_or_run_rejected(
    tmp_path: Path,
    rev29_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    if field_path.endswith("_decision"):
        _set_path(payload, field_path, {"decision": "granted"})
    elif field_path.endswith("_scope"):
        _set_path(payload, field_path, {"authorization_scope": LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_SCOPE})
    elif field_path.endswith("_run"):
        _set_path(payload, field_path, {"run_id": "should-not-exist"})
    else:
        _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_grant(payload)


@pytest.mark.parametrize("routes", [["/*"], ["/"], ["/api"], ["/api/recommendations/*"], ["/global-rollout"]])
def test_wildcard_or_broad_route_allowlist_rejected(
    tmp_path: Path,
    rev29_template_root: Path,
    routes: list[str],
) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="wildcard|broad|narrow"):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_grant(
            payload,
            route_path_allowlist=routes,
            owner_documents_equivalent_review="owner equivalent limited rollout grant review",
        )


def test_empty_monitoring_and_alerts_rejected(tmp_path: Path, rev29_template_root: Path) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="monitoring_and_alerts"):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_grant(
            payload,
            monitoring_and_alerts=[],
            owner_documents_equivalent_review="owner equivalent limited rollout grant review",
        )


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
        "shadow_and_production_blockers.online_shadow_execution_enabled",
        "shadow_and_production_blockers.production_default_allowed",
        "shadow_and_production_blockers.api_web_changes_allowed",
        "shadow_and_production_blockers.user_visible_ranking_changed",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
    ],
)
def test_forbidden_output_or_global_flags_rejected(
    tmp_path: Path,
    rev29_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_limited_production_recommendation_rollout_authorization_grant(
            payload,
            owner_documents_equivalent_review="owner equivalent limited rollout grant review",
        )


def test_compositional_strip_verifies_as_rev29_request(tmp_path: Path, rev29_template_root: Path) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _grant(root, bundle_path)
    payload = _load(bundle_path)

    stripped = bundle_module._without_limited_production_recommendation_rollout_grant_payload(payload)

    assert stripped["metadata"]["bundle_revision"] == 29
    assert stripped["recommended_next_stage"] == POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_REQUEST_NEXT_STAGE
    assert stripped["authorization"][
        "prod_scoped_shadow_limited_production_recommendation_rollout_authorization_requested"
    ] is True
    assert stripped["authorization"][
        "prod_scoped_shadow_limited_production_recommendation_rollout_authorization_granted"
    ] is False
    assert stripped["authorization"]["prod_scoped_shadow_limited_production_recommendation_rollout_authorized"] is False
    assert "limited_production_recommendation_rollout_grant_decision" not in stripped["authorization"]
    assert "limited_production_recommendation_rollout_granted_scope" not in stripped["authorization"]
    assert stripped["posture"][
        "missing_prod_scoped_shadow_limited_production_recommendation_rollout_authorization"
    ] is True
    for item in LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED:
        assert item not in stripped["authorization"]["explicitly_not_included"]
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_limited_production_recommendation_rollout_request_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["verification_mode"] == "post_limited_production_recommendation_rollout_request"


def test_cli_smoke_grant_then_verify_revision_thirty(tmp_path: Path, rev29_template_root: Path) -> None:
    root = _copy_template_repo(rev29_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-limited-production-recommendation-rollout",
        "--bundle",
        str(bundle_path),
        "--owner",
        "Matt Maitland",
        "--owner-documents-equivalent-review",
        "owner equivalent limited rollout grant review",
        "--grant-notes",
        "cli limited rollout grant",
        "--generated-at",
        "2026-05-31T14:00:00Z",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-limited-production-recommendation-rollout-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_limited_production_recommendation_rollout_grant",
        "online-shadow-production-scoped-v1",
        POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_limited_production_recommendation_rollout_grant_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 30:
        pytest.skip("committed production-scoped bundle is not revision 30 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_limited_production_recommendation_rollout_grant_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 30
    assert result["verification_mode"] == "post_limited_production_recommendation_rollout_grant"
    assert result["recommended_next_stage"] == POST_LIMITED_PRODUCTION_RECOMMENDATION_ROLLOUT_GRANT_NEXT_STAGE
