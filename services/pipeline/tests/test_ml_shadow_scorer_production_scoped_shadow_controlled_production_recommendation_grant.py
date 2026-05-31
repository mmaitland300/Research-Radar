"""Tests for granting controlled production recommendation authorization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY,
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_CAVEATS,
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED,
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_SCOPE,
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_STILL_NOT_INCLUDED,
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_TIME_BOUNDARIES,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE,
    apply_production_scoped_shadow_controlled_production_recommendation_authorization_grant,
    grant_controlled_production_recommendation_ml_shadow_scorer_production_scoped_shadow_bundle,
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


def _prepare_rev25_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 26:
        payload = bundle_module._without_controlled_production_recommendation_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] != 25:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 25 after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE:
        raise AssertionError("expected post controlled production recommendation request next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev25_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("controlled-production-recommendation-grant-template"))
    _prepare_rev25_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _grant(root: Path, bundle_path: Path, **kwargs: Any) -> dict[str, Any]:
    grant_kwargs = {
        "owner_documents_equivalent_review": "owner equivalent controlled production recommendation grant review",
        "generated_at": "2026-05-31T02:00:00Z",
        **kwargs,
    }
    return grant_controlled_production_recommendation_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        **grant_kwargs,
    )


def test_happy_path_rev25_to_rev26_grant(tmp_path: Path, rev25_template_root: Path) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    granted = _grant(
        root,
        bundle_path,
        owner="Controlled Grant Owner",
        second_reviewer="Controlled Grant Reviewer",
        grant_notes="grant only, no output",
    )
    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_controlled_production_recommendation_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 25
    assert granted["metadata"]["bundle_revision"] == 26
    assert granted["plan"] == before["plan"]
    assert granted["proof"] == before["proof"]
    assert granted["execution"] == before["execution"]
    assert granted["review"] == before["review"]
    assert granted["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert (
        granted["authorization"]["controlled_production_recommendation_request_decision"]
        == before["authorization"]["controlled_production_recommendation_request_decision"]
    )
    assert (
        granted["authorization"]["controlled_production_recommendation_requested_scope"]
        == before["authorization"]["controlled_production_recommendation_requested_scope"]
    )

    authorization = granted["authorization"]
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorization_requested"] is True
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorization_granted"] is True
    assert authorization["prod_scoped_shadow_controlled_production_recommendation_authorized"] is True
    assert authorization["prod_scoped_shadow_execution_authorized"] is False
    grant_decision = authorization["controlled_production_recommendation_grant_decision"]
    assert grant_decision["decision"] == "granted"
    assert grant_decision["owner"] == "Controlled Grant Owner"
    assert grant_decision["second_reviewer"] == "Controlled Grant Reviewer"
    assert grant_decision["grant_notes"] == "grant only, no output"
    granted_scope = authorization["controlled_production_recommendation_granted_scope"]
    assert granted_scope["authorization_scope"] == CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_SCOPE
    assert granted_scope["authorizes_for_chain_only"] == list(
        CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY
    )
    assert granted_scope["explicitly_still_not_included"] == list(
        CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_STILL_NOT_INCLUDED
    )
    assert granted_scope["grant_time_controlled_production_recommendation_boundaries"] == list(
        CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_TIME_BOUNDARIES
    )
    assert granted_scope["controlled_production_recommendation_output_authorized_for_chain_only"] is True
    assert granted_scope["real_recommendations_emitted_at_grant_time"] is False
    assert granted_scope["bridge_recommendations_included"] is False

    for section_name in ("posture", "shadow_and_production_blockers"):
        section = granted[section_name]
        assert section["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] is False
        assert section["prod_scoped_shadow_controlled_production_recommendation_authorized"] is True
        assert section["prod_scoped_shadow_execution_authorized"] is False
        assert section["online_shadow_execution_enabled"] is False
        assert section["production_default_allowed"] is False
        assert section["api_web_changes_allowed"] is False
        assert section["user_visible_ranking_changed"] is False
        assert section["writes_performed"] is False
        assert section["runtime_writes_performed"] is False

    blockers = granted["shadow_and_production_blockers"]
    assert blockers["blockers_cleared_by_controlled_production_recommendation_grant"] == [
        "missing_prod_scoped_shadow_controlled_production_recommendation_authorization"
    ]
    assert blockers["blockers_introduced_by_controlled_production_recommendation_grant"] == []
    assert blockers["blockers_unchanged_by_controlled_production_recommendation_grant"] is True
    assert "blockers_changed_by_controlled_production_recommendation_grant" not in blockers
    assert "controlled_production_recommendation_pilot_run" not in granted["execution"]
    assert granted["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE
    assert set(CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_CAVEATS).issubset(granted["caveats"])
    assert verified["verification_mode"] == "post_controlled_production_recommendation_grant"


def test_compositional_strip_verifies_as_rev25(tmp_path: Path, rev25_template_root: Path) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _grant(root, bundle_path)
    payload = _load(bundle_path)

    stripped = bundle_module._without_controlled_production_recommendation_grant_payload(payload)

    assert stripped["metadata"]["bundle_revision"] == 25
    assert stripped["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_REQUEST_NEXT_STAGE
    assert "controlled_production_recommendation_grant_decision" not in stripped["authorization"]
    assert "controlled_production_recommendation_granted_scope" not in stripped["authorization"]
    assert stripped["posture"]["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] is True
    for item in CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED:
        assert item not in stripped["authorization"]["explicitly_not_included"]
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_controlled_production_recommendation_request_filed=True,
        verify_local_pilot_files=False,
    )


def test_wrong_revision_wrong_stage_and_double_grant_rejected(
    tmp_path: Path,
    rev25_template_root: Path,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 24
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        _grant(root, bundle_path)

    root = _copy_template_repo(rev25_template_root, tmp_path / "wrong-stage")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        _grant(root, bundle_path)

    root = _copy_template_repo(rev25_template_root, tmp_path / "double")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _grant(root, bundle_path)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        _grant(root, bundle_path)


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        (
            "authorization.prod_scoped_shadow_controlled_production_recommendation_authorization_requested",
            False,
        ),
        ("authorization.controlled_production_recommendation_request_decision.decision", "rejected"),
        ("authorization.controlled_production_recommendation_requested_scope.authorization_scope", "wrong_scope"),
    ],
)
def test_grant_without_valid_prior_request_fails_closed(
    tmp_path: Path,
    rev25_template_root: Path,
    field_path: str,
    value: Any,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _grant(root, bundle_path)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorization_granted",
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorized",
    ],
)
def test_grant_or_authorized_true_at_wrong_input_state_rejected(
    tmp_path: Path,
    rev25_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _grant(root, bundle_path)


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
def test_forbidden_output_or_global_flags_true_rejected(
    tmp_path: Path,
    rev25_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _grant(root, bundle_path)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_posture_blocker",
        "missing_shadow_blocker",
        "missing_request_introduced_delta",
        "missing_request_cleared_delta",
        "missing_request_unchanged_delta",
    ],
)
def test_missing_request_blocker_or_delta_rejected(
    tmp_path: Path,
    rev25_template_root: Path,
    mutation: str,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    blockers = payload["shadow_and_production_blockers"]
    if mutation == "missing_posture_blocker":
        payload["posture"]["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] = False
    elif mutation == "missing_shadow_blocker":
        blockers["missing_prod_scoped_shadow_controlled_production_recommendation_authorization"] = False
    elif mutation == "missing_request_introduced_delta":
        blockers.pop("blockers_introduced_by_controlled_production_recommendation_request")
    elif mutation == "missing_request_cleared_delta":
        blockers.pop("blockers_cleared_by_controlled_production_recommendation_request")
    else:
        blockers.pop("blockers_unchanged_by_controlled_production_recommendation_request")
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _grant(root, bundle_path)


def test_grant_section_verifier_validates_decision_scope_boundaries_and_exclusions(
    tmp_path: Path,
    rev25_template_root: Path,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    updated = apply_production_scoped_shadow_controlled_production_recommendation_authorization_grant(
        payload,
        owner_documents_equivalent_review="owner equivalent controlled production recommendation grant review",
        generated_at="2026-05-31T02:00:00Z",
    )
    bundle_module._verify_controlled_production_recommendation_grant_section(updated["authorization"])

    bad = _load(bundle_path)
    bad = apply_production_scoped_shadow_controlled_production_recommendation_authorization_grant(
        bad,
        owner_documents_equivalent_review="owner equivalent controlled production recommendation grant review",
        generated_at="2026-05-31T02:00:00Z",
    )
    bad["authorization"]["controlled_production_recommendation_granted_scope"][
        "grant_time_controlled_production_recommendation_boundaries"
    ].remove("no real production recommendation output emitted at grant time")
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="no real production"):
        bundle_module._verify_controlled_production_recommendation_grant_section(bad["authorization"])


def test_rejects_controlled_production_recommendation_pilot_run_at_grant_time(
    tmp_path: Path,
    rev25_template_root: Path,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["execution"]["controlled_production_recommendation_pilot_run"] = {"pilot_run_id": "should-not-exist"}
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="controlled_production_recommendation"):
        _grant(root, bundle_path)


def test_cli_smoke_grant_then_verify_revision_twenty_six(
    tmp_path: Path,
    rev25_template_root: Path,
) -> None:
    root = _copy_template_repo(rev25_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-controlled-production-recommendation",
        "--bundle",
        str(bundle_path),
        "--owner",
        "Matt Maitland",
        "--owner-documents-equivalent-review",
        "owner equivalent controlled production recommendation grant review",
        "--grant-notes",
        "cli controlled production recommendation grant",
        "--generated-at",
        "2026-05-31T02:00:00Z",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-controlled-production-recommendation-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_controlled_production_recommendation_grant",
        "online-shadow-production-scoped-v1",
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_controlled_production_recommendation_grant_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_controlled_production_recommendation_grant_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 26
    assert result["verification_mode"] == "post_controlled_production_recommendation_grant"
