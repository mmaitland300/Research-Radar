"""Tests for requesting production default/API/user-visible shadow authorization."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_CAVEATS,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_FUTURE_GRANT_REQUIREMENTS,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE,
    apply_production_scoped_shadow_production_default_api_user_visible_authorization_request,
    request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    REPO_ROOT,
    _copy_fixture_repo,
    _copy_template_repo,
    _load,
    _write_json,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


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


def _prepare_rev20_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 23:
        payload = bundle_module._without_production_default_api_user_visible_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] == 22:
        payload = bundle_module._without_production_default_api_user_visible_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 21:
        payload = bundle_module._without_production_default_api_user_visible_request_payload(payload)
    if payload["metadata"]["bundle_revision"] != 20:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 20 or 21, got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE:
        raise AssertionError("expected post flag enablement pilot review next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev20_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("production-default-api-user-visible-request-template"))
    _prepare_rev20_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def test_happy_path_requests_revision_twenty_one_without_grant_or_execution(
    tmp_path: Path,
    rev20_template_root: Path,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    requested = request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Production Output Requester",
        request_notes="request only, no output",
        repo_root=root,
    )
    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_production_default_api_user_visible_request_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 20
    assert requested["metadata"]["bundle_revision"] == 21
    assert requested["plan"] == before["plan"]
    assert requested["proof"] == before["proof"]
    assert requested["execution"] == before["execution"]
    assert requested["review"] == before["review"]
    assert requested["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    authorization = requested["authorization"]
    assert authorization["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert authorization["prod_scoped_shadow_live_execution_authorized"] is True
    assert authorization["prod_scoped_shadow_execution_authorized"] is False
    assert authorization["prod_scoped_shadow_production_default_api_user_visible_authorization_requested"] is True
    assert authorization["prod_scoped_shadow_production_default_api_user_visible_authorization_granted"] is False
    assert authorization["prod_scoped_shadow_production_default_api_user_visible_authorized"] is False
    decision = authorization["production_default_api_user_visible_request_decision"]
    assert decision["decision"] == "requested"
    assert decision["requester"] == "Production Output Requester"
    assert decision["request_notes"] == "request only, no output"
    scope = authorization["production_default_api_user_visible_requested_scope"]
    assert scope["authorization_scope"] == PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_FUTURE_GRANT_REQUIREMENTS).issubset(
        scope["future_grant_would_require"]
    )
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        scope["explicitly_not_included"]
    )
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        authorization["explicitly_not_included"]
    )
    for section_name in ("posture", "shadow_and_production_blockers"):
        section = requested[section_name]
        assert section["prod_scoped_shadow_flag_enablement_authorized"] is True
        assert section["prod_scoped_shadow_live_execution_authorized"] is True
        assert section["prod_scoped_shadow_execution_authorized"] is False
        assert section["prod_scoped_shadow_production_default_api_user_visible_authorization_requested"] is True
        assert section["prod_scoped_shadow_production_default_api_user_visible_authorization_granted"] is False
        assert section["prod_scoped_shadow_production_default_api_user_visible_authorized"] is False
        assert section["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"] is True
        assert section["online_shadow_execution_enabled"] is False
        assert section["production_default_allowed"] is False
        assert section["api_web_changes_allowed"] is False
        assert section["user_visible_ranking_changed"] is False
    assert requested["shadow_and_production_blockers"][
        "blockers_introduced_by_production_default_api_user_visible_request"
    ] == ["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"]
    assert "blockers_changed_by_production_default_api_user_visible_request" not in requested[
        "shadow_and_production_blockers"
    ]
    assert requested["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE
    for caveat in PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_CAVEATS:
        assert caveat in requested["caveats"]
    assert _shadow_runs_files(root) == shadow_before
    assert verified["verification_mode"] == "post_production_default_api_user_visible_request"


def test_compositional_verify_strips_revision_twenty_one_overlay(
    tmp_path: Path,
    rev20_template_root: Path,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_production_default_api_user_visible_request_payload(payload)
    assert stripped["metadata"]["bundle_revision"] == 20
    assert stripped["recommended_next_stage"] == POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_flag_enablement_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )


def test_rejects_wrong_revision_wrong_stage_and_double_request(
    tmp_path: Path,
    rev20_template_root: Path,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 19
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )

    root = _copy_template_repo(rev20_template_root, tmp_path / "wrong-stage")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )

    root = _copy_template_repo(rev20_template_root, tmp_path / "double")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.production_default_api_user_visible_request_decision",
        "authorization.production_default_api_user_visible_requested_scope",
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
    ],
)
def test_rejects_existing_request_or_grant_payload(
    tmp_path: Path,
    rev20_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    if field_path.endswith("_decision"):
        _set_path(payload, field_path, {"decision": "requested"})
    elif field_path.endswith("_scope"):
        _set_path(payload, field_path, {"authorization_scope": PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE})
    else:
        _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_production_default_api_user_visible_authorization_request(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "review.prod_scoped_shadow_flag_enablement_pilot_accepted",
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed",
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        "authorization.prod_scoped_shadow_live_execution_authorized",
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
    ],
)
def test_rejects_missing_required_prior_chain_truth(
    tmp_path: Path,
    rev20_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    _set_path(payload, field_path, False)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=field_path.split(".")[-1]):
        apply_production_scoped_shadow_production_default_api_user_visible_authorization_request(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "posture.prod_scoped_shadow_execution_authorized",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
    ],
)
def test_rejects_accidental_global_production_api_or_plan_flag_changes(
    tmp_path: Path,
    rev20_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_production_default_api_user_visible_authorization_request(payload)


def test_request_does_not_import_runtime_database_modules_or_open_shadow_runs(
    tmp_path: Path,
    rev20_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_scoped_shadow_bundle.py"
    ).read_text(encoding="utf-8")
    start = module_source.index(
        "def apply_production_scoped_shadow_production_default_api_user_visible_authorization_request"
    )
    end = module_source.index("\ndef run_flag_enablement_pilot_ml_shadow_scorer_production_scoped_shadow_bundle", start)
    apply_source = module_source[start:end]
    for forbidden in ("psycopg", "openai", "openalex", "sklearn", "_connect_readonly"):
        assert f"import {forbidden}" not in apply_source
        assert f"from {forbidden}" not in apply_source

    root = _copy_template_repo(rev20_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        if "shadow-runs" in str(self).replace("\\", "/"):
            raise AssertionError("production default/API/user-visible request must not read shadow-runs files")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )


def test_cli_smoke_request_then_verify_revision_twenty_one(
    tmp_path: Path,
    rev20_template_root: Path,
) -> None:
    root = _copy_template_repo(rev20_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-production-default-api-user-visible",
        "--bundle",
        str(bundle_path),
        "--requester",
        "CLI Production Output Requester",
        "--request-notes",
        "cli request only",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-production-default-api-user-visible-request-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_production_default_api_user_visible_request",
        "online-shadow-production-scoped-v1",
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_production_default_api_user_visible_request_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 21:
        pytest.skip("committed production-scoped bundle is not revision 21 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_production_default_api_user_visible_request_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 21
    assert result["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE
