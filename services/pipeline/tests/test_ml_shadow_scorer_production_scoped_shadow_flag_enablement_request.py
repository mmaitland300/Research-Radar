"""Tests for requesting production-scoped online shadow flag enablement authorization."""

from __future__ import annotations

import builtins
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FEATURE_FLAG,
    FLAG_ENABLEMENT_REQUEST_CAVEATS,
    FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED,
    FLAG_ENABLEMENT_REQUEST_FUTURE_GRANT_REQUIREMENTS,
    FLAG_ENABLEMENT_REQUEST_SCOPE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE,
    POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    apply_production_scoped_shadow_flag_enablement_authorization_request,
    request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    _copy_fixture_repo,
    _copy_template_repo,
    _load,
    _prepare_rev16_template_bundle,
    _write_json,
    rev16_template_root,
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


def test_happy_path_request_from_revision_sixteen_to_seventeen(
    tmp_path: Path,
    rev16_template_root: Path,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    requested = request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Flag Enablement Requester",
        request_notes="flag enablement request notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 16
    assert before["authorization"].get("prod_scoped_shadow_flag_enablement_authorization_requested") is not True
    assert requested["metadata"]["bundle_revision"] == 17
    assert requested["plan"] == before["plan"]
    assert requested["proof"] == before["proof"]
    assert requested["execution"] == before["execution"]
    assert requested["review"] == before["review"]
    assert requested["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert requested["authorization"]["grant_decision"] == before["authorization"]["grant_decision"]
    assert requested["authorization"]["granted_scope"] == before["authorization"]["granted_scope"]
    assert requested["authorization"]["request_decision"] == before["authorization"]["request_decision"]
    assert requested["authorization"]["requested_scope"] == before["authorization"]["requested_scope"]
    assert requested["authorization"]["live_read_only_grant_decision"] == before["authorization"][
        "live_read_only_grant_decision"
    ]
    assert requested["authorization"]["live_read_only_granted_scope"] == before["authorization"][
        "live_read_only_granted_scope"
    ]
    assert requested["authorization"]["live_execution_request_decision"] == before["authorization"][
        "live_execution_request_decision"
    ]
    assert requested["authorization"]["live_execution_requested_scope"] == before["authorization"][
        "live_execution_requested_scope"
    ]
    assert requested["authorization"]["live_execution_grant_decision"] == before["authorization"][
        "live_execution_grant_decision"
    ]
    assert requested["authorization"]["live_execution_granted_scope"] == before["authorization"][
        "live_execution_granted_scope"
    ]
    assert requested["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert requested["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert requested["authorization"]["prod_scoped_shadow_flag_enablement_authorization_requested"] is True
    assert requested["authorization"]["prod_scoped_shadow_flag_enablement_authorization_granted"] is False
    assert requested["authorization"]["prod_scoped_shadow_flag_enablement_authorized"] is False
    decision = requested["authorization"]["flag_enablement_request_decision"]
    assert decision["decision"] == "requested"
    assert decision["requester"] == "Flag Enablement Requester"
    assert decision["request_notes"] == "flag enablement request notes"
    scope = requested["authorization"]["flag_enablement_requested_scope"]
    assert scope["authorization_scope"] == FLAG_ENABLEMENT_REQUEST_SCOPE
    assert scope["runtime_feature_flag"] == FEATURE_FLAG
    assert set(FLAG_ENABLEMENT_REQUEST_FUTURE_GRANT_REQUIREMENTS).issubset(scope["future_grant_would_require"])
    assert set(FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(scope["explicitly_not_included"])
    assert set(FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        requested["authorization"]["explicitly_not_included"]
    )
    assert requested["posture"]["online_shadow_execution_enabled"] is False
    assert requested["posture"]["missing_prod_scoped_shadow_flag_enablement_authorization"] is True
    assert requested["posture"]["missing_prod_scoped_shadow_live_execution_authorization"] is False
    assert (
        requested["plan"]["feature_flag_iam_config_requirements"]["prod_scoped_flag_enablement_authorized_now"]
        is False
    )
    assert requested["shadow_and_production_blockers"]["blockers_introduced_by_flag_enablement_request"] == [
        "missing_prod_scoped_shadow_flag_enablement_authorization"
    ]
    assert requested["shadow_and_production_blockers"]["blockers_cleared_by_flag_enablement_request"] == []
    assert requested["shadow_and_production_blockers"]["blockers_unchanged_by_flag_enablement_request"] is True
    assert "blockers_changed_by_flag_enablement_request" not in requested["shadow_and_production_blockers"]
    assert requested["recommended_next_stage"] == POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE
    assert "Bundle flag-enablement request milestone only; grants no flag enablement authorization." in requested[
        "caveats"
    ]
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_flag_enablement_request"


def test_compositional_verify_strips_rev_seventeen_overlay(
    tmp_path: Path,
    rev16_template_root: Path,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_flag_enablement_request_payload(payload)
    assert stripped["metadata"]["bundle_revision"] == 16
    assert stripped["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=False,
    )


def test_wrong_bundle_revision_rejection(tmp_path: Path, rev16_template_root: Path) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 15
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_wrong_recommended_next_stage_rejection(tmp_path: Path, rev16_template_root: Path) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_double_request_rejection(tmp_path: Path, rev16_template_root: Path) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_grant_already_filed_rejection(tmp_path: Path, rev16_template_root: Path) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["authorization"]["prod_scoped_shadow_flag_enablement_authorization_granted"] = True
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="grant must not already be filed"):
        apply_production_scoped_shadow_flag_enablement_authorization_request(payload)


@pytest.mark.parametrize(
    ("authorization_field", "mutation", "error_match"),
    [
        ("request_decision", "pop", "request_decision"),
        ("requested_scope", "pop", "requested_scope"),
        ("live_read_only_grant_decision", "pop", "live_read_only_grant_decision"),
        ("live_read_only_granted_scope", "pop", "live_read_only_granted_scope"),
        ("live_execution_request_decision", "pop", "live_execution_request_decision"),
        ("live_execution_requested_scope", "pop", "live_execution_requested_scope"),
        ("live_execution_grant_decision", "pop", "live_execution_grant_decision"),
        ("live_execution_granted_scope", "pop", "live_execution_granted_scope"),
        (
            "prod_scoped_shadow_live_read_only_authorization_requested",
            "false",
            "prod_scoped_shadow_live_read_only_authorization_requested",
        ),
        (
            "prod_scoped_shadow_live_read_only_authorization_granted",
            "false",
            "prod_scoped_shadow_live_read_only_authorization_granted",
        ),
    ],
)
def test_apply_rejects_missing_live_read_only_or_live_execution_slices(
    tmp_path: Path,
    rev16_template_root: Path,
    authorization_field: str,
    mutation: str,
    error_match: str,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    if mutation == "pop":
        payload["authorization"].pop(authorization_field, None)
    else:
        payload["authorization"][authorization_field] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=error_match):
        apply_production_scoped_shadow_flag_enablement_authorization_request(payload)


def test_apply_rejects_missing_live_execution_pilot_review_section(
    tmp_path: Path,
    rev16_template_root: Path,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["review"].pop("live_execution_pilot_review_decision", None)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_execution_pilot_review_decision"):
        apply_production_scoped_shadow_flag_enablement_authorization_request(payload)


def test_apply_rejects_missing_live_execution_pilot_run(
    tmp_path: Path,
    rev16_template_root: Path,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["execution"].pop("live_execution_pilot_run", None)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_execution_pilot_run"):
        apply_production_scoped_shadow_flag_enablement_authorization_request(payload)


def test_apply_rejects_failed_live_execution_pilot_review(
    tmp_path: Path,
    rev16_template_root: Path,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["review"]["prod_scoped_shadow_live_execution_pilot_accepted"] = False
    payload["review"]["live_execution_pilot_review_decision"]["decision"] = "not_accepted"
    payload["review"]["live_execution_pilot_review_decision"]["failed_review_checks"] = ["runtime_row_count_528"]
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="prod_scoped_shadow_live_execution_pilot_accepted|decision|failed_review_checks",
    ):
        apply_production_scoped_shadow_flag_enablement_authorization_request(_load(bundle_path))


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        "posture.online_shadow_execution_enabled",
    ],
)
def test_apply_rejects_pre_flagged_grant_authorized_or_online_shadow_enabled(
    tmp_path: Path,
    rev16_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_flag_enablement_authorization_request(payload)


def test_apply_preserves_non_regression_authorization_and_plan_fields(
    tmp_path: Path,
    rev16_template_root: Path,
) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    updated = apply_production_scoped_shadow_flag_enablement_authorization_request(payload)

    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert (
        updated["plan"]["feature_flag_iam_config_requirements"]["prod_scoped_flag_enablement_authorized_now"] is False
    )


def test_request_does_not_import_runtime_database_modules_or_open_shadow_runs(
    tmp_path: Path,
    rev16_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_scoped_shadow_bundle.py"
    ).read_text(encoding="utf-8")
    start = module_source.index("def apply_production_scoped_shadow_flag_enablement_authorization_request")
    end = module_source.index("\ndef _infer_plan_mode", start)
    apply_source = module_source[start:end]
    for forbidden in ("psycopg", "openai", "openalex", "sklearn", "_connect_readonly"):
        assert f"import {forbidden}" not in apply_source
        assert f"from {forbidden}" not in apply_source

    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        if "shadow-runs" in str(self).replace("\\", "/"):
            raise AssertionError("flag enablement request must not read shadow-runs files")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )


def test_cli_smoke(tmp_path: Path, rev16_template_root: Path) -> None:
    root = _copy_template_repo(rev16_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-flag-enablement",
        "--bundle",
        str(bundle_path),
        "--requester",
        "Matt Maitland",
        "--request-notes",
        "cli flag enablement request",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-flag-enablement-request-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_flag_enablement_request",
        "online-shadow-production-scoped-v1",
        POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_flag_enablement_request_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 17:
        pytest.skip("committed production-scoped bundle is not revision 17 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 17
    assert result["recommended_next_stage"] == POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE
    for caveat in FLAG_ENABLEMENT_REQUEST_CAVEATS:
        assert caveat in payload["caveats"]
