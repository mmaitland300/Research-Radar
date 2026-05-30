"""Tests for granting production-scoped online shadow flag enablement authorization."""

from __future__ import annotations

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
    FLAG_ENABLEMENT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY,
    FLAG_ENABLEMENT_GRANT_CAVEATS,
    FLAG_ENABLEMENT_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED,
    FLAG_ENABLEMENT_GRANT_SCOPE,
    FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED,
    FLAG_ENABLEMENT_GRANT_TIME_BOUNDARIES,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE,
    POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE,
    apply_production_scoped_shadow_flag_enablement_authorization_grant,
    grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    _copy_fixture_repo,
    _copy_template_repo,
    _load,
    _prepare_rev17_template_bundle,
    _write_json,
    rev17_template_root,
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


def _apply_grant(payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    grant_kwargs = {
        "owner_documents_equivalent_review": "owner equivalent flag enablement review",
        **kwargs,
    }
    return apply_production_scoped_shadow_flag_enablement_authorization_grant(payload, **grant_kwargs)


def test_happy_path_grant_from_revision_seventeen_to_eighteen(
    tmp_path: Path,
    rev17_template_root: Path,
) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    granted = grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner="Flag Enablement Grant Owner",
        second_reviewer="Flag Enablement Grant Reviewer",
        grant_notes="flag enablement grant notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_flag_enablement_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 17
    assert granted["metadata"]["bundle_revision"] == 18
    assert granted["plan"] == before["plan"]
    assert granted["proof"] == before["proof"]
    assert granted["execution"] == before["execution"]
    assert granted["review"] == before["review"]
    assert granted["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert granted["authorization"]["flag_enablement_request_decision"] == before["authorization"][
        "flag_enablement_request_decision"
    ]
    assert granted["authorization"]["flag_enablement_requested_scope"] == before["authorization"][
        "flag_enablement_requested_scope"
    ]
    assert granted["authorization"]["prod_scoped_shadow_flag_enablement_authorization_requested"] is True
    assert granted["authorization"]["prod_scoped_shadow_flag_enablement_authorization_granted"] is True
    assert granted["authorization"]["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    grant_decision = granted["authorization"]["flag_enablement_grant_decision"]
    assert grant_decision["decision"] == "granted"
    assert grant_decision["owner"] == "Flag Enablement Grant Owner"
    assert grant_decision["second_reviewer"] == "Flag Enablement Grant Reviewer"
    assert grant_decision["grant_notes"] == "flag enablement grant notes"
    granted_scope = granted["authorization"]["flag_enablement_granted_scope"]
    assert granted_scope["authorization_scope"] == FLAG_ENABLEMENT_GRANT_SCOPE
    assert granted_scope["runtime_feature_flag"] == FEATURE_FLAG
    assert set(FLAG_ENABLEMENT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY).issubset(granted_scope["authorizes_for_chain_only"])
    assert set(FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED).issubset(granted_scope["explicitly_still_not_included"])
    assert set(FLAG_ENABLEMENT_GRANT_TIME_BOUNDARIES).issubset(granted_scope["grant_time_flag_enablement_boundaries"])
    assert granted["posture"]["missing_prod_scoped_shadow_flag_enablement_authorization"] is False
    assert granted["posture"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert granted["posture"]["online_shadow_execution_enabled"] is False
    assert (
        granted["plan"]["feature_flag_iam_config_requirements"]["prod_scoped_flag_enablement_authorized_now"] is False
    )
    assert granted["shadow_and_production_blockers"]["blockers_cleared_by_flag_enablement_grant"] == [
        "missing_prod_scoped_shadow_flag_enablement_authorization"
    ]
    assert granted["shadow_and_production_blockers"]["blockers_introduced_by_flag_enablement_grant"] == []
    assert granted["shadow_and_production_blockers"]["blockers_unchanged_by_flag_enablement_grant"] is True
    assert "blockers_changed_by_flag_enablement_grant" not in granted["shadow_and_production_blockers"]
    assert granted["recommended_next_stage"] == POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE
    assert set(FLAG_ENABLEMENT_GRANT_CAVEATS).issubset(granted["caveats"])
    assert "flag_enablement_pilot_run" not in granted.get("execution", {})
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_flag_enablement_grant"
    assert granted["authorization"]["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert granted["posture"]["online_shadow_execution_enabled"] is False


def test_compositional_verify_strips_rev_eighteen_overlay(
    tmp_path: Path,
    rev17_template_root: Path,
) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent flag enablement review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_flag_enablement_grant_payload(payload)
    assert stripped["metadata"]["bundle_revision"] == 17
    assert stripped["recommended_next_stage"] == POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE
    for item in FLAG_ENABLEMENT_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED:
        assert item not in stripped["authorization"]["explicitly_not_included"]
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=False,
    )


def test_wrong_bundle_revision_rejection(tmp_path: Path, rev17_template_root: Path) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 16
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent flag enablement review",
            repo_root=root,
        )


def test_wrong_recommended_next_stage_rejection(tmp_path: Path, rev17_template_root: Path) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent flag enablement review",
            repo_root=root,
        )


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        ("authorization.prod_scoped_shadow_flag_enablement_authorization_granted", True),
        ("authorization.prod_scoped_shadow_flag_enablement_authorized", True),
    ],
)
def test_double_grant_rejection(
    tmp_path: Path,
    rev17_template_root: Path,
    field_path: str,
    value: bool,
) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent flag enablement review",
            repo_root=root,
        )


def test_rejects_pre_existing_flag_enablement_grant_slices(
    tmp_path: Path,
    rev17_template_root: Path,
) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["authorization"]["flag_enablement_grant_decision"] = {"decision": "granted", "owner": "stale"}
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="flag_enablement_grant_decision"):
        _apply_grant(_load(bundle_path))

    payload = _load(bundle_path)
    payload["authorization"].pop("flag_enablement_grant_decision", None)
    payload["authorization"]["flag_enablement_granted_scope"] = {"authorization_scope": "stale"}
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="flag_enablement_granted_scope"):
        _apply_grant(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "posture.online_shadow_execution_enabled",
        "authorization.prod_scoped_shadow_execution_authorized",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
    ],
)
def test_rejects_accidental_runtime_or_global_enablement_flags(
    tmp_path: Path,
    rev17_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _apply_grant(payload)


def test_rejects_execution_flag_enablement_pilot_run_slice(
    tmp_path: Path,
    rev17_template_root: Path,
) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["execution"]["flag_enablement_pilot_run"] = {"pilot_surface": "forbidden"}

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="flag_enablement_pilot_run"):
        _apply_grant(payload)


def test_cli_smoke(tmp_path: Path, rev17_template_root: Path) -> None:
    root = _copy_template_repo(rev17_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]

    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-flag-enablement",
        "--bundle",
        str(bundle_path),
        "--owner",
        "Matt Maitland",
        "--owner-documents-equivalent-review",
        "owner equivalent flag enablement review",
        "--grant-notes",
        "cli flag enablement grant",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-flag-enablement-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_flag_enablement_grant",
        "online-shadow-production-scoped-v1",
        POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_flag_enablement_grant_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 18:
        pytest.skip("committed production-scoped bundle is not revision 18 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_flag_enablement_grant_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 18
    assert result["recommended_next_stage"] == POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE
    for caveat in FLAG_ENABLEMENT_GRANT_CAVEATS:
        assert caveat in payload["caveats"]
