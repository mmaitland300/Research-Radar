"""Tests for granting production-scoped live execution shadow authorization."""

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
    LIVE_EXECUTION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY,
    LIVE_EXECUTION_GRANT_CAVEATS,
    LIVE_EXECUTION_GRANT_SCOPE,
    LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED,
    LIVE_EXECUTION_GRANT_TIME_BOUNDARIES,
    LIVE_EXECUTION_REQUEST_CAVEATS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_EXECUTION_GRANT_NEXT_STAGE,
    POST_LIVE_EXECUTION_REQUEST_NEXT_STAGE,
    POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    apply_production_scoped_shadow_live_execution_authorization_grant,
    grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "production_scoped_bundle": "docs/audit/bundles/production-scoped-shadow-v1/bundle.json",
    "production_scoped_bundle_md": "docs/audit/bundles/production-scoped-shadow-v1/bundle.md",
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
    "learned_probability": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
    "offline_audit_embedding_scorer": "docs/audit/ml-offline-audit-embedding-scorer-v2.json",
    "generalization_audit": "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _copy_template_repo(template_root: Path, tmp_path: Path) -> Path:
    root = tmp_path / "case-repo"
    shutil.copytree(template_root, root)
    return root


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def _downgrade_to_rev12(payload: dict[str, Any]) -> dict[str, Any]:
    downgraded = json.loads(json.dumps(payload))
    downgraded["metadata"]["bundle_revision"] = 12
    downgraded["recommended_next_stage"] = POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    downgraded["caveats"] = bundle_module._caveats(mode="post_live_read_only_pilot_review")
    authorization = downgraded["authorization"]
    authorization.pop("prod_scoped_shadow_live_execution_authorization_requested", None)
    authorization.pop("prod_scoped_shadow_live_execution_authorization_granted", None)
    authorization["prod_scoped_shadow_live_execution_authorized"] = False
    authorization.pop("live_execution_request_decision", None)
    authorization.pop("live_execution_requested_scope", None)
    authorization.pop("live_execution_grant_decision", None)
    authorization.pop("live_execution_granted_scope", None)
    posture = downgraded["posture"]
    posture.pop("prod_scoped_shadow_live_execution_authorization_requested", None)
    posture.pop("prod_scoped_shadow_live_execution_authorization_granted", None)
    posture.pop("prod_scoped_shadow_live_execution_authorized", None)
    posture.pop("missing_prod_scoped_shadow_live_execution_authorization", None)
    blockers = downgraded["shadow_and_production_blockers"]
    blockers.pop("prod_scoped_shadow_live_execution_authorization_requested", None)
    blockers.pop("prod_scoped_shadow_live_execution_authorization_granted", None)
    blockers.pop("prod_scoped_shadow_live_execution_authorized", None)
    blockers.pop("missing_prod_scoped_shadow_live_execution_authorization", None)
    blockers.pop("blockers_cleared_by_live_execution_request", None)
    blockers.pop("blockers_introduced_by_live_execution_request", None)
    blockers.pop("blockers_unchanged_by_live_execution_request", None)
    blockers.pop("blockers_changed_by_live_execution_request", None)
    blockers.pop("blockers_cleared_by_live_execution_grant", None)
    blockers.pop("blockers_introduced_by_live_execution_grant", None)
    blockers.pop("blockers_unchanged_by_live_execution_grant", None)
    blockers.pop("blockers_changed_by_live_execution_grant", None)
    return downgraded


def _prepare_rev12_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    revision = payload["metadata"]["bundle_revision"]
    if revision == 20:
        payload = bundle_module._without_flag_enablement_pilot_review_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 19:
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 20:
        payload = bundle_module._without_flag_enablement_pilot_review_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 19:
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 17:
        payload = bundle_module._without_flag_enablement_request_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 16:
        payload = bundle_module._without_live_execution_pilot_review_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 15:
        payload = bundle_module._without_live_execution_pilot_run_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 14:
        payload = bundle_module._without_live_execution_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 13:
        payload = _downgrade_to_rev12(payload)
    elif payload["metadata"]["bundle_revision"] != 12:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 12 through 19, got {revision}"
        )
    _write_json(bundle_path, payload)


def _ensure_rev13_bundle(root: Path) -> Path:
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    revision = payload["metadata"]["bundle_revision"]
    if revision == 20:
        payload = bundle_module._without_flag_enablement_pilot_review_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 19:
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 17:
        payload = bundle_module._without_flag_enablement_request_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 16:
        payload = bundle_module._without_live_execution_pilot_review_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 15:
        payload = bundle_module._without_live_execution_pilot_run_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 14:
        payload = bundle_module._without_live_execution_grant_payload(payload)
        _write_json(bundle_path, payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 13:
        return bundle_path
    if revision == 12:
        request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )
        return bundle_path
    raise AssertionError(f"expected production-scoped bundle revision 12 through 19, got {revision}")


def _apply_grant(payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    grant_kwargs = {
        "owner_documents_equivalent_review": "owner equivalent live execution review",
        **kwargs,
    }
    return apply_production_scoped_shadow_live_execution_authorization_grant(payload, **grant_kwargs)


@pytest.fixture(scope="module")
def live_execution_request_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("live-execution-grant-template"))
    _prepare_rev12_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def test_happy_path_grant_from_revision_thirteen_to_revision_fourteen(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    granted = grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner="Live Execution Grant Owner",
        second_reviewer="Live Execution Grant Reviewer",
        grant_notes="live execution grant notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 13
    assert granted["metadata"]["bundle_revision"] == 14
    assert granted["plan"] == before["plan"]
    assert granted["proof"] == before["proof"]
    assert granted["execution"] == before["execution"]
    assert granted["review"] == before["review"]
    assert granted["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert granted["authorization"]["grant_decision"] == before["authorization"]["grant_decision"]
    assert granted["authorization"]["granted_scope"] == before["authorization"]["granted_scope"]
    assert granted["authorization"]["request_decision"] == before["authorization"]["request_decision"]
    assert granted["authorization"]["requested_scope"] == before["authorization"]["requested_scope"]
    assert granted["authorization"]["live_read_only_grant_decision"] == before["authorization"][
        "live_read_only_grant_decision"
    ]
    assert granted["authorization"]["live_read_only_granted_scope"] == before["authorization"][
        "live_read_only_granted_scope"
    ]
    assert granted["authorization"]["live_execution_request_decision"] == before["authorization"][
        "live_execution_request_decision"
    ]
    assert granted["authorization"]["live_execution_requested_scope"] == before["authorization"][
        "live_execution_requested_scope"
    ]
    assert granted["authorization"]["prod_scoped_shadow_live_execution_authorization_requested"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_execution_authorization_granted"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert granted["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert granted["authorization"]["live_execution_grant_decision"]["decision"] == "granted"
    assert granted["authorization"]["live_execution_grant_decision"]["owner"] == "Live Execution Grant Owner"
    assert granted["authorization"]["live_execution_grant_decision"]["second_reviewer"] == "Live Execution Grant Reviewer"
    assert granted["authorization"]["live_execution_grant_decision"]["grant_notes"] == "live execution grant notes"
    assert (
        granted["authorization"]["live_execution_granted_scope"]["authorization_scope"]
        == LIVE_EXECUTION_GRANT_SCOPE
    )
    assert set(LIVE_EXECUTION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY).issubset(
        granted["authorization"]["live_execution_granted_scope"]["authorizes_for_chain_only"]
    )
    assert set(LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED).issubset(
        granted["authorization"]["live_execution_granted_scope"]["explicitly_still_not_included"]
    )
    assert set(LIVE_EXECUTION_GRANT_TIME_BOUNDARIES).issubset(
        granted["authorization"]["live_execution_granted_scope"]["grant_time_live_execution_boundaries"]
    )
    assert granted["posture"]["missing_prod_scoped_shadow_live_execution_authorization"] is False
    assert granted["posture"]["live_prod_source_reads_performed"] is True
    assert granted["posture"]["online_shadow_execution_enabled"] is False
    assert granted["posture"]["production_default_allowed"] is False
    assert granted["posture"]["api_web_changes_allowed"] is False
    assert granted["posture"]["user_visible_ranking_changed"] is False
    assert granted["writes_performed"] is False
    assert granted["runtime_writes_performed"] is False
    assert granted["shadow_and_production_blockers"]["blockers_cleared_by_live_execution_grant"] == [
        "missing_prod_scoped_shadow_live_execution_authorization"
    ]
    assert granted["shadow_and_production_blockers"]["blockers_introduced_by_live_execution_grant"] == []
    assert granted["shadow_and_production_blockers"]["blockers_unchanged_by_live_execution_grant"] is True
    assert "blockers_changed_by_live_execution_grant" not in granted["shadow_and_production_blockers"]
    assert granted["recommended_next_stage"] == POST_LIVE_EXECUTION_GRANT_NEXT_STAGE
    assert set(LIVE_EXECUTION_GRANT_CAVEATS).issubset(granted["caveats"])
    assert "live_execution_pilot_run" not in granted.get("execution", {})
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_live_execution_grant"


def test_committed_bundle_matches_post_live_execution_grant() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] == 20:
        payload = bundle_module._without_flag_enablement_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 19:
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] == 18:
        payload = bundle_module._without_flag_enablement_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 17:
        payload = bundle_module._without_flag_enablement_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 16:
        payload = bundle_module._without_live_execution_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 15:
        payload = bundle_module._without_live_execution_pilot_run_payload(payload)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=REPO_ROOT,
        expect_live_execution_grant_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 14
    assert result["recommended_next_stage"] == POST_LIVE_EXECUTION_GRANT_NEXT_STAGE


def test_compositional_verify_strips_rev_fourteen_overlay(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_live_execution_grant_payload(payload)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_live_execution_request_filed=True,
        verify_local_pilot_files=False,
    )


def test_wrong_bundle_revision_rejection(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 12
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent live execution review",
            repo_root=root,
        )


def test_wrong_recommended_next_stage_rejection(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent live execution review",
            repo_root=root,
        )


def test_rejects_missing_live_execution_request(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="bundle_revision",
    ):
        _apply_grant(payload)


def test_double_grant_rejection(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent live execution review",
            repo_root=root,
        )


@pytest.mark.parametrize(
    ("authorization_field", "mutation", "error_match"),
    [
        ("live_execution_request_decision", "pop", "live_execution_request_decision"),
        ("live_execution_requested_scope", "pop", "live_execution_requested_scope"),
        ("request_decision", "pop", "request_decision"),
        ("requested_scope", "pop", "requested_scope"),
        ("live_read_only_grant_decision", "pop", "live_read_only_grant_decision"),
        ("live_read_only_granted_scope", "pop", "live_read_only_granted_scope"),
    ],
)
def test_apply_rejects_missing_or_invalid_live_execution_or_read_only_slices(
    tmp_path: Path,
    live_execution_request_template_root: Path,
    authorization_field: str,
    mutation: str,
    error_match: str,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    if mutation == "pop":
        payload["authorization"].pop(authorization_field, None)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=error_match):
        _apply_grant(payload)


def test_rejects_missing_accepted_live_read_only_pilot_review(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    payload["review"]["prod_scoped_shadow_live_read_only_pilot_accepted"] = False
    payload["review"]["live_read_only_pilot_review_decision"]["decision"] = "not_accepted"
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="prod_scoped_shadow_live_read_only_pilot_accepted",
    ):
        _apply_grant(_load(bundle_path))


def test_rejects_non_empty_failed_review_checks(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    payload["review"]["live_read_only_pilot_review_decision"]["failed_review_checks"] = [
        "live_prod_source_reads_true"
    ]
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="failed_review_checks"):
        _apply_grant(_load(bundle_path))


def test_rejects_posture_missing_live_execution_authorization_not_true(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    payload["posture"]["missing_prod_scoped_shadow_live_execution_authorization"] = False
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="missing_prod_scoped_shadow_live_execution_authorization",
    ):
        _apply_grant(_load(bundle_path))


@pytest.mark.parametrize(
    "field_path",
    [
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
    ],
)
def test_rejects_live_read_only_pilot_not_executed_or_passed(
    tmp_path: Path,
    live_execution_request_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    _set_path(payload, field_path, False)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=field_path.split(".")[-1]):
        _apply_grant(_load(bundle_path))


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
    ],
)
def test_verifier_rejects_accidental_global_default_api_user_visible_or_write_enablement(
    tmp_path: Path,
    live_execution_request_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=field_path.split(".")[-1]):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_execution_grant_filed=True,
            verify_local_pilot_files=False,
        )


def test_live_read_only_execution_authorization_stays_true_after_grant(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    granted = grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )
    assert granted["authorization"]["prod_scoped_shadow_live_read_only_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True


def test_grant_does_not_import_runtime_or_database_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    original_import = builtins.__import__
    forbidden = {
        "psycopg",
        "openai",
        "openalex",
        "sklearn",
        "pipeline.ml_shadow_scorer_online_shadow_runtime",
    }

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name in forbidden:
            raise AssertionError(f"live execution grant must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )


def test_grant_does_not_open_shadow_runs_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"live execution grant must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )


def test_grant_requires_independent_or_equivalent_review(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="requires second_reviewer"):
        grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="second_reviewer"):
        grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner="Same Reviewer",
            second_reviewer="Same Reviewer",
            repo_root=root,
        )


def test_caveats_include_live_execution_grant_items_and_not_stale_request_only_items(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    granted = grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live execution review",
        repo_root=root,
    )
    for caveat in LIVE_EXECUTION_GRANT_CAVEATS:
        assert caveat in granted["caveats"]
    assert set(LIVE_EXECUTION_REQUEST_CAVEATS).issubset(granted["caveats"])


def test_cli_smoke_grant_live_execution_then_verify(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-live-execution",
        "--bundle",
        str(bundle_path),
        "--owner",
        "CLI Live Execution Grant Owner",
        "--owner-documents-equivalent-review",
        "cli owner equivalent live execution review",
        "--grant-notes",
        "cli live execution grant notes",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        POST_LIVE_EXECUTION_GRANT_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-live-execution-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_live_execution_grant",
        "online-shadow-production-scoped-v1",
        POST_LIVE_EXECUTION_GRANT_NEXT_STAGE,
    ]


def test_apply_rejects_already_granted(
    tmp_path: Path,
    live_execution_request_template_root: Path,
) -> None:
    root = _copy_template_repo(live_execution_request_template_root, tmp_path)
    bundle_path = _ensure_rev13_bundle(root)
    payload = _load(bundle_path)
    payload["authorization"]["prod_scoped_shadow_live_execution_authorization_granted"] = True
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        _apply_grant(_load(bundle_path))
