"""Tests for requesting production-scoped live execution shadow authorization."""

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
    LIVE_EXECUTION_REQUEST_CAVEATS,
    LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED,
    LIVE_EXECUTION_REQUEST_FUTURE_GRANT_REQUIREMENTS,
    LIVE_EXECUTION_REQUEST_SCOPE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    apply_production_scoped_shadow_live_execution_authorization_request,
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


def _write_rev12_bundle(root: Path) -> Path:
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    revision = payload["metadata"]["bundle_revision"]
    if revision == 15:
        payload = bundle_module._without_live_execution_pilot_run_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 14:
        payload = bundle_module._without_live_execution_grant_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 13:
        payload = _downgrade_to_rev12(payload)
        _write_json(bundle_path, payload)
    elif revision != 12:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 12, 13, 14, or 15, got {revision}"
        )
    return bundle_path


@pytest.fixture(scope="module")
def live_read_only_pilot_review_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("live-execution-request-template"))
    _write_rev12_bundle(root)
    return root


def test_happy_path_request_from_revision_twelve_to_revision_thirteen(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    requested = request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Live Execution Requester",
        request_notes="live execution request notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_request_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 12
    assert before["authorization"].get("prod_scoped_shadow_live_execution_authorization_requested") is not True
    assert requested["metadata"]["bundle_revision"] == 13
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
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_authorized"] is True
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert requested["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert requested["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert requested["authorization"]["prod_scoped_shadow_live_execution_authorization_requested"] is True
    assert requested["authorization"]["prod_scoped_shadow_live_execution_authorization_granted"] is False
    assert requested["authorization"]["live_execution_request_decision"]["decision"] == "requested"
    assert requested["authorization"]["live_execution_request_decision"]["requester"] == "Live Execution Requester"
    assert requested["authorization"]["live_execution_request_decision"]["request_notes"] == "live execution request notes"
    assert (
        requested["authorization"]["live_execution_requested_scope"]["authorization_scope"]
        == LIVE_EXECUTION_REQUEST_SCOPE
    )
    assert set(LIVE_EXECUTION_REQUEST_FUTURE_GRANT_REQUIREMENTS).issubset(
        requested["authorization"]["live_execution_requested_scope"]["future_grant_would_require"]
    )
    assert set(LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        requested["authorization"]["live_execution_requested_scope"]["explicitly_not_included"]
    )
    assert requested["posture"]["live_prod_source_reads_performed"] is True
    assert requested["posture"]["missing_prod_scoped_shadow_live_execution_authorization"] is True
    assert requested["shadow_and_production_blockers"]["blockers_introduced_by_live_execution_request"] == [
        "missing_prod_scoped_shadow_live_execution_authorization"
    ]
    assert requested["shadow_and_production_blockers"]["blockers_cleared_by_live_execution_request"] == []
    assert requested["shadow_and_production_blockers"]["blockers_unchanged_by_live_execution_request"] is True
    assert "blockers_changed_by_live_execution_request" not in requested["shadow_and_production_blockers"]
    assert requested["recommended_next_stage"] == (
        "record_production_scoped_online_shadow_live_execution_authorization_grant_v1"
    )
    assert "Bundle live-execution request milestone only; grants no live execution authorization." in requested["caveats"]
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_live_execution_request"


def test_compositional_verify_strips_rev_thirteen_overlay(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_live_execution_request_payload(payload)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_live_read_only_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )


def test_wrong_bundle_revision_rejection(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 11
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_wrong_recommended_next_stage_rejection(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = "wrong_next_stage"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="recommended_next_stage"):
        request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_double_request_rejection(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_rejects_live_read_only_pilot_review_not_accepted(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["review"]["prod_scoped_shadow_live_read_only_pilot_accepted"] = False
    payload["review"]["live_read_only_pilot_review_decision"]["decision"] = "not_accepted"
    payload["review"]["live_read_only_pilot_review_decision"]["failed_review_checks"] = [
        "live_prod_source_reads_true"
    ]
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="prod_scoped_shadow_live_read_only_pilot_accepted"):
        apply_production_scoped_shadow_live_execution_authorization_request(_load(bundle_path))


def test_rejects_non_empty_failed_review_checks(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["review"]["live_read_only_pilot_review_decision"]["failed_review_checks"] = [
        "live_prod_source_reads_true"
    ]
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="failed_review_checks"):
        apply_production_scoped_shadow_live_execution_authorization_request(_load(bundle_path))


@pytest.mark.parametrize(
    ("authorization_field", "mutation", "error_match"),
    [
        ("request_decision", "pop", "request_decision"),
        ("requested_scope", "pop", "requested_scope"),
        ("live_read_only_grant_decision", "pop", "live_read_only_grant_decision"),
        ("live_read_only_granted_scope", "pop", "live_read_only_granted_scope"),
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
def test_apply_rejects_missing_live_read_only_request_or_grant_slices(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
    authorization_field: str,
    mutation: str,
    error_match: str,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    if mutation == "pop":
        payload["authorization"].pop(authorization_field, None)
    else:
        payload["authorization"][authorization_field] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=error_match):
        apply_production_scoped_shadow_live_execution_authorization_request(payload)


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        "authorization.prod_scoped_shadow_live_execution_authorized",
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
    ],
)
def test_verifier_rejects_accidental_grant_live_global_default_api_or_user_visible_enablement(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=field_path.split(".")[-1]):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_execution_request_filed=True,
            verify_local_pilot_files=False,
        )


def test_live_read_only_authorization_stays_true_after_request(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    requested = request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_authorized"] is True
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True


def test_request_does_not_import_runtime_or_database_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
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
            raise AssertionError(f"live execution request must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )


def test_request_does_not_open_shadow_runs_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"live execution request must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )


def test_verifier_passes_without_shadow_runs_on_disk(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )

    assert not (root / "docs/audit/shadow-runs").exists()
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_request_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["verification_mode"] == "post_live_execution_request"


def test_cli_smoke_request_live_execution_then_verify(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-live-execution",
        "--bundle",
        str(bundle_path),
        "--requester",
        "CLI Live Execution Requester",
        "--request-notes",
        "cli live execution request notes",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        "record_production_scoped_online_shadow_live_execution_authorization_grant_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-live-execution-request-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_live_execution_request",
        "online-shadow-production-scoped-v1",
        "record_production_scoped_online_shadow_live_execution_authorization_grant_v1",
    ]


def test_caveats_include_live_execution_request_items(
    tmp_path: Path,
    live_read_only_pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(live_read_only_pilot_review_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    requested = request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    for caveat in LIVE_EXECUTION_REQUEST_CAVEATS:
        assert caveat in requested["caveats"]
