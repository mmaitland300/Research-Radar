"""Tests for reviewing the recorded live read-only production-scoped pilot run."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot_review as review_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot_review import (
    MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError,
    review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot,
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


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _downgrade_to_rev11(payload: dict[str, Any]) -> dict[str, Any]:
    downgraded = json.loads(json.dumps(payload))
    downgraded["metadata"]["bundle_revision"] = 11
    downgraded["recommended_next_stage"] = POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE
    downgraded["caveats"] = bundle_module._caveats(mode="post_live_read_only_pilot_run")
    for section in ("review", "posture", "shadow_and_production_blockers"):
        target = downgraded[section]
        target.pop("prod_scoped_shadow_live_read_only_pilot_reviewed", None)
        target.pop("prod_scoped_shadow_live_read_only_pilot_accepted", None)
    downgraded["review"].pop("live_read_only_pilot_review_decision", None)
    blockers = downgraded["shadow_and_production_blockers"]
    blockers.pop("blockers_cleared_by_live_read_only_pilot_review", None)
    blockers.pop("blockers_introduced_by_live_read_only_pilot_review", None)
    blockers.pop("blockers_unchanged_by_live_read_only_pilot_review", None)
    blockers.pop("blockers_changed_by_live_read_only_pilot_review", None)
    return downgraded


def _write_rev11_bundle(root: Path) -> Path:
    bundle_path = _fixture(root, "production_scoped_bundle")
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 22:
        payload = bundle_module._without_production_default_api_user_visible_grant_payload(payload)
    if payload["metadata"]["bundle_revision"] == 21:
        payload = bundle_module._without_production_default_api_user_visible_request_payload(payload)
    revision = payload["metadata"]["bundle_revision"]
    if revision == 20:
        payload = bundle_module._without_flag_enablement_pilot_review_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 19:
        payload = bundle_module._without_flag_enablement_pilot_run_payload(payload)
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
        revision = payload["metadata"]["bundle_revision"]
    if revision == 13:
        payload = bundle_module._without_live_execution_request_payload(payload)
        revision = payload["metadata"]["bundle_revision"]
    if revision == 12:
        payload = _downgrade_to_rev11(payload)
        _write_json(bundle_path, payload)
    elif revision != 11:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 11 through 19, got {revision}"
        )
    return bundle_path


def test_happy_path_accepts_revision_eleven_live_read_only_pilot_evidence(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    before = _load(bundle_path)

    result = review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path,
        reviewer="Live Review Person",
        review_notes="live read-only review notes",
        repo_root=root,
        reviewed_at="2026-05-30T04:30:00Z",
    )
    updated = _load(bundle_path)

    assert result["live_read_only_pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 12
    assert updated["metadata"]["generated_at"] == "2026-05-30T04:30:00Z"
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"] == before["execution"]
    assert updated["authorization"]["grant_decision"] == before["authorization"]["grant_decision"]
    assert updated["authorization"]["granted_scope"] == before["authorization"]["granted_scope"]
    assert updated["authorization"]["request_decision"] == before["authorization"]["request_decision"]
    assert updated["authorization"]["requested_scope"] == before["authorization"]["requested_scope"]
    assert updated["authorization"]["live_read_only_grant_decision"] == before["authorization"][
        "live_read_only_grant_decision"
    ]
    assert updated["authorization"]["live_read_only_granted_scope"] == before["authorization"][
        "live_read_only_granted_scope"
    ]
    assert updated["review"]["review_decision"] == before["review"]["review_decision"]
    assert updated["review"]["pilot_review_decision"] == before["review"]["pilot_review_decision"]
    assert updated["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert updated["review"]["prod_scoped_shadow_live_read_only_pilot_reviewed"] is True
    assert updated["review"]["prod_scoped_shadow_live_read_only_pilot_accepted"] is True
    decision = updated["review"]["live_read_only_pilot_review_decision"]
    assert decision["decision"] == "accepted"
    assert decision["reviewer"] == "Live Review Person"
    assert decision["review_notes"] == "live read-only review notes"
    assert decision["failed_review_checks"] == []
    assert set(decision["checks"]) == set(LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS)
    assert updated["posture"]["live_prod_source_reads_performed"] is True
    assert updated["posture"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_read_only_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["recommended_next_stage"] == (
        "request_production_scoped_online_shadow_live_execution_authorization_v1"
    )
    assert "No live production DB/source reads were performed." not in updated["caveats"]
    assert "Live read-only production shadow access remains a separate future authorization chain." not in updated[
        "caveats"
    ]
    assert verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )["verification_mode"] == "post_live_read_only_pilot_review"


def test_failing_review_check_files_not_accepted_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    original_evaluate = review_module.evaluate_production_scoped_shadow_live_read_only_pilot_review_checks

    def failing_evaluate(bundle: dict[str, Any]) -> dict[str, bool]:
        checks = original_evaluate(bundle)
        checks["live_prod_source_reads_true"] = False
        return checks

    monkeypatch.setattr(
        review_module,
        "evaluate_production_scoped_shadow_live_read_only_pilot_review_checks",
        failing_evaluate,
    )

    result = review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path,
        reviewer="Live Review Person",
        repo_root=root,
    )
    updated = _load(bundle_path)

    assert result["live_read_only_pilot_accepted"] is False
    assert updated["review"]["live_read_only_pilot_review_decision"]["decision"] == "not_accepted"
    assert updated["review"]["live_read_only_pilot_review_decision"]["failed_review_checks"] == [
        "live_prod_source_reads_true"
    ]
    assert updated["recommended_next_stage"] == (
        "remediate_production_scoped_online_shadow_live_read_only_pilot_v1"
    )
    assert verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )["verification_mode"] == "post_live_read_only_pilot_review"


def test_rejects_pre_live_read_only_pilot_or_double_review(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 10
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError, match="bundle_revision"):
        review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            reviewer="Live Review Person",
            repo_root=root,
        )

    root = _copy_fixture_repo(tmp_path / "double")
    bundle_path = _write_rev11_bundle(root)
    review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path,
        reviewer="Live Review Person",
        repo_root=root,
    )
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError, match="bundle_revision|already"):
        review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
            bundle_path=bundle_path,
            reviewer="Live Review Person",
            repo_root=root,
        )


def test_review_does_not_import_runtime_database_modules_or_open_shadow_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot_review.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("psycopg", "openai", "openalex", "sklearn", "_connect_readonly"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
        assert forbidden not in module_source
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source

    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"review must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path,
        reviewer="Live Review Person",
        repo_root=root,
    )


def test_verifier_passes_without_shadow_runs_on_disk(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path,
        reviewer="Live Review Person",
        repo_root=root,
    )

    assert not (root / "docs/audit/shadow-runs").exists()
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["verification_mode"] == "post_live_read_only_pilot_review"


def test_verifier_rejects_bad_live_read_only_pilot_review_decision(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
        bundle_path=bundle_path,
        reviewer="Live Review Person",
        repo_root=root,
    )
    payload = _load(bundle_path)
    payload["review"]["live_read_only_pilot_review_decision"]["decision"] = "not_accepted"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="decision"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )


def test_cli_smoke_review_then_verify_revision_twelve(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_rev11_bundle(root)
    review_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-live-read-only-pilot-review",
        "--bundle",
        str(bundle_path),
        "--reviewer",
        "CLI Live Reviewer",
        "--review-notes",
        "cli live review notes",
        "--repo-root",
        str(root),
    ]
    reviewed = subprocess.run(review_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert reviewed.stdout.splitlines() == [
        "accepted",
        "True",
        "request_production_scoped_online_shadow_live_execution_authorization_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-live-read-only-pilot-review-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_live_read_only_pilot_review",
        "online-shadow-production-scoped-v1",
        "request_production_scoped_online_shadow_live_execution_authorization_v1",
    ]
