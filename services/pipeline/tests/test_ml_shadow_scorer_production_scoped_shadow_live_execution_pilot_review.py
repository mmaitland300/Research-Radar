"""Tests for reviewing the recorded live execution production-scoped pilot run."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_review as review_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_live_execution_pilot_review,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_review import (
    MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError,
    build_production_scoped_shadow_live_execution_pilot_review_slice,
    review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot,
)
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    _copy_fixture_repo,
    _copy_template_repo,
    _load,
    _prepare_rev15_template_bundle,
    _write_json,
    rev15_template_root,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def test_happy_path_accepts_revision_fifteen_live_execution_pilot_evidence(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    result = review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        review_notes="live execution review notes",
        repo_root=root,
        reviewed_at="2026-05-30T20:00:00Z",
    )
    updated = _load(bundle_path)

    assert result["live_execution_pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 16
    assert updated["metadata"]["generated_at"] == "2026-05-30T20:00:00Z"
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"] == before["execution"]
    assert updated["authorization"] == before["authorization"]
    assert updated["review"]["review_decision"] == before["review"]["review_decision"]
    assert updated["review"]["pilot_review_decision"] == before["review"]["pilot_review_decision"]
    assert updated["review"]["live_read_only_pilot_review_decision"] == before["review"][
        "live_read_only_pilot_review_decision"
    ]
    assert updated["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert updated["review"]["prod_scoped_shadow_live_execution_pilot_reviewed"] is True
    assert updated["review"]["prod_scoped_shadow_live_execution_pilot_accepted"] is True
    decision = updated["review"]["live_execution_pilot_review_decision"]
    assert decision["decision"] == "accepted"
    assert decision["reviewer"] == "Matt Maitland"
    assert decision["review_notes"] == "live execution review notes"
    assert decision["failed_review_checks"] == []
    assert isinstance(decision["accepted_evidence"], list)
    assert isinstance(decision["limitations"], list)
    assert set(decision["checks"]) == set(LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS)
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    assert "Review is required before any further enablement chain." not in updated["caveats"]
    assert verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )["verification_mode"] == "post_live_execution_pilot_review"


def test_build_review_slice_then_apply_review_slice(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)

    review_slice = build_production_scoped_shadow_live_execution_pilot_review_slice(
        payload,
        reviewer="Matt Maitland",
        review_notes="two-step review",
        reviewed_at="2026-05-30T20:05:00Z",
    )
    updated = apply_production_scoped_shadow_live_execution_pilot_review(
        payload,
        review_slice,
        generated_at="2026-05-30T20:05:00Z",
    )

    assert updated["review"]["prod_scoped_shadow_live_execution_pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 16


def test_failing_review_check_files_not_accepted_review(
    tmp_path: Path,
    rev15_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_evaluate = review_module.evaluate_production_scoped_shadow_live_execution_pilot_review_checks

    def failing_evaluate(bundle: dict[str, Any]) -> dict[str, bool]:
        checks = original_evaluate(bundle)
        checks["runtime_row_count_528"] = False
        return checks

    monkeypatch.setattr(
        review_module,
        "evaluate_production_scoped_shadow_live_execution_pilot_review_checks",
        failing_evaluate,
    )

    result = review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    updated = _load(bundle_path)

    assert result["live_execution_pilot_accepted"] is False
    decision = updated["review"]["live_execution_pilot_review_decision"]
    assert decision["decision"] == "not_accepted"
    assert decision["failed_review_checks"] == ["runtime_row_count_528"]
    assert isinstance(decision["accepted_evidence"], list)
    assert isinstance(decision["limitations"], list)
    assert updated["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE


def test_compositional_verify_strips_rev16_overlay(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_live_execution_pilot_review_payload(payload)
    assert stripped["metadata"]["bundle_revision"] == 15
    assert stripped["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE
    verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )


def test_rejects_wrong_revision_wrong_next_stage_or_double_review(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 14
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError, match="bundle_revision"):
        review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
            bundle_path=bundle_path,
            reviewer="Matt Maitland",
            repo_root=root,
        )

    root = _copy_template_repo(rev15_template_root, tmp_path / "wrong-stage")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError, match="recommended_next_stage"):
        review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
            bundle_path=bundle_path,
            reviewer="Matt Maitland",
            repo_root=root,
        )

    root = _copy_template_repo(rev15_template_root, tmp_path / "double")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    with pytest.raises(
        MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError,
        match="bundle_revision|already",
    ):
        review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
            bundle_path=bundle_path,
            reviewer="Matt Maitland",
            repo_root=root,
        )


def test_review_does_not_import_runtime_database_modules_or_open_shadow_runs(
    tmp_path: Path,
    rev15_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_review.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("psycopg", "openai", "openalex", "sklearn", "_connect_readonly"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
        assert forbidden not in module_source
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source

    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"review must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )


def test_verifier_passes_without_shadow_runs_on_disk(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )

    assert not (root / "docs/audit/shadow-runs").exists()
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["verification_mode"] == "post_live_execution_pilot_review"


def test_verifier_rejects_bad_live_execution_pilot_review_decision(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    payload = _load(bundle_path)
    payload["review"]["live_execution_pilot_review_decision"]["decision"] = "not_accepted"
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="decision"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_execution_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )


def test_cli_smoke_review_then_verify_revision_sixteen(
    tmp_path: Path,
    rev15_template_root: Path,
) -> None:
    root = _copy_template_repo(rev15_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-live-execution-pilot-review",
        "--bundle",
        str(bundle_path),
        "--reviewer",
        "CLI Live Execution Reviewer",
        "--review-notes",
        "cli live execution review notes",
        "--repo-root",
        str(root),
    ]
    reviewed = subprocess.run(review_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert reviewed.stdout.splitlines() == [
        "accepted",
        "True",
        POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-live-execution-pilot-review-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_live_execution_pilot_review",
        "online-shadow-production-scoped-v1",
        POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_live_execution_pilot_review_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 16:
        pytest.skip("committed production-scoped bundle is not revision 16 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 16
    assert result["recommended_next_stage"] == POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
