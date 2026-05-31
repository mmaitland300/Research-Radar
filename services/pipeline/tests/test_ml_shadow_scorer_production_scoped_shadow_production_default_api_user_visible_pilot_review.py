"""Tests for reviewing the recorded production default/API/user-visible pilot run."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline import (
    ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot_review as review_module,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_REVIEW_CHECKS,
    apply_production_scoped_shadow_production_default_api_user_visible_pilot_review,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot_review import (
    MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError,
    build_production_scoped_shadow_production_default_api_user_visible_pilot_review_slice,
    evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks,
    review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot,
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


def _prepare_rev23_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 25:
        payload = bundle_module._without_controlled_production_recommendation_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 24:
        payload = bundle_module._without_production_default_api_user_visible_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] != 23:
        raise AssertionError(
            f"expected committed production-scoped bundle revision 23 after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE:
        raise AssertionError(
            "expected recommended_next_stage post production default/API/user-visible pilot run "
            "after template preparation"
        )
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev23_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(
        tmp_path_factory.mktemp("production-default-api-user-visible-pilot-review-template")
    )
    _prepare_rev23_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def test_happy_path_accepts_revision_twenty_three_production_default_pilot_evidence(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    result = review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        review_notes="production default review notes",
        repo_root=root,
        generated_at="2026-05-30T22:00:00Z",
    )
    updated = _load(bundle_path)

    assert result["production_default_api_user_visible_pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 24
    assert updated["metadata"]["generated_at"] == "2026-05-30T22:00:00Z"
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"] == before["execution"]
    assert updated["authorization"] == before["authorization"]
    assert updated["review"]["review_decision"] == before["review"]["review_decision"]
    assert updated["review"]["pilot_review_decision"] == before["review"]["pilot_review_decision"]
    assert updated["review"]["live_read_only_pilot_review_decision"] == before["review"][
        "live_read_only_pilot_review_decision"
    ]
    assert updated["review"]["live_execution_pilot_review_decision"] == before["review"][
        "live_execution_pilot_review_decision"
    ]
    assert updated["review"]["flag_enablement_pilot_review_decision"] == before["review"][
        "flag_enablement_pilot_review_decision"
    ]
    assert updated["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert updated["review"]["prod_scoped_shadow_production_default_api_user_visible_pilot_reviewed"] is True
    assert updated["review"]["prod_scoped_shadow_production_default_api_user_visible_pilot_accepted"] is True
    decision = updated["review"]["production_default_api_user_visible_pilot_review_decision"]
    assert decision["decision"] == "accepted"
    assert decision["reviewer"] == "Matt Maitland"
    assert decision["review_notes"] == "production default review notes"
    assert decision["failed_review_checks"] == []
    assert set(decision["checks"]) == set(PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_REVIEW_CHECKS)
    assert updated["authorization"]["prod_scoped_shadow_production_default_api_user_visible_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    assert verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_production_default_api_user_visible_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )["verification_mode"] == "post_production_default_api_user_visible_pilot_review"


def test_build_review_slice_then_apply_review_slice(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)

    review_slice = build_production_scoped_shadow_production_default_api_user_visible_pilot_review_slice(
        payload,
        reviewer="Matt Maitland",
        review_notes="two-step review",
        reviewed_at="2026-05-30T22:05:00Z",
    )
    updated = apply_production_scoped_shadow_production_default_api_user_visible_pilot_review(
        payload,
        review_slice,
        generated_at="2026-05-30T22:05:00Z",
    )

    assert updated["review"]["prod_scoped_shadow_production_default_api_user_visible_pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 24


def test_failing_review_check_files_not_accepted_review(
    tmp_path: Path,
    rev23_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_evaluate = (
        review_module.evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks
    )

    def failing_evaluate(bundle: dict[str, Any]) -> dict[str, bool]:
        checks = original_evaluate(bundle)
        checks["runtime_row_count_528"] = False
        return checks

    monkeypatch.setattr(
        review_module,
        "evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks",
        failing_evaluate,
    )

    result = review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    updated = _load(bundle_path)

    assert result["production_default_api_user_visible_pilot_accepted"] is False
    decision = updated["review"]["production_default_api_user_visible_pilot_review_decision"]
    assert decision["decision"] == "not_accepted"
    assert decision["failed_review_checks"] == ["runtime_row_count_528"]
    assert updated["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_REJECTED_NEXT_STAGE


def test_accepted_review_with_failed_checks_rejected(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    review_slice = build_production_scoped_shadow_production_default_api_user_visible_pilot_review_slice(
        payload,
        reviewer="Matt Maitland",
        reviewed_at="2026-05-30T22:10:00Z",
    )
    decision = review_slice["production_default_api_user_visible_pilot_review_decision"]
    decision["decision"] = "accepted"
    decision["failed_review_checks"] = ["runtime_row_count_528"]
    decision["checks"]["runtime_row_count_528"] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="failed checks"):
        apply_production_scoped_shadow_production_default_api_user_visible_pilot_review(
            payload,
            review_slice,
            generated_at="2026-05-30T22:10:00Z",
        )


def test_evaluate_reads_production_default_pilot_run_not_other_pilot_slices(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    baseline = evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks(payload)
    assert baseline["runtime_row_count_528"] is True
    assert baseline["joined_candidate_count_528"] is True

    payload["execution"]["flag_enablement_pilot_run"]["input_join_summary"]["runtime_row_count"] = 0
    payload["execution"]["flag_enablement_pilot_run"]["input_join_summary"]["joined_candidate_count"] = 0
    after_flag_corruption = evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks(
        payload
    )
    assert after_flag_corruption["runtime_row_count_528"] is True
    assert after_flag_corruption["joined_candidate_count_528"] is True

    payload["execution"]["production_default_api_user_visible_pilot_run"]["input_join_summary"][
        "runtime_row_count"
    ] = 0
    after_prod_corruption = (
        evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks(payload)
    )
    assert after_prod_corruption["runtime_row_count_528"] is False


def test_compositional_verify_strips_rev24_overlay(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    payload = _load(bundle_path)
    stripped = bundle_module._without_production_default_api_user_visible_pilot_review_payload(payload)
    assert stripped["metadata"]["bundle_revision"] == 23
    assert stripped["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE
    verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_production_default_api_user_visible_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )


def test_rejects_wrong_revision_wrong_next_stage_or_double_review(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 22
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError,
        match="bundle_revision",
    ):
        review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
            bundle_path=bundle_path,
            reviewer="Matt Maitland",
            repo_root=root,
        )

    root = _copy_template_repo(rev23_template_root, tmp_path / "wrong-stage")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    payload["recommended_next_stage"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    _write_json(bundle_path, payload)
    with pytest.raises(
        MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError,
        match="recommended_next_stage",
    ):
        review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
            bundle_path=bundle_path,
            reviewer="Matt Maitland",
            repo_root=root,
        )

    root = _copy_template_repo(rev23_template_root, tmp_path / "double")
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    with pytest.raises(
        MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError,
        match="bundle_revision|already",
    ):
        review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
            bundle_path=bundle_path,
            reviewer="Matt Maitland",
            repo_root=root,
        )


def test_guardrails_stay_false_and_upstream_authorizations_stay_true(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )
    updated = _load(bundle_path)
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_flag_enablement_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_read_only_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_production_default_api_user_visible_authorized"] is True
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["posture"]["writes_performed"] is False
    assert updated["posture"]["runtime_writes_performed"] is False
    assert (
        updated["plan"]["feature_flag_iam_config_requirements"]["prod_scoped_flag_enablement_authorized_now"] is False
    )


def test_review_does_not_import_runtime_database_modules_or_open_shadow_runs(
    tmp_path: Path,
    rev23_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot_review.py"
    ).read_text(encoding="utf-8")
    for forbidden in ("psycopg", "openai", "openalex", "sklearn", "_connect_readonly"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
        assert forbidden not in module_source
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source

    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"review must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        repo_root=root,
    )


def test_cli_smoke_review_then_verify_revision_twenty_four(
    tmp_path: Path,
    rev23_template_root: Path,
) -> None:
    root = _copy_template_repo(rev23_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-production-default-api-user-visible-pilot-review",
        "--bundle",
        str(bundle_path),
        "--reviewer",
        "CLI Production Default Reviewer",
        "--review-notes",
        "cli production default review notes",
        "--repo-root",
        str(root),
    ]
    reviewed = subprocess.run(review_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert reviewed.stdout.splitlines() == [
        "accepted",
        "True",
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-production-default-api-user-visible-pilot-review-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_production_default_api_user_visible_pilot_review",
        "online-shadow-production-scoped-v1",
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_production_default_api_user_visible_pilot_review_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 24:
        pytest.skip("committed production-scoped bundle is not revision 24 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_production_default_api_user_visible_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 24
    assert result["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
