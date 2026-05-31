"""Tests for reviewing the controlled production recommendation pilot run."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import (
    ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot_review
    as review_module,
)
from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_REVIEW_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_controlled_production_recommendation_pilot_review,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot_review import (
    MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError,
    build_production_scoped_shadow_controlled_production_recommendation_pilot_review_slice,
    review_ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot,
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


def _prepare_rev27_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 29:
        payload = bundle_module._without_limited_production_recommendation_rollout_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 28:
        payload = bundle_module._without_controlled_production_recommendation_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] != 27:
        raise AssertionError(
            "expected committed production-scoped bundle revision 27 pilot-run state after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE:
        raise AssertionError("expected post controlled production recommendation pilot-run next stage")
    _write_json(bundle_path, payload)


@pytest.fixture(scope="module")
def rev27_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("controlled-production-recommendation-pilot-review-template"))
    _prepare_rev27_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _review(root: Path, bundle_path: Path) -> dict[str, Any]:
    return review_ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot(
        bundle_path=bundle_path,
        reviewer="Matt Maitland",
        review_notes="controlled recommendation pilot review notes",
        repo_root=root,
        generated_at="2026-05-31T12:00:00Z",
    )


def test_happy_accepted_review_rev27_to_rev28(
    tmp_path: Path,
    rev27_template_root: Path,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    prior_review_decisions = {
        key: before["review"][key]
        for key in (
            "review_decision",
            "pilot_review_decision",
            "live_read_only_pilot_review_decision",
            "live_execution_pilot_review_decision",
            "flag_enablement_pilot_review_decision",
            "production_default_api_user_visible_pilot_review_decision",
        )
    }

    result = _review(root, bundle_path)
    updated = _load(bundle_path)
    decision = updated["review"]["controlled_production_recommendation_pilot_review_decision"]

    assert result["controlled_production_recommendation_pilot_accepted"] is True
    assert updated["metadata"]["bundle_revision"] == 28
    assert updated["metadata"]["generated_at"] == "2026-05-31T12:00:00Z"
    assert updated["execution"] == before["execution"]
    assert updated["authorization"] == before["authorization"]
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert {key: updated["review"][key] for key in prior_review_decisions} == prior_review_decisions
    assert decision["decision"] == "accepted"
    assert decision["failed_review_checks"] == []
    assert set(decision["checks"]) == set(CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_REVIEW_CHECKS)
    assert updated["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    assert updated["authorization"]["prod_scoped_shadow_controlled_production_recommendation_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["writes_performed"] is False
    assert updated["runtime_writes_performed"] is False
    assert "Review is required before any further rollout." not in updated["caveats"]

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_controlled_production_recommendation_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert verified["verification_mode"] == "post_controlled_production_recommendation_pilot_review"


def test_rejected_path_when_one_review_check_fails(
    tmp_path: Path,
    rev27_template_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    original_evaluate = (
        review_module.evaluate_production_scoped_shadow_controlled_production_recommendation_pilot_review_checks
    )

    def failing_evaluate(bundle: dict[str, Any]) -> dict[str, bool]:
        checks = original_evaluate(bundle)
        checks["response_status_200"] = False
        return checks

    monkeypatch.setattr(
        review_module,
        "evaluate_production_scoped_shadow_controlled_production_recommendation_pilot_review_checks",
        failing_evaluate,
    )

    result = _review(root, bundle_path)
    updated = _load(bundle_path)

    assert result["controlled_production_recommendation_pilot_accepted"] is False
    assert updated["review"]["controlled_production_recommendation_pilot_review_decision"]["decision"] == "not_accepted"
    assert updated["review"]["controlled_production_recommendation_pilot_review_decision"][
        "failed_review_checks"
    ] == ["response_status_200"]
    assert updated["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_REJECTED_NEXT_STAGE


def test_accepted_review_with_failed_checks_rejected(
    tmp_path: Path,
    rev27_template_root: Path,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    review_slice = build_production_scoped_shadow_controlled_production_recommendation_pilot_review_slice(
        payload,
        reviewer="Matt Maitland",
        reviewed_at="2026-05-31T12:05:00Z",
    )
    decision = review_slice["controlled_production_recommendation_pilot_review_decision"]
    decision["decision"] = "accepted"
    decision["failed_review_checks"] = ["response_status_200"]
    decision["checks"]["response_status_200"] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="failed checks"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_review(
            payload,
            review_slice,
            generated_at="2026-05-31T12:05:00Z",
        )


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 26, "bundle_revision"),
        ("recommended_next_stage", POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE, "recommended_next_stage"),
    ],
)
def test_wrong_revision_or_stage_rejected(
    tmp_path: Path,
    rev27_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError,
        match=message,
    ):
        _review(root, bundle_path)


@pytest.mark.parametrize(
    "section_name",
    ["review", "posture", "shadow_and_production_blockers"],
)
def test_double_review_rejected_for_review_posture_or_blocker_flags(
    tmp_path: Path,
    rev27_template_root: Path,
    section_name: str,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    review_slice = build_production_scoped_shadow_controlled_production_recommendation_pilot_review_slice(
        payload,
        reviewer="Matt Maitland",
    )
    payload[section_name]["prod_scoped_shadow_controlled_production_recommendation_pilot_reviewed"] = True

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already|must not"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_review(payload, review_slice)


@pytest.mark.parametrize(
    "path",
    [
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
    ],
)
def test_direct_apply_rejects_forbidden_output_or_global_flags_true(
    tmp_path: Path,
    rev27_template_root: Path,
    path: str,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    payload = _load(root / FIXTURE_RELS["production_scoped_bundle"])
    review_slice = build_production_scoped_shadow_controlled_production_recommendation_pilot_review_slice(
        payload,
        reviewer="Matt Maitland",
    )
    _set_path(payload, path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_review(payload, review_slice)


def test_compositional_strip_verifies_as_rev27_pilot_run(
    tmp_path: Path,
    rev27_template_root: Path,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _review(root, bundle_path)
    payload = _load(bundle_path)
    stripped = bundle_module._without_controlled_production_recommendation_pilot_review_payload(payload)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_controlled_production_recommendation_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )

    assert stripped["metadata"]["bundle_revision"] == 27
    assert stripped["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE
    assert result["verification_mode"] == "post_controlled_production_recommendation_pilot_run"


def test_review_module_has_no_runtime_database_api_or_shadow_runs_imports() -> None:
    module_source = (
        PACKAGE_ROOT
        / "pipeline"
        / "ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot_review.py"
    ).read_text(encoding="utf-8")
    import_lines = [
        line for line in module_source.splitlines() if line.startswith("import ") or line.startswith("from ")
    ]
    for forbidden in ("psycopg", "openai", "openalex", "sklearn", "requests", "httpx", "shadow-runs"):
        assert all(forbidden not in line for line in import_lines)
    assert "run_controlled_production_recommendation_pilot" not in module_source


def test_cli_smoke_review_then_verify_revision_twenty_eight(
    tmp_path: Path,
    rev27_template_root: Path,
) -> None:
    root = _copy_template_repo(rev27_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    review_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-controlled-production-recommendation-pilot-review",
        "--bundle",
        str(bundle_path),
        "--reviewer",
        "CLI Controlled Reviewer",
        "--review-notes",
        "cli controlled review notes",
        "--repo-root",
        str(root),
    ]
    reviewed = subprocess.run(review_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert reviewed.stdout.splitlines() == [
        "accepted",
        "True",
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-controlled-production-recommendation-pilot-review-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_controlled_production_recommendation_pilot_review",
        "online-shadow-production-scoped-v1",
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    ]


def test_committed_bundle_verifies_with_expect_controlled_production_recommendation_pilot_review_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 28:
        pytest.skip("committed production-scoped bundle is not revision 28 yet")

    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_controlled_production_recommendation_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 28
    assert result["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
