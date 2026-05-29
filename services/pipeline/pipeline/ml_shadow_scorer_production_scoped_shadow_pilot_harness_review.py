"""Review recorded production-scoped pilot harness evidence without rerunning it."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    MLShadowScorerProductionScopedShadowBundleError,
    PILOT_HARNESS_EXPECTED_FILES,
    PILOT_HARNESS_REVIEW_CHECKS,
    PILOT_HARNESS_SURFACE,
    POST_PILOT_HARNESS_BUNDLE_REVISION,
    POST_PILOT_HARNESS_NEXT_STAGE,
    apply_production_scoped_shadow_pilot_harness_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root


class MLShadowScorerProductionScopedShadowPilotHarnessReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError(f"Expected JSON object in {path}")
    return payload


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _require_revision_five_harness(bundle: Mapping[str, Any]) -> None:
    required = {
        "metadata.bundle_revision": POST_PILOT_HARNESS_BUNDLE_REVISION,
        "execution.prod_scoped_shadow_pilot_harness_executed": True,
        "execution.prod_scoped_shadow_pilot_harness_passed": True,
        "execution.prod_scoped_shadow_pilot_executed": False,
        "execution.pilot_harness.pilot_surface": PILOT_HARNESS_SURFACE,
        "execution.pilot_harness.live_prod_source_reads_performed": False,
        "authorization.prod_scoped_shadow_pilot_authorized": True,
        "authorization.prod_scoped_shadow_pilot_harness_allowed_by_grant": True,
        "authorization.prod_scoped_shadow_live_execution_authorized": False,
        "authorization.prod_scoped_shadow_execution_authorized": False,
        "posture.online_shadow_execution_enabled": False,
        "posture.production_default_allowed": False,
        "posture.api_web_changes_allowed": False,
        "posture.user_visible_ranking_changed": False,
        "posture.prod_scoped_shadow_pilot_executed": False,
        "recommended_next_stage": POST_PILOT_HARNESS_NEXT_STAGE,
    }
    for path, expected in required.items():
        observed = bundle.get(path) if "." not in path else _get(bundle, path)
        if observed != expected:
            raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError(
                f"{path} must be {expected!r}, got {observed!r}"
            )
    if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError("pilot harness review has already been filed")
    if not isinstance(_get(bundle, "execution.pilot_harness"), Mapping):
        raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError("execution.pilot_harness must be an object")


def _forbidden_write_counts_zero(harness: Mapping[str, Any]) -> bool:
    counts = _get(harness, "write_count_verification.write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(count == 0 for target, count in counts.items() if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)


def evaluate_production_scoped_shadow_pilot_harness_review_checks(
    bundle: Mapping[str, Any],
) -> dict[str, bool]:
    harness = _get(bundle, "execution.pilot_harness")
    if not isinstance(harness, Mapping):
        return {check: False for check in PILOT_HARNESS_REVIEW_CHECKS}
    runtime_drill = harness.get("runtime_drill") if isinstance(harness.get("runtime_drill"), Mapping) else {}
    pass_fail = harness.get("pass_fail_evaluation") if isinstance(harness.get("pass_fail_evaluation"), Mapping) else {}
    write_counts = (
        harness.get("write_count_verification")
        if isinstance(harness.get("write_count_verification"), Mapping)
        else {}
    )
    files = harness.get("files_written") if isinstance(harness.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    return {
        "runtime_drill_pilot_status_succeeded_test_only": _get(runtime_drill, "pilot.status") == "succeeded_test_only",
        "fixture_row_count_3": harness.get("fixture_row_count") == 3,
        "runtime_drill_call_order": runtime_drill.get("call_order")
        == ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        "environment_restored": runtime_drill.get("environment_restored") is True,
        "forbidden_write_counts_zero": write_counts.get("forbidden_write_counts_zero") is True
        and _forbidden_write_counts_zero(harness),
        "isolated_artifact_count_4": _get(write_counts, "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)
        == 4,
        "expected_files_recorded": observed_files == set(PILOT_HARNESS_EXPECTED_FILES),
        "runtime_writes_false": _get(runtime_drill, "pilot.writes_performed") is False
        and write_counts.get("runtime_writes_performed") is False,
        "live_prod_source_reads_false": harness.get("live_prod_source_reads_performed") is False
        and _get(harness, "observability_summary.live_prod_source_reads_performed") is False
        and write_counts.get("live_prod_source_reads_performed") is False,
        "pilot_surface_bounded_fixture": harness.get("pilot_surface") == PILOT_HARNESS_SURFACE,
        "actual_pilot_executed_false": _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is False
        and _get(bundle, "posture.prod_scoped_shadow_pilot_executed") is False,
        "production_api_user_visible_unchanged": _get(bundle, "posture.production_default_allowed") is False
        and _get(bundle, "posture.api_web_changes_allowed") is False
        and _get(bundle, "posture.user_visible_ranking_changed") is False
        and _get(runtime_drill, "pilot.production_default_changed") is False
        and _get(runtime_drill, "pilot.user_visible_ranking_changed") is False,
        "labels_not_used": _get(runtime_drill, "pilot.labels_used_for_scoring") is False
        and _get(pass_fail, "checks.labels_used_for_scoring_false") is True,
        "pass_fail_overall_passed": pass_fail.get("overall_passed") is True,
        "pass_fail_failed_checks_empty": pass_fail.get("failed_checks") == [],
    }


def build_production_scoped_shadow_pilot_harness_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError("reviewer must be populated")
    _require_revision_five_harness(bundle)
    checks = evaluate_production_scoped_shadow_pilot_harness_review_checks(bundle)
    failed = sorted(check for check in PILOT_HARNESS_REVIEW_CHECKS if checks.get(check) is not True)
    accepted = not failed
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "runtime drill succeeded in test-only harness context",
            "preflight and postflight remained disabled",
            "isolated prod-scoped artifact target count was four",
            "forbidden production write counts were zero",
            "observability and flag restoration evidence were recorded",
        ],
        "limitations": [
            "not live production traffic",
            "no live prod source reads were reviewed",
            "no runtime rerun was performed",
            "no shadow-runs artifact reads or writes were performed",
            "actual production-scoped pilot remains unexecuted",
        ],
    }
    return {
        "prod_scoped_shadow_pilot_harness_reviewed": True,
        "prod_scoped_shadow_pilot_harness_accepted": accepted,
        "review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
    *,
    bundle_path: Path,
    reviewer: str,
    review_notes: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    try:
        review_slice = build_production_scoped_shadow_pilot_harness_review_slice(
            payload,
            reviewer=reviewer,
            review_notes=review_notes,
            reviewed_at=reviewed_at,
        )
        updated = apply_production_scoped_shadow_pilot_harness_review(
            payload,
            review_slice,
            generated_at=review_slice["review_decision"]["reviewed_at"],
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_pilot_harness_review_filed=True,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowPilotHarnessReviewError(str(exc)) from exc
    if update_bundle:
        bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
            encoding="utf-8",
        )
    return {
        "pilot_harness_reviewed": True,
        "pilot_harness_accepted": review_slice["prod_scoped_shadow_pilot_harness_accepted"],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": update_bundle,
        "recommended_next_stage": updated["recommended_next_stage"],
    }
