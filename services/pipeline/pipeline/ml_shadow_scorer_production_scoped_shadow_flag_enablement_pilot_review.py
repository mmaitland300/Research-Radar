"""Review recorded flag enablement production-scoped pilot evidence.

This module is paperwork-only: it evaluates the rev 19 bundle slice and does
not rerun runtime, connect to databases, or read/write shadow-runs artifacts.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES,
    FLAG_ENABLEMENT_PILOT_RUN_RANKING_VERSION,
    FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS,
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_FLAG_ENABLEMENT_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION,
    POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE,
    _verify_flag_enablement_grant_section,
    _verify_flag_enablement_request_section,
    _verify_live_execution_grant_section,
    _verify_live_execution_request_section,
    _verify_live_read_only_grant_section,
    _verify_live_read_only_request_section,
    apply_production_scoped_shadow_flag_enablement_pilot_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root


class MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            f"Expected JSON object in {path}"
        )
    return payload


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _authorization_section_valid(authorization: Mapping[str, Any], verifier: Any) -> bool:
    try:
        verifier(authorization)
        return True
    except MLShadowScorerProductionScopedShadowBundleError:
        return False


def _forbidden_write_counts_zero(flag_run: Mapping[str, Any]) -> bool:
    write_counts = flag_run.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        return False
    if write_counts.get("forbidden_write_counts_zero") is not True:
        return False
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(counts.get(target, 0) == 0 for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS)


def _require_revision_nineteen_flag_enablement_pilot(bundle: Mapping[str, Any]) -> None:
    required = {
        "metadata.bundle_revision": POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION,
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed": True,
        "execution.prod_scoped_shadow_flag_enablement_pilot_passed": True,
        "execution.flag_enablement_pilot_run.pass_fail_evaluation.overall_passed": True,
        "execution.flag_enablement_pilot_run.pass_fail_evaluation.failed_checks": [],
        "execution.prod_scoped_shadow_live_read_only_pilot_executed": True,
        "execution.prod_scoped_shadow_live_read_only_pilot_passed": True,
        "execution.prod_scoped_shadow_live_execution_pilot_executed": True,
        "execution.prod_scoped_shadow_live_execution_pilot_passed": True,
        "posture.prod_scoped_shadow_flag_enablement_pilot_executed": True,
        "posture.prod_scoped_shadow_flag_enablement_pilot_passed": True,
        "posture.prod_scoped_shadow_flag_enablement_authorized": True,
        "posture.prod_scoped_shadow_live_execution_authorized": True,
        "posture.prod_scoped_shadow_execution_authorized": False,
        "posture.prod_scoped_shadow_live_read_only_execution_authorized": True,
        "posture.live_prod_source_reads_performed": True,
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization": False,
        "posture.missing_prod_scoped_shadow_live_execution_authorization": False,
        "posture.online_shadow_execution_enabled": False,
        "posture.production_default_allowed": False,
        "posture.api_web_changes_allowed": False,
        "posture.user_visible_ranking_changed": False,
        "posture.writes_performed": False,
        "posture.runtime_writes_performed": False,
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested": True,
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted": True,
        "authorization.prod_scoped_shadow_flag_enablement_authorized": True,
        "authorization.prod_scoped_shadow_live_execution_authorized": True,
        "authorization.prod_scoped_shadow_execution_authorized": False,
        "recommended_next_stage": POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE,
    }
    for path, expected in required.items():
        observed = bundle.get(path) if "." not in path else _get(bundle, path)
        if observed != expected:
            raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
                f"{path} must be {expected!r}, got {observed!r}"
            )
    if _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "flag enablement pilot review has already been filed"
        )
    if _get(bundle, "review.flag_enablement_pilot_review_decision") is not None:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "review.flag_enablement_pilot_review_decision must not already exist"
        )
    flag_enablement_pilot_run = _get(bundle, "execution.flag_enablement_pilot_run")
    if not isinstance(flag_enablement_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "execution.flag_enablement_pilot_run must be an object"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "authorization must be an object"
        )
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError("review must be an object")
    for verifier in (
        _verify_live_read_only_request_section,
        _verify_live_read_only_grant_section,
        _verify_live_execution_request_section,
        _verify_live_execution_grant_section,
        _verify_flag_enablement_request_section,
        _verify_flag_enablement_grant_section,
    ):
        try:
            verifier(authorization)
        except MLShadowScorerProductionScopedShadowBundleError as exc:
            raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(str(exc)) from exc
    if _get(review, "prod_scoped_shadow_live_read_only_pilot_reviewed") is not True:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "review.prod_scoped_shadow_live_read_only_pilot_reviewed must be true"
        )
    if _get(review, "live_read_only_pilot_review_decision.decision") != "accepted":
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "review.live_read_only_pilot_review_decision.decision must be accepted"
        )
    if _get(review, "prod_scoped_shadow_live_execution_pilot_reviewed") is not True:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "review.prod_scoped_shadow_live_execution_pilot_reviewed must be true"
        )
    if _get(review, "live_execution_pilot_review_decision.decision") != "accepted":
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "review.live_execution_pilot_review_decision.decision must be accepted"
        )


def evaluate_production_scoped_shadow_flag_enablement_pilot_review_checks(
    bundle: Mapping[str, Any],
) -> dict[str, bool]:
    flag_run = _get(bundle, "execution.flag_enablement_pilot_run")
    if not isinstance(flag_run, Mapping):
        return {check: False for check in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS}
    runtime_drill = flag_run.get("runtime_drill") if isinstance(flag_run.get("runtime_drill"), Mapping) else {}
    pass_fail = (
        flag_run.get("pass_fail_evaluation")
        if isinstance(flag_run.get("pass_fail_evaluation"), Mapping)
        else {}
    )
    write_counts = (
        flag_run.get("write_count_verification")
        if isinstance(flag_run.get("write_count_verification"), Mapping)
        else {}
    )
    incomplete = (
        flag_run.get("incomplete_coverage_drill")
        if isinstance(flag_run.get("incomplete_coverage_drill"), Mapping)
        else {}
    )
    files = flag_run.get("files_written") if isinstance(flag_run.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    authorization = bundle.get("authorization") if isinstance(bundle.get("authorization"), Mapping) else {}
    review = bundle.get("review") if isinstance(bundle.get("review"), Mapping) else {}
    provenance = flag_run.get("input_provenance") if isinstance(flag_run.get("input_provenance"), Mapping) else {}
    pass_fail_checks = pass_fail.get("checks") if isinstance(pass_fail.get("checks"), Mapping) else {}
    blockers = (
        bundle.get("shadow_and_production_blockers")
        if isinstance(bundle.get("shadow_and_production_blockers"), Mapping)
        else {}
    )
    return {
        "flag_enablement_pilot_run_pass_fail_overall_passed": pass_fail.get("overall_passed") is True
        and pass_fail.get("failed_checks") == [],
        "joined_candidate_count_528": _get(flag_run, "input_join_summary.joined_candidate_count") == 528,
        "runtime_row_count_528": _get(runtime_drill, "pilot.shadow_row_count") == 528,
        "runtime_drill_call_order": runtime_drill.get("call_order")
        == ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        "preflight_postflight_disabled": _get(runtime_drill, "preflight.status") == "skipped_runtime_disabled"
        and _get(runtime_drill, "postflight.status") == "skipped_runtime_disabled",
        "pilot_status_succeeded_test_only": _get(runtime_drill, "pilot.status") == "succeeded_test_only",
        "process_scoped_runtime_flag_only": runtime_drill.get("process_scoped_runtime_flag_only") is True,
        "runtime_flag_enabled_only_during_pilot": _get(runtime_drill, "preflight.runtime_enabled") is False
        and _get(runtime_drill, "pilot.runtime_enabled") is True
        and _get(runtime_drill, "postflight.runtime_enabled") is False
        and runtime_drill.get("environment_restored") is True,
        "environment_restored": runtime_drill.get("environment_restored") is True,
        "incomplete_coverage_skip_verified": incomplete.get("status") == "skipped_incomplete_coverage"
        and incomplete.get("shadow_row_count") == 0
        and incomplete.get("writes_performed") is False
        and incomplete.get("live_prod_source_reads_performed") is False,
        "forbidden_write_counts_zero": _forbidden_write_counts_zero(flag_run),
        "isolated_artifact_count_4": _get(
            write_counts,
            "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        == 4,
        "expected_files_recorded": observed_files == set(FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES),
        "flag_enablement_grant_slices_present": isinstance(authorization, Mapping)
        and _authorization_section_valid(authorization, _verify_flag_enablement_grant_section),
        "live_execution_chain_still_valid": isinstance(authorization, Mapping)
        and _authorization_section_valid(authorization, _verify_live_execution_request_section)
        and _authorization_section_valid(authorization, _verify_live_execution_grant_section)
        and review.get("prod_scoped_shadow_live_execution_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_live_execution_pilot_accepted") is True
        and _get(review, "live_execution_pilot_review_decision.decision") == "accepted"
        and authorization.get("prod_scoped_shadow_live_execution_authorized") is True,
        "production_api_user_visible_unchanged": _get(bundle, "posture.production_default_allowed") is False
        and _get(bundle, "posture.api_web_changes_allowed") is False
        and _get(bundle, "posture.user_visible_ranking_changed") is False
        and blockers.get("production_default_allowed") is False
        and blockers.get("api_web_changes_allowed") is False
        and blockers.get("user_visible_ranking_changed") is False,
        "global_execution_authorization_false": _get(
            bundle,
            "authorization.prod_scoped_shadow_execution_authorized",
        )
        is False
        and _get(bundle, "posture.prod_scoped_shadow_execution_authorized") is False,
        "plan_flag_authorized_now_false": _get(
            bundle,
            "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        )
        is False
        and _get(bundle, "posture.online_shadow_execution_enabled") is False,
        "no_labels_refit_embedding_generation_or_label_ingest": pass_fail_checks.get(
            "no_labels_refit_embedding_generation_or_label_ingest"
        )
        is True,
        "ranking_version_not_test_fixture": provenance.get("fixture_ranking_version_used") is False
        and provenance.get("ranking_version") == FLAG_ENABLEMENT_PILOT_RUN_RANKING_VERSION,
    }


def build_production_scoped_shadow_flag_enablement_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError("reviewer must be populated")
    _require_revision_nineteen_flag_enablement_pilot(bundle)
    checks = evaluate_production_scoped_shadow_flag_enablement_pilot_review_checks(bundle)
    failed = sorted(check for check in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS if checks.get(check) is not True)
    accepted = not failed
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "recorded rev 19 flag enablement pilot passed all review checks",
            "bounded flag-enablement pilot stayed process-scoped with runtime flag restored afterward",
            "forbidden production write counts were zero",
            "flag enablement authorization grant slices remained present",
            "live execution authorization chain remained valid",
            "production default, API/web, and user-visible behavior remained unchanged",
            "global online shadow execution remained unauthorized",
        ],
        "limitations": [
            "flag enablement pilot review evaluates recorded rev 19 flag enablement pilot evidence only",
            "no runtime rerun was performed",
            "no database connection was opened by the review",
            "no shadow-runs artifact reads or writes were performed",
            "global/fleet online shadow execution remains unauthorized",
            "accepted review clears only the flag-enablement pilot evidence gate; it does not authorize production default, API/web, or user-visible output",
        ],
    }
    return {
        "prod_scoped_shadow_flag_enablement_pilot_reviewed": True,
        "prod_scoped_shadow_flag_enablement_pilot_accepted": accepted,
        "flag_enablement_pilot_review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot(
    *,
    bundle_path: Path,
    reviewer: str,
    review_notes: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            payload,
            repo_root=root,
            expect_flag_enablement_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(str(exc)) from exc
    execution_before = deepcopy(payload["execution"])
    authorization_before = deepcopy(payload["authorization"])
    review_slice = build_production_scoped_shadow_flag_enablement_pilot_review_slice(
        payload,
        reviewer=reviewer,
        review_notes=review_notes,
        reviewed_at=generated_at,
    )
    try:
        updated = apply_production_scoped_shadow_flag_enablement_pilot_review(
            payload,
            review_slice,
            generated_at=_get(review_slice, "flag_enablement_pilot_review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_flag_enablement_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(str(exc)) from exc
    if updated["execution"] != execution_before:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "flag enablement pilot review must not modify bundle.execution"
        )
    if updated["authorization"] != authorization_before:
        raise MLShadowScorerProductionScopedShadowFlagEnablementPilotReviewError(
            "flag enablement pilot review must not modify bundle.authorization"
        )
    if update_bundle:
        bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
            encoding="utf-8",
        )
    return {
        "flag_enablement_pilot_reviewed": True,
        "flag_enablement_pilot_accepted": review_slice["prod_scoped_shadow_flag_enablement_pilot_accepted"],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": update_bundle,
        "recommended_next_stage": (
            POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["prod_scoped_shadow_flag_enablement_pilot_accepted"]
            else POST_FLAG_ENABLEMENT_PILOT_REVIEW_REJECTED_NEXT_STAGE
        ),
    }
