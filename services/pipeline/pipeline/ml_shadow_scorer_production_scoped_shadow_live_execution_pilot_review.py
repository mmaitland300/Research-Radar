"""Review recorded live execution production-scoped pilot evidence.

This module is paperwork-only: it evaluates the rev 15 bundle slice and does
not rerun runtime, connect to databases, or read/write shadow-runs artifacts.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    LIVE_EXECUTION_PILOT_RUN_EXPECTED_FILES,
    LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION,
    LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION,
    POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE,
    _verify_live_execution_grant_section,
    _verify_live_execution_request_section,
    _verify_live_read_only_grant_section,
    _verify_live_read_only_request_section,
    apply_production_scoped_shadow_live_execution_pilot_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root


class MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
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


def _forbidden_write_counts_zero(live_run: Mapping[str, Any]) -> bool:
    write_counts = live_run.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        return False
    if write_counts.get("forbidden_write_counts_zero") is not True:
        return False
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(counts.get(target, 0) == 0 for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS)


def _require_revision_fifteen_live_execution_pilot(bundle: Mapping[str, Any]) -> None:
    required = {
        "metadata.bundle_revision": POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION,
        "execution.prod_scoped_shadow_live_execution_pilot_executed": True,
        "execution.prod_scoped_shadow_live_execution_pilot_passed": True,
        "execution.live_execution_pilot_run.pass_fail_evaluation.overall_passed": True,
        "execution.live_execution_pilot_run.pass_fail_evaluation.failed_checks": [],
        "posture.prod_scoped_shadow_live_execution_pilot_executed": True,
        "posture.prod_scoped_shadow_live_execution_pilot_passed": True,
        "posture.prod_scoped_shadow_live_execution_authorized": True,
        "posture.prod_scoped_shadow_execution_authorized": False,
        "posture.prod_scoped_shadow_live_read_only_execution_authorized": True,
        "posture.live_prod_source_reads_performed": True,
        "posture.missing_prod_scoped_shadow_live_execution_authorization": False,
        "posture.online_shadow_execution_enabled": False,
        "posture.production_default_allowed": False,
        "posture.api_web_changes_allowed": False,
        "posture.user_visible_ranking_changed": False,
        "posture.writes_performed": False,
        "posture.runtime_writes_performed": False,
        "authorization.prod_scoped_shadow_live_execution_authorization_requested": True,
        "authorization.prod_scoped_shadow_live_execution_authorization_granted": True,
        "authorization.prod_scoped_shadow_live_execution_authorized": True,
        "authorization.prod_scoped_shadow_execution_authorized": False,
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized": True,
        "recommended_next_stage": POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE,
    }
    for path, expected in required.items():
        observed = bundle.get(path) if "." not in path else _get(bundle, path)
        if observed != expected:
            raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
                f"{path} must be {expected!r}, got {observed!r}"
            )
    if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
            "live execution pilot review has already been filed"
        )
    live_execution_pilot_run = _get(bundle, "execution.live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
            "execution.live_execution_pilot_run must be an object"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
            "authorization must be an object"
        )
    for verifier in (
        _verify_live_execution_request_section,
        _verify_live_execution_grant_section,
        _verify_live_read_only_request_section,
        _verify_live_read_only_grant_section,
    ):
        try:
            verifier(authorization)
        except MLShadowScorerProductionScopedShadowBundleError as exc:
            raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(str(exc)) from exc


def evaluate_production_scoped_shadow_live_execution_pilot_review_checks(
    bundle: Mapping[str, Any],
) -> dict[str, bool]:
    live_run = _get(bundle, "execution.live_execution_pilot_run")
    if not isinstance(live_run, Mapping):
        return {check: False for check in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS}
    runtime_drill = live_run.get("runtime_drill") if isinstance(live_run.get("runtime_drill"), Mapping) else {}
    pass_fail = (
        live_run.get("pass_fail_evaluation")
        if isinstance(live_run.get("pass_fail_evaluation"), Mapping)
        else {}
    )
    write_counts = (
        live_run.get("write_count_verification")
        if isinstance(live_run.get("write_count_verification"), Mapping)
        else {}
    )
    incomplete = (
        live_run.get("incomplete_coverage_drill")
        if isinstance(live_run.get("incomplete_coverage_drill"), Mapping)
        else {}
    )
    live_source_reads = live_run.get("live_source_reads")
    files = live_run.get("files_written") if isinstance(live_run.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    authorization = bundle.get("authorization") if isinstance(bundle.get("authorization"), Mapping) else {}
    provenance = live_run.get("input_provenance") if isinstance(live_run.get("input_provenance"), Mapping) else {}
    return {
        "live_execution_pilot_run_pass_fail_overall_passed": pass_fail.get("overall_passed") is True
        and pass_fail.get("failed_checks") == [],
        "joined_candidate_count_528": _get(live_run, "input_join_summary.joined_candidate_count") == 528,
        "runtime_row_count_528": _get(live_run, "input_join_summary.runtime_row_count") == 528
        and _get(runtime_drill, "pilot.shadow_row_count") == 528,
        "runtime_drill_call_order": runtime_drill.get("call_order")
        == ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        "preflight_postflight_disabled": _get(runtime_drill, "preflight.status") == "skipped_runtime_disabled"
        and _get(runtime_drill, "postflight.status") == "skipped_runtime_disabled"
        and _get(runtime_drill, "preflight.shadow_row_count") == 0
        and _get(runtime_drill, "postflight.shadow_row_count") == 0,
        "pilot_status_succeeded_test_only": _get(runtime_drill, "pilot.status") == "succeeded_test_only",
        "process_scoped_runtime_flag_only": runtime_drill.get("process_scoped_runtime_flag_only") is True,
        "environment_restored": runtime_drill.get("environment_restored") is True,
        "incomplete_coverage_skip_verified": incomplete.get("status") == "skipped_incomplete_coverage"
        and incomplete.get("shadow_row_count") == 0
        and incomplete.get("writes_performed") is False,
        "forbidden_write_counts_zero": _forbidden_write_counts_zero(live_run),
        "isolated_artifact_count_4": _get(
            write_counts,
            "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        == 4,
        "expected_files_recorded": observed_files == set(LIVE_EXECUTION_PILOT_RUN_EXPECTED_FILES),
        "live_execution_grant_slices_present": isinstance(authorization, Mapping)
        and _authorization_section_valid(authorization, _verify_live_execution_grant_section),
        "live_read_only_chain_still_valid": _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed")
        is True
        and _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_accepted") is True
        and _get(bundle, "review.live_read_only_pilot_review_decision.decision") == "accepted"
        and isinstance(authorization, Mapping)
        and _authorization_section_valid(authorization, _verify_live_read_only_request_section)
        and _authorization_section_valid(authorization, _verify_live_read_only_grant_section)
        and authorization.get("prod_scoped_shadow_live_read_only_execution_authorized") is True
        and _get(bundle, "execution.pilot_harness.live_prod_source_reads_performed") is False
        and _get(bundle, "execution.pilot_run.live_prod_source_reads_performed") is False,
        "production_api_user_visible_unchanged": _get(bundle, "posture.production_default_allowed") is False
        and _get(bundle, "posture.api_web_changes_allowed") is False
        and _get(bundle, "posture.user_visible_ranking_changed") is False
        and _get(runtime_drill, "pilot.production_default_changed") is False
        and _get(runtime_drill, "pilot.user_visible_ranking_changed") is False,
        "global_execution_authorization_false": _get(
            bundle,
            "authorization.prod_scoped_shadow_execution_authorized",
        )
        is False
        and _get(bundle, "posture.prod_scoped_shadow_execution_authorized") is False
        and _get(bundle, "posture.online_shadow_execution_enabled") is False,
        "no_labels_refit_embedding_generation_or_label_ingest": isinstance(live_source_reads, Mapping)
        and live_source_reads.get("labels_not_used_for_scoring") is True
        and live_source_reads.get("refit_training_performed") is False
        and live_source_reads.get("embedding_generation_performed") is False
        and live_source_reads.get("label_ingest_performed") is False
        and _get(runtime_drill, "pilot.labels_used_for_scoring") is False,
        "ranking_version_not_test_fixture": provenance.get("fixture_ranking_version_used") is False
        and provenance.get("ranking_version") == LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION,
    }


def build_production_scoped_shadow_live_execution_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError("reviewer must be populated")
    _require_revision_fifteen_live_execution_pilot(bundle)
    checks = evaluate_production_scoped_shadow_live_execution_pilot_review_checks(bundle)
    failed = sorted(check for check in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS if checks.get(check) is not True)
    accepted = not failed
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "recorded rev 15 live execution pilot passed all review checks",
            "bounded live execution pilot stayed process-scoped with runtime flag restored afterward",
            "forbidden production write counts were zero",
            "live read-only authorization chain remained valid",
            "production default, API/web, and user-visible behavior remained unchanged",
            "global online shadow execution remained unauthorized",
        ],
        "limitations": [
            "live execution pilot review evaluates recorded rev 15 evidence only",
            "no runtime rerun was performed",
            "no database connection was opened by the review",
            "no shadow-runs artifact reads or writes were performed",
            "global online shadow execution remains unauthorized",
            "accepted review clears only the live execution pilot evidence gate",
        ],
    }
    return {
        "prod_scoped_shadow_live_execution_pilot_reviewed": True,
        "prod_scoped_shadow_live_execution_pilot_accepted": accepted,
        "live_execution_pilot_review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
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
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            payload,
            repo_root=root,
            expect_live_execution_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(str(exc)) from exc
    execution_before = deepcopy(payload["execution"])
    authorization_before = deepcopy(payload["authorization"])
    review_slice = build_production_scoped_shadow_live_execution_pilot_review_slice(
        payload,
        reviewer=reviewer,
        review_notes=review_notes,
        reviewed_at=reviewed_at,
    )
    try:
        updated = apply_production_scoped_shadow_live_execution_pilot_review(
            payload,
            review_slice,
            generated_at=_get(review_slice, "live_execution_pilot_review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_live_execution_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(str(exc)) from exc
    if updated["execution"] != execution_before:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
            "live execution pilot review must not modify bundle.execution"
        )
    if updated["authorization"] != authorization_before:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotReviewError(
            "live execution pilot review must not modify bundle.authorization"
        )
    if update_bundle:
        bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
            encoding="utf-8",
        )
    return {
        "live_execution_pilot_reviewed": True,
        "live_execution_pilot_accepted": review_slice["prod_scoped_shadow_live_execution_pilot_accepted"],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": update_bundle,
        "recommended_next_stage": (
            POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["prod_scoped_shadow_live_execution_pilot_accepted"]
            else POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE
        ),
    }
