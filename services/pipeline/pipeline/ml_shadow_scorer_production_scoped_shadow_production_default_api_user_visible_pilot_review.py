"""Review recorded production default/API/user-visible production-scoped pilot evidence.

This module is paperwork-only: it evaluates the rev 23 bundle slice and does
not rerun runtime, connect to databases, read/write shadow-runs artifacts, call
API routes, or bind HTTP.
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
    MLShadowScorerProductionScopedShadowBundleError,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_BUNDLE_REVISION,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_RANKING_VERSION,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_REVIEW_CHECKS,
    _verify_flag_enablement_grant_section,
    _verify_flag_enablement_pilot_review_section,
    _verify_flag_enablement_request_section,
    _verify_live_execution_grant_section,
    _verify_live_execution_pilot_review_section,
    _verify_live_execution_request_section,
    _verify_live_read_only_grant_section,
    _verify_live_read_only_pilot_review_section,
    _verify_live_read_only_request_section,
    _verify_production_default_api_user_visible_grant_section,
    _verify_production_default_api_user_visible_request_section,
    apply_production_scoped_shadow_production_default_api_user_visible_pilot_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root


class MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
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

def _forbidden_write_counts_zero(prod_run: Mapping[str, Any]) -> bool:
    write_counts = prod_run.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        return False
    if write_counts.get("forbidden_write_counts_zero") is not True:
        return False
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(counts.get(target, 0) == 0 for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS)


def _require_revision_twenty_three_production_default_pilot(bundle: Mapping[str, Any]) -> None:
    required = {
        "metadata.bundle_revision": POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_BUNDLE_REVISION,
        "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_executed": True,
        "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_passed": True,
        "execution.production_default_api_user_visible_pilot_run.pass_fail_evaluation.overall_passed": True,
        "execution.production_default_api_user_visible_pilot_run.pass_fail_evaluation.failed_checks": [],
        "posture.prod_scoped_shadow_production_default_api_user_visible_pilot_executed": True,
        "posture.prod_scoped_shadow_production_default_api_user_visible_pilot_passed": True,
        "posture.prod_scoped_shadow_production_default_api_user_visible_authorized": True,
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
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested": True,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted": True,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized": True,
        "authorization.prod_scoped_shadow_flag_enablement_authorized": True,
        "authorization.prod_scoped_shadow_live_execution_authorized": True,
        "authorization.prod_scoped_shadow_execution_authorized": False,
        "recommended_next_stage": POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE,
    }
    for path, expected in required.items():
        observed = bundle.get(path) if "." not in path else _get(bundle, path)
        if observed != expected:
            raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
                f"{path} must be {expected!r}, got {observed!r}"
            )
    if _get(bundle, "review.prod_scoped_shadow_production_default_api_user_visible_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "production default/API/user-visible pilot review has already been filed"
        )
    if _get(bundle, "review.production_default_api_user_visible_pilot_review_decision") is not None:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "review.production_default_api_user_visible_pilot_review_decision must not already exist"
        )
    prod_run = _get(bundle, "execution.production_default_api_user_visible_pilot_run")
    if not isinstance(prod_run, Mapping):
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "execution.production_default_api_user_visible_pilot_run must be an object"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "authorization must be an object"
        )
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "review must be an object"
        )
    for verifier in (
        _verify_live_read_only_request_section,
        _verify_live_read_only_grant_section,
        _verify_live_execution_request_section,
        _verify_live_execution_grant_section,
        _verify_flag_enablement_request_section,
        _verify_flag_enablement_grant_section,
        _verify_production_default_api_user_visible_request_section,
        _verify_production_default_api_user_visible_grant_section,
    ):
        try:
            verifier(authorization)
        except MLShadowScorerProductionScopedShadowBundleError as exc:
            raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(str(exc)) from exc
    for verifier in (
        _verify_live_read_only_pilot_review_section,
        _verify_live_execution_pilot_review_section,
        _verify_flag_enablement_pilot_review_section,
    ):
        try:
            verifier(review)
        except MLShadowScorerProductionScopedShadowBundleError as exc:
            raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(str(exc)) from exc


def evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks(
    bundle: Mapping[str, Any],
) -> dict[str, bool]:
    prod_run = _get(bundle, "execution.production_default_api_user_visible_pilot_run")
    if not isinstance(prod_run, Mapping):
        return {check: False for check in PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_REVIEW_CHECKS}
    runtime_drill = prod_run.get("runtime_drill") if isinstance(prod_run.get("runtime_drill"), Mapping) else {}
    pass_fail = (
        prod_run.get("pass_fail_evaluation")
        if isinstance(prod_run.get("pass_fail_evaluation"), Mapping)
        else {}
    )
    write_counts = (
        prod_run.get("write_count_verification")
        if isinstance(prod_run.get("write_count_verification"), Mapping)
        else {}
    )
    incomplete = (
        prod_run.get("incomplete_coverage_drill")
        if isinstance(prod_run.get("incomplete_coverage_drill"), Mapping)
        else {}
    )
    probe = (
        prod_run.get("production_default_api_user_visible_probe")
        if isinstance(prod_run.get("production_default_api_user_visible_probe"), Mapping)
        else {}
    )
    scope = (
        prod_run.get("production_default_api_user_visible_scope")
        if isinstance(prod_run.get("production_default_api_user_visible_scope"), Mapping)
        else {}
    )
    granted_scope = scope.get("production_default_api_user_visible_granted_scope")
    files = prod_run.get("files_written") if isinstance(prod_run.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    provenance = prod_run.get("input_provenance") if isinstance(prod_run.get("input_provenance"), Mapping) else {}
    pass_fail_checks = pass_fail.get("checks") if isinstance(pass_fail.get("checks"), Mapping) else {}
    authorization = bundle.get("authorization") if isinstance(bundle.get("authorization"), Mapping) else {}
    review = bundle.get("review") if isinstance(bundle.get("review"), Mapping) else {}
    return {
        "production_default_api_user_visible_pilot_run_pass_fail_overall_passed": pass_fail.get("overall_passed") is True
        and pass_fail.get("failed_checks") == [],
        "joined_candidate_count_528": _get(prod_run, "input_join_summary.joined_candidate_count") == 528,
        "runtime_row_count_528": _get(prod_run, "input_join_summary.runtime_row_count") == 528,
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
        "approved_source_reread_verified": provenance.get("reread_approved_production_sources") is True,
        "ranking_version_not_test_fixture": provenance.get("fixture_ranking_version_used") is False
        and provenance.get("ranking_version") == PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_RANKING_VERSION,
        "bounded_api_surface_probe_performed": probe.get("current_output_read_only_probe_performed") is True
        and probe.get("in_process_audit_only_probe") is True,
        "would_be_shadow_scorer_output_built": probe.get("would_be_shadow_scorer_output_built") is True,
        "no_public_user_traffic": probe.get("user_visible_response_emitted_to_users") is False,
        "no_http_server_bind_or_outbound_api_route": probe.get("http_server_bound") is False
        and probe.get("outbound_api_route_called") is False,
        "production_default_api_user_visible_changed_false": probe.get("production_default_changed") is False
        and probe.get("api_web_changed") is False
        and probe.get("user_visible_ranking_changed") is False
        and scope.get("production_default_allowed") is False
        and scope.get("api_web_changes_allowed") is False
        and scope.get("user_visible_ranking_changed") is False,
        "paper_scores_and_ranking_runs_not_written": probe.get("paper_scores_written") is False
        and probe.get("ranking_runs_written") is False
        and probe.get("production_config_written") is False,
        "forbidden_write_counts_zero": _forbidden_write_counts_zero(prod_run),
        "isolated_artifact_count_expected": _get(
            write_counts,
            "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        == 4,
        "expected_files_recorded": observed_files == set(PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES),
        "production_default_api_user_visible_grant_slices_present": isinstance(granted_scope, Mapping)
        and granted_scope.get("authorization_scope") == PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE
        and isinstance(scope.get("production_default_api_user_visible_grant_decision"), Mapping),
        "upstream_live_flag_read_only_chain_still_valid": isinstance(authorization, Mapping)
        and _authorization_section_valid(authorization, _verify_live_read_only_request_section)
        and _authorization_section_valid(authorization, _verify_live_read_only_grant_section)
        and _authorization_section_valid(authorization, _verify_live_execution_request_section)
        and _authorization_section_valid(authorization, _verify_live_execution_grant_section)
        and _authorization_section_valid(authorization, _verify_flag_enablement_request_section)
        and _authorization_section_valid(authorization, _verify_flag_enablement_grant_section)
        and review.get("prod_scoped_shadow_live_read_only_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_live_read_only_pilot_accepted") is True
        and _get(review, "live_read_only_pilot_review_decision.decision") == "accepted"
        and review.get("prod_scoped_shadow_live_execution_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_live_execution_pilot_accepted") is True
        and _get(review, "live_execution_pilot_review_decision.decision") == "accepted"
        and review.get("prod_scoped_shadow_flag_enablement_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_flag_enablement_pilot_accepted") is True
        and _get(review, "flag_enablement_pilot_review_decision.decision") == "accepted"
        and authorization.get("prod_scoped_shadow_live_read_only_execution_authorized") is True
        and authorization.get("prod_scoped_shadow_live_execution_authorized") is True
        and authorization.get("prod_scoped_shadow_flag_enablement_authorized") is True,
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
        "bridge_surface_not_included": probe.get("bridge_surface_included") is False,
        "no_labels_refit_embedding_generation_or_label_ingest": pass_fail_checks.get(
            "no_labels_refit_embedding_generation_or_label_ingest"
        )
        is True,
    }


def build_production_scoped_shadow_production_default_api_user_visible_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "reviewer must be populated"
        )
    _require_revision_twenty_three_production_default_pilot(bundle)
    checks = evaluate_production_scoped_shadow_production_default_api_user_visible_pilot_review_checks(bundle)
    failed = sorted(
        check for check in PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_REVIEW_CHECKS if checks.get(check) is not True
    )
    accepted = not failed
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "recorded rev 23 production default/API/user-visible pilot passed all review checks",
            "bounded production default/API/user-visible pilot stayed process-scoped with runtime flag restored afterward",
            "forbidden production write counts were zero",
            "production default/API/user-visible authorization grant slices remained present in recorded pilot evidence",
            "upstream live read-only, live execution, and flag enablement authorization chain remained valid",
            "production default, API/web, and user-visible behavior remained unchanged",
            "global online shadow execution remained unauthorized",
            "bridge recommendations remained out of scope",
        ],
        "limitations": [
            "production default/API/user-visible pilot review evaluates recorded rev 23 pilot evidence only",
            "no runtime rerun was performed",
            "no database connection was opened by the review",
            "no shadow-runs artifact reads or writes were performed",
            "no API routes were called and no HTTP server was bound",
            "no live production recommendation/output enablement was performed",
            "production default, API/web, and user-visible ranking remained unchanged",
            "bridge recommendations remain out of scope",
        ],
    }
    return {
        "prod_scoped_shadow_production_default_api_user_visible_pilot_reviewed": True,
        "prod_scoped_shadow_production_default_api_user_visible_pilot_accepted": accepted,
        "production_default_api_user_visible_pilot_review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
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
            expect_production_default_api_user_visible_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(str(exc)) from exc
    execution_before = deepcopy(payload["execution"])
    authorization_before = deepcopy(payload["authorization"])
    review_slice = build_production_scoped_shadow_production_default_api_user_visible_pilot_review_slice(
        payload,
        reviewer=reviewer,
        review_notes=review_notes,
        reviewed_at=generated_at,
    )
    try:
        updated = apply_production_scoped_shadow_production_default_api_user_visible_pilot_review(
            payload,
            review_slice,
            generated_at=_get(review_slice, "production_default_api_user_visible_pilot_review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_production_default_api_user_visible_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(str(exc)) from exc
    if updated["execution"] != execution_before:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "production default/API/user-visible pilot review must not modify bundle.execution"
        )
    if updated["authorization"] != authorization_before:
        raise MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotReviewError(
            "production default/API/user-visible pilot review must not modify bundle.authorization"
        )
    if update_bundle:
        bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
            encoding="utf-8",
        )
    return {
        "production_default_api_user_visible_pilot_reviewed": True,
        "production_default_api_user_visible_pilot_accepted": review_slice[
            "prod_scoped_shadow_production_default_api_user_visible_pilot_accepted"
        ],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": update_bundle,
        "recommended_next_stage": (
            POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["prod_scoped_shadow_production_default_api_user_visible_pilot_accepted"]
            else POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_REVIEW_REJECTED_NEXT_STAGE
        ),
    }
