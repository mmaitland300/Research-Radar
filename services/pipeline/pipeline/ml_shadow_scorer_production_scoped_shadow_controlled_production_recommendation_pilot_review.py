"""Review recorded controlled production recommendation pilot evidence.

This module is paperwork-only: it evaluates the committed rev 27 bundle slice
and does not rerun runtime, connect to databases, read shadow-runs artifacts,
call API routes, bind HTTP, or authorize public recommendation serving.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_SCOPE,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_EXPECTED_FILES,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_FAMILY,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_LIMIT,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_ROUTE_ALLOWLIST,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_RANKING_VERSION,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_REVIEW_CHECKS,
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_BUNDLE_REVISION,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE,
    _validate_controlled_production_recommendation_pilot_run_slice,
    _validate_flag_enablement_pilot_run_slice,
    _validate_live_execution_pilot_run_slice,
    _validate_live_read_only_pilot_run_slice,
    _validate_production_default_api_user_visible_pilot_run_slice,
    _verify_controlled_production_recommendation_grant_section,
    _verify_controlled_production_recommendation_request_section,
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
    _verify_production_default_api_user_visible_pilot_review_section,
    _verify_production_default_api_user_visible_request_section,
    apply_production_scoped_shadow_controlled_production_recommendation_pilot_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)


class MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
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


def _require_equal(path: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            f"{path} must be {expected!r}, got {observed!r}"
        )


def _require_true(path: str, observed: Any) -> None:
    _require_equal(path, observed, True)


def _require_false(path: str, observed: Any) -> None:
    _require_equal(path, observed, False)


def _bundle_section_valid(section: Mapping[str, Any], verifier: Any) -> bool:
    try:
        verifier(section)
        return True
    except MLShadowScorerProductionScopedShadowBundleError:
        return False


def _forbidden_write_counts_zero(controlled_run: Mapping[str, Any]) -> bool:
    write_counts = controlled_run.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        return False
    if write_counts.get("forbidden_write_counts_zero") is not True:
        return False
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(counts.get(target, 0) == 0 for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS)


def _require_revision_twenty_seven_controlled_pilot(bundle: Mapping[str, Any]) -> None:
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE,
    )
    _require_true(
        "execution.prod_scoped_shadow_controlled_production_recommendation_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_controlled_production_recommendation_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_controlled_production_recommendation_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_controlled_production_recommendation_pilot_passed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_controlled_production_recommendation_pilot_executed",
        _get(bundle, "posture.prod_scoped_shadow_controlled_production_recommendation_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_controlled_production_recommendation_pilot_passed",
        _get(bundle, "posture.prod_scoped_shadow_controlled_production_recommendation_pilot_passed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_controlled_production_recommendation_pilot_executed",
        _get(
            bundle,
            "shadow_and_production_blockers.prod_scoped_shadow_controlled_production_recommendation_pilot_executed",
        ),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_controlled_production_recommendation_pilot_passed",
        _get(
            bundle,
            "shadow_and_production_blockers.prod_scoped_shadow_controlled_production_recommendation_pilot_passed",
        ),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_controlled_production_recommendation_pilot_run",
        _get(bundle, "shadow_and_production_blockers.blockers_cleared_by_controlled_production_recommendation_pilot_run"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_controlled_production_recommendation_pilot_run",
        _get(bundle, "shadow_and_production_blockers.blockers_introduced_by_controlled_production_recommendation_pilot_run"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_controlled_production_recommendation_pilot_run",
        _get(bundle, "shadow_and_production_blockers.blockers_unchanged_by_controlled_production_recommendation_pilot_run"),
    )
    controlled_run = _get(bundle, "execution.controlled_production_recommendation_pilot_run")
    if not isinstance(controlled_run, Mapping):
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "execution.controlled_production_recommendation_pilot_run must be an object"
        )
    try:
        _validate_controlled_production_recommendation_pilot_run_slice(controlled_run)
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(str(exc)) from exc
    _require_true(
        "execution.controlled_production_recommendation_pilot_run.pass_fail_evaluation.overall_passed",
        _get(controlled_run, "pass_fail_evaluation.overall_passed"),
    )
    _require_equal(
        "execution.controlled_production_recommendation_pilot_run.pass_fail_evaluation.failed_checks",
        _get(controlled_run, "pass_fail_evaluation.failed_checks"),
        [],
    )

    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "authorization must be an object"
        )
    _require_true(
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorization_requested",
        authorization.get("prod_scoped_shadow_controlled_production_recommendation_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorization_granted",
        authorization.get("prod_scoped_shadow_controlled_production_recommendation_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_controlled_production_recommendation_authorized",
        authorization.get("prod_scoped_shadow_controlled_production_recommendation_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )

    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "review must be an object"
        )
    if review.get("prod_scoped_shadow_controlled_production_recommendation_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "controlled production recommendation pilot review has already been filed"
        )
    if review.get("controlled_production_recommendation_pilot_review_decision") is not None:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "review.controlled_production_recommendation_pilot_review_decision must not already exist"
        )
    _require_true(
        "review.prod_scoped_shadow_production_default_api_user_visible_pilot_reviewed",
        review.get("prod_scoped_shadow_production_default_api_user_visible_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_production_default_api_user_visible_pilot_accepted",
        review.get("prod_scoped_shadow_production_default_api_user_visible_pilot_accepted"),
    )
    _require_equal(
        "review.production_default_api_user_visible_pilot_review_decision.decision",
        _get(review, "production_default_api_user_visible_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.production_default_api_user_visible_pilot_review_decision.failed_review_checks",
        _get(review, "production_default_api_user_visible_pilot_review_decision.failed_review_checks"),
        [],
    )
    for verifier in (
        _verify_controlled_production_recommendation_request_section,
        _verify_controlled_production_recommendation_grant_section,
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
            raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(str(exc)) from exc
    for verifier in (
        _verify_live_read_only_pilot_review_section,
        _verify_live_execution_pilot_review_section,
        _verify_flag_enablement_pilot_review_section,
        _verify_production_default_api_user_visible_pilot_review_section,
    ):
        try:
            verifier(review)
        except MLShadowScorerProductionScopedShadowBundleError as exc:
            raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(str(exc)) from exc

    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(bundle, "plan.production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    for section_name in ("posture", "shadow_and_production_blockers"):
        _require_false(f"{section_name}.online_shadow_execution_enabled", _get(bundle, f"{section_name}.online_shadow_execution_enabled"))
        _require_false(f"{section_name}.production_default_allowed", _get(bundle, f"{section_name}.production_default_allowed"))
        _require_false(f"{section_name}.api_web_changes_allowed", _get(bundle, f"{section_name}.api_web_changes_allowed"))
        _require_false(f"{section_name}.user_visible_ranking_changed", _get(bundle, f"{section_name}.user_visible_ranking_changed"))
        _require_false(f"{section_name}.prod_scoped_shadow_execution_authorized", _get(bundle, f"{section_name}.prod_scoped_shadow_execution_authorized"))


def evaluate_production_scoped_shadow_controlled_production_recommendation_pilot_review_checks(
    bundle: Mapping[str, Any],
) -> dict[str, bool]:
    controlled_run = _get(bundle, "execution.controlled_production_recommendation_pilot_run")
    if not isinstance(controlled_run, Mapping):
        return {check: False for check in CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_REVIEW_CHECKS}
    runtime_drill = controlled_run.get("runtime_drill") if isinstance(controlled_run.get("runtime_drill"), Mapping) else {}
    pass_fail = controlled_run.get("pass_fail_evaluation") if isinstance(controlled_run.get("pass_fail_evaluation"), Mapping) else {}
    write_counts = controlled_run.get("write_count_verification") if isinstance(controlled_run.get("write_count_verification"), Mapping) else {}
    incomplete = controlled_run.get("incomplete_coverage_drill") if isinstance(controlled_run.get("incomplete_coverage_drill"), Mapping) else {}
    probe = controlled_run.get("controlled_serving_probe") if isinstance(controlled_run.get("controlled_serving_probe"), Mapping) else {}
    response = controlled_run.get("controlled_response_summary") if isinstance(controlled_run.get("controlled_response_summary"), Mapping) else {}
    scope = controlled_run.get("controlled_recommendation_scope") if isinstance(controlled_run.get("controlled_recommendation_scope"), Mapping) else {}
    provenance = controlled_run.get("input_provenance") if isinstance(controlled_run.get("input_provenance"), Mapping) else {}
    pass_fail_checks = pass_fail.get("checks") if isinstance(pass_fail.get("checks"), Mapping) else {}
    files = controlled_run.get("files_written") if isinstance(controlled_run.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    authorization = bundle.get("authorization") if isinstance(bundle.get("authorization"), Mapping) else {}
    review = bundle.get("review") if isinstance(bundle.get("review"), Mapping) else {}
    execution = bundle.get("execution") if isinstance(bundle.get("execution"), Mapping) else {}
    granted_scope = scope.get("controlled_production_recommendation_granted_scope")

    controlled_route_allowlisted = (
        probe.get("requested_route") in CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_ROUTE_ALLOWLIST
        and probe.get("route_allowlist") == list(CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_ROUTE_ALLOWLIST)
        and probe.get("route_allowlisted") is True
        and probe.get("requested_family") == CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_FAMILY
        and probe.get("family_allowlisted") is True
        and probe.get("requested_limit") == CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_LIMIT
        and response.get("allowed_route") in CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_ROUTE_ALLOWLIST
        and response.get("recommendation_family") == CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_FAMILY
        and response.get("limit") == CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_LIMIT
    )
    production_output_flags_false = (
        scope.get("production_default_allowed") is False
        and scope.get("api_web_changes_allowed") is False
        and scope.get("user_visible_ranking_changed") is False
        and scope.get("online_shadow_execution_enabled") is False
        and scope.get("prod_scoped_shadow_execution_authorized") is False
        and response.get("production_default_allowed") is False
        and response.get("api_web_changes_allowed") is False
        and response.get("user_visible_ranking_changed") is False
        and response.get("production_default_changed") is False
        and response.get("api_web_changed") is False
        and _get(bundle, "posture.production_default_allowed") is False
        and _get(bundle, "posture.api_web_changes_allowed") is False
        and _get(bundle, "posture.user_visible_ranking_changed") is False
        and _get(bundle, "shadow_and_production_blockers.production_default_allowed") is False
        and _get(bundle, "shadow_and_production_blockers.api_web_changes_allowed") is False
        and _get(bundle, "shadow_and_production_blockers.user_visible_ranking_changed") is False
    )
    upstream_chains_still_valid = (
        isinstance(authorization, Mapping)
        and _bundle_section_valid(authorization, _verify_controlled_production_recommendation_request_section)
        and _bundle_section_valid(authorization, _verify_controlled_production_recommendation_grant_section)
        and _bundle_section_valid(authorization, _verify_live_read_only_request_section)
        and _bundle_section_valid(authorization, _verify_live_read_only_grant_section)
        and _bundle_section_valid(authorization, _verify_live_execution_request_section)
        and _bundle_section_valid(authorization, _verify_live_execution_grant_section)
        and _bundle_section_valid(authorization, _verify_flag_enablement_request_section)
        and _bundle_section_valid(authorization, _verify_flag_enablement_grant_section)
        and _bundle_section_valid(authorization, _verify_production_default_api_user_visible_request_section)
        and _bundle_section_valid(authorization, _verify_production_default_api_user_visible_grant_section)
        and review.get("prod_scoped_shadow_live_read_only_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_live_read_only_pilot_accepted") is True
        and _get(review, "live_read_only_pilot_review_decision.decision") == "accepted"
        and _get(review, "live_read_only_pilot_review_decision.failed_review_checks") == []
        and review.get("prod_scoped_shadow_live_execution_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_live_execution_pilot_accepted") is True
        and _get(review, "live_execution_pilot_review_decision.decision") == "accepted"
        and _get(review, "live_execution_pilot_review_decision.failed_review_checks") == []
        and review.get("prod_scoped_shadow_flag_enablement_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_flag_enablement_pilot_accepted") is True
        and _get(review, "flag_enablement_pilot_review_decision.decision") == "accepted"
        and _get(review, "flag_enablement_pilot_review_decision.failed_review_checks") == []
        and review.get("prod_scoped_shadow_production_default_api_user_visible_pilot_reviewed") is True
        and review.get("prod_scoped_shadow_production_default_api_user_visible_pilot_accepted") is True
        and _get(review, "production_default_api_user_visible_pilot_review_decision.decision") == "accepted"
        and _get(review, "production_default_api_user_visible_pilot_review_decision.failed_review_checks") == []
        and authorization.get("prod_scoped_shadow_live_read_only_execution_authorized") is True
        and authorization.get("prod_scoped_shadow_live_execution_authorized") is True
        and authorization.get("prod_scoped_shadow_flag_enablement_authorized") is True
        and authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorized") is True
        and authorization.get("prod_scoped_shadow_controlled_production_recommendation_authorized") is True
        and execution.get("prod_scoped_shadow_live_read_only_pilot_executed") is True
        and execution.get("prod_scoped_shadow_live_execution_pilot_executed") is True
        and execution.get("prod_scoped_shadow_flag_enablement_pilot_executed") is True
        and execution.get("prod_scoped_shadow_production_default_api_user_visible_pilot_executed") is True
    )
    return {
        "controlled_production_recommendation_pilot_run_pass_fail_overall_passed": pass_fail.get("overall_passed") is True
        and pass_fail.get("failed_checks") == [],
        "joined_candidate_count_528": _get(controlled_run, "input_join_summary.joined_candidate_count") == 528,
        "runtime_row_count_528": _get(controlled_run, "input_join_summary.runtime_row_count") == 528,
        "runtime_drill_call_order": runtime_drill.get("call_order")
        == ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        "preflight_postflight_disabled": _get(runtime_drill, "preflight.status") == "skipped_runtime_disabled"
        and _get(runtime_drill, "postflight.status") == "skipped_runtime_disabled",
        "pilot_status_succeeded_controlled_test_client": _get(runtime_drill, "pilot.status")
        == "succeeded_controlled_test_client",
        "process_scoped_runtime_flag_only": runtime_drill.get("process_scoped_runtime_flag_only") is True,
        "runtime_flag_enabled_only_during_pilot": _get(runtime_drill, "preflight.runtime_enabled") is False
        and _get(runtime_drill, "pilot.runtime_enabled") is True
        and _get(runtime_drill, "postflight.runtime_enabled") is False
        and runtime_drill.get("environment_restored") is True,
        "environment_restored": runtime_drill.get("environment_restored") is True,
        "rollback_flag_off_drill_verified": runtime_drill.get("rollback_flag_off_drill_verified") is True,
        "incomplete_coverage_skip_verified": incomplete.get("status") == "skipped_incomplete_coverage"
        and incomplete.get("shadow_row_count") == 0
        and incomplete.get("writes_performed") is False
        and incomplete.get("live_prod_source_reads_performed") is False,
        "approved_source_reread_verified": provenance.get("reread_approved_production_sources") is True,
        "ranking_version_not_test_fixture": provenance.get("fixture_ranking_version_used") is False
        and provenance.get("ranking_version") == CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_RANKING_VERSION,
        "controlled_route_allowlisted": controlled_route_allowlisted,
        "controlled_test_client_response_emitted": probe.get("response_emitted") is True
        and probe.get("controlled_test_client_allowlisted") is True
        and probe.get("in_process_controlled_test_client") is True
        and response.get("emitted_to_allowlisted_pilot_client") is True,
        "response_status_200": response.get("response_status_code") == 200,
        "response_schema_valid": response.get("response_schema_valid") is True,
        "response_items_match_shadow_top_k": response.get("response_items_match_shadow_top_k") is True,
        "public_user_traffic_false": response.get("public_user_traffic_received") is False
        and probe.get("public_user_traffic_received") is False,
        "emitted_to_public_users_false": response.get("emitted_to_public_users") is False,
        "no_http_server_bind": probe.get("http_server_bound") is False and response.get("http_server_bound") is False,
        "no_outbound_api_call": probe.get("outbound_api_route_called") is False
        and response.get("outbound_api_route_called") is False,
        "production_default_api_user_visible_global_flags_false": production_output_flags_false,
        "global_execution_authorization_false": _get(bundle, "authorization.prod_scoped_shadow_execution_authorized")
        is False
        and _get(bundle, "posture.prod_scoped_shadow_execution_authorized") is False
        and _get(bundle, "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized") is False,
        "paper_scores_and_ranking_runs_not_written": response.get("paper_scores_written") is False
        and response.get("ranking_runs_written") is False
        and response.get("production_config_written") is False,
        "forbidden_write_counts_zero": _forbidden_write_counts_zero(controlled_run),
        "isolated_artifact_count_expected": _get(
            write_counts,
            "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        == 5,
        "expected_files_recorded": observed_files == set(CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_EXPECTED_FILES),
        "controlled_production_recommendation_grant_slices_present": isinstance(granted_scope, Mapping)
        and granted_scope.get("authorization_scope") == CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_SCOPE
        and isinstance(scope.get("controlled_production_recommendation_grant_decision"), Mapping),
        "upstream_chains_still_valid": upstream_chains_still_valid,
        "plan_flag_authorized_now_false": _get(
            bundle,
            "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        )
        is False,
        "bridge_surface_not_included": response.get("bridge_recommendations_included") is False
        and _get(probe, "bridge_family_probe.rejected") is True
        and _get(probe, "bridge_family_probe.bridge_recommendations_included") is False
        and _get(scope, "controlled_production_recommendation_granted_scope.bridge_recommendations_included") is False,
        "no_labels_refit_embedding_generation_or_label_ingest": pass_fail_checks.get(
            "no_labels_refit_embedding_generation_or_label_ingest"
        )
        is True
        and _get(controlled_run, "input_join_summary.labels_not_used_for_scoring") is True
        and _get(controlled_run, "input_join_summary.refit_training_performed") is False
        and _get(controlled_run, "input_join_summary.embedding_generation_performed") is False
        and _get(controlled_run, "input_join_summary.label_ingest_performed") is False,
    }


def build_production_scoped_shadow_controlled_production_recommendation_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "reviewer must be populated"
        )
    _require_revision_twenty_seven_controlled_pilot(bundle)
    checks = evaluate_production_scoped_shadow_controlled_production_recommendation_pilot_review_checks(bundle)
    failed = sorted(
        check
        for check in CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_REVIEW_CHECKS
        if checks.get(check) is not True
    )
    accepted = not failed
    controlled_run = _get(bundle, "execution.controlled_production_recommendation_pilot_run")
    response = controlled_run.get("controlled_response_summary") if isinstance(controlled_run, Mapping) else {}
    items = response.get("items") if isinstance(response, Mapping) and isinstance(response.get("items"), list) else []
    top_work_ids = [
        str(item.get("canonical_openalex_work_id"))
        for item in items[:5]
        if isinstance(item, Mapping) and item.get("canonical_openalex_work_id")
    ]
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "recorded rev 27 controlled production recommendation pilot evidence: "
            f"pilot_run_id={controlled_run.get('pilot_run_id')}; route={response.get('allowed_route')}; "
            f"family={response.get('recommendation_family')}; item count={len(items)}; "
            f"top work IDs={', '.join(top_work_ids)}; pass/fail summary overall_passed="
            f"{_get(controlled_run, 'pass_fail_evaluation.overall_passed')} failed_checks="
            f"{_get(controlled_run, 'pass_fail_evaluation.failed_checks')}",
            "controlled pilot response was emitted only to the allowlisted in-process pilot client",
            "recorded route, family, response schema, and top-k match checks passed",
            "public user traffic and public-user emission were recorded false",
            "forbidden production write counts were zero and only isolated audit artifacts were recorded",
            "upstream live read-only, live execution, flag enablement, production default/API/user-visible, "
            "and controlled recommendation authorization chains remained valid",
            "global online shadow execution, production defaults, API/web changes, and user-visible ranking remained disabled",
            "bridge recommendations remained out of scope",
        ],
        "limitations": [
            "controlled production recommendation pilot review evaluates recorded rev 27 pilot evidence only; "
            "no runtime rerun was performed",
            "no database connection was opened by the review",
            "no shadow-runs artifact reads or writes were performed",
            "no API routes were called and no HTTP server was bound by the review",
            "no public production serving and no broad rollout are authorized by this review",
            "controlled recommendation output remains bounded to the allowlisted in-process pilot client only",
            "bridge recommendations remain out of scope",
        ],
    }
    return {
        "prod_scoped_shadow_controlled_production_recommendation_pilot_reviewed": True,
        "prod_scoped_shadow_controlled_production_recommendation_pilot_accepted": accepted,
        "controlled_production_recommendation_pilot_review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot(
    *,
    bundle_path: Path,
    reviewer: str = "Matt Maitland",
    review_notes: str | None = None,
    generated_at: str | None = None,
    repo_root: Path,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            payload,
            repo_root=root,
            expect_controlled_production_recommendation_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(str(exc)) from exc

    execution_before = deepcopy(payload["execution"])
    authorization_before = deepcopy(payload["authorization"])
    prior_review_decisions = {
        "review_decision": deepcopy(_get(payload, "review.review_decision")),
        "pilot_review_decision": deepcopy(_get(payload, "review.pilot_review_decision")),
        "live_read_only_pilot_review_decision": deepcopy(_get(payload, "review.live_read_only_pilot_review_decision")),
        "live_execution_pilot_review_decision": deepcopy(_get(payload, "review.live_execution_pilot_review_decision")),
        "flag_enablement_pilot_review_decision": deepcopy(_get(payload, "review.flag_enablement_pilot_review_decision")),
        "production_default_api_user_visible_pilot_review_decision": deepcopy(
            _get(payload, "review.production_default_api_user_visible_pilot_review_decision")
        ),
    }
    review_slice = build_production_scoped_shadow_controlled_production_recommendation_pilot_review_slice(
        payload,
        reviewer=reviewer,
        review_notes=review_notes,
        reviewed_at=generated_at,
    )
    try:
        updated = apply_production_scoped_shadow_controlled_production_recommendation_pilot_review(
            payload,
            review_slice,
            generated_at=_get(review_slice, "controlled_production_recommendation_pilot_review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_controlled_production_recommendation_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(str(exc)) from exc
    if updated["execution"] != execution_before:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "controlled production recommendation pilot review must not modify bundle.execution"
        )
    if updated["authorization"] != authorization_before:
        raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
            "controlled production recommendation pilot review must not modify bundle.authorization"
        )
    for key, before in prior_review_decisions.items():
        if _get(updated, f"review.{key}") != before:
            raise MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotReviewError(
                f"controlled production recommendation pilot review must preserve review.{key}"
            )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return {
        "controlled_production_recommendation_pilot_reviewed": True,
        "controlled_production_recommendation_pilot_accepted": review_slice[
            "prod_scoped_shadow_controlled_production_recommendation_pilot_accepted"
        ],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": True,
        "recommended_next_stage": (
            POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["prod_scoped_shadow_controlled_production_recommendation_pilot_accepted"]
            else POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_REVIEW_REJECTED_NEXT_STAGE
        ),
    }
