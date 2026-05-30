"""Review recorded live read-only production-scoped pilot evidence.

This module is paperwork-only: it evaluates the rev 11 bundle slice and does
not rerun runtime, connect to databases, or read/write shadow-runs artifacts.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    LIVE_READ_ONLY_PILOT_RUN_EXPECTED_FILES,
    LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS,
    LIVE_READ_ONLY_PILOT_RUN_SURFACE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_LIVE_READ_ONLY_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION,
    POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_live_read_only_pilot_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root


class MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
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


def _require_revision_eleven_live_read_only_pilot(bundle: Mapping[str, Any]) -> None:
    required = {
        "metadata.bundle_revision": POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION,
        "execution.prod_scoped_shadow_live_read_only_pilot_executed": True,
        "execution.prod_scoped_shadow_live_read_only_pilot_passed": True,
        "execution.live_read_only_pilot_run.pilot_surface": LIVE_READ_ONLY_PILOT_RUN_SURFACE,
        "execution.live_read_only_pilot_run.live_prod_source_reads_performed": True,
        "execution.live_read_only_pilot_run.pass_fail_evaluation.overall_passed": True,
        "execution.live_read_only_pilot_run.pass_fail_evaluation.failed_checks": [],
        "posture.live_prod_source_reads_performed": True,
        "authorization.prod_scoped_shadow_live_read_only_authorized": True,
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized": True,
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted": True,
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested": True,
        "authorization.prod_scoped_shadow_live_execution_authorized": False,
        "authorization.prod_scoped_shadow_execution_authorized": False,
        "posture.online_shadow_execution_enabled": False,
        "posture.production_default_allowed": False,
        "posture.api_web_changes_allowed": False,
        "posture.user_visible_ranking_changed": False,
        "recommended_next_stage": POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE,
    }
    for path, expected in required.items():
        observed = bundle.get(path) if "." not in path else _get(bundle, path)
        if observed != expected:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
                f"{path} must be {expected!r}, got {observed!r}"
            )
    if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
            "live read-only pilot review has already been filed"
        )
    if not isinstance(_get(bundle, "execution.live_read_only_pilot_run"), Mapping):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
            "execution.live_read_only_pilot_run must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_read_only_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
            "authorization.live_read_only_grant_decision must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_read_only_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
            "authorization.live_read_only_granted_scope must be an object"
        )


def _forbidden_write_counts_zero(live_run: Mapping[str, Any]) -> bool:
    counts = _get(live_run, "write_count_verification.write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(count == 0 for target, count in counts.items() if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)


def _live_source_reads_documented(live_source_reads: Any) -> bool:
    if not isinstance(live_source_reads, Mapping):
        return False
    read_only = live_source_reads.get("read_only_assertions")
    identity = live_source_reads.get("input_identity_verification")
    approved_tables = live_source_reads.get("approved_tables")
    return (
        isinstance(approved_tables, list)
        and sorted(approved_tables) == ["embeddings", "paper_scores", "ranking_runs", "works"]
        and isinstance(read_only, Mapping)
        and all(
            read_only.get(field) is True
            for field in (
                "select_only_sql_enforced",
                "approved_source_allowlist_enforced",
                "default_transaction_read_only",
                "no_write_sql_detected",
            )
        )
        and isinstance(identity, Mapping)
        and identity.get("matches_pinned_identity") is True
    )


def _live_read_only_grant_slices_present(bundle: Mapping[str, Any]) -> bool:
    grant = _get(bundle, "authorization.live_read_only_grant_decision")
    scope = _get(bundle, "authorization.live_read_only_granted_scope")
    if not isinstance(grant, Mapping) or not isinstance(scope, Mapping):
        return False
    reviewer = grant.get("reviewer") or grant.get("owner") or grant.get("second_reviewer")
    return (
        isinstance(grant.get("decision"), str)
        and bool(grant.get("decision"))
        and isinstance(reviewer, str)
        and bool(reviewer)
        and isinstance(grant.get("granted_at"), str)
        and bool(grant.get("granted_at"))
        and isinstance(scope.get("authorization_scope"), str)
        and bool(scope.get("authorization_scope"))
    )


def evaluate_production_scoped_shadow_live_read_only_pilot_review_checks(
    bundle: Mapping[str, Any],
) -> dict[str, bool]:
    live_run = _get(bundle, "execution.live_read_only_pilot_run")
    if not isinstance(live_run, Mapping):
        return {check: False for check in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS}
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
    files = live_run.get("files_written") if isinstance(live_run.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    live_source_reads = live_run.get("live_source_reads")
    return {
        "live_read_only_pilot_run_pass_fail_overall_passed": pass_fail.get("overall_passed") is True
        and pass_fail.get("failed_checks") == [],
        "joined_candidate_count_528": _get(live_run, "input_join_summary.joined_candidate_count") == 528,
        "runtime_row_count_528": _get(live_run, "input_join_summary.runtime_row_count") == 528,
        "runtime_drill_call_order": runtime_drill.get("call_order")
        == ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        "preflight_postflight_disabled": _get(runtime_drill, "preflight.status") == "skipped_runtime_disabled"
        and _get(runtime_drill, "postflight.status") == "skipped_runtime_disabled",
        "environment_restored": runtime_drill.get("environment_restored") is True,
        "forbidden_write_counts_zero": write_counts.get("forbidden_write_counts_zero") is True
        and _forbidden_write_counts_zero(live_run),
        "isolated_artifact_count_4": _get(
            write_counts,
            "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        == 4,
        "expected_files_recorded": observed_files == set(LIVE_READ_ONLY_PILOT_RUN_EXPECTED_FILES),
        "live_prod_source_reads_true": live_run.get("live_prod_source_reads_performed") is True
        and _get(live_run, "observability_summary.live_prod_source_reads_performed") is True
        and _get(bundle, "posture.live_prod_source_reads_performed") is True
        and _get(bundle, "execution.pilot_harness.live_prod_source_reads_performed") is False
        and _get(bundle, "execution.pilot_run.live_prod_source_reads_performed") is False,
        "live_source_reads_documented": _live_source_reads_documented(live_source_reads),
        "no_labels_refit_embedding_or_label_ingest": isinstance(live_source_reads, Mapping)
        and live_source_reads.get("labels_not_used_for_scoring") is True
        and live_source_reads.get("refit_training_performed") is False
        and live_source_reads.get("embedding_generation_performed") is False
        and live_source_reads.get("label_ingest_performed") is False
        and _get(runtime_drill, "pilot.labels_used_for_scoring") is False,
        "pilot_surface_bounded_live_read_only_prod_scoped": live_run.get("pilot_surface")
        == LIVE_READ_ONLY_PILOT_RUN_SURFACE,
        "harness_and_audit_pilot_still_no_live_reads": _get(
            bundle,
            "execution.pilot_harness.live_prod_source_reads_performed",
        )
        is False
        and _get(bundle, "execution.pilot_run.live_prod_source_reads_performed") is False,
        "production_api_user_visible_unchanged": _get(bundle, "posture.production_default_allowed") is False
        and _get(bundle, "posture.api_web_changes_allowed") is False
        and _get(bundle, "posture.user_visible_ranking_changed") is False
        and _get(runtime_drill, "pilot.production_default_changed") is False
        and _get(runtime_drill, "pilot.user_visible_ranking_changed") is False,
        "global_live_execution_authorization_false": _get(
            bundle,
            "authorization.prod_scoped_shadow_live_execution_authorized",
        )
        is False
        and _get(bundle, "authorization.prod_scoped_shadow_execution_authorized") is False
        and _get(bundle, "posture.online_shadow_execution_enabled") is False,
        "live_read_only_grant_slices_present": _live_read_only_grant_slices_present(bundle),
    }


def build_production_scoped_shadow_live_read_only_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError("reviewer must be populated")
    _require_revision_eleven_live_read_only_pilot(bundle)
    checks = evaluate_production_scoped_shadow_live_read_only_pilot_review_checks(bundle)
    failed = sorted(check for check in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS if checks.get(check) is not True)
    accepted = not failed
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "recorded rev 11 live read-only pilot passed all review checks",
            "live prod source reads were bounded to approved read-only source tables",
            "runtime drill stayed disabled before and after the scoped pilot",
            "forbidden production write counts were zero",
            "harness and audit-artifact pilot slices still record no live reads",
            "production default, API/web, and user-visible behavior remained unchanged",
        ],
        "limitations": [
            "live read-only pilot review evaluates recorded rev 11 evidence only",
            "no runtime rerun was performed",
            "no database connection was opened by the review",
            "no shadow-runs artifact reads or writes were performed",
            "global/live/fleet online shadow execution remains unauthorized",
            "accepted review clears only the live read-only pilot evidence gate",
        ],
    }
    return {
        "prod_scoped_shadow_live_read_only_pilot_reviewed": True,
        "prod_scoped_shadow_live_read_only_pilot_accepted": accepted,
        "live_read_only_pilot_review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
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
            expect_live_read_only_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(str(exc)) from exc
    execution_before = deepcopy(payload["execution"])
    review_slice = build_production_scoped_shadow_live_read_only_pilot_review_slice(
        payload,
        reviewer=reviewer,
        review_notes=review_notes,
        reviewed_at=reviewed_at,
    )
    try:
        updated = apply_production_scoped_shadow_live_read_only_pilot_review(
            payload,
            review_slice,
            generated_at=_get(review_slice, "live_read_only_pilot_review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_live_read_only_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(str(exc)) from exc
    if updated["execution"] != execution_before:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotReviewError(
            "live read-only pilot review must not modify bundle.execution"
        )
    if update_bundle:
        bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
            encoding="utf-8",
        )
    return {
        "live_read_only_pilot_reviewed": True,
        "live_read_only_pilot_accepted": review_slice["prod_scoped_shadow_live_read_only_pilot_accepted"],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": update_bundle,
        "recommended_next_stage": (
            POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["prod_scoped_shadow_live_read_only_pilot_accepted"]
            else POST_LIVE_READ_ONLY_PILOT_REVIEW_REJECTED_NEXT_STAGE
        ),
    }
