"""Review recorded production-scoped pilot evidence without rerunning it."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    MLShadowScorerProductionScopedShadowBundleError,
    PILOT_RUN_EXPECTED_FILES,
    PILOT_RUN_REVIEW_CHECKS,
    PILOT_RUN_SURFACE,
    POST_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_PILOT_REVIEW_REJECTED_NEXT_STAGE,
    POST_PILOT_RUN_BUNDLE_REVISION,
    POST_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_pilot_review,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root


class MLShadowScorerProductionScopedShadowPilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowPilotReviewError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowPilotReviewError(f"Expected JSON object in {path}")
    return payload


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _require_revision_seven_pilot(bundle: Mapping[str, Any]) -> None:
    required = {
        "metadata.bundle_revision": POST_PILOT_RUN_BUNDLE_REVISION,
        "execution.prod_scoped_shadow_pilot_executed": True,
        "execution.prod_scoped_shadow_pilot_passed": True,
        "execution.pilot_run.pilot_surface": PILOT_RUN_SURFACE,
        "execution.pilot_run.input_join_summary.joined_candidate_count": 528,
        "execution.pilot_run.live_prod_source_reads_performed": False,
        "execution.pilot_run.pass_fail_evaluation.overall_passed": True,
        "authorization.prod_scoped_shadow_pilot_execution_authorized": True,
        "authorization.prod_scoped_shadow_live_execution_authorized": False,
        "authorization.prod_scoped_shadow_execution_authorized": False,
        "posture.online_shadow_execution_enabled": False,
        "posture.production_default_allowed": False,
        "posture.api_web_changes_allowed": False,
        "posture.user_visible_ranking_changed": False,
        "recommended_next_stage": POST_PILOT_RUN_NEXT_STAGE,
    }
    for path, expected in required.items():
        observed = bundle.get(path) if "." not in path else _get(bundle, path)
        if observed != expected:
            raise MLShadowScorerProductionScopedShadowPilotReviewError(
                f"{path} must be {expected!r}, got {observed!r}"
            )
    if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowPilotReviewError("pilot review has already been filed")
    if not isinstance(_get(bundle, "execution.pilot_run"), Mapping):
        raise MLShadowScorerProductionScopedShadowPilotReviewError("execution.pilot_run must be an object")


def _forbidden_write_counts_zero(pilot_run: Mapping[str, Any]) -> bool:
    counts = _get(pilot_run, "write_count_verification.write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        return False
    return all(count == 0 for target, count in counts.items() if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)


def evaluate_production_scoped_shadow_pilot_review_checks(bundle: Mapping[str, Any]) -> dict[str, bool]:
    pilot_run = _get(bundle, "execution.pilot_run")
    if not isinstance(pilot_run, Mapping):
        return {check: False for check in PILOT_RUN_REVIEW_CHECKS}
    runtime_drill = pilot_run.get("runtime_drill") if isinstance(pilot_run.get("runtime_drill"), Mapping) else {}
    pass_fail = pilot_run.get("pass_fail_evaluation") if isinstance(pilot_run.get("pass_fail_evaluation"), Mapping) else {}
    write_counts = (
        pilot_run.get("write_count_verification")
        if isinstance(pilot_run.get("write_count_verification"), Mapping)
        else {}
    )
    files = pilot_run.get("files_written") if isinstance(pilot_run.get("files_written"), list) else []
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    source_artifacts = pilot_run.get("source_artifacts") if isinstance(pilot_run.get("source_artifacts"), Mapping) else {}
    source_artifact_records = [
        source_artifacts.get("learned_probability_artifact"),
        source_artifacts.get("second_surface_generalization_audit"),
    ]
    return {
        "pilot_run_pass_fail_overall_passed": pass_fail.get("overall_passed") is True
        and pass_fail.get("failed_checks") == [],
        "joined_candidate_count_528": _get(pilot_run, "input_join_summary.joined_candidate_count") == 528,
        "runtime_row_count_528": _get(pilot_run, "input_join_summary.runtime_row_count") == 528,
        "runtime_drill_call_order": runtime_drill.get("call_order")
        == ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        "preflight_postflight_disabled": _get(runtime_drill, "preflight.status") == "skipped_runtime_disabled"
        and _get(runtime_drill, "postflight.status") == "skipped_runtime_disabled",
        "environment_restored": runtime_drill.get("environment_restored") is True,
        "forbidden_write_counts_zero": write_counts.get("forbidden_write_counts_zero") is True
        and _forbidden_write_counts_zero(pilot_run),
        "isolated_artifact_count_4": _get(write_counts, "write_counts_by_isolated_target." + ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)
        == 4,
        "expected_files_recorded": observed_files == set(PILOT_RUN_EXPECTED_FILES),
        "source_artifacts_verified": all(
            isinstance(record, Mapping)
            and record.get("verification_status") == "confirmed"
            and isinstance(record.get("sha256"), str)
            and len(record.get("sha256")) == 64
            for record in source_artifact_records
        ),
        "runtime_writes_false": _get(runtime_drill, "pilot.writes_performed") is False
        and write_counts.get("runtime_writes_performed") is False,
        "live_prod_source_reads_false": pilot_run.get("live_prod_source_reads_performed") is False
        and _get(pilot_run, "observability_summary.live_prod_source_reads_performed") is False
        and write_counts.get("live_prod_source_reads_performed") is False,
        "pilot_surface_bounded_read_only_audit_artifact": pilot_run.get("pilot_surface") == PILOT_RUN_SURFACE,
        "production_api_user_visible_unchanged": _get(bundle, "posture.production_default_allowed") is False
        and _get(bundle, "posture.api_web_changes_allowed") is False
        and _get(bundle, "posture.user_visible_ranking_changed") is False
        and _get(runtime_drill, "pilot.production_default_changed") is False
        and _get(runtime_drill, "pilot.user_visible_ranking_changed") is False,
        "global_live_execution_authorization_false": _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized") is False
        and _get(bundle, "authorization.prod_scoped_shadow_execution_authorized") is False
        and _get(bundle, "posture.online_shadow_execution_enabled") is False,
    }


def build_production_scoped_shadow_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str,
    review_notes: str | None = None,
    reviewed_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise MLShadowScorerProductionScopedShadowPilotReviewError("reviewer must be populated")
    _require_revision_seven_pilot(bundle)
    checks = evaluate_production_scoped_shadow_pilot_review_checks(bundle)
    failed = sorted(check for check in PILOT_RUN_REVIEW_CHECKS if checks.get(check) is not True)
    accepted = not failed
    decision = {
        "decision": "accepted" if accepted else "not_accepted",
        "reviewer": reviewer,
        "reviewed_at": reviewed_at or _now_iso_z(),
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": [
            "bounded 528-work audit-artifact pilot passed all review checks",
            "approved source artifacts were verified by path and SHA-256",
            "runtime drill stayed disabled before and after the scoped pilot",
            "forbidden production write counts were zero",
            "live production source reads were not performed",
            "production default, API/web, and user-visible behavior remained unchanged",
        ],
        "limitations": [
            "not live production traffic",
            "no live read-only production source access was reviewed",
            "no runtime rerun was performed",
            "no shadow-runs artifact reads or writes were performed",
            "global/live/fleet online shadow execution remains unauthorized",
        ],
    }
    return {
        "prod_scoped_shadow_pilot_reviewed": True,
        "prod_scoped_shadow_pilot_accepted": accepted,
        "pilot_review_decision": decision,
    }


def review_ml_shadow_scorer_production_scoped_shadow_pilot(
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
            expect_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowPilotReviewError(str(exc)) from exc
    execution_before = deepcopy(payload["execution"])
    review_slice = build_production_scoped_shadow_pilot_review_slice(
        payload,
        reviewer=reviewer,
        review_notes=review_notes,
        reviewed_at=reviewed_at,
    )
    try:
        updated = apply_production_scoped_shadow_pilot_review(
            payload,
            review_slice,
            generated_at=_get(review_slice, "pilot_review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated,
            repo_root=root,
            expect_pilot_review_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowPilotReviewError(str(exc)) from exc
    if updated["execution"] != execution_before:
        raise MLShadowScorerProductionScopedShadowPilotReviewError("pilot review must not modify bundle.execution")
    if update_bundle:
        bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
            encoding="utf-8",
        )
    return {
        "pilot_reviewed": True,
        "pilot_accepted": review_slice["prod_scoped_shadow_pilot_accepted"],
        "review": review_slice,
        "bundle": updated,
        "bundle_updated": update_bundle,
        "recommended_next_stage": (
            POST_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["prod_scoped_shadow_pilot_accepted"]
            else POST_PILOT_REVIEW_REJECTED_NEXT_STAGE
        ),
    }
