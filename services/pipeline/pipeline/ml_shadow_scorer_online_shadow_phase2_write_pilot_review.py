"""Review the completed Phase 2 isolated audit write pilot from the bundle.

This helper reads the canonical phase bundle execution slice and records a
review decision back into the bundle. It does not rerun runtime scoring, read or
write shadow-run files, access databases, or change production/API/default
behavior.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_shadow_scorer_phase_bundle import (
    MLShadowScorerPhaseBundleError,
    POST_REVIEW_ACCEPTED_NEXT_STAGE,
    POST_REVIEW_REMEDIATE_NEXT_STAGE,
    apply_phase2_write_pilot_review,
    markdown_from_ml_shadow_scorer_phase_bundle,
    verify_ml_shadow_scorer_phase_bundle_payload,
)
from pipeline.repo_paths import default_repo_root

EXPECTED_ROW_COUNT = 528
DEFAULT_REVIEWER = "Matt Maitland"
EXPECTED_DISABLE_DRILL_ORDER = ["preflight_disabled", "pilot_enabled", "postflight_disabled"]

ACCEPTED_EVIDENCE = (
    "Phase 2 write pilot passed all pass/fail checks",
    "528 rows scored and persisted in isolated audit tree",
    "Forbidden write targets all zero; isolated_audit_shadow_tables zero",
    "runtime_writes_performed false; runtime flag scoped to process only",
    "Disable drill passed; environment restored; correct call order",
    "No production/API/user-visible changes; labels not used",
)

LIMITATIONS = (
    "Review is of isolated audit file-tree pilot only; not production shadow",
    "online_shadow_execution_enabled remains false globally",
    "Production readiness authorization remains separate and missing",
)


class MLShadowScorerOnlineShadowPhase2WritePilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError(f"Expected JSON object in {path}")
    return payload


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _all_mapping_values_true(value: Any) -> bool:
    return isinstance(value, Mapping) and bool(value) and all(item is True for item in value.values())


def review_checks_from_bundle(bundle: Mapping[str, Any]) -> dict[str, bool]:
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError("bundle execution must be an object")
    runtime_summary = execution.get("pilot_runtime_summary")
    if not isinstance(runtime_summary, Mapping):
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError(
            "bundle execution.pilot_runtime_summary must be an object"
        )
    write_counts = _get(execution, "write_count_verification.write_counts_by_isolated_target")
    pass_fail = execution.get("pass_fail_evaluation")
    pass_fail_failed = _get(execution, "pass_fail_evaluation.failed_checks")
    runtime_summary_writes_false = (
        runtime_summary.get("writes_performed") is False
        or runtime_summary.get("runtime_writes_performed") is False
    )
    return {
        "pilot_runtime_succeeded": runtime_summary.get("status") == "succeeded_test_only",
        "complete_row_coverage": _get(execution, "input_join_summary.joined_candidate_count") == EXPECTED_ROW_COUNT
        and _get(execution, "observability.component_coverage.complete") is True,
        "disable_drill_passed": _get(execution, "disable_drill.passed") is True
        and _get(execution, "disable_drill.environment_restored") is True
        and _get(execution, "disable_drill.call_order") == EXPECTED_DISABLE_DRILL_ORDER,
        "forbidden_write_targets_zero": _get(execution, "write_count_verification.forbidden_targets_zero") is True
        and isinstance(write_counts, Mapping)
        and write_counts.get("isolated_audit_shadow_tables") == 0,
        "isolated_files_written": _get(execution, "isolated_file_writes.file_count") == 4
        and execution.get("isolated_artifact_tree_writes_performed") is True,
        "runtime_writes_false": execution.get("runtime_writes_performed") is False
        and runtime_summary_writes_false,
        "production_api_user_visible_unchanged": execution.get("production_default_changed") is False
        and execution.get("user_visible_ranking_changed") is False
        and execution.get("api_web_changes_allowed") is False,
        "labels_not_used": execution.get("labels_used_for_scoring") is False,
        "observability_policy_contract_satisfied": _all_mapping_values_true(
            _get(execution, "observability.policy_contract_satisfied")
        ),
        "pass_fail_evaluation_passed": isinstance(pass_fail, Mapping)
        and pass_fail.get("passed") is True
        and pass_fail_failed == [],
    }


def build_phase2_write_pilot_review_slice(
    bundle: Mapping[str, Any],
    *,
    reviewer: str = DEFAULT_REVIEWER,
    review_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    reviewed_at = generated_at or _now_iso_z()
    checks = review_checks_from_bundle(bundle)
    failed_checks = [name for name, passed in checks.items() if passed is not True]
    accepted = not failed_checks
    decision = "accepted" if accepted else "not_accepted"
    review_decision: dict[str, Any] = {
        "decision": decision,
        "reviewer": reviewer,
        "reviewed_at": reviewed_at,
        "review_notes": review_notes,
        "checks": checks,
        "failed_review_checks": failed_checks,
        "limitations": list(LIMITATIONS),
    }
    if accepted:
        review_decision["accepted_evidence"] = list(ACCEPTED_EVIDENCE)
    return {
        "phase2_write_pilot_reviewed": True,
        "phase2_write_pilot_accepted": accepted,
        "review_decision": review_decision,
    }


def review_ml_shadow_scorer_online_shadow_phase2_write_pilot(
    *,
    bundle_path: Path,
    reviewer: str = DEFAULT_REVIEWER,
    review_notes: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    bundle = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root, expect_pilot_reviewed=False)
    except MLShadowScorerPhaseBundleError as exc:
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError(str(exc)) from exc
    execution_before = deepcopy(bundle["execution"])
    review_slice = build_phase2_write_pilot_review_slice(
        bundle,
        reviewer=reviewer,
        review_notes=review_notes,
        generated_at=generated_at,
    )
    try:
        updated_bundle = apply_phase2_write_pilot_review(
            bundle,
            review_slice,
            generated_at=_get(review_slice, "review_decision.reviewed_at"),
        )
        verify_ml_shadow_scorer_phase_bundle_payload(
            updated_bundle,
            repo_root=root,
            expect_pilot_reviewed=True,
        )
    except MLShadowScorerPhaseBundleError as exc:
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError(str(exc)) from exc
    if updated_bundle["execution"] != execution_before:
        raise MLShadowScorerOnlineShadowPhase2WritePilotReviewError(
            "review update must not modify bundle.execution"
        )
    if update_bundle:
        bundle_path.write_text(json.dumps(updated_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_phase_bundle(updated_bundle),
            encoding="utf-8",
        )
    return {
        "review": review_slice,
        "bundle": updated_bundle,
        "bundle_updated": update_bundle,
        "phase2_write_pilot_accepted": review_slice["phase2_write_pilot_accepted"],
        "recommended_next_stage": (
            POST_REVIEW_ACCEPTED_NEXT_STAGE
            if review_slice["phase2_write_pilot_accepted"]
            else POST_REVIEW_REMEDIATE_NEXT_STAGE
        ),
    }
