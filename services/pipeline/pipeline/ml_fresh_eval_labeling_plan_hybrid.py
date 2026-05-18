"""Fresh eval labeling plan for hybrid validation.

This module writes a plan artifact only. It reads the materialized fresh
surface, the policy, the existing label dataset, and the conflict policy, then
computes machine-checkable readiness gaps. It does not query databases, create
worksheets, import labels, train, score, generate embeddings, or authorize
shadow/prod.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_eval_labeling_plan_hybrid"
PLAN_VERSION = "ml-fresh-eval-labeling-plan-hybrid-v1"
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
RECOMMENDED_NEXT_STAGE = "create_fresh_eval_labeling_plan_hybrid_v1"
TARGET = "good_or_acceptable"

CAVEATS = (
    "Plan only; no labels collected and no worksheet written.",
    "Not confirmatory validation.",
    "Current 44-work confirmatory surface cannot be made ready by labeling alone.",
    "A larger fresh product-candidate source is required before hybrid validation scoring.",
    "Negative labels are currently absent from the confirmatory-eligible surface.",
    "No shadow or production authorization.",
)


class MLFreshEvalLabelingPlanHybridError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshEvalLabelingPlanHybridError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshEvalLabelingPlanHybridError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshEvalLabelingPlanHybridError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLFreshEvalLabelingPlanHybridError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-surface")
    if metadata.get("artifact_type") != SURFACE_ARTIFACT_TYPE:
        raise MLFreshEvalLabelingPlanHybridError(
            f"expected surface metadata.artifact_type={SURFACE_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("surface_version") != SURFACE_VERSION:
        raise MLFreshEvalLabelingPlanHybridError(
            f"expected surface metadata.surface_version={SURFACE_VERSION!r}, got {metadata.get('surface_version')!r}"
        )
    if metadata.get("status") != "materialized_needs_labels":
        raise MLFreshEvalLabelingPlanHybridError("fresh eval surface status must be materialized_needs_labels")
    if payload.get("ready_for_hybrid_validation_scoring") is not False:
        raise MLFreshEvalLabelingPlanHybridError("ready_for_hybrid_validation_scoring must be false")
    if payload.get("recommended_next_stage") != RECOMMENDED_NEXT_STAGE:
        raise MLFreshEvalLabelingPlanHybridError(
            f"fresh eval surface recommended_next_stage must be {RECOMMENDED_NEXT_STAGE!r}"
        )
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshEvalLabelingPlanHybridError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshEvalLabelingPlanHybridError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshEvalLabelingPlanHybridError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )


def _thresholds(policy_payload: Mapping[str, Any], surface_payload: Mapping[str, Any]) -> dict[str, float | int]:
    policy_thresholds = _get(policy_payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(policy_thresholds, Mapping):
        raise MLFreshEvalLabelingPlanHybridError("fresh surface policy missing label thresholds")
    keys = (
        "minimum_candidate_work_count",
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_label_coverage_rate",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
    )
    out: dict[str, float | int] = {}
    surface_check = surface_payload.get("threshold_check")
    for key in keys:
        value = policy_thresholds.get(key)
        if value is None and isinstance(surface_check, Mapping):
            row = surface_check.get(key)
            if isinstance(row, Mapping):
                value = row.get("threshold")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise MLFreshEvalLabelingPlanHybridError(f"threshold {key} must be numeric")
        out[key] = value
    return out


def _int_value(value: Any, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise MLFreshEvalLabelingPlanHybridError(f"{field} must be an integer")
    return value


def _number_value(value: Any, *, field: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise MLFreshEvalLabelingPlanHybridError(f"{field} must be numeric")
    return float(value)


def _status(observed: float | int, threshold: float | int) -> str:
    return "pass" if float(observed) >= float(threshold) else "fail"


def _deficit(observed: float | int, threshold: float | int) -> float | int:
    gap = float(threshold) - float(observed)
    if gap <= 0:
        return 0
    if isinstance(observed, int) and isinstance(threshold, int):
        return int(math.ceil(gap))
    return gap


def _gap_row(
    *,
    threshold: str,
    observed: float | int,
    required: float | int,
    notes: str,
    deficit: float | int | str | None = None,
) -> dict[str, Any]:
    return {
        "threshold": threshold,
        "required": required,
        "observed": observed,
        "deficit": _deficit(observed, required) if deficit is None else deficit,
        "status": _status(observed, required),
        "notes": notes,
    }


def _surface_summary(surface_payload: Mapping[str, Any]) -> dict[str, Any]:
    work_level = _get(surface_payload, "label_coverage.work_level")
    if not isinstance(work_level, Mapping):
        raise MLFreshEvalLabelingPlanHybridError("surface label_coverage.work_level must be present")
    return {
        "status": _get(surface_payload, "metadata.status"),
        "ranking_run_id": _get(surface_payload, "candidate_source.ranking_run_id"),
        "family": _get(surface_payload, "candidate_source.family"),
        "corpus_snapshot_version": _get(surface_payload, "candidate_source.corpus_snapshot_version"),
        "candidate_work_count": _get(surface_payload, "candidate_pool.candidate_work_count"),
        "candidate_work_set_sha256": _get(surface_payload, "candidate_pool.candidate_work_set_sha256"),
        "disallowed_eval_work_set_sha256": _get(surface_payload, "candidate_pool.old_eval_work_set_sha256"),
        "excluded_previous_eval_overlap_count": _get(surface_payload, "disallowed_overlap_report.excluded_previous_eval_overlap_count"),
        "confirmatory_eligible_work_count": work_level.get("confirmatory_candidate_work_count"),
        "confirmatory_labeled_work_count": work_level.get("confirmatory_labeled_work_count"),
        "confirmatory_unlabeled_work_count": work_level.get("confirmatory_unlabeled_work_count"),
        "confirmatory_positive_work_count": work_level.get("confirmatory_positive_work_count"),
        "confirmatory_negative_work_count": work_level.get("confirmatory_negative_work_count"),
        "distinct_negative_work_count": work_level.get("distinct_negative_work_count"),
        "label_coverage_rate": work_level.get("label_coverage_rate"),
        "ready_for_hybrid_validation_scoring": surface_payload.get("ready_for_hybrid_validation_scoring"),
        "source_recommended_next_stage": surface_payload.get("recommended_next_stage"),
    }


def build_ml_fresh_eval_labeling_plan_hybrid_payload(
    *,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    surface_path = Path(fresh_eval_surface_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    surface_payload = _load_json_object(surface_path)
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(label_path)
    surface_metadata = _validate_surface(surface_payload)
    policy_metadata = _validate_policy(policy_payload)
    _validate_label_dataset(label_payload)
    if not conflict_path.exists():
        raise MLFreshEvalLabelingPlanHybridError(f"conflict policy does not exist: {conflict_path}")

    inputs = [
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    thresholds = _thresholds(policy_payload, surface_payload)
    summary = _surface_summary(surface_payload)

    eligible = _int_value(summary["confirmatory_eligible_work_count"], field="confirmatory_eligible_work_count")
    labeled = _int_value(summary["confirmatory_labeled_work_count"], field="confirmatory_labeled_work_count")
    positive = _int_value(summary["confirmatory_positive_work_count"], field="confirmatory_positive_work_count")
    negative = _int_value(summary["confirmatory_negative_work_count"], field="confirmatory_negative_work_count")
    distinct_negative = _int_value(summary["distinct_negative_work_count"], field="distinct_negative_work_count")
    coverage_rate = _number_value(summary["label_coverage_rate"], field="label_coverage_rate")
    unlabeled = _int_value(summary["confirmatory_unlabeled_work_count"], field="confirmatory_unlabeled_work_count")

    min_candidate = int(thresholds["minimum_candidate_work_count"])
    min_labeled = int(thresholds["minimum_confirmatory_labeled_work_count"])
    min_positive = int(thresholds["minimum_confirmatory_positive_work_count"])
    min_negative = int(thresholds["minimum_confirmatory_negative_work_count"])
    min_distinct_negative = int(thresholds["minimum_distinct_negative_work_count"])
    min_coverage = float(thresholds["minimum_confirmatory_label_coverage_rate"])

    candidate_deficit = max(0, min_candidate - eligible)
    labeled_abs_deficit = max(0, min_labeled - labeled)
    coverage_needed_at_current_count = math.ceil(min_coverage * eligible)
    coverage_only_deficit = max(0, coverage_needed_at_current_count - labeled)
    positive_deficit = max(0, min_positive - positive)
    negative_deficit = max(0, min_negative - negative)
    distinct_negative_deficit = max(0, min_distinct_negative - distinct_negative)

    threshold_gap_analysis = [
        _gap_row(
            threshold="minimum_confirmatory_candidate_work_count",
            observed=eligible,
            required=min_candidate,
            deficit=candidate_deficit,
            notes="Candidate-source blocker",
        ),
        _gap_row(
            threshold="minimum_confirmatory_labeled_work_count",
            observed=labeled,
            required=min_labeled,
            deficit=labeled_abs_deficit,
            notes="Hard labeled-work blocker (policy absolute minimum; requires expansion before achievable)",
        ),
        _gap_row(
            threshold="minimum_confirmatory_label_coverage_rate",
            observed=coverage_rate,
            required=min_coverage,
            deficit=f"{coverage_only_deficit} at current {eligible}",
            notes=(
                f"ceil({min_coverage:.2f} * {eligible}) = {coverage_needed_at_current_count}; "
                f"{coverage_needed_at_current_count} - {labeled} = {coverage_only_deficit}; "
                "coverage-only on current surface; subordinate to 100 labeled-work rule"
            ),
        ),
        _gap_row(
            threshold="minimum_confirmatory_positive_work_count",
            observed=positive,
            required=min_positive,
            deficit=positive_deficit,
            notes="Positive-work blocker",
        ),
        _gap_row(
            threshold="minimum_confirmatory_negative_work_count",
            observed=negative,
            required=min_negative,
            deficit=negative_deficit,
            notes="Negative-work blocker",
        ),
        _gap_row(
            threshold="minimum_distinct_negative_work_count",
            observed=distinct_negative,
            required=min_distinct_negative,
            deficit=distinct_negative_deficit,
            notes="Distinct negative-work blocker",
        ),
    ]

    reason = (
        f"confirmatory_eligible_work_count {eligible} < policy minimum {min_candidate}; "
        f"labeling all {unlabeled} current unlabeled eligible works cannot satisfy candidate_count "
        f"or absolute labeled_work_count {min_labeled} thresholds"
    )

    primary_arm = _get(policy_payload, "frozen_hybrid_arms.primary_confirmatory_arm") or "hybrid_rank_mean_50_50"
    secondary_arm = _get(policy_payload, "frozen_hybrid_arms.secondary_reporting_arm") or "hybrid_rank_mean_25_75_heuristic"
    baselines = _get(policy_payload, "frozen_hybrid_arms.baselines_for_future_comparison") or [
        "heuristic_final_score_baseline",
        "holdout_embedding_probability_baseline",
    ]
    disallowed_sha = str(summary["disallowed_eval_work_set_sha256"] or "")

    payload: dict[str, Any] = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "source_surface_version": surface_metadata.get("surface_version"),
            "source_surface_status": surface_metadata.get("status"),
            "source_policy_version": policy_metadata.get("policy_version"),
            "label_dataset_version": label_payload.get("dataset_version"),
            "conflict_policy_sha256": inputs[-1]["sha256"],
            "target": TARGET,
            "caveats": list(CAVEATS),
        },
        "surface_summary": summary,
        "threshold_gap_analysis": threshold_gap_analysis,
        "labeled_work_absolute_deficit": labeled_abs_deficit,
        "labeled_work_absolute_deficit_formula": (
            f"policy_minimum_confirmatory_labeled_work_count - observed_labeled_works = {min_labeled} - {labeled}"
        ),
        "coverage_only_deficit_at_current_eligible_count": coverage_only_deficit,
        "coverage_only_deficit_formula": (
            f"ceil({min_coverage:.2f} * confirmatory_eligible_work_count) - observed_labeled_works = "
            f"ceil({min_coverage:.2f} * {eligible}) - {labeled} = {coverage_needed_at_current_count} - {labeled}"
        ),
        "hard_blocker_for_labeled_work_threshold": "minimum_confirmatory_labeled_work_count_absolute_minimum_100",
        "blocking_diagnosis": {
            "candidate_source_under_minimum": eligible < min_candidate,
            "label_coverage_under_minimum": coverage_rate < min_coverage,
            "negative_coverage_under_minimum": negative < min_negative or distinct_negative < min_distinct_negative,
            "labeled_work_absolute_minimum_under_policy": labeled < min_labeled,
            "coverage_rate_under_minimum_on_current_surface": coverage_rate < min_coverage,
            "current_surface_can_be_made_ready_by_labeling_alone": False,
            "reason": reason,
        },
        "recommended_collection_plan": {
            "primary_next_action": "create_larger_fresh_product_candidate_source",
            "secondary_next_action": "prepare_manual_labeling_for_current_and_future_fresh_candidates",
            "minimum_additional_confirmatory_candidate_works_needed": candidate_deficit,
            "minimum_additional_labeled_works_needed_for_policy_absolute_minimum": labeled_abs_deficit,
            "minimum_additional_labeled_works_note": (
                "This is after candidate-source expansion to >=100 eligible works; it is not satisfiable on the current 44-only pool."
            ),
            "coverage_only_additional_labeled_works_needed_on_current_44_surface": coverage_only_deficit,
            "coverage_only_note": "Informational only; this does not unlock readiness while the absolute 100-work thresholds fail.",
            "minimum_negative_works_needed": negative_deficit,
            "minimum_distinct_negative_works_needed": distinct_negative_deficit,
            "label_target": TARGET,
        },
        "labeling_scope_for_current_surface": {
            "current_confirmatory_eligible_works": eligible,
            "already_labeled_works": labeled,
            "unlabeled_confirmatory_eligible_works": unlabeled,
            "note": (
                f"Labeling all {unlabeled} is useful for partial audit practice but insufficient for readiness "
                f"(candidate count < {min_candidate}; absolute labeled minimum {min_labeled}; {negative} negatives)."
            ),
        },
        "future_candidate_source_requirements": {
            "new_or_expanded_snapshot_or_run_after_this_plan": True,
            "new_candidate_work_set_sha256_must_differ_from_disallowed": disallowed_sha,
            "old_217_overlap_must_be_tagged_and_excluded_from_confirmatory_denominators": True,
            "canonical_openalex_work_ids_required": True,
            "frozen_primary_hybrid_arm": primary_arm,
            "frozen_secondary_reporting_arm": secondary_arm,
            "retuning_on_fresh_eval_labels_allowed": False,
        },
        "frozen_hybrid_arms_reference": {
            "primary_confirmatory_arm": primary_arm,
            "secondary_reporting_arm": secondary_arm,
            "baselines_for_future_comparison": baselines,
            "no_retuning": True,
        },
        "allowed_next_stages": [
            "create_fresh_product_candidate_ranking_source_v1",
            "create_fresh_eval_labeling_worksheet_hybrid_v1",
        ],
        "recommended_next_stage": "create_fresh_product_candidate_ranking_source_v1",
        "blocked_next_stages": [
            "execute_hybrid_validation_on_fresh_surface_v1",
            "hybrid_validation_metric_gates_v1",
            "ml-shadow-scorer-v1",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
            "missing_sufficient_fresh_candidate_surface": True,
            "missing_sufficient_fresh_labels": True,
            "missing_negative_labels": True,
        },
        "policy_assertions": {
            "current_surface_can_be_made_ready_by_labeling_alone": False,
            "candidate_source_expansion_required": True,
            "labels_on_current_surface_useful_but_insufficient": True,
            "retuning_on_fresh_eval_labels_allowed": False,
            "shadow_allowed_by_this_plan": False,
            "production_default_allowed_by_this_plan": False,
        },
        "caveats": list(CAVEATS),
    }
    return payload


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}" if abs(value) < 1 else f"{value:.2f}"
    return str(value)


def markdown_from_ml_fresh_eval_labeling_plan_hybrid(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    surface = payload["surface_summary"]
    diagnosis = payload["blocking_diagnosis"]
    plan = payload["recommended_collection_plan"]
    scope = payload["labeling_scope_for_current_surface"]
    lines = [
        f"# Fresh Eval Labeling Plan Hybrid ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This plan explains why the DB-backed fresh hybrid surface is not ready for confirmatory hybrid validation scoring. It is plan-only: no DB query, scoring, training, labeling, worksheet generation, shadow, or production change is performed.",
        "",
        f"- **Surface status:** `{surface['status']}`",
        f"- **Ranking run:** `{surface['ranking_run_id']}`",
        f"- **Snapshot:** `{surface['corpus_snapshot_version']}`",
        f"- **Confirmatory eligible works:** {surface['confirmatory_eligible_work_count']}",
        f"- **Ready for hybrid validation scoring:** {surface['ready_for_hybrid_validation_scoring']}",
        f"- **Current surface can be made ready by labeling alone:** {diagnosis['current_surface_can_be_made_ready_by_labeling_alone']}",
        f"- **Reason:** {diagnosis['reason']}",
        "",
        "## Surface Status",
        "",
        f"- Candidate works: {surface['candidate_work_count']}",
        f"- Candidate SHA: `{surface['candidate_work_set_sha256']}`",
        f"- Disallowed old eval SHA: `{surface['disallowed_eval_work_set_sha256']}`",
        f"- Old-surface overlaps excluded: {surface['excluded_previous_eval_overlap_count']}",
        f"- Labeled works: {surface['confirmatory_labeled_work_count']}",
        f"- Label coverage: {_fmt(surface['label_coverage_rate'])}",
        f"- Positive works: {surface['confirmatory_positive_work_count']}",
        f"- Negative works: {surface['confirmatory_negative_work_count']}",
        "",
        "## Threshold Gap Table",
        "",
        "| Threshold | Observed | Required | Deficit | Status | Notes |",
        "| --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in payload["threshold_gap_analysis"]:
        lines.append(
            f"| `{row['threshold']}` | {_fmt(row['observed'])} | {_fmt(row['required'])} | {_fmt(row['deficit'])} | {row['status']} | {row['notes']} |"
        )
    lines.extend(
        [
            "",
            "## Two Label Deficits",
            "",
            f"- **Absolute labeled-work deficit:** {payload['labeled_work_absolute_deficit']} additional labeled works are needed to meet the policy absolute minimum. Formula: `{payload['labeled_work_absolute_deficit_formula']}`.",
            f"- **Coverage-only deficit on current 44-work surface:** {payload['coverage_only_deficit_at_current_eligible_count']} additional labeled works would reach 60% coverage on this slice. Formula: `{payload['coverage_only_deficit_formula']}`.",
            "- The coverage-only gap is informational. It does not override the absolute 100 labeled-work minimum or the 100 eligible-work candidate-source minimum.",
            "",
            "## Why Labeling Alone Cannot Make This Surface Ready",
            "",
            diagnosis["reason"],
            "",
            "## Current-Surface Labeling Opportunity",
            "",
            f"- Current confirmatory eligible works: {scope['current_confirmatory_eligible_works']}",
            f"- Already labeled works: {scope['already_labeled_works']}",
            f"- Unlabeled confirmatory eligible works: {scope['unlabeled_confirmatory_eligible_works']}",
            f"- Note: {scope['note']}",
            "",
            "## Required Larger Fresh Candidate Source",
            "",
            f"- Minimum additional confirmatory-eligible works needed: {plan['minimum_additional_confirmatory_candidate_works_needed']}",
            f"- Minimum additional labeled works needed for policy absolute minimum: {plan['minimum_additional_labeled_works_needed_for_policy_absolute_minimum']}",
            f"- Minimum negative works needed: {plan['minimum_negative_works_needed']}",
            f"- Minimum distinct negative works needed: {plan['minimum_distinct_negative_works_needed']}",
            "",
            "## Recommended Next Stages",
            "",
        ]
    )
    lines.extend(f"- `{item}`" for item in payload["allowed_next_stages"])
    lines.extend(
        [
            "",
            "## Not Shadow / Not Production",
            "",
            f"- Shadow scoring allowed: {payload['shadow_and_production_blockers']['shadow_scoring_allowed']}",
            f"- Production default allowed: {payload['shadow_and_production_blockers']['production_default_allowed']}",
            f"- Confirmatory validation complete: {payload['shadow_and_production_blockers']['confirmatory_validation_complete']}",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_eval_labeling_plan_hybrid(
    *,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_fresh_eval_labeling_plan_hybrid_payload(
        fresh_eval_surface_path=fresh_eval_surface_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_eval_labeling_plan_hybrid(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLFreshEvalLabelingPlanHybridError",
    "build_ml_fresh_eval_labeling_plan_hybrid_payload",
    "markdown_from_ml_fresh_eval_labeling_plan_hybrid",
    "write_ml_fresh_eval_labeling_plan_hybrid",
]
