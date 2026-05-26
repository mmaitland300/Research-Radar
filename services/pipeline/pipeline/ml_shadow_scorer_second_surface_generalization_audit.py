"""Audit ml-shadow-scorer-v1 on the second fresh shadow surface.

This module reads committed JSON artifacts only, computes the frozen
ml-shadow-scorer-v1 rank-fusion score over the second fresh surface, joins v11
labels for confirmatory metrics, and writes an offline audit artifact. It does
not query databases, apply/refit learned scorers, generate embeddings, ingest
labels, or authorize runtime/shadow/production behavior.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping, Sequence

from pipeline.ml_hybrid_scorer_offline_experiment import _arm_metric, _metric_delta, _pr_value
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_v1 import compute_shadow_score_rows
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_generalization_audit"
ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-surface-generalization-audit-v1"

SECOND_SURFACE_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SECOND_SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"
SECOND_SURFACE_STATUS = "selected_ready_for_generalization_audit"
SECOND_SURFACE_NEXT_STAGE = "audit_ml_shadow_scorer_v1_on_second_fresh_surface"

LEARNED_PROBABILITY_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_learned_probability"
LEARNED_PROBABILITY_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-surface-learned-probability-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v11"
SHADOW_SCORER_SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SHADOW_SCORER_SPEC_VERSION = "ml-shadow-scorer-v1-spec"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
FRESH_SURFACE_POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
FRESH_SURFACE_POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
ONLINE_SHADOW_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_SHADOW_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"

RANKING_RUN_ID = "rank-83787b91ef"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
EXPECTED_CANDIDATE_POOL_WORK_COUNT = 528
EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT = 168
EXPECTED_POSITIVE_COUNT = 94
EXPECTED_NEGATIVE_COUNT = 74
EXPECTED_CANDIDATE_SHA = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
EXPECTED_OLD_217_OVERLAP_COUNT = 217
EXPECTED_FIRST_SURFACE_OVERLAP_COUNT = 358
EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT = 360
FORMULA_ID = "hybrid_rank_mean_50_50"
SCORER_ID = "ml-shadow-scorer-v1"
LABEL_REVIEW_POOL_VARIANT = "ml_shadow_scorer_second_surface_generalization_v1"
TARGET = "good_or_acceptable"
RECOMMENDED_NEXT_STAGE = "run_ml_shadow_scorer_v1_generalization_audit_gates_v1"
K_VALUES = (5, 10, 20)

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

CAVEATS = (
    "Offline audit artifact only; this does not pass generalization gates.",
    "The frozen ml-shadow-scorer-v1 formula is applied to committed second-surface probabilities only.",
    "Labels are used only for confirmatory metric evaluation and never for scoring, ranks, weights, or row ordering.",
    "Prior-surface overlap rows remain scored in the full pool but are excluded from confirmatory metrics.",
    "No database access, DB writes, ranking, embedding generation, learned scorer refit, label ingest, shadow runtime, API/web, or production/default changes.",
)


class MLShadowScorerSecondSurfaceGeneralizationAuditError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _quantile(sorted_values: Sequence[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = (len(sorted_values) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return float(sorted_values[lower])
    weight = position - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def _distribution(values: Sequence[Any]) -> dict[str, Any]:
    ordered = sorted(value for value in (_float_or_none(item) for item in values) if value is not None)
    return {
        "min": ordered[0] if ordered else None,
        "p25": _quantile(ordered, 0.25),
        "median": _quantile(ordered, 0.5),
        "p75": _quantile(ordered, 0.75),
        "max": ordered[-1] if ordered else None,
        "mean": float(mean(ordered)) if ordered else None,
        "count": len(ordered),
    }


def _dataset_version(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("dataset_version") or _get(payload, "metadata.dataset_version")
    return str(value) if value else None


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _validate_discovery(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-second-surface")
    _require_equal("discovery metadata.artifact_type", metadata.get("artifact_type"), SECOND_SURFACE_ARTIFACT_TYPE)
    _require_equal("discovery metadata.surface_version", metadata.get("surface_version"), SECOND_SURFACE_VERSION)
    _require_equal("discovery_summary.status", _get(payload, "discovery_summary.status"), SECOND_SURFACE_STATUS)
    _require_equal("recommended_next_stage", payload.get("recommended_next_stage"), SECOND_SURFACE_NEXT_STAGE)
    _require_equal(
        "readiness_for_generalization_audit.ready_for_generalization_audit_execution",
        _get(payload, "readiness_for_generalization_audit.ready_for_generalization_audit_execution"),
        True,
    )
    selected = payload.get("selected_second_surface")
    if not isinstance(selected, Mapping):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError("selected_second_surface must be populated")
    expected_selected = {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
    }
    for key, expected in expected_selected.items():
        _require_equal(f"selected_second_surface.{key}", selected.get(key), expected)
    threshold_check = payload.get("threshold_check") or selected.get("threshold_check")
    if not isinstance(threshold_check, Mapping) or not threshold_check:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError("discovery threshold_check must be populated")
    failed = [
        str(key)
        for key, value in threshold_check.items()
        if not isinstance(value, Mapping) or value.get("passed") is not True
    ]
    if failed:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"discovery threshold checks failed: {failed}")
    overlap = payload.get("overlap_report")
    if not isinstance(overlap, Mapping):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError("discovery overlap_report must be populated")
    expected_overlap = {
        "old_217_overlap_count": EXPECTED_OLD_217_OVERLAP_COUNT,
        "rank_9f4b2a2084_overlap_count": EXPECTED_FIRST_SURFACE_OVERLAP_COUNT,
        "combined_prior_surface_overlap_count": EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT,
        "confirmatory_denominator_excludes_prior_overlaps": True,
    }
    for key, expected in expected_overlap.items():
        _require_equal(f"overlap_report.{key}", overlap.get(key), expected)
    return metadata


def _validate_learned_probability(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="learned-probability-artifact")
    _require_equal("learned probability metadata.artifact_type", metadata.get("artifact_type"), LEARNED_PROBABILITY_ARTIFACT_TYPE)
    _require_equal(
        "learned probability metadata.artifact_version",
        metadata.get("artifact_version"),
        LEARNED_PROBABILITY_ARTIFACT_VERSION,
    )
    _require_equal("learned probability execution_summary.status", _get(payload, "execution_summary.status"), "succeeded")
    expected = {
        "execution_summary.candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "execution_summary.output_row_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "execution_summary.learned_probability_coverage_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "execution_summary.missing_learned_probability_count": 0,
        "coverage_summary.learned_probability_coverage_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "coverage_summary.missing_probability_count": 0,
        "metadata.ranking_run_id": RANKING_RUN_ID,
        "metadata.family": FAMILY,
        "metadata.corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "metadata.embedding_version": EMBEDDING_VERSION,
        "metadata.candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
    }
    for path, value in expected.items():
        _require_equal(f"learned probability {path}", _get(payload, path), value)
    rows = payload.get("candidate_work_scores")
    if not isinstance(rows, list) or len(rows) != EXPECTED_CANDIDATE_POOL_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            f"candidate_work_scores must contain {EXPECTED_CANDIDATE_POOL_WORK_COUNT} rows"
        )
    work_ids = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"candidate_work_scores[{index}] must be object")
        canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
        if not canonical:
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
                f"candidate_work_scores[{index}] missing canonical_openalex_work_id"
            )
        if _float_or_none(row.get("final_score")) is None:
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError(f"candidate row {canonical} missing final_score")
        if _float_or_none(row.get("audit_embedding_probability_work")) is None:
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
                f"candidate row {canonical} missing audit_embedding_probability_work"
            )
        work_ids.append(canonical)
    observed_sha = _work_set_sha256(work_ids)
    _require_equal("recomputed candidate_work_scores SHA", observed_sha, EXPECTED_CANDIDATE_SHA)
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, Mapping[str, Any]], dict[str, Any]]:
    metadata = _metadata(payload, name="label-dataset")
    _require_equal("label dataset_version", _dataset_version(payload), LABEL_DATASET_VERSION)
    ingest = _get(payload, "metadata.shadow_generalization_second_surface_v1_ingest")
    if not isinstance(ingest, Mapping):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            "label dataset missing shadow_generalization_second_surface_v1_ingest metadata"
        )
    expected_ingest = {
        "label_thresholds_passed": True,
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
        "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT,
        "labeled_count": EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT,
        "positive_count": EXPECTED_POSITIVE_COUNT,
        "negative_count": EXPECTED_NEGATIVE_COUNT,
    }
    for key, expected in expected_ingest.items():
        _require_equal(f"label ingest {key}", ingest.get(key), expected)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError("label dataset rows must be a list")
    selected_rows = [
        row
        for row in rows
        if isinstance(row, Mapping)
        and row.get("review_pool_variant") == LABEL_REVIEW_POOL_VARIANT
        and row.get("ranking_run_id") == RANKING_RUN_ID
    ]
    if len(selected_rows) != EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            f"expected {EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT} v11 shadow generalization labels, got {len(selected_rows)}"
        )
    labels_by_work: dict[str, Mapping[str, Any]] = {}
    conflicts: list[str] = []
    relevance_counts = {"good": 0, "acceptable": 0, "miss": 0, "irrelevant": 0}
    positives = 0
    negatives = 0
    for row in selected_rows:
        canonical = _canonical_from_value(row.get("work_id") or row.get("openalex_work_id") or row.get("paper_id"))
        if not canonical:
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError("label row missing canonical work_id")
        if canonical in labels_by_work:
            conflicts.append(canonical)
            continue
        target_value = row.get("good_or_acceptable")
        if not isinstance(target_value, bool):
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
                f"label row {canonical} missing boolean good_or_acceptable"
            )
        relevance = str(row.get("relevance_label") or "")
        if relevance not in relevance_counts:
            raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
                f"label row {canonical} has unsupported relevance_label {relevance!r}"
            )
        relevance_counts[relevance] += 1
        if target_value:
            positives += 1
        else:
            negatives += 1
        labels_by_work[canonical] = row
    if conflicts:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            f"conflicting or duplicate v11 confirmatory labels: {sorted(conflicts)}"
        )
    if len(labels_by_work) != EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError("confirmatory labels must be unique by canonical work")
    _require_equal("v11 positive label count", positives, EXPECTED_POSITIVE_COUNT)
    _require_equal("v11 negative label count", negatives, EXPECTED_NEGATIVE_COUNT)
    label_summary = {
        "label_dataset_version": LABEL_DATASET_VERSION,
        "review_pool_variant": LABEL_REVIEW_POOL_VARIANT,
        "joined_label_count": len(labels_by_work),
        "positive_count": positives,
        "negative_count": negatives,
        "relevance_label_counts": relevance_counts,
        "good_or_acceptable_target": TARGET,
        "conflicting_target_work_group_count": 0,
    }
    return metadata, labels_by_work, label_summary


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="shadow-scorer-spec")
    _require_equal("spec metadata.artifact_type", metadata.get("artifact_type"), SHADOW_SCORER_SPEC_ARTIFACT_TYPE)
    _require_equal("spec metadata.spec_version", metadata.get("spec_version"), SHADOW_SCORER_SPEC_VERSION)
    _require_equal("spec scoring_formula.formula_id", _get(payload, "scoring_formula.formula_id"), FORMULA_ID)
    _require_equal("spec shadow_scoring_allowed", payload.get("shadow_scoring_allowed"), False)
    _require_equal("spec production_default_allowed", payload.get("production_default_allowed"), False)
    return metadata


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-audit-plan")
    _require_equal("generalization plan artifact_type", metadata.get("artifact_type"), GENERALIZATION_PLAN_ARTIFACT_TYPE)
    _require_equal("generalization plan plan_version", metadata.get("plan_version"), GENERALIZATION_PLAN_VERSION)
    _require_equal("generalization_audit_plan_defined", payload.get("generalization_audit_plan_defined"), True)
    _require_equal("generalization plan runtime_implementation_authorized", payload.get("runtime_implementation_authorized"), False)
    return metadata


def _validate_fresh_surface_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, float]]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    _require_equal("fresh policy artifact_type", metadata.get("artifact_type"), FRESH_SURFACE_POLICY_ARTIFACT_TYPE)
    _require_equal("fresh policy policy_version", metadata.get("policy_version"), FRESH_SURFACE_POLICY_VERSION)
    thresholds = _get(payload, "gate_linkage.material_lift_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError("fresh policy missing material_lift_thresholds")
    roc = _float_or_none(thresholds.get("delta_roc_auc_gte"))
    ap = _float_or_none(thresholds.get("or_delta_average_precision_gte"))
    if roc != 0.03 or ap != 0.02:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            "fresh policy material lift thresholds must be 0.03 ROC-AUC or 0.02 AP"
        )
    return metadata, {"delta_roc_auc_gte": float(roc), "or_delta_average_precision_gte": float(ap)}


def _validate_online_shadow_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="online-shadow-policy")
    _require_equal("online policy artifact_type", metadata.get("artifact_type"), ONLINE_SHADOW_POLICY_ARTIFACT_TYPE)
    _require_equal("online policy policy_version", metadata.get("policy_version"), ONLINE_SHADOW_POLICY_VERSION)
    _require_equal("online policy runtime_implementation_authorized", payload.get("runtime_implementation_authorized"), False)
    return metadata


def _assign_ranks(rows: Sequence[Mapping[str, Any]], *, field: str) -> dict[str, int]:
    ranked = sorted(rows, key=lambda row: (-float(row[field]), str(row["canonical_openalex_work_id"])))
    return {str(row["canonical_openalex_work_id"]): index for index, row in enumerate(ranked, start=1)}


def _top_k_overlap(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    shadow_sorted = sorted(
        rows, key=lambda row: (-float(row["ml_shadow_scorer_v1_score"]), str(row["canonical_openalex_work_id"]))
    )
    heuristic_sorted = sorted(rows, key=lambda row: (-float(row["final_score"]), str(row["canonical_openalex_work_id"])))
    summary: dict[str, Any] = {}
    for k in K_VALUES:
        shadow_top = [str(row["canonical_openalex_work_id"]) for row in shadow_sorted[:k]]
        heuristic_top = [str(row["canonical_openalex_work_id"]) for row in heuristic_sorted[:k]]
        overlap = sorted(set(shadow_top) & set(heuristic_top))
        union = set(shadow_top) | set(heuristic_top)
        summary[str(k)] = {
            "k": k,
            "overlap_count": len(overlap),
            "jaccard": (len(overlap) / len(union)) if union else None,
            "shadow_top_k": shadow_top,
            "heuristic_top_k": heuristic_top,
            "overlap_work_ids": overlap,
        }
    return summary


def _rank_displacement_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = sorted(abs(int(row["shadow_rank"]) - int(row["heuristic_rank"])) for row in rows)
    if not values:
        return {"count": 0, "mean": None, "median": None, "max": None, "p90": None}
    return {
        "count": len(values),
        "mean": float(mean(values)),
        "median": float(median(values)),
        "max": int(values[-1]),
        "p90": _quantile([float(value) for value in values], 0.9),
    }


def _comparison(metrics: Mapping[str, Any], baseline: Mapping[str, Any], thresholds: Mapping[str, float]) -> dict[str, Any]:
    delta_roc = _metric_delta(metrics.get("roc_auc_mann_whitney"), baseline.get("roc_auc_mann_whitney"))
    delta_ap = _metric_delta(metrics.get("average_precision"), baseline.get("average_precision"))
    delta_p5 = _metric_delta(_pr_value(metrics, 5), _pr_value(baseline, 5))
    delta_p10 = _metric_delta(_pr_value(metrics, 10), _pr_value(baseline, 10))
    delta_p20 = _metric_delta(_pr_value(metrics, 20), _pr_value(baseline, 20))
    material = (
        (delta_roc is not None and delta_roc >= thresholds["delta_roc_auc_gte"])
        or (delta_ap is not None and delta_ap >= thresholds["or_delta_average_precision_gte"])
    )
    return {
        "primary_arm_id": FORMULA_ID,
        "baseline_arm_id": "heuristic_final_score_baseline",
        "delta_roc_auc": delta_roc,
        "delta_average_precision": delta_ap,
        "delta_precision_at_5": delta_p5,
        "delta_precision_at_10": delta_p10,
        "delta_precision_at_20": delta_p20,
        "material_lift_thresholds": dict(thresholds),
        "material_lift_observed": material,
        "generalization_gates_passed": False,
        "gates_not_run_in_this_artifact": True,
    }


def _blocked_actions() -> list[str]:
    return [
        "production_default_change",
        "api_web_change",
        "online_shadow_execution",
        "runtime_implementation",
        "ranking_run_creation",
        "embedding_generation",
        "learned_scorer_refit",
        "learned_probability_generation",
        "label_ingest",
        "database_access_or_writes",
        "user_visible_ranking_change",
    ]


def _build_shadow_rows(
    candidate_work_scores: Sequence[Mapping[str, Any]],
    labels_by_work: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scored = compute_shadow_score_rows(candidate_work_scores)
    source_by_work = {str(row["canonical_openalex_work_id"]): row for row in candidate_work_scores}
    shadow_ranks = _assign_ranks(scored, field="ml_shadow_scorer_v1_score")
    heuristic_ranks = _assign_ranks(scored, field="final_score")
    output_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    for row in scored:
        canonical = str(row["canonical_openalex_work_id"])
        source = source_by_work[canonical]
        label_row = labels_by_work.get(canonical)
        is_metric = label_row is not None
        label_any_positive = bool(label_row["good_or_acceptable"]) if label_row is not None else None
        arm_scores = {
            "heuristic_final_score_baseline": float(row["final_score"]),
            FORMULA_ID: float(row["ml_shadow_scorer_v1_score"]),
        }
        enriched = {
            "shadow_rank": shadow_ranks[canonical],
            "heuristic_rank": heuristic_ranks[canonical],
            "canonical_openalex_work_id": canonical,
            "title": row.get("title") or source.get("title"),
            "year": source.get("year"),
            "ranking_run_id": row.get("ranking_run_id") or source.get("ranking_run_id"),
            "family": row.get("family") or source.get("family"),
            "corpus_snapshot_version": source.get("corpus_snapshot_version"),
            "final_score": float(row["final_score"]),
            "audit_embedding_probability_work": float(row["audit_embedding_probability_work"]),
            "final_score_rank_pct": float(row["final_score_rank_pct"]),
            "audit_embedding_probability_rank_pct": float(row["audit_embedding_probability_rank_pct"]),
            "ml_shadow_scorer_v1_score": float(row["ml_shadow_scorer_v1_score"]),
            "embedding_version": source.get("embedding_version"),
            "scorer_version": source.get("scorer_version"),
            "candidate_pool_work_set_sha256": source.get("candidate_pool_work_set_sha256"),
            "confirmatory_metric_eligible": is_metric,
            "label_any_positive_not_used_for_scoring": True,
            "labels_used_for_scoring": False,
            "arm_scores": arm_scores,
        }
        if label_row is not None:
            enriched.update(
                {
                    "label_any_positive": label_any_positive,
                    "relevance_label": label_row.get("relevance_label"),
                    "novelty_label": label_row.get("novelty_label"),
                    "bridge_like_label": label_row.get("bridge_like_label"),
                    "review_pool_variant": label_row.get("review_pool_variant"),
                }
            )
            metric_rows.append(enriched)
        output_rows.append(enriched)
    output_rows.sort(
        key=lambda item: (-float(item["ml_shadow_scorer_v1_score"]), str(item["canonical_openalex_work_id"]))
    )
    metric_rows.sort(
        key=lambda item: (-float(item["ml_shadow_scorer_v1_score"]), str(item["canonical_openalex_work_id"]))
    )
    return output_rows, metric_rows


def build_ml_shadow_scorer_second_surface_generalization_audit_payload(
    *,
    generalization_second_surface_path: Path,
    learned_probability_artifact_path: Path,
    label_dataset_path: Path,
    shadow_scorer_spec_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    online_shadow_policy_path: Path,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    discovery_path = Path(generalization_second_surface_path).resolve()
    learned_path = Path(learned_probability_artifact_path).resolve()
    labels_path = Path(label_dataset_path).resolve()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    online_policy_path = Path(online_shadow_policy_path).resolve()

    discovery_payload = _load_json_object(discovery_path)
    learned_payload = _load_json_object(learned_path)
    label_payload = _load_json_object(labels_path)
    spec_payload = _load_json_object(spec_path)
    audit_plan_payload = _load_json_object(audit_plan_path)
    fresh_policy_payload = _load_json_object(policy_path)
    online_policy_payload = _load_json_object(online_policy_path)

    discovery_metadata = _validate_discovery(discovery_payload)
    learned_metadata = _validate_learned_probability(learned_payload)
    label_metadata, labels_by_work, label_summary = _validate_label_dataset(label_payload)
    spec_metadata = _validate_spec(spec_payload)
    audit_plan_metadata = _validate_generalization_plan(audit_plan_payload)
    fresh_policy_metadata, material_thresholds = _validate_fresh_surface_policy(fresh_policy_payload)
    online_policy_metadata = _validate_online_shadow_policy(online_policy_payload)

    candidate_rows = learned_payload["candidate_work_scores"]
    shadow_output_rows, metric_rows = _build_shadow_rows(candidate_rows, labels_by_work)
    metric_work_ids = {row["canonical_openalex_work_id"] for row in metric_rows}
    if len(metric_rows) != EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            f"metric rows must contain {EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT} joined confirmatory rows"
        )
    missing_metric_labels = sorted(set(labels_by_work) - metric_work_ids)
    if missing_metric_labels:
        raise MLShadowScorerSecondSurfaceGeneralizationAuditError(
            f"confirmatory labels not present in candidate pool: {missing_metric_labels[:10]}"
        )

    arm_metrics = {
        "heuristic_final_score_baseline": _arm_metric(
            arm_id="heuristic_final_score_baseline",
            candidates=shadow_output_rows,
            metric_rows=metric_rows,
        ),
        FORMULA_ID: _arm_metric(arm_id=FORMULA_ID, candidates=shadow_output_rows, metric_rows=metric_rows),
    }
    comparison = _comparison(arm_metrics[FORMULA_ID], arm_metrics["heuristic_final_score_baseline"], material_thresholds)

    inputs = [
        _input_record("generalization_second_surface", discovery_path, repo_root=root),
        _input_record("learned_probability_artifact", learned_path, repo_root=root),
        _input_record("label_dataset", labels_path, repo_root=root),
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("online_shadow_policy", online_policy_path, repo_root=root),
    ]

    selected = discovery_payload["selected_second_surface"]
    overlap = discovery_payload["overlap_report"]
    label_coverage = discovery_payload["label_coverage"]["work_level"]
    learned_coverage = learned_payload["coverage_summary"]
    metric_positive = sum(1 for row in metric_rows if row.get("label_any_positive") is True)
    metric_negative = len(metric_rows) - metric_positive

    payload: dict[str, Any] = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "artifact_version": artifact_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "scorer_id": SCORER_ID,
            "formula_id": FORMULA_ID,
            "source_label_dataset_version": _dataset_version(label_payload),
            "source_learned_probability_artifact_version": learned_metadata.get("artifact_version"),
            "source_second_surface_version": discovery_metadata.get("surface_version"),
            "source_shadow_scorer_spec_version": spec_metadata.get("spec_version"),
            "source_generalization_plan_version": audit_plan_metadata.get("plan_version"),
            "source_fresh_surface_policy_version": fresh_policy_metadata.get("policy_version"),
            "source_online_shadow_policy_version": online_policy_metadata.get("policy_version"),
            "execution_mode": "offline_audit_file_only",
            "labels_used_for_scoring": False,
            "generalization_audit_executed": True,
            "generalization_audit_gates_passed": False,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "generalization_audit_executed": True,
        "generalization_audit_gates_passed": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "audit_scope": {
            "ranking_run_id": selected["ranking_run_id"],
            "family": selected["family"],
            "corpus_snapshot_version": selected["corpus_snapshot_version"],
            "embedding_version": selected["embedding_version"],
            "candidate_pool_work_count": selected["candidate_pool_work_count"],
            "confirmatory_metric_work_count": EXPECTED_CONFIRMATORY_METRIC_WORK_COUNT,
            "metric_denominator": "v11 shadow-generalization confirmatory worksheet rows only",
            "formula_id": FORMULA_ID,
            "baseline_arm_id": "heuristic_final_score_baseline",
        },
        "evidence_summary": {
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "learned_probability_coverage": {
                "learned_probability_coverage_count": learned_coverage["learned_probability_coverage_count"],
                "missing_probability_count": learned_coverage["missing_probability_count"],
                "source_artifact_version": learned_metadata.get("artifact_version"),
            },
            "label_coverage": {
                "confirmatory_labeled_work_count": label_coverage["confirmatory_labeled_work_count"],
                "confirmatory_positive_work_count": label_coverage["confirmatory_positive_work_count"],
                "confirmatory_negative_work_count": label_coverage["confirmatory_negative_work_count"],
                "label_coverage_rate": label_coverage["label_coverage_rate"],
                "conflicting_target_work_group_count": label_coverage["conflicting_target_work_group_count"],
            },
            "prior_surface_overlap": {
                "old_217_overlap_count": overlap["old_217_overlap_count"],
                "rank_9f4b2a2084_overlap_count": overlap["rank_9f4b2a2084_overlap_count"],
                "combined_prior_surface_overlap_count": overlap["combined_prior_surface_overlap_count"],
            },
        },
        "label_join_summary": {
            **label_summary,
            "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "metric_rows_joined": len(metric_rows),
            "join_key": "v11 work_id == candidate_work_scores canonical_openalex_work_id",
            "labels_used_for_scoring": False,
            "metric_positive_count": metric_positive,
            "metric_negative_count": metric_negative,
        },
        "metric_coverage": {
            "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "shadow_output_row_count": len(shadow_output_rows),
            "confirmatory_metric_work_count": len(metric_rows),
            "pool_only_non_metric_row_count": len(shadow_output_rows) - len(metric_rows),
            "metric_positive_count": metric_positive,
            "metric_negative_count": metric_negative,
            "target": TARGET,
            "prior_overlap_rows_scored_but_excluded_from_metric_denominator": True,
        },
        "score_distributions": {
            "ml_shadow_scorer_v1_score": _distribution(
                [row["ml_shadow_scorer_v1_score"] for row in shadow_output_rows]
            ),
            "final_score": _distribution([row["final_score"] for row in shadow_output_rows]),
            "audit_embedding_probability_work": _distribution(
                [row["audit_embedding_probability_work"] for row in shadow_output_rows]
            ),
        },
        "top_k_overlap_with_heuristic": _top_k_overlap(shadow_output_rows),
        "rank_displacement_summary": _rank_displacement_summary(shadow_output_rows),
        "arm_metrics": arm_metrics,
        "comparisons_vs_heuristic": {FORMULA_ID: comparison},
        "confirmatory_decision_inputs": {
            "primary_confirmatory_arm": FORMULA_ID,
            "baseline_arm": "heuristic_final_score_baseline",
            "material_lift_thresholds": material_thresholds,
            "metric_rows_count": len(metric_rows),
            "positive_count": metric_positive,
            "negative_count": metric_negative,
            "material_lift_observed": comparison["material_lift_observed"],
            "generalization_audit_gates_not_run": True,
            "generalization_audit_gates_passed": False,
        },
        "shadow_output_rows": shadow_output_rows,
        "leakage_report": {
            "old_217_overlap_excluded_from_confirmatory_metrics": True,
            "first_validated_surface_overlap_excluded_from_confirmatory_metrics": True,
            "old_217_overlap_count_in_full_pool": EXPECTED_OLD_217_OVERLAP_COUNT,
            "rank_9f4b2a2084_overlap_count_in_full_pool": EXPECTED_FIRST_SURFACE_OVERLAP_COUNT,
            "combined_prior_surface_overlap_count_in_full_pool": EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT,
            "train_rows_used": 0,
            "supervised_fit_used": False,
            "eval_label_weight_tuning_used": False,
            "scorer_refit_used": False,
            "labels_used_for_scoring": False,
        },
        "blocked_actions": _blocked_actions(),
        "shadow_and_production_blockers": {
            "missing_generalization_audit_on_second_surface": False,
            "missing_generalization_audit_gates": True,
            "missing_online_shadow_implementation_disabled_by_default": True,
            "missing_shadow_runtime_isolation_verification": True,
            "missing_production_readiness_authorization": True,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }
    return payload


def _fmt(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    number = _float_or_none(value)
    if number is not None:
        return f"{number:.6f}"
    return "" if value is None else str(value)


def markdown_from_ml_shadow_scorer_second_surface_generalization_audit(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    scope = payload["audit_scope"]
    labels = payload["label_join_summary"]
    comparison = payload["comparisons_vs_heuristic"][FORMULA_ID]
    arms = payload["arm_metrics"]
    heuristic = arms["heuristic_final_score_baseline"]
    hybrid = arms[FORMULA_ID]
    leakage = payload["leakage_report"]
    blockers = payload["shadow_and_production_blockers"]
    top20 = payload["shadow_output_rows"][:20]
    lines = [
        f"# Second-Surface ml-shadow-scorer-v1 Generalization Audit ({metadata['artifact_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact audits the frozen ml-shadow-scorer-v1 formula on the selected second fresh surface. It prepares evidence for later gates only; it does not pass gates or authorize runtime, online shadowing, API/web behavior, or production defaults.",
        "",
        f"- Ranking run: `{scope['ranking_run_id']}`",
        f"- Family: `{scope['family']}`",
        f"- Candidate pool: {scope['candidate_pool_work_count']}",
        f"- Confirmatory metric works: {scope['confirmatory_metric_work_count']}",
        f"- Candidate SHA: `{metadata['candidate_pool_work_set_sha256']}`",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Confirmatory Denominator",
        "",
        f"- Joined v11 labels: {labels['metric_rows_joined']} ({labels['metric_positive_count']} positive, {labels['metric_negative_count']} negative)",
        f"- Review pool variant: `{labels['review_pool_variant']}`",
        f"- Metric denominator: {scope['metric_denominator']}",
        "- Labels used for scoring: false",
        "",
        "## Learned Probability Coverage",
        "",
        f"- Coverage: {payload['evidence_summary']['learned_probability_coverage']['learned_probability_coverage_count']} / {scope['candidate_pool_work_count']}",
        f"- Missing probabilities: {payload['evidence_summary']['learned_probability_coverage']['missing_probability_count']}",
        "",
        "## Metrics Vs Heuristic",
        "",
        "| Arm | ROC-AUC | AP | P@5 | P@10 | P@20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for arm_id, metrics in (("heuristic_final_score_baseline", heuristic), (FORMULA_ID, hybrid)):
        lines.append(
            f"| `{arm_id}` | {_fmt(metrics.get('roc_auc_mann_whitney'))} | "
            f"{_fmt(metrics.get('average_precision'))} | "
            f"{_fmt(_pr_value(metrics, 5))} | {_fmt(_pr_value(metrics, 10))} | {_fmt(_pr_value(metrics, 20))} |"
        )
    lines.extend(
        [
            "",
            "## Material Lift",
            "",
            f"- Delta ROC-AUC: {_fmt(comparison['delta_roc_auc'])}",
            f"- Delta AP: {_fmt(comparison['delta_average_precision'])}",
            f"- Delta P@5 / P@10 / P@20: {_fmt(comparison['delta_precision_at_5'])} / {_fmt(comparison['delta_precision_at_10'])} / {_fmt(comparison['delta_precision_at_20'])}",
            f"- Material lift observed: {comparison['material_lift_observed']}",
            "- Generalization gates passed: false (gates not run in this artifact)",
            "",
            "## Top 20 Shadow Preview",
            "",
            "| Rank | Work | Score | Heuristic rank | Label metric row |",
            "| ---: | --- | ---: | ---: | --- |",
        ]
    )
    for row in top20:
        lines.append(
            f"| {row['shadow_rank']} | `{row['canonical_openalex_work_id']}` | "
            f"{_fmt(row['ml_shadow_scorer_v1_score'])} | {row['heuristic_rank']} | "
            f"{row['confirmatory_metric_eligible']} |"
        )
    displacement = payload["rank_displacement_summary"]
    lines.extend(
        [
            "",
            "## Top-k And Rank Displacement",
            "",
            f"- Top-20 overlap with heuristic: {payload['top_k_overlap_with_heuristic']['20']['overlap_count']}",
            f"- Mean absolute rank displacement: {_fmt(displacement['mean'])}",
            f"- Median absolute rank displacement: {_fmt(displacement['median'])}",
            f"- P90 absolute rank displacement: {_fmt(displacement['p90'])}",
            f"- Max absolute rank displacement: {_fmt(displacement['max'])}",
            "",
            "## Leakage Report",
            "",
            f"- Old 217 overlap excluded from confirmatory metrics: {leakage['old_217_overlap_excluded_from_confirmatory_metrics']}",
            f"- First validated surface overlap excluded from confirmatory metrics: {leakage['first_validated_surface_overlap_excluded_from_confirmatory_metrics']}",
            f"- Full-pool prior overlap counts: old 217 = {leakage['old_217_overlap_count_in_full_pool']}, first surface = {leakage['rank_9f4b2a2084_overlap_count_in_full_pool']}, combined = {leakage['combined_prior_surface_overlap_count_in_full_pool']}",
            f"- Train rows used: {leakage['train_rows_used']}",
            f"- Scorer refit used: {leakage['scorer_refit_used']}",
            f"- Labels used for scoring: {leakage['labels_used_for_scoring']}",
            "",
            "## Remaining Blockers",
            "",
        ]
    )
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_second_surface_generalization_audit(
    *,
    generalization_second_surface_path: Path,
    learned_probability_artifact_path: Path,
    label_dataset_path: Path,
    shadow_scorer_spec_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_second_surface_generalization_audit_payload(
        generalization_second_surface_path=generalization_second_surface_path,
        learned_probability_artifact_path=learned_probability_artifact_path,
        label_dataset_path=label_dataset_path,
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        online_shadow_policy_path=online_shadow_policy_path,
        artifact_version=artifact_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_surface_generalization_audit(payload),
        encoding="utf-8",
    )
    return payload
