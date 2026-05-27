"""Gate evaluator for ml-shadow-scorer-v1 second-surface generalization audit.

This command reads committed JSON artifacts only. It verifies the second fresh
surface audit identity, coverage, formula replay, leakage isolation, material
lift, and continued production/runtime separation. It does not query databases,
rerun scoring, generate embeddings, ingest labels, implement runtime, or enable
online shadow execution.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_gates"
GATES_VERSION = "ml-shadow-scorer-v1-generalization-audit-gates-v1"

AUDIT_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_generalization_audit"
AUDIT_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-surface-generalization-audit-v1"
SECOND_SURFACE_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SECOND_SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"
FRESH_SURFACE_POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
FRESH_SURFACE_POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

RANKING_RUN_ID = "rank-83787b91ef"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
EXPECTED_CANDIDATE_POOL_SHA = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
EXPECTED_POOL_SIZE = 528
EXPECTED_CONFIRMATORY_COUNT = 168
EXPECTED_POOL_ONLY_NON_METRIC_COUNT = 360
EXPECTED_POSITIVE_COUNT = 94
EXPECTED_NEGATIVE_COUNT = 74
EXPECTED_OLD_217_OVERLAP_COUNT = 217
EXPECTED_FIRST_SURFACE_OVERLAP_COUNT = 358
EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT = 360
FORMULA_ID = "hybrid_rank_mean_50_50"
FORMULA_TOLERANCE = 1e-12

PASSED_NEXT_STAGE = "implement_online_shadow_runtime_disabled_by_default"
FAILED_NEXT_STAGE = "write_second_surface_generalization_failure_analysis_v1"

REQUIRED_ROW_FIELDS = (
    "shadow_rank",
    "canonical_openalex_work_id",
    "final_score",
    "audit_embedding_probability_work",
    "final_score_rank_pct",
    "audit_embedding_probability_rank_pct",
    "ml_shadow_scorer_v1_score",
)

CAVEATS = (
    "Gates pass, if observed, means second-surface generalization met the preregistered offline bar.",
    "Gates pass does not enable online shadow execution.",
    "Gates pass does not authorize production default or API/web behavior.",
    "The next allowed step is disabled-by-default runtime implementation only.",
    "Precision@k movements are reported as advisory; the preregistered material lift gate is ROC-AUC/AP.",
    "Discovery JSON may still be stale about missing_generalization_audit_gates until a later sync/rerun; this gates artifact is the source of truth for gate status.",
)


class MLShadowScorerGeneralizationAuditGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerGeneralizationAuditGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerGeneralizationAuditGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerGeneralizationAuditGatesError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerGeneralizationAuditGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerGeneralizationAuditGatesError(f"{name} must be {expected!r}, got {observed!r}")


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    _require_equal(f"{name} metadata.artifact_type", metadata.get("artifact_type"), artifact_type)
    _require_equal(f"{name} metadata.{version_field}", metadata.get(version_field), version)
    return metadata


def _gate(gate_id: str, title: str, passed: bool, observed: Any, rationale: str) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "title": title,
        "status": "pass" if passed else "fail",
        "passed": passed,
        "observed_value": observed,
        "rationale": rationale,
    }


def _validate_audit_identity(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="second-surface-generalization-audit",
        artifact_type=AUDIT_ARTIFACT_TYPE,
        version_field="artifact_version",
        version=AUDIT_ARTIFACT_VERSION,
    )
    required = {
        "generalization_audit_executed": True,
        "generalization_audit_gates_passed": False,
        "recommended_next_stage": "run_ml_shadow_scorer_v1_generalization_audit_gates_v1",
        "metadata.ranking_run_id": RANKING_RUN_ID,
        "metadata.family": FAMILY,
        "metadata.corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "metadata.embedding_version": EMBEDDING_VERSION,
        "metadata.candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
        "audit_scope.candidate_pool_work_count": EXPECTED_POOL_SIZE,
        "audit_scope.confirmatory_metric_work_count": EXPECTED_CONFIRMATORY_COUNT,
        "label_join_summary.joined_label_count": EXPECTED_CONFIRMATORY_COUNT,
        "label_join_summary.positive_count": EXPECTED_POSITIVE_COUNT,
        "label_join_summary.negative_count": EXPECTED_NEGATIVE_COUNT,
        "label_join_summary.conflicting_target_work_group_count": 0,
        "metric_coverage.candidate_pool_work_count": EXPECTED_POOL_SIZE,
        "metric_coverage.shadow_output_row_count": EXPECTED_POOL_SIZE,
        "metric_coverage.confirmatory_metric_work_count": EXPECTED_CONFIRMATORY_COUNT,
        "metric_coverage.pool_only_non_metric_row_count": EXPECTED_POOL_ONLY_NON_METRIC_COUNT,
        "metric_coverage.prior_overlap_rows_scored_but_excluded_from_metric_denominator": True,
        "leakage_report.labels_used_for_scoring": False,
        "leakage_report.scorer_refit_used": False,
        "leakage_report.supervised_fit_used": False,
        "leakage_report.eval_label_weight_tuning_used": False,
        "leakage_report.old_217_overlap_count_in_full_pool": EXPECTED_OLD_217_OVERLAP_COUNT,
        "leakage_report.rank_9f4b2a2084_overlap_count_in_full_pool": EXPECTED_FIRST_SURFACE_OVERLAP_COUNT,
        "leakage_report.combined_prior_surface_overlap_count_in_full_pool": EXPECTED_COMBINED_PRIOR_OVERLAP_COUNT,
    }
    for path, expected in required.items():
        observed = payload.get(path) if "." not in path else _get(payload, path)
        _require_equal(f"audit {path}", observed, expected)
    return metadata


def _validate_rows(rows: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(rows, list):
        raise MLShadowScorerGeneralizationAuditGatesError("shadow_output_rows must be a list")
    if len(rows) != EXPECTED_POOL_SIZE:
        raise MLShadowScorerGeneralizationAuditGatesError(
            f"shadow_output_rows length must be {EXPECTED_POOL_SIZE}, got {len(rows)}"
        )
    missing_field_count = 0
    nonnumeric_count = 0
    duplicate_ids: set[str] = set()
    seen_ids: set[str] = set()
    ranks: list[int] = []
    confirmatory_count = 0
    max_formula_delta = 0.0
    mismatches: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerGeneralizationAuditGatesError(f"shadow_output_rows[{index}] must be an object")
        missing = [field for field in REQUIRED_ROW_FIELDS if field not in row]
        missing_field_count += len(missing)
        if missing:
            raise MLShadowScorerGeneralizationAuditGatesError(
                f"shadow_output_rows[{index}] missing required fields: {missing}"
            )
        canonical = str(row.get("canonical_openalex_work_id") or "").strip()
        if not canonical:
            raise MLShadowScorerGeneralizationAuditGatesError(f"shadow_output_rows[{index}] missing canonical id")
        if canonical in seen_ids:
            duplicate_ids.add(canonical)
        seen_ids.add(canonical)
        try:
            ranks.append(int(row["shadow_rank"]))
        except (TypeError, ValueError) as exc:
            raise MLShadowScorerGeneralizationAuditGatesError(f"shadow_rank for {canonical} must be an integer") from exc
        if row.get("confirmatory_metric_eligible") is True:
            confirmatory_count += 1
        final_rank = _float_or_none(row.get("final_score_rank_pct"))
        learned_rank = _float_or_none(row.get("audit_embedding_probability_rank_pct"))
        score = _float_or_none(row.get("ml_shadow_scorer_v1_score"))
        final_score = _float_or_none(row.get("final_score"))
        learned_probability = _float_or_none(row.get("audit_embedding_probability_work"))
        if None in (final_rank, learned_rank, score, final_score, learned_probability):
            nonnumeric_count += 1
            raise MLShadowScorerGeneralizationAuditGatesError(f"row {canonical} has nonnumeric score fields")
        expected_score = 0.5 * float(final_rank) + 0.5 * float(learned_rank)
        delta = abs(float(score) - expected_score)
        max_formula_delta = max(max_formula_delta, delta)
        if delta > FORMULA_TOLERANCE:
            mismatches.append(
                {
                    "canonical_openalex_work_id": canonical,
                    "observed": score,
                    "expected": expected_score,
                    "delta": delta,
                }
            )
    observed_sha = _work_set_sha256([str(row["canonical_openalex_work_id"]) for row in rows])
    if observed_sha != EXPECTED_CANDIDATE_POOL_SHA:
        raise MLShadowScorerGeneralizationAuditGatesError(
            f"shadow_output_rows candidate SHA mismatch: expected {EXPECTED_CANDIDATE_POOL_SHA}, got {observed_sha}"
        )
    if confirmatory_count != EXPECTED_CONFIRMATORY_COUNT:
        raise MLShadowScorerGeneralizationAuditGatesError(
            f"confirmatory shadow_output_rows count must be {EXPECTED_CONFIRMATORY_COUNT}, got {confirmatory_count}"
        )
    if confirmatory_count != EXPECTED_POOL_SIZE - EXPECTED_POOL_ONLY_NON_METRIC_COUNT:
        raise MLShadowScorerGeneralizationAuditGatesError("confirmatory count must equal pool size minus prior-overlap rows")
    expected_ranks = set(range(1, EXPECTED_POOL_SIZE + 1))
    rank_set = set(ranks)
    rank_complete = rank_set == expected_ranks and len(ranks) == len(rank_set)
    if not rank_complete:
        raise MLShadowScorerGeneralizationAuditGatesError("shadow ranks must be complete and unique 1..pool_size")
    if mismatches:
        raise MLShadowScorerGeneralizationAuditGatesError(
            f"formula replay mismatch beyond {FORMULA_TOLERANCE}: {mismatches[:5]}"
        )
    row_schema = {
        "row_count": len(rows),
        "required_fields_present": missing_field_count == 0,
        "numeric_score_fields_complete": nonnumeric_count == 0,
        "duplicate_canonical_work_id_count": len(duplicate_ids),
        "shadow_rank_unique_1_to_pool_size": rank_complete,
        "confirmatory_metric_eligible_count": confirmatory_count,
        "pool_only_non_metric_row_count": EXPECTED_POOL_SIZE - confirmatory_count,
        "candidate_pool_work_set_sha256": observed_sha,
    }
    formula = {
        "formula_id": FORMULA_ID,
        "replay_tolerance": FORMULA_TOLERANCE,
        "max_abs_formula_delta": max_formula_delta,
        "mismatched_work_count": len(mismatches),
        "rank_completeness_passed": rank_complete,
        "formula_replay_exact": max_formula_delta <= FORMULA_TOLERANCE and not mismatches,
    }
    return row_schema, formula


def _material_lift(payload: Mapping[str, Any], thresholds: Mapping[str, float]) -> dict[str, Any]:
    comparison = _get(payload, f"comparisons_vs_heuristic.{FORMULA_ID}")
    if not isinstance(comparison, Mapping):
        raise MLShadowScorerGeneralizationAuditGatesError("audit missing comparisons_vs_heuristic.hybrid_rank_mean_50_50")
    delta_roc = _float_or_none(comparison.get("delta_roc_auc"))
    delta_ap = _float_or_none(comparison.get("delta_average_precision"))
    material_lift_recomputed = (
        (delta_roc is not None and delta_roc >= thresholds["delta_roc_auc_gte"])
        or (delta_ap is not None and delta_ap >= thresholds["or_delta_average_precision_gte"])
    )
    audit_observed = comparison.get("material_lift_observed") is True
    return {
        "material_lift_observed_in_audit": audit_observed,
        "material_lift_recomputed": material_lift_recomputed,
        "material_lift_gate_passed": audit_observed and material_lift_recomputed,
        "delta_roc_auc": delta_roc,
        "delta_average_precision": delta_ap,
        "thresholds": dict(thresholds),
        "advisory_precision_at_k_deltas": {
            "precision_at_5": comparison.get("delta_precision_at_5"),
            "precision_at_10": comparison.get("delta_precision_at_10"),
            "precision_at_20": comparison.get("delta_precision_at_20"),
            "gate_effect": "reported_only_not_gate_failing",
        },
    }


def _validate_discovery(payload: Mapping[str, Any], audit_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-second-surface",
        artifact_type=SECOND_SURFACE_ARTIFACT_TYPE,
        version_field="surface_version",
        version=SECOND_SURFACE_VERSION,
    )
    _require_equal("discovery status", _get(payload, "discovery_summary.status"), "selected_ready_for_generalization_audit")
    _require_equal(
        "discovery ready flag",
        _get(payload, "readiness_for_generalization_audit.ready_for_generalization_audit_execution"),
        True,
    )
    selected = payload.get("selected_second_surface")
    if not isinstance(selected, Mapping):
        raise MLShadowScorerGeneralizationAuditGatesError("discovery selected_second_surface must be populated")
    identity_paths = (
        "ranking_run_id",
        "family",
        "corpus_snapshot_version",
        "embedding_version",
        "candidate_pool_work_set_sha256",
    )
    for key in identity_paths:
        _require_equal(
            f"discovery/audit identity {key}",
            selected.get(key),
            audit_payload["metadata"].get(key),
        )
    _require_equal("discovery candidate_pool_work_count", selected.get("candidate_pool_work_count"), EXPECTED_POOL_SIZE)
    _require_equal(
        "discovery confirmatory_metric_eligible_work_count",
        selected.get("confirmatory_metric_eligible_work_count"),
        EXPECTED_CONFIRMATORY_COUNT,
    )
    return metadata


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-plan",
        artifact_type=GENERALIZATION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=GENERALIZATION_PLAN_VERSION,
    )
    _require_equal("generalization_audit_plan_defined", payload.get("generalization_audit_plan_defined"), True)
    _require_equal("generalization plan runtime_implementation_authorized", payload.get("runtime_implementation_authorized"), False)
    contract = payload.get("generalization_gate_contract")
    if not isinstance(contract, Mapping) or not contract.get("passes_only_if"):
        raise MLShadowScorerGeneralizationAuditGatesError("generalization audit plan missing gate contract")
    return metadata


def _validate_online_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    _require_equal("online policy runtime_implementation_authorized", payload.get("runtime_implementation_authorized"), False)
    _require_equal("online_shadow_execution_enabled", payload.get("online_shadow_execution_enabled"), False)
    _require_equal("online policy production_default_allowed", payload.get("production_default_allowed"), False)
    return metadata


def _validate_fresh_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, float]]:
    metadata = _validate_identity(
        payload,
        name="fresh-surface-policy",
        artifact_type=FRESH_SURFACE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=FRESH_SURFACE_POLICY_VERSION,
    )
    thresholds = _get(payload, "gate_linkage.material_lift_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLShadowScorerGeneralizationAuditGatesError("fresh surface policy missing material_lift_thresholds")
    roc = _float_or_none(thresholds.get("delta_roc_auc_gte"))
    ap = _float_or_none(thresholds.get("or_delta_average_precision_gte"))
    if roc != 0.03 or ap != 0.02:
        raise MLShadowScorerGeneralizationAuditGatesError("fresh policy material lift thresholds must be 0.03/0.02")
    return metadata, {"delta_roc_auc_gte": float(roc), "or_delta_average_precision_gte": float(ap)}


def _production_plan_observed_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    good = _get(payload, "targets.good_or_acceptable")
    return {
        "overall_status": payload.get("overall_status") or _get(payload, "metadata.overall_status"),
        "production_default_authorized": payload.get("production_default_authorized"),
        "good_or_acceptable_production_eligible": good.get("production_eligible") if isinstance(good, Mapping) else None,
    }


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    observed = _production_plan_observed_fields(payload)
    return (
        observed["overall_status"] == "research_only"
        and observed["production_default_authorized"] is not True
        and observed["good_or_acceptable_production_eligible"] is False
    )


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _validate_identity(
        payload,
        name="production-readiness-plan",
        artifact_type=PRODUCTION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PRODUCTION_PLAN_VERSION,
    )


def build_ml_shadow_scorer_generalization_audit_gates_payload(
    *,
    second_surface_generalization_audit_path: Path,
    generalization_second_surface_path: Path,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    audit_path = Path(second_surface_generalization_audit_path).resolve()
    discovery_path = Path(generalization_second_surface_path).resolve()
    plan_path = Path(generalization_audit_plan_path).resolve()
    online_policy_path = Path(online_shadow_policy_path).resolve()
    fresh_policy_path = Path(fresh_surface_policy_path).resolve()
    production_plan_path = Path(production_readiness_plan_path).resolve()

    audit_payload = _load_json_object(audit_path)
    discovery_payload = _load_json_object(discovery_path)
    plan_payload = _load_json_object(plan_path)
    online_policy_payload = _load_json_object(online_policy_path)
    fresh_policy_payload = _load_json_object(fresh_policy_path)
    production_plan_payload = _load_json_object(production_plan_path)

    audit_metadata = _validate_audit_identity(audit_payload)
    discovery_metadata = _validate_discovery(discovery_payload, audit_payload)
    plan_metadata = _validate_generalization_plan(plan_payload)
    online_policy_metadata = _validate_online_policy(online_policy_payload)
    fresh_policy_metadata, material_thresholds = _validate_fresh_policy(fresh_policy_payload)
    production_plan_metadata = _validate_production_plan(production_plan_payload)

    row_schema, formula_replay = _validate_rows(audit_payload.get("shadow_output_rows"))
    material = _material_lift(audit_payload, material_thresholds)
    production_plan_blocked = _production_plan_blocked(production_plan_payload)
    production_plan_observed = {
        **_production_plan_observed_fields(production_plan_payload),
        "production_plan_blocked": production_plan_blocked,
    }
    shadow_runtime_blocked = (
        audit_payload.get("runtime_implementation_authorized") is False
        and audit_payload.get("online_shadow_execution_enabled") is False
        and audit_payload.get("shadow_scoring_allowed") is False
        and audit_payload.get("production_default_allowed") is False
        and audit_payload.get("api_web_changes_allowed") is not True
        and _get(audit_payload, "shadow_and_production_blockers.runtime_implementation_authorized") is False
        and _get(audit_payload, "shadow_and_production_blockers.online_shadow_execution_enabled") is False
        and _get(audit_payload, "shadow_and_production_blockers.shadow_scoring_allowed") is False
        and _get(audit_payload, "shadow_and_production_blockers.production_default_allowed") is False
    )

    leakage = audit_payload.get("leakage_report") if isinstance(audit_payload.get("leakage_report"), Mapping) else {}
    leakage_ok = (
        leakage.get("labels_used_for_scoring") is False
        and leakage.get("scorer_refit_used") is False
        and leakage.get("supervised_fit_used") is False
        and leakage.get("eval_label_weight_tuning_used") is False
        and leakage.get("train_rows_used") == 0
    )
    prior_exclusion_ok = (
        _get(audit_payload, "metric_coverage.confirmatory_metric_work_count") == EXPECTED_CONFIRMATORY_COUNT
        and _get(audit_payload, "metric_coverage.pool_only_non_metric_row_count") == EXPECTED_POOL_ONLY_NON_METRIC_COUNT
        and _get(audit_payload, "metric_coverage.prior_overlap_rows_scored_but_excluded_from_metric_denominator") is True
        and _get(audit_payload, "leakage_report.old_217_overlap_excluded_from_confirmatory_metrics") is True
        and _get(audit_payload, "leakage_report.first_validated_surface_overlap_excluded_from_confirmatory_metrics") is True
    )

    g01 = (
        audit_metadata.get("ranking_run_id") == RANKING_RUN_ID
        and audit_metadata.get("family") == FAMILY
        and audit_metadata.get("corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION
        and audit_metadata.get("embedding_version") == EMBEDDING_VERSION
        and audit_metadata.get("candidate_pool_work_set_sha256") == EXPECTED_CANDIDATE_POOL_SHA
        and _get(discovery_payload, "selected_second_surface.ranking_run_id") == RANKING_RUN_ID
    )
    g02 = (
        _get(audit_payload, "audit_scope.candidate_pool_work_count") == EXPECTED_POOL_SIZE
        and _get(audit_payload, "audit_scope.confirmatory_metric_work_count") == EXPECTED_CONFIRMATORY_COUNT
        and _get(audit_payload, "label_join_summary.positive_count") == EXPECTED_POSITIVE_COUNT
        and _get(audit_payload, "label_join_summary.negative_count") == EXPECTED_NEGATIVE_COUNT
        and _get(audit_payload, "label_join_summary.conflicting_target_work_group_count") == 0
    )
    g03 = (
        row_schema["row_count"] == EXPECTED_POOL_SIZE
        and row_schema["required_fields_present"]
        and row_schema["numeric_score_fields_complete"]
    )
    g04 = formula_replay["formula_replay_exact"] and formula_replay["rank_completeness_passed"]
    g05 = leakage_ok
    g06 = prior_exclusion_ok
    g07 = material["material_lift_gate_passed"]
    g08 = shadow_runtime_blocked
    g09 = production_plan_blocked
    required = [g01, g02, g03, g04, g05, g06, g07, g08, g09]
    g10 = all(required)

    gate_results = [
        _gate(
            "G01_second_surface_identity",
            "Second Surface Identity",
            g01,
            {
                "ranking_run_id": audit_metadata.get("ranking_run_id"),
                "family": audit_metadata.get("family"),
                "corpus_snapshot_version": audit_metadata.get("corpus_snapshot_version"),
                "embedding_version": audit_metadata.get("embedding_version"),
                "candidate_pool_work_set_sha256": audit_metadata.get("candidate_pool_work_set_sha256"),
            },
            "Run, family, snapshot, embedding, and SHA match the selected second surface.",
        ),
        _gate(
            "G02_candidate_and_label_coverage",
            "Candidate And Label Coverage",
            g02,
            {
                "candidate_pool_work_count": _get(audit_payload, "audit_scope.candidate_pool_work_count"),
                "confirmatory_metric_work_count": _get(audit_payload, "audit_scope.confirmatory_metric_work_count"),
                "positive_count": _get(audit_payload, "label_join_summary.positive_count"),
                "negative_count": _get(audit_payload, "label_join_summary.negative_count"),
                "conflicting_target_work_group_count": _get(
                    audit_payload, "label_join_summary.conflicting_target_work_group_count"
                ),
            },
            "Pool, metric rows, target counts, and conflict counts match the preregistered second-surface contract.",
        ),
        _gate(
            "G03_learned_probability_and_score_coverage",
            "Learned Probability And Score Coverage",
            g03,
            row_schema,
            "Every pool row has learned probability, rank percentile, and shadow score fields.",
        ),
        _gate(
            "G04_formula_replay_exact",
            "Formula Replay Exact",
            g04,
            formula_replay,
            "Frozen 50/50 rank-fusion score replays exactly within tolerance and shadow ranks are complete.",
        ),
        _gate(
            "G05_no_leakage_or_refit",
            "No Leakage Or Refit",
            g05,
            leakage,
            "Labels are metric-only; no refit, supervised fit, tuning, or training rows are used.",
        ),
        _gate(
            "G06_prior_surface_exclusion",
            "Prior Surface Exclusion",
            g06,
            {
                "confirmatory_metric_work_count": _get(audit_payload, "metric_coverage.confirmatory_metric_work_count"),
                "pool_only_non_metric_row_count": _get(audit_payload, "metric_coverage.pool_only_non_metric_row_count"),
                "old_217_overlap_count": _get(audit_payload, "leakage_report.old_217_overlap_count_in_full_pool"),
                "rank_9f4b2a2084_overlap_count": _get(
                    audit_payload, "leakage_report.rank_9f4b2a2084_overlap_count_in_full_pool"
                ),
                "combined_prior_surface_overlap_count": _get(
                    audit_payload, "leakage_report.combined_prior_surface_overlap_count_in_full_pool"
                ),
            },
            "Prior-overlap rows are scored in the full pool but excluded from the confirmatory denominator.",
        ),
        _gate(
            "G07_material_lift_vs_heuristic",
            "Material Lift Vs Heuristic",
            g07,
            material,
            "Material lift gate uses ROC-AUC/AP only; precision@k deltas are advisory.",
        ),
        _gate(
            "G08_shadow_prod_runtime_blocked",
            "Shadow Prod Runtime Blocked",
            g08,
            {
                "runtime_implementation_authorized": audit_payload.get("runtime_implementation_authorized"),
                "online_shadow_execution_enabled": audit_payload.get("online_shadow_execution_enabled"),
                "shadow_scoring_allowed": audit_payload.get("shadow_scoring_allowed"),
                "production_default_allowed": audit_payload.get("production_default_allowed"),
                "api_web_changes_allowed": audit_payload.get("api_web_changes_allowed", False),
            },
            "Online shadow, runtime implementation, production default, and API/web changes remain blocked.",
        ),
        _gate(
            "G09_production_readiness_still_separate",
            "Production Readiness Still Separate",
            g09,
            production_plan_observed,
            "Production readiness remains research-only and separate from shadow generalization gates.",
        ),
        _gate(
            "G10_generalization_audit_gate_decision",
            "Generalization Audit Gate Decision",
            g10,
            {"prior_gate_statuses": ["pass" if value else "fail" for value in required]},
            "Second-surface generalization gates pass iff G01 through G09 pass.",
        ),
    ]

    inputs = [
        _input_record("second_surface_generalization_audit", audit_path, repo_root=root),
        _input_record("generalization_second_surface", discovery_path, repo_root=root),
        _input_record("generalization_audit_plan", plan_path, repo_root=root),
        _input_record("online_shadow_policy", online_policy_path, repo_root=root),
        _input_record("fresh_surface_policy", fresh_policy_path, repo_root=root),
        _input_record("production_readiness_plan", production_plan_path, repo_root=root),
    ]
    blockers = {
        "missing_generalization_audit_on_second_surface": False,
        "missing_generalization_audit_gates": not g10,
        "missing_online_shadow_implementation_disabled_by_default": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "runtime_implementation_authorized": False,
    }
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": gates_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
        "source_generalization_audit_version": audit_metadata.get("artifact_version"),
        "source_second_surface_version": discovery_metadata.get("surface_version"),
        "source_generalization_plan_version": plan_metadata.get("plan_version"),
        "source_online_shadow_policy_version": online_policy_metadata.get("policy_version"),
        "source_fresh_surface_policy_version": fresh_policy_metadata.get("policy_version"),
        "source_production_readiness_plan_version": production_plan_metadata.get("plan_version"),
    }
    return {
        "metadata": metadata,
        "generalization_audit_gates_passed": g10,
        "second_surface_generalization_passed": g10,
        "material_lift_gate_passed": g07,
        "disabled_by_default_runtime_implementation_next_stage_allowed": g10,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "evidence_summary": {
            "candidate_pool_work_count": EXPECTED_POOL_SIZE,
            "confirmatory_metric_work_count": EXPECTED_CONFIRMATORY_COUNT,
            "pool_only_non_metric_row_count": EXPECTED_POOL_ONLY_NON_METRIC_COUNT,
            "positive_count": EXPECTED_POSITIVE_COUNT,
            "negative_count": EXPECTED_NEGATIVE_COUNT,
            "heuristic_metrics": audit_payload.get("arm_metrics", {}).get("heuristic_final_score_baseline"),
            "shadow_hybrid_metrics": audit_payload.get("arm_metrics", {}).get(FORMULA_ID),
            "material_lift": material,
        },
        "gate_results": gate_results,
        "metric_gate_results": {
            "material_lift_gate": material,
            "precision_at_k_advisory": material["advisory_precision_at_k_deltas"],
        },
        "formula_replay_verification": formula_replay,
        "leakage_verification": {
            "leakage_ok": leakage_ok,
            "labels_used_for_scoring": leakage.get("labels_used_for_scoring"),
            "scorer_refit_used": leakage.get("scorer_refit_used"),
            "supervised_fit_used": leakage.get("supervised_fit_used"),
            "eval_label_weight_tuning_used": leakage.get("eval_label_weight_tuning_used"),
            "train_rows_used": leakage.get("train_rows_used"),
        },
        "blocker_update": {
            "missing_generalization_audit_gates_before": True,
            "missing_generalization_audit_gates_after": not g10,
            "source_of_truth": "this gates artifact",
        },
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": PASSED_NEXT_STAGE if g10 else FAILED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return str(value).lower()
    number = _float_or_none(value)
    if number is not None:
        return f"{number:.6f}"
    return str(value)


def markdown_from_ml_shadow_scorer_generalization_audit_gates(payload: Mapping[str, Any]) -> str:
    evidence = payload["evidence_summary"]
    material = payload["metric_gate_results"]["material_lift_gate"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ML Shadow Scorer v1 Generalization Audit Gates ({payload['metadata']['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact evaluates the second-surface ml-shadow-scorer-v1 generalization audit against the preregistered offline gate contract. It does not implement runtime, enable online shadow execution, or authorize production/API behavior.",
        "",
        f"- Generalization audit gates passed: {payload['generalization_audit_gates_passed']}",
        f"- Second-surface generalization passed: {payload['second_surface_generalization_passed']}",
        f"- Material lift gate passed: {payload['material_lift_gate_passed']}",
        f"- Disabled-by-default runtime next stage allowed: {payload['disabled_by_default_runtime_implementation_next_stage_allowed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Evidence Summary",
        "",
        f"- Candidate pool: {evidence['candidate_pool_work_count']}",
        f"- Confirmatory metric rows: {evidence['confirmatory_metric_work_count']}",
        f"- Pool-only prior-overlap rows: {evidence['pool_only_non_metric_row_count']}",
        f"- Label counts: {evidence['positive_count']} positive / {evidence['negative_count']} negative",
        f"- Candidate SHA: `{payload['metadata']['candidate_pool_work_set_sha256']}`",
        "",
        "## Gate Results",
        "",
        "| Gate | Status | Rationale |",
        "| --- | --- | --- |",
    ]
    for gate in payload["gate_results"]:
        lines.append(f"| `{gate['gate_id']}` | {gate['status']} | {gate['rationale']} |")
    lines.extend(
        [
            "",
            "## Material Lift",
            "",
            f"- Delta ROC-AUC: {_fmt(material['delta_roc_auc'])}",
            f"- Delta AP: {_fmt(material['delta_average_precision'])}",
            f"- Thresholds: ROC-AUC >= {_fmt(material['thresholds']['delta_roc_auc_gte'])} OR AP >= {_fmt(material['thresholds']['or_delta_average_precision_gte'])}",
            f"- Audit material_lift_observed: {material['material_lift_observed_in_audit']}",
            f"- Recomputed material lift: {material['material_lift_recomputed']}",
            "",
            "## Advisory Precision@k",
            "",
        ]
    )
    advisory = payload["metric_gate_results"]["precision_at_k_advisory"]
    for key in ("precision_at_5", "precision_at_10", "precision_at_20"):
        lines.append(f"- `{key}` delta: {_fmt(advisory[key])}")
    lines.append(f"- Gate effect: {advisory['gate_effect']}")
    formula = payload["formula_replay_verification"]
    lines.extend(
        [
            "",
            "## Formula Replay",
            "",
            f"- Formula replay exact: {formula['formula_replay_exact']}",
            f"- Max absolute formula delta: {_fmt(formula['max_abs_formula_delta'])}",
            f"- Mismatched work count: {formula['mismatched_work_count']}",
            f"- Rank completeness passed: {formula['rank_completeness_passed']}",
            "",
            "## Remaining Blockers",
            "",
        ]
    )
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_generalization_audit_gates(
    *,
    second_surface_generalization_audit_path: Path,
    generalization_second_surface_path: Path,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_generalization_audit_gates_payload(
        second_surface_generalization_audit_path=second_surface_generalization_audit_path,
        generalization_second_surface_path=generalization_second_surface_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        online_shadow_policy_path=online_shadow_policy_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        gates_version=gates_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_generalization_audit_gates(payload),
        encoding="utf-8",
    )
    return payload
