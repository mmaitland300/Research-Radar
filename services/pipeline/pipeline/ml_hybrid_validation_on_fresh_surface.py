"""Execute hybrid validation on the fresh hybrid eval surface.

This command applies the frozen holdout audit embedding scorer to an already
materialized fresh eval surface, computes pre-registered hybrid rank-fusion
arms, and writes an offline audit artifact. It is SELECT-only against local
Postgres and does not train, refit, generate embeddings, rerank, import labels,
or authorize shadow/prod.
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_fresh_eval_surface_hybrid_materialize import (
    _database_url_from_env,
    assert_local_database_url,
)
from pipeline.ml_hybrid_scorer_offline_experiment import (
    EXPECTED_ARMS,
    HYBRID_ARM_IDS,
    K_VALUES as OFFLINE_K_VALUES,
    _arm_metric,
    _arm_scores,
    _metric_delta,
    _pr_value,
    _rank_percentiles,
)
from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.ml_offline_audit_embedding_scorer_export import score_audit_embedding_probability
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_hybrid_validation_on_fresh_surface"
VALIDATION_VERSION = "ml-hybrid-validation-on-fresh-surface-v1"
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v10"
SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
SCORER_FIT_MODE = "holdout_bound_train_only"
EMBEDDINGS_ARTIFACT_TYPE = "ml_fresh_hybrid_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-fresh-hybrid-snapshot-embeddings-v1"
EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-fresh-hybrid-v1-20260518"
RANKING_RUN_ID = "rank-9f4b2a2084"
FAMILY = "emerging"
PRIMARY_CONFIRMATORY_ARM = "hybrid_rank_mean_50_50"
SECONDARY_REPORTING_ARM = "hybrid_rank_mean_25_75_heuristic"
K_VALUES = OFFLINE_K_VALUES
TARGET = "good_or_acceptable"

DEFAULT_MATERIAL_LIFT_ROC_AUC = 0.03
DEFAULT_MATERIAL_LIFT_AVERAGE_PRECISION = 0.02
RECOMMENDED_NEXT_STAGE = "run_hybrid_validation_metric_gates_v1"

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)
_WRITE_SQL_RE = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|merge)\b", re.IGNORECASE)

CAVEATS = (
    "Not live recommender validation.",
    "Fresh confirmatory surface; old 217 overlaps are excluded from confirmatory metrics.",
    "Frozen holdout scorer v2 applied without refit.",
    "Single-reviewer audit labels.",
    "Best-arm metrics are exploratory only; primary confirmatory arm is fixed at hybrid_rank_mean_50_50.",
    "No shadow/production authorization.",
    "confirmatory_validation_passed requires a separate metric gates command.",
)


class MLHybridValidationOnFreshSurfaceError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLHybridValidationOnFreshSurfaceError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLHybridValidationOnFreshSurfaceError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLHybridValidationOnFreshSurfaceError(f"{name} JSON missing metadata object")
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
        raise MLHybridValidationOnFreshSurfaceError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _redacted_database_url(database_url: str) -> str:
    parsed = urlparse(str(database_url or ""))
    if not parsed.scheme:
        return "(unparseable local database target)"
    user = parsed.username or ""
    userinfo = f"{user}:***@" if user else ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{userinfo}{parsed.hostname or ''}{port}{parsed.path or ''}"


def _database_summary(database_url: str | None) -> dict[str, Any]:
    if database_url is None:
        return {
            "database_target_redacted": None,
            "local_database_url_confirmed": None,
            "read_only_contract": "SELECT-only queries; no database mutations",
        }
    try:
        summary = dict(assert_local_database_url(database_url))
    except Exception as exc:
        raise MLHybridValidationOnFreshSurfaceError(str(exc)) from exc
    summary["database_target_redacted"] = _redacted_database_url(database_url)
    return summary


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _canonical_work_id_from_label(row: Mapping[str, Any]) -> str | None:
    for field in ("work_id", "openalex_work_id", "paper_id"):
        canonical = _canonical_from_value(row.get(field))
        if canonical:
            return canonical
    return None


def _label_row_is_explicit(row: Mapping[str, Any]) -> bool:
    return row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()})


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _validate_surface(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[dict[str, Any]]]:
    metadata = _metadata(payload, name="fresh-eval-surface")
    if metadata.get("artifact_type") != SURFACE_ARTIFACT_TYPE:
        raise MLHybridValidationOnFreshSurfaceError("fresh surface metadata.artifact_type mismatch")
    if metadata.get("surface_version") != SURFACE_VERSION:
        raise MLHybridValidationOnFreshSurfaceError(f"fresh surface metadata.surface_version must be {SURFACE_VERSION}")
    if metadata.get("status") != "materialized_ready":
        raise MLHybridValidationOnFreshSurfaceError("fresh surface metadata.status must be materialized_ready")
    if payload.get("ready_for_hybrid_validation_scoring") is not True:
        raise MLHybridValidationOnFreshSurfaceError("fresh surface ready_for_hybrid_validation_scoring must be true")
    if payload.get("recommended_next_stage") != "execute_hybrid_validation_on_fresh_surface_v1":
        raise MLHybridValidationOnFreshSurfaceError(
            "fresh surface recommended_next_stage must be execute_hybrid_validation_on_fresh_surface_v1"
        )
    if metadata.get("label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLHybridValidationOnFreshSurfaceError("fresh surface metadata.label_dataset_version must be ml-label-dataset-v10")
    if metadata.get("expected_label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLHybridValidationOnFreshSurfaceError(
            "fresh surface metadata.expected_label_dataset_version must be ml-label-dataset-v10"
        )
    threshold_check = payload.get("threshold_check")
    if not isinstance(threshold_check, Mapping) or not threshold_check:
        raise MLHybridValidationOnFreshSurfaceError("fresh surface threshold_check must be a non-empty object")
    failed = [key for key, item in threshold_check.items() if not (isinstance(item, Mapping) and item.get("passed") is True)]
    if failed:
        raise MLHybridValidationOnFreshSurfaceError(f"fresh surface threshold_check entries failed: {failed[:10]}")

    candidate_pool = payload.get("candidate_pool")
    if not isinstance(candidate_pool, Mapping):
        raise MLHybridValidationOnFreshSurfaceError("fresh surface missing candidate_pool object")
    rows = candidate_pool.get("candidate_rows")
    if not isinstance(rows, list) or not rows:
        raise MLHybridValidationOnFreshSurfaceError("fresh surface candidate_pool.candidate_rows must be non-empty")
    if candidate_pool.get("candidate_work_count") != len(rows):
        raise MLHybridValidationOnFreshSurfaceError("candidate_pool.candidate_work_count must match len(candidate_rows)")
    confirmatory_count = sum(1 for row in rows if isinstance(row, Mapping) and row.get("confirmatory_metric_eligible") is True)
    if _get(payload, "confirmatory_eligibility.confirmatory_metric_eligible_work_count") != confirmatory_count:
        raise MLHybridValidationOnFreshSurfaceError("confirmatory eligibility count does not match candidate rows")
    if _get(payload, "candidate_source.ranking_run_id") != RANKING_RUN_ID:
        raise MLHybridValidationOnFreshSurfaceError(f"fresh surface candidate_source.ranking_run_id must be {RANKING_RUN_ID}")
    if _get(payload, "candidate_source.family") != FAMILY:
        raise MLHybridValidationOnFreshSurfaceError(f"fresh surface candidate_source.family must be {FAMILY}")

    return metadata, [dict(row) for row in rows if isinstance(row, Mapping)]


def _validate_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, float]]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLHybridValidationOnFreshSurfaceError("fresh surface policy metadata.artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLHybridValidationOnFreshSurfaceError(f"fresh surface policy metadata.policy_version must be {POLICY_VERSION}")
    if _get(payload, "frozen_hybrid_arms.primary_confirmatory_arm") != PRIMARY_CONFIRMATORY_ARM:
        raise MLHybridValidationOnFreshSurfaceError("policy frozen_hybrid_arms.primary_confirmatory_arm mismatch")
    if _get(payload, "frozen_hybrid_arms.secondary_reporting_arm") != SECONDARY_REPORTING_ARM:
        raise MLHybridValidationOnFreshSurfaceError("policy frozen_hybrid_arms.secondary_reporting_arm mismatch")
    thresholds = _get(payload, "gate_linkage.material_lift_thresholds")
    roc = DEFAULT_MATERIAL_LIFT_ROC_AUC
    ap = DEFAULT_MATERIAL_LIFT_AVERAGE_PRECISION
    if isinstance(thresholds, Mapping):
        if _is_number(thresholds.get("delta_roc_auc_gte")):
            roc = float(thresholds["delta_roc_auc_gte"])
        if _is_number(thresholds.get("or_delta_average_precision_gte")):
            ap = float(thresholds["or_delta_average_precision_gte"])
    return metadata, {
        "minimum_delta_roc_auc_for_material_lift": roc,
        "minimum_delta_average_precision_for_material_lift": ap,
    }


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLHybridValidationOnFreshSurfaceError("label dataset dataset_version must be ml-label-dataset-v10")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLHybridValidationOnFreshSurfaceError("label dataset rows must be an array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _validate_scorer(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export")
    if metadata.get("artifact_type") != SCORER_ARTIFACT_TYPE:
        raise MLHybridValidationOnFreshSurfaceError("scorer metadata.artifact_type mismatch")
    if metadata.get("scorer_version") != SCORER_VERSION:
        raise MLHybridValidationOnFreshSurfaceError(f"scorer metadata.scorer_version must be {SCORER_VERSION}")
    if metadata.get("fit_mode") != SCORER_FIT_MODE:
        raise MLHybridValidationOnFreshSurfaceError(f"scorer metadata.fit_mode must be {SCORER_FIT_MODE}")
    if _get(payload, "policy_compliance.eval_works_excluded_from_fit") is not True:
        raise MLHybridValidationOnFreshSurfaceError("scorer policy_compliance.eval_works_excluded_from_fit must be true")
    if not _is_number(metadata.get("embedding_dimensions")):
        raise MLHybridValidationOnFreshSurfaceError("scorer metadata.embedding_dimensions must be numeric")
    return metadata


def _validate_embeddings_artifact(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-hybrid-snapshot-embeddings")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLHybridValidationOnFreshSurfaceError("embeddings metadata.artifact_type mismatch")
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLHybridValidationOnFreshSurfaceError("embeddings metadata.artifact_version mismatch")
    if metadata.get("snapshot_version") != CORPUS_SNAPSHOT_VERSION:
        raise MLHybridValidationOnFreshSurfaceError("embeddings metadata.snapshot_version mismatch")
    if metadata.get("embedding_version") != EMBEDDING_VERSION:
        raise MLHybridValidationOnFreshSurfaceError("embeddings metadata.embedding_version mismatch")
    if _get(payload, "embedding_result.status") != "succeeded":
        raise MLHybridValidationOnFreshSurfaceError("embeddings embedding_result.status must be succeeded")
    if _get(payload, "embedding_result.full_snapshot_embedding_coverage") is not True:
        raise MLHybridValidationOnFreshSurfaceError("embeddings full_snapshot_embedding_coverage must be true")
    return metadata


def _validate_optional_spec(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="hybrid-experiment-spec")
    if metadata.get("artifact_type") != "ml_hybrid_scorer_offline_experiment_spec":
        raise MLHybridValidationOnFreshSurfaceError("hybrid experiment spec metadata.artifact_type mismatch")
    if metadata.get("spec_version") != "ml-hybrid-scorer-offline-experiment-v1-spec":
        raise MLHybridValidationOnFreshSurfaceError("hybrid experiment spec metadata.spec_version mismatch")
    arms = payload.get("pre_registered_hybrid_arms")
    expected = list(EXPECTED_ARMS)
    observed = [(str(item.get("arm_id")), str(item.get("score_formula"))) for item in arms if isinstance(item, Mapping)] if isinstance(arms, list) else []
    if observed != expected:
        raise MLHybridValidationOnFreshSurfaceError("hybrid experiment spec arms do not match expected formulas")


def _validate_candidates(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seen: set[str] = set()
    candidates: list[dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        canonical = str(raw.get("canonical_openalex_work_id") or "").strip()
        if not canonical:
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {idx} missing canonical_openalex_work_id")
        if canonical in seen:
            raise MLHybridValidationOnFreshSurfaceError(f"duplicate candidate canonical_openalex_work_id: {canonical}")
        seen.add(canonical)
        if raw.get("duplicate_candidate_canonical_work") is True:
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {canonical} is marked duplicate_candidate_canonical_work")
        if raw.get("internal_work_id") is None:
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {canonical} missing internal_work_id")
        if not _is_number(raw.get("final_score")):
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {canonical} missing numeric final_score")
        if not _is_number(raw.get("heuristic_rank")):
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {canonical} missing numeric heuristic_rank")
        if raw.get("ranking_run_id") != RANKING_RUN_ID:
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {canonical} ranking_run_id mismatch")
        if raw.get("family") != FAMILY:
            raise MLHybridValidationOnFreshSurfaceError(f"candidate row {canonical} family mismatch")
        candidates.append(dict(raw))
    confirmatory = [row for row in candidates if row.get("confirmatory_metric_eligible") is True]
    overlap_confirmatory = [row for row in confirmatory if row.get("previous_eval_overlap") is True]
    if overlap_confirmatory:
        raise MLHybridValidationOnFreshSurfaceError(
            f"confirmatory rows include previous_eval_overlap works: {[row['canonical_openalex_work_id'] for row in overlap_confirmatory[:10]]}"
        )
    return candidates, confirmatory


def _work_level_labels(
    label_rows: Sequence[Mapping[str, Any]],
    *,
    confirmatory_work_ids: set[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    explicit_rows = [row for row in label_rows if _label_row_is_explicit(row)]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_canonical = 0
    for row in explicit_rows:
        if not isinstance(row.get(TARGET), bool):
            continue
        canonical = _canonical_work_id_from_label(row)
        if canonical is None:
            missing_canonical += 1
            continue
        if canonical in confirmatory_work_ids:
            groups[canonical].append(dict(row))

    conflicts: list[str] = []
    labels: dict[str, dict[str, Any]] = {}
    for canonical, rows in groups.items():
        values = [bool(row[TARGET]) for row in rows]
        if any(values) and any(not value for value in values):
            conflicts.append(canonical)
            continue
        labels[canonical] = {
            "label_any_positive": any(values),
            "observation_count": len(values),
            "positive_observation_count": sum(1 for value in values if value),
            "negative_observation_count": sum(1 for value in values if not value),
            "row_ids": [str(row.get("row_id") or "") for row in rows],
        }
    if conflicts:
        raise MLHybridValidationOnFreshSurfaceError(f"conflicting target work groups in confirmatory set: {conflicts[:10]}")
    unlabeled = sorted(confirmatory_work_ids.difference(labels))
    if unlabeled:
        raise MLHybridValidationOnFreshSurfaceError(f"unlabeled confirmatory works remain: {unlabeled[:10]}")

    positives = sum(1 for item in labels.values() if item["label_any_positive"] is True)
    negatives = sum(1 for item in labels.values() if item["label_any_positive"] is False)
    observation_count = sum(int(item["observation_count"]) for item in labels.values())
    positive_observations = sum(int(item["positive_observation_count"]) for item in labels.values())
    negative_observations = sum(int(item["negative_observation_count"]) for item in labels.values())
    return labels, {
        "target": TARGET,
        "confirmatory_work_count": len(confirmatory_work_ids),
        "labeled_confirmatory_work_count": len(labels),
        "unlabeled_confirmatory_work_count": len(unlabeled),
        "positive_work_count": positives,
        "negative_work_count": negatives,
        "conflicting_target_work_group_count": len(conflicts),
        "confirmatory_labeled_observation_count": observation_count,
        "confirmatory_positive_observation_count": positive_observations,
        "confirmatory_negative_observation_count": negative_observations,
        "label_rows_missing_canonical_work_id": missing_canonical,
        "positive_work_prevalence": (positives / len(labels)) if labels else None,
        "work_level_rollup_policy": "positive if any explicit target true; negative if observations exist and none are true; conflicts fail",
    }


def _assert_select_only_sql(sql: str) -> None:
    if _WRITE_SQL_RE.search(sql):
        raise MLHybridValidationOnFreshSurfaceError("fresh hybrid validation DB access must be SELECT-only")


def _parse_vector(value: Any) -> list[float]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MLHybridValidationOnFreshSurfaceError(f"embedding vector string is not valid JSON: {exc}") from exc
    if not isinstance(raw, (list, tuple)):
        raise MLHybridValidationOnFreshSurfaceError("embedding vector must be an array")
    vector: list[float] = []
    for item in raw:
        try:
            number = float(item)
        except (TypeError, ValueError) as exc:
            raise MLHybridValidationOnFreshSurfaceError(f"embedding vector contains non-numeric value: {item!r}") from exc
        if not math.isfinite(number):
            raise MLHybridValidationOnFreshSurfaceError("embedding vector contains non-finite value")
        vector.append(number)
    return vector


def _load_embedding_vectors(
    conn: Any,
    *,
    snapshot_version: str,
    embedding_version: str,
) -> dict[int, list[float]]:
    sql = """
        SELECT w.id AS internal_work_id, e.vector
        FROM works w
        JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
        ORDER BY w.id ASC
    """
    _assert_select_only_sql(sql)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (embedding_version, snapshot_version))
        rows = cur.fetchall()
    out: dict[int, list[float]] = {}
    for row in rows:
        item = dict(row)
        work_id = int(item["internal_work_id"])
        out[work_id] = _parse_vector(item.get("vector"))
    return out


def _score_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    vectors_by_internal_id: Mapping[int, Sequence[float]],
    scorer_payload: Mapping[str, Any],
    expected_dimensions: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    scored: list[dict[str, Any]] = []
    for row in candidates:
        internal_id = int(row["internal_work_id"])
        vector = vectors_by_internal_id.get(internal_id)
        if vector is None:
            missing.append(
                {
                    "canonical_openalex_work_id": row["canonical_openalex_work_id"],
                    "internal_work_id": internal_id,
                    "title": row.get("title"),
                }
            )
            continue
        if len(vector) != expected_dimensions:
            raise MLHybridValidationOnFreshSurfaceError(
                f"embedding vector length for internal_work_id={internal_id} is {len(vector)}, expected {expected_dimensions}"
            )
        item = dict(row)
        item["audit_embedding_probability_work"] = score_audit_embedding_probability(vector, scorer_payload)
        scored.append(item)
    if missing:
        raise MLHybridValidationOnFreshSurfaceError(
            f"missing embeddings for candidate pool works; count={len(missing)}, preview={missing[:10]}"
        )
    return scored, {
        "pool_work_count": len(candidates),
        "embedded_work_count": len(scored),
        "missing_count": len(missing),
        "missing_work_preview": missing[:10],
        "embedding_version": EMBEDDING_VERSION,
        "snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_dimensions": expected_dimensions,
    }


def _attach_labels_and_arm_scores(
    candidates: Sequence[Mapping[str, Any]],
    labels_by_work: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    final_rank_pct = _rank_percentiles(candidates, "final_score")
    learned_rank_pct = _rank_percentiles(candidates, "audit_embedding_probability_work")
    out: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    for row in candidates:
        work_id = str(row["canonical_openalex_work_id"])
        item = dict(row)
        item["final_score_rank_pct"] = final_rank_pct[work_id]
        item["audit_embedding_probability_rank_pct"] = learned_rank_pct[work_id]
        item["arm_scores"] = _arm_scores(item)
        label = labels_by_work.get(work_id)
        if label is not None:
            item.update(label)
            metric_rows.append(item)
        out.append(item)
    return out, metric_rows


def _comparison_with_thresholds(
    metrics: Mapping[str, Any],
    baseline: Mapping[str, Any],
    *,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    delta_roc = _metric_delta(metrics.get("roc_auc_mann_whitney"), baseline.get("roc_auc_mann_whitney"))
    delta_ap = _metric_delta(metrics.get("average_precision"), baseline.get("average_precision"))
    delta_p5 = _metric_delta(_pr_value(metrics, 5), _pr_value(baseline, 5))
    delta_p10 = _metric_delta(_pr_value(metrics, 10), _pr_value(baseline, 10))
    delta_p20 = _metric_delta(_pr_value(metrics, 20), _pr_value(baseline, 20))
    min_roc = float(thresholds["minimum_delta_roc_auc_for_material_lift"])
    min_ap = float(thresholds["minimum_delta_average_precision_for_material_lift"])
    material = (
        (_is_number(delta_roc) and float(delta_roc) >= min_roc)
        or (_is_number(delta_ap) and float(delta_ap) >= min_ap)
    )
    return {
        "delta_roc_auc": delta_roc,
        "delta_average_precision": delta_ap,
        "delta_precision_at_5": delta_p5,
        "delta_precision_at_10": delta_p10,
        "delta_precision_at_20": delta_p20,
        "material_lift_passed_against_heuristic": material,
        "material_lift_thresholds": dict(thresholds),
        "precision_at_10_non_regression_advisory": "regression" if _is_number(delta_p10) and float(delta_p10) < 0 else "ok_or_not_applicable",
    }


def _best_arm(metrics_by_arm: Mapping[str, Mapping[str, Any]], metric_name: str) -> dict[str, Any]:
    candidates = [
        (arm_id, value)
        for arm_id, metrics in metrics_by_arm.items()
        for value in [metrics.get(metric_name)]
        if _is_number(value)
    ]
    if not candidates:
        return {"arm_id": None, metric_name: None}
    arm_id, value = max(candidates, key=lambda item: (float(item[1]), item[0]))
    return {"arm_id": arm_id, metric_name: value}


def _candidate_score_rows(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in candidates:
        rows.append(
            {
                "canonical_openalex_work_id": row["canonical_openalex_work_id"],
                "internal_work_id": row.get("internal_work_id"),
                "title": row.get("title"),
                "year": row.get("year"),
                "citation_count": row.get("citation_count"),
                "ranking_run_id": row.get("ranking_run_id"),
                "family": row.get("family"),
                "heuristic_rank": row.get("heuristic_rank"),
                "final_score": row.get("final_score"),
                "audit_embedding_probability_work": row.get("audit_embedding_probability_work"),
                "final_score_rank_pct": row.get("final_score_rank_pct"),
                "audit_embedding_probability_rank_pct": row.get("audit_embedding_probability_rank_pct"),
                "confirmatory_metric_eligible": row.get("confirmatory_metric_eligible"),
                "previous_eval_overlap": row.get("previous_eval_overlap"),
                "label_any_positive": row.get("label_any_positive"),
                "arm_scores": row.get("arm_scores"),
            }
        )
    return rows


def build_ml_hybrid_validation_on_fresh_surface_payload(
    conn: Any,
    *,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    audit_embedding_scorer_export_path: Path,
    fresh_hybrid_snapshot_embeddings_path: Path,
    hybrid_experiment_spec_path: Path | None = None,
    hybrid_metric_gates_path: Path | None = None,
    database_url: str | None = None,
    validation_version: str = VALIDATION_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    surface_path = Path(fresh_eval_surface_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    scorer_path = Path(audit_embedding_scorer_export_path).resolve()
    embeddings_path = Path(fresh_hybrid_snapshot_embeddings_path).resolve()
    spec_path = Path(hybrid_experiment_spec_path).resolve() if hybrid_experiment_spec_path is not None else None
    gates_path = Path(hybrid_metric_gates_path).resolve() if hybrid_metric_gates_path is not None else None

    surface_payload = _load_json_object(surface_path)
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(label_path)
    scorer_payload = _load_json_object(scorer_path)
    embeddings_payload = _load_json_object(embeddings_path)

    surface_metadata, candidate_rows_raw = _validate_surface(surface_payload)
    policy_metadata, material_thresholds = _validate_policy(policy_payload)
    label_rows = _validate_label_dataset(label_payload)
    scorer_metadata = _validate_scorer(scorer_payload)
    embeddings_metadata = _validate_embeddings_artifact(embeddings_payload)
    if spec_path is not None:
        _validate_optional_spec(_load_json_object(spec_path))
    if gates_path is not None:
        _metadata(_load_json_object(gates_path), name="hybrid-metric-gates")

    inputs = [
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("audit_embedding_scorer_export", scorer_path, repo_root=root),
        _input_record("fresh_hybrid_snapshot_embeddings", embeddings_path, repo_root=root),
    ]
    if spec_path is not None:
        inputs.append(_input_record("hybrid_experiment_spec", spec_path, repo_root=root))
    if gates_path is not None:
        inputs.append(_input_record("hybrid_metric_gates", gates_path, repo_root=root))

    database_summary = _database_summary(database_url)
    candidates, confirmatory_rows = _validate_candidates(candidate_rows_raw)
    confirmatory_ids = {str(row["canonical_openalex_work_id"]) for row in confirmatory_rows}
    labels_by_work, label_join_summary = _work_level_labels(label_rows, confirmatory_work_ids=confirmatory_ids)
    vectors = _load_embedding_vectors(
        conn,
        snapshot_version=CORPUS_SNAPSHOT_VERSION,
        embedding_version=EMBEDDING_VERSION,
    )
    expected_dimensions = int(scorer_metadata["embedding_dimensions"])
    scored_candidates, embedding_join_summary = _score_candidates(
        candidates,
        vectors_by_internal_id=vectors,
        scorer_payload=scorer_payload,
        expected_dimensions=expected_dimensions,
    )
    scored_candidates, metric_rows = _attach_labels_and_arm_scores(scored_candidates, labels_by_work)
    if len(metric_rows) != len(confirmatory_rows):
        raise MLHybridValidationOnFreshSurfaceError("metric row count does not match confirmatory row count")

    arm_metrics = {
        arm_id: _arm_metric(arm_id=arm_id, candidates=scored_candidates, metric_rows=metric_rows)
        for arm_id, _formula in EXPECTED_ARMS
    }
    heuristic = arm_metrics["heuristic_final_score_baseline"]
    comparisons = {
        arm_id: _comparison_with_thresholds(metrics, heuristic, thresholds=material_thresholds)
        for arm_id, metrics in arm_metrics.items()
        if arm_id != "heuristic_final_score_baseline"
    }
    primary_comparison = comparisons[PRIMARY_CONFIRMATORY_ARM]
    best_roc = _best_arm(arm_metrics, "roc_auc_mann_whitney")
    best_ap = _best_arm(arm_metrics, "average_precision")

    overlap_count = int(_get(surface_payload, "disallowed_overlap_report.overlap_work_count") or 0)
    candidate_pool_sha = str(_get(surface_payload, "candidate_pool.candidate_work_set_sha256") or "")
    coverage = {
        "candidate_pool_work_count": len(scored_candidates),
        "confirmatory_metric_work_count": len(metric_rows),
        "confirmatory_positive_work_count": label_join_summary["positive_work_count"],
        "confirmatory_negative_work_count": label_join_summary["negative_work_count"],
        "positive_work_prevalence": label_join_summary["positive_work_prevalence"],
        "candidate_pool_work_set_sha256": candidate_pool_sha,
    }

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "validation_version": validation_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "fresh_surface_sha256": sha256_file(surface_path),
            "label_dataset_sha256": sha256_file(label_path),
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "candidate_pool_work_set_sha256": candidate_pool_sha,
            "source_surface_version": surface_metadata.get("surface_version"),
            "source_policy_version": policy_metadata.get("policy_version"),
            "source_label_dataset_version": label_payload.get("dataset_version"),
            "source_scorer_version": scorer_metadata.get("scorer_version"),
            "source_embeddings_artifact_version": embeddings_metadata.get("artifact_version"),
            "database": database_summary,
            "caveats": list(CAVEATS),
        },
        "validation_scope": {
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "candidate_pool_work_count": len(scored_candidates),
            "confirmatory_metric_work_count": len(metric_rows),
            "metric_denominator": "confirmatory_metric_eligible labeled canonical works only",
            "rank_percentile_scope": "full_candidate_pool",
            "target": TARGET,
        },
        "candidate_eval_coverage": coverage,
        "label_join_summary": label_join_summary,
        "embedding_join_summary": embedding_join_summary,
        "pre_registered_arms_executed": [
            {"arm_id": arm_id, "score_formula": formula}
            for arm_id, formula in EXPECTED_ARMS
        ],
        "arm_metrics": arm_metrics,
        "comparisons_vs_heuristic": comparisons,
        "confirmatory_decision_inputs": {
            "primary_confirmatory_arm": PRIMARY_CONFIRMATORY_ARM,
            "secondary_reporting_arm": SECONDARY_REPORTING_ARM,
            "primary_arm_material_lift_passed_against_heuristic": primary_comparison[
                "material_lift_passed_against_heuristic"
            ],
            "primary_arm_comparison_vs_heuristic": primary_comparison,
            "material_lift_thresholds": material_thresholds,
            "best_arm_by_roc_auc": best_roc,
            "best_arm_by_average_precision": best_ap,
            "best_arm_selection_is_exploratory_only": True,
            "confirmatory_metrics_ready_for_gates": True,
            "confirmatory_validation_passed": False,
            "confirmatory_validation_passed_reason": "metric_gates_not_run",
        },
        "summary": {
            "heuristic_baseline_metrics": arm_metrics["heuristic_final_score_baseline"],
            "holdout_embedding_baseline_metrics": arm_metrics["holdout_embedding_probability_baseline"],
            "primary_confirmatory_arm_metrics": arm_metrics[PRIMARY_CONFIRMATORY_ARM],
            "primary_confirmatory_arm_deltas_vs_heuristic": primary_comparison,
            "best_arm_by_roc_auc": best_roc,
            "best_arm_by_average_precision": best_ap,
            "best_arm_selection_is_exploratory_only": True,
            "confirmatory_validation_passed": False,
            "confirmatory_validation_passed_reason": "metric_gates_not_run",
            "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        },
        "candidate_work_scores": _candidate_score_rows(scored_candidates),
        "leakage_report": {
            "old_217_overlap_excluded_from_confirmatory_metrics": True,
            "confirmatory_rows_with_previous_eval_overlap_count": 0,
            "overlap_work_count_in_full_pool_may_be_nonzero": True,
            "old_217_overlap_work_count_in_full_pool": overlap_count,
            "train_rows_used": 0,
            "supervised_fit_used": False,
            "eval_label_weight_tuning_used": False,
            "scorer_refit_used": False,
            "frozen_scorer_fit_corpus_note": (
                "scorer v2 fit on holdout train excluding old eval works; applied to fresh snapshot embeddings without refit"
            ),
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "missing_hybrid_validation_metric_gates": True,
            "missing_ml_shadow_scorer_v1": True,
            "no_production_model_artifact": True,
        },
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "confirmatory_validation_passed": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _precision(metrics: Mapping[str, Any], k: int) -> Any:
    return _get(metrics, f"precision_recall_at_k.{k}.precision")


def markdown_from_ml_hybrid_validation_on_fresh_surface(payload: Mapping[str, Any]) -> str:
    scope = payload["validation_scope"]
    coverage = payload["candidate_eval_coverage"]
    labels = payload["label_join_summary"]
    embeddings = payload["embedding_join_summary"]
    summary = payload["summary"]
    primary = payload["confirmatory_decision_inputs"]
    arms = payload["arm_metrics"]
    comparisons = payload["comparisons_vs_heuristic"]
    lines = [
        "# Hybrid Validation On Fresh Surface v1",
        "",
        "## Executive Summary",
        "",
        "Frozen holdout scorer v2 and pre-registered hybrid arms were applied to the ready fresh hybrid eval surface. This artifact produces metrics for a later gates command; it does not pass confirmatory validation or authorize shadow/production.",
        "",
        f"- **candidate pool:** {scope['candidate_pool_work_count']}",
        f"- **confirmatory metric works:** {scope['confirmatory_metric_work_count']}",
        f"- **label balance:** {labels['positive_work_count']} positive / {labels['negative_work_count']} negative",
        f"- **embedding coverage:** {embeddings['embedded_work_count']} / {embeddings['pool_work_count']} ({embeddings['embedding_version']})",
        f"- **primary confirmatory arm:** `{primary['primary_confirmatory_arm']}`",
        f"- **primary arm material lift vs heuristic:** {primary['primary_arm_material_lift_passed_against_heuristic']}",
        f"- **recommended next stage:** `{summary['recommended_next_stage']}`",
        "",
        "## Candidate And Label Coverage",
        "",
        "| Item | Value |",
        "| --- | ---: |",
        f"| Candidate pool works | {coverage['candidate_pool_work_count']} |",
        f"| Confirmatory metric works | {coverage['confirmatory_metric_work_count']} |",
        f"| Positive works | {coverage['confirmatory_positive_work_count']} |",
        f"| Negative works | {coverage['confirmatory_negative_work_count']} |",
        f"| Positive prevalence | {_fmt(coverage['positive_work_prevalence'])} |",
        "",
        "## Arm Metrics",
        "",
        "| Arm | ROC-AUC | AP | P@5 | P@10 | P@20 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for arm_id, _formula in EXPECTED_ARMS:
        metrics = arms[arm_id]
        lines.append(
            f"| `{arm_id}` | {_fmt(metrics.get('roc_auc_mann_whitney'))} | {_fmt(metrics.get('average_precision'))} | "
            f"{_fmt(_precision(metrics, 5))} | {_fmt(_precision(metrics, 10))} | {_fmt(_precision(metrics, 20))} |"
        )
    lines.extend(
        [
            "",
            "## Deltas Vs Heuristic",
            "",
            "| Arm | delta ROC-AUC | delta AP | delta P@5 | delta P@10 | delta P@20 | Material lift |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for arm_id, _formula in EXPECTED_ARMS:
        if arm_id == "heuristic_final_score_baseline":
            continue
        comp = comparisons[arm_id]
        lines.append(
            f"| `{arm_id}` | {_fmt(comp.get('delta_roc_auc'))} | {_fmt(comp.get('delta_average_precision'))} | "
            f"{_fmt(comp.get('delta_precision_at_5'))} | {_fmt(comp.get('delta_precision_at_10'))} | "
            f"{_fmt(comp.get('delta_precision_at_20'))} | {comp.get('material_lift_passed_against_heuristic')} |"
        )
    lines.extend(
        [
            "",
            "## Exploratory Best Arms",
            "",
            f"- **Best by ROC-AUC:** `{primary['best_arm_by_roc_auc']['arm_id']}` ({_fmt(primary['best_arm_by_roc_auc'].get('roc_auc_mann_whitney'))})",
            f"- **Best by AP:** `{primary['best_arm_by_average_precision']['arm_id']}` ({_fmt(primary['best_arm_by_average_precision'].get('average_precision'))})",
            "- Best-arm selection is exploratory only; the primary confirmatory arm remains fixed at `hybrid_rank_mean_50_50`.",
            "",
            "## Leakage Checks",
            "",
            "- Old 217 overlap is excluded from confirmatory metrics.",
            "- Confirmatory rows with previous-eval overlap: 0.",
            "- Supervised fit, eval-label weight tuning, scorer refit: false.",
            "",
            "## Not Shadow / Not Production",
            "",
            "- `shadow_scoring_allowed`: false",
            "- `production_default_allowed`: false",
            "- `confirmatory_validation_passed`: false until a separate gates command runs.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload.get("caveats", []))
    return "\n".join(lines).rstrip() + "\n"


def write_ml_hybrid_validation_on_fresh_surface(
    *,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    audit_embedding_scorer_export_path: Path,
    fresh_hybrid_snapshot_embeddings_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    hybrid_experiment_spec_path: Path | None = None,
    hybrid_metric_gates_path: Path | None = None,
    database_url: str | None = None,
    validation_version: str = VALIDATION_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    dsn = database_url or _database_url_from_env()
    _database_summary(dsn)
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        payload = build_ml_hybrid_validation_on_fresh_surface_payload(
            conn,
            fresh_eval_surface_path=fresh_eval_surface_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            label_dataset_path=label_dataset_path,
            audit_embedding_scorer_export_path=audit_embedding_scorer_export_path,
            fresh_hybrid_snapshot_embeddings_path=fresh_hybrid_snapshot_embeddings_path,
            hybrid_experiment_spec_path=hybrid_experiment_spec_path,
            hybrid_metric_gates_path=hybrid_metric_gates_path,
            database_url=dsn,
            validation_version=validation_version,
            repo_root=repo_root,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_hybrid_validation_on_fresh_surface(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "VALIDATION_VERSION",
    "MLHybridValidationOnFreshSurfaceError",
    "build_ml_hybrid_validation_on_fresh_surface_payload",
    "markdown_from_ml_hybrid_validation_on_fresh_surface",
    "write_ml_hybrid_validation_on_fresh_surface",
]
