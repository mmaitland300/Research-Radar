"""Offline diagnostic scorer for bridge_recommendable on the v14 three-pool bridge slice.

Trains/evaluates on deduped unique work_ids (130 rows) from negative-mining, top-ranked,
and shadow-pilot audit pools. Row-level evidence (160 rows) is retained for duplicate-sensitive
audit readouts only.

This is not validation and not a serving change.
"""

from __future__ import annotations

import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import GroupKFold, StratifiedKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_baseline_eval import pairwise_accuracy, precision_at_k, sha256_file
from pipeline.repo_paths import portable_repo_path

TARGET = "bridge_recommendable"
LABEL_DATASET_VERSION = "ml-label-dataset-v14"
READINESS_MATRIX_VERSION = "ml-label-readiness-matrix-v11"

ROW_LEVEL_ROWS = 160
ROW_LEVEL_POSITIVE = 87
ROW_LEVEL_NEGATIVE = 73
DEDUPED_ROWS = 130
DEDUPED_POSITIVE = 75
DEDUPED_NEGATIVE = 55
DEDUPED_POOL_COUNTS = {
    "ml_bridge_negative_mining_audit": 62,
    "ml_bridge_top_ranked_validation_audit": 8,
    "ml_bridge_shadow_pilot_audit": 60,
}
ROW_LEVEL_POOL_COUNTS = {
    "ml_bridge_negative_mining_audit": 70,
    "ml_bridge_top_ranked_validation_audit": 30,
    "ml_bridge_shadow_pilot_audit": 60,
}
EXPECTED_DUPLICATE_WORK_IDS_IN_SLICE = 30
EXPECTED_DERIVED_TARGET_CONFLICT_COUNT = 1
EXPECTED_CONFLICT_WORK_ID = "W4415316343"
EXPECTED_OVERLAP_COUNT_WITH_V13 = 32

EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
SCORER_VERSION = "ml-offline-bridge-recommendable-scorer-v3"
ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer_v3"
FIT_MODE = "bridge_three_slice_shadow_audit_diagnostic_v1"
DEFAULT_RANDOM_SEED = 20260602

FAMILY = "bridge"
POOL_SHADOW = "ml_bridge_shadow_pilot_audit"
POOL_TOP = "ml_bridge_top_ranked_validation_audit"
POOL_NEG = "ml_bridge_negative_mining_audit"
ALLOWED_REVIEW_POOL_VARIANTS = frozenset({POOL_NEG, POOL_TOP, POOL_SHADOW})
POOL_PRIORITY = {POOL_SHADOW: 0, POOL_TOP: 1, POOL_NEG: 2}

RANK_V1 = "rank-83787b91ef"
RANK_SHADOW = "rank-5a7efa5ca3"

EMBEDDINGS_ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
EXPECTED_EMBEDDED_WORK_COUNT = 528

V2_SCORER_VERSION = "ml-offline-bridge-recommendable-scorer-v2"
V2_OVERLAP_WORK_IDS = 100
V2_AGGREGATE_ROC_AUC_REFERENCE = 0.6495383380168608
MAJOR_REGRESSION_AUC_DELTA = 0.05
OVERFIT_AUC_GAP_THRESHOLD = 0.15

WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|merge|grant|revoke|vacuum|reindex|copy)\b"
)

ROW_LEVEL_AUDIT_FLAGS = {
    "duplicate_sensitive": True,
    "conflict_sensitive": True,
    "excluded_from_recommended_next_stage_decision": True,
}

CAVEATS = (
    "This is not validation.",
    "Primary training/evaluation uses deduped 130 unique work_ids; row-level 160-row readouts are audit-only.",
    "OOF CV metrics on the deduped slice are not in-sample metrics.",
    "Stratified deduped metrics reuse OOF probabilities from the deduped 130-row CV.",
    "Row-level stratified readouts map deduped OOF probabilities and are duplicate/conflict sensitive.",
    "Derived-target conflict on W4415316343 is reported; shadow-pilot row wins dedupe priority.",
    "No ranking, API, DB-write, shadow rollout, or production serving changes are made or authorized.",
)


class MLOfflineBridgeRecommendableScorerV3Error(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineBridgeRecommendableScorerV3Error(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineBridgeRecommendableScorerV3Error(f"{label} JSON must be an object: {path}")
    return payload


def _bool_key(value: Any) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "null"


def _norm_label(value: Any) -> str:
    return str(value or "").strip().lower()


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _mean_std(values: list[float | None]) -> dict[str, float | None]:
    clean = [float(v) for v in values if isinstance(v, (int, float)) and math.isfinite(float(v))]
    if not clean:
        return {"mean": None, "std": None}
    return {
        "mean": float(statistics.fmean(clean)),
        "std": float(statistics.pstdev(clean)) if len(clean) > 1 else 0.0,
    }


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[index]


def _parse_vector(value: Any) -> list[float]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise MLOfflineBridgeRecommendableScorerV3Error(f"embedding vector is not valid JSON: {exc}") from exc
    else:
        parsed = value
    if not isinstance(parsed, (list, tuple)):
        raise MLOfflineBridgeRecommendableScorerV3Error("embedding vector must be an array")
    out: list[float] = []
    for item in parsed:
        number = _as_float(item)
        if number is None:
            raise MLOfflineBridgeRecommendableScorerV3Error("embedding vector contains non-finite/non-numeric value")
        out.append(number)
    return out


def _execute_select(cur: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    stripped = sql.strip()
    lowered = stripped.lower()
    if not lowered.startswith("select"):
        raise MLOfflineBridgeRecommendableScorerV3Error("DB safety violation: SQL must start with SELECT")
    if WRITE_SQL_RE.search(lowered):
        raise MLOfflineBridgeRecommendableScorerV3Error("DB safety violation: SQL contains write/DDL verb")
    return cur.execute(sql, tuple(params or ()))


def _disagreement_bucket(row: Mapping[str, Any]) -> str | None:
    ctx = row.get("bridge_shadow_pilot_context")
    if isinstance(ctx, Mapping):
        bucket = ctx.get("disagreement_bucket")
        if isinstance(bucket, str) and bucket.strip():
            return bucket.strip()
    sample = row.get("sample_reason")
    if isinstance(sample, str) and sample.strip():
        return sample.strip()
    return None


def _slice_row_level_rows(label_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = label_payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineBridgeRecommendableScorerV3Error("label dataset missing rows array")
    out = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("split") == "audit_only"
        and row.get("family") == FAMILY
        and row.get("review_pool_variant") in ALLOWED_REVIEW_POOL_VARIANTS
        and isinstance(row.get(TARGET), bool)
    ]
    if len(out) != ROW_LEVEL_ROWS:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"row-level training slice has {len(out)} rows after mandatory filter; expected {ROW_LEVEL_ROWS}"
        )
    return out


def _dedupe_by_work_id(row_level_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_work: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in row_level_rows:
        work_id = str(row.get("work_id") or "")
        if not work_id:
            raise MLOfflineBridgeRecommendableScorerV3Error("slice row missing work_id")
        by_work[work_id].append(row)

    selected: list[dict[str, Any]] = []
    duplicate_work_ids: list[str] = []
    derived_target_conflicts: list[dict[str, Any]] = []
    for work_id in sorted(by_work):
        group = by_work[work_id]
        if len(group) > 1:
            duplicate_work_ids.append(work_id)
            targets = {row[TARGET] for row in group}
            if len(targets) > 1:
                derived_target_conflicts.append(
                    {
                        "work_id": work_id,
                        "winning_review_pool_variant": min(
                            group, key=lambda r: POOL_PRIORITY[r["review_pool_variant"]]
                        )["review_pool_variant"],
                        "rows": [
                            {
                                "row_id": row.get("row_id"),
                                "review_pool_variant": row.get("review_pool_variant"),
                                "bridge_recommendable": row.get(TARGET),
                                "ranking_run_id": row.get("ranking_run_id"),
                            }
                            for row in sorted(group, key=lambda r: POOL_PRIORITY[r["review_pool_variant"]])
                        ],
                    }
                )
        winner = min(group, key=lambda r: POOL_PRIORITY[str(r.get("review_pool_variant") or "")])
        selected.append(winner)

    if len(selected) != DEDUPED_ROWS:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"deduped slice has {len(selected)} rows; expected {DEDUPED_ROWS}"
        )
    meta = {
        "row_count": len(selected),
        "duplicate_work_id_count_in_three_pool_slice": len(duplicate_work_ids),
        "duplicate_work_ids_preview": duplicate_work_ids[:20],
        "derived_target_conflict_count": len(derived_target_conflicts),
        "derived_target_conflicts": derived_target_conflicts,
    }
    return selected, meta


def _validate_row_level_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    target_counts = Counter(_bool_key(row.get(TARGET)) for row in rows)
    pool_counts = Counter(str(row.get("review_pool_variant") or "") for row in rows)
    checks = {
        "row_count": len(rows),
        "positive_count": target_counts["true"],
        "negative_count": target_counts["false"],
        "review_pool_variant_counts": dict(sorted(pool_counts.items())),
    }
    if checks["positive_count"] != ROW_LEVEL_POSITIVE or checks["negative_count"] != ROW_LEVEL_NEGATIVE:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"row-level bridge_recommendable counts mismatch: {checks['positive_count']}/{checks['negative_count']}; "
            f"expected {ROW_LEVEL_POSITIVE}/{ROW_LEVEL_NEGATIVE}"
        )
    for pool, expected in ROW_LEVEL_POOL_COUNTS.items():
        if pool_counts.get(pool) != expected:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"row-level pool {pool} count={pool_counts.get(pool)!r}; expected {expected!r}"
            )
    return checks


def _validate_deduped_counts(rows: list[dict[str, Any]], dedupe_meta: dict[str, Any]) -> dict[str, Any]:
    target_counts = Counter(_bool_key(row.get(TARGET)) for row in rows)
    pool_counts = Counter(str(row.get("review_pool_variant") or "") for row in rows)
    checks = {
        "row_count": len(rows),
        "positive_count": target_counts["true"],
        "negative_count": target_counts["false"],
        "review_pool_variant_counts": dict(sorted(pool_counts.items())),
        **dedupe_meta,
    }
    if checks["positive_count"] != DEDUPED_POSITIVE or checks["negative_count"] != DEDUPED_NEGATIVE:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"deduped bridge_recommendable counts mismatch: {checks['positive_count']}/{checks['negative_count']}; "
            f"expected {DEDUPED_POSITIVE}/{DEDUPED_NEGATIVE}"
        )
    for pool, expected in DEDUPED_POOL_COUNTS.items():
        if pool_counts.get(pool) != expected:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"deduped pool {pool} count={pool_counts.get(pool)!r}; expected {expected!r}"
            )
    if dedupe_meta["duplicate_work_id_count_in_three_pool_slice"] != EXPECTED_DUPLICATE_WORK_IDS_IN_SLICE:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            "duplicate_work_id_count_in_three_pool_slice mismatch"
        )
    if dedupe_meta["derived_target_conflict_count"] != EXPECTED_DERIVED_TARGET_CONFLICT_COUNT:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"derived_target_conflict_count={dedupe_meta['derived_target_conflict_count']!r}; "
            f"expected {EXPECTED_DERIVED_TARGET_CONFLICT_COUNT!r}"
        )
    conflict_ids = [c["work_id"] for c in dedupe_meta.get("derived_target_conflicts", [])]
    if EXPECTED_CONFLICT_WORK_ID not in conflict_ids:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"expected derived-target conflict on {EXPECTED_CONFLICT_WORK_ID!r}"
        )
    return checks


def _validate_readiness_matrix(
    readiness_payload: dict[str, Any],
    *,
    label_dataset_sha256: str,
) -> dict[str, Any]:
    prov = readiness_payload.get("provenance")
    if not isinstance(prov, dict):
        raise MLOfflineBridgeRecommendableScorerV3Error("readiness matrix missing provenance object")
    if prov.get("label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"readiness matrix must point at {LABEL_DATASET_VERSION}"
        )
    if prov.get("label_dataset_sha256") != label_dataset_sha256:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            "readiness matrix label_dataset_sha256 does not match label dataset"
        )
    groups = readiness_payload.get("groups")
    if not isinstance(groups, list):
        raise MLOfflineBridgeRecommendableScorerV3Error("readiness matrix missing groups array")

    expected_groups = {
        (RANK_V1, 100, 53, 47, 100),
        (RANK_SHADOW, 60, 34, 26, 60),
    }
    found: dict[str, dict[str, Any]] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        if group.get("family") != FAMILY or group.get("target") != TARGET:
            continue
        run_id = group.get("ranking_run_id")
        if run_id in {RANK_V1, RANK_SHADOW}:
            found[str(run_id)] = group

    for run_id, total, pos, neg, joinable in (
        (RANK_V1, 100, 53, 47, 100),
        (RANK_SHADOW, 60, 34, 26, 60),
    ):
        group = found.get(run_id)
        if group is None:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"readiness matrix missing ({run_id}, bridge, bridge_recommendable) group"
            )
        expected = {
            "total_labeled_rows": total,
            "positive_count": pos,
            "negative_count": neg,
            "paper_scores_joinable_count": joinable,
            "missing_score_count": 0,
        }
        for key, value in expected.items():
            if group.get(key) != value:
                raise MLOfflineBridgeRecommendableScorerV3Error(
                    f"readiness matrix group {run_id} {key}={group.get(key)!r}; expected {value!r}"
                )
        readiness = group.get("readiness")
        if not isinstance(readiness, dict) or readiness.get("enough_for_diagnostic_auc") is not True:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"readiness matrix group {run_id} is not diagnostic-AUC ready"
            )
    return {"rank-83787b91ef": found[RANK_V1], "rank-5a7efa5ca3": found[RANK_SHADOW]}


def _validate_embeddings_provenance(payload: dict[str, Any]) -> int:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise MLOfflineBridgeRecommendableScorerV3Error("embeddings provenance missing metadata object")
    checks = {
        "artifact_type": EMBEDDINGS_ARTIFACT_TYPE,
        "artifact_version": EMBEDDINGS_ARTIFACT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "snapshot_version": CORPUS_SNAPSHOT_VERSION,
    }
    for key, expected in checks.items():
        if metadata.get(key) != expected:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"embeddings provenance metadata.{key}={metadata.get(key)!r}; expected {expected!r}"
            )
    result = payload.get("embedding_result")
    if not isinstance(result, dict) or result.get("status") != "succeeded":
        raise MLOfflineBridgeRecommendableScorerV3Error("embeddings provenance status must be succeeded")
    if result.get("full_snapshot_embedding_coverage") is not True:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            "embeddings provenance full_snapshot_embedding_coverage must be true"
        )
    dim = result.get("embedding_dimensions")
    if not isinstance(dim, int) or isinstance(dim, bool) or dim <= 0:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            "embeddings provenance embedding_dimensions must be positive integer"
        )
    coverage = payload.get("coverage")
    if not isinstance(coverage, dict):
        raise MLOfflineBridgeRecommendableScorerV3Error("embeddings provenance missing coverage object")
    if coverage.get("embedded_work_count") != EXPECTED_EMBEDDED_WORK_COUNT:
        raise MLOfflineBridgeRecommendableScorerV3Error("embeddings provenance embedded_work_count must be 528")
    if coverage.get("missing_embedding_count") != 0:
        raise MLOfflineBridgeRecommendableScorerV3Error("embeddings provenance missing_embedding_count must be 0")
    return dim


def _internal_work_ids(rows: list[dict[str, Any]], *, expected_count: int) -> list[int]:
    out: list[int] = []
    for row in rows:
        value = row.get("internal_work_id")
        if not isinstance(value, int) or isinstance(value, bool):
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"slice row missing integer internal_work_id (work_id={row.get('work_id')!r})"
            )
        out.append(value)
    if len(set(out)) != expected_count:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"internal_work_id values are not unique within slice (expected {expected_count} unique)"
        )
    return out


def _load_slice_embeddings_select_only(
    conn: psycopg.Connection,
    *,
    internal_work_ids: list[int],
    expected_dimensions: int,
) -> dict[int, list[float]]:
    if not internal_work_ids:
        raise MLOfflineBridgeRecommendableScorerV3Error("embedding query requires at least one work_id")
    sql = """
        SELECT work_id, vector
        FROM embeddings
        WHERE embedding_version = %s
          AND work_id = ANY(%s)
        ORDER BY work_id ASC
    """
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(cur, sql, (EMBEDDING_VERSION, internal_work_ids))
        fetched = cur.fetchall()
    out: dict[int, list[float]] = {}
    for row in fetched:
        work_id = int(_row_value(row, "work_id", 0))
        vector = _parse_vector(_row_value(row, "vector", 1))
        if len(vector) != expected_dimensions:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"embedding vector length for work_id={work_id} is {len(vector)}, expected {expected_dimensions}"
            )
        out[work_id] = vector
    missing = sorted(set(internal_work_ids) - set(out))
    if missing:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"embedding coverage mismatch: missing internal_work_id values {missing[:10]}"
        )
    return out


def _ranking_metrics(scores: list[float], labels: list[bool]) -> dict[str, Any]:
    positives = sum(1 for y in labels if y)
    negatives = len(labels) - positives
    if not labels or positives == 0 or negatives == 0:
        return {
            "status": "not_applicable",
            "roc_auc": None,
            "average_precision": None,
            "pairwise_accuracy": None,
            "precision_at_5": None,
            "precision_at_10": None,
            "precision_at_20": None,
        }
    score_labels = list(zip(scores, labels, strict=True))
    desc = sorted(score_labels, key=lambda item: (-item[0], item[1]))
    as_pairs = [(float(s), bool(l)) for s, l in score_labels]
    return {
        "status": "ok",
        "roc_auc": float(roc_auc_score(labels, scores)),
        "average_precision": float(average_precision_score(labels, scores)),
        "pairwise_accuracy": pairwise_accuracy(as_pairs),
        "precision_at_5": precision_at_k(desc, 5),
        "precision_at_10": precision_at_k(desc, 10),
        "precision_at_20": precision_at_k(desc, 20),
    }


def _stratified_metrics(
    rows: list[dict[str, Any]],
    probs: list[float],
    *,
    indices: list[int],
    label: str,
    audit_flags: dict[str, bool] | None = None,
) -> dict[str, Any]:
    sub_scores = [probs[i] for i in indices]
    sub_labels = [bool(rows[i][TARGET]) for i in indices]
    pos = sum(1 for y in sub_labels if y)
    neg = len(sub_labels) - pos
    out: dict[str, Any] = {
        "stratum": label,
        "n": len(indices),
        "positive_count": pos,
        "negative_count": neg,
        **_ranking_metrics(sub_scores, sub_labels),
    }
    if audit_flags:
        out.update(audit_flags)
    return out


def _heuristic_arm(rows: list[dict[str, Any]], *, field: str) -> dict[str, Any]:
    scores: list[float] = []
    labels: list[bool] = []
    missing = 0
    coverage_by_run: dict[str, dict[str, int]] = {}
    for row in rows:
        run_id = str(row.get("ranking_run_id") or "(null)")
        score = _as_float(row.get(field))
        bucket = coverage_by_run.setdefault(
            run_id,
            {"row_count": 0, "non_null_score_count": 0, "positive_with_score": 0, "negative_with_score": 0},
        )
        bucket["row_count"] += 1
        if score is None:
            missing += 1
            continue
        bucket["non_null_score_count"] += 1
        label = bool(row[TARGET])
        scores.append(score)
        labels.append(label)
        if label:
            bucket["positive_with_score"] += 1
        else:
            bucket["negative_with_score"] += 1
    positives = sum(1 for y in labels if y)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        metrics = {
            "status": "not_applicable",
            "reason": "non-null scores do not cover both target classes",
            "roc_auc": None,
            "average_precision": None,
            "pairwise_accuracy": None,
            "precision_at_5": None,
            "precision_at_10": None,
            "precision_at_20": None,
        }
    else:
        metrics = _ranking_metrics(scores, labels)
    return {
        "field": field,
        "score_kind": "rank_score_not_calibrated_classifier",
        "non_null_score_count": len(scores),
        "missing_score_count": missing,
        "positive_count_with_score": positives,
        "negative_count_with_score": negatives,
        "coverage_by_ranking_run_id": coverage_by_run,
        **metrics,
    }


def _confusion_dict(y_true: Sequence[bool], y_pred: Sequence[bool]) -> dict[str, int]:
    mat = confusion_matrix([int(y) for y in y_true], [int(y) for y in y_pred], labels=[0, 1])
    return {
        "tn": int(mat[0][0]),
        "fp": int(mat[0][1]),
        "fn": int(mat[1][0]),
        "tp": int(mat[1][1]),
    }


def _learned_cv(
    rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    random_seed: int,
) -> tuple[dict[str, Any], list[float]]:
    work_ids = _internal_work_ids(rows, expected_count=len(rows))
    x = [vectors_by_work[work_id] for work_id in work_ids]
    y_bool = [bool(row[TARGET]) for row in rows]
    y_int = [1 if y else 0 for y in y_bool]
    splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
    oof_prob = [float("nan")] * len(rows)
    per_fold: list[dict[str, Any]] = []

    for fold_id, (train_idx_arr, test_idx_arr) in enumerate(splitter.split(x, y_int)):
        train_idx = [int(i) for i in train_idx_arr]
        test_idx = [int(i) for i in test_idx_arr]
        model = make_pipeline(
            StandardScaler(),
            LogisticRegression(max_iter=1000, solver="liblinear", random_state=random_seed),
        )
        model.fit([x[i] for i in train_idx], [y_int[i] for i in train_idx])
        probs = [float(p) for p in model.predict_proba([x[i] for i in test_idx])[:, 1]]
        preds = [p >= 0.5 for p in probs]
        y_test_bool = [y_bool[i] for i in test_idx]
        y_test_int = [1 if y else 0 for y in y_test_bool]
        for i, prob in zip(test_idx, probs, strict=True):
            oof_prob[i] = prob
        per_fold.append(
            {
                "fold_id": fold_id,
                "n_train": len(train_idx),
                "n_test": len(test_idx),
                "test_positive_count": sum(y_test_int),
                "test_negative_count": len(y_test_int) - sum(y_test_int),
                "roc_auc": float(roc_auc_score(y_test_int, probs)),
                "average_precision": float(average_precision_score(y_test_int, probs)),
                "balanced_accuracy": float(balanced_accuracy_score(y_test_bool, preds)),
                "f1": float(f1_score(y_test_bool, preds, zero_division=0)),
                "confusion_matrix": _confusion_dict(y_test_bool, preds),
            }
        )

    if any(math.isnan(p) for p in oof_prob):
        raise MLOfflineBridgeRecommendableScorerV3Error("OOF probabilities did not cover all deduped rows")

    aggregate_ranking = _ranking_metrics(oof_prob, y_bool)
    aggregate_preds = [p >= 0.5 for p in oof_prob]
    per_fold_stats = {
        key: _mean_std([fold.get(key) for fold in per_fold])
        for key in ("roc_auc", "average_precision", "balanced_accuracy", "f1")
    }

    shadow_idx = [i for i, r in enumerate(rows) if r.get("review_pool_variant") == POOL_SHADOW]
    neg_idx = [i for i, r in enumerate(rows) if r.get("review_pool_variant") == POOL_NEG]
    top_idx = [i for i, r in enumerate(rows) if r.get("review_pool_variant") == POOL_TOP]
    rank_v1_idx = [i for i, r in enumerate(rows) if r.get("ranking_run_id") == RANK_V1]
    rank_shadow_idx = [i for i, r in enumerate(rows) if r.get("ranking_run_id") == RANK_SHADOW]
    bucket_indices: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        if row.get("review_pool_variant") != POOL_SHADOW:
            continue
        bucket = _disagreement_bucket(row)
        if bucket:
            bucket_indices[bucket].append(i)

    stratified = [
        _stratified_metrics(rows, oof_prob, indices=list(range(len(rows))), label="all_deduped_130_rows"),
        _stratified_metrics(rows, oof_prob, indices=neg_idx, label="negative_mining_selected_62_rows"),
        _stratified_metrics(rows, oof_prob, indices=top_idx, label="top_ranked_selected_8_rows"),
        _stratified_metrics(rows, oof_prob, indices=shadow_idx, label="shadow_pilot_60_rows"),
        _stratified_metrics(rows, oof_prob, indices=rank_v1_idx, label="rank-83787b91ef_deduped_70_rows"),
        _stratified_metrics(rows, oof_prob, indices=rank_shadow_idx, label="rank-5a7efa5ca3_deduped_60_rows"),
    ]
    for bucket, indices in sorted(bucket_indices.items()):
        stratified.append(
            _stratified_metrics(
                rows,
                oof_prob,
                indices=indices,
                label=f"shadow_by_disagreement_bucket_{bucket}",
            )
        )

    result = {
        "cv": {
            "strategy": "StratifiedKFold(n_splits=5, shuffle=True)",
            "random_seed": random_seed,
            "estimator": "StandardScaler + LogisticRegression(max_iter=1000, solver='liblinear')",
            "target": TARGET,
            "primary_evaluation_slice": "deduped_130_unique_work_ids",
        },
        "aggregate_oof": {
            **aggregate_ranking,
            "balanced_accuracy": float(balanced_accuracy_score(y_bool, aggregate_preds)),
            "f1": float(f1_score(y_bool, aggregate_preds, zero_division=0)),
            "confusion_matrix": _confusion_dict(y_bool, aggregate_preds),
        },
        "per_fold": per_fold,
        "per_fold_summary": per_fold_stats,
        "stratified_oof_metrics_deduped": stratified,
        "oof_predictions_deduped": [
            {
                "row_id": str(row.get("row_id") or ""),
                "work_id": row.get("work_id"),
                "internal_work_id": work_id,
                "review_pool_variant": row.get("review_pool_variant"),
                "ranking_run_id": row.get("ranking_run_id"),
                "disagreement_bucket": _disagreement_bucket(row),
                "label": bool(row[TARGET]),
                "probability": oof_prob[i],
            }
            for i, (row, work_id) in enumerate(zip(rows, work_ids, strict=True))
        ],
    }
    return result, oof_prob


def _grouped_cv_diagnostic(
    row_level_rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    random_seed: int,
) -> dict[str, Any]:
    work_ids = [str(row.get("work_id") or "") for row in row_level_rows]
    internal_ids = [int(row["internal_work_id"]) for row in row_level_rows]
    x = [vectors_by_work[iid] for iid in internal_ids]
    y_bool = [bool(row[TARGET]) for row in row_level_rows]
    y_int = [1 if y else 0 for y in y_bool]
    groups = work_ids
    splitter = GroupKFold(n_splits=5)
    oof_prob = [float("nan")] * len(row_level_rows)
    for train_idx_arr, test_idx_arr in splitter.split(x, y_int, groups=groups):
        train_idx = [int(i) for i in train_idx_arr]
        test_idx = [int(i) for i in test_idx_arr]
        model = make_pipeline(
            StandardScaler(),
            LogisticRegression(max_iter=1000, solver="liblinear", random_state=random_seed),
        )
        model.fit([x[i] for i in train_idx], [y_int[i] for i in train_idx])
        probs = [float(p) for p in model.predict_proba([x[i] for i in test_idx])[:, 1]]
        for i, prob in zip(test_idx, probs, strict=True):
            oof_prob[i] = prob
    metrics = _ranking_metrics(oof_prob, y_bool)
    return {
        "strategy": "GroupKFold(n_splits=5) grouped by work_id",
        "note": "duplicate_sensitive diagnostic only; not used for recommended_next_stage",
        **ROW_LEVEL_AUDIT_FLAGS,
        **metrics,
    }


def _map_deduped_probs_to_row_level(
    row_level_rows: list[dict[str, Any]],
    deduped_rows: list[dict[str, Any]],
    deduped_oof: list[float],
) -> list[float]:
    prob_by_work = {
        str(row.get("work_id") or ""): deduped_oof[i] for i, row in enumerate(deduped_rows)
    }
    mapped: list[float] = []
    for row in row_level_rows:
        work_id = str(row.get("work_id") or "")
        prob = prob_by_work.get(work_id)
        if prob is None or math.isnan(prob):
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"missing deduped OOF probability for row-level work_id={work_id!r}"
            )
        mapped.append(prob)
    return mapped


def _row_level_audit_stratified(
    row_level_rows: list[dict[str, Any]],
    row_level_probs: list[float],
    *,
    conflict_work_ids: set[str],
) -> list[dict[str, Any]]:
    flags = dict(ROW_LEVEL_AUDIT_FLAGS)
    flags["conflict_sensitive"] = True
    neg_idx = [i for i, r in enumerate(row_level_rows) if r.get("review_pool_variant") == POOL_NEG]
    top_idx = [i for i, r in enumerate(row_level_rows) if r.get("review_pool_variant") == POOL_TOP]
    shadow_idx = [i for i, r in enumerate(row_level_rows) if r.get("review_pool_variant") == POOL_SHADOW]
    bucket_indices: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(row_level_rows):
        if row.get("review_pool_variant") != POOL_SHADOW:
            continue
        bucket = _disagreement_bucket(row)
        if bucket:
            bucket_indices[bucket].append(i)
    strata = [
        _stratified_metrics(
            row_level_rows,
            row_level_probs,
            indices=list(range(len(row_level_rows))),
            label="all_row_level_160_rows",
            audit_flags=flags,
        ),
        _stratified_metrics(
            row_level_rows,
            row_level_probs,
            indices=neg_idx,
            label="negative_mining_row_level_70_rows",
            audit_flags=flags,
        ),
        _stratified_metrics(
            row_level_rows,
            row_level_probs,
            indices=top_idx,
            label="top_ranked_row_level_30_rows",
            audit_flags=flags,
        ),
        _stratified_metrics(
            row_level_rows,
            row_level_probs,
            indices=shadow_idx,
            label="shadow_pilot_row_level_60_rows",
            audit_flags=flags,
        ),
    ]
    for bucket, indices in sorted(bucket_indices.items()):
        strata.append(
            _stratified_metrics(
                row_level_rows,
                row_level_probs,
                indices=indices,
                label=f"shadow_row_level_by_bucket_{bucket}",
                audit_flags=flags,
            )
        )
    if conflict_work_ids:
        conflict_idx = [
            i for i, r in enumerate(row_level_rows) if str(r.get("work_id") or "") in conflict_work_ids
        ]
        strata.append(
            _stratified_metrics(
                row_level_rows,
                row_level_probs,
                indices=conflict_idx,
                label="derived_target_conflict_rows_audit_only",
                audit_flags={**flags, "conflict_sensitive": True},
            )
        )
    return strata


def _full_fit(
    rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    random_seed: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    work_ids = _internal_work_ids(rows, expected_count=len(rows))
    x = [vectors_by_work[work_id] for work_id in work_ids]
    y_bool = [bool(row[TARGET]) for row in rows]
    y_int = [1 if y else 0 for y in y_bool]
    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=1000, solver="liblinear", random_state=random_seed),
    )
    model.fit(x, y_int)
    probs = [float(p) for p in model.predict_proba(x)[:, 1]]
    preds = [p >= 0.5 for p in probs]
    scaler: StandardScaler = model.named_steps["standardscaler"]
    lr: LogisticRegression = model.named_steps["logisticregression"]
    metrics = {
        **_ranking_metrics(probs, y_bool),
        "balanced_accuracy": float(balanced_accuracy_score(y_bool, preds)),
        "f1": float(f1_score(y_bool, preds, zero_division=0)),
        "confusion_matrix": _confusion_dict(y_bool, preds),
        "note": "Full-slice fit metrics are in-sample only and not validation.",
        "evaluation_slice": "deduped_130_unique_work_ids",
    }
    frozen = {
        "scorer_version": SCORER_VERSION,
        "fit_mode": FIT_MODE,
        "target": TARGET,
        "feature_source": "embeddings.vector",
        "embedding_version": EMBEDDING_VERSION,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_dimensions": len(x[0]) if x else 0,
        "random_seed": random_seed,
        "estimator": "StandardScaler + LogisticRegression(max_iter=1000, solver='liblinear')",
        "scaler_mean": [float(v) for v in scaler.mean_.tolist()],
        "scaler_scale": [float(v) for v in scaler.scale_.tolist()],
        "coef": [float(v) for v in lr.coef_[0].tolist()],
        "intercept": float(lr.intercept_[0]),
        "classes": [int(v) for v in lr.classes_.tolist()],
    }
    return metrics, frozen


def _median(values: list[float]) -> float | None:
    clean = [float(v) for v in values if isinstance(v, (int, float)) and math.isfinite(float(v))]
    if not clean:
        return None
    return float(statistics.median(clean))


def _verdict_three_tier(
    *,
    strong: bool,
    partial: bool,
    strong_label: str,
    partial_label: str,
    fail_label: str,
) -> str:
    if strong:
        return strong_label
    if partial:
        return partial_label
    return fail_label


def _targeted_decision_readouts(
    deduped_rows: list[dict[str, Any]],
    deduped_oof: list[float],
) -> dict[str, Any]:
    shadow_indices = [i for i, r in enumerate(deduped_rows) if r.get("review_pool_variant") == POOL_SHADOW]
    shadow_pos_probs = [
        deduped_oof[i] for i in shadow_indices if deduped_rows[i].get(TARGET) is True
    ]
    shadow_neg_probs = [
        deduped_oof[i] for i in shadow_indices if deduped_rows[i].get(TARGET) is False
    ]
    shadow_pos_median = _median(shadow_pos_probs)
    shadow_neg_median = _median(shadow_neg_probs)

    def _rows_for_bucket(bucket: str) -> list[int]:
        return [
            i
            for i in shadow_indices
            if _disagreement_bucket(deduped_rows[i]) == bucket
        ]

    # 1. high_ml_low_bridge_score
    hml_idx = _rows_for_bucket("high_ml_low_bridge_score")
    hml_pos_probs = [deduped_oof[i] for i in hml_idx if deduped_rows[i].get(TARGET) is True]
    hml_mean = float(statistics.fmean(hml_pos_probs)) if hml_pos_probs else None
    hml_above_median = (
        sum(1 for p in hml_pos_probs if shadow_pos_median is not None and p >= shadow_pos_median)
        / len(hml_pos_probs)
        if hml_pos_probs and shadow_pos_median is not None
        else None
    )
    hml_verdict = _verdict_three_tier(
        strong=hml_above_median is not None and hml_above_median >= 0.7 and hml_mean is not None
        and shadow_pos_median is not None
        and hml_mean >= shadow_pos_median,
        partial=hml_above_median is not None and hml_above_median >= 0.5,
        strong_label="learns_social_platform_bridge_signal",
        partial_label="partial",
        fail_label="fails",
    )

    # 2. high_bridge_score_low_ml
    hbl_idx = _rows_for_bucket("high_bridge_score_low_ml")
    hbl_pos = [deduped_oof[i] for i in hbl_idx if deduped_rows[i].get(TARGET) is True]
    hbl_neg = [deduped_oof[i] for i in hbl_idx if deduped_rows[i].get(TARGET) is False]
    hbl_pairs = len(hbl_pos) * len(hbl_neg)
    hbl_correct = sum(1 for n in hbl_neg for p in hbl_pos if n < p)
    hbl_pairwise = hbl_correct / hbl_pairs if hbl_pairs else None
    hbl_verdict = _verdict_three_tier(
        strong=hbl_pairwise is not None and hbl_pairwise > 0.75,
        partial=hbl_pairwise is not None and hbl_pairwise > 0.5,
        strong_label="rejects_heuristic_false_positives",
        partial_label="partial",
        fail_label="fails",
    )

    # 3. promoted_by_hybrid
    prom_idx = _rows_for_bucket("promoted_by_hybrid")
    prom_pairs = sorted(
        [(deduped_oof[i], bool(deduped_rows[i][TARGET])) for i in prom_idx],
        key=lambda item: (-item[0], item[1]),
    )
    prom_p10 = precision_at_k(prom_pairs, 10) if len(prom_pairs) >= 10 else None
    prom_above_neg_median = (
        sum(
            1
            for i in prom_idx
            if shadow_neg_median is not None and deduped_oof[i] >= shadow_neg_median
        )
        if shadow_neg_median is not None
        else 0
    )

    # 4. demoted_by_hybrid
    dem_idx = _rows_for_bucket("demoted_by_hybrid")
    ml_probs = [
        _as_float(deduped_rows[i].get("ml_probability"))
        for i in dem_idx
        if _as_float(deduped_rows[i].get("ml_probability")) is not None
    ]
    ml_high = _median(ml_probs) or 0.5
    competitive = [
        deduped_oof[i]
        for i in dem_idx
        if deduped_rows[i].get(TARGET) is True
        and (_as_float(deduped_rows[i].get("ml_probability")) or 0.0) >= ml_high
    ]
    correct_rej = [
        deduped_oof[i]
        for i in dem_idx
        if deduped_rows[i].get(TARGET) is False
        and (_as_float(deduped_rows[i].get("ml_probability")) or 1.0) <= ml_high
    ]

    return {
        "high_ml_low_bridge_score": {
            "row_count": len(hml_idx),
            "positive_count": len(hml_pos_probs),
            "mean_oof_probability_positives": hml_mean,
            "shadow_positive_median_oof": shadow_pos_median,
            "fraction_positives_oof_gte_shadow_positive_median": hml_above_median,
            "verdict": hml_verdict,
        },
        "high_bridge_score_low_ml": {
            "row_count": len(hbl_idx),
            "positive_count": len(hbl_pos),
            "negative_count": len(hbl_neg),
            "mean_oof_negatives": float(statistics.fmean(hbl_neg)) if hbl_neg else None,
            "mean_oof_positives": float(statistics.fmean(hbl_pos)) if hbl_pos else None,
            "pairwise_correct_ordering_fraction_neg_below_pos": hbl_pairwise,
            "verdict": hbl_verdict,
        },
        "promoted_by_hybrid": {
            "row_count": len(prom_idx),
            "precision_at_10_by_oof_rank_within_promoted": prom_p10,
            "count_oof_gte_shadow_negative_median": prom_above_neg_median,
            "expected_above_median_count": 14,
            "shadow_negative_median_oof": shadow_neg_median,
        },
        "demoted_by_hybrid": {
            "row_count": len(dem_idx),
            "competitive_demotion_subgroup": {
                "definition": "bridge_recommendable=true with ml_probability at/above demoted-row median",
                "count": len(competitive),
                "mean_oof": float(statistics.fmean(competitive)) if competitive else None,
            },
            "correct_rejection_subgroup": {
                "definition": "bridge_recommendable=false with ml_probability at/below demoted-row median",
                "count": len(correct_rej),
                "mean_oof": float(statistics.fmean(correct_rej)) if correct_rej else None,
            },
        },
    }


def _v2_baseline_delta(
    deduped_rows: list[dict[str, Any]],
    deduped_oof: list[float],
    *,
    v2_payload: dict[str, Any] | None,
    dedupe_conflicts: list[dict[str, Any]],
) -> dict[str, Any]:
    if v2_payload is None:
        return {"status": "skipped", "reason": "no v2 baseline artifact provided"}
    v2_preds = v2_payload.get("evaluation", {}).get("learned_cv", {}).get("oof_predictions")
    if not isinstance(v2_preds, list):
        raise MLOfflineBridgeRecommendableScorerV3Error("v2 baseline missing learned_cv.oof_predictions")
    v2_by_work = {
        str(item.get("work_id") or ""): item
        for item in v2_preds
        if isinstance(item, dict) and item.get("work_id")
    }
    if len(v2_by_work) != V2_OVERLAP_WORK_IDS:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"v2 baseline work_id set has {len(v2_by_work)} entries; expected {V2_OVERLAP_WORK_IDS}"
        )
    v3_by_work = {str(row.get("work_id") or ""): deduped_oof[i] for i, row in enumerate(deduped_rows)}
    overlap_work_ids = sorted(v2_by_work)
    scores: list[float] = []
    labels: list[bool] = []
    label_drift: list[dict[str, Any]] = []
    for work_id in overlap_work_ids:
        v3_row = next((r for r in deduped_rows if str(r.get("work_id")) == work_id), None)
        if v3_row is None:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"v2 overlap work_id {work_id!r} missing from v3 deduped slice"
            )
        prob = v3_by_work.get(work_id)
        if prob is None:
            raise MLOfflineBridgeRecommendableScorerV3Error(
                f"missing v3 deduped OOF for v2 overlap work_id {work_id!r}"
            )
        scores.append(prob)
        labels.append(bool(v3_row[TARGET]))
        v2_label = v2_by_work[work_id].get("label")
        if v2_label != v3_row[TARGET]:
            label_drift.append(
                {
                    "work_id": work_id,
                    "v2_label": v2_label,
                    "v3_deduped_label": v3_row[TARGET],
                    "v3_winning_review_pool_variant": v3_row.get("review_pool_variant"),
                }
            )
    metrics = _ranking_metrics(scores, labels)
    excluded_work_ids = sorted(set(v3_by_work) - set(overlap_work_ids))
    excluded_rows = [r for r in deduped_rows if str(r.get("work_id") or "") in excluded_work_ids]
    excluded_scores = [v3_by_work[w] for w in excluded_work_ids if w in v3_by_work]
    excluded_labels = [bool(r[TARGET]) for r in excluded_rows]
    excluded_metrics = _ranking_metrics(excluded_scores, excluded_labels)
    v2_ref_auc = v2_payload.get("evaluation", {}).get("learned_cv", {}).get("aggregate_oof", {}).get("roc_auc")
    if not isinstance(v2_ref_auc, (int, float)):
        v2_ref_auc = V2_AGGREGATE_ROC_AUC_REFERENCE
    v3_auc = metrics.get("roc_auc")
    regression = (
        isinstance(v3_auc, (int, float))
        and float(v3_auc) < float(v2_ref_auc) - MAJOR_REGRESSION_AUC_DELTA
    )
    return {
        "status": "ok",
        "comparison_scope": "v3_deduped_oof_on_v2_work_id_set",
        "v2_work_id_count": len(overlap_work_ids),
        "v3_scores_available_count": len(scores),
        "v2_reference_aggregate_roc_auc": float(v2_ref_auc),
        "v3_on_v2_work_id_set_metrics": metrics,
        "v3_on_v2_work_id_set_label_counts": {
            "positive_count": sum(labels),
            "negative_count": len(labels) - sum(labels),
        },
        "excluded_from_v2_work_id_set": {
            "work_id_count": len(excluded_work_ids),
            "label_counts": {
                "positive_count": sum(excluded_labels),
                "negative_count": len(excluded_labels) - sum(excluded_labels),
            },
            "metrics_same_v3_deduped_oof": excluded_metrics,
            "note": (
                "Shadow-pilot-only deduped works not present in the v2 100-work training slice; "
                "they are included in aggregate deduped-130 OOF AUC but excluded from v2 drift comparison."
            ),
        },
        "major_regression_vs_v2_aggregate": regression,
        "label_policy_drift_rows": label_drift,
        "label_policy_drift_count": len(label_drift),
        "dedupe_policy_conflicts_reported": dedupe_conflicts,
        "note": (
            "rank-83787b91ef_deduped_70_rows stratum is descriptive only; "
            "v2 comparison uses the 100-work v2 overlap set regardless of winning pool."
        ),
    }


def _recommended_next_stage(
    *,
    learned_cv: dict[str, Any],
    full_fit_metrics: dict[str, Any],
    targeted: dict[str, Any],
    v2_delta: dict[str, Any],
) -> str:
    oof_auc = learned_cv.get("aggregate_oof", {}).get("roc_auc")
    full_auc = full_fit_metrics.get("roc_auc")
    gap = (
        float(full_auc) - float(oof_auc)
        if isinstance(oof_auc, (int, float)) and isinstance(full_auc, (int, float))
        else 0.0
    )
    if gap > OVERFIT_AUC_GAP_THRESHOLD:
        return "caution_overfit_on_deduped_slice_collect_more_labels"

    hml = targeted.get("high_ml_low_bridge_score", {}).get("verdict")
    hbl = targeted.get("high_bridge_score_low_ml", {}).get("verdict")
    prom = targeted.get("promoted_by_hybrid", {})
    prom_ok = (
        isinstance(prom.get("count_oof_gte_shadow_negative_median"), int)
        and prom["count_oof_gte_shadow_negative_median"] >= prom.get("expected_above_median_count", 14)
    )
    shadow_buckets_ok = (
        hml in {"learns_social_platform_bridge_signal", "partial"}
        and hbl in {"rejects_heuristic_false_positives", "partial"}
        and prom_ok
    )
    v2_regress = v2_delta.get("major_regression_vs_v2_aggregate") is True

    if (
        hml == "learns_social_platform_bridge_signal"
        and hbl == "rejects_heuristic_false_positives"
        and prom_ok
        and not v2_regress
    ):
        return "evaluate_hybrid_bridge_scorer_v3_offline_next"
    if shadow_buckets_ok and v2_regress:
        return "collect_more_labels_or_resolve_work_id_conflicts_before_hybrid_eval"
    return "do_not_authorize_bridge_hybrid_serving_collect_more_targeted_labels"


def _overlap_count_from_label_metadata(label_payload: dict[str, Any]) -> int:
    ingest = label_payload.get("metadata", {}).get("bridge_shadow_pilot_v1_ingest")
    if isinstance(ingest, dict):
        value = ingest.get("overlap_count_with_v13")
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return EXPECTED_OVERLAP_COUNT_WITH_V13


def _repo_root_from_label_path(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.parent.name == "audit" and resolved.parent.parent.name == "docs":
        return resolved.parent.parent.parent
    return Path.cwd().resolve()


def _metadata_input_paths(label_payload: dict[str, Any], *, label_dataset_path: Path) -> list[dict[str, Any]]:
    root = _repo_root_from_label_path(label_dataset_path)
    out: list[dict[str, Any]] = []
    meta = label_payload.get("metadata") or {}
    for ingest_key in (
        "bridge_negative_mining_v1_ingest",
        "bridge_top_ranked_v1_ingest",
        "bridge_shadow_pilot_v1_ingest",
    ):
        ingest = meta.get(ingest_key)
        if not isinstance(ingest, dict):
            continue
        for name, key in (
            (f"{ingest_key}_labeled_worksheet", "labeled_worksheet_path"),
            (f"{ingest_key}_context_sidecar", "context_sidecar_path"),
        ):
            value = ingest.get(key)
            if isinstance(value, str) and value:
                path = (root / value).resolve() if not Path(value).is_absolute() else Path(value).resolve()
                record: dict[str, Any] = {"name": name, "path": value}
                if path.is_file():
                    record["sha256"] = sha256_file(path)
                else:
                    record["sha256"] = None
                    record["missing"] = True
                out.append(record)
    return out


def build_ml_offline_bridge_recommendable_scorer_v3_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v2_baseline_path: Path | None = None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    label_path = label_dataset_path.resolve()
    readiness_path = readiness_matrix_path.resolve()
    embeddings_path = embeddings_provenance_path.resolve()
    for path in (label_path, readiness_path, embeddings_path):
        if not path.is_file():
            raise MLOfflineBridgeRecommendableScorerV3Error(f"required input not found: {path}")

    label_sha = sha256_file(label_path)
    readiness_sha = sha256_file(readiness_path)
    embeddings_sha = sha256_file(embeddings_path)
    label_payload = _load_json_object(label_path, label="label dataset")
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")

    if label_payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineBridgeRecommendableScorerV3Error(f"label dataset must be {LABEL_DATASET_VERSION}")

    readiness_groups = _validate_readiness_matrix(readiness_payload, label_dataset_sha256=label_sha)
    embedding_dimensions = _validate_embeddings_provenance(embeddings_payload)

    row_level_rows = _slice_row_level_rows(label_payload)
    row_level_counts = _validate_row_level_counts(row_level_rows)
    deduped_rows, dedupe_meta = _dedupe_by_work_id(row_level_rows)
    deduped_counts = _validate_deduped_counts(deduped_rows, dedupe_meta)

    overlap_count = _overlap_count_from_label_metadata(label_payload)
    if overlap_count != EXPECTED_OVERLAP_COUNT_WITH_V13:
        raise MLOfflineBridgeRecommendableScorerV3Error(
            f"overlap_count_with_v13_bridge_slice={overlap_count!r}; expected {EXPECTED_OVERLAP_COUNT_WITH_V13!r}"
        )

    deduped_work_ids = _internal_work_ids(deduped_rows, expected_count=DEDUPED_ROWS)
    vectors_by_work = _load_slice_embeddings_select_only(
        conn,
        internal_work_ids=deduped_work_ids,
        expected_dimensions=embedding_dimensions,
    )

    learned_cv, deduped_oof = _learned_cv(deduped_rows, vectors_by_work, random_seed=random_seed)
    row_level_oof = _map_deduped_probs_to_row_level(row_level_rows, deduped_rows, deduped_oof)
    conflict_ids = {c["work_id"] for c in dedupe_meta.get("derived_target_conflicts", []) if isinstance(c, dict)}
    row_level_strata = _row_level_audit_stratified(
        row_level_rows,
        row_level_oof,
        conflict_work_ids=conflict_ids,
    )
    grouped_cv = _grouped_cv_diagnostic(row_level_rows, vectors_by_work, random_seed=random_seed)

    heuristic_arms = {
        "final_score": _heuristic_arm(deduped_rows, field="final_score"),
        "bridge_score": _heuristic_arm(deduped_rows, field="bridge_score"),
        "semantic_score": _heuristic_arm(deduped_rows, field="semantic_score"),
    }
    full_fit_metrics, frozen_scorer = _full_fit(deduped_rows, vectors_by_work, random_seed=random_seed)
    targeted = _targeted_decision_readouts(deduped_rows, deduped_oof)

    v2_payload = _load_json_object(v2_baseline_path, label="v2 baseline") if v2_baseline_path else None
    if v2_payload is not None and v2_payload.get("scorer_version") != V2_SCORER_VERSION:
        raise MLOfflineBridgeRecommendableScorerV3Error("v2 baseline scorer_version mismatch")
    v2_delta = _v2_baseline_delta(
        deduped_rows,
        deduped_oof,
        v2_payload=v2_payload,
        dedupe_conflicts=dedupe_meta.get("derived_target_conflicts", []),
    )

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    input_records = [
        {"name": "label_dataset", "path": portable_repo_path(label_path), "sha256": label_sha},
        {"name": "readiness_matrix", "path": portable_repo_path(readiness_path), "sha256": readiness_sha},
        {"name": "embeddings_provenance", "path": portable_repo_path(embeddings_path), "sha256": embeddings_sha},
        *_metadata_input_paths(label_payload, label_dataset_path=label_path),
    ]
    if v2_baseline_path is not None:
        input_records.append(
            {
                "name": "v2_baseline_scorer",
                "path": portable_repo_path(v2_baseline_path.resolve()),
                "sha256": sha256_file(v2_baseline_path),
            }
        )

    overfit_gap = None
    oof_auc = learned_cv["aggregate_oof"].get("roc_auc")
    full_auc = full_fit_metrics.get("roc_auc")
    if isinstance(oof_auc, (int, float)) and isinstance(full_auc, (int, float)):
        overfit_gap = float(full_auc) - float(oof_auc)

    return {
        "artifact_type": ARTIFACT_TYPE,
        "scorer_version": SCORER_VERSION,
        "fit_mode": FIT_MODE,
        "generated_at": generated_at,
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "scorer_version": SCORER_VERSION,
            "fit_mode": FIT_MODE,
            "target": TARGET,
            "random_seed": random_seed,
            "inputs": input_records,
            "training_slice_filter": {
                "split": "audit_only",
                "family": FAMILY,
                "allowed_review_pool_variants": sorted(ALLOWED_REVIEW_POOL_VARIANTS),
                "target": TARGET,
                "target_must_be_boolean": True,
                "dedupe_policy": "ml_bridge_shadow_pilot_audit > ml_bridge_top_ranked_validation_audit > ml_bridge_negative_mining_audit",
            },
            "row_level_160": row_level_counts,
            "deduped_130": deduped_counts,
            "overlap_count_with_v13_bridge_slice": overlap_count,
            "readiness_groups": readiness_groups,
            "embedding_coverage": {
                "embedding_version": EMBEDDING_VERSION,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_dimensions": embedding_dimensions,
                "requested_internal_work_id_count": len(deduped_work_ids),
                "loaded_vector_count": len(vectors_by_work),
                "coverage_complete": len(vectors_by_work) == DEDUPED_ROWS,
                "query_limited_to_deduped_primary_slice": True,
            },
        },
        "evaluation": {
            "learned_cv_primary_deduped": learned_cv,
            "learned_cv_row_level_grouped_by_work_id_diagnostic": grouped_cv,
            "stratified_oof_metrics_row_level_audit": row_level_strata,
            "heuristic_arms_deduped": heuristic_arms,
            "targeted_decision_readouts": targeted,
            "v2_baseline_delta": v2_delta,
        },
        "in_sample_full_fit_only_not_validation": full_fit_metrics,
        "overfit_sanity": {
            "in_sample_roc_auc_minus_oof_roc_auc": overfit_gap,
            "caution_threshold": OVERFIT_AUC_GAP_THRESHOLD,
            "caution_overfit": overfit_gap is not None and overfit_gap > OVERFIT_AUC_GAP_THRESHOLD,
        },
        "frozen_scorer": frozen_scorer,
        "caveats": list(CAVEATS),
        "recommended_next_stage": _recommended_next_stage(
            learned_cv=learned_cv,
            full_fit_metrics=full_fit_metrics,
            targeted=targeted,
            v2_delta=v2_delta,
        ),
    }


def markdown_from_ml_offline_bridge_recommendable_scorer_v3(payload: dict[str, Any]) -> str:
    meta = payload["metadata"]
    learned = payload["evaluation"]["learned_cv_primary_deduped"]["aggregate_oof"]
    stratified = payload["evaluation"]["learned_cv_primary_deduped"].get("stratified_oof_metrics_deduped", [])
    targeted = payload["evaluation"].get("targeted_decision_readouts", {})
    v2_delta = payload["evaluation"].get("v2_baseline_delta", {})
    lines = [
        "# Offline bridge recommendable scorer v3 — three-pool shadow audit diagnostic",
        "",
        "Offline diagnostic model for `bridge_recommendable` on the v14 bridge audit slice "
        "(negative-mining, top-ranked validation, shadow-pilot). Primary CV uses **130 deduped** "
        "unique work_ids; **160 row-level** readouts are audit-only.",
        "",
        "## Slices",
        "",
        f"- Row-level audit rows: {meta['row_level_160']['row_count']} "
        f"({meta['row_level_160']['positive_count']} pos / {meta['row_level_160']['negative_count']} neg)",
        f"- Deduped primary rows: {meta['deduped_130']['row_count']} "
        f"({meta['deduped_130']['positive_count']} pos / {meta['deduped_130']['negative_count']} neg)",
        f"- Overlap with v13 bridge slice: {meta.get('overlap_count_with_v13_bridge_slice')}",
        f"- Derived-target conflicts: {meta['deduped_130'].get('derived_target_conflict_count')}",
        "",
        "## Learned OOF CV (deduped 130)",
        "",
        f"- ROC AUC: {learned.get('roc_auc')}",
        f"- Average precision: {learned.get('average_precision')}",
        f"- Pairwise accuracy: {learned.get('pairwise_accuracy')}",
        f"- Precision@5 / @10 / @20: {learned.get('precision_at_5')} / {learned.get('precision_at_10')} / {learned.get('precision_at_20')}",
        "",
        "## Stratified deduped OOF metrics",
        "",
        "| stratum | n | pos | neg | ROC AUC | AP | P@10 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for s in stratified:
        lines.append(
            f"| {s.get('stratum')} | {s.get('n')} | {s.get('positive_count')} | {s.get('negative_count')} | "
            f"{s.get('roc_auc')} | {s.get('average_precision')} | {s.get('precision_at_10')} |"
        )
    excluded = v2_delta.get("excluded_from_v2_work_id_set") or {}
    lines += [
        "",
        "## Targeted shadow disagreement readouts",
        "",
        f"- high_ml_low_bridge_score verdict: {targeted.get('high_ml_low_bridge_score', {}).get('verdict')}",
        f"- high_bridge_score_low_ml verdict: {targeted.get('high_bridge_score_low_ml', {}).get('verdict')}",
        f"- promoted_by_hybrid above-median vs shadow negatives: "
        f"{targeted.get('promoted_by_hybrid', {}).get('count_oof_gte_shadow_negative_median')}/"
        f"{targeted.get('promoted_by_hybrid', {}).get('row_count')}",
        "",
        "## v2 baseline delta (100 work_ids)",
        "",
        "Uses the **same v3 deduped OOF probabilities**, but only on the 100 work_ids from the v2 artifact "
        "(not the full deduped-130 slice). The other **30 deduped works** are shadow-pilot-only labels "
        "outside the v2 set; they are excluded here but included in aggregate deduped-130 AUC above.",
        "",
        f"- v3 ROC AUC on v2 work-id set (100 works): {v2_delta.get('v3_on_v2_work_id_set_metrics', {}).get('roc_auc')}",
        f"- v2 overlap subset labels (v3 deduped targets): "
        f"{v2_delta.get('v3_on_v2_work_id_set_label_counts', {}).get('positive_count')} pos / "
        f"{v2_delta.get('v3_on_v2_work_id_set_label_counts', {}).get('negative_count')} neg",
        f"- Excluded shadow-only works (not in v2 set): {excluded.get('work_id_count')} works; "
        f"subset ROC AUC {excluded.get('metrics_same_v3_deduped_oof', {}).get('roc_auc')} "
        f"({excluded.get('label_counts', {}).get('positive_count')} pos / "
        f"{excluded.get('label_counts', {}).get('negative_count')} neg)",
        f"- Major regression vs v2 aggregate (~0.65): {v2_delta.get('major_regression_vs_v2_aggregate')}",
        f"- Label policy drift count (v2 label vs v3 deduped label): {v2_delta.get('label_policy_drift_count')}",
        "",
        "## Overfit sanity",
        "",
        f"- In-sample minus OOF ROC AUC gap: {payload.get('overfit_sanity', {}).get('in_sample_roc_auc_minus_oof_roc_auc')}",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        f"Recommended next stage: `{payload.get('recommended_next_stage')}`.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_ml_offline_bridge_recommendable_scorer_v3(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v2_baseline_path: Path | None,
    json_path: Path,
    markdown_path: Path | None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    payload = build_ml_offline_bridge_recommendable_scorer_v3_payload(
        conn,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        v2_baseline_path=v2_baseline_path,
        random_seed=random_seed,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_ml_offline_bridge_recommendable_scorer_v3(payload), encoding="utf-8"
        )
    return payload


def run_ml_offline_bridge_recommendable_scorer_v3_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v2_baseline_path: Path | None,
    output_json: Path,
    markdown_output: Path | None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> None:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn) as conn:
        write_ml_offline_bridge_recommendable_scorer_v3(
            conn,
            label_dataset_path=label_dataset_path,
            readiness_matrix_path=readiness_matrix_path,
            embeddings_provenance_path=embeddings_provenance_path,
            v2_baseline_path=v2_baseline_path,
            json_path=output_json,
            markdown_path=markdown_output,
            random_seed=random_seed,
        )
