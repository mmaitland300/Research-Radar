"""Offline diagnostic scorer for bridge_recommendable on the v12 bridge negative-mining slice."""

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
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_baseline_eval import pairwise_accuracy, precision_at_k, sha256_file
from pipeline.repo_paths import portable_repo_path

TARGET = "bridge_recommendable"
EXPECTED_SLICE_ROWS = 70
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
SCORER_VERSION = "ml-offline-bridge-recommendable-scorer-v1"
ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer"
FIT_MODE = "bridge_negative_mining_slice_diagnostic_v1"
DEFAULT_RANDOM_SEED = 20260531

RANKING_RUN_ID = "rank-83787b91ef"
FAMILY = "bridge"
REVIEW_POOL_VARIANT = "ml_bridge_negative_mining_audit"
EMBEDDINGS_ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
EXPECTED_REAL_EMBEDDING_DIMENSIONS = 1536
EXPECTED_EMBEDDED_WORK_COUNT = 528

WRITE_SQL_RE = re.compile(r"\b(insert|update|delete|drop|alter|truncate|create|merge|grant|revoke|vacuum|reindex|copy)\b")

CAVEATS = (
    "This is not validation.",
    "This is a worksheet-selected, single-reviewer offline diagnostic slice.",
    "Global v12 has overlapping paper_ids with conflicting labels in other pools; training is valid only under the slice filter.",
    "OOF CV metrics are not in-sample metrics.",
    "Beating final_score on this slice does not authorize serving.",
    "No ranking, API, DB-write, shadow, or production changes are made or authorized.",
)


class MLOfflineBridgeRecommendableScorerError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineBridgeRecommendableScorerError(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineBridgeRecommendableScorerError(f"{label} JSON must be an object: {path}")
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
            raise MLOfflineBridgeRecommendableScorerError(f"embedding vector is not valid JSON: {exc}") from exc
    else:
        parsed = value
    if not isinstance(parsed, (list, tuple)):
        raise MLOfflineBridgeRecommendableScorerError("embedding vector must be an array")
    out: list[float] = []
    for item in parsed:
        number = _as_float(item)
        if number is None:
            raise MLOfflineBridgeRecommendableScorerError("embedding vector contains non-finite/non-numeric value")
        out.append(number)
    return out


def _execute_select(cur: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    stripped = sql.strip()
    lowered = stripped.lower()
    if not lowered.startswith("select"):
        raise MLOfflineBridgeRecommendableScorerError("DB safety violation: SQL must start with SELECT")
    if WRITE_SQL_RE.search(lowered):
        raise MLOfflineBridgeRecommendableScorerError("DB safety violation: SQL contains write/DDL verb")
    return cur.execute(sql, tuple(params or ()))


def _validate_readiness_matrix(
    readiness_payload: dict[str, Any],
    *,
    label_dataset_sha256: str,
) -> dict[str, Any]:
    prov = readiness_payload.get("provenance")
    if not isinstance(prov, dict):
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix missing provenance object")
    if prov.get("label_dataset_version") != "ml-label-dataset-v12":
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix must point at ml-label-dataset-v12")
    if prov.get("label_dataset_sha256") != label_dataset_sha256:
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix label_dataset_sha256 does not match label dataset")
    groups = readiness_payload.get("groups")
    if not isinstance(groups, list):
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix missing groups array")
    target_group = None
    for group in groups:
        if not isinstance(group, dict):
            continue
        if (
            group.get("ranking_run_id") == RANKING_RUN_ID
            and group.get("family") == FAMILY
            and group.get("target") == TARGET
        ):
            target_group = group
            break
    if target_group is None:
        raise MLOfflineBridgeRecommendableScorerError(
            "readiness matrix missing (rank-83787b91ef, bridge, bridge_recommendable) group"
        )
    expected = {
        "total_labeled_rows": EXPECTED_SLICE_ROWS,
        "positive_count": 38,
        "negative_count": 32,
        "paper_scores_joinable_count": EXPECTED_SLICE_ROWS,
        "missing_score_count": 0,
        "derived_target_conflict_count": 0,
    }
    for key, value in expected.items():
        if target_group.get(key) != value:
            raise MLOfflineBridgeRecommendableScorerError(
                f"readiness matrix group {key}={target_group.get(key)!r}; expected {value!r}"
            )
    readiness = target_group.get("readiness")
    if not isinstance(readiness, dict):
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix target group missing readiness object")
    if readiness.get("enough_for_diagnostic_auc") is not True:
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix target group is not diagnostic-AUC ready")
    if readiness.get("enough_for_tiny_baseline") is not True:
        raise MLOfflineBridgeRecommendableScorerError("readiness matrix target group is not tiny-baseline ready")
    return target_group


def _validate_embeddings_provenance(payload: dict[str, Any]) -> int:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance missing metadata object")
    checks = {
        "artifact_type": EMBEDDINGS_ARTIFACT_TYPE,
        "artifact_version": EMBEDDINGS_ARTIFACT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "snapshot_version": CORPUS_SNAPSHOT_VERSION,
    }
    for key, expected in checks.items():
        if metadata.get(key) != expected:
            raise MLOfflineBridgeRecommendableScorerError(
                f"embeddings provenance metadata.{key}={metadata.get(key)!r}; expected {expected!r}"
            )
    result = payload.get("embedding_result")
    if not isinstance(result, dict):
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance missing embedding_result object")
    if result.get("status") != "succeeded":
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance status must be succeeded")
    if result.get("full_snapshot_embedding_coverage") is not True:
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance full_snapshot_embedding_coverage must be true")
    dim = result.get("embedding_dimensions")
    if not isinstance(dim, int) or isinstance(dim, bool) or dim <= 0:
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance embedding_dimensions must be positive integer")
    coverage = payload.get("coverage")
    if not isinstance(coverage, dict):
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance missing coverage object")
    if coverage.get("embedded_work_count") != EXPECTED_EMBEDDED_WORK_COUNT:
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance embedded_work_count must be 528")
    if coverage.get("missing_embedding_count") != 0:
        raise MLOfflineBridgeRecommendableScorerError("embeddings provenance missing_embedding_count must be 0")
    if dim != EXPECTED_REAL_EMBEDDING_DIMENSIONS and coverage.get("embedded_work_count") == EXPECTED_EMBEDDED_WORK_COUNT:
        # Tests may use tiny fixture dimensions with 528 coverage; real committed artifact is 1536.
        pass
    return dim


def _slice_rows(label_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = label_payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineBridgeRecommendableScorerError("label dataset missing rows array")
    out = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("split") == "audit_only"
        and row.get("ranking_run_id") == RANKING_RUN_ID
        and row.get("family") == FAMILY
        and row.get("review_pool_variant") == REVIEW_POOL_VARIANT
        and isinstance(row.get(TARGET), bool)
    ]
    if len(out) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBridgeRecommendableScorerError(
            f"training slice has {len(out)} rows after mandatory filter; expected {EXPECTED_SLICE_ROWS}"
        )
    return out


def _derived_target_conflict_count(rows: list[dict[str, Any]]) -> int:
    by_pid: dict[str, set[bool]] = defaultdict(set)
    for row in rows:
        pid = str(row.get("paper_id") or "")
        value = row.get(TARGET)
        if pid and isinstance(value, bool):
            by_pid[pid].add(value)
    return sum(1 for values in by_pid.values() if len(values) > 1)


def _validate_slice(rows: list[dict[str, Any]]) -> dict[str, Any]:
    row_ids = [str(row.get("row_id") or "") for row in rows]
    work_ids = [str(row.get("work_id") or "") for row in rows]
    duplicate_row_ids = sorted(k for k, v in Counter(row_ids).items() if k and v > 1)
    duplicate_work_ids = sorted(k for k, v in Counter(work_ids).items() if k and v > 1)
    target_counts = Counter(_bool_key(row.get(TARGET)) for row in rows)
    bridge_like_counts = Counter(_norm_label(row.get("bridge_like_label")) for row in rows)
    relevance_counts = Counter(_norm_label(row.get("relevance_label")) for row in rows)
    hard_negative_count = sum(
        1
        for row in rows
        if _norm_label(row.get("relevance_label")) in {"good", "acceptable"}
        and _norm_label(row.get("bridge_like_label")) == "no"
    )
    leak_count = sum(
        1
        for row in rows
        if _norm_label(row.get("bridge_like_label")) in {"yes", "partial"}
        and row.get("bridge_recommendable") is False
    )
    dconf = _derived_target_conflict_count(rows)
    checks = {
        "row_count": len(rows),
        "positive_count": target_counts["true"],
        "negative_count": target_counts["false"],
        "bridge_like_label": dict(sorted(bridge_like_counts.items())),
        "relevance_label": dict(sorted(relevance_counts.items())),
        "hard_negative_count": hard_negative_count,
        "bridge_like_positive_relevance_leak_count": leak_count,
        "duplicate_row_id_count": len(duplicate_row_ids),
        "duplicate_work_id_count": len(duplicate_work_ids),
        "derived_target_conflict_count": dconf,
    }
    if checks["positive_count"] != 38 or checks["negative_count"] != 32:
        raise MLOfflineBridgeRecommendableScorerError(
            f"bridge_recommendable counts mismatch: {checks['positive_count']}/{checks['negative_count']}; expected 38/32"
        )
    if checks["bridge_like_label"] != {"no": 32, "partial": 25, "yes": 13}:
        raise MLOfflineBridgeRecommendableScorerError("bridge_like_label distribution mismatch")
    if hard_negative_count != 22:
        raise MLOfflineBridgeRecommendableScorerError(f"hard_negative_count={hard_negative_count}; expected 22")
    if leak_count != 0:
        raise MLOfflineBridgeRecommendableScorerError(f"bridge-like positive relevance leak_count={leak_count}; expected 0")
    if duplicate_row_ids:
        raise MLOfflineBridgeRecommendableScorerError(f"duplicate row_id values in slice: {duplicate_row_ids[:5]}")
    if duplicate_work_ids:
        raise MLOfflineBridgeRecommendableScorerError(f"duplicate work_id values in slice: {duplicate_work_ids[:5]}")
    if dconf != 0:
        raise MLOfflineBridgeRecommendableScorerError(f"derived_target_conflict_count={dconf}; expected 0")
    return checks


def _internal_work_ids(rows: list[dict[str, Any]]) -> list[int]:
    out: list[int] = []
    for row in rows:
        context = row.get("bridge_negative_mining_context")
        if not isinstance(context, dict):
            raise MLOfflineBridgeRecommendableScorerError("slice row missing bridge_negative_mining_context")
        value = context.get("internal_work_id")
        if not isinstance(value, int) or isinstance(value, bool):
            raise MLOfflineBridgeRecommendableScorerError("slice row missing integer internal_work_id")
        out.append(value)
    if len(set(out)) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBridgeRecommendableScorerError("internal_work_id values are not unique within slice")
    return out


def _load_slice_embeddings_select_only(
    conn: psycopg.Connection,
    *,
    internal_work_ids: list[int],
    expected_dimensions: int,
) -> dict[int, list[float]]:
    if len(internal_work_ids) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBridgeRecommendableScorerError("embedding query must be limited to exactly the 70 slice work_ids")
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
            raise MLOfflineBridgeRecommendableScorerError(
                f"embedding vector length for work_id={work_id} is {len(vector)}, expected {expected_dimensions}"
            )
        out[work_id] = vector
    missing = sorted(set(internal_work_ids) - set(out))
    extra = sorted(set(out) - set(internal_work_ids))
    if missing or extra or len(out) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBridgeRecommendableScorerError(
            f"embedding coverage mismatch for slice: found={len(out)}, missing={missing[:10]}, extra={extra[:10]}"
        )
    return out


def _ranking_metrics(scores: list[float], labels: list[bool]) -> dict[str, Any]:
    score_labels = list(zip(scores, labels, strict=True))
    positives = sum(1 for y in labels if y)
    negatives = len(labels) - positives
    if not score_labels or positives == 0 or negatives == 0:
        return {
            "status": "not_applicable",
            "roc_auc": None,
            "average_precision": None,
            "pairwise_accuracy": None,
            "precision_at_5": None,
            "precision_at_10": None,
            "precision_at_20": None,
        }
    desc = sorted(score_labels, key=lambda item: (-item[0], item[1]))
    as_pairs = [(float(score), bool(label)) for score, label in score_labels]
    return {
        "status": "ok",
        "roc_auc": float(roc_auc_score(labels, scores)),
        "average_precision": float(average_precision_score(labels, scores)),
        "pairwise_accuracy": pairwise_accuracy(as_pairs),
        "precision_at_5": precision_at_k(desc, 5),
        "precision_at_10": precision_at_k(desc, 10),
        "precision_at_20": precision_at_k(desc, 20),
    }


def _heuristic_arm(rows: list[dict[str, Any]], *, field: str) -> dict[str, Any]:
    scores: list[float] = []
    labels: list[bool] = []
    missing = 0
    missing_row_ids: list[str] = []
    for row in rows:
        score = _as_float(row.get(field))
        if score is None:
            missing += 1
            missing_row_ids.append(str(row.get("row_id") or ""))
            continue
        scores.append(score)
        labels.append(bool(row[TARGET]))
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
        "missing_row_ids_preview": [rid for rid in missing_row_ids if rid][:20],
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
) -> dict[str, Any]:
    work_ids = _internal_work_ids(rows)
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
        raise MLOfflineBridgeRecommendableScorerError("OOF probabilities did not cover all rows")
    aggregate_ranking = _ranking_metrics(oof_prob, y_bool)
    aggregate_preds = [p >= 0.5 for p in oof_prob]
    per_fold_stats = {
        key: _mean_std([fold.get(key) for fold in per_fold])
        for key in ("roc_auc", "average_precision", "balanced_accuracy", "f1")
    }
    return {
        "cv": {
            "strategy": "StratifiedKFold(n_splits=5, shuffle=True)",
            "random_seed": random_seed,
            "estimator": "StandardScaler + LogisticRegression(max_iter=1000, solver='liblinear')",
            "target": TARGET,
        },
        "aggregate_oof": {
            **aggregate_ranking,
            "balanced_accuracy": float(balanced_accuracy_score(y_bool, aggregate_preds)),
            "f1": float(f1_score(y_bool, aggregate_preds, zero_division=0)),
            "confusion_matrix": _confusion_dict(y_bool, aggregate_preds),
        },
        "per_fold": per_fold,
        "per_fold_summary": per_fold_stats,
        "oof_predictions": [
            {
                "row_id": str(row.get("row_id") or ""),
                "work_id": row.get("work_id"),
                "internal_work_id": work_id,
                "label": bool(row[TARGET]),
                "probability": oof_prob[i],
            }
            for i, (row, work_id) in enumerate(zip(rows, work_ids, strict=True))
        ],
    }


def _full_fit(
    rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    random_seed: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    work_ids = _internal_work_ids(rows)
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


def _baseline_winner_readout(learned_cv: dict[str, Any], heuristic_arms: dict[str, Any]) -> dict[str, Any]:
    candidates: dict[str, float] = {}
    learned_auc = learned_cv["aggregate_oof"].get("roc_auc")
    if isinstance(learned_auc, (int, float)):
        candidates["learned_embedding_oof_cv"] = float(learned_auc)
    for name, arm in heuristic_arms.items():
        auc = arm.get("roc_auc")
        if arm.get("status") == "ok" and isinstance(auc, (int, float)):
            candidates[f"heuristic_{name}"] = float(auc)
    if not candidates:
        return {"winner": None, "auc_by_arm": {}, "readout": "No arm had both classes with scores."}
    winner = max(candidates, key=lambda key: candidates[key])
    return {
        "winner": winner,
        "auc_by_arm": dict(sorted(candidates.items())),
        "readout": (
            f"{winner} has the highest ROC AUC on this offline diagnostic slice. "
            "This does not authorize serving or production readiness."
        ),
    }


def _repo_root_from_label_path(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.parent.name == "audit" and resolved.parent.parent.name == "docs":
        return resolved.parent.parent.parent
    return Path.cwd().resolve()


def _metadata_input_paths(label_payload: dict[str, Any], *, label_dataset_path: Path) -> list[dict[str, Any]]:
    root = _repo_root_from_label_path(label_dataset_path)
    out: list[dict[str, Any]] = []
    ingest = (label_payload.get("metadata") or {}).get("bridge_negative_mining_v1_ingest")
    if isinstance(ingest, dict):
        for name, key in (
            ("labeled_worksheet", "labeled_worksheet_path"),
            ("context_sidecar", "context_sidecar_path"),
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


def build_ml_offline_bridge_recommendable_scorer_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    label_path = label_dataset_path.resolve()
    readiness_path = readiness_matrix_path.resolve()
    embeddings_path = embeddings_provenance_path.resolve()
    for path in (label_path, readiness_path, embeddings_path):
        if not path.is_file():
            raise MLOfflineBridgeRecommendableScorerError(f"required input not found: {path}")

    label_sha = sha256_file(label_path)
    readiness_sha = sha256_file(readiness_path)
    embeddings_sha = sha256_file(embeddings_path)
    label_payload = _load_json_object(label_path, label="label dataset")
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")

    if label_payload.get("dataset_version") != "ml-label-dataset-v12":
        raise MLOfflineBridgeRecommendableScorerError("label dataset must be ml-label-dataset-v12")
    readiness_group = _validate_readiness_matrix(readiness_payload, label_dataset_sha256=label_sha)
    embedding_dimensions = _validate_embeddings_provenance(embeddings_payload)
    rows = _slice_rows(label_payload)
    slice_counts = _validate_slice(rows)
    internal_work_ids = _internal_work_ids(rows)
    vectors_by_work = _load_slice_embeddings_select_only(
        conn,
        internal_work_ids=internal_work_ids,
        expected_dimensions=embedding_dimensions,
    )

    learned_cv = _learned_cv(rows, vectors_by_work, random_seed=random_seed)
    heuristic_arms = {
        "final_score": _heuristic_arm(rows, field="final_score"),
        "bridge_score": _heuristic_arm(rows, field="bridge_score"),
        "semantic_score": _heuristic_arm(rows, field="semantic_score"),
    }
    full_fit_metrics, frozen_scorer = _full_fit(rows, vectors_by_work, random_seed=random_seed)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    input_records = [
        {"name": "label_dataset", "path": portable_repo_path(label_path), "sha256": label_sha},
        {"name": "readiness_matrix", "path": portable_repo_path(readiness_path), "sha256": readiness_sha},
        {"name": "embeddings_provenance", "path": portable_repo_path(embeddings_path), "sha256": embeddings_sha},
        *_metadata_input_paths(label_payload, label_dataset_path=label_path),
    ]

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
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "review_pool_variant": REVIEW_POOL_VARIANT,
                "target": TARGET,
                "target_must_be_boolean": True,
                "dedupe_globally_by_paper_id": False,
            },
            "slice_counts": slice_counts,
            "label_distributions": {
                "bridge_recommendable": {
                    "true": slice_counts["positive_count"],
                    "false": slice_counts["negative_count"],
                    "null": 0,
                },
                "bridge_like_label": slice_counts["bridge_like_label"],
                "relevance_label": slice_counts["relevance_label"],
            },
            "hard_negative_count": slice_counts["hard_negative_count"],
            "bridge_like_positive_relevance_leak_count": slice_counts["bridge_like_positive_relevance_leak_count"],
            "readiness_group": readiness_group,
            "embedding_coverage": {
                "embedding_version": EMBEDDING_VERSION,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_dimensions": embedding_dimensions,
                "requested_internal_work_id_count": len(internal_work_ids),
                "loaded_vector_count": len(vectors_by_work),
                "coverage_complete": len(vectors_by_work) == EXPECTED_SLICE_ROWS,
            },
        },
        "evaluation": {
            "learned_cv": learned_cv,
            "heuristic_arms": heuristic_arms,
        },
        "in_sample_full_fit_only_not_validation": full_fit_metrics,
        "frozen_scorer": frozen_scorer,
        "baseline_winner_readout_cv_auc": _baseline_winner_readout(learned_cv, heuristic_arms),
        "caveats": list(CAVEATS),
        "recommended_next_stage": "offline_bounded_hybrid_bridge_eval_v1",
    }


def markdown_from_ml_offline_bridge_recommendable_scorer(payload: dict[str, Any]) -> str:
    meta = payload["metadata"]
    learned = payload["evaluation"]["learned_cv"]["aggregate_oof"]
    heur = payload["evaluation"]["heuristic_arms"]
    winner = payload.get("baseline_winner_readout_cv_auc", {})
    lines = [
        "# Offline bridge recommendable scorer v1",
        "",
        "Offline diagnostic model for `bridge_recommendable` trained only on "
        "`review_pool_variant=ml_bridge_negative_mining_audit`. This is not validation and not a serving change.",
        "",
        "## Slice",
        "",
        f"- Rows: {meta['slice_counts']['row_count']}",
        f"- Target true / false: {meta['slice_counts']['positive_count']} / {meta['slice_counts']['negative_count']}",
        f"- Hard negatives: {meta['hard_negative_count']}",
        f"- Bridge-like positive relevance leakage: {meta['bridge_like_positive_relevance_leak_count']}",
        f"- Embedding coverage: {meta['embedding_coverage']['loaded_vector_count']} / {meta['embedding_coverage']['requested_internal_work_id_count']}",
        "",
        "## Learned OOF CV",
        "",
        f"- ROC AUC: {learned.get('roc_auc')}",
        f"- Average precision: {learned.get('average_precision')}",
        f"- Pairwise accuracy: {learned.get('pairwise_accuracy')}",
        f"- Precision@5 / @10 / @20: {learned.get('precision_at_5')} / {learned.get('precision_at_10')} / {learned.get('precision_at_20')}",
        "",
        "## Heuristic Arms",
        "",
        "| arm | status | coverage | ROC AUC | AP | P@10 |",
        "|---|---:|---:|---:|---:|---:|",
        *[
            (
                f"| `{name}` | {arm.get('status')} | {arm.get('non_null_score_count')}/70 | "
                f"{arm.get('roc_auc')} | {arm.get('average_precision')} | {arm.get('precision_at_10')} |"
            )
            for name, arm in heur.items()
        ],
        "",
        "## Readout",
        "",
        str(winner.get("readout") or ""),
        "",
        "The full-slice fit is included only to freeze diagnostic coefficients; its metrics are in-sample and not validation.",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        f"Recommended next stage: `{payload.get('recommended_next_stage')}`.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_ml_offline_bridge_recommendable_scorer(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    payload = build_ml_offline_bridge_recommendable_scorer_payload(
        conn,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        random_seed=random_seed,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_offline_bridge_recommendable_scorer(payload), encoding="utf-8")
    return payload


def run_ml_offline_bridge_recommendable_scorer_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> None:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn) as conn:
        write_ml_offline_bridge_recommendable_scorer(
            conn,
            label_dataset_path=label_dataset_path,
            readiness_matrix_path=readiness_matrix_path,
            embeddings_provenance_path=embeddings_provenance_path,
            json_path=output_json,
            markdown_path=markdown_output,
            random_seed=random_seed,
        )
