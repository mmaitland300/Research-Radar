"""Offline bounded hybrid evaluation for the bridge_recommendable scorer.

This module is file-only diagnostic plumbing. It does not query or write the
database, update ranking tables, or authorize serving behavior.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from sklearn.metrics import average_precision_score

from pipeline.ml_hybrid_scorer_offline_experiment import _rank_percentiles
from pipeline.ml_offline_baseline_eval import pairwise_accuracy, precision_at_k, roc_auc_mann_whitney, sha256_file
from pipeline.ml_offline_bridge_recommendable_scorer import (
    ARTIFACT_TYPE as BRIDGE_SCORER_ARTIFACT_TYPE,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    EXPECTED_SLICE_ROWS,
    FAMILY,
    RANKING_RUN_ID,
    REVIEW_POOL_VARIANT,
    SCORER_VERSION as BRIDGE_SCORER_VERSION,
    TARGET,
    MLOfflineBridgeRecommendableScorerError,
    _slice_rows as _bridge_slice_rows,
    _validate_embeddings_provenance as _validate_bridge_embeddings_provenance,
    _validate_readiness_matrix as _validate_bridge_readiness_matrix,
    _validate_slice as _validate_bridge_slice,
)
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_offline_bounded_hybrid_bridge_eval"
EVAL_VERSION = "ml-offline-bounded-hybrid-bridge-eval-v1"
RANK_PERCENTILE_SCOPE = "labeled_slice_only"
PRIMARY_CONFIRMATORY_ARM = "hybrid_rank_mean_50_50"

ARM_FORMULAS: dict[str, str] = {
    "heuristic_final_score": "rank_pct(final_score)",
    "learned_bridge_probability_oof": "rank_pct(oof_probability)",
    "hybrid_rank_mean_50_50": "0.5 * rank_pct(final_score) + 0.5 * rank_pct(oof_probability)",
    "hybrid_rank_mean_70_30_heuristic": "0.7 * rank_pct(final_score) + 0.3 * rank_pct(oof_probability)",
    "hybrid_rank_mean_30_70_heuristic": "0.3 * rank_pct(final_score) + 0.7 * rank_pct(oof_probability)",
}

METRIC_FIELDS = (
    "roc_auc",
    "average_precision",
    "pairwise_accuracy",
    "precision_at_5",
    "precision_at_10",
    "precision_at_20",
    "top_20_positive_count",
)

CAVEATS = (
    "This is not validation.",
    "This is a single-reviewer, worksheet-selected 70-row slice.",
    "Rank percentiles are labeled_slice_only and are not full-pool production scores.",
    "Labeled-row metrics use bridge scorer OOF probabilities only.",
    "Best-arm readout is exploratory only.",
    "No DB writes, ranking writes, serving changes, or production authorization are made.",
)


class MLOfflineBoundedHybridBridgeEvalError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineBoundedHybridBridgeEvalError(f"failed to load {label} JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineBoundedHybridBridgeEvalError(f"{label} JSON must be an object: {path}")
    return payload


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _wrap_bridge_validation(fn: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return fn(*args, **kwargs)
    except MLOfflineBridgeRecommendableScorerError as exc:
        raise MLOfflineBoundedHybridBridgeEvalError(str(exc), code=exc.code) from exc


def _validate_label_dataset(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != "ml-label-dataset-v12":
        raise MLOfflineBoundedHybridBridgeEvalError("label dataset must be ml-label-dataset-v12")
    rows = _wrap_bridge_validation(_bridge_slice_rows, payload)
    _wrap_bridge_validation(_validate_bridge_slice, rows)
    return rows


def _validate_readiness_matrix(readiness_payload: dict[str, Any], *, label_dataset_sha256: str) -> dict[str, Any]:
    return _wrap_bridge_validation(
        _validate_bridge_readiness_matrix,
        readiness_payload,
        label_dataset_sha256=label_dataset_sha256,
    )


def _validate_embeddings_provenance(embeddings_payload: dict[str, Any]) -> int:
    return _wrap_bridge_validation(_validate_bridge_embeddings_provenance, embeddings_payload)


def _validate_bridge_scorer_payload(
    scorer_payload: dict[str, Any],
    *,
    slice_rows: Sequence[Mapping[str, Any]],
) -> dict[str, float]:
    if scorer_payload.get("artifact_type") != BRIDGE_SCORER_ARTIFACT_TYPE:
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer artifact_type mismatch")
    if scorer_payload.get("scorer_version") != BRIDGE_SCORER_VERSION:
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer scorer_version mismatch")

    metadata = scorer_payload.get("metadata")
    if not isinstance(metadata, dict):
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer missing metadata object")
    target = scorer_payload.get("target") or metadata.get("target") or (scorer_payload.get("frozen_scorer") or {}).get("target")
    if target != TARGET:
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer target must be bridge_recommendable")

    learned_cv = (scorer_payload.get("evaluation") or {}).get("learned_cv")
    if not isinstance(learned_cv, dict):
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer missing evaluation.learned_cv")
    aggregate_oof = learned_cv.get("aggregate_oof")
    if not isinstance(aggregate_oof, dict):
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer missing evaluation.learned_cv.aggregate_oof")
    oof_predictions = learned_cv.get("oof_predictions")
    if not isinstance(oof_predictions, list):
        raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer missing learned_cv.oof_predictions")
    if len(oof_predictions) != EXPECTED_SLICE_ROWS:
        raise MLOfflineBoundedHybridBridgeEvalError(
            f"bridge scorer OOF prediction count={len(oof_predictions)}; expected {EXPECTED_SLICE_ROWS}"
        )

    slice_row_ids = {str(row.get("row_id") or "") for row in slice_rows}
    if "" in slice_row_ids:
        raise MLOfflineBoundedHybridBridgeEvalError("labeled slice contains blank row_id")

    oof_by_row: dict[str, float] = {}
    duplicate_oof_row_ids: list[str] = []
    for pred in oof_predictions:
        if not isinstance(pred, dict):
            raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer OOF predictions must be objects")
        row_id = str(pred.get("row_id") or "")
        if not row_id:
            raise MLOfflineBoundedHybridBridgeEvalError("bridge scorer OOF prediction missing row_id")
        if row_id in oof_by_row:
            duplicate_oof_row_ids.append(row_id)
        prob = _as_float(pred.get("probability"))
        if prob is None or prob < 0.0 or prob > 1.0:
            raise MLOfflineBoundedHybridBridgeEvalError(f"invalid OOF probability for row_id={row_id}")
        oof_by_row[row_id] = prob
    if duplicate_oof_row_ids:
        raise MLOfflineBoundedHybridBridgeEvalError(
            f"duplicate OOF prediction row_ids: {sorted(duplicate_oof_row_ids)[:10]}"
        )
    if set(oof_by_row) != slice_row_ids:
        missing = sorted(slice_row_ids - set(oof_by_row))
        extra = sorted(set(oof_by_row) - slice_row_ids)
        raise MLOfflineBoundedHybridBridgeEvalError(
            f"OOF row_id set does not match labeled slice; missing={missing[:10]}, extra={extra[:10]}"
        )
    return oof_by_row


def _final_score_for_row(row: Mapping[str, Any]) -> tuple[float, str]:
    score = _as_float(row.get("final_score"))
    if score is not None:
        return score, "row.final_score"
    context = row.get("bridge_negative_mining_context")
    if isinstance(context, Mapping):
        score = _as_float(context.get("final_score"))
        if score is not None:
            return score, "bridge_negative_mining_context.final_score"
    row_id = str(row.get("row_id") or "")
    raise MLOfflineBoundedHybridBridgeEvalError(f"labeled row {row_id} is missing final_score")


def _context_value(row: Mapping[str, Any], key: str) -> Any:
    context = row.get("bridge_negative_mining_context")
    if isinstance(context, Mapping):
        return context.get(key)
    return None


def _rank_percentile_scores(rows: Sequence[Mapping[str, Any]], score_field: str) -> dict[str, float]:
    records: list[dict[str, Any]] = []
    for row in rows:
        work_id = str(row.get("work_id") or "").strip()
        if not work_id:
            raise MLOfflineBoundedHybridBridgeEvalError("rank percentile row missing work_id")
        score = _as_float(row.get(score_field))
        if score is None:
            raise MLOfflineBoundedHybridBridgeEvalError(f"rank percentile row {work_id} missing numeric {score_field}")
        records.append({"canonical_openalex_work_id": work_id, score_field: score})
    return _rank_percentiles(records, score_field)


def _score_labeled_rows(
    slice_rows: Sequence[Mapping[str, Any]],
    *,
    oof_by_row_id: Mapping[str, float],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    row_ids = [str(row.get("row_id") or "") for row in slice_rows]
    work_ids = [str(row.get("work_id") or "") for row in slice_rows]
    duplicate_row_ids = sorted(k for k, v in Counter(row_ids).items() if k and v > 1)
    duplicate_work_ids = sorted(k for k, v in Counter(work_ids).items() if k and v > 1)
    if duplicate_row_ids:
        raise MLOfflineBoundedHybridBridgeEvalError(f"duplicate row_id values in labeled slice: {duplicate_row_ids[:10]}")
    if duplicate_work_ids:
        raise MLOfflineBoundedHybridBridgeEvalError(f"duplicate work_id values in labeled slice: {duplicate_work_ids[:10]}")

    score_source_counts: Counter[str] = Counter()
    scored: list[dict[str, Any]] = []
    for row in slice_rows:
        row_id = str(row.get("row_id") or "")
        final_score, source = _final_score_for_row(row)
        score_source_counts[source] += 1
        if row_id not in oof_by_row_id:
            raise MLOfflineBoundedHybridBridgeEvalError(f"missing OOF probability for row_id={row_id}")
        scored.append(
            {
                "row_id": row_id,
                "work_id": str(row.get("work_id") or ""),
                "paper_id": row.get("paper_id"),
                "title": row.get("title"),
                "label": bool(row[TARGET]),
                TARGET: bool(row[TARGET]),
                "final_score": final_score,
                "final_score_source": source,
                "oof_probability": float(oof_by_row_id[row_id]),
                "bridge_like_label": row.get("bridge_like_label"),
                "relevance_label": row.get("relevance_label"),
                "family_rank": _context_value(row, "family_rank"),
                "reason_short": _context_value(row, "reason_short"),
                "sample_reason": _context_value(row, "sample_reason"),
            }
        )

    final_rank_pct = _rank_percentile_scores(scored, "final_score")
    learned_rank_pct = _rank_percentile_scores(scored, "oof_probability")
    for row in scored:
        work_id = str(row["work_id"])
        final_rank = float(final_rank_pct[work_id])
        learned_rank = float(learned_rank_pct[work_id])
        row["final_score_rank_pct"] = final_rank
        row["learned_bridge_probability_oof_rank_pct"] = learned_rank
        row["arm_scores"] = {
            "heuristic_final_score": final_rank,
            "learned_bridge_probability_oof": learned_rank,
            "hybrid_rank_mean_50_50": 0.5 * final_rank + 0.5 * learned_rank,
            "hybrid_rank_mean_70_30_heuristic": 0.7 * final_rank + 0.3 * learned_rank,
            "hybrid_rank_mean_30_70_heuristic": 0.3 * final_rank + 0.7 * learned_rank,
        }

    validation = {
        "row_count": len(scored),
        "duplicate_row_id_count": len(duplicate_row_ids),
        "duplicate_work_id_count": len(duplicate_work_ids),
        "final_score_missing_count": 0,
        "final_score_source_counts": dict(sorted(score_source_counts.items())),
        "oof_probability_count": len(oof_by_row_id),
    }
    return scored, validation


def _arm_metrics(scored_rows: Sequence[Mapping[str, Any]], *, arm_name: str) -> dict[str, Any]:
    pairs: list[tuple[float, bool]] = []
    sortable: list[tuple[float, str, bool]] = []
    for row in scored_rows:
        arm_scores = row.get("arm_scores")
        if not isinstance(arm_scores, Mapping) or arm_name not in arm_scores:
            raise MLOfflineBoundedHybridBridgeEvalError(f"row missing arm score {arm_name}")
        score = _as_float(arm_scores.get(arm_name))
        if score is None:
            raise MLOfflineBoundedHybridBridgeEvalError(f"row has non-numeric arm score {arm_name}")
        label = bool(row["label"])
        work_id = str(row.get("work_id") or "")
        pairs.append((score, label))
        sortable.append((score, work_id, label))

    positive_count = sum(1 for _, label in pairs if label)
    negative_count = len(pairs) - positive_count
    if positive_count == 0 or negative_count == 0:
        return {
            "status": "not_applicable",
            "reason": "scores do not cover both target classes",
            "row_count": len(pairs),
            "positive_count": positive_count,
            "negative_count": negative_count,
            "roc_auc": None,
            "average_precision": None,
            "pairwise_accuracy": None,
            "precision_at_5": None,
            "precision_at_10": None,
            "precision_at_20": None,
            "top_20_positive_count": None,
        }

    desc = sorted(sortable, key=lambda item: (-item[0], item[1]))
    desc_pairs = [(score, label) for score, _, label in desc]
    labels = [label for _, label in pairs]
    scores = [score for score, _ in pairs]
    return {
        "status": "ok",
        "formula": ARM_FORMULAS[arm_name],
        "score_kind": "labeled_slice_rank_percentile" if arm_name != "learned_bridge_probability_oof" else "oof_probability_rank_percentile",
        "row_count": len(pairs),
        "positive_count": positive_count,
        "negative_count": negative_count,
        "roc_auc": roc_auc_mann_whitney(pairs),
        "average_precision": float(average_precision_score(labels, scores)),
        "pairwise_accuracy": pairwise_accuracy(pairs),
        "precision_at_5": precision_at_k(desc_pairs, 5),
        "precision_at_10": precision_at_k(desc_pairs, 10),
        "precision_at_20": precision_at_k(desc_pairs, 20),
        "top_20_positive_count": int(sum(1 for _, label in desc_pairs[:20] if label)),
    }


def _metric_deltas(arm_metrics: Mapping[str, Mapping[str, Any]], *, baseline_arm: str) -> dict[str, dict[str, float | None]]:
    baseline = arm_metrics[baseline_arm]
    out: dict[str, dict[str, float | None]] = {}
    for arm_name, metrics in arm_metrics.items():
        deltas: dict[str, float | None] = {}
        for key in METRIC_FIELDS:
            value = metrics.get(key)
            base = baseline.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and isinstance(base, (int, float)) and not isinstance(base, bool):
                deltas[key] = float(value) - float(base)
            else:
                deltas[key] = None
        out[arm_name] = deltas
    return out


def _best_arm_by_metric(arm_metrics: Mapping[str, Mapping[str, Any]], metric: str) -> dict[str, Any]:
    best_name: str | None = None
    best_value: float | None = None
    for arm_name in ARM_FORMULAS:
        value = arm_metrics.get(arm_name, {}).get(metric)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        if best_value is None or float(value) > best_value:
            best_name = arm_name
            best_value = float(value)
    return {
        "arm": best_name,
        "metric": metric,
        "value": best_value,
        "exploratory_only": True,
        "not_production_selection": True,
    }


def _recommended_next_stage(arm_metrics: Mapping[str, Mapping[str, Any]]) -> str:
    primary = arm_metrics[PRIMARY_CONFIRMATORY_ARM]
    heuristic = arm_metrics["heuristic_final_score"]
    learned = arm_metrics["learned_bridge_probability_oof"]
    primary_auc = primary.get("roc_auc")
    primary_ap = primary.get("average_precision")
    heuristic_auc = heuristic.get("roc_auc")
    heuristic_ap = heuristic.get("average_precision")
    learned_auc = learned.get("roc_auc")
    learned_ap = learned.get("average_precision")
    values = (primary_auc, primary_ap, heuristic_auc, heuristic_ap, learned_auc, learned_ap)
    if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
        return "create_bridge_active_learning_worksheet_v1"
    if (
        float(primary_auc) > float(heuristic_auc)
        and float(primary_auc) > float(learned_auc)
        and float(primary_ap) > float(heuristic_ap)
        and float(primary_ap) > float(learned_ap)
    ):
        return "bridge_shadow_offline_pilot_plan_v1"
    if float(primary_auc) < float(learned_auc) and float(primary_ap) < float(learned_ap):
        return "do_not_combine_signals_collect_labels_or_fix_features"
    return "create_bridge_active_learning_worksheet_v1"


def _preview_row(row: Mapping[str, Any]) -> dict[str, Any]:
    final_rank = float(row["final_score_rank_pct"])
    learned_rank = float(row["learned_bridge_probability_oof_rank_pct"])
    return {
        "row_id": row.get("row_id"),
        "work_id": row.get("work_id"),
        "paper_id": row.get("paper_id"),
        "title": row.get("title"),
        "bridge_recommendable": row.get(TARGET),
        "bridge_like_label": row.get("bridge_like_label"),
        "relevance_label": row.get("relevance_label"),
        "final_score": row.get("final_score"),
        "oof_probability": row.get("oof_probability"),
        "final_score_rank_pct": final_rank,
        "learned_bridge_probability_oof_rank_pct": learned_rank,
        "ml_minus_heuristic_rank_pct": learned_rank - final_rank,
        "hybrid_rank_mean_50_50": (row.get("arm_scores") or {}).get(PRIMARY_CONFIRMATORY_ARM),
        "family_rank": row.get("family_rank"),
        "reason_short": row.get("reason_short"),
        "sample_reason": row.get("sample_reason"),
    }


def _disagreement_analysis(scored_rows: Sequence[Mapping[str, Any]], *, limit: int = 10) -> dict[str, Any]:
    high_ml = sorted(
        scored_rows,
        key=lambda row: (
            -(float(row["learned_bridge_probability_oof_rank_pct"]) - float(row["final_score_rank_pct"])),
            str(row.get("work_id") or ""),
        ),
    )
    high_heuristic = sorted(
        scored_rows,
        key=lambda row: (
            -(float(row["final_score_rank_pct"]) - float(row["learned_bridge_probability_oof_rank_pct"])),
            str(row.get("work_id") or ""),
        ),
    )
    uncertain = sorted(
        scored_rows,
        key=lambda row: (abs(float(row["oof_probability"]) - 0.5), str(row.get("work_id") or "")),
    )
    return {
        "bucket_definitions": {
            "high_ml_low_heuristic": "largest positive learned_rank_pct - final_score_rank_pct within the labeled slice",
            "high_heuristic_low_ml": "largest positive final_score_rank_pct - learned_rank_pct within the labeled slice",
            "uncertain_ml": "OOF probabilities closest to 0.5",
        },
        "high_ml_low_heuristic": [_preview_row(row) for row in high_ml[:limit]],
        "high_heuristic_low_ml": [_preview_row(row) for row in high_heuristic[:limit]],
        "uncertain_ml": [_preview_row(row) for row in uncertain[:limit]],
    }


def _slice_count_summary(scored_rows: Sequence[Mapping[str, Any]], *, base_counts: Mapping[str, Any]) -> dict[str, Any]:
    labels = Counter("true" if row.get(TARGET) is True else "false" for row in scored_rows)
    return {
        "row_count": len(scored_rows),
        "positive_count": labels["true"],
        "negative_count": labels["false"],
        "duplicate_work_id_count": base_counts.get("duplicate_work_id_count"),
        "derived_target_conflict_count": base_counts.get("derived_target_conflict_count"),
        "bridge_like_label": base_counts.get("bridge_like_label"),
        "hard_negative_count": base_counts.get("hard_negative_count"),
        "bridge_like_positive_relevance_leak_count": base_counts.get("bridge_like_positive_relevance_leak_count"),
    }


def _input_record(name: str, path: Path) -> dict[str, str]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise MLOfflineBoundedHybridBridgeEvalError(f"required input not found: {resolved}")
    return {"name": name, "path": portable_repo_path(resolved), "sha256": sha256_file(resolved)}


def build_ml_offline_bounded_hybrid_bridge_eval_payload(
    *,
    label_dataset_path: Path,
    bridge_scorer_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
) -> dict[str, Any]:
    label_path = label_dataset_path.resolve()
    scorer_path = bridge_scorer_path.resolve()
    readiness_path = readiness_matrix_path.resolve()
    embeddings_path = embeddings_provenance_path.resolve()
    inputs = [
        _input_record("label_dataset", label_path),
        _input_record("bridge_scorer", scorer_path),
        _input_record("readiness_matrix", readiness_path),
        _input_record("embeddings_provenance", embeddings_path),
    ]
    input_sha256s = {record["name"]: record["sha256"] for record in inputs}

    label_payload = _load_json_object(label_path, label="label dataset")
    scorer_payload = _load_json_object(scorer_path, label="bridge scorer")
    readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
    embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")

    slice_rows = _validate_label_dataset(label_payload)
    readiness_group = _validate_readiness_matrix(readiness_payload, label_dataset_sha256=input_sha256s["label_dataset"])
    embedding_dimensions = _validate_embeddings_provenance(embeddings_payload)
    oof_by_row_id = _validate_bridge_scorer_payload(scorer_payload, slice_rows=slice_rows)
    base_slice_counts = _wrap_bridge_validation(_validate_bridge_slice, slice_rows)
    scored_rows, score_validation = _score_labeled_rows(slice_rows, oof_by_row_id=oof_by_row_id)

    arm_metrics = {arm_name: _arm_metrics(scored_rows, arm_name=arm_name) for arm_name in ARM_FORMULAS}
    deltas_vs_heuristic = _metric_deltas(arm_metrics, baseline_arm="heuristic_final_score")
    deltas_vs_learned = _metric_deltas(arm_metrics, baseline_arm="learned_bridge_probability_oof")
    for arm_name, metrics in arm_metrics.items():
        metrics["delta_vs_heuristic_final_score"] = deltas_vs_heuristic[arm_name]
        metrics["delta_vs_learned_bridge_probability_oof"] = deltas_vs_learned[arm_name]

    payload = {
        "artifact_type": ARTIFACT_TYPE,
        "eval_version": EVAL_VERSION,
        "generated_at": _now_iso_z(),
        "inputs": inputs,
        "input_sha256s": input_sha256s,
        "rank_percentile_scope": RANK_PERCENTILE_SCOPE,
        "rank_percentile_policy": {
            "higher_score_is_better": True,
            "tie_policy": "average_rank",
            "tie_breaker_for_display_only": "work_id ascending",
            "n_equals_1_behavior": "rank_pct = 1.0",
            "otherwise": "rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0))",
            "scope": RANK_PERCENTILE_SCOPE,
        },
        "training_eval_slice_filter": {
            "split": "audit_only",
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "target": TARGET,
            "target_must_be_boolean": True,
        },
        "slice_counts": _slice_count_summary(scored_rows, base_counts=base_slice_counts),
        "validation_summary": {
            "readiness_group_validated": True,
            "bridge_scorer_oof_validated": True,
            "embeddings_provenance_validated": True,
            "embedding_version": EMBEDDING_VERSION,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_dimensions": embedding_dimensions,
            "readiness_group": readiness_group,
            "score_validation": score_validation,
        },
        "arm_formulas": ARM_FORMULAS,
        "arm_metrics": arm_metrics,
        "primary_confirmatory_arm": PRIMARY_CONFIRMATORY_ARM,
        "best_arm_by_roc_auc": _best_arm_by_metric(arm_metrics, "roc_auc"),
        "best_arm_by_average_precision": _best_arm_by_metric(arm_metrics, "average_precision"),
        "deltas_vs_heuristic_final_score": deltas_vs_heuristic,
        "deltas_vs_learned_bridge_probability_oof": deltas_vs_learned,
        "disagreement_analysis": _disagreement_analysis(scored_rows),
        "labeled_row_scores": scored_rows,
        "recommended_next_stage": _recommended_next_stage(arm_metrics),
        "caveats": list(CAVEATS),
        "no_db_access_required": True,
        "db_writes": False,
        "production_authorization": False,
    }
    return payload


def markdown_from_ml_offline_bounded_hybrid_bridge_eval(payload: Mapping[str, Any]) -> str:
    counts = payload["slice_counts"]
    arm_metrics = payload["arm_metrics"]
    best_auc = payload["best_arm_by_roc_auc"]
    best_ap = payload["best_arm_by_average_precision"]
    lines = [
        "# Offline bounded hybrid bridge eval v1",
        "",
        "Offline diagnostic only: evaluates bounded rank-mean hybrids for `bridge_recommendable` on "
        "`review_pool_variant=ml_bridge_negative_mining_audit`. This is not validation and not a serving change.",
        "",
        f"- Rank percentile scope: `{payload.get('rank_percentile_scope')}`",
        f"- Rows: {counts.get('row_count')}",
        f"- Target true / false: {counts.get('positive_count')} / {counts.get('negative_count')}",
        f"- Primary fixed arm: `{payload.get('primary_confirmatory_arm')}`",
        "",
        "## Arms",
        "",
        "| arm | ROC AUC | AP | Pairwise | P@5 | P@10 | P@20 | top20 positives |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm_name in ARM_FORMULAS:
        metrics = arm_metrics[arm_name]
        lines.append(
            f"| `{arm_name}` | {metrics.get('roc_auc')} | {metrics.get('average_precision')} | "
            f"{metrics.get('pairwise_accuracy')} | {metrics.get('precision_at_5')} | "
            f"{metrics.get('precision_at_10')} | {metrics.get('precision_at_20')} | "
            f"{metrics.get('top_20_positive_count')} |"
        )
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- Best arm by ROC AUC: `{best_auc.get('arm')}` = `{best_auc.get('value')}` (exploratory only).",
            f"- Best arm by average precision: `{best_ap.get('arm')}` = `{best_ap.get('value')}` (exploratory only).",
            f"- Recommended next stage: `{payload.get('recommended_next_stage')}`",
            "",
            "The primary arm is fixed as `hybrid_rank_mean_50_50`; best-arm selection is exploratory only and does not authorize production serving.",
            "",
            "## Caveats",
            "",
            *[f"- {caveat}" for caveat in payload.get("caveats", [])],
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_ml_offline_bounded_hybrid_bridge_eval(
    *,
    label_dataset_path: Path,
    bridge_scorer_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_offline_bounded_hybrid_bridge_eval_payload(
        label_dataset_path=label_dataset_path,
        bridge_scorer_path=bridge_scorer_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_offline_bounded_hybrid_bridge_eval(payload), encoding="utf-8")
    return payload


def run_ml_offline_bounded_hybrid_bridge_eval_cli(
    *,
    label_dataset_path: Path,
    bridge_scorer_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    write_ml_offline_bounded_hybrid_bridge_eval(
        label_dataset_path=label_dataset_path,
        bridge_scorer_path=bridge_scorer_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        json_path=output_json,
        markdown_path=markdown_output,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "EVAL_VERSION",
    "MLOfflineBoundedHybridBridgeEvalError",
    "build_ml_offline_bounded_hybrid_bridge_eval_payload",
    "markdown_from_ml_offline_bounded_hybrid_bridge_eval",
    "run_ml_offline_bounded_hybrid_bridge_eval_cli",
    "write_ml_offline_bounded_hybrid_bridge_eval",
]
