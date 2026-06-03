"""Offline regularization sensitivity for bridge_recommendable scorer v3.

This is a diagnostic-only sweep over the existing v3 bridge_recommendable
training slice. It reads the same inputs as v3, writes sibling artifacts only,
and does not change serving, API behavior, or database state.
"""

from __future__ import annotations

import json
import hashlib
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_recommendable_scorer_v3 import (
    ALLOWED_REVIEW_POOL_VARIANTS,
    CAVEATS as V3_CAVEATS,
    CORPUS_SNAPSHOT_VERSION,
    DEDUPED_ROWS,
    EMBEDDING_VERSION,
    FAMILY,
    FIT_MODE,
    LABEL_DATASET_VERSION,
    MLOfflineBridgeRecommendableScorerV3Error,
    OVERFIT_AUC_GAP_THRESHOLD,
    READINESS_MATRIX_VERSION,
    SCORER_VERSION as V3_SCORER_VERSION,
    TARGET,
    V2_AGGREGATE_ROC_AUC_REFERENCE,
    V2_OVERLAP_WORK_IDS,
    V2_SCORER_VERSION,
    _confusion_dict,
    _dedupe_by_work_id,
    _disagreement_bucket,
    _internal_work_ids,
    _load_json_object,
    _load_slice_embeddings_select_only,
    _metadata_input_paths,
    _overlap_count_from_label_metadata,
    _ranking_metrics,
    _repo_root_from_label_path,
    _slice_row_level_rows,
    _targeted_decision_readouts,
    _validate_deduped_counts,
    _validate_embeddings_provenance,
    _validate_readiness_matrix,
    _validate_row_level_counts,
)
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity"
ARTIFACT_VERSION = "ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1"
SENSITIVITY_FIT_MODE = f"{FIT_MODE}_regularization_sensitivity"
DEFAULT_RANDOM_SEED = 20260602
SWEEP_C_VALUES = (1.0, 0.1, 0.01, 0.001, 0.0001)
MIN_ACCEPTABLE_OOF_AUC = 0.70
MAX_REGRESSION_VS_C_0_1_AUC = 0.01
TOO_STRONG_MIN_OOF_AUC = 0.66
TOO_STRONG_MAX_AUC_DROP_VS_C_0_1 = 0.03


class MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _as_float_metric(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    out = float(value)
    return out if math.isfinite(out) else None


def _model_for_c(*, c_value: float, random_seed: int) -> Any:
    return make_pipeline(
        StandardScaler(),
        LogisticRegression(
            C=float(c_value),
            max_iter=1000,
            solver="liblinear",
            random_state=random_seed,
        ),
    )


def _validate_cv_labels(y_int: Sequence[int]) -> None:
    positives = sum(1 for y in y_int if y == 1)
    negatives = len(y_int) - positives
    if positives < 5 or negatives < 5:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            "StratifiedKFold(5) requires at least 5 positive and 5 negative labels"
        )


def _learned_cv_for_c(
    rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    c_value: float,
    random_seed: int,
) -> tuple[dict[str, Any], list[float]]:
    work_ids = _internal_work_ids(rows, expected_count=len(rows))
    x = [vectors_by_work[work_id] for work_id in work_ids]
    y_bool = [bool(row[TARGET]) for row in rows]
    y_int = [1 if y else 0 for y in y_bool]
    _validate_cv_labels(y_int)

    splitter = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_seed)
    oof_prob = [float("nan")] * len(rows)
    per_fold: list[dict[str, Any]] = []

    for fold_id, (train_idx_arr, test_idx_arr) in enumerate(splitter.split(x, y_int)):
        train_idx = [int(i) for i in train_idx_arr]
        test_idx = [int(i) for i in test_idx_arr]
        model = _model_for_c(c_value=c_value, random_seed=random_seed)
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
                "C": float(c_value),
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
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            "OOF probabilities did not cover all deduped rows"
        )

    aggregate_ranking = _ranking_metrics(oof_prob, y_bool)
    aggregate_preds = [p >= 0.5 for p in oof_prob]
    result = {
        "cv": {
            "strategy": "StratifiedKFold(n_splits=5, shuffle=True)",
            "random_seed": random_seed,
            "estimator": (
                "StandardScaler + LogisticRegression(max_iter=1000, "
                f"solver='liblinear', C={float(c_value)})"
            ),
            "C": float(c_value),
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


def _full_fit_for_c(
    rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    c_value: float,
    random_seed: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    work_ids = _internal_work_ids(rows, expected_count=len(rows))
    x = [vectors_by_work[work_id] for work_id in work_ids]
    y_bool = [bool(row[TARGET]) for row in rows]
    y_int = [1 if y else 0 for y in y_bool]
    model = _model_for_c(c_value=c_value, random_seed=random_seed)
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
        "in_sample_full_fit_only_not_validation": True,
    }
    frozen = {
        "scorer_version": ARTIFACT_VERSION,
        "source_diagnostic_scorer_version": V3_SCORER_VERSION,
        "fit_mode": SENSITIVITY_FIT_MODE,
        "target": TARGET,
        "feature_source": "embeddings.vector",
        "embedding_version": EMBEDDING_VERSION,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_dimensions": len(x[0]) if x else 0,
        "random_seed": random_seed,
        "estimator": (
            "StandardScaler + LogisticRegression(max_iter=1000, "
            f"solver='liblinear', C={float(c_value)})"
        ),
        "C": float(c_value),
        "regularization": "l2_inverse_strength_C",
        "in_sample_full_fit_only_not_validation": True,
        "scaler_mean": [float(v) for v in scaler.mean_.tolist()],
        "scaler_scale": [float(v) for v in scaler.scale_.tolist()],
        "coef": [float(v) for v in lr.coef_[0].tolist()],
        "intercept": float(lr.intercept_[0]),
        "classes": [int(v) for v in lr.classes_.tolist()],
    }
    return metrics, frozen


def _targeted_readouts_with_verdicts(
    deduped_rows: list[dict[str, Any]],
    deduped_oof: list[float],
) -> dict[str, Any]:
    targeted = _targeted_decision_readouts(deduped_rows, deduped_oof)

    promoted = dict(targeted.get("promoted_by_hybrid") or {})
    p10 = _as_float_metric(promoted.get("precision_at_10_by_oof_rank_within_promoted"))
    above_median = promoted.get("count_oof_gte_shadow_negative_median")
    expected_above = promoted.get("expected_above_median_count", 14)
    promoted["verdict"] = (
        "supports_hybrid_promotion"
        if p10 is not None
        and p10 >= 0.7
        and isinstance(above_median, int)
        and isinstance(expected_above, int)
        and above_median >= expected_above
        else "partial"
        if (p10 is not None and p10 >= 0.5) or (isinstance(above_median, int) and above_median > 0)
        else "fails"
    )

    demoted = dict(targeted.get("demoted_by_hybrid") or {})
    competitive = dict(demoted.get("competitive_demotion_subgroup") or {})
    correct_rejection = dict(demoted.get("correct_rejection_subgroup") or {})
    competitive_mean = _as_float_metric(competitive.get("mean_oof"))
    correct_rejection_mean = _as_float_metric(correct_rejection.get("mean_oof"))
    competitive_count = competitive.get("count")
    correct_rejection_count = correct_rejection.get("count")
    demoted["verdict"] = (
        "separates_competitive_demotions_from_correct_rejections"
        if competitive_mean is not None
        and correct_rejection_mean is not None
        and competitive_mean > correct_rejection_mean
        else "partial"
        if isinstance(competitive_count, int)
        and competitive_count > 0
        and isinstance(correct_rejection_count, int)
        and correct_rejection_count > 0
        else "fails"
    )

    targeted["promoted_by_hybrid"] = promoted
    targeted["demoted_by_hybrid"] = demoted
    return targeted


def _v3_on_v2_work_id_set_metrics(
    deduped_rows: list[dict[str, Any]],
    deduped_oof: list[float],
    *,
    v2_payload: dict[str, Any] | None,
    expected_v2_work_id_count: int | None,
) -> dict[str, Any]:
    if v2_payload is None:
        return {"status": "skipped", "reason": "no v2 baseline artifact provided"}
    v2_preds = v2_payload.get("evaluation", {}).get("learned_cv", {}).get("oof_predictions")
    if not isinstance(v2_preds, list):
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            "v2 baseline missing learned_cv.oof_predictions"
        )
    v2_by_work = {
        str(item.get("work_id") or ""): item
        for item in v2_preds
        if isinstance(item, dict) and item.get("work_id")
    }
    if expected_v2_work_id_count is not None and len(v2_by_work) != expected_v2_work_id_count:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            f"v2 baseline work_id set has {len(v2_by_work)} entries; expected {expected_v2_work_id_count}"
        )
    oof_by_work = {str(row.get("work_id") or ""): deduped_oof[i] for i, row in enumerate(deduped_rows)}
    row_by_work = {str(row.get("work_id") or ""): row for row in deduped_rows}
    scores: list[float] = []
    labels: list[bool] = []
    label_drift: list[dict[str, Any]] = []
    for work_id in sorted(v2_by_work):
        v3_row = row_by_work.get(work_id)
        if v3_row is None:
            raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
                f"v2 overlap work_id {work_id!r} missing from v3 deduped slice"
            )
        prob = oof_by_work.get(work_id)
        if prob is None:
            raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
                f"missing v3 deduped OOF for v2 overlap work_id {work_id!r}"
            )
        label = bool(v3_row[TARGET])
        scores.append(prob)
        labels.append(label)
        if v2_by_work[work_id].get("label") != v3_row[TARGET]:
            label_drift.append(
                {
                    "work_id": work_id,
                    "v2_label": v2_by_work[work_id].get("label"),
                    "v3_deduped_label": v3_row[TARGET],
                    "v3_winning_review_pool_variant": v3_row.get("review_pool_variant"),
                }
            )

    metrics = _ranking_metrics(scores, labels)
    v2_ref_auc = v2_payload.get("evaluation", {}).get("learned_cv", {}).get("aggregate_oof", {}).get("roc_auc")
    if not isinstance(v2_ref_auc, (int, float)) or isinstance(v2_ref_auc, bool):
        v2_ref_auc = V2_AGGREGATE_ROC_AUC_REFERENCE
    return {
        "status": "ok",
        "comparison_scope": "sensitivity_deduped_oof_on_v2_work_id_set",
        "v2_work_id_count": len(v2_by_work),
        "v3_scores_available_count": len(scores),
        "v2_reference_aggregate_roc_auc": float(v2_ref_auc),
        "roc_auc": metrics.get("roc_auc"),
        "metrics": metrics,
        "label_policy_drift_rows": label_drift,
        "label_policy_drift_count": len(label_drift),
    }


def _mark_too_strong_regularization(results: list[dict[str, Any]]) -> None:
    auc_by_c = {float(item["C"]): _as_float_metric(item.get("oof_roc_auc")) for item in results}
    c_0_1_auc = auc_by_c.get(0.1)
    for item in results:
        oof_auc = _as_float_metric(item.get("oof_roc_auc"))
        auc_drop_vs_c_0_1 = (
            float(c_0_1_auc) - float(oof_auc)
            if c_0_1_auc is not None and oof_auc is not None
            else None
        )
        too_strong = (
            oof_auc is not None
            and (
                oof_auc < TOO_STRONG_MIN_OOF_AUC
                or (
                    auc_drop_vs_c_0_1 is not None
                    and auc_drop_vs_c_0_1 > TOO_STRONG_MAX_AUC_DROP_VS_C_0_1
                )
            )
        )
        item["auc_drop_vs_C_0_1"] = auc_drop_vs_c_0_1
        item["too_strong_regularization"] = bool(too_strong)
        item["acceptable_for_offline_hybrid_eval"] = (
            oof_auc is not None
            and oof_auc >= MIN_ACCEPTABLE_OOF_AUC
            and not too_strong
            and (
                c_0_1_auc is None
                or auc_drop_vs_c_0_1 is None
                or auc_drop_vs_c_0_1 <= MAX_REGRESSION_VS_C_0_1_AUC
            )
        )


def _run_sweep(
    deduped_rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    *,
    c_values: Sequence[float],
    random_seed: int,
    v2_payload: dict[str, Any] | None,
    expected_v2_work_id_count: int | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for c_value in c_values:
        learned_cv, deduped_oof = _learned_cv_for_c(
            deduped_rows,
            vectors_by_work,
            c_value=float(c_value),
            random_seed=random_seed,
        )
        full_metrics, frozen_scorer = _full_fit_for_c(
            deduped_rows,
            vectors_by_work,
            c_value=float(c_value),
            random_seed=random_seed,
        )
        oof_auc = _as_float_metric(learned_cv["aggregate_oof"].get("roc_auc"))
        full_auc = _as_float_metric(full_metrics.get("roc_auc"))
        gap = full_auc - oof_auc if full_auc is not None and oof_auc is not None else None
        v2_metrics = _v3_on_v2_work_id_set_metrics(
            deduped_rows,
            deduped_oof,
            v2_payload=v2_payload,
            expected_v2_work_id_count=expected_v2_work_id_count,
        )
        results.append(
            {
                "C": float(c_value),
                "regularization": {
                    "penalty": "l2",
                    "C": float(c_value),
                    "interpretation": "smaller C means stronger regularization",
                },
                "learned_cv_primary_deduped": learned_cv,
                "oof_roc_auc": oof_auc,
                "oof_average_precision": _as_float_metric(learned_cv["aggregate_oof"].get("average_precision")),
                "oof_precision_at_20": _as_float_metric(learned_cv["aggregate_oof"].get("precision_at_20")),
                "in_sample_full_fit_only_not_validation": {
                    "metrics": full_metrics,
                    "frozen_scorer": frozen_scorer,
                },
                "in_sample_roc_auc": full_auc,
                "in_sample_auc_minus_oof_auc": gap,
                "targeted_decision_readouts": _targeted_readouts_with_verdicts(deduped_rows, deduped_oof),
                "v3_on_v2_work_id_set": v2_metrics,
                "v3_on_v2_work_id_set_roc_auc": v2_metrics.get("roc_auc"),
            }
        )
    _mark_too_strong_regularization(results)
    return results


def _selection_from_sweep(results: list[dict[str, Any]]) -> dict[str, Any]:
    order = {float(c): i for i, c in enumerate(SWEEP_C_VALUES)}
    acceptable = [item for item in results if item.get("acceptable_for_offline_hybrid_eval") is True]
    if not acceptable:
        return {
            "ready_for_offline_hybrid_eval": False,
            "selected_frozen_coefficient_C": None,
            "selected_result": None,
            "selection_reason": "no_C_met_oof_auc_trend_gate",
        }
    selected = min(
        acceptable,
        key=lambda item: (
            -(_as_float_metric(item.get("oof_roc_auc")) or float("-inf")),
            order.get(float(item["C"]), 999),
        ),
    )
    return {
        "ready_for_offline_hybrid_eval": True,
        "selected_frozen_coefficient_C": float(selected["C"]),
        "selected_result": selected,
        "selection_reason": "acceptable_C_with_best_oof_auc",
    }


def _load_baseline_reference(path: Path, *, git_ref: str | None = None) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            f"v3 baseline artifact not found: {resolved}"
        )
    portable_path = portable_repo_path(resolved)
    source = "file"
    if git_ref:
        repo_root = _repo_root_from_label_path(resolved)
        rel_path = portable_repo_path(resolved).replace("\\", "/")
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "show", f"{git_ref}:{rel_path}"],
            check=False,
            capture_output=True,
        )
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace").strip()
            raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
                f"failed to load v3 baseline artifact from git ref {git_ref!r}: {stderr}"
            )
        data = proc.stdout
        source = f"git:{git_ref}"
    else:
        data = resolved.read_bytes()

    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            f"v3 baseline artifact is not valid JSON: {resolved}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("scorer_version") != V3_SCORER_VERSION:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            "v3 baseline artifact scorer_version mismatch"
        )
    return {
        "name": "v3_baseline_scorer",
        "path": portable_path,
        "sha256": hashlib.sha256(data).hexdigest(),
        "sha256_source": source,
        "git_ref": git_ref,
        "artifact_type": payload.get("artifact_type"),
        "scorer_version": payload.get("scorer_version"),
        "baseline_C": 1.0,
        "read_only_reference": True,
    }


def build_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_payload_from_slice(
    *,
    deduped_rows: list[dict[str, Any]],
    vectors_by_work: dict[int, list[float]],
    baseline_reference: dict[str, Any],
    input_records: list[dict[str, Any]] | None = None,
    metadata_extra: dict[str, Any] | None = None,
    v2_payload: dict[str, Any] | None = None,
    expected_v2_work_id_count: int | None = None,
    random_seed: int = DEFAULT_RANDOM_SEED,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not deduped_rows:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError("deduped rows are required")
    generated = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    inputs = list(input_records or [])
    if not any(item.get("name") == "v3_baseline_scorer" for item in inputs):
        inputs.append(dict(baseline_reference))

    sweep = _run_sweep(
        deduped_rows,
        vectors_by_work,
        c_values=SWEEP_C_VALUES,
        random_seed=random_seed,
        v2_payload=v2_payload,
        expected_v2_work_id_count=expected_v2_work_id_count,
    )
    selection = _selection_from_sweep(sweep)

    payload: dict[str, Any] = {
        "artifact_type": ARTIFACT_TYPE,
        "artifact_version": ARTIFACT_VERSION,
        "source_diagnostic_scorer_version": V3_SCORER_VERSION,
        "fit_mode": SENSITIVITY_FIT_MODE,
        "generated_at": generated,
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "artifact_version": ARTIFACT_VERSION,
            "source_diagnostic_scorer_version": V3_SCORER_VERSION,
            "target": TARGET,
            "random_seed": random_seed,
            "inputs": inputs,
            "baseline_reference": baseline_reference,
            "sweep_C_values": [float(c) for c in SWEEP_C_VALUES],
            "training": {
                "primary_evaluation_slice": "deduped_130_unique_work_ids",
                "deduped_row_count": len(deduped_rows),
                "cv": "StratifiedKFold(n_splits=5, shuffle=True)",
                "estimator": "StandardScaler + LogisticRegression(max_iter=1000, solver='liblinear')",
                "database_access": "SELECT-only embeddings read; no writes",
            },
            **(metadata_extra or {}),
        },
        "selection_rule": {
            "acceptable_for_offline_hybrid_eval_if": {
                "oof_auc_gte": MIN_ACCEPTABLE_OOF_AUC,
                "too_strong_regularization": False,
                "oof_auc_regression_vs_C_0_1_lte": MAX_REGRESSION_VS_C_0_1_AUC,
                "note": (
                    "The in-sample AUC minus OOF AUC gap remains reported, but is not used "
                    "as the selection gate for this high-dimensional embedding classifier."
                ),
            },
            "too_strong_regularization_if": {
                "oof_auc_lt": TOO_STRONG_MIN_OOF_AUC,
                "auc_drop_vs_C_0_1_gt": TOO_STRONG_MAX_AUC_DROP_VS_C_0_1,
            },
            "selected_C_rule": "acceptable C with the best OOF ROC AUC",
            "selection_reason": selection["selection_reason"],
        },
        "regularization_sweep": sweep,
        "selected_frozen_coefficient_C": selection["selected_frozen_coefficient_C"],
        "ready_for_offline_hybrid_eval": selection["ready_for_offline_hybrid_eval"],
        "caveats": [
            "This is an offline regularization sensitivity diagnostic only.",
            "It does not enable Bridge serving or production output.",
            "It does not mutate the committed v3 baseline artifact.",
            *list(V3_CAVEATS),
        ],
    }
    if selection["ready_for_offline_hybrid_eval"] and selection["selected_result"] is not None:
        payload["selected_frozen_scorer"] = selection["selected_result"][
            "in_sample_full_fit_only_not_validation"
        ]["frozen_scorer"]
    return payload


def build_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v3_baseline_path: Path,
    v2_baseline_path: Path | None = None,
    v3_baseline_git_ref: str | None = None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    label_path = label_dataset_path.resolve()
    readiness_path = readiness_matrix_path.resolve()
    embeddings_path = embeddings_provenance_path.resolve()
    for path in (label_path, readiness_path, embeddings_path):
        if not path.is_file():
            raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
                f"required input not found: {path}"
            )

    try:
        label_sha = sha256_file(label_path)
        readiness_sha = sha256_file(readiness_path)
        embeddings_sha = sha256_file(embeddings_path)
        label_payload = _load_json_object(label_path, label="label dataset")
        readiness_payload = _load_json_object(readiness_path, label="readiness matrix")
        embeddings_payload = _load_json_object(embeddings_path, label="embeddings provenance")
    except MLOfflineBridgeRecommendableScorerV3Error as exc:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(str(exc)) from exc

    if label_payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
            f"label dataset must be {LABEL_DATASET_VERSION}"
        )

    try:
        readiness_groups = _validate_readiness_matrix(readiness_payload, label_dataset_sha256=label_sha)
        embedding_dimensions = _validate_embeddings_provenance(embeddings_payload)
        row_level_rows = _slice_row_level_rows(label_payload)
        row_level_counts = _validate_row_level_counts(row_level_rows)
        deduped_rows, dedupe_meta = _dedupe_by_work_id(row_level_rows)
        deduped_counts = _validate_deduped_counts(deduped_rows, dedupe_meta)
    except MLOfflineBridgeRecommendableScorerV3Error as exc:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(str(exc)) from exc

    overlap_count = _overlap_count_from_label_metadata(label_payload)
    deduped_work_ids = _internal_work_ids(deduped_rows, expected_count=DEDUPED_ROWS)
    try:
        vectors_by_work = _load_slice_embeddings_select_only(
            conn,
            internal_work_ids=deduped_work_ids,
            expected_dimensions=embedding_dimensions,
        )
    except MLOfflineBridgeRecommendableScorerV3Error as exc:
        raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(str(exc)) from exc

    baseline_reference = _load_baseline_reference(v3_baseline_path, git_ref=v3_baseline_git_ref)
    v2_payload = None
    if v2_baseline_path is not None:
        v2_payload = _load_json_object(v2_baseline_path, label="v2 baseline")
        if v2_payload.get("scorer_version") != V2_SCORER_VERSION:
            raise MLOfflineBridgeRecommendableScorerV3RegularizationSensitivityError(
                "v2 baseline scorer_version mismatch"
            )

    input_records = [
        {"name": "label_dataset", "path": portable_repo_path(label_path), "sha256": label_sha},
        {"name": "readiness_matrix", "path": portable_repo_path(readiness_path), "sha256": readiness_sha},
        {"name": "embeddings_provenance", "path": portable_repo_path(embeddings_path), "sha256": embeddings_sha},
        dict(baseline_reference),
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

    metadata_extra = {
        "training_slice_filter": {
            "split": "audit_only",
            "family": FAMILY,
            "allowed_review_pool_variants": sorted(ALLOWED_REVIEW_POOL_VARIANTS),
            "target": TARGET,
            "target_must_be_boolean": True,
            "dedupe_policy": (
                "ml_bridge_shadow_pilot_audit > ml_bridge_top_ranked_validation_audit > "
                "ml_bridge_negative_mining_audit"
            ),
        },
        "label_dataset_version": LABEL_DATASET_VERSION,
        "readiness_matrix_version": READINESS_MATRIX_VERSION,
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
    }

    return build_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_payload_from_slice(
        deduped_rows=deduped_rows,
        vectors_by_work=vectors_by_work,
        baseline_reference=baseline_reference,
        input_records=input_records,
        metadata_extra=metadata_extra,
        v2_payload=v2_payload,
        expected_v2_work_id_count=V2_OVERLAP_WORK_IDS if v2_payload is not None else None,
        random_seed=random_seed,
    )


def markdown_from_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity(
    payload: dict[str, Any],
) -> str:
    baseline = payload.get("metadata", {}).get("baseline_reference", {})
    lines = [
        "# Offline bridge recommendable scorer v3 regularization sensitivity v1",
        "",
        "Offline-only sweep over the existing v3 `bridge_recommendable` diagnostic. "
        "No serving, API, ranking, or database-write behavior is changed.",
        "",
        "## Baseline reference",
        "",
        f"- v3 baseline path: `{baseline.get('path')}`",
        f"- v3 baseline sha256: `{baseline.get('sha256')}`",
        f"- Baseline C: {baseline.get('baseline_C')}",
        "",
        "## Selection",
        "",
        f"- ready_for_offline_hybrid_eval: {payload.get('ready_for_offline_hybrid_eval')}",
        f"- selected_frozen_coefficient_C: {payload.get('selected_frozen_coefficient_C')}",
        f"- selection reason: {payload.get('selection_rule', {}).get('selection_reason')}",
        "",
        "## Sweep",
        "",
        "| C | OOF ROC AUC | OOF AP | OOF P@20 | In-sample ROC AUC | Gap | v2-set ROC AUC | Acceptable | Too strong |",
        "|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for item in payload.get("regularization_sweep", []):
        lines.append(
            f"| {item.get('C')} | {item.get('oof_roc_auc')} | {item.get('oof_average_precision')} | "
            f"{item.get('oof_precision_at_20')} | {item.get('in_sample_roc_auc')} | "
            f"{item.get('in_sample_auc_minus_oof_auc')} | {item.get('v3_on_v2_work_id_set_roc_auc')} | "
            f"{item.get('acceptable_for_offline_hybrid_eval')} | {item.get('too_strong_regularization')} |"
        )
    lines += [
        "",
        "## Targeted verdicts",
        "",
    ]
    for item in payload.get("regularization_sweep", []):
        targeted = item.get("targeted_decision_readouts", {})
        lines += [
            f"### C={item.get('C')}",
            "",
            f"- high_ml_low_bridge_score: {targeted.get('high_ml_low_bridge_score', {}).get('verdict')}",
            f"- high_bridge_score_low_ml: {targeted.get('high_bridge_score_low_ml', {}).get('verdict')}",
            f"- promoted_by_hybrid: {targeted.get('promoted_by_hybrid', {}).get('verdict')}",
            f"- demoted_by_hybrid: {targeted.get('demoted_by_hybrid', {}).get('verdict')}",
            "",
        ]
    lines += [
        "## Caveats",
        "",
        *[f"- {caveat}" for caveat in payload.get("caveats", [])],
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v3_baseline_path: Path,
    v2_baseline_path: Path | None,
    json_path: Path,
    markdown_path: Path | None,
    v3_baseline_git_ref: str | None = None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict[str, Any]:
    payload = build_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_payload(
        conn,
        label_dataset_path=label_dataset_path,
        readiness_matrix_path=readiness_matrix_path,
        embeddings_provenance_path=embeddings_provenance_path,
        v3_baseline_path=v3_baseline_path,
        v2_baseline_path=v2_baseline_path,
        v3_baseline_git_ref=v3_baseline_git_ref,
        random_seed=random_seed,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity(payload),
            encoding="utf-8",
        )
    return payload


def run_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    readiness_matrix_path: Path,
    embeddings_provenance_path: Path,
    v3_baseline_path: Path,
    v2_baseline_path: Path | None,
    output_json: Path,
    markdown_output: Path | None,
    v3_baseline_git_ref: str | None = None,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> None:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn) as conn:
        write_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity(
            conn,
            label_dataset_path=label_dataset_path,
            readiness_matrix_path=readiness_matrix_path,
            embeddings_provenance_path=embeddings_provenance_path,
            v3_baseline_path=v3_baseline_path,
            v2_baseline_path=v2_baseline_path,
            json_path=output_json,
            markdown_path=markdown_output,
            v3_baseline_git_ref=v3_baseline_git_ref,
            random_seed=random_seed,
        )
