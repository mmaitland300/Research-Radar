"""Cross-pool text-only source-transfer diagnostic.

Consumes frozen labeled text embeddings plus the v7 label dataset. It does not
use Postgres, ranking outputs, persistent split files, or production artifacts.
"""

from __future__ import annotations

import json
import warnings
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_text_baseline_cross_pool"
BASELINE_VERSION = "ml-text-baseline-cross-pool-v1"
EMBEDDING_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-labeled-text-embeddings-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v7"
TARGETS = ("good_or_acceptable", "surprising_or_useful")

RANK_SHAPED_VARIANTS = frozenset(
    {
        "full_family_top_k",
        "bridge_eligible_only",
        "ml_contrastive_offline_audit",
        "ml_emerging_target_gap_audit:good_or_acceptable",
    }
)

SLICE_DEFINITIONS: dict[str, str] = {
    "external_near_miss": 'review_pool_variant == "ml_external_near_miss_audit"',
    "blind_snapshot": 'review_pool_variant == "ml_blind_snapshot_audit"',
    "rank_shaped_family": (
        "review_pool_variant in {full_family_top_k, bridge_eligible_only, "
        "ml_contrastive_offline_audit, ml_emerging_target_gap_audit:good_or_acceptable}"
    ),
    "hard_negative": 'review_pool_variant == "ml_hard_negative_audit"',
    "legacy_or_uncategorized": "review_pool_variant is null, empty, or whitespace-only",
}

IN_POOL_SLICES = tuple(SLICE_DEFINITIONS.keys())
TRANSFER_COMPARISONS = (
    ("external_near_miss_to_blind_snapshot", ("external_near_miss",), ("blind_snapshot",)),
    ("blind_snapshot_to_external_near_miss", ("blind_snapshot",), ("external_near_miss",)),
    ("rank_shaped_family_to_external_near_miss", ("rank_shaped_family",), ("external_near_miss",)),
    ("rank_shaped_family_to_blind_snapshot", ("rank_shaped_family",), ("blind_snapshot",)),
    (
        "external_near_miss_plus_blind_snapshot_to_rank_shaped_family",
        ("external_near_miss", "blind_snapshot"),
        ("rank_shaped_family",),
    ),
    (
        "all_not_external_near_miss_to_external_near_miss",
        ("blind_snapshot", "rank_shaped_family", "hard_negative", "legacy_or_uncategorized"),
        ("external_near_miss",),
    ),
    (
        "all_not_blind_snapshot_to_blind_snapshot",
        ("external_near_miss", "rank_shaped_family", "hard_negative", "legacy_or_uncategorized"),
        ("blind_snapshot",),
    ),
)

CAVEATS = (
    "Not validation; single-reviewer audit labels; observation-level duplicates/conflicts preserved.",
    "Source-transfer diagnostic only; not production ranking evidence.",
    (
        "Text format differs across rows (verbatim external vs OpenAlex-hydrated labeled format) and can be "
        "confounded with review_pool_variant; interpret cautiously, not causally."
    ),
    "No persistent train/dev/test split artifact.",
)

PRODUCTION_WARNING = (
    "This is not a production recommender test. Production-grade evaluation would still require deliberate splits, "
    "larger and multi-reviewer labels, product-matched candidate pools, top-k workflow metrics, and shadow or flagged "
    "experiments."
)


class MLTextBaselineCrossPoolError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLTextBaselineCrossPoolError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLTextBaselineCrossPoolError(f"Expected JSON object in {path}")
    return payload


def _duplicate_values(values: Sequence[str]) -> list[str]:
    counts = Counter(values)
    return sorted(k for k, v in counts.items() if v > 1)


def _validate_embedding_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, dict):
        raise MLTextBaselineCrossPoolError("embedding artifact missing metadata object")
    if not isinstance(rows, list):
        raise MLTextBaselineCrossPoolError("embedding artifact missing rows array")
    if metadata.get("artifact_type") != EMBEDDING_ARTIFACT_TYPE:
        raise MLTextBaselineCrossPoolError(
            f"expected embedding metadata.artifact_type={EMBEDDING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDING_ARTIFACT_VERSION:
        raise MLTextBaselineCrossPoolError(
            "expected embedding metadata.embedding_artifact_version="
            f"{EMBEDDING_ARTIFACT_VERSION!r}, got {metadata.get('embedding_artifact_version')!r}"
        )
    expected_dim = metadata.get("embedding_dimensions")
    if not isinstance(expected_dim, int) or expected_dim <= 0:
        raise MLTextBaselineCrossPoolError("embedding artifact missing positive metadata.embedding_dimensions")

    normalized: list[dict[str, Any]] = []
    row_ids: list[str] = []
    bad_status: list[str] = []
    bad_vector: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise MLTextBaselineCrossPoolError(f"embedding row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLTextBaselineCrossPoolError(f"embedding row {idx} missing row_id")
        row_ids.append(row_id)
        if row.get("embedding_status") != "ok":
            bad_status.append(row_id)
        vector = row.get("embedding")
        if not isinstance(vector, list) or len(vector) != expected_dim:
            bad_vector.append(row_id)
        else:
            try:
                row["embedding"] = [float(v) for v in vector]
            except (TypeError, ValueError):
                bad_vector.append(row_id)
        normalized.append(row)
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLTextBaselineCrossPoolError(f"embedding artifact contains duplicate row_id values: {dupes[:10]}")
    if bad_status:
        raise MLTextBaselineCrossPoolError(f"embedding rows are not ok for row_id values: {bad_status[:20]}")
    if bad_vector:
        raise MLTextBaselineCrossPoolError(f"embedding rows have invalid vector dimensions/values: {bad_vector[:20]}")
    return metadata, sorted(normalized, key=lambda row: str(row["row_id"]))


def _validate_label_payload(payload: Mapping[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    dataset_version = str(payload.get("dataset_version") or "")
    if dataset_version != LABEL_DATASET_VERSION:
        raise MLTextBaselineCrossPoolError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {dataset_version!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLTextBaselineCrossPoolError("label dataset missing rows array")
    normalized: list[dict[str, Any]] = []
    row_ids: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise MLTextBaselineCrossPoolError(f"label row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLTextBaselineCrossPoolError(f"label row {idx} missing row_id")
        row_ids.append(row_id)
        normalized.append(row)
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLTextBaselineCrossPoolError(f"label dataset contains duplicate row_id values: {dupes[:10]}")
    return dataset_version, normalized


def _slice_name(row: Mapping[str, Any]) -> str | None:
    variant = row.get("review_pool_variant")
    raw = "" if variant is None else str(variant).strip()
    if raw == "ml_external_near_miss_audit":
        return "external_near_miss"
    if raw == "ml_blind_snapshot_audit":
        return "blind_snapshot"
    if raw in RANK_SHAPED_VARIANTS:
        return "rank_shaped_family"
    if raw == "ml_hard_negative_audit":
        return "hard_negative"
    if not raw:
        return "legacy_or_uncategorized"
    return None


def _join_rows(
    *,
    embedding_rows: Sequence[dict[str, Any]],
    label_rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    labels_by_id = {str(row["row_id"]): row for row in label_rows}
    embeddings_by_id = {str(row["row_id"]): row for row in embedding_rows}
    embedding_ids = set(embeddings_by_id)
    label_ids = set(labels_by_id)
    missing_labels = sorted(embedding_ids - label_ids)
    extra_labels = sorted(label_ids - embedding_ids)
    if missing_labels or extra_labels:
        raise MLTextBaselineCrossPoolError(
            "row_id key mismatch between labels and embeddings: "
            f"missing_labels={len(missing_labels)}, extra_labels={len(extra_labels)}"
        )

    joined: list[dict[str, Any]] = []
    unknown_slice: list[str] = []
    for emb in embedding_rows:
        row_id = str(emb["row_id"])
        label = labels_by_id[row_id]
        slice_name = _slice_name(label)
        if slice_name is None:
            unknown_slice.append(row_id)
            continue
        joined.append(
            {
                "row_id": row_id,
                "paper_id": label.get("paper_id") or emb.get("paper_id"),
                "openalex_work_id": label.get("openalex_work_id") or emb.get("openalex_work_id"),
                "work_id": label.get("work_id") or emb.get("work_id"),
                "review_pool_variant": label.get("review_pool_variant"),
                "family": label.get("family"),
                "sample_reason": str(label.get("sample_reason") or "").strip() or "(missing)",
                "slice": slice_name,
                "embedding_text_format_version": emb.get("embedding_text_format_version"),
                "embedding": emb,
                "label": label,
            }
        )
    if unknown_slice:
        raise MLTextBaselineCrossPoolError(f"rows did not match any declared source slice: {unknown_slice[:20]}")
    return joined, {
        "embedding_rows": len(embedding_rows),
        "label_rows": len(label_rows),
        "joined_rows": len(joined),
        "missing_labels_for_embedding_count": len(missing_labels),
        "extra_labels_without_embedding_count": len(extra_labels),
    }


def _hist(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(field) or "(null)") for row in rows).items()))


def _comparison_histograms(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    return {
        "review_pool_variant": _hist(rows, "review_pool_variant"),
        "family": _hist(rows, "family"),
        "embedding_text_format_version": _hist(rows, "embedding_text_format_version"),
    }


def _target_rows(rows: Sequence[dict[str, Any]], target: str) -> tuple[list[dict[str, Any]], list[str]]:
    included: list[dict[str, Any]] = []
    excluded: list[str] = []
    for row in rows:
        value = row["label"].get(target)
        if isinstance(value, bool):
            included.append(row)
        else:
            excluded.append(str(row["row_id"]))
    return included, excluded


def _class_counts(rows: Sequence[Mapping[str, Any]], target: str) -> dict[str, int]:
    vals = [bool(row["label"][target]) for row in rows]
    return {
        "n": len(vals),
        "positive": sum(1 for v in vals if v),
        "negative": sum(1 for v in vals if not v),
    }


def _confusion_counts(y_true: Sequence[bool], y_pred: Sequence[bool]) -> dict[str, int]:
    matrix = confusion_matrix(y_true, y_pred, labels=[False, True])
    return {
        "tn": int(matrix[0][0]),
        "fp": int(matrix[0][1]),
        "fn": int(matrix[1][0]),
        "tp": int(matrix[1][1]),
    }


def _scores_vary(scores: Sequence[float] | None) -> bool:
    if scores is None or len(scores) < 2:
        return False
    return len({round(float(s), 15) for s in scores}) > 1


def _metrics(
    *,
    y_true: Sequence[bool],
    y_pred: Sequence[bool],
    scores: Sequence[float] | None,
    train_rows: Sequence[Mapping[str, Any]],
    test_rows: Sequence[Mapping[str, Any]],
    roc_auc_skip_reason: str | None = None,
) -> dict[str, Any]:
    train_counts = _bool_counts([bool(row["_target_value"]) for row in train_rows])
    test_counts = _bool_counts(list(y_true))
    out: dict[str, Any] = {
        "train_n": len(train_rows),
        "train_pos": train_counts["positive"],
        "train_neg": train_counts["negative"],
        "test_n": len(test_rows),
        "test_pos": test_counts["positive"],
        "test_neg": test_counts["negative"],
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "confusion": _confusion_counts(y_true, y_pred),
    }
    if len(set(y_true)) == 2 and _scores_vary(scores):
        out["roc_auc"] = float(roc_auc_score(y_true, scores))
        out["roc_auc_skip_reason"] = None
    else:
        out["roc_auc"] = None
        out["roc_auc_skip_reason"] = roc_auc_skip_reason or "ROC-AUC requires both test classes and non-constant scores."
    return out


def _bool_counts(values: Sequence[bool]) -> dict[str, int]:
    return {
        "positive": sum(1 for v in values if v),
        "negative": sum(1 for v in values if not v),
    }


def _embedding_model(*, random_seed: int) -> Pipeline:
    return Pipeline(
        [
            ("scaler", StandardScaler(with_mean=True)),
            (
                "classifier",
                LogisticRegression(
                    solver="lbfgs",
                    penalty="l2",
                    max_iter=5000,
                    random_state=random_seed,
                ),
            ),
        ]
    )


def _metadata_model(*, random_seed: int) -> Pipeline:
    return Pipeline(
        [
            (
                "encoder",
                ColumnTransformer(
                    [
                        (
                            "sample_reason",
                            OneHotEncoder(handle_unknown="ignore"),
                            [0],
                        )
                    ]
                ),
            ),
            (
                "classifier",
                LogisticRegression(
                    solver="lbfgs",
                    penalty="l2",
                    max_iter=5000,
                    random_state=random_seed,
                ),
            ),
        ]
    )


def _embedding_x(rows: Sequence[Mapping[str, Any]]) -> list[list[float]]:
    return [list(row["embedding"]["embedding"]) for row in rows]


def _metadata_x(rows: Sequence[Mapping[str, Any]]) -> list[list[str]]:
    return [[str(row.get("sample_reason") or "(missing)")] for row in rows]


def _fit_predict_model(
    *,
    model_name: str,
    train_rows: Sequence[dict[str, Any]],
    test_rows: Sequence[dict[str, Any]],
    random_seed: int,
) -> tuple[list[bool], list[float] | None]:
    y_train = [bool(row["_target_value"]) for row in train_rows]
    if model_name == "embedding_logistic":
        model = _embedding_model(random_seed=random_seed)
        x_train = _embedding_x(train_rows)
        x_test = _embedding_x(test_rows)
    elif model_name == "metadata_sample_reason_logistic":
        model = _metadata_model(random_seed=random_seed)
        x_train = _metadata_x(train_rows)
        x_test = _metadata_x(test_rows)
    elif model_name == "majority_train_baseline":
        model = DummyClassifier(strategy="most_frequent")
        x_train = [[0] for _ in train_rows]
        x_test = [[0] for _ in test_rows]
    else:
        raise ValueError(model_name)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="'penalty' was deprecated", category=FutureWarning)
        model.fit(x_train, y_train)
    pred = [bool(v) for v in model.predict(x_test)]
    if model_name == "majority_train_baseline":
        return pred, None
    if hasattr(model, "decision_function"):
        scores = [float(v) for v in model.decision_function(x_test)]
    else:
        proba = model.predict_proba(x_test)
        class_order = list(model.named_steps["classifier"].classes_)
        true_index = class_order.index(True)
        scores = [float(row[true_index]) for row in proba]
    return pred, scores


def _prevalence_baseline(
    *,
    train_rows: Sequence[dict[str, Any]],
    test_rows: Sequence[dict[str, Any]],
) -> tuple[list[bool], list[float]]:
    prevalence = sum(1 for row in train_rows if row["_target_value"]) / len(train_rows)
    pred = [prevalence >= 0.5 for _ in test_rows]
    return pred, [float(prevalence)] * len(test_rows)


def _prepare_target_rows(rows: Sequence[dict[str, Any]], target: str) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for row in rows:
        copy = dict(row)
        copy["_target_value"] = bool(row["label"][target])
        prepared.append(copy)
    return prepared


def _skip_block(
    *,
    comparison_name: str,
    comparison_type: str,
    target: str,
    reason: str,
    train_rows: Sequence[Mapping[str, Any]] = (),
    test_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "comparison_name": comparison_name,
        "comparison_type": comparison_type,
        "target": target,
        "skipped": True,
        "skip_reason": reason,
        "train_histograms": _comparison_histograms(train_rows),
        "test_histograms": _comparison_histograms(test_rows),
        "models": {},
    }


def _evaluate_models(
    *,
    comparison_name: str,
    comparison_type: str,
    target: str,
    train_rows: Sequence[dict[str, Any]],
    test_rows: Sequence[dict[str, Any]],
    random_seed: int,
    include_metadata: bool,
) -> dict[str, Any]:
    y_test = [bool(row["_target_value"]) for row in test_rows]
    model_names = ["embedding_logistic", "majority_train_baseline", "train_prevalence_score_baseline"]
    if include_metadata:
        model_names.append("metadata_sample_reason_logistic")
    models: dict[str, Any] = {}
    for model_name in model_names:
        if model_name == "train_prevalence_score_baseline":
            pred, scores = _prevalence_baseline(train_rows=train_rows, test_rows=test_rows)
            metrics = _metrics(
                y_true=y_test,
                y_pred=pred,
                scores=None,
                train_rows=train_rows,
                test_rows=test_rows,
                roc_auc_skip_reason="constant train-prevalence score is not an informative ranking score.",
            )
            metrics["constant_score"] = scores[0] if scores else None
        else:
            pred, scores = _fit_predict_model(
                model_name=model_name,
                train_rows=train_rows,
                test_rows=test_rows,
                random_seed=random_seed,
            )
            metrics = _metrics(
                y_true=y_test,
                y_pred=pred,
                scores=scores,
                train_rows=train_rows,
                test_rows=test_rows,
            )
        models[model_name] = metrics
    return {
        "comparison_name": comparison_name,
        "comparison_type": comparison_type,
        "target": target,
        "skipped": False,
        "skip_reason": None,
        "train_histograms": _comparison_histograms(train_rows),
        "test_histograms": _comparison_histograms(test_rows),
        "models": models,
    }


def _evaluate_transfer(
    *,
    comparison_name: str,
    target: str,
    source_slices: Sequence[str],
    target_slices: Sequence[str],
    rows_by_slice: Mapping[str, list[dict[str, Any]]],
    random_seed: int,
) -> dict[str, Any]:
    train_rows = [row for slice_name in source_slices for row in rows_by_slice.get(slice_name, [])]
    test_rows = [row for slice_name in target_slices for row in rows_by_slice.get(slice_name, [])]
    train_counts = _bool_counts([row["_target_value"] for row in train_rows])
    test_counts = _bool_counts([row["_target_value"] for row in test_rows])
    if not train_rows or not test_rows:
        return _skip_block(
            comparison_name=comparison_name,
            comparison_type="source_transfer",
            target=target,
            reason="empty train or test slice",
            train_rows=train_rows,
            test_rows=test_rows,
        )
    if train_counts["positive"] == 0 or train_counts["negative"] == 0:
        return _skip_block(
            comparison_name=comparison_name,
            comparison_type="source_transfer",
            target=target,
            reason="train slice lacks both target classes",
            train_rows=train_rows,
            test_rows=test_rows,
        )
    if test_counts["positive"] == 0 or test_counts["negative"] == 0:
        return _skip_block(
            comparison_name=comparison_name,
            comparison_type="source_transfer",
            target=target,
            reason="test slice lacks both target classes",
            train_rows=train_rows,
            test_rows=test_rows,
        )
    return _evaluate_models(
        comparison_name=comparison_name,
        comparison_type="source_transfer",
        target=target,
        train_rows=train_rows,
        test_rows=test_rows,
        random_seed=random_seed,
        include_metadata=False,
    )


def _evaluate_in_pool_cv(
    *,
    slice_name: str,
    target: str,
    rows: Sequence[dict[str, Any]],
    random_seed: int,
) -> dict[str, Any]:
    counts = _bool_counts([row["_target_value"] for row in rows])
    min_class_count = min(counts["positive"], counts["negative"]) if rows else 0
    if len(rows) < 2:
        return _skip_block(
            comparison_name=slice_name,
            comparison_type="in_pool_cv",
            target=target,
            reason="slice has fewer than 2 rows",
            train_rows=rows,
            test_rows=rows,
        )
    if min_class_count < 2:
        return _skip_block(
            comparison_name=slice_name,
            comparison_type="in_pool_cv",
            target=target,
            reason="slice lacks enough rows in both classes for stratified CV",
            train_rows=rows,
            test_rows=rows,
        )
    n_splits = min(5, min_class_count)
    splitter = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=random_seed)
    y = [1 if row["_target_value"] else 0 for row in rows]

    fold_blocks: list[dict[str, Any]] = []
    model_oof: dict[str, dict[str, list[Any]]] = {
        name: {"true": [], "pred": [], "scores": [], "train_rows": [], "test_rows": []}
        for name in (
            "embedding_logistic",
            "majority_train_baseline",
            "train_prevalence_score_baseline",
            "metadata_sample_reason_logistic",
        )
    }
    for fold_idx, (train_idx, test_idx) in enumerate(splitter.split([[0]] * len(rows), y), start=1):
        train_rows = [rows[i] for i in train_idx]
        test_rows = [rows[i] for i in test_idx]
        block = _evaluate_models(
            comparison_name=f"{slice_name}_fold_{fold_idx}",
            comparison_type="in_pool_cv_fold",
            target=target,
            train_rows=train_rows,
            test_rows=test_rows,
            random_seed=random_seed,
            include_metadata=True,
        )
        fold_blocks.append(block)
        y_test = [bool(row["_target_value"]) for row in test_rows]
        for model_name, metrics in block["models"].items():
            # Reconstruct aggregate inputs from confusion is impossible, so refit per fold once here.
            if model_name == "train_prevalence_score_baseline":
                pred, scores = _prevalence_baseline(train_rows=train_rows, test_rows=test_rows)
                scores_for_oof: list[float] | None = None
            else:
                pred, scores = _fit_predict_model(
                    model_name=model_name,
                    train_rows=train_rows,
                    test_rows=test_rows,
                    random_seed=random_seed,
                )
                scores_for_oof = scores
            model_oof[model_name]["true"].extend(y_test)
            model_oof[model_name]["pred"].extend(pred)
            if scores_for_oof is not None:
                model_oof[model_name]["scores"].extend(scores_for_oof)
            model_oof[model_name]["train_rows"].extend(train_rows)
            model_oof[model_name]["test_rows"].extend(test_rows)

    aggregate_models: dict[str, Any] = {}
    for model_name, data in model_oof.items():
        scores = data["scores"] if data["scores"] else None
        if model_name == "train_prevalence_score_baseline":
            scores = None
            reason = "constant train-prevalence score is not an informative ranking score."
        elif model_name == "majority_train_baseline":
            scores = None
            reason = "majority baseline has no row-specific probabilistic scores."
        else:
            reason = None
        aggregate_models[model_name] = _metrics(
            y_true=data["true"],
            y_pred=data["pred"],
            scores=scores,
            train_rows=rows,
            test_rows=rows,
            roc_auc_skip_reason=reason,
        )

    return {
        "comparison_name": slice_name,
        "comparison_type": "in_pool_cv",
        "target": target,
        "skipped": False,
        "skip_reason": None,
        "effective_cv_folds": n_splits,
        "train_histograms": _comparison_histograms(rows),
        "test_histograms": _comparison_histograms(rows),
        "models": aggregate_models,
        "per_fold": fold_blocks,
    }


def build_ml_text_baseline_cross_pool_payload(
    *,
    embeddings_path: Path,
    label_dataset_path: Path,
    random_seed: int = 0,
    generated_at: str | None = None,
) -> dict[str, Any]:
    emb_path = Path(embeddings_path)
    label_path = Path(label_dataset_path)
    emb_payload = _load_json_object(emb_path)
    label_payload = _load_json_object(label_path)
    embedding_metadata, embedding_rows = _validate_embedding_payload(emb_payload)
    label_dataset_version, label_rows = _validate_label_payload(label_payload)
    joined_rows, join_summary = _join_rows(embedding_rows=embedding_rows, label_rows=label_rows)

    per_target: dict[str, Any] = {}
    for target in TARGETS:
        target_rows_raw, excluded_ids = _target_rows(joined_rows, target)
        target_rows = _prepare_target_rows(target_rows_raw, target)
        rows_by_slice: dict[str, list[dict[str, Any]]] = {name: [] for name in IN_POOL_SLICES}
        for row in target_rows:
            rows_by_slice[row["slice"]].append(row)

        in_pool = {
            slice_name: _evaluate_in_pool_cv(
                slice_name=slice_name,
                target=target,
                rows=rows_by_slice[slice_name],
                random_seed=random_seed,
            )
            for slice_name in IN_POOL_SLICES
        }
        transfer = {
            name: _evaluate_transfer(
                comparison_name=name,
                target=target,
                source_slices=source_slices,
                target_slices=target_slices,
                rows_by_slice=rows_by_slice,
                random_seed=random_seed,
            )
            for name, source_slices, target_slices in TRANSFER_COMPARISONS
        }
        per_target[target] = {
            "target": target,
            "eligible_row_count": len(target_rows),
            "excluded_count": len(excluded_ids),
            "excluded_row_ids": excluded_ids,
            "slice_counts": {
                name: {
                    **_bool_counts([row["_target_value"] for row in rows]),
                    "n": len(rows),
                }
                for name, rows in rows_by_slice.items()
            },
            "in_pool_cv": in_pool,
            "source_transfer": transfer,
        }

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "baseline_version": BASELINE_VERSION,
        "generated_at": generated_at or _now_iso_z(),
        "embeddings_path": portable_repo_path(emb_path),
        "embeddings_sha256": sha256_file(emb_path),
        "label_dataset_path": portable_repo_path(label_path),
        "label_dataset_sha256": sha256_file(label_path),
        "embedding_artifact_version": embedding_metadata.get("embedding_artifact_version"),
        "label_dataset_version": label_dataset_version,
        "sklearn_version": sklearn.__version__,
        "random_seed": int(random_seed),
        "slice_definitions": dict(SLICE_DEFINITIONS),
        "caveats": list(CAVEATS),
        "production_recommender_warning": PRODUCTION_WARNING,
    }
    return {"metadata": metadata, "join_summary": join_summary, "per_target": per_target}


def _fmt(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def render_markdown(payload: Mapping[str, Any]) -> str:
    meta = payload["metadata"]
    lines = [
        "# Text Baseline Cross-Pool v1",
        "",
        "Offline source-transfer diagnostic over frozen labeled text embeddings and v7 labels.",
        "",
        "## Inputs",
        "",
        f"- **embeddings:** `{meta.get('embeddings_path')}`",
        f"- **embeddings_sha256:** `{meta.get('embeddings_sha256')}`",
        f"- **label_dataset:** `{meta.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{meta.get('label_dataset_sha256')}`",
        f"- **joined rows:** `{payload['join_summary']['joined_rows']}`",
        f"- **random_seed:** `{meta.get('random_seed')}`",
        "",
        "## Slice Definitions",
        "",
        "| Slice | Definition |",
        "|---|---|",
    ]
    for name, definition in meta.get("slice_definitions", {}).items():
        lines.append(f"| `{name}` | {definition} |")
    for target, block in payload["per_target"].items():
        lines.extend(
            [
                "",
                f"## {target}",
                "",
                f"- **eligible rows:** `{block['eligible_row_count']}`",
                f"- **excluded rows:** `{block['excluded_count']}`",
                "",
                "### In-Pool CV Summary",
                "",
                "| Slice | Model | Skipped | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for slice_name, comparison in block["in_pool_cv"].items():
            if comparison["skipped"]:
                lines.append(f"| `{slice_name}` | all | true: {comparison['skip_reason']} |  |  |  |  |  |  |  |")
                continue
            for model_name, metrics in comparison["models"].items():
                conf = metrics["confusion"]
                lines.append(
                    f"| `{slice_name}` | `{model_name}` | false | {_fmt(metrics['balanced_accuracy'])} | "
                    f"{_fmt(metrics['macro_f1'])} | {_fmt(metrics['roc_auc'])} | {conf['tn']} | {conf['fp']} | "
                    f"{conf['fn']} | {conf['tp']} |"
                )
        lines.extend(
            [
                "",
                "### Transfer Summary",
                "",
                "| Comparison | Model | Skipped | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for comparison_name, comparison in block["source_transfer"].items():
            if comparison["skipped"]:
                lines.append(
                    f"| `{comparison_name}` | all | true: {comparison['skip_reason']} |  |  |  |  |  |  |  |"
                )
                continue
            for model_name, metrics in comparison["models"].items():
                conf = metrics["confusion"]
                lines.append(
                    f"| `{comparison_name}` | `{model_name}` | false | {_fmt(metrics['balanced_accuracy'])} | "
                    f"{_fmt(metrics['macro_f1'])} | {_fmt(metrics['roc_auc'])} | {conf['tn']} | {conf['fp']} | "
                    f"{conf['fn']} | {conf['tp']} |"
                )
    lines.extend(
        [
            "",
            "## What This Means",
            "",
            "This artifact compares in-source and cross-source text-only signal. Differences can reflect real label signal, source/worksheet selection effects, label imbalance, and text-format confounding; interpret as diagnostic evidence, not causal proof.",
            "",
            "## Not A Production Recommender Test",
            "",
            str(meta.get("production_recommender_warning")),
            "",
            "## Caveats",
            "",
            *[f"- {caveat}" for caveat in meta.get("caveats", [])],
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_text_baseline_cross_pool(
    *,
    embeddings_path: Path,
    label_dataset_path: Path,
    output_path: Path,
    markdown_output_path: Path | None,
    random_seed: int = 0,
) -> dict[str, Any]:
    payload = build_ml_text_baseline_cross_pool_payload(
        embeddings_path=embeddings_path,
        label_dataset_path=label_dataset_path,
        random_seed=random_seed,
    )
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if markdown_output_path is not None:
        md = Path(markdown_output_path)
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(render_markdown(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "BASELINE_VERSION",
    "CAVEATS",
    "MLTextBaselineCrossPoolError",
    "SLICE_DEFINITIONS",
    "TARGETS",
    "build_ml_text_baseline_cross_pool_payload",
    "render_markdown",
    "write_ml_text_baseline_cross_pool",
]
