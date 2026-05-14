"""Text-only baseline diagnostic over frozen external embedding artifacts.

This command is file-in/file-out only. It does not use Postgres, ranking
artifacts, paper_scores, or production model outputs.
"""

from __future__ import annotations

import json
import random
import warnings
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import sklearn
from sklearn.compose import ColumnTransformer
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

ARTIFACT_TYPE = "ml_text_only_baseline_external"
BASELINE_VERSION = "ml-text-only-baseline-external-v7"
EMBEDDING_ARTIFACT_TYPE = "ml_external_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-external-text-embeddings-v7"
REVIEW_POOL_VARIANT = "ml_external_near_miss_audit"
EXPECTED_ROWS_V7 = 60
TARGETS = ("good_or_acceptable", "surprising_or_useful")
MODEL_NAMES = (
    "embedding_logistic",
    "metadata_sample_reason_logistic",
    "majority_class",
    "stratified_random_prevalence",
)

CAVEATS = (
    "Not validation.",
    "Single-reviewer audit labels.",
    "External near-miss pool only.",
    "No product ranking or API behavior change.",
    "Frozen offline embeddings only.",
    "No claim of live recommender quality.",
    "CV leakage guard applies only to the described folds and preprocessing, not to broader sampling bias.",
)

PRODUCTION_WARNING = (
    "This is not a production recommender test. Production-grade evaluation would still require deliberate splits, "
    "larger and multi-reviewer labels, product-matched candidate pools, top-k workflow metrics, and shadow or flagged "
    "experiments."
)


class MLTextOnlyBaselineError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLTextOnlyBaselineError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLTextOnlyBaselineError(f"Expected JSON object in {path}")
    return payload


def _duplicate_values(values: Sequence[str]) -> list[str]:
    counts = Counter(values)
    return sorted(k for k, v in counts.items() if v > 1)


def _validate_embeddings_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, dict):
        raise MLTextOnlyBaselineError("embedding artifact missing metadata object")
    if not isinstance(rows, list):
        raise MLTextOnlyBaselineError("embedding artifact missing rows array")
    if metadata.get("artifact_type") != EMBEDDING_ARTIFACT_TYPE:
        raise MLTextOnlyBaselineError(
            f"expected embedding metadata.artifact_type={EMBEDDING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDING_ARTIFACT_VERSION:
        raise MLTextOnlyBaselineError(
            "expected embedding metadata.embedding_artifact_version="
            f"{EMBEDDING_ARTIFACT_VERSION!r}, got {metadata.get('embedding_artifact_version')!r}"
        )

    normalized: list[dict[str, Any]] = []
    row_ids: list[str] = []
    bad_status: list[str] = []
    bad_vector: list[str] = []
    expected_dim = metadata.get("embedding_dimensions")
    if not isinstance(expected_dim, int) or expected_dim <= 0:
        raise MLTextOnlyBaselineError("embedding artifact missing positive metadata.embedding_dimensions")
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise MLTextOnlyBaselineError(f"embedding row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLTextOnlyBaselineError(f"embedding row {idx} missing row_id")
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
        raise MLTextOnlyBaselineError(f"embedding artifact contains duplicate row_id values: {dupes[:10]}")
    if bad_status:
        raise MLTextOnlyBaselineError(f"embedding rows are not ok for row_id values: {bad_status[:20]}")
    if bad_vector:
        raise MLTextOnlyBaselineError(f"embedding rows have invalid vector dimensions/values: {bad_vector[:20]}")
    if len(normalized) != EXPECTED_ROWS_V7:
        raise MLTextOnlyBaselineError(f"expected {EXPECTED_ROWS_V7} embedding rows for v7, found {len(normalized)}")
    return metadata, sorted(normalized, key=lambda row: str(row["row_id"]))


def _select_external_label_rows(payload: Mapping[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLTextOnlyBaselineError("label dataset missing rows array")
    out = [
        dict(row)
        for row in rows
        if isinstance(row, dict) and str(row.get("review_pool_variant") or "").strip() == REVIEW_POOL_VARIANT
    ]
    row_ids = [str(row.get("row_id") or "").strip() for row in out]
    if any(not rid for rid in row_ids):
        raise MLTextOnlyBaselineError("selected external label rows contain empty row_id")
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLTextOnlyBaselineError(f"selected external label rows contain duplicate row_id values: {dupes[:10]}")
    if len(out) != EXPECTED_ROWS_V7:
        raise MLTextOnlyBaselineError(f"expected {EXPECTED_ROWS_V7} external label rows for v7, found {len(out)}")
    return str(payload.get("dataset_version") or ""), sorted(out, key=lambda row: str(row["row_id"]))


def _joined_rows(
    *,
    label_rows: Sequence[dict[str, Any]],
    embedding_rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    labels_by_id = {str(row["row_id"]): row for row in label_rows}
    embeddings_by_id = {str(row["row_id"]): row for row in embedding_rows}
    label_ids = set(labels_by_id)
    embedding_ids = set(embeddings_by_id)
    missing_embedding = sorted(label_ids - embedding_ids)
    extra_embedding = sorted(embedding_ids - label_ids)
    if missing_embedding or extra_embedding:
        raise MLTextOnlyBaselineError(
            "row_id key mismatch between labels and embeddings: "
            f"missing_embeddings={len(missing_embedding)}, extra_embeddings={len(extra_embedding)}"
        )
    joined = []
    for row_id in sorted(label_ids):
        label = labels_by_id[row_id]
        emb = embeddings_by_id[row_id]
        joined.append(
            {
                "row_id": row_id,
                "paper_id": label.get("paper_id") or emb.get("paper_id"),
                "openalex_work_id": label.get("openalex_work_id") or emb.get("openalex_work_id"),
                "work_id": label.get("work_id") or emb.get("work_id"),
                "title": label.get("title"),
                "sample_reason": str(label.get("sample_reason") or "").strip() or "(missing)",
                "labels": label,
                "embedding": emb,
            }
        )
    return joined, {
        "selected_external_label_rows": len(label_rows),
        "embedding_rows": len(embedding_rows),
        "joined_rows": len(joined),
        "expected_rows_v7": EXPECTED_ROWS_V7,
        "missing_embedding_for_label_count": len(missing_embedding),
        "extra_embedding_without_label_count": len(extra_embedding),
    }


def _validate_target_values(rows: Sequence[Mapping[str, Any]], target: str) -> list[bool]:
    bad = [str(row.get("row_id")) for row in rows if not isinstance(row["labels"].get(target), bool)]
    if bad:
        raise MLTextOnlyBaselineError(f"target {target} has non-boolean/null values for row_id values: {bad[:20]}")
    return [bool(row["labels"][target]) for row in rows]


def _confusion_counts(y_true: Sequence[bool], y_pred: Sequence[bool]) -> dict[str, int]:
    matrix = confusion_matrix(y_true, y_pred, labels=[False, True])
    return {
        "tn": int(matrix[0][0]),
        "fp": int(matrix[0][1]),
        "fn": int(matrix[1][0]),
        "tp": int(matrix[1][1]),
    }


def _metrics(
    y_true: Sequence[bool],
    y_pred: Sequence[bool],
    *,
    scores: Sequence[float] | None,
    roc_auc_null_reason: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "confusion": _confusion_counts(y_true, y_pred),
    }
    if scores is not None and len(set(y_true)) == 2:
        out["roc_auc"] = float(roc_auc_score(y_true, scores))
        out["roc_auc_reason"] = None
    else:
        out["roc_auc"] = None
        out["roc_auc_reason"] = roc_auc_null_reason or "ROC-AUC requires row-specific probabilistic scores and both classes."
    return out


def _model_for_name(name: str, *, random_seed: int) -> Pipeline:
    if name == "embedding_logistic":
        return Pipeline(
            [
                ("scaler", StandardScaler()),
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
    if name == "metadata_sample_reason_logistic":
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
    raise ValueError(name)


def _features_for_model(rows: Sequence[Mapping[str, Any]], model_name: str) -> list[Any]:
    if model_name == "embedding_logistic":
        return [list(row["embedding"]["embedding"]) for row in rows]
    if model_name == "metadata_sample_reason_logistic":
        return [[str(row.get("sample_reason") or "(missing)")] for row in rows]
    raise ValueError(model_name)


def _evaluate_target(
    *,
    rows: Sequence[dict[str, Any]],
    target: str,
    random_seed: int,
    cv_folds: int,
) -> dict[str, Any]:
    y = _validate_target_values(rows, target)
    counts = {
        "positive": sum(1 for v in y if v),
        "negative": sum(1 for v in y if not v),
        "null": 0,
        "total": len(y),
    }
    min_class = min(counts["positive"], counts["negative"])
    if min_class < 2:
        raise MLTextOnlyBaselineError(f"cannot evaluate binary target {target}: min_class_count={min_class} < 2")
    effective_folds = min(int(cv_folds), min_class)
    if effective_folds < 2:
        raise MLTextOnlyBaselineError(f"cannot evaluate binary target {target}: effective cv folds < 2")

    splitter = StratifiedKFold(n_splits=effective_folds, shuffle=True, random_state=random_seed)
    y_int = [1 if v else 0 for v in y]
    fold_indices = list(splitter.split([[0]] * len(rows), y_int))

    models: dict[str, Any] = {}
    for model_name in MODEL_NAMES:
        oof_true: list[bool] = []
        oof_pred: list[bool] = []
        oof_scores: list[float] | None = [] if model_name in {"embedding_logistic", "metadata_sample_reason_logistic"} else None
        per_fold: list[dict[str, Any]] = []
        oof_rows: list[dict[str, Any]] = []
        for fold_index, (train_idx, test_idx) in enumerate(fold_indices, start=1):
            train_rows = [rows[i] for i in train_idx]
            test_rows = [rows[i] for i in test_idx]
            y_train = [y[i] for i in train_idx]
            y_test = [y[i] for i in test_idx]
            fold_scores: list[float] | None = None

            if model_name in {"embedding_logistic", "metadata_sample_reason_logistic"}:
                model = _model_for_name(model_name, random_seed=random_seed)
                x_train = _features_for_model(train_rows, model_name)
                x_test = _features_for_model(test_rows, model_name)
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="'penalty' was deprecated",
                        category=FutureWarning,
                    )
                    model.fit(x_train, y_train)
                pred = [bool(v) for v in model.predict(x_test)]
                proba = model.predict_proba(x_test)
                class_order = list(model.named_steps["classifier"].classes_)
                true_index = class_order.index(True)
                fold_scores = [float(row[true_index]) for row in proba]
            elif model_name == "majority_class":
                train_prevalence = sum(1 for v in y_train if v) / len(y_train)
                majority = train_prevalence >= 0.5
                pred = [majority] * len(y_test)
            elif model_name == "stratified_random_prevalence":
                train_prevalence = sum(1 for v in y_train if v) / len(y_train)
                rng = random.Random(f"{random_seed}|{target}|{fold_index}|stratified_random_prevalence")
                pred = [rng.random() < train_prevalence for _ in y_test]
            else:
                raise ValueError(model_name)

            fold_metrics = _metrics(
                y_test,
                pred,
                scores=fold_scores,
                roc_auc_null_reason="baseline has no row-specific probabilistic scores.",
            )
            per_fold.append(
                {
                    "fold": fold_index,
                    "train_count": len(train_rows),
                    "test_count": len(test_rows),
                    "train_positive": sum(1 for v in y_train if v),
                    "train_negative": sum(1 for v in y_train if not v),
                    "test_positive": sum(1 for v in y_test if v),
                    "test_negative": sum(1 for v in y_test if not v),
                    "metrics": fold_metrics,
                }
            )
            for row, actual, predicted, score in zip(
                test_rows,
                y_test,
                pred,
                fold_scores if fold_scores is not None else [None] * len(test_rows),
                strict=True,
            ):
                oof_rows.append(
                    {
                        "row_id": row["row_id"],
                        "paper_id": row.get("paper_id"),
                        "openalex_work_id": row.get("openalex_work_id"),
                        "sample_reason": row.get("sample_reason"),
                        "actual": actual,
                        "predicted": predicted,
                        "score": score,
                    }
                )
            oof_true.extend(y_test)
            oof_pred.extend(pred)
            if oof_scores is not None and fold_scores is not None:
                oof_scores.extend(fold_scores)

        aggregate = _metrics(
            oof_true,
            oof_pred,
            scores=oof_scores,
            roc_auc_null_reason="baseline has no row-specific probabilistic scores.",
        )
        models[model_name] = {
            "aggregate_metrics": aggregate,
            "confusion_summary": aggregate["confusion"],
            "per_fold": per_fold,
            "oof_rows": sorted(oof_rows, key=lambda row: str(row["row_id"])),
        }

    return {
        "target": target,
        "class_counts": counts,
        "effective_cv_folds": effective_folds,
        "requested_cv_folds": cv_folds,
        "models": models,
    }


def build_ml_text_only_baseline_payload(
    *,
    embeddings_path: Path,
    label_dataset_path: Path,
    random_seed: int = 0,
    cv_folds: int = 5,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if cv_folds < 2:
        raise MLTextOnlyBaselineError("--cv-folds must be >= 2")

    emb_path = Path(embeddings_path)
    label_path = Path(label_dataset_path)
    emb_payload = _load_json_object(emb_path)
    label_payload = _load_json_object(label_path)
    embedding_metadata, embedding_rows = _validate_embeddings_payload(emb_payload)
    label_dataset_version, label_rows = _select_external_label_rows(label_payload)
    joined, join_summary = _joined_rows(label_rows=label_rows, embedding_rows=embedding_rows)

    per_target = {
        target: _evaluate_target(rows=joined, target=target, random_seed=random_seed, cv_folds=cv_folds)
        for target in TARGETS
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
        "cv_folds": int(cv_folds),
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "caveats": list(CAVEATS),
        "production_recommender_warning": PRODUCTION_WARNING,
    }
    return {
        "metadata": metadata,
        "join_summary": join_summary,
        "per_target": per_target,
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def render_markdown(payload: Mapping[str, Any]) -> str:
    meta = payload["metadata"]
    lines = [
        "# Text-Only Baseline: External Near-Miss v7",
        "",
        "Offline diagnostic over frozen external near-miss embeddings and v7 labels only.",
        "",
        "## Inputs",
        "",
        f"- **embeddings:** `{meta.get('embeddings_path')}`",
        f"- **embeddings_sha256:** `{meta.get('embeddings_sha256')}`",
        f"- **label_dataset:** `{meta.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{meta.get('label_dataset_sha256')}`",
        f"- **review_pool_variant:** `{meta.get('review_pool_variant')}`",
        f"- **joined rows:** `{payload['join_summary']['joined_rows']}`",
        f"- **random_seed:** `{meta.get('random_seed')}`",
        "",
        "## Model Comparison",
        "",
        "| Target | Model | Accuracy | Balanced accuracy | Macro F1 | ROC-AUC | TN | FP | FN | TP |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for target, block in payload["per_target"].items():
        counts = block["class_counts"]
        lines.append(
            f"| {target} class counts | pos={counts['positive']} neg={counts['negative']} null={counts['null']} "
            f"folds={block['effective_cv_folds']} |  |  |  |  |  |  |  |  |"
        )
        for model_name, model in block["models"].items():
            metrics = model["aggregate_metrics"]
            conf = metrics["confusion"]
            lines.append(
                f"| {target} | {model_name} | {_fmt(metrics['accuracy'])} | "
                f"{_fmt(metrics['balanced_accuracy'])} | {_fmt(metrics['macro_f1'])} | "
                f"{_fmt(metrics['roc_auc'])} | {conf['tn']} | {conf['fp']} | {conf['fn']} | {conf['tp']} |"
            )
    lines.extend(
        [
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


def write_ml_text_only_baseline(
    *,
    embeddings_path: Path,
    label_dataset_path: Path,
    output_path: Path,
    markdown_output_path: Path | None,
    random_seed: int = 0,
    cv_folds: int = 5,
) -> dict[str, Any]:
    payload = build_ml_text_only_baseline_payload(
        embeddings_path=embeddings_path,
        label_dataset_path=label_dataset_path,
        random_seed=random_seed,
        cv_folds=cv_folds,
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
    "MLTextOnlyBaselineError",
    "REVIEW_POOL_VARIANT",
    "TARGETS",
    "build_ml_text_only_baseline_payload",
    "render_markdown",
    "write_ml_text_only_baseline",
]
