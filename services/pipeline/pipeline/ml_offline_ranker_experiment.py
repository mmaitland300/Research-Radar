"""Offline grouped-CV ranker experiment over frozen labeled text embeddings.

This is an audit diagnostic only. It reads committed label, split-policy, and
embedding JSON artifacts, runs grouped cross-validation in memory, and writes
JSON/Markdown summaries. It does not use Postgres, OpenAI/OpenAlex calls,
ranking services, production model files, or product/runtime code.
"""

from __future__ import annotations

import json
import math
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence

import sklearn
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.ml_label_split_policy import (
    ARTIFACT_TYPE as SPLIT_POLICY_ARTIFACT_TYPE,
    POLICY_VERSION as SPLIT_POLICY_VERSION,
    TARGET_GOOD,
    TARGET_SURPRISING,
    canonical_openalex_work_id,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_offline_ranker_experiment"
EXPERIMENT_VERSION = "ml-offline-ranker-experiment-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
EMBEDDING_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-labeled-text-embeddings-v3"
MODEL_NAMES = ("majority_class", "prevalence_probability", "embedding_logistic")

CAVEATS = (
    "Not validation.",
    "Offline diagnostic only.",
    "Single-reviewer audit labels and rubric limits remain.",
    "Observation-level labels are preserved; duplicate/conflicting work observations are not silently merged.",
    "Grouped CV reduces same-work leakage but does not eliminate source-selection, pool, or label-context bias.",
    "No production ranking/API/web behavior change is supported.",
    "No production model artifact is produced.",
)


class MLOfflineRankerExperimentError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineRankerExperimentError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineRankerExperimentError(f"Expected JSON object in {path}")
    return payload


def _input_record(name: str, path: Path, *, repo_root: Path | None = None) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLOfflineRankerExperimentError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _duplicate_values(values: Sequence[str]) -> list[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if count > 1)


def _bucket(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "(null)"


def _bool_counts(values: Sequence[bool]) -> dict[str, int]:
    return {
        "positive": sum(1 for value in values if value),
        "negative": sum(1 for value in values if not value),
    }


def _confusion_counts(y_true: Sequence[bool], y_pred: Sequence[bool]) -> dict[str, int]:
    matrix = confusion_matrix(y_true, y_pred, labels=[False, True])
    return {
        "tn": int(matrix[0][0]),
        "fp": int(matrix[0][1]),
        "fn": int(matrix[1][0]),
        "tp": int(matrix[1][1]),
    }


def _scores_available(scores: Sequence[float] | None) -> bool:
    return scores is not None and len(scores) > 0


def _classification_metrics(
    *,
    y_true: Sequence[bool],
    y_pred: Sequence[bool],
    scores: Sequence[float] | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "confusion": _confusion_counts(y_true, y_pred),
    }
    if len(set(y_true)) == 2 and _scores_available(scores):
        out["roc_auc"] = float(roc_auc_score(y_true, scores))
        out["roc_auc_skip_reason"] = None
        out["average_precision"] = float(average_precision_score(y_true, scores))
        out["average_precision_skip_reason"] = None
    else:
        reason = "requires eval rows with both classes and probabilistic scores"
        out["roc_auc"] = None
        out["roc_auc_skip_reason"] = reason
        out["average_precision"] = None
        out["average_precision_skip_reason"] = reason
    return out


def _validate_split_policy(payload: Mapping[str, Any], *, target: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLOfflineRankerExperimentError("split policy missing metadata object")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineRankerExperimentError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLOfflineRankerExperimentError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    allowed = payload.get("allowed_targets_for_v1_split")
    forbidden = payload.get("forbidden_targets")
    if allowed != [TARGET_GOOD]:
        raise MLOfflineRankerExperimentError("split policy must allow exactly good_or_acceptable for v1")
    if TARGET_SURPRISING not in (forbidden or []):
        raise MLOfflineRankerExperimentError("split policy must hard-forbid surprising_or_useful for v1")
    if target not in allowed:
        raise MLOfflineRankerExperimentError(f"target {target!r} is not allowed by split policy")
    assertions = payload.get("policy_assertions")
    if not isinstance(assertions, Mapping):
        raise MLOfflineRankerExperimentError("split policy missing policy_assertions object")
    unsafe = {
        "permits_row_level_random_split": False,
        "permits_silent_conflict_resolution": False,
        "production_default_change_allowed": False,
        "requires_grouped_split_by_work": True,
        "surprising_or_useful_allowed_for_v1_split": False,
    }
    for key, expected in unsafe.items():
        if assertions.get(key) is not expected:
            raise MLOfflineRankerExperimentError(
                f"split policy assertion {key!r} must be {expected!r}, got {assertions.get(key)!r}"
            )
    return metadata


def _validate_label_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineRankerExperimentError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineRankerExperimentError("label dataset missing rows array")
    normalized: list[dict[str, Any]] = []
    row_ids: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLOfflineRankerExperimentError(f"label row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLOfflineRankerExperimentError(f"label row {idx} missing row_id")
        row_ids.append(row_id)
        normalized.append(row)
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLOfflineRankerExperimentError(f"label dataset contains duplicate row_id values: {dupes[:10]}")
    return normalized


def _validate_embedding_payload(
    payload: Mapping[str, Any],
    *,
    label_dataset_sha256: str,
) -> tuple[Mapping[str, Any], dict[str, dict[str, Any]]]:
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, Mapping):
        raise MLOfflineRankerExperimentError("embedding artifact missing metadata object")
    if metadata.get("artifact_type") != EMBEDDING_ARTIFACT_TYPE:
        raise MLOfflineRankerExperimentError(
            f"expected embedding metadata.artifact_type={EMBEDDING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDING_ARTIFACT_VERSION:
        raise MLOfflineRankerExperimentError(
            "expected embedding metadata.embedding_artifact_version="
            f"{EMBEDDING_ARTIFACT_VERSION!r}, got {metadata.get('embedding_artifact_version')!r}"
        )
    if metadata.get("source_label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineRankerExperimentError(
            "embedding artifact source_label_dataset_version is not compatible with ml-label-dataset-v8"
        )
    if metadata.get("source_label_dataset_sha256") != label_dataset_sha256:
        raise MLOfflineRankerExperimentError("embedding artifact source_label_dataset_sha256 does not match label dataset")
    expected_dim = metadata.get("embedding_dimensions")
    if not isinstance(expected_dim, int) or expected_dim <= 0:
        raise MLOfflineRankerExperimentError("embedding artifact missing positive metadata.embedding_dimensions")
    if not isinstance(rows, list):
        raise MLOfflineRankerExperimentError("embedding artifact missing rows array")

    by_id: dict[str, dict[str, Any]] = {}
    bad_status: list[str] = []
    bad_vector: list[str] = []
    row_ids: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLOfflineRankerExperimentError(f"embedding row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLOfflineRankerExperimentError(f"embedding row {idx} missing row_id")
        row_ids.append(row_id)
        if row.get("embedding_status") != "ok":
            bad_status.append(row_id)
        vector = row.get("embedding")
        if not isinstance(vector, list) or len(vector) != expected_dim:
            bad_vector.append(row_id)
        else:
            try:
                row["embedding"] = [float(value) for value in vector]
            except (TypeError, ValueError):
                bad_vector.append(row_id)
        by_id[row_id] = row
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLOfflineRankerExperimentError(f"embedding artifact contains duplicate row_id values: {dupes[:10]}")
    if bad_status:
        raise MLOfflineRankerExperimentError(f"embedding rows are not ok for row_id values: {bad_status[:20]}")
    if bad_vector:
        raise MLOfflineRankerExperimentError(f"embedding rows have invalid vector dimensions/values: {bad_vector[:20]}")
    return metadata, by_id


def _is_explicit_label_row(row: Mapping[str, Any]) -> bool:
    return row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()})


def _eligible_rows(
    *,
    label_rows: Sequence[dict[str, Any]],
    embeddings_by_id: Mapping[str, dict[str, Any]],
    target: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    excluded: Counter[str] = Counter()
    eligible: list[dict[str, Any]] = []
    missing_embedding: list[str] = []
    for row in label_rows:
        row_id = str(row.get("row_id") or "").strip()
        if str(row.get("split") or "").strip() != "audit_only":
            excluded["split_not_audit_only"] += 1
            continue
        if not _is_explicit_label_row(row):
            excluded["no_explicit_manual_label"] += 1
            continue
        if not isinstance(row.get(target), bool):
            excluded["target_not_boolean"] += 1
            continue
        canonical = canonical_openalex_work_id(row)
        if canonical is None:
            excluded["missing_canonical_work_id"] += 1
            continue
        embedding = embeddings_by_id.get(row_id)
        if embedding is None:
            missing_embedding.append(row_id)
            continue
        item = dict(row)
        item["_canonical_work_id"] = canonical
        item["_target_value"] = bool(row[target])
        item["_embedding"] = embedding
        eligible.append(item)
    if missing_embedding:
        raise MLOfflineRankerExperimentError(f"missing embeddings for eligible row_id values: {missing_embedding[:20]}")
    return sorted(eligible, key=lambda row: str(row["row_id"])), dict(sorted(excluded.items()))


def _hist(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(_bucket(row.get(field)) for row in rows).items()))


def _group_values(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[bool]]:
    groups: dict[str, list[bool]] = defaultdict(list)
    for row in rows:
        groups[str(row["_canonical_work_id"])].append(bool(row["_target_value"]))
    return dict(groups)


def _work_rollup_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups = _group_values(rows)
    any_values = [any(values) for values in groups.values()]
    majority_values: list[bool] = []
    tie_count = 0
    conflict_count = 0
    for values in groups.values():
        positives = sum(1 for value in values if value)
        negatives = len(values) - positives
        if positives and negatives:
            conflict_count += 1
        if positives > negatives:
            majority_values.append(True)
        elif negatives > positives:
            majority_values.append(False)
        else:
            tie_count += 1
    return {
        "any_positive": _bool_counts(any_values),
        "majority_vote_non_tie": _bool_counts(majority_values),
        "majority_vote_tie_group_count": tie_count,
        "conflicting_target_work_group_count": conflict_count,
        "group_count": len(groups),
    }


def _dataset_inventory(label_payload: Mapping[str, Any], eligible: Sequence[Mapping[str, Any]], excluded: Mapping[str, int]) -> dict[str, Any]:
    label_rows = label_payload.get("rows") if isinstance(label_payload.get("rows"), list) else []
    groups = _group_values(eligible)
    duplicate_groups = {work_id: values for work_id, values in groups.items() if len(values) > 1}
    observation_values = [bool(row["_target_value"]) for row in eligible]
    metadata = label_payload.get("metadata") if isinstance(label_payload.get("metadata"), Mapping) else {}
    duplicate = metadata.get("duplicate_paper_id_report") if isinstance(metadata.get("duplicate_paper_id_report"), Mapping) else {}
    raw_conflict = metadata.get("conflicting_label_report") if isinstance(metadata.get("conflicting_label_report"), Mapping) else {}
    derived_conflict = (
        metadata.get("derived_target_conflict_report")
        if isinstance(metadata.get("derived_target_conflict_report"), Mapping)
        else {}
    )
    return {
        "total_label_rows": len(label_rows),
        "eligible_observations": len(eligible),
        "excluded_rows_by_reason": dict(sorted(excluded.items())),
        "unique_eligible_canonical_work_count": len(groups),
        "duplicate_eligible_work_group_count": len(duplicate_groups),
        "duplicate_observation_pressure": len(eligible) - len(groups),
        "review_pool_variant_counts": _hist(eligible, "review_pool_variant"),
        "family_counts": _hist(eligible, "family"),
        "target_class_counts": {
            "observation_level": _bool_counts(observation_values),
            "work_group_reporting_level": _work_rollup_counts(eligible),
        },
        "conflict_counts": {
            "duplicate_paper_id_count": int(duplicate.get("duplicate_paper_id_count") or 0),
            "raw_conflicting_label_count": int(raw_conflict.get("conflicting_label_count") or 0),
            "derived_target_conflict_count": int(derived_conflict.get("derived_target_conflict_count") or 0),
        },
    }


def _group_label_for_stratification(rows: Sequence[Mapping[str, Any]]) -> dict[str, bool]:
    return {work_id: any(values) for work_id, values in _group_values(rows).items()}


def _effective_cv_folds(rows: Sequence[Mapping[str, Any]], requested: int) -> tuple[int, dict[str, int]]:
    group_labels = _group_label_for_stratification(rows)
    counts = _bool_counts(list(group_labels.values()))
    if counts["positive"] < 2 or counts["negative"] < 2:
        raise MLOfflineRankerExperimentError(
            "not enough stratified groups: v1 requires at least 2 positive and 2 negative canonical work groups"
        )
    effective = max(2, min(int(requested), counts["positive"], counts["negative"]))
    return effective, counts


def _fallback_grouped_stratified_splits(
    rows: Sequence[Mapping[str, Any]],
    *,
    n_splits: int,
    random_seed: int,
) -> list[tuple[list[int], list[int]]]:
    import random

    group_labels = _group_label_for_stratification(rows)
    groups_by_label = {True: [], False: []}
    for work_id, label in sorted(group_labels.items()):
        groups_by_label[label].append(work_id)
    rng = random.Random(random_seed)
    for group_list in groups_by_label.values():
        rng.shuffle(group_list)
    fold_groups: list[set[str]] = [set() for _ in range(n_splits)]
    for label in (True, False):
        for idx, work_id in enumerate(groups_by_label[label]):
            fold_groups[idx % n_splits].add(work_id)
    splits: list[tuple[list[int], list[int]]] = []
    for fold_group in fold_groups:
        test_idx = [idx for idx, row in enumerate(rows) if str(row["_canonical_work_id"]) in fold_group]
        train_idx = [idx for idx, row in enumerate(rows) if str(row["_canonical_work_id"]) not in fold_group]
        splits.append((train_idx, test_idx))
    return splits


def _build_grouped_splits(
    rows: Sequence[Mapping[str, Any]],
    *,
    n_splits: int,
    random_seed: int,
) -> tuple[str, list[tuple[list[int], list[int]]]]:
    try:
        from sklearn.model_selection import StratifiedGroupKFold

        splitter = StratifiedGroupKFold(n_splits=n_splits, shuffle=True, random_state=random_seed)
        y = [1 if row["_target_value"] else 0 for row in rows]
        groups = [str(row["_canonical_work_id"]) for row in rows]
        splits = [(list(train), list(test)) for train, test in splitter.split([[0]] * len(rows), y, groups)]
        return "sklearn.StratifiedGroupKFold", splits
    except ImportError:
        return (
            "deterministic_grouped_stratified_fallback",
            _fallback_grouped_stratified_splits(rows, n_splits=n_splits, random_seed=random_seed),
        )


def _embedding_x(rows: Sequence[Mapping[str, Any]]) -> list[list[float]]:
    return [list(row["_embedding"]["embedding"]) for row in rows]


def _fit_predict_embedding_logistic(
    *,
    train_rows: Sequence[Mapping[str, Any]],
    eval_rows: Sequence[Mapping[str, Any]],
    random_seed: int,
) -> tuple[list[bool], list[float], dict[str, Any]]:
    y_train = [bool(row["_target_value"]) for row in train_rows]
    model = Pipeline(
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
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="'penalty' was deprecated", category=FutureWarning)
        model.fit(_embedding_x(train_rows), y_train)
    pred = [bool(value) for value in model.predict(_embedding_x(eval_rows))]
    proba = model.predict_proba(_embedding_x(eval_rows))
    class_order = list(model.named_steps["classifier"].classes_)
    true_index = class_order.index(True)
    scores = [float(row[true_index]) for row in proba]
    classifier = model.named_steps["classifier"]
    coefficients = [float(value) for value in classifier.coef_[0].tolist()]
    intercept = float(classifier.intercept_[0])
    return pred, scores, {
        "coefficients_standardized_space": coefficients,
        "intercept_standardized_space": intercept,
    }


def _majority_class_baseline(
    *,
    train_rows: Sequence[Mapping[str, Any]],
    eval_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[bool], None, dict[str, Any]]:
    y_train = [bool(row["_target_value"]) for row in train_rows]
    model = DummyClassifier(strategy="most_frequent")
    model.fit([[0]] * len(train_rows), y_train)
    pred = [bool(value) for value in model.predict([[0]] * len(eval_rows))]
    counts = _bool_counts(y_train)
    return pred, None, {
        "train_majority_class": bool(pred[0]) if pred else counts["positive"] >= counts["negative"],
        "train_positive_count": counts["positive"],
        "train_negative_count": counts["negative"],
    }


def _prevalence_probability_baseline(
    *,
    train_rows: Sequence[Mapping[str, Any]],
    eval_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[bool], list[float], dict[str, Any]]:
    prevalence = sum(1 for row in train_rows if row["_target_value"]) / len(train_rows)
    pred = [prevalence >= 0.5 for _ in eval_rows]
    return pred, [float(prevalence)] * len(eval_rows), {"train_positive_prevalence": float(prevalence)}


def _work_reporting(
    *,
    eval_rows: Sequence[Mapping[str, Any]],
    pred: Sequence[bool],
    scores: Sequence[float] | None,
) -> dict[str, Any]:
    grouped: dict[str, dict[str, list[Any]]] = defaultdict(lambda: {"true": [], "pred": [], "scores": []})
    for row, y_pred, score in zip(eval_rows, pred, scores or [None] * len(eval_rows), strict=True):
        group = grouped[str(row["_canonical_work_id"])]
        group["true"].append(bool(row["_target_value"]))
        group["pred"].append(bool(y_pred))
        if score is not None:
            group["scores"].append(float(score))

    any_true: list[bool] = []
    any_pred: list[bool] = []
    any_scores: list[float] = []
    majority_true: list[bool] = []
    majority_pred: list[bool] = []
    majority_scores: list[float] = []
    majority_ties = 0
    conflict_groups = 0
    for values in grouped.values():
        true_values = [bool(v) for v in values["true"]]
        pred_values = [bool(v) for v in values["pred"]]
        score_values = [float(v) for v in values["scores"]]
        positives = sum(1 for value in true_values if value)
        negatives = len(true_values) - positives
        if positives and negatives:
            conflict_groups += 1
        any_true.append(any(true_values))
        any_pred.append(any(pred_values))
        if score_values:
            any_scores.append(float(mean(score_values)))
        if positives > negatives:
            majority_true.append(True)
        elif negatives > positives:
            majority_true.append(False)
        else:
            majority_ties += 1
            continue
        majority_pred.append(sum(1 for value in pred_values if value) >= (len(pred_values) / 2))
        if score_values:
            majority_scores.append(float(mean(score_values)))

    any_score_arg = any_scores if len(any_scores) == len(any_true) else None
    majority_score_arg = majority_scores if len(majority_scores) == len(majority_true) else None
    return {
        "aggregation_note": "Reporting only: model fits observation labels; work summaries use mean predicted probability and any-positive/all-negative or majority-vote labels.",
        "conflicting_target_work_group_count": conflict_groups,
        "any_positive": {
            "work_group_count": len(any_true),
            **_classification_metrics(y_true=any_true, y_pred=any_pred, scores=any_score_arg),
        },
        "majority_vote_non_tie": {
            "work_group_count": len(majority_true),
            "excluded_tie_work_group_count": majority_ties,
            **(
                _classification_metrics(y_true=majority_true, y_pred=majority_pred, scores=majority_score_arg)
                if majority_true
                else {
                    "accuracy": None,
                    "balanced_accuracy": None,
                    "macro_f1": None,
                    "confusion": {"tn": 0, "fp": 0, "fn": 0, "tp": 0},
                    "roc_auc": None,
                    "roc_auc_skip_reason": "no non-tie work groups",
                    "average_precision": None,
                    "average_precision_skip_reason": "no non-tie work groups",
                }
            ),
        },
    }


def _evaluate_model(
    *,
    model_name: str,
    train_rows: Sequence[Mapping[str, Any]],
    eval_rows: Sequence[Mapping[str, Any]],
    random_seed: int,
) -> dict[str, Any]:
    if model_name == "embedding_logistic":
        pred, scores, model_details = _fit_predict_embedding_logistic(
            train_rows=train_rows,
            eval_rows=eval_rows,
            random_seed=random_seed,
        )
    elif model_name == "majority_class":
        pred, scores, model_details = _majority_class_baseline(train_rows=train_rows, eval_rows=eval_rows)
    elif model_name == "prevalence_probability":
        pred, scores, model_details = _prevalence_probability_baseline(train_rows=train_rows, eval_rows=eval_rows)
    else:
        raise ValueError(model_name)

    y_true = [bool(row["_target_value"]) for row in eval_rows]
    metrics = _classification_metrics(y_true=y_true, y_pred=pred, scores=scores)
    return {
        "metrics": metrics,
        "work_group_reporting_metrics": _work_reporting(eval_rows=eval_rows, pred=pred, scores=scores),
        "model_details": model_details,
    }


def _row_count_block(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    y = [bool(row["_target_value"]) for row in rows]
    return {
        "observation_count": len(rows),
        "unique_work_count": len({str(row["_canonical_work_id"]) for row in rows}),
        "positive_observation_count": sum(1 for value in y if value),
        "negative_observation_count": sum(1 for value in y if not value),
        "work_group_reporting_level": _work_rollup_counts(rows),
    }


def _summarize_model_folds(folds: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    evaluated = [fold for fold in folds if not fold.get("skipped")]
    skipped = [fold for fold in folds if fold.get("skipped")]
    metric_names = ("accuracy", "balanced_accuracy", "macro_f1", "roc_auc", "average_precision")
    mean_std: dict[str, Any] = {}
    for name in metric_names:
        values = [fold["metrics"].get(name) for fold in evaluated]
        nums = [float(value) for value in values if isinstance(value, (int, float)) and not math.isnan(float(value))]
        mean_std[name] = {
            "mean": float(mean(nums)) if nums else None,
            "std": float(pstdev(nums)) if len(nums) > 1 else 0.0 if nums else None,
            "n": len(nums),
        }
    confusion = Counter()
    for fold in evaluated:
        confusion.update(fold["metrics"].get("confusion", {}))
    return {
        "folds_evaluated": len(evaluated),
        "folds_skipped": len(skipped),
        "skipped_reasons": dict(sorted(Counter(str(fold.get("skip_reason")) for fold in skipped).items())),
        "observation_metrics_mean_std": mean_std,
        "summed_confusion": {key: int(confusion[key]) for key in ("tn", "fp", "fn", "tp")},
    }


def _evaluate_cv(
    *,
    rows: Sequence[dict[str, Any]],
    splits: Sequence[tuple[list[int], list[int]]],
    random_seed: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    model_outputs: dict[str, list[dict[str, Any]]] = {name: [] for name in MODEL_NAMES}
    leakage_folds: list[dict[str, Any]] = []
    for fold_index, (train_idx, eval_idx) in enumerate(splits, start=1):
        train_rows = [rows[idx] for idx in train_idx]
        eval_rows = [rows[idx] for idx in eval_idx]
        train_work_ids = {str(row["_canonical_work_id"]) for row in train_rows}
        eval_work_ids = {str(row["_canonical_work_id"]) for row in eval_rows}
        overlap = sorted(train_work_ids & eval_work_ids)
        leakage_folds.append(
            {
                "fold_index": fold_index,
                "train_unique_work_count": len(train_work_ids),
                "eval_unique_work_count": len(eval_work_ids),
                "leakage_work_overlap_count": len(overlap),
            }
        )
        train_counts = _bool_counts([bool(row["_target_value"]) for row in train_rows])
        eval_counts = _bool_counts([bool(row["_target_value"]) for row in eval_rows])
        skip_reason = None
        if len(train_rows) == 0 or len(eval_rows) == 0:
            skip_reason = "empty train or eval fold"
        elif train_counts["positive"] == 0 or train_counts["negative"] == 0:
            skip_reason = "train fold lacks both classes"

        for model_name in MODEL_NAMES:
            base = {
                "fold_index": fold_index,
                "skipped": skip_reason is not None,
                "skip_reason": skip_reason,
                "train": _row_count_block(train_rows),
                "eval": _row_count_block(eval_rows),
                "leakage_work_overlap_count": len(overlap),
            }
            if skip_reason is None:
                base.update(_evaluate_model(model_name=model_name, train_rows=train_rows, eval_rows=eval_rows, random_seed=random_seed))
            model_outputs[model_name].append(base)

    models: dict[str, Any] = {}
    for model_name, folds in model_outputs.items():
        models[model_name] = {
            "pipeline_steps": ["scaler", "classifier"] if model_name == "embedding_logistic" else [],
            "per_fold": folds,
            "aggregate": _summarize_model_folds(folds),
        }
    return models, leakage_folds


def build_ml_offline_ranker_experiment_payload(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    target: str = TARGET_GOOD,
    random_seed: int | None = None,
    cv_folds: int = 5,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if target != TARGET_GOOD:
        raise MLOfflineRankerExperimentError("ml-offline-ranker-experiment-v1 supports only good_or_acceptable")
    if cv_folds < 2:
        raise MLOfflineRankerExperimentError("--cv-folds must be at least 2")
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    emb_path = Path(embeddings_path).resolve()
    label_payload = _load_json_object(label_path)
    policy_payload = _load_json_object(policy_path)
    emb_payload = _load_json_object(emb_path)

    label_rows = _validate_label_payload(label_payload)
    policy_metadata = _validate_split_policy(policy_payload, target=target)
    label_sha = sha256_file(label_path)
    embedding_metadata, embeddings_by_id = _validate_embedding_payload(emb_payload, label_dataset_sha256=label_sha)

    seed = int(random_seed) if random_seed is not None else int(
        policy_payload.get("randomness_policy", {}).get("recommended_default_seed", 0)
    )
    eligible, excluded = _eligible_rows(label_rows=label_rows, embeddings_by_id=embeddings_by_id, target=target)
    effective_folds, group_class_counts = _effective_cv_folds(eligible, int(cv_folds))
    splitter_name, splits = _build_grouped_splits(eligible, n_splits=effective_folds, random_seed=seed)
    models, leakage_folds = _evaluate_cv(rows=eligible, splits=splits, random_seed=seed)
    leakage_passed = all(fold["leakage_work_overlap_count"] == 0 for fold in leakage_folds)

    inventory = _dataset_inventory(label_payload, eligible, excluded)
    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("embeddings", emb_path, repo_root=root),
    ]
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "experiment_version": EXPERIMENT_VERSION,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "target": target,
        "random_seed": seed,
        "requested_cv_folds": int(cv_folds),
        "effective_cv_folds": effective_folds,
        "group_splitter": splitter_name,
        "sklearn_version": sklearn.__version__,
        "label_dataset_version": label_payload.get("dataset_version"),
        "split_policy_version": policy_metadata.get("policy_version"),
        "embedding_artifact_version": embedding_metadata.get("embedding_artifact_version"),
        "embedding_dimensions": embedding_metadata.get("embedding_dimensions"),
        "group_level_stratification_class_counts": group_class_counts,
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "policy_compliance": {
            "allowed_target_verified": True,
            "forbidden_targets_verified": True,
            "grouped_split_required": True,
            "grouped_split_used": True,
            "row_level_random_split_used": False,
            "production_artifact_written": False,
            "leakage_checks_passed": leakage_passed,
            "review_pool_variant_policy": policy_payload.get("eligibility_rules", {}).get("review_pool_variant_handling"),
        },
        "dataset_inventory": inventory,
        "models": models,
        "leakage_report": {
            "per_fold": leakage_folds,
            "global_leakage_work_overlap_count": sum(fold["leakage_work_overlap_count"] for fold in leakage_folds),
            "global_zero_assertion": leakage_passed,
        },
        "interpretation": {
            "summary": (
                "Offline diagnostic only: grouped cross-validation over frozen text embeddings tests whether the "
                "good_or_acceptable audit labels are learnable under the v1 split policy."
            ),
            "not_claimed": [
                "validation",
                "production ranking evidence",
                "shadow-scoring readiness",
                "live recommender quality",
            ],
            "caveat": (
                "Grouped CV reduces same-paper leakage but does not remove single-reviewer, rubric, source-pool, "
                "or product-candidate-pool limits. Production remains blocked pending explicit metric gates and shadow plans."
            ),
        },
    }


def _fmt_metric(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


def markdown_from_ml_offline_ranker_experiment(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    inventory = payload["dataset_inventory"]
    leakage = payload["leakage_report"]
    lines = [
        f"# Offline Ranker Experiment ({metadata['experiment_version']})",
        "",
        "## Eligibility Summary",
        "",
        f"- **Target:** `{metadata['target']}`",
        f"- **Eligible observations:** {inventory['eligible_observations']}",
        f"- **Unique canonical works:** {inventory['unique_eligible_canonical_work_count']}",
        f"- **Duplicate observation pressure:** {inventory['duplicate_observation_pressure']}",
        f"- **Effective grouped CV folds:** {metadata['effective_cv_folds']}",
        "",
        "## Class Balance",
        "",
        "| Level | Positive | Negative |",
        "| --- | ---: | ---: |",
    ]
    obs = inventory["target_class_counts"]["observation_level"]
    any_work = inventory["target_class_counts"]["work_group_reporting_level"]["any_positive"]
    lines.append(f"| Observation | {obs['positive']} | {obs['negative']} |")
    lines.append(f"| Work any-positive | {any_work['positive']} | {any_work['negative']} |")

    lines.extend(
        [
            "",
            "## Model Summary",
            "",
            "| Model | Balanced Accuracy Mean | ROC-AUC Mean | Average Precision Mean | Macro F1 Mean | TN | FP | FN | TP |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for model_name, model in payload["models"].items():
        agg = model["aggregate"]
        ms = agg["observation_metrics_mean_std"]
        conf = agg["summed_confusion"]
        lines.append(
            f"| `{model_name}` | {_fmt_metric(ms['balanced_accuracy']['mean'])} | {_fmt_metric(ms['roc_auc']['mean'])} | "
            f"{_fmt_metric(ms['average_precision']['mean'])} | {_fmt_metric(ms['macro_f1']['mean'])} | "
            f"{conf['tn']} | {conf['fp']} | {conf['fn']} | {conf['tp']} |"
        )

    lines.extend(
        [
            "",
            "## Leakage",
            "",
            f"- **Global work-overlap count:** {leakage['global_leakage_work_overlap_count']}",
            f"- **Leakage checks passed:** {leakage['global_zero_assertion']}",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.extend(
        [
            "",
            "## Not Production",
            "",
            "This artifact is not validation, not shadow scoring, and not production ranking evidence. No model file, API behavior, web behavior, or ranking default is changed.",
            "",
            "## Next Step",
            "",
            "If metrics are credible after review, define `ml-offline-metric-gates-v1`; otherwise prioritize labeling and rubric work. No shadow scoring should start without metric gates.",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_offline_ranker_experiment(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    target: str = TARGET_GOOD,
    random_seed: int | None = None,
    cv_folds: int = 5,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_ranker_experiment_payload(
        label_dataset_path=label_dataset_path,
        split_policy_path=split_policy_path,
        embeddings_path=embeddings_path,
        target=target,
        random_seed=random_seed,
        cv_folds=cv_folds,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_offline_ranker_experiment(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "EXPERIMENT_VERSION",
    "MLOfflineRankerExperimentError",
    "build_ml_offline_ranker_experiment_payload",
    "markdown_from_ml_offline_ranker_experiment",
    "write_ml_offline_ranker_experiment",
]
