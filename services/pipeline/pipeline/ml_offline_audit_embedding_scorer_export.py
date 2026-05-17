"""Offline full-fit audit embedding scorer export.

This command fits a frozen JSON-serialized logistic scorer on the eligible
audit-labeled embedding corpus. It does not run CV, score product candidates,
write binary model files, access databases, call external services, or change
production behavior.
"""

from __future__ import annotations

import json
import math
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import sklearn
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

ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
SCORER_VERSION = "ml-offline-audit-embedding-scorer-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
EMBEDDING_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-labeled-text-embeddings-v3"
PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
PRODUCT_CANDIDATE_GATES_VERSION = "ml-offline-production-candidate-metric-gates-v1"
PRODUCT_CANDIDATE_GATES_NEXT_STAGE = "create_frozen_audit_embedding_scorer_export_v1"
RANKER_ARTIFACT_TYPE = "ml_offline_ranker_experiment"
RANKER_VERSION = "ml-offline-ranker-experiment-v1"
IN_SAMPLE_LABEL = "IN-SAMPLE FULL-FIT ONLY — NOT VALIDATION"

CAVEATS = (
    "Not validation.",
    "Full-fit on audit-labeled corpus only; not product-candidate training.",
    "In-sample training metrics are diagnostic only.",
    "Grouped CV leakage controls from ranker experiment do not apply to this full-fit artifact.",
    "Does not authorize shadow scoring or production default.",
    "Heuristic final_score and learned audit scorer are separate evidence lines.",
    "No ranking/API/web changes.",
    "JSON scorer is an offline audit artifact, not a production model artifact.",
)


class MLOfflineAuditEmbeddingScorerExportError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineAuditEmbeddingScorerExportError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineAuditEmbeddingScorerExportError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLOfflineAuditEmbeddingScorerExportError(f"{name} JSON missing metadata object")
    return metadata


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLOfflineAuditEmbeddingScorerExportError(f"Input {name} does not exist: {path}")
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
    if len(set(y_true)) == 2 and scores is not None and len(scores) > 0:
        out["roc_auc"] = float(roc_auc_score(y_true, scores))
        out["roc_auc_skip_reason"] = None
        out["average_precision"] = float(average_precision_score(y_true, scores))
        out["average_precision_skip_reason"] = None
    else:
        reason = "requires rows with both classes and probabilistic scores"
        out["roc_auc"] = None
        out["roc_auc_skip_reason"] = reason
        out["average_precision"] = None
        out["average_precision_skip_reason"] = reason
    return out


def _validate_label_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineAuditEmbeddingScorerExportError("label dataset missing rows array")
    normalized: list[dict[str, Any]] = []
    row_ids: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLOfflineAuditEmbeddingScorerExportError(f"label row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLOfflineAuditEmbeddingScorerExportError(f"label row {idx} missing row_id")
        row_ids.append(row_id)
        normalized.append(row)
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLOfflineAuditEmbeddingScorerExportError(f"label dataset contains duplicate row_id values: {dupes[:10]}")
    return normalized


def _validate_split_policy(payload: Mapping[str, Any], *, target: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    allowed = payload.get("allowed_targets_for_v1_split")
    forbidden = payload.get("forbidden_targets")
    if allowed != [TARGET_GOOD]:
        raise MLOfflineAuditEmbeddingScorerExportError("split policy allowed_targets_for_v1_split must be ['good_or_acceptable']")
    if TARGET_SURPRISING not in (forbidden or []):
        raise MLOfflineAuditEmbeddingScorerExportError("split policy forbidden_targets must include surprising_or_useful")
    if target != TARGET_GOOD or target not in allowed:
        raise MLOfflineAuditEmbeddingScorerExportError("ml-offline-audit-embedding-scorer-v1 supports only good_or_acceptable")
    assertions = payload.get("policy_assertions")
    if not isinstance(assertions, Mapping):
        raise MLOfflineAuditEmbeddingScorerExportError("split policy missing policy_assertions object")
    safe_assertions = {
        "permits_row_level_random_split": False,
        "permits_silent_conflict_resolution": False,
        "production_default_change_allowed": False,
        "requires_grouped_split_by_work": True,
        "surprising_or_useful_allowed_for_v1_split": False,
    }
    for key, expected in safe_assertions.items():
        if assertions.get(key) is not expected:
            raise MLOfflineAuditEmbeddingScorerExportError(
                f"split policy assertion {key!r} must be {expected!r}, got {assertions.get(key)!r}"
            )
    return metadata


def _validate_embedding_payload(
    payload: Mapping[str, Any],
    *,
    label_dataset_sha256: str,
) -> tuple[Mapping[str, Any], dict[str, dict[str, Any]]]:
    metadata = _metadata(payload, name="embeddings")
    rows = payload.get("rows")
    if metadata.get("artifact_type") != EMBEDDING_ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected embeddings metadata.artifact_type={EMBEDDING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDING_ARTIFACT_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected embeddings metadata.embedding_artifact_version={EMBEDDING_ARTIFACT_VERSION!r}, "
            f"got {metadata.get('embedding_artifact_version')!r}"
        )
    if metadata.get("source_label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "embeddings source_label_dataset_version must match ml-label-dataset-v8"
        )
    if metadata.get("source_label_dataset_sha256") != label_dataset_sha256:
        raise MLOfflineAuditEmbeddingScorerExportError("embeddings source_label_dataset_sha256 must match supplied label dataset")
    expected_dim = metadata.get("embedding_dimensions")
    if not isinstance(expected_dim, int) or expected_dim <= 0:
        raise MLOfflineAuditEmbeddingScorerExportError("embeddings metadata.embedding_dimensions must be a positive integer")
    if not isinstance(rows, list):
        raise MLOfflineAuditEmbeddingScorerExportError("embeddings missing rows array")

    by_id: dict[str, dict[str, Any]] = {}
    row_ids: list[str] = []
    bad_status: list[str] = []
    bad_vector: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLOfflineAuditEmbeddingScorerExportError(f"embedding row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLOfflineAuditEmbeddingScorerExportError(f"embedding row {idx} missing row_id")
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
        raise MLOfflineAuditEmbeddingScorerExportError(f"embeddings contain duplicate row_id values: {dupes[:10]}")
    if bad_status:
        raise MLOfflineAuditEmbeddingScorerExportError(f"embedding rows are not ok for row_id values: {bad_status[:20]}")
    if bad_vector:
        raise MLOfflineAuditEmbeddingScorerExportError(f"embedding rows have invalid vector dimensions/values: {bad_vector[:20]}")
    return metadata, by_id


def _validate_product_candidate_metric_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates")
    if metadata.get("artifact_type") != PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "expected production-candidate metric gates metadata.artifact_type="
            f"{PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != PRODUCT_CANDIDATE_GATES_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected production-candidate metric gates metadata.gates_version={PRODUCT_CANDIDATE_GATES_VERSION!r}, "
            f"got {metadata.get('gates_version')!r}"
        )
    if payload.get("product_candidate_heuristic_gates_passed") is not True:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "production-candidate metric gates product_candidate_heuristic_gates_passed must be true"
        )
    if payload.get("recommended_next_stage") != PRODUCT_CANDIDATE_GATES_NEXT_STAGE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "production-candidate metric gates recommended_next_stage must be "
            f"{PRODUCT_CANDIDATE_GATES_NEXT_STAGE}"
        )
    if payload.get("shadow_scoring_allowed") is not False:
        raise MLOfflineAuditEmbeddingScorerExportError("production-candidate metric gates shadow_scoring_allowed must be false")
    if payload.get("production_default_allowed") is not False:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "production-candidate metric gates production_default_allowed must be false"
        )
    return metadata


def _validate_ranker_experiment(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="ranker-experiment")
    if metadata.get("artifact_type") != RANKER_ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected ranker metadata.artifact_type={RANKER_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != RANKER_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected ranker metadata.experiment_version={RANKER_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("target") != TARGET_GOOD:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected ranker metadata.target={TARGET_GOOD!r}, got {metadata.get('target')!r}"
        )
    return metadata


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
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"missing embeddings for eligible row_id values: {missing_embedding[:20]}"
        )
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


def _duplicate_conflict_rollups(label_payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = label_payload.get("metadata") if isinstance(label_payload.get("metadata"), Mapping) else {}
    duplicate = metadata.get("duplicate_paper_id_report") if isinstance(metadata.get("duplicate_paper_id_report"), Mapping) else {}
    raw_conflict = metadata.get("conflicting_label_report") if isinstance(metadata.get("conflicting_label_report"), Mapping) else {}
    derived_conflict = (
        metadata.get("derived_target_conflict_report")
        if isinstance(metadata.get("derived_target_conflict_report"), Mapping)
        else {}
    )
    return {
        "duplicate_paper_id_count": int(duplicate.get("duplicate_paper_id_count") or 0),
        "raw_conflicting_label_count": int(raw_conflict.get("conflicting_label_count") or 0),
        "derived_target_conflict_count": int(derived_conflict.get("derived_target_conflict_count") or 0),
    }


def _training_inventory(
    *,
    label_payload: Mapping[str, Any],
    eligible: Sequence[Mapping[str, Any]],
    excluded: Mapping[str, int],
) -> dict[str, Any]:
    groups = _group_values(eligible)
    observation_values = [bool(row["_target_value"]) for row in eligible]
    work_rollup = _work_rollup_counts(eligible)
    return {
        "total_label_rows": len(label_payload.get("rows") if isinstance(label_payload.get("rows"), list) else []),
        "eligible_observation_count": len(eligible),
        "unique_canonical_work_count": len(groups),
        "duplicate_observation_pressure": len(eligible) - len(groups),
        "positive_observation_count": sum(1 for value in observation_values if value),
        "negative_observation_count": sum(1 for value in observation_values if not value),
        "positive_work_count_any_positive": work_rollup["any_positive"]["positive"],
        "negative_work_count_any_positive": work_rollup["any_positive"]["negative"],
        "conflicting_target_work_group_count": work_rollup["conflicting_target_work_group_count"],
        "duplicate_eligible_work_group_count": sum(1 for values in groups.values() if len(values) > 1),
        "excluded_rows_by_reason": dict(sorted(excluded.items())),
        "review_pool_variant_counts": _hist(eligible, "review_pool_variant"),
        "family_counts": _hist(eligible, "family"),
        "target_class_counts": {
            "observation_level": _bool_counts(observation_values),
            "work_group_reporting_level": work_rollup,
        },
        "duplicate_conflict_rollups": _duplicate_conflict_rollups(label_payload),
    }


def _embedding_x(rows: Sequence[Mapping[str, Any]]) -> list[list[float]]:
    return [list(row["_embedding"]["embedding"]) for row in rows]


def _fit_full_audit_logistic(
    *,
    rows: Sequence[Mapping[str, Any]],
    random_seed: int,
) -> tuple[Pipeline, list[bool], list[float]]:
    if len(rows) < 2:
        raise MLOfflineAuditEmbeddingScorerExportError("at least two eligible observations are required")
    y = [bool(row["_target_value"]) for row in rows]
    counts = _bool_counts(y)
    if counts["positive"] == 0 or counts["negative"] == 0:
        raise MLOfflineAuditEmbeddingScorerExportError("eligible observations must contain both positive and negative labels")
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
        model.fit(_embedding_x(rows), y)
    pred = [bool(value) for value in model.predict(_embedding_x(rows))]
    proba = model.predict_proba(_embedding_x(rows))
    class_order = [bool(value) for value in model.named_steps["classifier"].classes_.tolist()]
    true_index = class_order.index(True)
    scores = [float(row[true_index]) for row in proba]
    return model, pred, scores


def _export_scorer(model: Pipeline) -> dict[str, Any]:
    scaler = model.named_steps["scaler"]
    classifier = model.named_steps["classifier"]
    classes = [bool(value) for value in classifier.classes_.tolist()]
    feature_count = int(getattr(classifier, "n_features_in_", len(classifier.coef_[0])))
    return {
        "pipeline_steps": ["scaler", "classifier"],
        "scaler": {
            "with_mean": True,
            "feature_count": feature_count,
            "mean": [float(value) for value in scaler.mean_.tolist()],
            "scale": [float(value) for value in scaler.scale_.tolist()],
            "zero_variance_note": "sklearn StandardScaler scale_ uses 1 for zero-variance dimensions",
        },
        "classifier": {
            "solver": "lbfgs",
            "penalty": "l2",
            "max_iter": 5000,
            "classes": classes,
            "coefficients_standardized_space": [float(value) for value in classifier.coef_[0].tolist()],
            "intercept_standardized_space": float(classifier.intercept_[0]),
        },
        "apply_instructions": {
            "standardize": "z = (x - mean) / scale per dimension",
            "logit_true": "dot(coefficients_standardized_space, z) + intercept_standardized_space",
            "probability_true": "sigmoid(logit_true)",
            "boolean_prediction": "probability_true >= 0.5",
        },
        "row_identity_key": "row_id",
    }


def _reference_cv_baseline(
    *,
    ranker_payload: Mapping[str, Any] | None,
    ranker_input: Mapping[str, str] | None,
) -> dict[str, Any]:
    if ranker_payload is None or ranker_input is None:
        return {
            "source_input": None,
            "embedding_logistic_aggregate": None,
            "reason": "No ranker experiment supplied.",
        }
    aggregate = ranker_payload.get("models", {}).get("embedding_logistic", {}).get("aggregate")
    if not isinstance(aggregate, Mapping):
        return {
            "source_input": dict(ranker_input),
            "embedding_logistic_aggregate": None,
            "reason": "Ranker experiment did not contain models.embedding_logistic.aggregate.",
        }
    return {
        "source_input": dict(ranker_input),
        "embedding_logistic_aggregate": {
            "folds_evaluated": aggregate.get("folds_evaluated"),
            "folds_skipped": aggregate.get("folds_skipped"),
            "skipped_reasons": aggregate.get("skipped_reasons"),
            "observation_metrics_mean_std": aggregate.get("observation_metrics_mean_std"),
            "summed_confusion": aggregate.get("summed_confusion"),
        },
        "reason": None,
        "coefficient_reuse": "not_used; per-fold coefficients are not reused or averaged",
    }


def _copy_embedding_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for source_key, output_key in (
        ("embedding_model", "embedding_model"),
        ("embedding_provider", "embedding_provider"),
        ("embedding_dimensions", "embedding_dimensions"),
    ):
        if source_key in metadata:
            copied[output_key] = metadata[source_key]
    return copied


def score_audit_embedding_probability(embedding_vector: Sequence[float], scorer_payload: Mapping[str, Any]) -> float:
    scorer = scorer_payload.get("scorer")
    if not isinstance(scorer, Mapping):
        raise MLOfflineAuditEmbeddingScorerExportError("scorer_payload missing scorer object")
    scaler = scorer.get("scaler")
    classifier = scorer.get("classifier")
    if not isinstance(scaler, Mapping) or not isinstance(classifier, Mapping):
        raise MLOfflineAuditEmbeddingScorerExportError("scorer payload missing scaler/classifier objects")
    mean = scaler.get("mean")
    scale = scaler.get("scale")
    coef = classifier.get("coefficients_standardized_space")
    intercept = classifier.get("intercept_standardized_space")
    if not isinstance(mean, list) or not isinstance(scale, list) or not isinstance(coef, list):
        raise MLOfflineAuditEmbeddingScorerExportError("scorer mean/scale/coefficients must be arrays")
    expected = int(scaler.get("feature_count") or len(mean))
    if len(embedding_vector) != expected:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"embedding vector length {len(embedding_vector)} does not match scorer feature_count {expected}"
        )
    if len(mean) != expected or len(scale) != expected or len(coef) != expected:
        raise MLOfflineAuditEmbeddingScorerExportError("scorer feature arrays do not match feature_count")
    if not isinstance(intercept, (int, float)):
        raise MLOfflineAuditEmbeddingScorerExportError("scorer intercept must be numeric")
    try:
        z = [
            (float(value) - float(m)) / float(s)
            for value, m, s in zip(embedding_vector, mean, scale, strict=True)
        ]
        logit = sum(float(c) * value for c, value in zip(coef, z, strict=True)) + float(intercept)
    except (TypeError, ValueError, ZeroDivisionError) as exc:
        raise MLOfflineAuditEmbeddingScorerExportError(f"failed to score embedding vector: {exc}") from exc
    if logit >= 0:
        return float(1.0 / (1.0 + math.exp(-logit)))
    exp_logit = math.exp(logit)
    return float(exp_logit / (1.0 + exp_logit))


def build_ml_offline_audit_embedding_scorer_export_payload(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    production_candidate_metric_gates_path: Path,
    ranker_experiment_path: Path | None = None,
    target: str = TARGET_GOOD,
    random_seed: int | None = None,
    scorer_version: str = SCORER_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if target != TARGET_GOOD:
        raise MLOfflineAuditEmbeddingScorerExportError("ml-offline-audit-embedding-scorer-v1 supports only good_or_acceptable")
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    emb_path = Path(embeddings_path).resolve()
    gates_path = Path(production_candidate_metric_gates_path).resolve()
    ranker_path = Path(ranker_experiment_path).resolve() if ranker_experiment_path is not None else None

    label_payload = _load_json_object(label_path)
    policy_payload = _load_json_object(policy_path)
    emb_payload = _load_json_object(emb_path)
    gates_payload = _load_json_object(gates_path)
    ranker_payload = _load_json_object(ranker_path) if ranker_path is not None else None

    label_rows = _validate_label_payload(label_payload)
    split_metadata = _validate_split_policy(policy_payload, target=target)
    label_sha = sha256_file(label_path)
    embedding_metadata, embeddings_by_id = _validate_embedding_payload(emb_payload, label_dataset_sha256=label_sha)
    gates_metadata = _validate_product_candidate_metric_gates(gates_payload)
    ranker_metadata = _validate_ranker_experiment(ranker_payload) if ranker_payload is not None else None

    seed = int(random_seed) if random_seed is not None else int(
        policy_payload.get("randomness_policy", {}).get("recommended_default_seed", 20260515)
    )
    eligible, excluded = _eligible_rows(label_rows=label_rows, embeddings_by_id=embeddings_by_id, target=target)
    model, pred, scores = _fit_full_audit_logistic(rows=eligible, random_seed=seed)
    y_true = [bool(row["_target_value"]) for row in eligible]
    in_sample_metrics = {
        "label": IN_SAMPLE_LABEL,
        **_classification_metrics(y_true=y_true, y_pred=pred, scores=scores),
    }

    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("embeddings", emb_path, repo_root=root),
        _input_record("production_candidate_metric_gates", gates_path, repo_root=root),
    ]
    ranker_input = None
    if ranker_path is not None:
        ranker_input = _input_record("ranker_experiment", ranker_path, repo_root=root)
        inputs.append(ranker_input)
    emb_sha = sha256_file(emb_path)
    gates_sha = sha256_file(gates_path)
    policy_sha = sha256_file(policy_path)
    ranker_sha = sha256_file(ranker_path) if ranker_path is not None else None

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "scorer_version": scorer_version,
        "target": target,
        "fit_mode": "full_fit_audit_corpus",
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "random_seed": seed,
        "sklearn_version": sklearn.__version__,
        "label_dataset_version": label_payload.get("dataset_version"),
        "label_dataset_sha256": label_sha,
        "split_policy_version": split_metadata.get("policy_version"),
        "split_policy_sha256": policy_sha,
        "embedding_artifact_version": embedding_metadata.get("embedding_artifact_version"),
        "embedding_artifact_sha256": emb_sha,
        **_copy_embedding_metadata(embedding_metadata),
        "production_candidate_metric_gates_version": gates_metadata.get("gates_version"),
        "production_candidate_metric_gates_sha256": gates_sha,
        "ranker_experiment_version": ranker_metadata.get("experiment_version") if ranker_metadata else None,
        "ranker_experiment_sha256": ranker_sha,
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "scorer": _export_scorer(model),
        "training_summary": {
            **_training_inventory(label_payload=label_payload, eligible=eligible, excluded=excluded),
            "in_sample_training_metrics": in_sample_metrics,
            "reference_cv_baseline": _reference_cv_baseline(
                ranker_payload=ranker_payload,
                ranker_input=ranker_input,
            ),
        },
        "policy_compliance": {
            "grouped_split_required": True,
            "grouped_split_used_in_this_artifact": False,
            "full_fit_on_audit_corpus_only": True,
            "observation_level_training": True,
            "product_candidate_pool_used_for_training": False,
            "production_artifact_written": False,
            "shadow_scoring_authorized": False,
        },
        "interpretation": {
            "summary": (
                "Frozen audit-only embedding logistic scorer for good_or_acceptable, intended for offline application "
                "to labeled product-candidate overlaps in a later command."
            ),
            "not_claimed": [
                "validation",
                "production ranking evidence",
                "shadow readiness",
                "live recommender quality",
                "product-pool generalization",
            ],
            "next_authorized_step": "regenerate production-candidate scoring with learned audit scorer on existing pool",
        },
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_offline_audit_embedding_scorer_export(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    scorer = payload["scorer"]
    summary = payload["training_summary"]
    metrics = summary["in_sample_training_metrics"]
    obs = summary["target_class_counts"]["observation_level"]
    work = summary["target_class_counts"]["work_group_reporting_level"]["any_positive"]
    reference = summary["reference_cv_baseline"]
    classifier = scorer["classifier"]
    scaler = scorer["scaler"]
    lines = [
        f"# Offline Audit Embedding Scorer Export ({metadata['scorer_version']})",
        "",
        "## Executive Summary",
        "",
        "Frozen JSON scorer fit on the full eligible audit-labeled embedding corpus for `good_or_acceptable`. It is intended only for a later offline product-candidate scoring pass.",
        "",
        f"- **Target:** `{metadata['target']}`",
        f"- **Fit mode:** `{metadata['fit_mode']}`",
        f"- **Shadow scoring authorized:** {payload['policy_compliance']['shadow_scoring_authorized']}",
        f"- **Production artifact written:** {payload['policy_compliance']['production_artifact_written']}",
        "",
        "## Training Inventory",
        "",
        f"- **Eligible observations:** {summary['eligible_observation_count']}",
        f"- **Unique canonical works:** {summary['unique_canonical_work_count']}",
        f"- **Duplicate observation pressure:** {summary['duplicate_observation_pressure']}",
        f"- **Conflicting target work groups:** {summary['conflicting_target_work_group_count']}",
        "",
        "| Level | Positive | Negative |",
        "| --- | ---: | ---: |",
        f"| Observation | {obs['positive']} | {obs['negative']} |",
        f"| Work any-positive | {work['positive']} | {work['negative']} |",
        "",
        "## Frozen Scorer Parameters",
        "",
        f"- **Dimensions:** {scaler['feature_count']}",
        f"- **Classes:** `{classifier['classes']}`",
        f"- **Random seed:** {metadata['random_seed']}",
        f"- **Pipeline:** `StandardScaler(with_mean=True) -> LogisticRegression(solver='lbfgs', penalty='l2', max_iter=5000)`",
        "",
        "## Apply Instructions",
        "",
        "1. Load the JSON scorer.",
        "2. For each embedding vector, compute `z = (x - mean) / scale` per dimension.",
        "3. Compute `logit_true = dot(coefficients_standardized_space, z) + intercept_standardized_space`.",
        "4. Compute `probability_true = sigmoid(logit_true)`.",
        "5. Use `probability_true >= 0.5` only as an offline diagnostic boolean prediction.",
        "",
        "## In-Sample Training Metrics",
        "",
        f"**{str(metrics['label']).replace('—', '-')}**",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Accuracy | {_fmt(metrics['accuracy'])} |",
        f"| Balanced accuracy | {_fmt(metrics['balanced_accuracy'])} |",
        f"| Macro F1 | {_fmt(metrics['macro_f1'])} |",
        f"| ROC-AUC | {_fmt(metrics['roc_auc'])} |",
        f"| Average precision | {_fmt(metrics['average_precision'])} |",
        f"| TN | {metrics['confusion']['tn']} |",
        f"| FP | {metrics['confusion']['fp']} |",
        f"| FN | {metrics['confusion']['fn']} |",
        f"| TP | {metrics['confusion']['tp']} |",
        "",
        "## CV Baseline Reference",
        "",
    ]
    if reference.get("embedding_logistic_aggregate") is None:
        lines.append(reference.get("reason") or "No reference CV baseline available.")
    else:
        agg = reference["embedding_logistic_aggregate"]
        metric_block = agg["observation_metrics_mean_std"]
        lines.extend(
            [
                f"- **Source:** `{reference['source_input']['path']}`",
                "- **Coefficient reuse:** none; per-fold coefficients are not reused or averaged.",
                "",
                "| CV metric | Mean | Std |",
                "| --- | ---: | ---: |",
                f"| Balanced accuracy | {_fmt(metric_block['balanced_accuracy']['mean'])} | {_fmt(metric_block['balanced_accuracy']['std'])} |",
                f"| ROC-AUC | {_fmt(metric_block['roc_auc']['mean'])} | {_fmt(metric_block['roc_auc']['std'])} |",
                f"| Average precision | {_fmt(metric_block['average_precision']['mean'])} | {_fmt(metric_block['average_precision']['std'])} |",
            ]
        )
    lines.extend(
        [
            "",
            "## Not Shadow / Not Production",
            "",
            "- This is not shadow scoring.",
            "- This is not production scoring.",
            "- This is not validation.",
            "- No binary model file was written.",
            "- No product-candidate pool was used for training.",
            "- Production defaults remain blocked.",
            "",
            "## Next Authorized Step",
            "",
            "Regenerate production-candidate scoring with the learned audit scorer on the existing pool.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_offline_audit_embedding_scorer_export(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    production_candidate_metric_gates_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    ranker_experiment_path: Path | None = None,
    target: str = TARGET_GOOD,
    random_seed: int | None = None,
    scorer_version: str = SCORER_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_audit_embedding_scorer_export_payload(
        label_dataset_path=label_dataset_path,
        split_policy_path=split_policy_path,
        embeddings_path=embeddings_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        ranker_experiment_path=ranker_experiment_path,
        target=target,
        random_seed=random_seed,
        scorer_version=scorer_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_offline_audit_embedding_scorer_export(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "SCORER_VERSION",
    "MLOfflineAuditEmbeddingScorerExportError",
    "build_ml_offline_audit_embedding_scorer_export_payload",
    "markdown_from_ml_offline_audit_embedding_scorer_export",
    "score_audit_embedding_probability",
    "write_ml_offline_audit_embedding_scorer_export",
]
