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
SCORER_VERSION_V2 = "ml-offline-audit-embedding-scorer-v2"
FIT_MODE_FULL = "full_fit_audit_corpus"
FIT_MODE_HOLDOUT = "holdout_bound_train_only"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
EMBEDDING_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-labeled-text-embeddings-v3"
PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
PRODUCT_CANDIDATE_GATES_VERSION = "ml-offline-production-candidate-metric-gates-v1"
PRODUCT_CANDIDATE_GATES_NEXT_STAGE = "create_frozen_audit_embedding_scorer_export_v1"
PRODUCT_CANDIDATE_GATES_VERSION_V2 = "ml-offline-production-candidate-metric-gates-v2"
HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE = "ml_learned_scorer_holdout_assignment"
HOLDOUT_ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"
HOLDOUT_POLICY_ARTIFACT_TYPE = "ml_learned_scorer_holdout_policy"
HOLDOUT_POLICY_VERSION = "ml-learned-scorer-holdout-policy-v1"
HOLDOUT_STRATEGY_ID = "product_candidate_snapshot_holdout"
RANKER_ARTIFACT_TYPE = "ml_offline_ranker_experiment"
RANKER_VERSION = "ml-offline-ranker-experiment-v1"
IN_SAMPLE_TRAIN_LABEL = "IN_SAMPLE_TRAIN_ARM_ONLY_NOT_VALIDATION"
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

CAVEATS_HOLDOUT = (
    "Not validation.",
    "Train-only fit on holdout train arm; eval arm reserved for scoring v3.",
    "In-sample train metrics are diagnostic only.",
    "Do not equate train in-sample metrics with v1 full-corpus in-sample metrics.",
    "Product snapshot eval works excluded from this fit.",
    "No shadow/production authorization.",
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


def _validate_holdout_assignment(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("artifact_type") != HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected holdout assignment metadata.artifact_type={HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("assignment_version") != HOLDOUT_ASSIGNMENT_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected holdout assignment metadata.assignment_version={HOLDOUT_ASSIGNMENT_VERSION!r}, "
            f"got {metadata.get('assignment_version')!r}"
        )
    if metadata.get("strategy_id") != HOLDOUT_STRATEGY_ID:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"holdout assignment metadata.strategy_id must be {HOLDOUT_STRATEGY_ID!r}"
        )
    leakage = payload.get("leakage_report")
    if not isinstance(leakage, Mapping):
        raise MLOfflineAuditEmbeddingScorerExportError("holdout assignment missing leakage_report object")
    if int(leakage.get("train_eval_work_overlap_count") or 0) != 0:
        raise MLOfflineAuditEmbeddingScorerExportError("holdout assignment train_eval_work_overlap_count must be 0")
    if leakage.get("global_zero_assertion") is not True:
        raise MLOfflineAuditEmbeddingScorerExportError("holdout assignment global_zero_assertion must be true")
    if metadata.get("production_candidate_metric_gates_version") != PRODUCT_CANDIDATE_GATES_VERSION_V2:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "holdout assignment metadata.production_candidate_metric_gates_version must be "
            f"{PRODUCT_CANDIDATE_GATES_VERSION_V2}"
        )
    assignments = payload.get("assignments")
    if not isinstance(assignments, list) or not assignments:
        raise MLOfflineAuditEmbeddingScorerExportError("holdout assignment missing assignments array")
    row_ids: list[str] = []
    for idx, raw in enumerate(assignments, start=1):
        if not isinstance(raw, Mapping):
            raise MLOfflineAuditEmbeddingScorerExportError(f"holdout assignment row {idx} is not an object")
        row_id = str(raw.get("row_id") or "").strip()
        if not row_id:
            raise MLOfflineAuditEmbeddingScorerExportError(f"holdout assignment row {idx} missing row_id")
        assignment = str(raw.get("assignment") or "").strip()
        if assignment not in {"train", "eval"}:
            raise MLOfflineAuditEmbeddingScorerExportError(
                f"holdout assignment row {idx} has invalid assignment {assignment!r}"
            )
        row_ids.append(row_id)
    dupes = _duplicate_values(row_ids)
    if dupes:
        raise MLOfflineAuditEmbeddingScorerExportError(f"holdout assignment contains duplicate row_id values: {dupes[:10]}")
    return metadata


def _validate_holdout_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-policy")
    if metadata.get("artifact_type") != HOLDOUT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected holdout policy metadata.artifact_type={HOLDOUT_POLICY_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != HOLDOUT_POLICY_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected holdout policy metadata.policy_version={HOLDOUT_POLICY_VERSION!r}, "
            f"got {metadata.get('policy_version')!r}"
        )
    return metadata


def _validate_full_fit_v1_reference(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export-v1")
    if metadata.get("artifact_type") != ARTIFACT_TYPE:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected v1 scorer metadata.artifact_type={ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("scorer_version") != SCORER_VERSION:
        raise MLOfflineAuditEmbeddingScorerExportError(
            f"expected v1 scorer metadata.scorer_version={SCORER_VERSION!r}, got {metadata.get('scorer_version')!r}"
        )
    if metadata.get("fit_mode") != FIT_MODE_FULL:
        raise MLOfflineAuditEmbeddingScorerExportError("v1 scorer reference fit_mode must be full_fit_audit_corpus")
    if metadata.get("target") != TARGET_GOOD:
        raise MLOfflineAuditEmbeddingScorerExportError("v1 scorer reference target must be good_or_acceptable")
    return metadata


def _holdout_policy_eval_work_sha(payload: Mapping[str, Any]) -> str | None:
    dataset_inventory = payload.get("dataset_inventory")
    if isinstance(dataset_inventory, Mapping):
        value = dataset_inventory.get("product_candidate_eval_work_set_sha256")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _holdout_strategy_eval_work_sha(payload: Mapping[str, Any]) -> str | None:
    strategy = payload.get("primary_holdout_strategy")
    if not isinstance(strategy, Mapping):
        return None
    definition = strategy.get("eval_work_set_definition")
    if not isinstance(definition, Mapping):
        return None
    value = definition.get("eval_work_set_sha256")
    return value.strip() if isinstance(value, str) and value.strip() else None


def _validate_holdout_assignment_policy_link(
    *,
    assignment_metadata: Mapping[str, Any],
    policy_payload: Mapping[str, Any],
) -> None:
    assignment_sha = assignment_metadata.get("eval_work_set_sha256")
    policy_inventory_sha = _holdout_policy_eval_work_sha(policy_payload)
    policy_strategy_sha = _holdout_strategy_eval_work_sha(policy_payload)
    if not assignment_sha:
        raise MLOfflineAuditEmbeddingScorerExportError("holdout assignment metadata.eval_work_set_sha256 is required")
    if assignment_sha != policy_inventory_sha:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "holdout assignment eval_work_set_sha256 must match holdout policy dataset_inventory"
        )
    if assignment_sha != policy_strategy_sha:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "holdout assignment eval_work_set_sha256 must match holdout policy primary_holdout_strategy"
        )


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


def _holdout_assignment_rows(
    *,
    label_rows: Sequence[dict[str, Any]],
    embeddings_by_id: Mapping[str, dict[str, Any]],
    assignment_payload: Mapping[str, Any],
    target: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    labels_by_id = {str(row.get("row_id") or "").strip(): row for row in label_rows}
    train_rows: list[dict[str, Any]] = []
    eval_rows: list[dict[str, Any]] = []
    for raw in assignment_payload.get("assignments", []):
        if not isinstance(raw, Mapping):
            continue
        row_id = str(raw.get("row_id") or "").strip()
        label_row = labels_by_id.get(row_id)
        if label_row is None:
            raise MLOfflineAuditEmbeddingScorerExportError(f"holdout assignment row_id not found in label dataset: {row_id}")
        if str(label_row.get("split") or "").strip() != "audit_only":
            raise MLOfflineAuditEmbeddingScorerExportError(f"assigned row {row_id} is not split=audit_only")
        if not _is_explicit_label_row(label_row):
            raise MLOfflineAuditEmbeddingScorerExportError(f"assigned row {row_id} does not have an explicit manual label")
        if not isinstance(label_row.get(target), bool):
            raise MLOfflineAuditEmbeddingScorerExportError(f"assigned row {row_id} target {target} is not boolean")
        canonical = canonical_openalex_work_id(label_row)
        if canonical is None:
            raise MLOfflineAuditEmbeddingScorerExportError(f"assigned row {row_id} has no resolvable canonical work id")
        assignment_canonical = str(raw.get("canonical_openalex_work_id") or "").strip()
        if assignment_canonical and assignment_canonical != canonical:
            raise MLOfflineAuditEmbeddingScorerExportError(
                f"assigned row {row_id} canonical work id {assignment_canonical!r} does not match label dataset {canonical!r}"
            )
        embedding = embeddings_by_id.get(row_id)
        if embedding is None:
            raise MLOfflineAuditEmbeddingScorerExportError(f"assigned row {row_id} missing embedding")
        item = dict(label_row)
        item["_canonical_work_id"] = canonical
        item["_target_value"] = bool(label_row[target])
        item["_embedding"] = embedding
        item["_assignment"] = str(raw.get("assignment") or "").strip()
        if item["_assignment"] == "train":
            train_rows.append(item)
        elif item["_assignment"] == "eval":
            eval_rows.append(item)
        else:
            raise MLOfflineAuditEmbeddingScorerExportError(f"assigned row {row_id} has invalid assignment")
    train_rows = sorted(train_rows, key=lambda row: str(row["row_id"]))
    eval_rows = sorted(eval_rows, key=lambda row: str(row["row_id"]))
    if not train_rows:
        raise MLOfflineAuditEmbeddingScorerExportError("holdout assignment has no train rows")
    return train_rows, eval_rows


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


def _arm_inventory(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups = _group_values(rows)
    observation_values = [bool(row["_target_value"]) for row in rows]
    work_rollup = _work_rollup_counts(rows)
    return {
        "observation_count": len(rows),
        "unique_canonical_work_count": len(groups),
        "duplicate_observation_pressure": len(rows) - len(groups),
        "positive_observation_count": sum(1 for value in observation_values if value),
        "negative_observation_count": sum(1 for value in observation_values if not value),
        "positive_work_count_any_positive": work_rollup["any_positive"]["positive"],
        "negative_work_count_any_positive": work_rollup["any_positive"]["negative"],
        "conflicting_target_work_group_count": work_rollup["conflicting_target_work_group_count"],
        "duplicate_eligible_work_group_count": sum(1 for values in groups.values() if len(values) > 1),
        "review_pool_variant_counts": _hist(rows, "review_pool_variant"),
        "family_counts": _hist(rows, "family"),
        "target_class_counts": {
            "observation_level": _bool_counts(observation_values),
            "work_group_reporting_level": work_rollup,
        },
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


def _reference_v1_full_fit(
    *,
    scorer_payload: Mapping[str, Any] | None,
    scorer_input: Mapping[str, str] | None,
) -> dict[str, Any]:
    if scorer_payload is None or scorer_input is None:
        return {
            "source_input": None,
            "in_sample_training_metrics": None,
            "reason": "No v1 full-fit scorer export supplied.",
        }
    summary = scorer_payload.get("training_summary")
    metrics = summary.get("in_sample_training_metrics") if isinstance(summary, Mapping) else None
    return {
        "source_input": dict(scorer_input),
        "in_sample_training_metrics": dict(metrics) if isinstance(metrics, Mapping) else None,
        "reason": None if isinstance(metrics, Mapping) else "v1 scorer export did not contain in_sample_training_metrics.",
        "coefficient_reuse": "not_used; v1 full-fit parameters are referenced only as diagnostics",
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
    production_candidate_metric_gates_path: Path | None = None,
    holdout_assignment_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    audit_embedding_scorer_export_v1_path: Path | None = None,
    ranker_experiment_path: Path | None = None,
    target: str = TARGET_GOOD,
    random_seed: int | None = None,
    scorer_version: str | None = None,
    fit_mode: str = FIT_MODE_FULL,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if target != TARGET_GOOD:
        raise MLOfflineAuditEmbeddingScorerExportError("ml-offline-audit-embedding-scorer-export supports only good_or_acceptable")
    if fit_mode not in {FIT_MODE_FULL, FIT_MODE_HOLDOUT}:
        raise MLOfflineAuditEmbeddingScorerExportError(f"unsupported fit_mode: {fit_mode}")
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    emb_path = Path(embeddings_path).resolve()
    ranker_path = Path(ranker_experiment_path).resolve() if ranker_experiment_path is not None else None

    label_payload = _load_json_object(label_path)
    policy_payload = _load_json_object(policy_path)
    emb_payload = _load_json_object(emb_path)
    ranker_payload = _load_json_object(ranker_path) if ranker_path is not None else None

    label_rows = _validate_label_payload(label_payload)
    split_metadata = _validate_split_policy(policy_payload, target=target)
    label_sha = sha256_file(label_path)
    embedding_metadata, embeddings_by_id = _validate_embedding_payload(emb_payload, label_dataset_sha256=label_sha)
    ranker_metadata = _validate_ranker_experiment(ranker_payload) if ranker_payload is not None else None

    split_seed = int(policy_payload.get("randomness_policy", {}).get("recommended_default_seed", 20260515))
    eligible, excluded = _eligible_rows(label_rows=label_rows, embeddings_by_id=embeddings_by_id, target=target)
    ranker_input = None
    if ranker_path is not None:
        ranker_input = _input_record("ranker_experiment", ranker_path, repo_root=root)
    emb_sha = sha256_file(emb_path)
    policy_sha = sha256_file(policy_path)
    ranker_sha = sha256_file(ranker_path) if ranker_path is not None else None

    if fit_mode == FIT_MODE_FULL:
        if production_candidate_metric_gates_path is None:
            raise MLOfflineAuditEmbeddingScorerExportError(
                "--production-candidate-metric-gates is required for full_fit_audit_corpus"
            )
        gates_path = Path(production_candidate_metric_gates_path).resolve()
        gates_payload = _load_json_object(gates_path)
        gates_metadata = _validate_product_candidate_metric_gates(gates_payload)
        seed = int(random_seed) if random_seed is not None else split_seed
        resolved_scorer_version = scorer_version or SCORER_VERSION
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
        if ranker_input is not None:
            inputs.append(ranker_input)
        gates_sha = sha256_file(gates_path)

        metadata = {
            "artifact_type": ARTIFACT_TYPE,
            "scorer_version": resolved_scorer_version,
            "target": target,
            "fit_mode": FIT_MODE_FULL,
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

    if holdout_assignment_path is None or holdout_policy_path is None:
        raise MLOfflineAuditEmbeddingScorerExportError(
            "--holdout-assignment and --holdout-policy are required for holdout_bound_train_only"
        )
    assignment_path = Path(holdout_assignment_path).resolve()
    holdout_policy_path = Path(holdout_policy_path).resolve()
    v1_reference_path = (
        Path(audit_embedding_scorer_export_v1_path).resolve()
        if audit_embedding_scorer_export_v1_path is not None
        else None
    )
    assignment_payload = _load_json_object(assignment_path)
    holdout_policy_payload = _load_json_object(holdout_policy_path)
    v1_reference_payload = _load_json_object(v1_reference_path) if v1_reference_path is not None else None

    assignment_metadata = _validate_holdout_assignment(assignment_payload)
    holdout_policy_metadata = _validate_holdout_policy(holdout_policy_payload)
    _validate_holdout_assignment_policy_link(
        assignment_metadata=assignment_metadata,
        policy_payload=holdout_policy_payload,
    )
    v1_reference_metadata = (
        _validate_full_fit_v1_reference(v1_reference_payload) if v1_reference_payload is not None else None
    )

    train_rows, eval_rows = _holdout_assignment_rows(
        label_rows=label_rows,
        embeddings_by_id=embeddings_by_id,
        assignment_payload=assignment_payload,
        target=target,
    )
    train_counts = _bool_counts([bool(row["_target_value"]) for row in train_rows])
    if train_counts["positive"] == 0 or train_counts["negative"] == 0:
        raise MLOfflineAuditEmbeddingScorerExportError("holdout train rows must contain both positive and negative labels")
    train_work_ids = {str(row["_canonical_work_id"]) for row in train_rows}
    eval_work_ids = {str(row["_canonical_work_id"]) for row in eval_rows}
    overlap = sorted(train_work_ids & eval_work_ids)
    if overlap:
        raise MLOfflineAuditEmbeddingScorerExportError(f"train/eval work overlap detected: {overlap[:20]}")
    train_row_ids = {str(row.get("row_id")) for row in train_rows}
    eval_row_ids = {str(row.get("row_id")) for row in eval_rows}
    row_overlap = sorted(train_row_ids & eval_row_ids)
    if row_overlap:
        raise MLOfflineAuditEmbeddingScorerExportError(f"train/eval row_id overlap detected: {row_overlap[:20]}")

    assignment_seed = assignment_metadata.get("seed")
    seed = int(
        random_seed
        if random_seed is not None
        else assignment_seed
        if assignment_seed is not None
        else split_seed
    )
    resolved_scorer_version = scorer_version or SCORER_VERSION_V2
    model, pred, scores = _fit_full_audit_logistic(rows=train_rows, random_seed=seed)
    y_true = [bool(row["_target_value"]) for row in train_rows]
    in_sample_metrics = {
        "label": IN_SAMPLE_TRAIN_LABEL,
        **_classification_metrics(y_true=y_true, y_pred=pred, scores=scores),
    }

    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("embeddings", emb_path, repo_root=root),
        _input_record("holdout_assignment", assignment_path, repo_root=root),
        _input_record("holdout_policy", holdout_policy_path, repo_root=root),
    ]
    if ranker_input is not None:
        inputs.append(ranker_input)
    v1_reference_input = None
    if v1_reference_path is not None:
        v1_reference_input = _input_record("audit_embedding_scorer_export_v1", v1_reference_path, repo_root=root)
        inputs.append(v1_reference_input)

    assignment_sha = sha256_file(assignment_path)
    holdout_policy_sha = sha256_file(holdout_policy_path)
    v1_reference_sha = sha256_file(v1_reference_path) if v1_reference_path is not None else None
    eval_work_count = int(assignment_metadata.get("eval_work_count") or len(eval_work_ids))

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "scorer_version": resolved_scorer_version,
        "fit_mode": FIT_MODE_HOLDOUT,
        "target": target,
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
        "holdout_assignment_version": assignment_metadata.get("assignment_version"),
        "holdout_assignment_sha256": assignment_sha,
        "holdout_policy_version": holdout_policy_metadata.get("policy_version"),
        "holdout_policy_sha256": holdout_policy_sha,
        "eval_work_set_sha256": assignment_metadata.get("eval_work_set_sha256"),
        "eval_work_count": eval_work_count,
        "train_work_count": len(train_work_ids),
        "train_observation_count": len(train_rows),
        "ranker_experiment_version": ranker_metadata.get("experiment_version") if ranker_metadata else None,
        "ranker_experiment_sha256": ranker_sha,
        "audit_embedding_scorer_export_v1_version": (
            v1_reference_metadata.get("scorer_version") if v1_reference_metadata else None
        ),
        "audit_embedding_scorer_export_v1_sha256": v1_reference_sha,
        "caveats": list(CAVEATS_HOLDOUT),
    }

    return {
        "metadata": metadata,
        "scorer": _export_scorer(model),
        "training_summary": {
            "train_arm_inventory": {
                **_arm_inventory(train_rows),
                "duplicate_conflict_rollups_from_assignment": (
                    assignment_payload.get("duplicate_and_conflict_report", {}).get("train")
                    if isinstance(assignment_payload.get("duplicate_and_conflict_report"), Mapping)
                    else None
                ),
            },
            "in_sample_training_metrics": in_sample_metrics,
            "reference_baselines": {
                "cv": _reference_cv_baseline(ranker_payload=ranker_payload, ranker_input=ranker_input),
                "full_fit_v1": _reference_v1_full_fit(
                    scorer_payload=v1_reference_payload,
                    scorer_input=v1_reference_input,
                ),
            },
            "eval_arm_reserved": {
                **_arm_inventory(eval_rows),
                "not_used_in_fit": True,
                "eval_work_set_sha256": assignment_metadata.get("eval_work_set_sha256"),
            },
        },
        "policy_compliance": {
            "holdout_assignment_honored": True,
            "eval_works_excluded_from_fit": True,
            "train_eval_work_overlap_count": 0,
            "product_candidate_pool_used_for_training": False,
            "production_artifact_written": False,
            "shadow_scoring_authorized": False,
            "independent_validation_complete": False,
        },
        "interpretation": {
            "summary": "Holdout-bound scorer trained only on non-product audit works.",
            "next_authorized_step": "ml-offline-production-candidate-scoring-v3",
            "not_claimed": [
                "validation",
                "shadow readiness",
                "production readiness",
                "product-pool generalization without scoring v3",
            ],
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_authorized": False,
            "production_default_authorized": False,
            "missing_holdout_bound_scorer_export": False,
            "missing_product_candidate_scoring_v3": True,
            "missing_metric_gates_v3": True,
            "missing_ml_shadow_scorer_v1": True,
        },
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _markdown_holdout_bound(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    scorer = payload["scorer"]
    summary = payload["training_summary"]
    train = summary["train_arm_inventory"]
    eval_reserved = summary["eval_arm_reserved"]
    metrics = summary["in_sample_training_metrics"]
    classifier = scorer["classifier"]
    scaler = scorer["scaler"]
    compliance = payload["policy_compliance"]
    train_obs = train["target_class_counts"]["observation_level"]
    train_work = train["target_class_counts"]["work_group_reporting_level"]["any_positive"]
    eval_obs = eval_reserved["target_class_counts"]["observation_level"]
    eval_work = eval_reserved["target_class_counts"]["work_group_reporting_level"]["any_positive"]
    baselines = summary["reference_baselines"]
    cv = baselines["cv"]
    full_fit = baselines["full_fit_v1"]

    lines = [
        f"# Offline Audit Embedding Scorer Export ({metadata['scorer_version']})",
        "",
        "## Executive Summary",
        "",
        "Frozen JSON scorer fit only on the holdout assignment train arm for `good_or_acceptable`. Product-candidate eval works are reserved for scoring v3.",
        "",
        f"- **Target:** `{metadata['target']}`",
        f"- **Fit mode:** `{metadata['fit_mode']}`",
        f"- **Train observations:** {metadata['train_observation_count']}",
        f"- **Train works:** {metadata['train_work_count']}",
        f"- **Eval works excluded:** {metadata['eval_work_count']}",
        f"- **Eval work-set SHA:** `{metadata['eval_work_set_sha256']}`",
        f"- **Shadow scoring authorized:** {compliance['shadow_scoring_authorized']}",
        f"- **Production artifact written:** {compliance['production_artifact_written']}",
        "",
        "## Leakage Checks",
        "",
        f"- **Holdout assignment honored:** {compliance['holdout_assignment_honored']}",
        f"- **Eval works excluded from fit:** {compliance['eval_works_excluded_from_fit']}",
        f"- **Train/eval work overlap count:** {compliance['train_eval_work_overlap_count']}",
        "",
        "## Train Class Balance",
        "",
        "| Level | Count | Positive | Negative | Conflicts |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| Observation | {train['observation_count']} | {train_obs['positive']} | {train_obs['negative']} | n/a |",
        f"| Work any-positive | {train['unique_canonical_work_count']} | {train_work['positive']} | {train_work['negative']} | {train['conflicting_target_work_group_count']} |",
        "",
        "## Scorer Summary",
        "",
        f"- **Dimensions:** {scaler['feature_count']}",
        f"- **Classes:** `{classifier['classes']}`",
        f"- **Random seed:** {metadata['random_seed']}",
        f"- **Pipeline:** `StandardScaler(with_mean=True) -> LogisticRegression(solver='lbfgs', penalty='l2', max_iter=5000)`",
        "",
        "## In-Sample Train Metrics",
        "",
        f"**{metrics['label']} - NOT VALIDATION**",
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
        "## Baselines",
        "",
    ]
    if cv.get("embedding_logistic_aggregate") is None:
        lines.append(f"- **Grouped CV baseline:** {cv.get('reason')}")
    else:
        lines.append(f"- **Grouped CV baseline:** referenced from `{cv['source_input']['path']}`; fold coefficients not reused.")
    if full_fit.get("in_sample_training_metrics") is None:
        lines.append(f"- **V1 full-fit reference:** {full_fit.get('reason')}")
    else:
        reference_metrics = full_fit["in_sample_training_metrics"]
        lines.append(
            "- **V1 full-fit reference:** "
            f"ROC-AUC {_fmt(reference_metrics.get('roc_auc'))}, "
            f"AP {_fmt(reference_metrics.get('average_precision'))}; parameters not reused."
        )
    lines.extend(
        [
            "",
            "## Eval Reserved",
            "",
            "| Level | Count | Positive | Negative | Conflicts |",
            "| --- | ---: | ---: | ---: | ---: |",
            f"| Observation | {eval_reserved['observation_count']} | {eval_obs['positive']} | {eval_obs['negative']} | n/a |",
            f"| Work any-positive | {eval_reserved['unique_canonical_work_count']} | {eval_work['positive']} | {eval_work['negative']} | {eval_reserved['conflicting_target_work_group_count']} |",
            "",
            f"- **Not used in fit:** {eval_reserved['not_used_in_fit']}",
            "",
            "## Next Step",
            "",
            "`ml-offline-production-candidate-scoring-v3` may apply this holdout-bound scorer to the reserved product-candidate eval works.",
            "",
            "## Not Shadow / Not Production",
            "",
            "- This is not validation.",
            "- This is not shadow scoring.",
            "- This is not production scoring.",
            "- No binary model file was written.",
            "- Product-candidate eval works were excluded from training.",
            "- Production defaults remain blocked.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def markdown_from_ml_offline_audit_embedding_scorer_export(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    if metadata.get("fit_mode") == FIT_MODE_HOLDOUT:
        return _markdown_holdout_bound(payload)
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
    production_candidate_metric_gates_path: Path | None = None,
    holdout_assignment_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    audit_embedding_scorer_export_v1_path: Path | None = None,
    output_path: Path,
    markdown_output_path: Path,
    ranker_experiment_path: Path | None = None,
    target: str = TARGET_GOOD,
    random_seed: int | None = None,
    scorer_version: str | None = None,
    fit_mode: str = FIT_MODE_FULL,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_audit_embedding_scorer_export_payload(
        label_dataset_path=label_dataset_path,
        split_policy_path=split_policy_path,
        embeddings_path=embeddings_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        holdout_assignment_path=holdout_assignment_path,
        holdout_policy_path=holdout_policy_path,
        audit_embedding_scorer_export_v1_path=audit_embedding_scorer_export_v1_path,
        ranker_experiment_path=ranker_experiment_path,
        target=target,
        random_seed=random_seed,
        scorer_version=scorer_version,
        fit_mode=fit_mode,
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
    "FIT_MODE_FULL",
    "FIT_MODE_HOLDOUT",
    "SCORER_VERSION",
    "SCORER_VERSION_V2",
    "MLOfflineAuditEmbeddingScorerExportError",
    "build_ml_offline_audit_embedding_scorer_export_payload",
    "markdown_from_ml_offline_audit_embedding_scorer_export",
    "score_audit_embedding_probability",
    "write_ml_offline_audit_embedding_scorer_export",
]
