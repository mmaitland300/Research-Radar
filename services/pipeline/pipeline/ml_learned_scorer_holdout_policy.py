"""Learned scorer holdout policy for offline audit embedding scorers.

This module writes a deterministic policy/spec artifact only. It does not
assign split rows, train or refit scorers, generate embeddings, query a
database, run ranking, or authorize shadow/production behavior.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_learned_scorer_holdout_policy"
POLICY_VERSION = "ml-learned-scorer-holdout-policy-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
SPLIT_POLICY_ARTIFACT_TYPE = "ml_label_split_policy"
SPLIT_POLICY_VERSION = "ml-label-split-policy-v1"
EMBEDDING_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-labeled-text-embeddings-v3"
SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
SCORING_VERSION = "ml-offline-production-candidate-scoring-v2"
SCORING_MODE = "heuristic_and_audit_embedding_scorer"
METRIC_GATES_ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
METRIC_GATES_VERSION = "ml-offline-production-candidate-metric-gates-v2"
METRIC_GATES_NEXT_STAGE = "create_learned_scorer_holdout_policy_v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
TARGET_GOOD = "good_or_acceptable"
TARGET_SURPRISING = "surprising_or_useful"
SOURCE_FIELDS_CHECKED_IN_ORDER = ("work_id", "openalex_work_id", "paper_id")

CAVEATS = (
    "Not validation.",
    "Policy only; no assignments yet.",
    "Single-reviewer audit labels.",
    "Product snapshot is one ranking run/family, not live recommender quality.",
    "The eval work set is reserved from training, but label-based metrics require labeled observations.",
    "Observation-level duplicates/conflicts are preserved.",
    "No ranking/API/web changes.",
    "No shadow or production default authorization.",
)

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)


class MLLearnedScorerHoldoutPolicyError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLLearnedScorerHoldoutPolicyError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLLearnedScorerHoldoutPolicyError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLLearnedScorerHoldoutPolicyError(f"{name} JSON missing metadata object")
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
        raise MLLearnedScorerHoldoutPolicyError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _canonical_work_id(row: Mapping[str, Any]) -> str | None:
    for field in SOURCE_FIELDS_CHECKED_IN_ORDER:
        value = str(row.get(field) or "").strip()
        match = _WORK_ID_RE.search(value)
        if match:
            return match.group(1).upper()
    return None


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _is_explicit_label_row(row: Mapping[str, Any]) -> bool:
    return row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()})


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLearnedScorerHoldoutPolicyError("label dataset missing rows array")
    normalized: list[dict[str, Any]] = []
    row_ids: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLLearnedScorerHoldoutPolicyError(f"label row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLLearnedScorerHoldoutPolicyError(f"label row {idx} missing row_id")
        row_ids.append(row_id)
        normalized.append(row)
    duplicates = [row_id for row_id, count in Counter(row_ids).items() if count > 1]
    if duplicates:
        raise MLLearnedScorerHoldoutPolicyError(f"label dataset contains duplicate row_id values: {duplicates[:10]}")
    return normalized


def _validate_split_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    assertions = payload.get("policy_assertions")
    if not isinstance(assertions, Mapping):
        raise MLLearnedScorerHoldoutPolicyError("split policy missing policy_assertions object")
    required = {
        "requires_grouped_split_by_work": True,
        "permits_row_level_random_split": False,
    }
    for key, expected in required.items():
        if assertions.get(key) is not expected:
            raise MLLearnedScorerHoldoutPolicyError(f"split policy policy_assertions.{key} must be {expected}")
    if payload.get("allowed_targets_for_v1_split") != [TARGET_GOOD]:
        raise MLLearnedScorerHoldoutPolicyError("split policy allowed_targets_for_v1_split must be ['good_or_acceptable']")
    forbidden = payload.get("forbidden_targets")
    if not isinstance(forbidden, Sequence) or isinstance(forbidden, (str, bytes)) or TARGET_SURPRISING not in forbidden:
        raise MLLearnedScorerHoldoutPolicyError("split policy forbidden_targets must include surprising_or_useful")
    return metadata


def _validate_embeddings(
    payload: Mapping[str, Any],
    *,
    label_dataset_sha256: str,
) -> tuple[Mapping[str, Any], dict[str, dict[str, Any]], int | None]:
    metadata = _metadata(payload, name="embeddings")
    if metadata.get("artifact_type") != EMBEDDING_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected embeddings metadata.artifact_type={EMBEDDING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDING_ARTIFACT_VERSION:
        raise MLLearnedScorerHoldoutPolicyError(
            "expected embeddings metadata.embedding_artifact_version="
            f"{EMBEDDING_ARTIFACT_VERSION!r}, got {metadata.get('embedding_artifact_version')!r}"
        )
    source_sha = metadata.get("source_label_dataset_sha256")
    if source_sha is not None and source_sha != label_dataset_sha256:
        raise MLLearnedScorerHoldoutPolicyError(
            "embeddings metadata.source_label_dataset_sha256 must match supplied label dataset"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLearnedScorerHoldoutPolicyError("embeddings missing rows array")
    expected_dim = metadata.get("embedding_dimensions")
    expected_dim_int = expected_dim if isinstance(expected_dim, int) and expected_dim > 0 else None
    by_id: dict[str, dict[str, Any]] = {}
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLLearnedScorerHoldoutPolicyError(f"embedding row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLLearnedScorerHoldoutPolicyError(f"embedding row {idx} missing row_id")
        by_id[row_id] = row
    return metadata, by_id, expected_dim_int


def _validate_scoring(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("scoring_mode") != SCORING_MODE:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected scoring metadata.scoring_mode={SCORING_MODE!r}, got {metadata.get('scoring_mode')!r}"
        )
    rows = payload.get("candidate_pool_rows")
    if not isinstance(rows, list) or not rows:
        raise MLLearnedScorerHoldoutPolicyError(
            "production candidate scoring candidate_pool_rows[] must exist; this policy does not query DB"
        )
    missing: list[int] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping) or _canonical_from_value(row.get("canonical_openalex_work_id")) is None:
            missing.append(idx)
    if missing:
        raise MLLearnedScorerHoldoutPolicyError(
            "production candidate scoring candidate_pool_rows[] must contain canonical_openalex_work_id values; "
            f"bad row indexes: {missing[:20]}"
        )
    return metadata


def _validate_metric_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates")
    if metadata.get("artifact_type") != METRIC_GATES_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected metric gates metadata.artifact_type={METRIC_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != METRIC_GATES_VERSION:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected metric gates metadata.gates_version={METRIC_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("learned_scorer_application_gates_passed") is not True:
        raise MLLearnedScorerHoldoutPolicyError("metric gates learned_scorer_application_gates_passed must be true")
    if payload.get("independent_learned_validation_passed") is not False:
        raise MLLearnedScorerHoldoutPolicyError("metric gates independent_learned_validation_passed must be false")
    if payload.get("recommended_next_stage") != METRIC_GATES_NEXT_STAGE:
        raise MLLearnedScorerHoldoutPolicyError(
            f"metric gates recommended_next_stage must be {METRIC_GATES_NEXT_STAGE!r}"
        )
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutPolicyError(
            "expected production readiness plan metadata.artifact_type="
            f"{PRODUCTION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLLearnedScorerHoldoutPolicyError(
            f"expected production readiness plan metadata.plan_version={PRODUCTION_PLAN_VERSION!r}, "
            f"got {metadata.get('plan_version')!r}"
        )
    if _get(payload, "targets.good_or_acceptable.production_eligible") is not False:
        raise MLLearnedScorerHoldoutPolicyError(
            "production readiness plan targets.good_or_acceptable.production_eligible must be false"
        )
    for path in (
        "production_default_authorized",
        "production_default_allowed",
        "metadata.production_default_authorized",
        "metadata.production_default_allowed",
    ):
        if _get(payload, path) is True:
            raise MLLearnedScorerHoldoutPolicyError("production readiness plan must keep production default unauthorized")
    return metadata


def _embedding_ok(row: Mapping[str, Any] | None, *, expected_dim: int | None) -> bool:
    if row is None or row.get("embedding_status") != "ok":
        return False
    vector = row.get("embedding")
    if expected_dim is None:
        return isinstance(vector, list)
    return isinstance(vector, list) and len(vector) == expected_dim


def _eligible_rows(
    *,
    label_rows: Sequence[dict[str, Any]],
    embeddings_by_id: Mapping[str, dict[str, Any]],
    expected_dim: int | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    audit_eligible: list[dict[str, Any]] = []
    with_embedding: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    for row in label_rows:
        row_id = str(row.get("row_id") or "").strip()
        if str(row.get("split") or "").strip() != "audit_only":
            excluded["split_not_audit_only"] += 1
            continue
        if not _is_explicit_label_row(row):
            excluded["no_explicit_manual_label"] += 1
            continue
        if not isinstance(row.get(TARGET_GOOD), bool):
            excluded["target_not_boolean"] += 1
            continue
        canonical = _canonical_work_id(row)
        if canonical is None:
            excluded["missing_canonical_work_id"] += 1
            continue
        item = dict(row)
        item["_canonical_work_id"] = canonical
        item["_target_value"] = bool(row[TARGET_GOOD])
        audit_eligible.append(item)
        embedding = embeddings_by_id.get(row_id)
        if not _embedding_ok(embedding, expected_dim=expected_dim):
            excluded["missing_or_bad_embedding"] += 1
            continue
        with_embedding.append(item)
    return (
        sorted(audit_eligible, key=lambda row: str(row["row_id"])),
        sorted(with_embedding, key=lambda row: str(row["row_id"])),
        dict(sorted(excluded.items())),
    )


def _candidate_eval_work_set(scoring_payload: Mapping[str, Any]) -> list[str]:
    rows = scoring_payload["candidate_pool_rows"]
    works = {
        canonical
        for row in rows
        if isinstance(row, Mapping)
        for canonical in [_canonical_from_value(row.get("canonical_openalex_work_id"))]
        if canonical is not None
    }
    return sorted(works)


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    payload = "".join(f"{work_id}\n" for work_id in sorted(work_ids))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _group_targets(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[bool]]:
    groups: dict[str, list[bool]] = defaultdict(list)
    for row in rows:
        groups[str(row["_canonical_work_id"])].append(bool(row["_target_value"]))
    return dict(groups)


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


def _inventory(
    *,
    label_payload: Mapping[str, Any],
    audit_eligible: Sequence[Mapping[str, Any]],
    with_embedding: Sequence[Mapping[str, Any]],
    eval_work_set: Sequence[str],
    excluded_rows_by_reason: Mapping[str, int],
) -> dict[str, Any]:
    eval_works = set(eval_work_set)
    audit_work_set = {str(row["_canonical_work_id"]) for row in audit_eligible}
    embedding_work_set = {str(row["_canonical_work_id"]) for row in with_embedding}
    eval_rows = [row for row in with_embedding if str(row["_canonical_work_id"]) in eval_works]
    train_rows = [row for row in with_embedding if str(row["_canonical_work_id"]) not in eval_works]
    eval_labeled_work_set = {str(row["_canonical_work_id"]) for row in eval_rows}
    groups = _group_targets(with_embedding)
    duplicate_groups = {work_id: values for work_id, values in groups.items() if len(values) > 1}
    conflict_groups = {
        work_id: values
        for work_id, values in groups.items()
        if any(values) and not all(values)
    }
    eval_values = [bool(row["_target_value"]) for row in eval_rows]
    return {
        "audit_eligible_observation_count": len(audit_eligible),
        "audit_eligible_unique_work_count": len(audit_work_set),
        "audit_eligible_observations_with_embedding_count": len(with_embedding),
        "audit_eligible_unique_works_with_embedding_count": len(embedding_work_set),
        "product_candidate_pool_work_count": len(eval_works),
        "product_candidate_labeled_eval_work_count": len(eval_labeled_work_set),
        "product_candidate_unlabeled_eval_work_count": len(eval_works - eval_labeled_work_set),
        "product_candidate_eval_work_set_sha256": _work_set_sha256(eval_work_set),
        "train_work_count_estimate": len(embedding_work_set - eval_works),
        "train_observation_count_estimate": len(train_rows),
        "eval_observation_count_estimate": len(eval_rows),
        "eval_labeled_observation_count_estimate": len(eval_rows),
        "eval_positive_observation_count": sum(1 for value in eval_values if value),
        "eval_negative_observation_count": sum(1 for value in eval_values if not value),
        "duplicate_work_group_count": len(duplicate_groups),
        "duplicate_observation_pressure": len(with_embedding) - len(embedding_work_set),
        "conflicting_target_work_group_count": len(conflict_groups),
        "overlap_work_count_between_full_fit_training_universe_and_eval_set": len(embedding_work_set & eval_works),
        "excluded_rows_by_reason": dict(sorted(excluded_rows_by_reason.items())),
        "duplicate_conflict_rollups": _duplicate_conflict_rollups(label_payload),
    }


def _target_policy() -> dict[str, Any]:
    return {
        "eligible_target": TARGET_GOOD,
        "forbidden_targets": [TARGET_SURPRISING],
        "surprising_or_useful_reason": (
            "forbidden by split policy v1 and deferred for rubric/transfer instability"
        ),
        "production_eligible": False,
    }


def _grouping_policy() -> dict[str, Any]:
    return {
        "grouping_key": "canonical_openalex_work_id",
        "source_fields_checked_in_order": list(SOURCE_FIELDS_CHECKED_IN_ORDER),
        "normalization_rules": "inherit from ml-label-split-policy-v1",
        "leakage_rule": "no canonical OpenAlex work may appear in both train and eval",
        "all_observations_for_one_canonical_work_share_one_future_assignment": True,
    }


def _eligibility_policy() -> dict[str, Any]:
    return {
        "inherits": "ml-label-split-policy-v1 row eligibility",
        "required_split": "audit_only",
        "require_explicit_manual_labels": True,
        "require_boolean_good_or_acceptable": True,
        "require_resolvable_canonical_openalex_work_id": True,
        "require_embedding_row_present_with_status_ok": EMBEDDING_ARTIFACT_VERSION,
        "preserve_observation_level_rows": True,
        "do_not_dedupe_or_silently_resolve_conflicts": True,
    }


def _primary_holdout_strategy(
    *,
    scoring_payload: Mapping[str, Any],
    eval_work_set: Sequence[str],
    eval_work_set_sha256: str,
) -> dict[str, Any]:
    return {
        "strategy_id": "product_candidate_snapshot_holdout",
        "status": "selected_for_v1",
        "eval_work_set_definition": {
            "source_artifact": SCORING_VERSION,
            "source_field": "candidate_pool_rows[].canonical_openalex_work_id",
            "source_pool": "product-candidate paper_scores pool already materialized in scoring v2",
            "ranking_run_id": _get(scoring_payload, "metadata.ranking_run_id")
            or _get(scoring_payload, "candidate_pool_definition.ranking_run_id"),
            "family": _get(scoring_payload, "metadata.family") or _get(scoring_payload, "candidate_pool_definition.family"),
            "eval_work_set": "all unique canonical works in candidate_pool_rows, not only labeled works",
            "eval_work_set_count": len(eval_work_set),
            "eval_work_set_sha256": eval_work_set_sha256,
            "full_assignment_rows_listed_here": False,
        },
        "train_work_set_definition": {
            "definition": (
                "all audit-eligible canonical works from v8/v3 embeddings whose canonical work ID is not in eval_work_set"
            ),
            "all_observations_for_train_works_belong_to_train_in_future_assignment": True,
            "no_eval_work_may_be_used_to_fit_holdout_bound_scorer": True,
        },
        "metric_subset_definition": {
            "future_held_out_metrics_require_explicit_boolean_good_or_acceptable_labels": True,
            "unlabeled_product_candidate_eval_works_reserved_from_training": True,
            "unlabeled_eval_works_excluded_from_label_metric_denominators": True,
            "future_artifacts_must_report_labeled_vs_unlabeled_eval_work_counts": True,
        },
    }


def _product_candidate_overlap_honesty() -> dict[str, Any]:
    return {
        "product_pool_works_are_eval_only_for_holdout_bound_scorer_training": True,
        "scoring_v2_metrics_interpretation": (
            "application diagnostics because the frozen scorer was full-fit on the audit label universe"
        ),
        "cites_metric_gates_v2_gate": "G12_independent_validation_status",
        "independent_validation_incomplete_until": [
            "holdout assignment artifact exists",
            "holdout-bound scorer is trained only on train works",
            "product-candidate scoring v3 applies that scorer to eval works",
            "metric gates v3 evaluate held-out learned metrics",
        ],
    }


def _conflict_duplicate_policy() -> dict[str, Any]:
    return {
        "silent_label_merge_allowed": False,
        "duplicate_work_groups_assigned_as_unit": True,
        "preserve_observation_level_rows": True,
        "downstream_commands_must_report": [
            "duplicate_work_group_count",
            "duplicate_observation_pressure",
            "conflicting_target_work_group_count",
            "conflict handling policy reference",
        ],
    }


def _randomness_policy(split_policy_payload: Mapping[str, Any]) -> dict[str, Any]:
    seed = _get(split_policy_payload, "randomness_policy.recommended_default_seed")
    if not isinstance(seed, int):
        seed = 20260515
    return {
        "assigns_folds_or_row_level_split_ids": False,
        "recommended_default_seed": seed,
        "future_assignment_command_must_declare": [
            "seed",
            "strategy_id",
            "grouping key",
            "stratification/reporting fields",
            "leakage report",
        ],
    }


def _future_commands() -> list[dict[str, Any]]:
    return [
        {
            "command": "ml-learned-scorer-holdout-assignment-v1",
            "purpose": "materialize per-work and per-row train/eval assignments",
            "must_include": ["leakage report"],
        },
        {
            "command": "ml-offline-audit-embedding-scorer-export-v2",
            "purpose": "train only on train-arm observations",
            "must_not_use": ["eval work in fitting"],
        },
        {
            "command": "ml-offline-production-candidate-scoring-v3",
            "purpose": "apply holdout-bound scorer to product-candidate eval arm",
        },
        {
            "command": "ml-offline-production-candidate-metric-gates-v3",
            "purpose": "evaluate held-out learned metrics",
            "independent_learned_validation_may_become_true_only_here": True,
        },
    ]


def _shadow_and_production_blockers() -> dict[str, bool]:
    return {
        "shadow_scoring_authorized": False,
        "production_default_authorized": False,
        "independent_validation_complete": False,
        "missing_holdout_assignment": True,
        "missing_holdout_bound_scorer_export": True,
        "missing_product_candidate_scoring_v3": True,
        "missing_metric_gates_v3": True,
        "missing_ml_shadow_scorer_v1": True,
    }


def build_ml_learned_scorer_holdout_policy_payload(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    production_candidate_scoring_path: Path,
    production_candidate_metric_gates_path: Path,
    production_readiness_plan_path: Path,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    embeddings_resolved = Path(embeddings_path).resolve()
    scoring_path = Path(production_candidate_scoring_path).resolve()
    gates_path = Path(production_candidate_metric_gates_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()

    label_payload = _load_json_object(label_path)
    split_policy_payload = _load_json_object(policy_path)
    embeddings_payload = _load_json_object(embeddings_resolved)
    scoring_payload = _load_json_object(scoring_path)
    gates_payload = _load_json_object(gates_path)
    plan_payload = _load_json_object(plan_path)

    label_sha = sha256_file(label_path)
    label_rows = _validate_label_dataset(label_payload)
    split_policy_metadata = _validate_split_policy(split_policy_payload)
    embeddings_metadata, embeddings_by_id, expected_dim = _validate_embeddings(
        embeddings_payload,
        label_dataset_sha256=label_sha,
    )
    scoring_metadata = _validate_scoring(scoring_payload)
    gates_metadata = _validate_metric_gates(gates_payload)
    plan_metadata = _validate_production_plan(plan_payload)

    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("embeddings", embeddings_resolved, repo_root=root),
        _input_record("production_candidate_scoring", scoring_path, repo_root=root),
        _input_record("production_candidate_metric_gates", gates_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]

    audit_eligible, with_embedding, excluded = _eligible_rows(
        label_rows=label_rows,
        embeddings_by_id=embeddings_by_id,
        expected_dim=expected_dim,
    )
    eval_work_set = _candidate_eval_work_set(scoring_payload)
    eval_work_set_sha = _work_set_sha256(eval_work_set)
    inventory = _inventory(
        label_payload=label_payload,
        audit_eligible=audit_eligible,
        with_embedding=with_embedding,
        eval_work_set=eval_work_set,
        excluded_rows_by_reason=excluded,
    )

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "policy_version": policy_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "label_dataset_version": label_payload.get("dataset_version"),
        "split_policy_version": split_policy_metadata.get("policy_version"),
        "embedding_artifact_version": embeddings_metadata.get("embedding_artifact_version"),
        "production_candidate_scoring_version": scoring_metadata.get("experiment_version"),
        "production_candidate_metric_gates_version": gates_metadata.get("gates_version"),
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "target_policy": _target_policy(),
        "grouping_policy": _grouping_policy(),
        "eligibility_policy": _eligibility_policy(),
        "primary_holdout_strategy": _primary_holdout_strategy(
            scoring_payload=scoring_payload,
            eval_work_set=eval_work_set,
            eval_work_set_sha256=eval_work_set_sha,
        ),
        "product_candidate_overlap_honesty": _product_candidate_overlap_honesty(),
        "conflict_and_duplicate_policy": _conflict_duplicate_policy(),
        "randomness_policy": _randomness_policy(split_policy_payload),
        "dataset_inventory": inventory,
        "future_commands": _future_commands(),
        "shadow_and_production_blockers": _shadow_and_production_blockers(),
        "policy_assertions": {
            "policy_only_no_assignments": True,
            "no_training_or_scorer_refit": True,
            "no_product_candidate_scoring_rerun": True,
            "no_shadow_or_production_authorization": True,
            "eval_work_set_reserved_from_future_training": True,
        },
    }


def markdown_from_ml_learned_scorer_holdout_policy(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    strategy = payload["primary_holdout_strategy"]
    eval_def = strategy["eval_work_set_definition"]
    train_def = strategy["train_work_set_definition"]
    inventory = payload["dataset_inventory"]
    blockers = payload["shadow_and_production_blockers"]

    lines = [
        f"# Learned Scorer Holdout Policy ({metadata['policy_version']})",
        "",
        "## Executive Summary",
        "",
        "This policy defines an independent evaluation boundary for future learned audit embedding scorers. It is policy only: no train/eval assignment rows are written, no scorer is trained, and no product-candidate scoring is rerun.",
        "",
        f"- **Eligible target:** `{payload['target_policy']['eligible_target']}`",
        f"- **Primary strategy:** `{strategy['strategy_id']}`",
        f"- **Eval work count:** {eval_def['eval_work_set_count']}",
        f"- **Eval work-set SHA256:** `{eval_def['eval_work_set_sha256']}`",
        "- **Shadow/prod:** blocked",
        "",
        "## Why V2 Gates Require This Policy",
        "",
        "The v2 learned metrics showed that the frozen audit scorer could be applied to the product-candidate labeled overlap, but the scorer was full-fit on the audit-labeled corpus. Because the product-candidate overlap uses the same label dataset and embedding rows, those metrics are application diagnostics, not independent validation.",
        "",
        "## Primary Holdout Strategy",
        "",
        f"`{strategy['strategy_id']}` is selected for v1. Product-candidate snapshot works become the reserved eval work set for the next holdout-bound scorer chain.",
        "",
        "## Train Vs Eval Definitions",
        "",
        f"- **Eval:** {eval_def['eval_work_set']}.",
        f"- **Train:** {train_def['definition']}.",
        "- All observations for one canonical work must share one future assignment.",
        "- No eval work may be used to fit the holdout-bound scorer.",
        "",
        "## Product-Candidate Eval Work-Set Source",
        "",
        f"- **Source artifact:** `{eval_def['source_artifact']}`",
        f"- **Source field:** `{eval_def['source_field']}`",
        f"- **Ranking run:** `{eval_def['ranking_run_id']}`",
        f"- **Family:** `{eval_def['family']}`",
        "",
        "## Leakage Rules",
        "",
        "- No canonical OpenAlex work may appear in both train and eval.",
        "- The eval work set is all unique product-candidate snapshot works, not only labeled works.",
        "- Label-based metrics may only use eval observations with explicit boolean labels.",
        "",
        "## Conflict/Duplicate Handling",
        "",
        "- Silent label merge is not allowed.",
        "- Duplicate work groups are assigned as a unit.",
        "- Observation-level rows are preserved.",
        "",
        "## Dataset Inventory Summary",
        "",
        "| Metric | Count |",
        "| --- | ---: |",
        f"| Audit-eligible observations | {inventory['audit_eligible_observation_count']} |",
        f"| Audit-eligible unique works | {inventory['audit_eligible_unique_work_count']} |",
        f"| Audit-eligible observations with embeddings | {inventory['audit_eligible_observations_with_embedding_count']} |",
        f"| Audit-eligible works with embeddings | {inventory['audit_eligible_unique_works_with_embedding_count']} |",
        f"| Product-candidate eval works | {inventory['product_candidate_pool_work_count']} |",
        f"| Product-candidate labeled eval works | {inventory['product_candidate_labeled_eval_work_count']} |",
        f"| Product-candidate unlabeled eval works | {inventory['product_candidate_unlabeled_eval_work_count']} |",
        f"| Train work estimate | {inventory['train_work_count_estimate']} |",
        f"| Train observation estimate | {inventory['train_observation_count_estimate']} |",
        f"| Eval observation estimate | {inventory['eval_observation_count_estimate']} |",
        f"| Eval positive observations | {inventory['eval_positive_observation_count']} |",
        f"| Eval negative observations | {inventory['eval_negative_observation_count']} |",
        f"| Duplicate work groups | {inventory['duplicate_work_group_count']} |",
        f"| Duplicate observation pressure | {inventory['duplicate_observation_pressure']} |",
        f"| Conflicting target work groups | {inventory['conflicting_target_work_group_count']} |",
        f"| Full-fit/eval overlap fixed by this policy | {inventory['overlap_work_count_between_full_fit_training_universe_and_eval_set']} |",
        "",
        "## Future Command Chain",
        "",
    ]
    for command in payload["future_commands"]:
        lines.append(f"- `{command['command']}`: {command['purpose']}")

    lines.extend(
        [
            "",
            "## What Still Blocks Shadow",
            "",
            f"- Independent validation complete: {blockers['independent_validation_complete']}",
            f"- Missing holdout assignment: {blockers['missing_holdout_assignment']}",
            f"- Missing holdout-bound scorer export: {blockers['missing_holdout_bound_scorer_export']}",
            f"- Missing product-candidate scoring v3: {blockers['missing_product_candidate_scoring_v3']}",
            f"- Missing metric gates v3: {blockers['missing_metric_gates_v3']}",
            f"- Missing `ml-shadow-scorer-v1`: {blockers['missing_ml_shadow_scorer_v1']}",
            f"- Shadow scoring authorized: {blockers['shadow_scoring_authorized']}",
            f"- Production default authorized: {blockers['production_default_authorized']}",
            "",
            "## Not Validation / Not Production Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_learned_scorer_holdout_policy(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    production_candidate_scoring_path: Path,
    production_candidate_metric_gates_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    policy_version: str = POLICY_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_learned_scorer_holdout_policy_payload(
        label_dataset_path=label_dataset_path,
        split_policy_path=split_policy_path,
        embeddings_path=embeddings_path,
        production_candidate_scoring_path=production_candidate_scoring_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        production_readiness_plan_path=production_readiness_plan_path,
        policy_version=policy_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_learned_scorer_holdout_policy(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "POLICY_VERSION",
    "MLLearnedScorerHoldoutPolicyError",
    "build_ml_learned_scorer_holdout_policy_payload",
    "markdown_from_ml_learned_scorer_holdout_policy",
    "write_ml_learned_scorer_holdout_policy",
]
