"""Materialize learned scorer holdout train/eval assignments.

This is an offline assignment artifact writer only. It does not train models,
refit scorers, generate embeddings, query databases, run ranking, implement
shadow scoring, or change production behavior.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_learned_scorer_holdout_policy import (
    EMBEDDING_ARTIFACT_TYPE,
    EMBEDDING_ARTIFACT_VERSION,
    LABEL_DATASET_VERSION,
    METRIC_GATES_ARTIFACT_TYPE,
    METRIC_GATES_VERSION,
    POLICY_VERSION as HOLDOUT_POLICY_VERSION,
    SCORING_ARTIFACT_TYPE,
    SCORING_MODE,
    SCORING_VERSION,
    SPLIT_POLICY_ARTIFACT_TYPE,
    SPLIT_POLICY_VERSION,
    TARGET_GOOD,
    _canonical_from_value,
    _canonical_work_id,
    _duplicate_conflict_rollups,
    _embedding_ok,
    _get,
    _is_explicit_label_row,
    _work_set_sha256,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_learned_scorer_holdout_assignment"
ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"
HOLDOUT_POLICY_ARTIFACT_TYPE = "ml_learned_scorer_holdout_policy"
STRATEGY_ID = "product_candidate_snapshot_holdout"
NEXT_AUTHORIZED_STEP = "ml-offline-audit-embedding-scorer-export-v2"
TRAIN_NEGATIVE_WORKS_ADVISORY_THRESHOLD = 10

CAVEATS = (
    "Not validation.",
    "Assignment materializes policy only; no model fit.",
    "Eval work set is full product snapshot; label metrics use labeled eval observations only.",
    "Train arm may be small; report class balance before export v2.",
    "Observation-level conflicts preserved.",
    "No shadow/production authorization.",
)


class MLLearnedScorerHoldoutAssignmentError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLLearnedScorerHoldoutAssignmentError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLLearnedScorerHoldoutAssignmentError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLLearnedScorerHoldoutAssignmentError(f"{name} JSON missing metadata object")
    return metadata


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLLearnedScorerHoldoutAssignmentError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _input_sha(metadata: Mapping[str, Any], name: str) -> str | None:
    inputs = metadata.get("inputs")
    if not isinstance(inputs, Sequence) or isinstance(inputs, (str, bytes)):
        return None
    for item in inputs:
        if isinstance(item, Mapping) and item.get("name") == name and isinstance(item.get("sha256"), str):
            return str(item["sha256"])
    return None


def _validate_policy_input_shas(
    policy_metadata: Mapping[str, Any],
    *,
    supplied: Mapping[str, str],
) -> None:
    for name, actual_sha in supplied.items():
        recorded_sha = _input_sha(policy_metadata, name)
        if recorded_sha is not None and recorded_sha != actual_sha:
            raise MLLearnedScorerHoldoutAssignmentError(
                f"holdout policy metadata.inputs {name!r} sha256 mismatch: expected {actual_sha!r}, got {recorded_sha!r}"
            )


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLearnedScorerHoldoutAssignmentError("label dataset missing rows array")
    normalized: list[dict[str, Any]] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLLearnedScorerHoldoutAssignmentError(f"label row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLLearnedScorerHoldoutAssignmentError(f"label row {idx} missing row_id")
        normalized.append(row)
    return normalized


def _validate_split_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    assertions = payload.get("policy_assertions")
    if not isinstance(assertions, Mapping):
        raise MLLearnedScorerHoldoutAssignmentError("split policy missing policy_assertions object")
    if assertions.get("requires_grouped_split_by_work") is not True:
        raise MLLearnedScorerHoldoutAssignmentError(
            "split policy policy_assertions.requires_grouped_split_by_work must be true"
        )
    if assertions.get("permits_row_level_random_split") is not False:
        raise MLLearnedScorerHoldoutAssignmentError(
            "split policy policy_assertions.permits_row_level_random_split must be false"
        )
    return metadata


def _validate_embeddings(
    payload: Mapping[str, Any],
    *,
    label_dataset_sha256: str,
) -> tuple[Mapping[str, Any], dict[str, dict[str, Any]], int | None]:
    metadata = _metadata(payload, name="embeddings")
    if metadata.get("artifact_type") != EMBEDDING_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected embeddings metadata.artifact_type={EMBEDDING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDING_ARTIFACT_VERSION:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected embeddings metadata.embedding_artifact_version={EMBEDDING_ARTIFACT_VERSION!r}, "
            f"got {metadata.get('embedding_artifact_version')!r}"
        )
    source_sha = metadata.get("source_label_dataset_sha256")
    if source_sha is not None and source_sha != label_dataset_sha256:
        raise MLLearnedScorerHoldoutAssignmentError(
            "embeddings metadata.source_label_dataset_sha256 must match supplied label dataset"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLearnedScorerHoldoutAssignmentError("embeddings missing rows array")
    expected_dim = metadata.get("embedding_dimensions")
    expected_dim_int = expected_dim if isinstance(expected_dim, int) and expected_dim > 0 else None
    by_id: dict[str, dict[str, Any]] = {}
    row_ids: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, Mapping):
            raise MLLearnedScorerHoldoutAssignmentError(f"embedding row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLLearnedScorerHoldoutAssignmentError(f"embedding row {idx} missing row_id")
        row_ids.append(row_id)
        by_id[row_id] = row
    duplicates = [row_id for row_id, count in Counter(row_ids).items() if count > 1]
    if duplicates:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"embeddings contain duplicate row_id values: {duplicates[:10]}"
        )
    return metadata, by_id, expected_dim_int


def _validate_scoring(payload: Mapping[str, Any], *, eval_work_ids: Sequence[str]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("scoring_mode") != SCORING_MODE:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected scoring metadata.scoring_mode={SCORING_MODE!r}, got {metadata.get('scoring_mode')!r}"
        )
    candidate_count = _get(payload, "candidate_pool_summary.candidate_unique_canonical_work_count")
    if isinstance(candidate_count, int) and candidate_count != len(eval_work_ids):
        raise MLLearnedScorerHoldoutAssignmentError(
            "computed eval work count must match scoring candidate_pool_summary.candidate_unique_canonical_work_count"
        )
    return metadata


def _candidate_eval_work_set(scoring_payload: Mapping[str, Any]) -> list[str]:
    rows = scoring_payload.get("candidate_pool_rows")
    if not isinstance(rows, list) or not rows:
        raise MLLearnedScorerHoldoutAssignmentError(
            "production candidate scoring candidate_pool_rows[] must exist; this assignment does not query DB"
        )
    missing: list[int] = []
    works: set[str] = set()
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            missing.append(idx)
            continue
        canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
        if canonical is None:
            missing.append(idx)
            continue
        works.add(canonical)
    if missing:
        raise MLLearnedScorerHoldoutAssignmentError(
            "production candidate scoring candidate_pool_rows[] must contain canonical_openalex_work_id values; "
            f"bad row indexes: {missing[:20]}"
        )
    return sorted(works)


def _validate_holdout_policy(
    payload: Mapping[str, Any],
    *,
    strategy_id: str,
    eval_work_ids: Sequence[str],
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-policy")
    if metadata.get("artifact_type") != HOLDOUT_POLICY_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected holdout policy metadata.artifact_type={HOLDOUT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != HOLDOUT_POLICY_VERSION:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected holdout policy metadata.policy_version={HOLDOUT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    if _get(payload, "primary_holdout_strategy.strategy_id") != STRATEGY_ID:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"holdout policy primary_holdout_strategy.strategy_id must be {STRATEGY_ID!r}"
        )
    if strategy_id != STRATEGY_ID:
        raise MLLearnedScorerHoldoutAssignmentError(f"unsupported strategy_id: {strategy_id!r}")
    eval_sha = _work_set_sha256(eval_work_ids)
    policy_inventory_sha = _get(payload, "dataset_inventory.product_candidate_eval_work_set_sha256")
    policy_strategy_sha = _get(payload, "primary_holdout_strategy.eval_work_set_definition.eval_work_set_sha256")
    if policy_inventory_sha != eval_sha:
        raise MLLearnedScorerHoldoutAssignmentError(
            "eval_work_set_sha256 mismatch vs holdout policy dataset_inventory"
        )
    if policy_strategy_sha != eval_sha:
        raise MLLearnedScorerHoldoutAssignmentError(
            "eval_work_set_sha256 mismatch vs holdout policy primary_holdout_strategy"
        )
    policy_count = _get(payload, "dataset_inventory.product_candidate_pool_work_count")
    if policy_count != len(eval_work_ids):
        raise MLLearnedScorerHoldoutAssignmentError(
            "eval work count must match holdout policy dataset_inventory.product_candidate_pool_work_count"
        )
    return metadata


def _validate_metric_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates")
    if metadata.get("artifact_type") != METRIC_GATES_ARTIFACT_TYPE:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected metric gates metadata.artifact_type={METRIC_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != METRIC_GATES_VERSION:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"expected metric gates metadata.gates_version={METRIC_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("learned_scorer_application_gates_passed") is not True:
        raise MLLearnedScorerHoldoutAssignmentError("metric gates learned_scorer_application_gates_passed must be true")
    if payload.get("independent_learned_validation_passed") is not False:
        raise MLLearnedScorerHoldoutAssignmentError("metric gates independent_learned_validation_passed must be false")
    return metadata


def _select_seed(
    *,
    explicit_seed: int | None,
    holdout_policy_payload: Mapping[str, Any],
    split_policy_payload: Mapping[str, Any],
) -> int:
    if explicit_seed is not None:
        return int(explicit_seed)
    policy_seed = _get(holdout_policy_payload, "randomness_policy.recommended_default_seed")
    if isinstance(policy_seed, int):
        return policy_seed
    split_seed = _get(split_policy_payload, "randomness_policy.recommended_default_seed")
    if isinstance(split_seed, int):
        return split_seed
    return 20260515


def _eligible_assignable_rows(
    *,
    label_rows: Sequence[dict[str, Any]],
    embeddings_by_id: Mapping[str, dict[str, Any]],
    expected_dim: int | None,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, list[str]]]:
    eligible_candidates: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    previews: dict[str, list[str]] = defaultdict(list)

    def exclude(reason: str, row_id: str) -> None:
        excluded[reason] += 1
        if len(previews[reason]) < 20:
            previews[reason].append(row_id)

    for idx, row in enumerate(label_rows, start=1):
        row_id = str(row.get("row_id") or "").strip()
        if str(row.get("split") or "").strip() != "audit_only":
            exclude("split_not_audit_only", row_id or f"row_{idx}")
            continue
        if not _is_explicit_label_row(row):
            exclude("no_explicit_manual_label", row_id or f"row_{idx}")
            continue
        if not isinstance(row.get(TARGET_GOOD), bool):
            exclude("target_not_boolean", row_id or f"row_{idx}")
            continue
        canonical = _canonical_work_id(row)
        if canonical is None:
            exclude("missing_canonical_work_id", row_id or f"row_{idx}")
            continue
        eligible_candidates.append(
            {
                "row": row,
                "row_id": row_id,
                "canonical_openalex_work_id": canonical,
                "good_or_acceptable": bool(row[TARGET_GOOD]),
            }
        )

    candidate_ids = [str(row["row_id"]) for row in eligible_candidates]
    duplicates = [row_id for row_id, count in Counter(candidate_ids).items() if count > 1]
    if duplicates:
        raise MLLearnedScorerHoldoutAssignmentError(
            f"label dataset contains duplicate row_id values among assignment-eligible rows: {duplicates[:10]}"
        )

    eligible: list[dict[str, Any]] = []
    for item in eligible_candidates:
        row_id = str(item["row_id"])
        embedding = embeddings_by_id.get(row_id)
        if not _embedding_ok(embedding, expected_dim=expected_dim):
            exclude("missing_or_bad_embedding", row_id)
            continue
        out = dict(item)
        out["embedding_status"] = str(embedding.get("embedding_status"))
        eligible.append(out)
    return (
        sorted(eligible, key=lambda item: str(item["row_id"])),
        dict(sorted(excluded.items())),
        {key: values for key, values in sorted(previews.items())},
    )


def _build_assignments(
    *,
    eligible_rows: Sequence[Mapping[str, Any]],
    eval_work_ids: set[str],
) -> list[dict[str, Any]]:
    assignments: list[dict[str, Any]] = []
    for item in eligible_rows:
        row = item["row"]
        canonical = str(item["canonical_openalex_work_id"])
        assignment = "eval" if canonical in eval_work_ids else "train"
        assignments.append(
            {
                "row_id": item["row_id"],
                "canonical_openalex_work_id": canonical,
                "assignment": assignment,
                "good_or_acceptable": bool(item["good_or_acceptable"]),
                "embedding_status": item["embedding_status"],
                "review_pool_variant": row.get("review_pool_variant"),
                "family": row.get("family"),
                "ranking_run_id": row.get("ranking_run_id"),
            }
        )
    return sorted(assignments, key=lambda item: (str(item["canonical_openalex_work_id"]), str(item["row_id"])))


def _group_assignment_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row["canonical_openalex_work_id"])].append(row)
    return {key: sorted(value, key=lambda item: str(item["row_id"])) for key, value in sorted(groups.items())}


def _work_assignments(assignments: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for work_id, rows in _group_assignment_rows(assignments).items():
        assignment_values = {str(row["assignment"]) for row in rows}
        positives = sum(1 for row in rows if row["good_or_acceptable"] is True)
        negatives = len(rows) - positives
        out.append(
            {
                "canonical_openalex_work_id": work_id,
                "assignment": sorted(assignment_values)[0] if len(assignment_values) == 1 else "mixed",
                "observation_count": len(rows),
                "positive_observation_count": positives,
                "negative_observation_count": negatives,
                "conflicting_target_observations": positives > 0 and negatives > 0,
                "row_ids": [str(row["row_id"]) for row in rows],
            }
        )
    return out


def _obs_balance(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    positive = sum(1 for row in rows if row["good_or_acceptable"] is True)
    negative = count - positive
    return {
        "count": count,
        "positive": positive,
        "negative": negative,
        "positive_rate": (positive / count) if count else None,
    }


def _work_any_positive_balance(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    groups = _group_assignment_rows(rows)
    positive_works = 0
    negative_works = 0
    conflicts = 0
    for group_rows in groups.values():
        values = [bool(row["good_or_acceptable"]) for row in group_rows]
        if any(values):
            positive_works += 1
        else:
            negative_works += 1
        if any(values) and not all(values):
            conflicts += 1
    return {
        "work_count": len(groups),
        "positive_works": positive_works,
        "negative_works": negative_works,
        "conflicting_target_work_group_count": conflicts,
    }


def _work_majority_balance(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    groups = _group_assignment_rows(rows)
    positive_works = 0
    negative_works = 0
    ties = 0
    for group_rows in groups.values():
        positive = sum(1 for row in group_rows if row["good_or_acceptable"] is True)
        negative = len(group_rows) - positive
        if positive > negative:
            positive_works += 1
        elif negative > positive:
            negative_works += 1
        else:
            ties += 1
    return {
        "work_count": len(groups),
        "positive_works": positive_works,
        "negative_works": negative_works,
        "tie_works": ties,
    }


def _class_balance(assignments: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    train = [row for row in assignments if row["assignment"] == "train"]
    eval_rows = [row for row in assignments if row["assignment"] == "eval"]
    train_work_any = _work_any_positive_balance(train)
    return {
        "observation_level": {
            "train": _obs_balance(train),
            "eval": _obs_balance(eval_rows),
        },
        "work_level_any_positive": {
            "train": train_work_any,
            "eval": _work_any_positive_balance(eval_rows),
        },
        "work_level_majority_vote_non_tie": {
            "train": _work_majority_balance(train),
            "eval": _work_majority_balance(eval_rows),
        },
        "train_negative_works_below_threshold": (
            train_work_any["negative_works"] < TRAIN_NEGATIVE_WORKS_ADVISORY_THRESHOLD
        ),
    }


def _duplicate_conflict_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    groups = _group_assignment_rows(rows)
    duplicate_groups = 0
    duplicate_pressure = 0
    conflicts = 0
    for group_rows in groups.values():
        if len(group_rows) > 1:
            duplicate_groups += 1
            duplicate_pressure += len(group_rows) - 1
        values = [bool(row["good_or_acceptable"]) for row in group_rows]
        if any(values) and not all(values):
            conflicts += 1
    return {
        "duplicate_work_group_count": duplicate_groups,
        "duplicate_observation_pressure": duplicate_pressure,
        "conflicting_target_work_group_count": conflicts,
    }


def _duplicate_and_conflict_report(
    *,
    label_payload: Mapping[str, Any],
    assignments: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    train = [row for row in assignments if row["assignment"] == "train"]
    eval_rows = [row for row in assignments if row["assignment"] == "eval"]
    groups = _group_assignment_rows(assignments)
    conflict_ids = []
    for work_id, rows in groups.items():
        values = [bool(row["good_or_acceptable"]) for row in rows]
        if any(values) and not all(values):
            conflict_ids.append(work_id)
    return {
        "global": _duplicate_conflict_counts(assignments),
        "train": _duplicate_conflict_counts(train),
        "eval": _duplicate_conflict_counts(eval_rows),
        "duplicate_conflict_rollups_from_label_dataset_metadata": _duplicate_conflict_rollups(label_payload),
        "conflicting_work_ids_preview": conflict_ids[:20],
    }


def _leakage_report(
    *,
    assignments: Sequence[Mapping[str, Any]],
    eval_work_ids: set[str],
    work_assignments: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    train_rows = [row for row in assignments if row["assignment"] == "train"]
    eval_rows = [row for row in assignments if row["assignment"] == "eval"]
    train_work_ids = {str(row["canonical_openalex_work_id"]) for row in train_rows}
    assigned_eval_work_ids = {str(row["canonical_openalex_work_id"]) for row in eval_rows}
    train_row_ids = {str(row["row_id"]) for row in train_rows}
    eval_row_ids = {str(row["row_id"]) for row in eval_rows}
    per_work_violations = [
        {
            "canonical_openalex_work_id": row["canonical_openalex_work_id"],
            "assignment": row["assignment"],
        }
        for row in work_assignments
        if row["assignment"] not in {"train", "eval"}
    ]
    work_overlap = len(train_work_ids & eval_work_ids)
    row_overlap = len(train_row_ids & eval_row_ids)
    return {
        "train_unique_work_count": len(train_work_ids),
        "eval_unique_work_count": len(eval_work_ids),
        "assigned_eval_unique_work_count": len(assigned_eval_work_ids),
        "train_eval_work_overlap_count": work_overlap,
        "train_eval_row_id_overlap_count": row_overlap,
        "global_zero_assertion": work_overlap == 0 and row_overlap == 0 and not per_work_violations,
        "per_work_violations": per_work_violations,
    }


def _dataset_inventory(
    *,
    assignments: Sequence[Mapping[str, Any]],
    eval_work_ids: set[str],
    eval_work_set_sha: str,
) -> dict[str, Any]:
    train_rows = [row for row in assignments if row["assignment"] == "train"]
    eval_rows = [row for row in assignments if row["assignment"] == "eval"]
    train_work_ids = {str(row["canonical_openalex_work_id"]) for row in train_rows}
    eval_assigned_work_ids = {str(row["canonical_openalex_work_id"]) for row in eval_rows}
    return {
        "audit_eligible_observation_count": len(assignments),
        "assigned_train_observation_count": len(train_rows),
        "assigned_eval_observation_count": len(eval_rows),
        "assigned_train_work_count": len(train_work_ids),
        "assigned_eval_work_count": len(eval_assigned_work_ids),
        "product_candidate_unlabeled_eval_work_count": len(eval_work_ids - eval_assigned_work_ids),
        "overlap_work_count_between_full_fit_training_universe_and_eval_set": len(train_work_ids & eval_work_ids),
        "eval_work_set_sha256": eval_work_set_sha,
    }


def build_ml_learned_scorer_holdout_assignment_payload(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    production_candidate_scoring_path: Path,
    holdout_policy_path: Path,
    production_candidate_metric_gates_path: Path,
    assignment_version: str = ASSIGNMENT_VERSION,
    strategy_id: str = STRATEGY_ID,
    seed: int | None = None,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    split_path = Path(split_policy_path).resolve()
    embeddings_resolved = Path(embeddings_path).resolve()
    scoring_path = Path(production_candidate_scoring_path).resolve()
    policy_path = Path(holdout_policy_path).resolve()
    gates_path = Path(production_candidate_metric_gates_path).resolve()

    label_payload = _load_json_object(label_path)
    split_payload = _load_json_object(split_path)
    embeddings_payload = _load_json_object(embeddings_resolved)
    scoring_payload = _load_json_object(scoring_path)
    policy_payload = _load_json_object(policy_path)
    gates_payload = _load_json_object(gates_path)

    label_sha = sha256_file(label_path)
    split_sha = sha256_file(split_path)
    embeddings_sha = sha256_file(embeddings_resolved)
    scoring_sha = sha256_file(scoring_path)
    policy_sha = sha256_file(policy_path)
    gates_sha = sha256_file(gates_path)

    label_rows = _validate_label_dataset(label_payload)
    split_metadata = _validate_split_policy(split_payload)
    embeddings_metadata, embeddings_by_id, expected_dim = _validate_embeddings(
        embeddings_payload,
        label_dataset_sha256=label_sha,
    )
    eval_work_ids = _candidate_eval_work_set(scoring_payload)
    scoring_metadata = _validate_scoring(scoring_payload, eval_work_ids=eval_work_ids)
    policy_metadata = _validate_holdout_policy(policy_payload, strategy_id=strategy_id, eval_work_ids=eval_work_ids)
    gates_metadata = _validate_metric_gates(gates_payload)
    _validate_policy_input_shas(
        policy_metadata,
        supplied={
            "label_dataset": label_sha,
            "split_policy": split_sha,
            "embeddings": embeddings_sha,
            "production_candidate_scoring": scoring_sha,
            "production_candidate_metric_gates": gates_sha,
        },
    )

    chosen_seed = _select_seed(
        explicit_seed=seed,
        holdout_policy_payload=policy_payload,
        split_policy_payload=split_payload,
    )
    eval_work_set_sha = _work_set_sha256(eval_work_ids)
    eval_work_id_set = set(eval_work_ids)
    eligible_rows, excluded, excluded_previews = _eligible_assignable_rows(
        label_rows=label_rows,
        embeddings_by_id=embeddings_by_id,
        expected_dim=expected_dim,
    )
    assignments = _build_assignments(eligible_rows=eligible_rows, eval_work_ids=eval_work_id_set)
    work_rows = _work_assignments(assignments)
    leakage = _leakage_report(
        assignments=assignments,
        eval_work_ids=eval_work_id_set,
        work_assignments=work_rows,
    )
    class_balance = _class_balance(assignments)
    duplicate_report = _duplicate_and_conflict_report(label_payload=label_payload, assignments=assignments)
    inventory = _dataset_inventory(
        assignments=assignments,
        eval_work_ids=eval_work_id_set,
        eval_work_set_sha=eval_work_set_sha,
    )

    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("split_policy", split_path, repo_root=root),
        _input_record("embeddings", embeddings_resolved, repo_root=root),
        _input_record("production_candidate_scoring", scoring_path, repo_root=root),
        _input_record("holdout_policy", policy_path, repo_root=root),
        _input_record("production_candidate_metric_gates", gates_path, repo_root=root),
    ]
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "assignment_version": assignment_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "label_dataset_version": label_payload.get("dataset_version"),
        "split_policy_version": split_metadata.get("policy_version"),
        "embedding_artifact_version": embeddings_metadata.get("embedding_artifact_version"),
        "holdout_policy_version": policy_metadata.get("policy_version"),
        "holdout_policy_sha256": policy_sha,
        "production_candidate_scoring_version": scoring_metadata.get("experiment_version"),
        "production_candidate_metric_gates_version": gates_metadata.get("gates_version"),
        "strategy_id": strategy_id,
        "seed": chosen_seed,
        "target": TARGET_GOOD,
        "grouping_key": "canonical_openalex_work_id",
        "ranking_run_id": scoring_metadata.get("ranking_run_id")
        or _get(scoring_payload, "candidate_pool_definition.ranking_run_id"),
        "family": scoring_metadata.get("family") or _get(scoring_payload, "candidate_pool_definition.family"),
        "eval_work_set_sha256": eval_work_set_sha,
        "eval_work_count": len(eval_work_ids),
        "thresholds": {
            "minimum_train_negative_works_threshold_for_advisory": TRAIN_NEGATIVE_WORKS_ADVISORY_THRESHOLD,
        },
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "policy_compliance": {
            "grouped_by_work": True,
            "row_level_random_split_used": False,
            "silent_conflict_merge_used": False,
            "eval_work_set_matches_holdout_policy": True,
            "train_eval_work_overlap_count": leakage["train_eval_work_overlap_count"],
            "holdout_assignment_materialized": True,
        },
        "leakage_report": leakage,
        "class_balance": class_balance,
        "duplicate_and_conflict_report": duplicate_report,
        "assignments": assignments,
        "work_assignments": work_rows,
        "excluded_rows_by_reason": excluded,
        "excluded_row_id_previews_by_reason": excluded_previews,
        "dataset_inventory": inventory,
        "interpretation": {
            "summary": (
                "Materialized per-row and per-work holdout assignments for the learned scorer product-candidate "
                "snapshot boundary."
            ),
            "next_authorized_step": NEXT_AUTHORIZED_STEP,
            "not_claimed": [
                "validation",
                "shadow readiness",
                "production readiness",
                "live recommender quality",
            ],
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_authorized": False,
            "production_default_authorized": False,
            "independent_validation_complete": False,
            "missing_holdout_assignment": False,
            "missing_holdout_bound_scorer_export": True,
            "missing_product_candidate_scoring_v3": True,
            "missing_metric_gates_v3": True,
            "missing_ml_shadow_scorer_v1": True,
        },
    }


def _fmt_rate(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_learned_scorer_holdout_assignment(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    leakage = payload["leakage_report"]
    balance = payload["class_balance"]
    dup = payload["duplicate_and_conflict_report"]
    inventory = payload["dataset_inventory"]
    train_obs = balance["observation_level"]["train"]
    eval_obs = balance["observation_level"]["eval"]
    train_work = balance["work_level_any_positive"]["train"]
    eval_work = balance["work_level_any_positive"]["eval"]

    lines = [
        f"# Learned Scorer Holdout Assignment ({metadata['assignment_version']})",
        "",
        "## Executive Summary",
        "",
        "This materializes the holdout policy into deterministic per-row and per-work assignments. It does not train or refit a scorer, rerun product scoring, or authorize shadow or production use.",
        "",
        f"- **Strategy:** `{metadata['strategy_id']}`",
        f"- **Eval work count:** {metadata['eval_work_count']}",
        f"- **Eval work-set SHA256:** `{metadata['eval_work_set_sha256']}`",
        f"- **Train/eval work overlap:** {leakage['train_eval_work_overlap_count']}",
        f"- **Global zero leakage assertion:** {leakage['global_zero_assertion']}",
        "",
        "## Strategy And Eval Work-Set Source",
        "",
        f"The eval set is all unique canonical works from `ml-offline-production-candidate-scoring-v2` candidate rows for ranking run `{metadata['ranking_run_id']}` and family `{metadata['family']}`.",
        "",
        "## Leakage Report",
        "",
        "| Measure | Value |",
        "| --- | ---: |",
        f"| Train unique works | {leakage['train_unique_work_count']} |",
        f"| Eval unique works | {leakage['eval_unique_work_count']} |",
        f"| Assigned eval unique works | {leakage['assigned_eval_unique_work_count']} |",
        f"| Train/eval work overlap | {leakage['train_eval_work_overlap_count']} |",
        f"| Train/eval row_id overlap | {leakage['train_eval_row_id_overlap_count']} |",
        f"| Per-work violations | {len(leakage['per_work_violations'])} |",
        "",
        "## Class Balance",
        "",
        "### Observation Level",
        "",
        "| Assignment | Count | Positive | Negative | Positive Rate |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| Train | {train_obs['count']} | {train_obs['positive']} | {train_obs['negative']} | {_fmt_rate(train_obs['positive_rate'])} |",
        f"| Eval | {eval_obs['count']} | {eval_obs['positive']} | {eval_obs['negative']} | {_fmt_rate(eval_obs['positive_rate'])} |",
        "",
        "### Work Level Any-Positive",
        "",
        "| Assignment | Works | Positive Works | Negative Works | Conflicting Works |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| Train | {train_work['work_count']} | {train_work['positive_works']} | {train_work['negative_works']} | {train_work['conflicting_target_work_group_count']} |",
        f"| Eval | {eval_work['work_count']} | {eval_work['positive_works']} | {eval_work['negative_works']} | {eval_work['conflicting_target_work_group_count']} |",
        "",
        "## Duplicate/Conflict Summary",
        "",
        "| Scope | Duplicate Work Groups | Duplicate Pressure | Conflicting Work Groups |",
        "| --- | ---: | ---: | ---: |",
    ]
    for scope in ("global", "train", "eval"):
        row = dup[scope]
        lines.append(
            f"| {scope.title()} | {row['duplicate_work_group_count']} | {row['duplicate_observation_pressure']} | {row['conflicting_target_work_group_count']} |"
        )

    lines.extend(
        [
            "",
            "## Train Negative Advisory",
            "",
            f"- **Threshold:** {metadata['thresholds']['minimum_train_negative_works_threshold_for_advisory']} train negative works",
            f"- **Triggered:** {balance['train_negative_works_below_threshold']}",
            "",
            "## Assignments Overview",
            "",
            f"- **Eligible assigned rows:** {inventory['audit_eligible_observation_count']}",
            f"- **Train rows:** {inventory['assigned_train_observation_count']}",
            f"- **Eval rows:** {inventory['assigned_eval_observation_count']}",
            f"- **Train works:** {inventory['assigned_train_work_count']}",
            f"- **Assigned eval works:** {inventory['assigned_eval_work_count']}",
            f"- **Unlabeled eval works reserved from training:** {inventory['product_candidate_unlabeled_eval_work_count']}",
            "",
            "## Next Step",
            "",
            f"`{payload['interpretation']['next_authorized_step']}`",
            "",
            "## Not Validation / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.extend(
        [
            "- Shadow scoring authorized: False",
            "- Production default authorized: False",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_learned_scorer_holdout_assignment(
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    embeddings_path: Path,
    production_candidate_scoring_path: Path,
    holdout_policy_path: Path,
    production_candidate_metric_gates_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    assignment_version: str = ASSIGNMENT_VERSION,
    strategy_id: str = STRATEGY_ID,
    seed: int | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_learned_scorer_holdout_assignment_payload(
        label_dataset_path=label_dataset_path,
        split_policy_path=split_policy_path,
        embeddings_path=embeddings_path,
        production_candidate_scoring_path=production_candidate_scoring_path,
        holdout_policy_path=holdout_policy_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        assignment_version=assignment_version,
        strategy_id=strategy_id,
        seed=seed,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_learned_scorer_holdout_assignment(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "ASSIGNMENT_VERSION",
    "MLLearnedScorerHoldoutAssignmentError",
    "build_ml_learned_scorer_holdout_assignment_payload",
    "markdown_from_ml_learned_scorer_holdout_assignment",
    "write_ml_learned_scorer_holdout_assignment",
]
