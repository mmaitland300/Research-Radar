"""Production-candidate offline scoring diagnostic over an existing paper_scores pool.

This command is read-only: it reuses a completed ranking run, joins v8 audit
labels by canonical OpenAlex work id, and reports heuristic final_score metrics
for the labeled overlap. It does not run ranking, train on product candidates,
or write a model artifact.
"""

from __future__ import annotations

import json
import hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_label_split_policy import canonical_openalex_work_id
from pipeline.ml_offline_baseline_eval import (
    MLOfflineBaselineEvalError,
    fetch_ranking_run_row,
    precision_at_k,
    roc_auc_mann_whitney,
    sha256_file,
)
from pipeline.ml_offline_audit_embedding_scorer_export import score_audit_embedding_probability
from pipeline.recommendation_review_worksheet import cluster_version_from_config
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
EXPERIMENT_VERSION = "ml-offline-production-candidate-scoring-v1"
EXPERIMENT_VERSION_V2 = "ml-offline-production-candidate-scoring-v2"
EXPERIMENT_VERSION_V3 = "ml-offline-production-candidate-scoring-v3"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
SPLIT_POLICY_ARTIFACT_TYPE = "ml_label_split_policy"
SPLIT_POLICY_VERSION = "ml-label-split-policy-v1"
METRIC_GATES_ARTIFACT_TYPE = "ml_offline_metric_gates"
METRIC_GATES_VERSION = "ml-offline-metric-gates-v1"
RANKER_ARTIFACT_TYPE = "ml_offline_ranker_experiment"
RANKER_VERSION = "ml-offline-ranker-experiment-v1"
EMBEDDINGS_ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDINGS_VERSION = "ml-labeled-text-embeddings-v3"
AUDIT_SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
AUDIT_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v1"
AUDIT_SCORER_VERSION_V2 = "ml-offline-audit-embedding-scorer-v2"
HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE = "ml_learned_scorer_holdout_assignment"
HOLDOUT_ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"
HOLDOUT_POLICY_ARTIFACT_TYPE = "ml_learned_scorer_holdout_policy"
HOLDOUT_POLICY_VERSION = "ml-learned-scorer-holdout-policy-v1"
HOLDOUT_STRATEGY_ID = "product_candidate_snapshot_holdout"
PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
PRODUCT_CANDIDATE_GATES_VERSION_V2 = "ml-offline-production-candidate-metric-gates-v2"
TARGET_GOOD = "good_or_acceptable"
FORBIDDEN_TARGET = "surprising_or_useful"
DEFAULT_FAMILY = "emerging"
ALLOWED_FAMILIES = frozenset({DEFAULT_FAMILY})
SCORING_MODE_HEURISTIC = "heuristic_and_coverage_only"
SCORING_MODE_AUDIT_EMBEDDING = "heuristic_and_audit_embedding_scorer"
SCORING_MODE_HOLDOUT_EMBEDDING = "heuristic_and_holdout_embedding_scorer"
SCORING_MODES = frozenset({SCORING_MODE_HEURISTIC, SCORING_MODE_AUDIT_EMBEDDING, SCORING_MODE_HOLDOUT_EMBEDDING})
LEARNED_UNAVAILABLE_REASON = (
    "ml-offline-ranker-experiment-v1 contains per-fold coefficients only; "
    "no frozen full-fit audit scorer export exists."
)
LEARNED_SCORE_AGGREGATION_POLICY = "max_probability"
LEARNED_METRIC_THRESHOLDS = {
    "minimum_learned_roc_auc": 0.70,
    "minimum_learned_average_precision": 0.85,
    "minimum_learned_precision_at_10": 0.80,
}
K_VALUES = (5, 10, 20)
RANK_BUCKETS: tuple[tuple[int, int | None], ...] = ((1, 5), (6, 10), (11, 20), (21, 50), (51, 100), (101, None))

CAVEATS = (
    "Not validation.",
    "Product-candidate offline diagnostic only.",
    "Existing ranking run reused read-only; no new ranking was run.",
    "Single-reviewer audit labels.",
    "Label coverage is incomplete and may bias metrics.",
    "No production model artifact.",
    "No shadow scoring or production default change.",
)

BLOCKERS_TO_SHADOW = (
    "product-candidate metric gates not yet evaluated",
    "no ml-shadow-scorer-v1 contract exists",
    "production default blocked by readiness plan",
    "no production model artifact exists",
)

LEARNED_BLOCKERS_TO_SHADOW = (
    "product-candidate learned metric gates not yet evaluated",
    "no ml-shadow-scorer-v1 contract exists",
    "production default blocked by readiness plan",
    "no production model artifact exists",
)

LEARNED_CAVEATS = (
    "Not validation.",
    "Frozen audit scorer trained on audit corpus only.",
    "Product pool application is offline diagnostic only.",
    "Product candidates were not used for training.",
    "Learned audit scorer and heuristic final_score are separate evidence lines.",
    "No shadow scoring or production default change.",
    "No ranking/API/web changes.",
)

HOLDOUT_LEARNED_CAVEATS = (
    "Not live recommender validation.",
    "Held out relative to scorer v2 train works; still single-reviewer audit labels.",
    "One frozen ranking run/family.",
    "Positive-heavy eval may inflate P@k.",
    "No shadow/production authorization.",
)

HOLDOUT_LEARNED_BLOCKERS_TO_SHADOW = (
    "product-candidate metric gates v3 not yet evaluated",
    "no ml-shadow-scorer-v1 contract exists",
    "production default blocked by readiness plan",
    "no production model artifact exists",
)


class MLOfflineProductionCandidateScoringError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineProductionCandidateScoringError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineProductionCandidateScoringError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLOfflineProductionCandidateScoringError(f"{name} JSON missing metadata object")
    return metadata


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLOfflineProductionCandidateScoringError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _input_by_name(payload: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    metadata = payload.get("metadata")
    inputs = metadata.get("inputs") if isinstance(metadata, Mapping) else None
    if not isinstance(inputs, Sequence) or isinstance(inputs, (str, bytes)):
        return None
    for item in inputs:
        if isinstance(item, Mapping) and item.get("name") == name:
            return item
    return None


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineProductionCandidateScoringError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _validate_split_policy(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, "
            f"got {metadata.get('policy_version')!r}"
        )
    if payload.get("allowed_targets_for_v1_split") != [TARGET_GOOD]:
        raise MLOfflineProductionCandidateScoringError("split policy allowed_targets_for_v1_split must be ['good_or_acceptable']")
    forbidden = payload.get("forbidden_targets")
    if not isinstance(forbidden, list) or FORBIDDEN_TARGET not in forbidden:
        raise MLOfflineProductionCandidateScoringError("split policy forbidden_targets must include surprising_or_useful")


def _validate_metric_gates(
    payload: Mapping[str, Any],
    *,
    audit_ranker_experiment_path: Path,
    audit_ranker_experiment_sha256: str,
    repo_root: Path,
) -> None:
    metadata = _metadata(payload, name="metric-gates")
    if metadata.get("artifact_type") != METRIC_GATES_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected metric gates metadata.artifact_type={METRIC_GATES_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != METRIC_GATES_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected metric gates metadata.gates_version={METRIC_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("audit_ranker_gates_passed") is not True:
        raise MLOfflineProductionCandidateScoringError("metric gates audit_ranker_gates_passed must be true")
    if payload.get("recommended_next_stage") != "proceed_to_production_candidate_offline_scoring":
        raise MLOfflineProductionCandidateScoringError(
            "metric gates recommended_next_stage must be proceed_to_production_candidate_offline_scoring"
        )
    if payload.get("shadow_scoring_allowed") is not False:
        raise MLOfflineProductionCandidateScoringError("metric gates shadow_scoring_allowed must be false")
    if payload.get("production_default_allowed") is not False:
        raise MLOfflineProductionCandidateScoringError("metric gates production_default_allowed must be false")

    inputs = metadata.get("inputs")
    if not isinstance(inputs, Sequence) or isinstance(inputs, (str, bytes)):
        raise MLOfflineProductionCandidateScoringError("metric gates metadata.inputs must include ranker_experiment")
    portable_ranker_path = portable_repo_path(audit_ranker_experiment_path.resolve(), repo_root=repo_root)
    ranker_inputs = [
        item
        for item in inputs
        if isinstance(item, Mapping)
        and (
            item.get("name") in {"ranker_experiment", "audit_ranker_experiment"}
            or str(item.get("path") or "").endswith("ml-offline-ranker-experiment-v1.json")
        )
    ]
    if not ranker_inputs:
        raise MLOfflineProductionCandidateScoringError("metric gates inputs missing ranker experiment record")
    consistent = any(
        item.get("sha256") == audit_ranker_experiment_sha256 or item.get("path") == portable_ranker_path
        for item in ranker_inputs
    )
    if not consistent:
        raise MLOfflineProductionCandidateScoringError("metric gates ranker experiment input SHA/path does not match supplied audit ranker experiment")


def _validate_audit_ranker_experiment(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="audit-ranker-experiment")
    if metadata.get("artifact_type") != RANKER_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit ranker metadata.artifact_type={RANKER_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != RANKER_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit ranker metadata.experiment_version={RANKER_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )


def _validate_embeddings(
    payload: Mapping[str, Any],
    *,
    label_dataset_sha256: str,
    label_dataset_version: str,
) -> tuple[Mapping[str, Any], dict[str, dict[str, Any]]]:
    metadata = _metadata(payload, name="embeddings")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected embeddings metadata.artifact_type={EMBEDDINGS_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("embedding_artifact_version") != EMBEDDINGS_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected embeddings metadata.embedding_artifact_version={EMBEDDINGS_VERSION!r}, "
            f"got {metadata.get('embedding_artifact_version')!r}"
        )
    if metadata.get("source_label_dataset_sha256") != label_dataset_sha256:
        raise MLOfflineProductionCandidateScoringError("embeddings source_label_dataset_sha256 must match supplied label dataset")
    if metadata.get("source_label_dataset_version") != label_dataset_version:
        raise MLOfflineProductionCandidateScoringError("embeddings source_label_dataset_version must match supplied label dataset")
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLOfflineProductionCandidateScoringError("embeddings missing rows array")
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            continue
        if row_id in by_id:
            raise MLOfflineProductionCandidateScoringError(f"embeddings contains duplicate row_id: {row_id}")
        by_id[row_id] = dict(row)
    return metadata, by_id


def _input_sha_matches(payload: Mapping[str, Any], names: set[str], expected_sha: str) -> bool:
    metadata = payload.get("metadata")
    inputs = metadata.get("inputs") if isinstance(metadata, Mapping) else None
    if not isinstance(inputs, Sequence) or isinstance(inputs, (str, bytes)):
        return False
    for item in inputs:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("name") or "") in names and item.get("sha256") == expected_sha:
            return True
    return False


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()}))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _validate_audit_embedding_scorer(
    payload: Mapping[str, Any],
    *,
    label_dataset_sha256: str,
    embeddings_sha256: str,
    embedding_dimensions: Any,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export")
    if metadata.get("artifact_type") != AUDIT_SCORER_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit scorer metadata.artifact_type={AUDIT_SCORER_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("scorer_version") != AUDIT_SCORER_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit scorer metadata.scorer_version={AUDIT_SCORER_VERSION!r}, got {metadata.get('scorer_version')!r}"
        )
    if metadata.get("target") != TARGET_GOOD:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit scorer metadata.target={TARGET_GOOD!r}, got {metadata.get('target')!r}"
        )
    if metadata.get("fit_mode") != "full_fit_audit_corpus":
        raise MLOfflineProductionCandidateScoringError("audit scorer metadata.fit_mode must be full_fit_audit_corpus")
    policy = payload.get("policy_compliance")
    if not isinstance(policy, Mapping):
        raise MLOfflineProductionCandidateScoringError("audit scorer missing policy_compliance object")
    if policy.get("shadow_scoring_authorized") is not False:
        raise MLOfflineProductionCandidateScoringError("audit scorer shadow_scoring_authorized must be false")
    if policy.get("product_candidate_pool_used_for_training") is not False:
        raise MLOfflineProductionCandidateScoringError("audit scorer product_candidate_pool_used_for_training must be false")
    if policy.get("production_artifact_written") is not False:
        raise MLOfflineProductionCandidateScoringError("audit scorer production_artifact_written must be false")

    scorer_dimensions = metadata.get("embedding_dimensions")
    if scorer_dimensions is None:
        scorer_block = payload.get("scorer")
        scaler_block = scorer_block.get("scaler") if isinstance(scorer_block, Mapping) else None
        scorer_dimensions = scaler_block.get("feature_count") if isinstance(scaler_block, Mapping) else None
    if not isinstance(embedding_dimensions, int) or embedding_dimensions <= 0:
        raise MLOfflineProductionCandidateScoringError("embeddings metadata.embedding_dimensions is required in learned mode")
    if scorer_dimensions != embedding_dimensions:
        raise MLOfflineProductionCandidateScoringError(
            f"audit scorer embedding dimensions {scorer_dimensions!r} do not match embeddings metadata.embedding_dimensions "
            f"{embedding_dimensions!r}"
        )

    direct_label_sha = metadata.get("label_dataset_sha256")
    direct_embeddings_sha = metadata.get("embedding_artifact_sha256")
    label_ok = (
        direct_label_sha == label_dataset_sha256
        if direct_label_sha is not None
        else _input_sha_matches(payload, {"label_dataset"}, label_dataset_sha256)
    )
    embeddings_ok = (
        direct_embeddings_sha == embeddings_sha256
        if direct_embeddings_sha is not None
        else _input_sha_matches(payload, {"embeddings"}, embeddings_sha256)
    )
    if not label_ok:
        raise MLOfflineProductionCandidateScoringError(
            "audit scorer label_dataset_sha256/provenance does not match supplied label dataset"
        )
    if not embeddings_ok:
        raise MLOfflineProductionCandidateScoringError(
            "audit scorer embedding_artifact_sha256/provenance does not match supplied embeddings"
        )
    return metadata


def _validate_holdout_assignment(
    payload: Mapping[str, Any],
    *,
    ranking_run_id: str,
    family: str,
) -> tuple[Mapping[str, Any], dict[str, dict[str, Any]]]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("artifact_type") != HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected holdout assignment metadata.artifact_type={HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("assignment_version") != HOLDOUT_ASSIGNMENT_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected holdout assignment metadata.assignment_version={HOLDOUT_ASSIGNMENT_VERSION!r}, "
            f"got {metadata.get('assignment_version')!r}"
        )
    if metadata.get("strategy_id") != HOLDOUT_STRATEGY_ID:
        raise MLOfflineProductionCandidateScoringError(
            f"holdout assignment metadata.strategy_id must be {HOLDOUT_STRATEGY_ID!r}"
        )
    if metadata.get("ranking_run_id") != ranking_run_id:
        raise MLOfflineProductionCandidateScoringError("holdout assignment metadata.ranking_run_id must match ranking_run_id")
    if metadata.get("family") != family:
        raise MLOfflineProductionCandidateScoringError("holdout assignment metadata.family must match family")
    leakage = payload.get("leakage_report")
    if not isinstance(leakage, Mapping):
        raise MLOfflineProductionCandidateScoringError("holdout assignment missing leakage_report")
    if leakage.get("global_zero_assertion") is not True:
        raise MLOfflineProductionCandidateScoringError("holdout assignment leakage_report.global_zero_assertion must be true")
    if int(leakage.get("train_eval_work_overlap_count") or 0) != 0:
        raise MLOfflineProductionCandidateScoringError("holdout assignment train_eval_work_overlap_count must be 0")
    assignments = payload.get("assignments")
    if not isinstance(assignments, list) or not assignments:
        raise MLOfflineProductionCandidateScoringError("holdout assignment missing assignments array")
    by_row_id: dict[str, dict[str, Any]] = {}
    for idx, raw in enumerate(assignments, start=1):
        if not isinstance(raw, Mapping):
            raise MLOfflineProductionCandidateScoringError(f"holdout assignment row {idx} is not an object")
        row_id = str(raw.get("row_id") or "").strip()
        if not row_id:
            raise MLOfflineProductionCandidateScoringError(f"holdout assignment row {idx} missing row_id")
        if row_id in by_row_id:
            raise MLOfflineProductionCandidateScoringError(f"holdout assignment duplicate row_id: {row_id}")
        assignment = str(raw.get("assignment") or "").strip()
        if assignment not in {"train", "eval"}:
            raise MLOfflineProductionCandidateScoringError(f"holdout assignment row {idx} has invalid assignment")
        by_row_id[row_id] = dict(raw)
    return metadata, by_row_id


def _validate_holdout_policy(payload: Mapping[str, Any], *, expected_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-policy")
    if metadata.get("artifact_type") != HOLDOUT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected holdout policy metadata.artifact_type={HOLDOUT_POLICY_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != HOLDOUT_POLICY_VERSION:
        raise MLOfflineProductionCandidateScoringError(
            f"expected holdout policy metadata.policy_version={HOLDOUT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    inventory = payload.get("dataset_inventory")
    strategy = payload.get("primary_holdout_strategy")
    definition = strategy.get("eval_work_set_definition") if isinstance(strategy, Mapping) else None
    inventory_sha = inventory.get("product_candidate_eval_work_set_sha256") if isinstance(inventory, Mapping) else None
    strategy_sha = definition.get("eval_work_set_sha256") if isinstance(definition, Mapping) else None
    if inventory_sha != expected_eval_sha or strategy_sha != expected_eval_sha:
        raise MLOfflineProductionCandidateScoringError("holdout policy eval_work_set_sha256 must match assignment/scorer")
    return metadata


def _validate_product_candidate_metric_gates_v2(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates-v2")
    if metadata.get("artifact_type") != PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected production-candidate gates metadata.artifact_type={PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != PRODUCT_CANDIDATE_GATES_VERSION_V2:
        raise MLOfflineProductionCandidateScoringError(
            f"expected production-candidate gates metadata.gates_version={PRODUCT_CANDIDATE_GATES_VERSION_V2!r}, "
            f"got {metadata.get('gates_version')!r}"
        )
    return metadata


def _validate_holdout_embedding_scorer(
    payload: Mapping[str, Any],
    *,
    assignment_metadata: Mapping[str, Any],
    assignment_sha256: str,
    embedding_dimensions: Any,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export")
    if metadata.get("artifact_type") != AUDIT_SCORER_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit scorer metadata.artifact_type={AUDIT_SCORER_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("scorer_version") != AUDIT_SCORER_VERSION_V2:
        raise MLOfflineProductionCandidateScoringError(
            f"expected audit scorer metadata.scorer_version={AUDIT_SCORER_VERSION_V2!r}, got {metadata.get('scorer_version')!r}"
        )
    if metadata.get("fit_mode") != "holdout_bound_train_only":
        raise MLOfflineProductionCandidateScoringError("audit scorer metadata.fit_mode must be holdout_bound_train_only")
    if metadata.get("target") != TARGET_GOOD:
        raise MLOfflineProductionCandidateScoringError("audit scorer metadata.target must be good_or_acceptable")
    policy = payload.get("policy_compliance")
    if not isinstance(policy, Mapping):
        raise MLOfflineProductionCandidateScoringError("audit scorer missing policy_compliance object")
    if policy.get("eval_works_excluded_from_fit") is not True:
        raise MLOfflineProductionCandidateScoringError("audit scorer policy_compliance.eval_works_excluded_from_fit must be true")
    if metadata.get("eval_work_set_sha256") != assignment_metadata.get("eval_work_set_sha256"):
        raise MLOfflineProductionCandidateScoringError("audit scorer eval_work_set_sha256 must match holdout assignment")
    if metadata.get("holdout_assignment_sha256") != assignment_sha256:
        raise MLOfflineProductionCandidateScoringError("audit scorer holdout_assignment_sha256 must match supplied assignment")
    scorer_dimensions = metadata.get("embedding_dimensions")
    if scorer_dimensions is None:
        scorer_block = payload.get("scorer")
        scaler_block = scorer_block.get("scaler") if isinstance(scorer_block, Mapping) else None
        scorer_dimensions = scaler_block.get("feature_count") if isinstance(scaler_block, Mapping) else None
    if scorer_dimensions != embedding_dimensions:
        raise MLOfflineProductionCandidateScoringError("audit scorer embedding dimensions do not match supplied embeddings")
    return metadata


def _validate_scoring_mode(
    scoring_mode: str,
    audit_embedding_scorer_export_path: Path | None,
    holdout_assignment_path: Path | None = None,
) -> str:
    normalized = str(scoring_mode or "").strip()
    if normalized not in SCORING_MODES:
        raise MLOfflineProductionCandidateScoringError(
            f"unsupported scoring_mode {normalized!r}; expected one of {sorted(SCORING_MODES)}"
        )
    if normalized == SCORING_MODE_AUDIT_EMBEDDING and audit_embedding_scorer_export_path is None:
        raise MLOfflineProductionCandidateScoringError(
            "--audit-embedding-scorer-export is required when --scoring-mode heuristic_and_audit_embedding_scorer"
        )
    if normalized == SCORING_MODE_HOLDOUT_EMBEDDING:
        if audit_embedding_scorer_export_path is None:
            raise MLOfflineProductionCandidateScoringError(
                "--audit-embedding-scorer-export is required when --scoring-mode heuristic_and_holdout_embedding_scorer"
            )
        if holdout_assignment_path is None:
            raise MLOfflineProductionCandidateScoringError(
                "--holdout-assignment is required when --scoring-mode heuristic_and_holdout_embedding_scorer"
            )
    return normalized


def _default_experiment_version(scoring_mode: str, experiment_version: str | None) -> str:
    if experiment_version:
        return str(experiment_version)
    if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
        return EXPERIMENT_VERSION_V3
    if scoring_mode == SCORING_MODE_AUDIT_EMBEDDING:
        return EXPERIMENT_VERSION_V2
    return EXPERIMENT_VERSION


def _validate_target_and_family(*, target: str, family: str) -> tuple[str, str]:
    normalized_target = str(target or "").strip()
    if normalized_target != TARGET_GOOD:
        raise MLOfflineProductionCandidateScoringError("ml-offline-production-candidate-scoring-v1 supports only good_or_acceptable")
    normalized_family = str(family or "").strip().lower()
    if normalized_family not in ALLOWED_FAMILIES:
        raise MLOfflineProductionCandidateScoringError("ml-offline-production-candidate-scoring-v1 supports only family=emerging")
    return normalized_target, normalized_family


def assert_local_database_url(database_url: str) -> dict[str, Any]:
    text = str(database_url or "").strip()
    if not text:
        raise MLOfflineProductionCandidateScoringError("database URL is required")
    lower = text.lower()
    forbidden = ("railway", "rlwy", "render.com", "amazonaws", "neon.tech", "supabase", "herokuapp", "azure.com")
    matched_forbidden = [token for token in forbidden if token in lower]
    if matched_forbidden:
        raise MLOfflineProductionCandidateScoringError(
            "database URL must target local Docker Postgres, not hosted production infrastructure"
        )
    parsed = urlparse(text)
    host = parsed.hostname
    local_hosts = {None, "", "localhost", "127.0.0.1", "::1", "host.docker.internal"}
    if host not in local_hosts and not str(host).endswith(".local"):
        raise MLOfflineProductionCandidateScoringError(
            f"database URL must target local Docker Postgres; host {host!r} is not allowed"
        )
    return {
        "local_database_url_confirmed": True,
        "database_url_host": host or "(local socket)",
        "database_url_port": parsed.port,
        "database_name": (parsed.path or "").lstrip("/") or None,
        "read_only_contract": "SELECT-only queries; no Postgres writes",
    }


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(payload) if isinstance(payload, Mapping) else {}
    return {}


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _candidate_canonical(row: Mapping[str, Any]) -> str | None:
    return canonical_openalex_work_id({"paper_id": row.get("openalex_id")})


def fetch_product_candidate_pool(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    family: str,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ps.work_id AS internal_work_id,
                ps.recommendation_family,
                ps.semantic_score,
                ps.citation_velocity_score,
                ps.topic_growth_score,
                ps.bridge_score,
                ps.diversity_penalty,
                ps.final_score,
                ps.bridge_eligible,
                ps.reason_short,
                w.openalex_id,
                w.title,
                w.year,
                w.citation_count
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            WHERE ps.ranking_run_id = %s
              AND ps.recommendation_family = %s
            ORDER BY ps.final_score DESC, ps.work_id ASC
            """,
            (ranking_run_id, family),
        )
        raw_rows = [dict(row) for row in cur.fetchall()]

    out: list[dict[str, Any]] = []
    for index, row in enumerate(raw_rows, start=1):
        canonical = _candidate_canonical(row)
        out.append(
            {
                "ranking_run_id": ranking_run_id,
                "family": row.get("recommendation_family"),
                "heuristic_rank": index,
                "internal_work_id": row.get("internal_work_id"),
                "openalex_id": row.get("openalex_id"),
                "canonical_openalex_work_id": canonical,
                "title": row.get("title"),
                "year": row.get("year"),
                "citation_count": row.get("citation_count"),
                "final_score": _float_or_none(row.get("final_score")),
                "semantic_score": _float_or_none(row.get("semantic_score")),
                "citation_velocity_score": _float_or_none(row.get("citation_velocity_score")),
                "topic_growth_score": _float_or_none(row.get("topic_growth_score")),
                "diversity_penalty": _float_or_none(row.get("diversity_penalty")),
                "bridge_score": _float_or_none(row.get("bridge_score")),
                "bridge_eligible": row.get("bridge_eligible"),
                "reason_short": row.get("reason_short"),
            }
        )
    return out


def _best_candidate_by_work(candidate_rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    by_work: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        canonical = row.get("canonical_openalex_work_id")
        score = _float_or_none(row.get("final_score"))
        if not canonical or score is None:
            continue
        current = by_work.get(str(canonical))
        if current is None:
            by_work[str(canonical)] = dict(row)
            continue
        current_score = _float_or_none(current.get("final_score"))
        current_rank = int(current.get("heuristic_rank") or 10**12)
        row_rank = int(row.get("heuristic_rank") or 10**12)
        if current_score is None or score > current_score or (score == current_score and row_rank < current_rank):
            by_work[str(canonical)] = dict(row)
    return by_work


def _explicit_target_label_rows(label_rows: Sequence[Mapping[str, Any]], target: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    missing_canonical = 0
    missing_target = 0
    for row in label_rows:
        canonical = canonical_openalex_work_id(row)
        if canonical is None:
            missing_canonical += 1
            continue
        if not isinstance(row.get(target), bool):
            missing_target += 1
            continue
        labeled = dict(row)
        labeled["canonical_openalex_work_id"] = canonical
        rows.append(labeled)
    return rows, {
        "label_rows_without_canonical_work_id": missing_canonical,
        "label_rows_without_boolean_target": missing_target,
    }


def _join_labels_to_candidates(
    explicit_label_rows: Sequence[Mapping[str, Any]],
    *,
    candidates_by_work: Mapping[str, Mapping[str, Any]],
    embeddings_by_row_id: Mapping[str, Mapping[str, Any]],
    target: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    joined: list[dict[str, Any]] = []
    missing_embeddings: list[str] = []
    for label in explicit_label_rows:
        canonical = str(label["canonical_openalex_work_id"])
        candidate = candidates_by_work.get(canonical)
        if candidate is None:
            continue
        row_id = str(label.get("row_id") or "").strip()
        embedding = embeddings_by_row_id.get(row_id)
        embedding_present = embedding is not None and str(embedding.get("embedding_status") or "ok") in {"ok", "mock"}
        if not embedding_present:
            missing_embeddings.append(row_id or f"(missing row_id for {canonical})")
        joined.append(
            {
                "row_id": row_id or None,
                "canonical_openalex_work_id": canonical,
                "paper_id": label.get("paper_id"),
                "label_work_id": label.get("work_id"),
                "label_ranking_run_id": label.get("ranking_run_id"),
                "label_family": label.get("family"),
                "review_pool_variant": label.get("review_pool_variant"),
                "source_worksheet_path": label.get("source_worksheet_path"),
                "target": target,
                "target_value": bool(label[target]),
                "candidate_internal_work_id": candidate.get("internal_work_id"),
                "candidate_openalex_id": candidate.get("openalex_id"),
                "candidate_heuristic_rank": candidate.get("heuristic_rank"),
                "candidate_final_score": candidate.get("final_score"),
                "embedding_present": embedding_present,
            }
        )
    summary = {
        "labeled_candidate_observation_count": len(joined),
        "labeled_candidate_unique_work_count": len({row["canonical_openalex_work_id"] for row in joined}),
        "missing_embedding_count": len(missing_embeddings),
        "missing_embedding_row_ids": missing_embeddings[:50],
    }
    return joined, summary


def _attach_holdout_assignments(
    joined_observations: Sequence[Mapping[str, Any]],
    *,
    assignment_by_row_id: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out: list[dict[str, Any]] = []
    missing_assignment_row_ids: list[str] = []
    assignment_counts: Counter[str] = Counter()
    train_work_ids: set[str] = set()
    eval_work_ids: set[str] = set()
    for row in joined_observations:
        item = dict(row)
        row_id = str(item.get("row_id") or "").strip()
        assignment_record = assignment_by_row_id.get(row_id)
        if assignment_record is None:
            missing_assignment_row_ids.append(row_id or "(missing row_id)")
            item["holdout_assignment"] = None
        else:
            assignment = str(assignment_record.get("assignment") or "").strip()
            item["holdout_assignment"] = assignment
            item["holdout_assignment_canonical_openalex_work_id"] = assignment_record.get("canonical_openalex_work_id")
            assignment_counts[assignment] += 1
            canonical = str(item.get("canonical_openalex_work_id") or "").strip()
            if assignment == "train":
                train_work_ids.add(canonical)
            elif assignment == "eval":
                eval_work_ids.add(canonical)
        out.append(item)
    return out, {
        "joined_observation_assignment_counts": dict(sorted(assignment_counts.items())),
        "missing_assignment_count": len(missing_assignment_row_ids),
        "missing_assignment_row_ids_preview": missing_assignment_row_ids[:50],
        "train_assignment_rows_in_join_count": assignment_counts["train"],
        "train_assignment_works_in_join_count": len(train_work_ids),
        "eval_assignment_row_count": assignment_counts["eval"],
        "eval_assignment_work_count": len(eval_work_ids),
    }


def _majority_label(pos: int, neg: int) -> bool | None:
    if pos > neg:
        return True
    if neg > pos:
        return False
    return None


def _build_labeled_eval_subset(
    joined_observations: Sequence[Mapping[str, Any]],
    *,
    candidates_by_work: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    observations_by_work: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in joined_observations:
        observations_by_work[str(row["canonical_openalex_work_id"])].append(row)

    eval_rows: list[dict[str, Any]] = []
    conflict_work_ids: list[str] = []
    duplicate_work_ids: list[str] = []
    majority_counts = Counter()
    for canonical, observations in sorted(observations_by_work.items()):
        candidate = candidates_by_work[canonical]
        pos = sum(1 for row in observations if row.get("target_value") is True)
        neg = sum(1 for row in observations if row.get("target_value") is False)
        if len(observations) > 1:
            duplicate_work_ids.append(canonical)
        if pos > 0 and neg > 0:
            conflict_work_ids.append(canonical)
        majority = _majority_label(pos, neg)
        majority_counts["positive" if majority is True else "negative" if majority is False else "tie"] += 1
        eval_row = {
            "canonical_openalex_work_id": canonical,
            "candidate_internal_work_id": candidate.get("internal_work_id"),
            "candidate_openalex_id": candidate.get("openalex_id"),
            "heuristic_rank": candidate.get("heuristic_rank"),
            "final_score": candidate.get("final_score"),
            "label_any_positive": pos > 0,
            "positive_observation_count": pos,
            "negative_observation_count": neg,
            "observation_count": len(observations),
            "majority_vote_label": majority,
            "conflicting_target_observations": pos > 0 and neg > 0,
            "row_ids": [row.get("row_id") for row in observations],
        }
        learned_scores = [
            float(row["audit_embedding_probability_observation"])
            for row in observations
            if _float_or_none(row.get("audit_embedding_probability_observation")) is not None
        ]
        if learned_scores:
            eval_row.update(
                {
                    "audit_embedding_probability_work": max(learned_scores),
                    "audit_embedding_probability_observation_values": learned_scores,
                    "observation_level_score_count": len(learned_scores),
                    "learned_score_aggregation_policy": LEARNED_SCORE_AGGREGATION_POLICY,
                }
            )
        eval_rows.append(eval_row)

    eval_rows.sort(key=lambda row: (-(float(row.get("final_score") or 0.0)), int(row.get("heuristic_rank") or 10**12)))
    diagnostics = {
        "observation_level_labels_preserved": True,
        "duplicate_labeled_work_count": len(duplicate_work_ids),
        "duplicate_labeled_work_ids_preview": duplicate_work_ids[:25],
        "conflicting_target_work_count": len(conflict_work_ids),
        "conflicting_target_work_ids_preview": conflict_work_ids[:25],
        "majority_vote_label_counts": {
            "positive": majority_counts["positive"],
            "negative": majority_counts["negative"],
            "tie": majority_counts["tie"],
        },
    }
    return eval_rows, diagnostics


def _attach_audit_embedding_observation_scores(
    joined_observations: Sequence[Mapping[str, Any]],
    *,
    embeddings_by_row_id: Mapping[str, Mapping[str, Any]],
    audit_scorer_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in joined_observations:
        item = dict(row)
        row_id = str(row.get("row_id") or "").strip()
        embedding = embeddings_by_row_id.get(row_id)
        if row.get("embedding_present") is True and isinstance(embedding, Mapping):
            vector = embedding.get("embedding")
            if isinstance(vector, Sequence) and not isinstance(vector, (str, bytes)):
                item["audit_embedding_probability_observation"] = score_audit_embedding_probability(vector, audit_scorer_payload)
        out.append(item)
    return out


def average_precision(scores_labels_desc: Sequence[tuple[float, bool]]) -> float | None:
    positives = sum(1 for _score, label in scores_labels_desc if label)
    if positives == 0:
        return None
    running_pos = 0
    precision_sum = 0.0
    for idx, (_score, label) in enumerate(scores_labels_desc, start=1):
        if label:
            running_pos += 1
            precision_sum += running_pos / idx
    return precision_sum / positives


def _precision_recall_at_k(scores_labels_desc: Sequence[tuple[float, bool]], k: int) -> dict[str, Any]:
    total = len(scores_labels_desc)
    positives = sum(1 for _score, label in scores_labels_desc if label)
    if total < k:
        reason = f"requires at least {k} labeled candidate works"
        return {
            "precision": None,
            "recall": None,
            "reason": reason,
            "labeled_work_count": total,
            "positive_count": positives,
            "negative_count": total - positives,
        }
    top = list(scores_labels_desc[:k])
    top_pos = sum(1 for _score, label in top if label)
    recall = (top_pos / positives) if positives else None
    return {
        "precision": precision_at_k(list(scores_labels_desc), k),
        "recall": recall,
        "reason": None if positives else "recall requires at least one positive labeled candidate work",
        "labeled_work_count": total,
        "positive_count": positives,
        "negative_count": total - positives,
        "top_k_labeled_positive_count": top_pos,
        "top_k_labeled_negative_count": k - top_pos,
    }


def _score_metrics(
    eval_rows: Sequence[Mapping[str, Any]],
    *,
    score_field: str,
    score_name: str,
) -> dict[str, Any]:
    scores_labels = [
        (float(row[score_field]), bool(row["label_any_positive"]))
        for row in eval_rows
        if _float_or_none(row.get(score_field)) is not None
    ]
    scores_labels_desc = sorted(scores_labels, key=lambda item: (-item[0], item[1]))
    positives = sum(1 for _score, label in scores_labels if label)
    negatives = len(scores_labels) - positives
    auc = roc_auc_mann_whitney(scores_labels)
    if not scores_labels:
        auc_reason = f"no labeled candidate works with {score_name}"
    elif positives == 0 or negatives == 0:
        auc_reason = "ROC-AUC requires at least one positive and one negative labeled candidate work"
    else:
        auc_reason = None
    ap = average_precision(scores_labels_desc)
    ap_reason = None if ap is not None else "average precision requires at least one positive labeled candidate work"
    return {
        "metric_level": "canonical_work_labeled_eval_subset",
        "score_name": score_name,
        "labeled_eval_subset_work_count": len(eval_rows),
        "scored_labeled_work_count": len(scores_labels),
        "positive_work_count": positives,
        "negative_work_count": negatives,
        "roc_auc_mann_whitney": auc,
        "roc_auc_reason": auc_reason,
        "average_precision": ap,
        "average_precision_reason": ap_reason,
        "precision_recall_at_k": {str(k): _precision_recall_at_k(scores_labels_desc, k) for k in K_VALUES},
    }


def _heuristic_metrics(eval_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = _score_metrics(eval_rows, score_field="final_score", score_name="final_score")
    metrics.update(
        {
        "comparison_note": (
            "This extends ml-offline-baseline-eval-rank-ee2ba6c816-v8 with pool-first framing; "
            "heuristic numbers may match v8 eval on joined rows."
        ),
        }
    )
    return metrics


def _metric_delta(learned_value: Any, heuristic_value: Any) -> float | None:
    learned = _float_or_none(learned_value)
    heuristic = _float_or_none(heuristic_value)
    if learned is None or heuristic is None:
        return None
    return float(learned - heuristic)


def _pr_value(metrics: Mapping[str, Any], k: int, field: str) -> Any:
    block = metrics.get("precision_recall_at_k")
    if not isinstance(block, Mapping):
        return None
    entry = block.get(str(k))
    if not isinstance(entry, Mapping):
        return None
    return entry.get(field)


def _learned_thresholds_satisfied(metrics: Mapping[str, Any]) -> bool:
    return (
        _float_or_none(metrics.get("roc_auc_mann_whitney")) is not None
        and float(metrics["roc_auc_mann_whitney"]) >= LEARNED_METRIC_THRESHOLDS["minimum_learned_roc_auc"]
        and _float_or_none(metrics.get("average_precision")) is not None
        and float(metrics["average_precision"]) >= LEARNED_METRIC_THRESHOLDS["minimum_learned_average_precision"]
        and _float_or_none(_pr_value(metrics, 10, "precision")) is not None
        and float(_pr_value(metrics, 10, "precision")) >= LEARNED_METRIC_THRESHOLDS["minimum_learned_precision_at_10"]
    )


def _comparison_to_heuristic(
    *,
    heuristic_metrics: Mapping[str, Any],
    learned_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    side_by_side: dict[str, Any] = {
        "roc_auc_mann_whitney": {
            "heuristic_final_score": heuristic_metrics.get("roc_auc_mann_whitney"),
            "audit_embedding_probability_work": learned_metrics.get("roc_auc_mann_whitney"),
        },
        "average_precision": {
            "heuristic_final_score": heuristic_metrics.get("average_precision"),
            "audit_embedding_probability_work": learned_metrics.get("average_precision"),
        },
    }
    for k in K_VALUES:
        side_by_side[f"precision_at_{k}"] = {
            "heuristic_final_score": _pr_value(heuristic_metrics, k, "precision"),
            "audit_embedding_probability_work": _pr_value(learned_metrics, k, "precision"),
        }
        side_by_side[f"recall_at_{k}"] = {
            "heuristic_final_score": _pr_value(heuristic_metrics, k, "recall"),
            "audit_embedding_probability_work": _pr_value(learned_metrics, k, "recall"),
        }
    return {
        "delta_roc_auc": _metric_delta(learned_metrics.get("roc_auc_mann_whitney"), heuristic_metrics.get("roc_auc_mann_whitney")),
        "delta_average_precision": _metric_delta(learned_metrics.get("average_precision"), heuristic_metrics.get("average_precision")),
        "delta_precision_at_5": _metric_delta(
            _pr_value(learned_metrics, 5, "precision"),
            _pr_value(heuristic_metrics, 5, "precision"),
        ),
        "delta_precision_at_10": _metric_delta(
            _pr_value(learned_metrics, 10, "precision"),
            _pr_value(heuristic_metrics, 10, "precision"),
        ),
        "delta_precision_at_20": _metric_delta(
            _pr_value(learned_metrics, 20, "precision"),
            _pr_value(heuristic_metrics, 20, "precision"),
        ),
        "side_by_side": side_by_side,
    }


def _learned_metrics(
    *,
    eval_rows: Sequence[Mapping[str, Any]],
    heuristic_metrics: Mapping[str, Any],
    scorer_version: Any | None = None,
    scorer_fit_mode: Any | None = None,
    scorer_sha256: str | None = None,
    eval_only: bool = False,
) -> dict[str, Any]:
    metrics = _score_metrics(
        eval_rows,
        score_field="audit_embedding_probability_work",
        score_name="audit_embedding_probability_work",
    )
    return {
        "learned_product_scores_produced": True,
        "eval_only": eval_only,
        "scorer_version": scorer_version,
        "scorer_fit_mode": scorer_fit_mode,
        "scorer_sha256": scorer_sha256,
        "aggregation_policy": LEARNED_SCORE_AGGREGATION_POLICY,
        "product_candidate_rows_used_for_training": 0,
        "metrics": metrics,
        "comparison_to_heuristic": _comparison_to_heuristic(
            heuristic_metrics=heuristic_metrics,
            learned_metrics=metrics,
        ),
        "audit_embedding_scorer_export_present": True,
        "learned_metric_thresholds": dict(LEARNED_METRIC_THRESHOLDS),
        "learned_metric_thresholds_satisfied": _learned_thresholds_satisfied(metrics),
    }


def _top_k_tables(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    eval_by_work: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    top_k: dict[str, Any] = {}
    for k in K_VALUES:
        rows = [row for row in candidate_rows if row.get("canonical_openalex_work_id")][:k]
        labeled = [row for row in rows if str(row.get("canonical_openalex_work_id")) in eval_by_work]
        positives = [
            row
            for row in labeled
            if eval_by_work[str(row.get("canonical_openalex_work_id"))].get("label_any_positive") is True
        ]
        negatives = len(labeled) - len(positives)
        top_k[str(k)] = {
            "candidate_work_count": len(rows),
            "labeled_work_count": len(labeled),
            "unlabeled_work_count": len(rows) - len(labeled),
            "label_coverage_rate": (len(labeled) / len(rows)) if rows else None,
            "labeled_positive_work_count": len(positives),
            "labeled_negative_work_count": negatives,
            "positive_rate_among_labeled": (len(positives) / len(labeled)) if labeled else None,
        }
    return top_k


def _coverage_by_rank_bucket(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    eval_by_work: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    canonical_rows = [row for row in candidate_rows if row.get("canonical_openalex_work_id")]
    for start, end in RANK_BUCKETS:
        if end is None:
            bucket_rows = [row for row in canonical_rows if int(row.get("heuristic_rank") or 0) >= start]
            label = f"{start}+"
        else:
            bucket_rows = [
                row
                for row in canonical_rows
                if start <= int(row.get("heuristic_rank") or 0) <= end
            ]
            label = f"{start}-{end}"
        labeled = [row for row in bucket_rows if str(row.get("canonical_openalex_work_id")) in eval_by_work]
        positives = [
            row
            for row in labeled
            if eval_by_work[str(row.get("canonical_openalex_work_id"))].get("label_any_positive") is True
        ]
        conflicts = [
            row
            for row in labeled
            if eval_by_work[str(row.get("canonical_openalex_work_id"))].get("conflicting_target_observations") is True
        ]
        out.append(
            {
                "rank_bucket": label,
                "candidate_work_count": len(bucket_rows),
                "labeled_work_count": len(labeled),
                "unlabeled_work_count": len(bucket_rows) - len(labeled),
                "label_coverage_rate": (len(labeled) / len(bucket_rows)) if bucket_rows else None,
                "positive_work_count": len(positives),
                "negative_work_count": len(labeled) - len(positives),
                "conflicting_target_work_count": len(conflicts),
            }
        )
    return out


def _candidate_pool_summary(candidate_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    canonical = [str(row.get("canonical_openalex_work_id")) for row in candidate_rows if row.get("canonical_openalex_work_id")]
    internal_ids = [str(row.get("internal_work_id")) for row in candidate_rows if row.get("internal_work_id") is not None]
    return {
        "paper_scores_row_count": len(candidate_rows),
        "candidate_unique_internal_work_count": len(set(internal_ids)),
        "candidate_unique_canonical_work_count": len(set(canonical)),
        "candidate_pool_work_set_sha256": _work_set_sha256(canonical),
        "candidate_rows_without_canonical_work_id": len(candidate_rows) - len(canonical),
    }


def _label_join_summary(
    *,
    label_rows: Sequence[Mapping[str, Any]],
    explicit_label_rows: Sequence[Mapping[str, Any]],
    joined_observations: Sequence[Mapping[str, Any]],
    excluded_counts: Mapping[str, int],
    eval_rows: Sequence[Mapping[str, Any]],
    candidate_unique_canonical_work_count: int,
) -> dict[str, Any]:
    labeled_work_count = len({row["canonical_openalex_work_id"] for row in joined_observations})
    return {
        "label_dataset_row_count": len(label_rows),
        "explicit_target_observation_count": len(explicit_label_rows),
        "label_rows_without_canonical_work_id": int(excluded_counts.get("label_rows_without_canonical_work_id") or 0),
        "label_rows_without_boolean_target": int(excluded_counts.get("label_rows_without_boolean_target") or 0),
        "joined_labeled_observation_count": len(joined_observations),
        "joined_labeled_unique_work_count": labeled_work_count,
        "labeled_eval_subset_work_count": len(eval_rows),
        "labeled_eval_subset_positive_work_count": sum(1 for row in eval_rows if row.get("label_any_positive") is True),
        "labeled_eval_subset_negative_work_count": sum(1 for row in eval_rows if row.get("label_any_positive") is False),
        "candidate_overlap_rate_by_explicit_observation": (
            len(joined_observations) / len(explicit_label_rows) if explicit_label_rows else None
        ),
        "candidate_work_labeled_coverage_rate": (
            labeled_work_count / candidate_unique_canonical_work_count if candidate_unique_canonical_work_count else None
        ),
        "candidate_work_unlabeled_count": max(candidate_unique_canonical_work_count - labeled_work_count, 0),
    }


def _scoring_mode_details_heuristic() -> dict[str, Any]:
    return {
        "scoring_mode": SCORING_MODE_HEURISTIC,
        "learned_product_scores_produced": False,
        "reason": LEARNED_UNAVAILABLE_REASON,
        "no_fold_coefficient_averaging": True,
        "no_refit_in_this_command": True,
        "product_candidate_rows_used_for_training": 0,
    }


def _learned_metrics_null() -> dict[str, Any]:
    return {
        "metrics": None,
        "reason": LEARNED_UNAVAILABLE_REASON,
        "learned_product_scores_produced": False,
        "audit_embedding_scorer_export_present": False,
    }


def _scoring_mode_details_learned(
    *,
    audit_embedding_scorer_version: Any,
    audit_embedding_scorer_sha256: str,
    learned_metric_thresholds_satisfied: bool,
) -> dict[str, Any]:
    return {
        "scoring_mode": SCORING_MODE_AUDIT_EMBEDDING,
        "learned_product_scores_produced": True,
        "audit_embedding_scorer_export_present": True,
        "audit_embedding_scorer_version": audit_embedding_scorer_version,
        "audit_embedding_scorer_sha256": audit_embedding_scorer_sha256,
        "no_fold_coefficient_averaging": True,
        "no_refit_in_this_command": True,
        "product_candidate_rows_used_for_training": 0,
        "learned_score_aggregation_policy": LEARNED_SCORE_AGGREGATION_POLICY,
        "learned_metric_thresholds": dict(LEARNED_METRIC_THRESHOLDS),
        "learned_metric_thresholds_satisfied": learned_metric_thresholds_satisfied,
    }


def _scoring_mode_details_holdout_learned(
    *,
    audit_embedding_scorer_version: Any,
    audit_embedding_scorer_sha256: str,
    holdout_assignment_version: Any,
    holdout_assignment_sha256: str,
) -> dict[str, Any]:
    return {
        "scoring_mode": SCORING_MODE_HOLDOUT_EMBEDDING,
        "learned_product_scores_produced": True,
        "eval_only": True,
        "audit_embedding_scorer_version": audit_embedding_scorer_version,
        "audit_embedding_scorer_sha256": audit_embedding_scorer_sha256,
        "holdout_assignment_version": holdout_assignment_version,
        "holdout_assignment_sha256": holdout_assignment_sha256,
        "no_fold_coefficient_averaging": True,
        "no_refit_in_this_command": True,
        "product_candidate_rows_used_for_training": 0,
        "learned_score_aggregation_policy": LEARNED_SCORE_AGGREGATION_POLICY,
    }


def build_ml_offline_production_candidate_scoring_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    metric_gates_path: Path,
    audit_ranker_experiment_path: Path,
    embeddings_path: Path,
    ranking_run_id: str,
    family: str = DEFAULT_FAMILY,
    target: str = TARGET_GOOD,
    experiment_version: str | None = None,
    scoring_mode: str = SCORING_MODE_HEURISTIC,
    audit_embedding_scorer_export_path: Path | None = None,
    holdout_assignment_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    production_candidate_metric_gates_v2_path: Path | None = None,
    database_url: str | None = None,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    target, family = _validate_target_and_family(target=target, family=family)
    scoring_mode = _validate_scoring_mode(scoring_mode, audit_embedding_scorer_export_path, holdout_assignment_path)
    resolved_experiment_version = _default_experiment_version(scoring_mode, experiment_version)
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    label_path = Path(label_dataset_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    gates_path = Path(metric_gates_path).resolve()
    ranker_path = Path(audit_ranker_experiment_path).resolve()
    emb_path = Path(embeddings_path).resolve()
    scorer_path = Path(audit_embedding_scorer_export_path).resolve() if audit_embedding_scorer_export_path is not None else None
    assignment_path = Path(holdout_assignment_path).resolve() if holdout_assignment_path is not None else None
    holdout_policy_path_resolved = Path(holdout_policy_path).resolve() if holdout_policy_path is not None else None
    gates_v2_path = (
        Path(production_candidate_metric_gates_v2_path).resolve()
        if production_candidate_metric_gates_v2_path is not None
        else None
    )

    label_payload = _load_json_object(label_path)
    policy_payload = _load_json_object(policy_path)
    gates_payload = _load_json_object(gates_path)
    ranker_payload = _load_json_object(ranker_path)
    emb_payload = _load_json_object(emb_path)
    scorer_payload = _load_json_object(scorer_path) if scorer_path is not None else None
    assignment_payload = _load_json_object(assignment_path) if assignment_path is not None else None
    holdout_policy_payload = _load_json_object(holdout_policy_path_resolved) if holdout_policy_path_resolved is not None else None
    gates_v2_payload = _load_json_object(gates_v2_path) if gates_v2_path is not None else None

    label_rows = _validate_label_dataset(label_payload)
    _validate_split_policy(policy_payload)
    _validate_audit_ranker_experiment(ranker_payload)

    label_sha = sha256_file(label_path)
    embeddings_sha = sha256_file(emb_path)
    ranker_sha = sha256_file(ranker_path)
    _validate_metric_gates(
        gates_payload,
        audit_ranker_experiment_path=ranker_path,
        audit_ranker_experiment_sha256=ranker_sha,
        repo_root=root,
    )
    embedding_metadata, embeddings_by_row_id = _validate_embeddings(
        emb_payload,
        label_dataset_sha256=label_sha,
        label_dataset_version=LABEL_DATASET_VERSION,
    )
    scorer_metadata: Mapping[str, Any] | None = None
    scorer_sha: str | None = None
    assignment_metadata: Mapping[str, Any] | None = None
    assignment_by_row_id: dict[str, dict[str, Any]] = {}
    assignment_sha: str | None = None
    holdout_policy_metadata: Mapping[str, Any] | None = None
    gates_v2_metadata: Mapping[str, Any] | None = None
    if scoring_mode == SCORING_MODE_AUDIT_EMBEDDING:
        if scorer_payload is None or scorer_path is None:
            raise MLOfflineProductionCandidateScoringError(
                "--audit-embedding-scorer-export is required in heuristic_and_audit_embedding_scorer mode"
            )
        scorer_sha = sha256_file(scorer_path)
        scorer_metadata = _validate_audit_embedding_scorer(
            scorer_payload,
            label_dataset_sha256=label_sha,
            embeddings_sha256=embeddings_sha,
            embedding_dimensions=embedding_metadata.get("embedding_dimensions"),
        )
    elif scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
        if scorer_payload is None or scorer_path is None or assignment_payload is None or assignment_path is None:
            raise MLOfflineProductionCandidateScoringError(
                "--audit-embedding-scorer-export and --holdout-assignment are required in heuristic_and_holdout_embedding_scorer mode"
            )
        assignment_sha = sha256_file(assignment_path)
        assignment_metadata, assignment_by_row_id = _validate_holdout_assignment(
            assignment_payload,
            ranking_run_id=str(ranking_run_id or "").strip(),
            family=family,
        )
        scorer_sha = sha256_file(scorer_path)
        scorer_metadata = _validate_holdout_embedding_scorer(
            scorer_payload,
            assignment_metadata=assignment_metadata,
            assignment_sha256=assignment_sha,
            embedding_dimensions=embedding_metadata.get("embedding_dimensions"),
        )
        if holdout_policy_payload is not None:
            holdout_policy_metadata = _validate_holdout_policy(
                holdout_policy_payload,
                expected_eval_sha=str(assignment_metadata.get("eval_work_set_sha256")),
            )
        if gates_v2_payload is not None:
            gates_v2_metadata = _validate_product_candidate_metric_gates_v2(gates_v2_payload)

    database_summary = assert_local_database_url(database_url) if database_url is not None else {
        "local_database_url_confirmed": None,
        "read_only_contract": "SELECT-only queries; no Postgres writes",
    }

    rid = str(ranking_run_id or "").strip()
    if not rid:
        raise MLOfflineProductionCandidateScoringError("ranking_run_id must be non-empty")
    try:
        run_row = fetch_ranking_run_row(conn, ranking_run_id=rid)
    except MLOfflineBaselineEvalError as exc:
        raise MLOfflineProductionCandidateScoringError(str(exc), code=exc.code) from exc

    candidate_rows = fetch_product_candidate_pool(conn, ranking_run_id=rid, family=family)
    if not candidate_rows:
        raise MLOfflineProductionCandidateScoringError(
            f"ranking_run_id {rid!r} has no paper_scores rows for family {family!r}; local DB lacks the required run/scores"
        )

    candidate_pool_work_ids = sorted(
        {str(row.get("canonical_openalex_work_id")) for row in candidate_rows if row.get("canonical_openalex_work_id")}
    )
    candidate_pool_work_set_sha = _work_set_sha256(candidate_pool_work_ids)
    if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
        expected_eval_sha = str((assignment_metadata or {}).get("eval_work_set_sha256") or "")
        if candidate_pool_work_set_sha != expected_eval_sha:
            raise MLOfflineProductionCandidateScoringError(
                "candidate pool work-set SHA does not match holdout assignment/scorer eval_work_set_sha256"
            )
        expected_eval_count = int((assignment_metadata or {}).get("eval_work_count") or 0)
        if expected_eval_count and len(candidate_pool_work_ids) != expected_eval_count:
            raise MLOfflineProductionCandidateScoringError(
                "candidate pool unique canonical work count does not match holdout assignment eval_work_count"
            )

    candidates_by_work = _best_candidate_by_work(candidate_rows)
    explicit_label_rows, excluded_counts = _explicit_target_label_rows(label_rows, target)
    joined_observations, embedding_join_summary = _join_labels_to_candidates(
        explicit_label_rows,
        candidates_by_work=candidates_by_work,
        embeddings_by_row_id=embeddings_by_row_id,
        target=target,
    )
    assignment_join_summary: dict[str, Any] = {}
    metric_joined_observations = joined_observations
    if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
        joined_observations, assignment_join_summary = _attach_holdout_assignments(
            joined_observations,
            assignment_by_row_id=assignment_by_row_id,
        )
        if assignment_join_summary["missing_assignment_count"]:
            raise MLOfflineProductionCandidateScoringError(
                "joined candidate label rows are missing holdout assignments"
            )
        metric_joined_observations = [
            row for row in joined_observations if row.get("holdout_assignment") == "eval"
        ]
    if scoring_mode in {SCORING_MODE_AUDIT_EMBEDDING, SCORING_MODE_HOLDOUT_EMBEDDING}:
        joined_observations = _attach_audit_embedding_observation_scores(
            joined_observations,
            embeddings_by_row_id=embeddings_by_row_id,
            audit_scorer_payload=scorer_payload or {},
        )
        if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
            metric_joined_observations = [
                row for row in joined_observations if row.get("holdout_assignment") == "eval"
            ]
        else:
            metric_joined_observations = joined_observations
    eval_rows, conflict_diagnostics = _build_labeled_eval_subset(metric_joined_observations, candidates_by_work=candidates_by_work)
    eval_by_work = {str(row["canonical_openalex_work_id"]): row for row in eval_rows}
    heuristic_metrics = _heuristic_metrics(eval_rows)
    if scoring_mode == SCORING_MODE_AUDIT_EMBEDDING:
        learned_metrics = _learned_metrics(
            eval_rows=eval_rows,
            heuristic_metrics=heuristic_metrics,
            scorer_version=(scorer_metadata or {}).get("scorer_version"),
            scorer_fit_mode=(scorer_metadata or {}).get("fit_mode"),
            scorer_sha256=str(scorer_sha),
        )
        scoring_mode_details = _scoring_mode_details_learned(
            audit_embedding_scorer_version=(scorer_metadata or {}).get("scorer_version"),
            audit_embedding_scorer_sha256=str(scorer_sha),
            learned_metric_thresholds_satisfied=bool(learned_metrics["learned_metric_thresholds_satisfied"]),
        )
        blockers_to_shadow = list(LEARNED_BLOCKERS_TO_SHADOW)
        caveats = list(LEARNED_CAVEATS)
        interpretation_next_step = "Run product-candidate learned metric gates v2."
    elif scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
        learned_metrics = _learned_metrics(
            eval_rows=eval_rows,
            heuristic_metrics=heuristic_metrics,
            scorer_version=(scorer_metadata or {}).get("scorer_version"),
            scorer_fit_mode=(scorer_metadata or {}).get("fit_mode"),
            scorer_sha256=str(scorer_sha),
            eval_only=True,
        )
        scoring_mode_details = _scoring_mode_details_holdout_learned(
            audit_embedding_scorer_version=(scorer_metadata or {}).get("scorer_version"),
            audit_embedding_scorer_sha256=str(scorer_sha),
            holdout_assignment_version=(assignment_metadata or {}).get("assignment_version"),
            holdout_assignment_sha256=str(assignment_sha),
        )
        blockers_to_shadow = list(HOLDOUT_LEARNED_BLOCKERS_TO_SHADOW)
        caveats = list(HOLDOUT_LEARNED_CAVEATS)
        interpretation_next_step = "product-candidate metric gates v3"
    else:
        learned_metrics = _learned_metrics_null()
        scoring_mode_details = _scoring_mode_details_heuristic()
        blockers_to_shadow = list(BLOCKERS_TO_SHADOW)
        caveats = list(CAVEATS)
        interpretation_next_step = (
            "Run product-candidate metric gates v1 if the coverage and top-k diagnostics are credible; "
            "otherwise collect targeted product-pool labels."
        )

    cfg = _parse_config_json(run_row.get("config_json"))
    generated = generated_at or _now_iso_z()
    inputs = [
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("metric_gates", gates_path, repo_root=root),
        _input_record("audit_ranker_experiment", ranker_path, repo_root=root),
        _input_record("embeddings", emb_path, repo_root=root),
    ]
    if scorer_path is not None:
        inputs.append(_input_record("audit_embedding_scorer_export", scorer_path, repo_root=root))
    if assignment_path is not None:
        inputs.append(_input_record("holdout_assignment", assignment_path, repo_root=root))
    if holdout_policy_path_resolved is not None:
        inputs.append(_input_record("holdout_policy", holdout_policy_path_resolved, repo_root=root))
    if gates_v2_path is not None:
        inputs.append(_input_record("production_candidate_metric_gates_v2", gates_v2_path, repo_root=root))

    holdout_assignment_summary = None
    leakage_report = None
    if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING:
        train_rows_used_in_metrics = sum(1 for row in metric_joined_observations if row.get("holdout_assignment") == "train")
        train_works_used_in_metrics = len(
            {
                str(row.get("canonical_openalex_work_id"))
                for row in metric_joined_observations
                if row.get("holdout_assignment") == "train"
            }
        )
        if train_rows_used_in_metrics or train_works_used_in_metrics:
            raise MLOfflineProductionCandidateScoringError("train assignment rows cannot be used in holdout metrics")
        unlabeled_candidate_work_count = max(
            len(candidate_pool_work_ids) - len({str(row["canonical_openalex_work_id"]) for row in joined_observations}),
            0,
        )
        holdout_assignment_summary = {
            "assignment_version": (assignment_metadata or {}).get("assignment_version"),
            "assignment_sha256": assignment_sha,
            "eval_work_set_sha256": (assignment_metadata or {}).get("eval_work_set_sha256"),
            "pool_work_set_sha256": candidate_pool_work_set_sha,
            "pool_matches_eval_set": True,
            "eval_assignment_row_count": int(assignment_join_summary.get("eval_assignment_row_count") or 0),
            "train_assignment_rows_in_join_count": int(assignment_join_summary.get("train_assignment_rows_in_join_count") or 0),
            "unlabeled_candidate_work_count": unlabeled_candidate_work_count,
            "holdout_policy_version": holdout_policy_metadata.get("policy_version") if holdout_policy_metadata else None,
            "production_candidate_metric_gates_v2_version": (
                gates_v2_metadata.get("gates_version") if gates_v2_metadata else None
            ),
        }
        leakage_report = {
            "train_rows_used_in_metrics": train_rows_used_in_metrics,
            "train_works_used_in_metrics": train_works_used_in_metrics,
            "eval_work_set_matches_assignment": True,
            "candidate_pool_work_set_matches_eval_set": True,
        }

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "experiment_version": resolved_experiment_version,
            "generated_at": generated,
            "inputs": inputs,
            "ranking_run_id": rid,
            "family": family,
            "target": target,
            "scoring_mode": scoring_mode,
            "eval_work_set_sha256": (assignment_metadata or {}).get("eval_work_set_sha256") if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING else None,
            "scorer_version": (scorer_metadata or {}).get("scorer_version") if scorer_metadata else None,
            "scorer_fit_mode": (scorer_metadata or {}).get("fit_mode") if scorer_metadata else None,
            "caveats": caveats,
        },
        "preflight_db_summary": {
            **database_summary,
            "ranking_run_id": rid,
            "ranking_run_status": run_row.get("status"),
            "ranking_version": run_row.get("ranking_version"),
            "corpus_snapshot_version": run_row.get("corpus_snapshot_version"),
            "embedding_version": run_row.get("embedding_version"),
            "cluster_version": cluster_version_from_config(cfg) or "",
            "paper_scores_rows_for_family": len(candidate_rows),
            "ranking_run_succeeded": run_row.get("status") == "succeeded",
        },
        "candidate_pool_definition": {
            "source": "existing paper_scores rows joined to works",
            "ranking_run_id": rid,
            "family": family,
            "filters": {
                "paper_scores.ranking_run_id": rid,
                "paper_scores.recommendation_family": family,
            },
            "ordering": "final_score DESC, work_id ASC",
            "no_new_ranking_run": True,
            "postgres_write_allowed": False,
        },
        "candidate_pool_summary": _candidate_pool_summary(candidate_rows),
        "holdout_assignment_summary": holdout_assignment_summary,
        "label_join_summary": _label_join_summary(
            label_rows=label_rows,
            explicit_label_rows=explicit_label_rows,
            joined_observations=joined_observations,
            excluded_counts=excluded_counts,
            eval_rows=eval_rows,
            candidate_unique_canonical_work_count=len(candidates_by_work),
        ),
        "embedding_join_summary": {
            "embedding_rows_available": len(embeddings_by_row_id),
            **embedding_join_summary,
        },
        "scoring_mode_details": scoring_mode_details,
        "heuristic_metrics": heuristic_metrics,
        "learned_or_embedding_metrics": learned_metrics,
        "leakage_report": leakage_report,
        "top_k_tables": _top_k_tables(candidate_rows, eval_by_work=eval_by_work),
        "coverage_by_rank_bucket": _coverage_by_rank_bucket(candidate_rows, eval_by_work=eval_by_work),
        "duplicate_conflict_diagnostics": conflict_diagnostics,
        "blockers_to_shadow": blockers_to_shadow,
        "interpretation": {
            "summary": (
                "This artifact applies the holdout-bound audit embedding scorer to the product-candidate eval arm "
                "and compares held-out learned scores with heuristic final_score on the same labeled eval works."
                if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING
                else (
                    "This artifact measures label overlap and current heuristic final_score behavior on a product-like "
                    "candidate pool that already exists in paper_scores."
                )
            ),
            "readiness": "diagnostic_only_not_shadow_ready",
            "not_claimed": (
                [
                    "live recommender validation",
                    "shadow readiness",
                    "production readiness",
                ]
                if scoring_mode == SCORING_MODE_HOLDOUT_EMBEDDING
                else [
                    "validation",
                    "production readiness",
                    "shadow-scoring readiness",
                    "production ranking improvement",
                ]
            ),
            "next_step": interpretation_next_step,
        },
        "candidate_pool_rows": candidate_rows,
        "labeled_candidate_observations": joined_observations,
        "labeled_eval_subset": eval_rows,
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_offline_production_candidate_scoring(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    pool = payload["candidate_pool_summary"]
    labels = payload["label_join_summary"]
    embeddings = payload["embedding_join_summary"]
    metrics = payload["heuristic_metrics"]
    learned = payload["learned_or_embedding_metrics"]
    top_k = payload["top_k_tables"]
    learned_metrics = learned.get("metrics") if isinstance(learned.get("metrics"), Mapping) else None
    is_holdout = metadata.get("scoring_mode") == SCORING_MODE_HOLDOUT_EMBEDDING

    lines = [
        f"# Production-Candidate Offline Scoring ({metadata['experiment_version']})",
        "",
        "## Executive Summary",
        "",
        (
            "Offline product-candidate diagnostic over an existing ranking run. No ranking was run, no product scores "
            "were written, and no model artifact was produced."
            if not is_holdout
            else "Offline product-candidate diagnostic applying the holdout-bound audit embedding scorer to the reserved eval arm. No ranking was run, no product scores were written, and no model artifact was produced."
        ),
        "",
        f"- **ranking_run_id:** `{metadata['ranking_run_id']}`",
        f"- **family:** `{metadata['family']}`",
        f"- **target:** `{metadata['target']}`",
        f"- **scoring_mode:** `{metadata['scoring_mode']}`",
        f"- **candidate rows:** {pool['paper_scores_row_count']}",
        f"- **labeled eval works:** {labels['labeled_eval_subset_work_count']}",
        f"- **heuristic ROC-AUC/AP:** {_fmt(metrics['roc_auc_mann_whitney'])} / {_fmt(metrics['average_precision'])}",
    ]
    if learned_metrics is not None:
        comparison = learned.get("comparison_to_heuristic") if isinstance(learned.get("comparison_to_heuristic"), Mapping) else {}
        lines.extend(
            [
                f"- **learned scorer ROC-AUC/AP:** {_fmt(learned_metrics['roc_auc_mann_whitney'])} / {_fmt(learned_metrics['average_precision'])}",
                f"- **learned vs heuristic deltas (ROC-AUC/AP/P@10):** {_fmt(comparison.get('delta_roc_auc'))} / {_fmt(comparison.get('delta_average_precision'))} / {_fmt(comparison.get('delta_precision_at_10'))}",
            ]
        )
    lines.extend(
        [
        "",
        "## Candidate Pool Definition",
        "",
        "Existing `paper_scores` rows filtered by explicit `ranking_run_id` and `recommendation_family`, ordered by persisted `final_score` descending. This command is SELECT-only and reuses the materialized pool.",
        "",
        "## Label/Embedding Coverage",
        "",
        f"- **explicit target observations:** {labels['explicit_target_observation_count']}",
        f"- **joined labeled observations:** {labels['joined_labeled_observation_count']}",
        f"- **joined labeled works:** {labels['joined_labeled_unique_work_count']}",
        f"- **candidate work label coverage:** {_fmt(labels['candidate_work_labeled_coverage_rate'])}",
        f"- **unlabeled candidate works:** {labels['candidate_work_unlabeled_count']}",
        f"- **candidate overlap rate by observation:** {_fmt(labels['candidate_overlap_rate_by_explicit_observation'])}",
        f"- **embeddings present for joined observations:** {embeddings['labeled_candidate_observation_count'] - embeddings['missing_embedding_count']}",
        f"- **missing embeddings among joined observations:** {embeddings['missing_embedding_count']}",
        "",
        "## Heuristic Final_Score Metrics",
        "",
        "| Metric | Value | Note |",
        "| --- | ---: | --- |",
        f"| ROC-AUC (Mann-Whitney) | {_fmt(metrics['roc_auc_mann_whitney'])} | {metrics.get('roc_auc_reason') or ''} |",
        f"| Average precision | {_fmt(metrics['average_precision'])} | {metrics.get('average_precision_reason') or ''} |",
        ]
    )
    for k in K_VALUES:
        block = metrics["precision_recall_at_k"][str(k)]
        lines.append(f"| Precision@{k} | {_fmt(block['precision'])} | {block.get('reason') or ''} |")
        lines.append(f"| Recall@{k} | {_fmt(block['recall'])} | {block.get('reason') or ''} |")

    lines.extend(["", "## Holdout Learned Scorer Metrics" if is_holdout else "## Learned Audit Scorer Metrics", ""])
    if learned_metrics is None:
        lines.append(f"`{metadata['scoring_mode']}`: learned product scores were not produced. {learned['reason']}")
    else:
        lines.extend(
            [
                f"- **Score aggregation policy:** `{learned.get('aggregation_policy')}`",
                "",
                "| Metric | Value | Note |",
                "| --- | ---: | --- |",
                f"| ROC-AUC (Mann-Whitney) | {_fmt(learned_metrics['roc_auc_mann_whitney'])} | {learned_metrics.get('roc_auc_reason') or ''} |",
                f"| Average precision | {_fmt(learned_metrics['average_precision'])} | {learned_metrics.get('average_precision_reason') or ''} |",
            ]
        )
        for k in K_VALUES:
            block = learned_metrics["precision_recall_at_k"][str(k)]
            lines.append(f"| Precision@{k} | {_fmt(block['precision'])} | {block.get('reason') or ''} |")
            lines.append(f"| Recall@{k} | {_fmt(block['recall'])} | {block.get('reason') or ''} |")

        comparison = learned.get("comparison_to_heuristic") if isinstance(learned.get("comparison_to_heuristic"), Mapping) else {}
        side_by_side = comparison.get("side_by_side") if isinstance(comparison.get("side_by_side"), Mapping) else {}
        lines.extend(
            [
                "",
                "## Heuristic vs Learned Comparison",
                "",
                "| Metric | Heuristic final_score | Learned audit scorer | Delta learned-heuristic |",
                "| --- | ---: | ---: | ---: |",
                f"| ROC-AUC | {_fmt(side_by_side.get('roc_auc_mann_whitney', {}).get('heuristic_final_score'))} | {_fmt(side_by_side.get('roc_auc_mann_whitney', {}).get('audit_embedding_probability_work'))} | {_fmt(comparison.get('delta_roc_auc'))} |",
                f"| Average precision | {_fmt(side_by_side.get('average_precision', {}).get('heuristic_final_score'))} | {_fmt(side_by_side.get('average_precision', {}).get('audit_embedding_probability_work'))} | {_fmt(comparison.get('delta_average_precision'))} |",
                f"| Precision@5 | {_fmt(side_by_side.get('precision_at_5', {}).get('heuristic_final_score'))} | {_fmt(side_by_side.get('precision_at_5', {}).get('audit_embedding_probability_work'))} | {_fmt(comparison.get('delta_precision_at_5'))} |",
                f"| Precision@10 | {_fmt(side_by_side.get('precision_at_10', {}).get('heuristic_final_score'))} | {_fmt(side_by_side.get('precision_at_10', {}).get('audit_embedding_probability_work'))} | {_fmt(comparison.get('delta_precision_at_10'))} |",
                f"| Precision@20 | {_fmt(side_by_side.get('precision_at_20', {}).get('heuristic_final_score'))} | {_fmt(side_by_side.get('precision_at_20', {}).get('audit_embedding_probability_work'))} | {_fmt(comparison.get('delta_precision_at_20'))} |",
            ]
        )

    if is_holdout:
        holdout = payload.get("holdout_assignment_summary") if isinstance(payload.get("holdout_assignment_summary"), Mapping) else {}
        leakage = payload.get("leakage_report") if isinstance(payload.get("leakage_report"), Mapping) else {}
        lines.extend(
            [
                "",
                "## Leakage Checks",
                "",
                f"- **Eval work-set SHA:** `{holdout.get('eval_work_set_sha256')}`",
                f"- **Pool work-set SHA:** `{holdout.get('pool_work_set_sha256')}`",
                f"- **Pool matches eval set:** {holdout.get('pool_matches_eval_set')}",
                f"- **Train rows used in metrics:** {leakage.get('train_rows_used_in_metrics')}",
                f"- **Train works used in metrics:** {leakage.get('train_works_used_in_metrics')}",
                f"- **Eval work set matches assignment:** {leakage.get('eval_work_set_matches_assignment')}",
            ]
        )

    lines.extend(
        [
            "",
            "## Top-K Labeled Coverage",
            "",
            "| k | Candidate works | Labeled works | Coverage | Labeled positives | Labeled negatives |",
            "| ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for k in K_VALUES:
        block = top_k[str(k)]
        lines.append(
            f"| {k} | {block['candidate_work_count']} | {block['labeled_work_count']} | "
            f"{_fmt(block['label_coverage_rate'])} | {block['labeled_positive_work_count']} | "
            f"{block['labeled_negative_work_count']} |"
        )

    lines.extend(
        [
            "",
            "## Not Shadow / Not Production",
            "",
            "- This is not shadow scoring.",
            "- This is not production scoring.",
            "- Production defaults remain blocked.",
            "- No `ml-shadow-scorer-v1` contract exists.",
            "- No production model artifact exists.",
            "",
            "## Caveats",
            "",
            *[f"- {caveat}" for caveat in metadata.get("caveats", [])],
            "",
            "## Next Step",
            "",
            "Product-candidate metric gates v3."
            if is_holdout
            else "Product-candidate metric gates v2."
            if learned_metrics is not None
            else "Product-candidate metric gates v1 if results are credible; otherwise targeted product-pool labels.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_ml_offline_production_candidate_scoring(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    split_policy_path: Path,
    metric_gates_path: Path,
    audit_ranker_experiment_path: Path,
    embeddings_path: Path,
    ranking_run_id: str,
    family: str,
    target: str,
    output_path: Path,
    markdown_output_path: Path,
    experiment_version: str | None = None,
    scoring_mode: str = SCORING_MODE_HEURISTIC,
    audit_embedding_scorer_export_path: Path | None = None,
    holdout_assignment_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    production_candidate_metric_gates_v2_path: Path | None = None,
    database_url: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_production_candidate_scoring_payload(
        conn,
        label_dataset_path=label_dataset_path,
        split_policy_path=split_policy_path,
        metric_gates_path=metric_gates_path,
        audit_ranker_experiment_path=audit_ranker_experiment_path,
        embeddings_path=embeddings_path,
        ranking_run_id=ranking_run_id,
        family=family,
        target=target,
        experiment_version=experiment_version,
        scoring_mode=scoring_mode,
        audit_embedding_scorer_export_path=audit_embedding_scorer_export_path,
        holdout_assignment_path=holdout_assignment_path,
        holdout_policy_path=holdout_policy_path,
        production_candidate_metric_gates_v2_path=production_candidate_metric_gates_v2_path,
        database_url=database_url,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_offline_production_candidate_scoring(payload), encoding="utf-8")
    return payload


def run_ml_offline_production_candidate_scoring_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    split_policy_path: Path,
    metric_gates_path: Path,
    audit_ranker_experiment_path: Path,
    embeddings_path: Path,
    ranking_run_id: str,
    family: str,
    target: str,
    output_path: Path,
    markdown_output_path: Path,
    experiment_version: str | None = None,
    scoring_mode: str = SCORING_MODE_HEURISTIC,
    audit_embedding_scorer_export_path: Path | None = None,
    holdout_assignment_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    production_candidate_metric_gates_v2_path: Path | None = None,
    repo_root: Path | None = None,
) -> None:
    _validate_scoring_mode(scoring_mode, audit_embedding_scorer_export_path, holdout_assignment_path)
    assert_local_database_url(database_url)
    with psycopg.connect(database_url) as conn:
        write_ml_offline_production_candidate_scoring(
            conn,
            label_dataset_path=label_dataset_path,
            split_policy_path=split_policy_path,
            metric_gates_path=metric_gates_path,
            audit_ranker_experiment_path=audit_ranker_experiment_path,
            embeddings_path=embeddings_path,
            ranking_run_id=ranking_run_id,
            family=family,
            target=target,
            output_path=output_path,
            markdown_output_path=markdown_output_path,
            experiment_version=experiment_version,
            scoring_mode=scoring_mode,
            audit_embedding_scorer_export_path=audit_embedding_scorer_export_path,
            holdout_assignment_path=holdout_assignment_path,
            holdout_policy_path=holdout_policy_path,
            production_candidate_metric_gates_v2_path=production_candidate_metric_gates_v2_path,
            database_url=database_url,
            repo_root=repo_root,
        )


__all__ = [
    "MLOfflineProductionCandidateScoringError",
    "assert_local_database_url",
    "average_precision",
    "build_ml_offline_production_candidate_scoring_payload",
    "fetch_product_candidate_pool",
    "markdown_from_ml_offline_production_candidate_scoring",
    "run_ml_offline_production_candidate_scoring_cli",
    "write_ml_offline_production_candidate_scoring",
]
