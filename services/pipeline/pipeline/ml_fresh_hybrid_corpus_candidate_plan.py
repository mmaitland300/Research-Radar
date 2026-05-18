"""Fresh hybrid corpus candidate plan v1.

This command is a dry-run supply-side planning artifact. It reuses the
corpus-v2 OpenAlex candidate planner, then adds hybrid-confirmatory exclusions
and readiness accounting. It does not use Postgres, create snapshots, create
ranking runs, generate embeddings, import labels, score hybrids, or authorize
shadow/prod.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.corpus_expansion_preview import resolve_corpus_expansion_preview_mailto
from pipeline.corpus_v2_candidate_plan import run_corpus_v2_candidate_plan
from pipeline.ml_label_dataset import sha256_file
from pipeline.openalex_client import compute_contact_provenance, openalex_api_key_from_env
from pipeline.policy import CorpusPolicy
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_hybrid_corpus_candidate_plan"
PLAN_VERSION = "ml-fresh-hybrid-corpus-candidate-plan-v1"
SOURCE_BUILD_ARTIFACT_TYPE = "ml_fresh_product_candidate_source_build"
SOURCE_BUILD_VERSION = "ml-fresh-product-candidate-source-build-v1"
EXPANSION_PLAN_ARTIFACT_TYPE = "ml_fresh_candidate_source_expansion_plan"
EXPANSION_PLAN_VERSION = "ml-fresh-candidate-source-expansion-plan-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
DEFAULT_TARGET_MIN = 160
DEFAULT_TARGET_MAX = 500
OLD_EVAL_WORK_SET_SHA256 = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
UNDERPOWERED_WORK_SET_SHA256 = "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926"

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)
NEGATIVE_OR_BORDERLINE_BUCKET_IDS = frozenset(
    {
        "audio_ml_signal_processing",
        "source_separation_benchmarks",
        "symbolic_music_and_harmony",
        "cultural_computational_musicology",
        "ethics_law_fairness_user_studies",
    }
)

CAVEATS = (
    "Dry-run candidate plan only; no Postgres writes, source snapshot, ranking run, embeddings, or label import.",
    "OpenAlex metadata is read-only and may drift before a later ingest/snapshot step.",
    "Candidate selection is label-blind; v8 labels are not used to choose candidates.",
    "Old 217-work confirmatory surface is excluded from readiness estimates.",
    "Underpowered 44-work source overlap is reported separately.",
    "Not hybrid validation, not live recommender validation, and not shadow/production authorization.",
)


class MLFreshHybridCorpusCandidatePlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshHybridCorpusCandidatePlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshHybridCorpusCandidatePlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshHybridCorpusCandidatePlanError(f"{name} JSON missing metadata object")
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
        raise MLFreshHybridCorpusCandidatePlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()}))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _policy_input_record(policy_metadata: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    inputs = policy_metadata.get("inputs")
    if not isinstance(inputs, list):
        return None
    return next((item for item in inputs if isinstance(item, Mapping) and item.get("name") == name), None)


def _resolve_policy_input(record: Mapping[str, Any], *, repo_root: Path) -> Path:
    raw_path = str(record.get("path") or "").strip()
    if not raw_path:
        raise MLFreshHybridCorpusCandidatePlanError("policy input record missing path")
    path = Path(raw_path)
    resolved = path if path.is_absolute() else (repo_root / path)
    resolved = resolved.resolve()
    if not resolved.exists():
        raise MLFreshHybridCorpusCandidatePlanError(f"policy input path does not exist: {raw_path}")
    expected_sha = str(record.get("sha256") or "").strip()
    if expected_sha and sha256_file(resolved) != expected_sha:
        raise MLFreshHybridCorpusCandidatePlanError(f"policy input SHA mismatch for {raw_path}")
    return resolved


def _old_eval_ids_from_scoring(scoring_payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    rows = scoring_payload.get("candidate_pool_rows")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    return out


def _old_eval_ids_from_assignment(assignment_payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    for section in ("work_assignments", "assignments"):
        rows = assignment_payload.get(section)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, Mapping) and row.get("assignment") == "eval":
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    return out


def _old_eval_work_ids(policy_metadata: Mapping[str, Any], *, repo_root: Path) -> set[str]:
    old_ids: set[str] = set()
    scoring_record = _policy_input_record(policy_metadata, "production_candidate_scoring")
    assignment_record = _policy_input_record(policy_metadata, "holdout_assignment")
    if scoring_record is not None:
        old_ids.update(_old_eval_ids_from_scoring(_load_json_object(_resolve_policy_input(scoring_record, repo_root=repo_root))))
    if assignment_record is not None:
        old_ids.update(_old_eval_ids_from_assignment(_load_json_object(_resolve_policy_input(assignment_record, repo_root=repo_root))))
    if not old_ids:
        raise MLFreshHybridCorpusCandidatePlanError(
            "could not reconstruct old 217-work eval IDs from policy production_candidate_scoring/holdout_assignment inputs"
        )
    if _work_set_sha256(old_ids) != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshHybridCorpusCandidatePlanError("old eval work IDs from policy inputs do not match disallowed SHA")
    return old_ids


def _underpowered_work_ids(source_build_payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    rows = _get(source_build_payload, "candidate_source.candidate_rows")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping) and row.get("underpowered_source_overlap") is True:
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    return out


def _validate_source_build(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-product-candidate-source-build")
    if metadata.get("artifact_type") != SOURCE_BUILD_ARTIFACT_TYPE:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected source build metadata.artifact_type={SOURCE_BUILD_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("build_version") != SOURCE_BUILD_VERSION:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected source build metadata.build_version={SOURCE_BUILD_VERSION!r}, got {metadata.get('build_version')!r}"
        )
    if _get(payload, "build_result.status") != "blocked_needs_corpus_or_candidate_expansion":
        raise MLFreshHybridCorpusCandidatePlanError("source build status must be blocked_needs_corpus_or_candidate_expansion")
    if _get(payload, "build_result.recommended_next_stage") != "blocked_expand_corpus_or_candidate_generation":
        raise MLFreshHybridCorpusCandidatePlanError("source build recommended_next_stage must be blocked_expand_corpus_or_candidate_generation")
    return metadata


def _validate_expansion_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-candidate-source-expansion-plan")
    if metadata.get("artifact_type") != EXPANSION_PLAN_ARTIFACT_TYPE:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected expansion plan metadata.artifact_type={EXPANSION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != EXPANSION_PLAN_VERSION:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected expansion plan metadata.plan_version={EXPANSION_PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    if metadata.get("disallowed_eval_work_set_sha256") != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshHybridCorpusCandidatePlanError("policy disallowed old 217 eval_work_set_sha256 is missing or unexpected")
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshHybridCorpusCandidatePlanError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )


def _frozen_arms(policy_payload: Mapping[str, Any]) -> dict[str, Any]:
    arms = policy_payload.get("frozen_hybrid_arms")
    if not isinstance(arms, Mapping):
        arms = {}
    return {
        "primary_confirmatory_arm": arms.get("primary_confirmatory_arm") or "hybrid_rank_mean_50_50",
        "secondary_reporting_arm": arms.get("secondary_reporting_arm") or "hybrid_rank_mean_25_75_heuristic",
        "baselines_for_future_comparison": arms.get("baselines_for_future_comparison")
        or ["heuristic_final_score_baseline", "holdout_embedding_probability_baseline"],
        "no_retuning": True,
    }


def _dedupe_selected_candidates(
    rows: Sequence[Mapping[str, Any]],
    *,
    old_eval_ids: set[str],
    underpowered_ids: set[str],
) -> list[dict[str, Any]]:
    by_work: dict[str, dict[str, Any]] = {}
    for row in rows:
        canonical = _canonical_from_value(row.get("openalex_id")) or _canonical_from_value(row.get("canonical_openalex_work_id"))
        if canonical is None or canonical in by_work:
            continue
        item = dict(row)
        item["canonical_openalex_work_id"] = canonical
        item["old_217_overlap"] = canonical in old_eval_ids
        item["underpowered_source_overlap"] = canonical in underpowered_ids
        item["confirmatory_metric_candidate"] = canonical not in old_eval_ids
        item["negative_or_borderline_candidate"] = str(item.get("bucket_id") or "") in NEGATIVE_OR_BORDERLINE_BUCKET_IDS
        by_work[canonical] = item
    return [by_work[work_id] for work_id in sorted(by_work)]


def _bucket_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_bucket: dict[str, dict[str, Any]] = {}
    negative_count = 0
    for row in candidates:
        bucket = str(row.get("bucket_id") or "unknown")
        entry = by_bucket.setdefault(
            bucket,
            {
                "bucket_id": bucket,
                "selected_count": 0,
                "confirmatory_candidate_count_after_old_217_exclusion": 0,
                "old_217_overlap_count": 0,
                "underpowered_source_overlap_count": 0,
                "negative_or_borderline_intent": bucket in NEGATIVE_OR_BORDERLINE_BUCKET_IDS,
            },
        )
        entry["selected_count"] += 1
        if row.get("old_217_overlap") is True:
            entry["old_217_overlap_count"] += 1
        else:
            entry["confirmatory_candidate_count_after_old_217_exclusion"] += 1
        if row.get("underpowered_source_overlap") is True:
            entry["underpowered_source_overlap_count"] += 1
        if row.get("negative_or_borderline_candidate") is True:
            negative_count += 1
    shortfall = None
    if negative_count == 0:
        shortfall = {
            "shortfall_type": "negative_or_borderline_candidate_bucket_absent",
            "recommended_action": "revise_candidate_plan_queries_to_add_borderline_or_negative_oriented_buckets_before_ingest",
        }
    return {
        "by_bucket": [by_bucket[key] for key in sorted(by_bucket)],
        "negative_or_borderline_candidate": {
            "present": negative_count > 0,
            "selected_count": negative_count,
            "source_bucket_ids": sorted(NEGATIVE_OR_BORDERLINE_BUCKET_IDS),
        },
        "shortfall_report": shortfall,
    }


def build_ml_fresh_hybrid_corpus_candidate_plan_payload(
    *,
    fresh_product_candidate_source_build_path: Path,
    fresh_candidate_source_expansion_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    corpus_candidate_plan: Mapping[str, Any],
    target_min: int = DEFAULT_TARGET_MIN,
    target_max: int = DEFAULT_TARGET_MAX,
    mailto: str | None = None,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    source_build_path = Path(fresh_product_candidate_source_build_path).resolve()
    expansion_path = Path(fresh_candidate_source_expansion_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    source_build = _load_json_object(source_build_path)
    expansion = _load_json_object(expansion_path)
    policy = _load_json_object(policy_path)
    label_dataset = _load_json_object(label_path)
    source_build_metadata = _validate_source_build(source_build)
    expansion_metadata = _validate_expansion_plan(expansion)
    policy_metadata = _validate_policy(policy)
    _validate_label_dataset(label_dataset)
    if not conflict_path.exists():
        raise MLFreshHybridCorpusCandidatePlanError(f"conflict policy does not exist: {conflict_path}")

    inputs = [
        _input_record("fresh_product_candidate_source_build", source_build_path, repo_root=root),
        _input_record("fresh_candidate_source_expansion_plan", expansion_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    old_eval_ids = _old_eval_work_ids(policy_metadata, repo_root=root)
    underpowered_ids = _underpowered_work_ids(source_build)
    candidates = _dedupe_selected_candidates(
        list(corpus_candidate_plan.get("selected_candidates") or []),
        old_eval_ids=old_eval_ids,
        underpowered_ids=underpowered_ids,
    )
    selected_ids = [str(row["canonical_openalex_work_id"]) for row in candidates]
    confirmatory_ids = [str(row["canonical_openalex_work_id"]) for row in candidates if row.get("old_217_overlap") is not True]
    old_overlap = sorted(set(selected_ids).intersection(old_eval_ids))
    underpowered_overlap = sorted(set(confirmatory_ids).intersection(underpowered_ids))
    new_outside_underpowered = sorted(set(confirmatory_ids).difference(underpowered_ids))
    policy_minimum = int(_get(policy, "label_policy.minimum_confirmatory_label_thresholds.minimum_candidate_work_count") or 100)
    current_confirmatory = int(_get(source_build, "build_result.confirmatory_eligible_work_count") or 0)
    candidate_gap = int(_get(expansion, "current_blocker_summary.candidate_gap") or max(0, policy_minimum - current_confirmatory))
    candidate_threshold_plausibly_met = len(confirmatory_ids) >= policy_minimum
    expected_next_stage = (
        "ingest_fresh_hybrid_candidate_plan_as_snapshot_v1"
        if candidate_threshold_plausibly_met
        else "revise_candidate_plan_queries"
    )
    contact_mode = str(corpus_candidate_plan.get("contact_mode") or "none")
    contact_provided = bool(corpus_candidate_plan.get("contact_provided"))
    auth_mode = str(corpus_candidate_plan.get("auth_mode") or "unknown")
    api_key_provided = bool(corpus_candidate_plan.get("api_key_provided"))

    selected_candidate_sha = _work_set_sha256(selected_ids) if selected_ids else None
    bucket_summary = _bucket_summary(candidates)
    selected_candidates = [
        {
            "openalex_id": row.get("openalex_id"),
            "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
            "title": row.get("title"),
            "year": row.get("year"),
            "citation_count": row.get("citation_count"),
            "source_display_name": row.get("source_display_name"),
            "bucket_id": row.get("bucket_id"),
            "inclusion_reason": row.get("inclusion_reason"),
            "matched_terms": row.get("matched_terms") or [],
            "old_217_overlap": row.get("old_217_overlap"),
            "underpowered_source_overlap": row.get("underpowered_source_overlap"),
            "confirmatory_metric_candidate": row.get("confirmatory_metric_candidate"),
            "negative_or_borderline_candidate": row.get("negative_or_borderline_candidate"),
        }
        for row in candidates
    ]

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "source_build_version": source_build_metadata.get("build_version"),
            "expansion_plan_version": expansion_metadata.get("plan_version"),
            "fresh_surface_policy_version": policy_metadata.get("policy_version"),
            "label_dataset_version": label_dataset.get("dataset_version"),
            "conflict_policy_sha256": inputs[-1]["sha256"],
            "openalex_contact_provenance": {
                "contact_mode": contact_mode,
                "contact_provided": contact_provided,
                "auth_mode": auth_mode,
                "api_key_provided": api_key_provided,
                "raw_mailto_stored": False,
                "mailto_cli_provided": bool((mailto or "").strip()),
            },
            "caveats": list(CAVEATS),
        },
        "planning_context": {
            "current_best_local_source": _get(expansion, "current_blocker_summary.best_source_ranking_run_id") or "rank-3904fec89d",
            "current_confirmatory_eligible": current_confirmatory,
            "candidate_gap": candidate_gap,
            "old_eval_work_set_sha256": OLD_EVAL_WORK_SET_SHA256,
            "underpowered_source_sha256": UNDERPOWERED_WORK_SET_SHA256,
            "policy_minimum_confirmatory_candidate_works": policy_minimum,
            "target_min": target_min,
            "target_max": target_max,
            "buffer_rationale": "target_min 160 provides buffer for old-surface overlap, underpowered-source overlap, ingest attrition, and later ranking/filtering while aiming to leave at least 100 fresh eligible works.",
        },
        "candidate_selection": {
            "selected_total": len(candidates),
            "target_min": target_min,
            "target_max": target_max,
            "selected_candidates": selected_candidates,
            "selected_candidate_work_set_sha256": selected_candidate_sha,
            "estimated_confirmatory_eligible_after_old_217_exclusion": len(confirmatory_ids),
            "estimated_new_confirmatory_eligible_excluding_underpowered_source": len(new_outside_underpowered),
            "estimated_combined_confirmatory_eligible_with_current_local_source": current_confirmatory + len(new_outside_underpowered),
            "estimated_overlap_with_old_217": len(old_overlap),
            "estimated_overlap_with_underpowered_source": len(underpowered_overlap),
            "candidate_threshold_plausibly_met": candidate_threshold_plausibly_met,
            "old_217_overlap_work_ids_preview": old_overlap[:25],
            "underpowered_overlap_work_ids_preview": underpowered_overlap[:25],
            "source_corpus_v2_selected_total": corpus_candidate_plan.get("selected_total"),
            "source_corpus_v2_dedup_statistics": corpus_candidate_plan.get("dedup_statistics"),
        },
        "bucket_summary": bucket_summary,
        "readiness_estimate": {
            "enough_candidates_for_next_ingest": candidate_threshold_plausibly_met,
            "expected_next_stage": expected_next_stage,
            "candidate_threshold_rule": "candidate_threshold_plausibly_met is true only when estimated_confirmatory_eligible_after_old_217_exclusion >= 100",
        },
        "frozen_hybrid_arms_reference": _frozen_arms(policy),
        "forbidden_actions": [
            "no_db_writes",
            "no_ranking_run",
            "no_hybrid_validation",
            "no_shadow_or_production",
            "no_threshold_lowering",
            "no_label_based_candidate_selection",
        ],
        "materialization_path": [
            "ingest_fresh_hybrid_candidate_plan_as_snapshot_v1",
            "hydrate metadata/text if needed",
            "embed snapshot if required by ranking path",
            "run product-candidate ranking-run with eval-only namespacing",
            "rerun ml-fresh-product-candidate-ranking-source",
            "rerun ml-fresh-eval-surface-hybrid-materialize",
            "labeling worksheet if materialized_needs_labels",
            "hybrid validation only after materializer plus policy thresholds",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}" if abs(value) < 1 else f"{value:.2f}"
    return str(value)


def markdown_from_ml_fresh_hybrid_corpus_candidate_plan(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    context = payload["planning_context"]
    selection = payload["candidate_selection"]
    readiness = payload["readiness_estimate"]
    bucket_summary = payload["bucket_summary"]
    lines = [
        f"# Fresh Hybrid Corpus Candidate Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This is a dry-run OpenAlex candidate plan for expanding the supply side of the fresh hybrid confirmation path. It does not write Postgres, create a snapshot, rank, train, label, score hybrids, or authorize shadow/production.",
        "",
        f"- **Selected candidates:** {selection['selected_total']}",
        f"- **Estimated eligible after old-217 exclusion:** {selection['estimated_confirmatory_eligible_after_old_217_exclusion']}",
        f"- **Candidate threshold plausibly met:** {selection['candidate_threshold_plausibly_met']}",
        f"- **Expected next stage:** `{readiness['expected_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_and_production_blockers']['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['shadow_and_production_blockers']['production_default_allowed']}",
        "",
        "## Why Local DB Is Blocked",
        "",
        f"The best existing local source is `{context['current_best_local_source']}` with {context['current_confirmatory_eligible']} confirmatory-eligible works, leaving a candidate gap of {context['candidate_gap']} against the policy minimum of {context['policy_minimum_confirmatory_candidate_works']}.",
        "",
        "## Candidate Plan Size And SHA",
        "",
        f"- Selected total: {selection['selected_total']} (target range {context['target_min']}–{context['target_max']})",
        f"- Selected candidate work-set SHA: `{selection['selected_candidate_work_set_sha256']}`",
        "",
        "## Overlap Estimates",
        "",
        f"- Old 217 overlap estimate: {selection['estimated_overlap_with_old_217']}",
        f"- Underpowered 44 overlap estimate: {selection['estimated_overlap_with_underpowered_source']}",
        f"- New candidates excluding underpowered source: {selection['estimated_new_confirmatory_eligible_excluding_underpowered_source']}",
        "",
        "## Bucket Composition",
        "",
        "| Bucket | Selected | Confirmatory after old exclusion | Old overlap | Underpowered overlap | Borderline intent |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for bucket in bucket_summary["by_bucket"]:
        lines.append(
            f"| `{bucket['bucket_id']}` | {bucket['selected_count']} | "
            f"{bucket['confirmatory_candidate_count_after_old_217_exclusion']} | "
            f"{bucket['old_217_overlap_count']} | {bucket['underpowered_source_overlap_count']} | "
            f"{bucket['negative_or_borderline_intent']} |"
        )
    neg = bucket_summary["negative_or_borderline_candidate"]
    lines.extend(
        [
            "",
            "## Negative / Borderline Intent",
            "",
            f"- Present: {neg['present']}",
            f"- Selected count: {neg['selected_count']}",
        ]
    )
    if bucket_summary.get("shortfall_report"):
        lines.append(f"- Shortfall: {bucket_summary['shortfall_report']['shortfall_type']}")
    lines.extend(
        [
            "",
            "## Readiness Estimate",
            "",
            f"- Enough candidates for next ingest: {readiness['enough_candidates_for_next_ingest']}",
            f"- Expected next stage: `{readiness['expected_next_stage']}`",
            "",
            "## Materialization Path",
            "",
        ]
    )
    lines.extend(f"{idx}. {step}" for idx, step in enumerate(payload["materialization_path"], start=1))
    lines.extend(
        [
            "",
            "## Not Validation / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_hybrid_corpus_candidate_plan(
    *,
    fresh_product_candidate_source_build_path: Path,
    fresh_candidate_source_expansion_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    target_min: int = DEFAULT_TARGET_MIN,
    target_max: int = DEFAULT_TARGET_MAX,
    mailto: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    if target_min < 1 or target_max < target_min:
        raise MLFreshHybridCorpusCandidatePlanError("target-min/target-max are invalid")
    mailto_raw = (mailto or "").strip()
    has_env_mailto = bool((os.environ.get("OPENALEX_MAILTO") or "").strip())
    if not mailto_raw and not has_env_mailto and not openalex_api_key_from_env():
        raise MLFreshHybridCorpusCandidatePlanError(
            "ml-fresh-hybrid-corpus-candidate-plan live OpenAlex mode requires OPENALEX_API_KEY and/or contact: set OPENALEX_API_KEY, pass --mailto, or set OPENALEX_MAILTO"
        )
    contact_mode, contact_provided = compute_contact_provenance(mailto_cli=mailto or "", mock_openalex=False)
    resolved_mailto = resolve_corpus_expansion_preview_mailto(mailto=mailto or "", mock_openalex=False)
    corpus_plan = run_corpus_v2_candidate_plan(
        policy=CorpusPolicy(),
        mailto=resolved_mailto,
        contact_mode=contact_mode,
        contact_provided=contact_provided,
        per_bucket_limit=min(max(target_max, target_min, 100), 500),
        target_min=target_min,
        target_max=target_max,
        mock_openalex=False,
    )
    payload = build_ml_fresh_hybrid_corpus_candidate_plan_payload(
        fresh_product_candidate_source_build_path=fresh_product_candidate_source_build_path,
        fresh_candidate_source_expansion_plan_path=fresh_candidate_source_expansion_plan_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        corpus_candidate_plan=corpus_plan,
        target_min=target_min,
        target_max=target_max,
        mailto=mailto,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_fresh_hybrid_corpus_candidate_plan(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLFreshHybridCorpusCandidatePlanError",
    "build_ml_fresh_hybrid_corpus_candidate_plan_payload",
    "markdown_from_ml_fresh_hybrid_corpus_candidate_plan",
    "write_ml_fresh_hybrid_corpus_candidate_plan",
    "_work_set_sha256",
]
