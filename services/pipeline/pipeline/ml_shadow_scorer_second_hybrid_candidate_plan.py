"""Second fresh hybrid candidate plan for shadow scorer generalization.

This command is a dry-run candidate acquisition artifact. It reuses the
corpus-v2 OpenAlex candidate planner, then adds second-surface exclusion and
readiness accounting for ml-shadow-scorer-v1 generalization. It does not use
Postgres, create snapshots, create ranking runs, generate embeddings or learned
probabilities, execute scorers, ingest labels, implement online shadowing, or
authorize production behavior.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from pipeline import corpus_v2_candidate_plan as corpus_v2_candidate_plan_module
from pipeline.corpus_expansion_preview import resolve_corpus_expansion_preview_mailto
from pipeline.corpus_v2_candidate_plan import run_corpus_v2_candidate_plan
from pipeline.ml_label_dataset import sha256_file
from pipeline.policy import CorpusPolicy
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_hybrid_candidate_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1"

EXPANSION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_second_candidate_source_expansion_plan"
EXPANSION_PLAN_VERSION = "ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1"
GENERALIZATION_PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
GENERALIZATION_PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
FRESH_SURFACE_POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
FRESH_SURFACE_POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v10"
OFFLINE_SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
OFFLINE_SCORING_VERSION = "ml-offline-production-candidate-scoring-v3"
FIRST_SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
FIRST_SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
SECOND_SURFACE_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SECOND_SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"

DEFAULT_TARGET_MIN = 180
DEFAULT_TARGET_MAX = 600
OLD_217_EVAL_SHA = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
FIRST_VALIDATED_SURFACE_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
FIRST_VALIDATED_RANKING_RUN_ID = "rank-9f4b2a2084"
UNDERPOWERED_SOURCE_SHA = "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926"
UNDERPOWERED_RANKING_RUN_ID = "rank-3904fec89d"
EXPECTED_EXPANSION_NEXT_STAGE = "implement_or_run_second_fresh_candidate_source_build_for_shadow_generalization_v1"

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

ROLLUP_BUCKETS: dict[str, frozenset[str]] = {
    "core_product_candidate": frozenset({"core_mir_existing_sources", "ismir_proceedings_or_mir_conference"}),
    "borderline_or_negative_candidate": NEGATIVE_OR_BORDERLINE_BUCKET_IDS,
    "recommender_or_evaluation_candidate": frozenset(
        {"music_recommender_systems", "ethics_law_fairness_user_studies"}
    ),
    "MIR/audio_candidate": frozenset(
        {
            "core_mir_existing_sources",
            "ismir_proceedings_or_mir_conference",
            "audio_ml_signal_processing",
            "source_separation_benchmarks",
            "symbolic_music_and_harmony",
            "cultural_computational_musicology",
        }
    ),
}

CAVEATS = (
    "Dry-run plan only; no ingest, DB writes, ranking, embeddings, scorer execution, runtime, shadow/prod, or API changes.",
    "OpenAlex metadata may drift before later ingest.",
    "High overlap with the first validated surface may force query revision even with target-max 600.",
    "Underpowered-source overlap may be preview-limited; preview overlap must not be treated as full 59-work pool overlap.",
    "Candidate selection is label-blind; v10 labels are not used to choose works.",
)


class MLShadowScorerSecondHybridCandidatePlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondHybridCandidatePlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondHybridCandidatePlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondHybridCandidatePlanError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondHybridCandidatePlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    if metadata.get("artifact_type") != artifact_type:
        raise MLShadowScorerSecondHybridCandidatePlanError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerSecondHybridCandidatePlanError(f"{name} metadata.{version_field} must be {version}")
    return metadata


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()}))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _ids_from_rows(rows: Any, *, fields: Sequence[str]) -> set[str]:
    out: set[str] = set()
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        for field in fields:
            canonical = _canonical_from_value(row.get(field))
            if canonical:
                out.add(canonical)
                break
    return out


def _validate_expansion_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="second-candidate-source-expansion-plan",
        artifact_type=EXPANSION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=EXPANSION_PLAN_VERSION,
    )
    if payload.get("second_candidate_source_expansion_plan_defined") is not True:
        raise MLShadowScorerSecondHybridCandidatePlanError(
            "expansion plan second_candidate_source_expansion_plan_defined must be true"
        )
    if payload.get("recommended_next_stage") != EXPECTED_EXPANSION_NEXT_STAGE:
        raise MLShadowScorerSecondHybridCandidatePlanError(
            f"expansion plan recommended_next_stage must be {EXPECTED_EXPANSION_NEXT_STAGE}"
        )
    blocker = payload.get("current_blocker_summary")
    if not isinstance(blocker, Mapping):
        raise MLShadowScorerSecondHybridCandidatePlanError("expansion plan missing current_blocker_summary")
    observed_gap = int(blocker.get("candidate_gap") or 0)
    recomputed_gap = max(
        0,
        int(blocker.get("minimum_confirmatory_eligible_work_count") or 100)
        - int(blocker.get("best_confirmatory_eligible_work_count") or 0),
    )
    if observed_gap != recomputed_gap:
        raise MLShadowScorerSecondHybridCandidatePlanError("expansion plan candidate_gap does not match blocker counts")
    return metadata


def _validate_generalization_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-plan",
        artifact_type=GENERALIZATION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=GENERALIZATION_PLAN_VERSION,
    )
    if payload.get("generalization_audit_plan_defined") is not True:
        raise MLShadowScorerSecondHybridCandidatePlanError("generalization audit plan must be defined")
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerSecondHybridCandidatePlanError(
            "generalization audit plan runtime_implementation_authorized must be false"
        )
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    metadata = _validate_identity(
        payload,
        name="fresh-surface-policy",
        artifact_type=FRESH_SURFACE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=FRESH_SURFACE_POLICY_VERSION,
    )
    if metadata.get("disallowed_eval_work_set_sha256") != OLD_217_EVAL_SHA:
        raise MLShadowScorerSecondHybridCandidatePlanError("fresh-surface policy old 217 SHA mismatch")
    thresholds = _get(payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLShadowScorerSecondHybridCandidatePlanError("fresh-surface policy missing label thresholds")
    return metadata, thresholds


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    version = payload.get("dataset_version") or _get(payload, "metadata.dataset_version")
    if version != LABEL_DATASET_VERSION:
        raise MLShadowScorerSecondHybridCandidatePlanError(
            f"label dataset version must be {LABEL_DATASET_VERSION}, got {version!r}"
        )


def _validate_offline_scoring_v3(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="offline-production-candidate-scoring-v3",
        artifact_type=OFFLINE_SCORING_ARTIFACT_TYPE,
        version_field="experiment_version",
        version=OFFLINE_SCORING_VERSION,
    )
    if metadata.get("eval_work_set_sha256") != OLD_217_EVAL_SHA:
        raise MLShadowScorerSecondHybridCandidatePlanError("offline scoring v3 eval_work_set_sha256 mismatch")
    old_ids = _ids_from_rows(payload.get("candidate_pool_rows"), fields=("canonical_openalex_work_id", "openalex_id"))
    if old_ids and _work_set_sha256(sorted(old_ids)) != OLD_217_EVAL_SHA:
        raise MLShadowScorerSecondHybridCandidatePlanError("offline scoring v3 candidate_pool_rows do not match old 217 SHA")
    return metadata


def _validate_first_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="first-validated-surface",
        artifact_type=FIRST_SURFACE_ARTIFACT_TYPE,
        version_field="surface_version",
        version=FIRST_SURFACE_VERSION,
    )
    candidate_pool = payload.get("candidate_pool")
    if not isinstance(candidate_pool, Mapping):
        raise MLShadowScorerSecondHybridCandidatePlanError("first validated surface missing candidate_pool")
    if candidate_pool.get("candidate_work_set_sha256") != FIRST_VALIDATED_SURFACE_SHA:
        raise MLShadowScorerSecondHybridCandidatePlanError("first validated surface candidate SHA mismatch")
    rows = candidate_pool.get("candidate_rows")
    if not isinstance(rows, list) or not rows:
        raise MLShadowScorerSecondHybridCandidatePlanError("first validated surface candidate_pool.candidate_rows missing")
    return metadata


def _validate_second_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-second-surface",
        artifact_type=SECOND_SURFACE_ARTIFACT_TYPE,
        version_field="surface_version",
        version=SECOND_SURFACE_VERSION,
    )
    sources = payload.get("candidate_sources_considered")
    if not isinstance(sources, list) or not sources:
        raise MLShadowScorerSecondHybridCandidatePlanError("generalization second-surface candidate_sources_considered missing")
    return metadata


def _old_217_ids(scoring_payload: Mapping[str, Any]) -> set[str]:
    return _ids_from_rows(scoring_payload.get("candidate_pool_rows"), fields=("canonical_openalex_work_id", "openalex_id"))


def _first_surface_ids(surface_payload: Mapping[str, Any]) -> set[str]:
    return _ids_from_rows(
        _get(surface_payload, "candidate_pool.candidate_rows"),
        fields=("canonical_openalex_work_id", "openalex_id"),
    )


def _source_entry_for_underpowered(second_surface_payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    sources = second_surface_payload.get("candidate_sources_considered")
    if not isinstance(sources, list):
        return None
    for source in sources:
        if (
            isinstance(source, Mapping)
            and source.get("ranking_run_id") == UNDERPOWERED_RANKING_RUN_ID
            and source.get("candidate_pool_work_set_sha256") == UNDERPOWERED_SOURCE_SHA
        ):
            return source
    return None


def _underpowered_overlap_context(second_surface_payload: Mapping[str, Any]) -> dict[str, Any]:
    source = _source_entry_for_underpowered(second_surface_payload)
    if source is None:
        return {
            "underpowered_candidate_pool_work_count": None,
            "full_underpowered_overlap_available": False,
            "underpowered_ids": set(),
            "preview_ids": set(),
            "underpowered_source_overlap_preview_count": 0,
        }
    pool_count = int(source.get("candidate_pool_work_count") or 0)
    full_ids = _ids_from_rows(source.get("candidate_pool_work_ids"), fields=("canonical_openalex_work_id", "openalex_id", "id"))
    if not full_ids and isinstance(source.get("candidate_pool_work_ids"), list):
        full_ids = {
            canonical
            for canonical in (_canonical_from_value(value) for value in source.get("candidate_pool_work_ids") or [])
            if canonical
        }
    for rows_key in ("candidate_pool_rows", "candidate_rows"):
        rows = source.get(rows_key)
        ids = _ids_from_rows(rows, fields=("canonical_openalex_work_id", "openalex_id"))
        if ids and len(ids) == pool_count:
            full_ids = ids
            break
    if full_ids and len(full_ids) == pool_count:
        return {
            "underpowered_candidate_pool_work_count": pool_count,
            "full_underpowered_overlap_available": True,
            "underpowered_ids": full_ids,
            "preview_ids": set(),
            "underpowered_source_overlap_preview_count": 0,
        }
    preview_ids = set()
    preview_ids.update(_ids_from_rows(source.get("candidate_row_preview"), fields=("canonical_openalex_work_id", "openalex_id")))
    preview_ids.update(
        canonical
        for canonical in (_canonical_from_value(value) for value in source.get("overlap_work_ids_preview") or [])
        if canonical
    )
    return {
        "underpowered_candidate_pool_work_count": pool_count,
        "full_underpowered_overlap_available": False,
        "underpowered_ids": set(),
        "preview_ids": preview_ids,
        "underpowered_source_overlap_preview_count": len(preview_ids),
    }


def _dedupe_selected_candidates(
    rows: Sequence[Mapping[str, Any]],
    *,
    old_217_ids: set[str],
    first_surface_ids: set[str],
    underpowered_full_ids: set[str],
    underpowered_preview_ids: set[str],
    full_underpowered_overlap_available: bool,
) -> list[dict[str, Any]]:
    by_work: dict[str, dict[str, Any]] = {}
    for row in rows:
        canonical = _canonical_from_value(row.get("openalex_id")) or _canonical_from_value(row.get("canonical_openalex_work_id"))
        if canonical is None or canonical in by_work:
            continue
        bucket = str(row.get("bucket_id") or "unknown")
        old_overlap = canonical in old_217_ids
        first_overlap = canonical in first_surface_ids
        if full_underpowered_overlap_available:
            underpowered_overlap = canonical in underpowered_full_ids
        else:
            underpowered_overlap = canonical in underpowered_preview_ids
        item = {
            "openalex_id": row.get("openalex_id") or f"https://openalex.org/{canonical}",
            "canonical_openalex_work_id": canonical,
            "title": row.get("title"),
            "year": row.get("year"),
            "citation_count": row.get("citation_count"),
            "source_display_name": row.get("source_display_name"),
            "bucket_id": bucket,
            "inclusion_reason": row.get("inclusion_reason"),
            "matched_terms": row.get("matched_terms") or [],
            "old_217_overlap": old_overlap,
            "first_validated_surface_overlap": first_overlap,
            "underpowered_source_overlap": underpowered_overlap,
            "underpowered_overlap_basis": "full" if full_underpowered_overlap_available else "preview",
            "confirmatory_metric_candidate_after_exclusions": not old_overlap and not first_overlap,
            "negative_or_borderline_candidate": bucket in NEGATIVE_OR_BORDERLINE_BUCKET_IDS,
            "label_used_for_selection": False,
        }
        by_work[canonical] = item
    return [by_work[work_id] for work_id in sorted(by_work)]


def _bucket_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_bucket: dict[str, dict[str, Any]] = {}
    rollups = {
        key: {"rollup_bucket": key, "selected_count": 0, "confirmatory_after_exclusions_count": 0}
        for key in ROLLUP_BUCKETS
    }
    for row in candidates:
        bucket = str(row.get("bucket_id") or "unknown")
        entry = by_bucket.setdefault(
            bucket,
            {
                "bucket_id": bucket,
                "selected_count": 0,
                "confirmatory_after_exclusions_count": 0,
                "old_217_overlap_count": 0,
                "first_validated_surface_overlap_count": 0,
                "underpowered_source_overlap_count": 0,
                "negative_or_borderline_intent": bucket in NEGATIVE_OR_BORDERLINE_BUCKET_IDS,
            },
        )
        entry["selected_count"] += 1
        if row.get("confirmatory_metric_candidate_after_exclusions") is True:
            entry["confirmatory_after_exclusions_count"] += 1
        if row.get("old_217_overlap") is True:
            entry["old_217_overlap_count"] += 1
        if row.get("first_validated_surface_overlap") is True:
            entry["first_validated_surface_overlap_count"] += 1
        if row.get("underpowered_source_overlap") is True:
            entry["underpowered_source_overlap_count"] += 1
        for rollup, bucket_ids in ROLLUP_BUCKETS.items():
            if bucket in bucket_ids:
                rollups[rollup]["selected_count"] += 1
                if row.get("confirmatory_metric_candidate_after_exclusions") is True:
                    rollups[rollup]["confirmatory_after_exclusions_count"] += 1
    shortfall = None
    if rollups["borderline_or_negative_candidate"]["selected_count"] == 0:
        shortfall = {
            "shortfall_type": "borderline_or_negative_candidate_bucket_absent",
            "recommended_action": "revise_candidate_plan_queries_to_add_borderline_or_negative_oriented_buckets",
        }
    return {
        "rollups": rollups,
        "by_bucket": [by_bucket[key] for key in sorted(by_bucket)],
        "shortfall_report": shortfall,
    }


@contextmanager
def _target_aware_bucket_caps(target_max: int) -> Iterator[None]:
    original_caps = dict(corpus_v2_candidate_plan_module.V2_BUCKET_CAPS)
    total = sum(original_caps.values())
    if target_max <= total:
        yield
        return
    scale = target_max / total
    expanded = {bucket: max(cap, int(round(cap * scale))) for bucket, cap in original_caps.items()}
    while sum(expanded.values()) < target_max:
        largest = max(expanded, key=expanded.get)
        expanded[largest] += 1
    corpus_v2_candidate_plan_module.V2_BUCKET_CAPS = expanded
    try:
        yield
    finally:
        corpus_v2_candidate_plan_module.V2_BUCKET_CAPS = original_caps


def _contact_provenance(*, mailto: str | None, corpus_plan: Mapping[str, Any]) -> dict[str, Any]:
    mailto_cli = bool((mailto or "").strip())
    mailto_env = bool((os.environ.get("OPENALEX_MAILTO") or "").strip())
    api_key_env = bool((os.environ.get("OPENALEX_API_KEY") or "").strip())
    if mailto_cli:
        contact_mode = "cli"
    elif mailto_env:
        contact_mode = "env"
    elif api_key_env:
        contact_mode = "api_key_only"
    else:
        contact_mode = "none"
    return {
        "contact_mode": corpus_plan.get("contact_mode") or contact_mode,
        "contact_provided": bool(corpus_plan.get("contact_provided") or mailto_cli or mailto_env),
        "auth_mode": corpus_plan.get("auth_mode") or ("api_key" if api_key_env else "no_key"),
        "api_key_provided": bool(corpus_plan.get("api_key_provided") or api_key_env),
        "raw_mailto_stored": False,
        "mailto_cli_provided": mailto_cli,
    }


def build_ml_shadow_scorer_second_hybrid_candidate_plan_payload(
    *,
    second_candidate_source_expansion_plan_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    offline_production_candidate_scoring_v3_path: Path,
    first_validated_surface_path: Path,
    generalization_second_surface_path: Path,
    corpus_candidate_plan: Mapping[str, Any],
    target_min: int = DEFAULT_TARGET_MIN,
    target_max: int = DEFAULT_TARGET_MAX,
    mailto: str | None = None,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    expansion_path = Path(second_candidate_source_expansion_plan_path).resolve()
    audit_plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()
    scoring_path = Path(offline_production_candidate_scoring_v3_path).resolve()
    first_surface_path = Path(first_validated_surface_path).resolve()
    second_surface_path = Path(generalization_second_surface_path).resolve()

    expansion = _load_json_object(expansion_path)
    audit_plan = _load_json_object(audit_plan_path)
    policy = _load_json_object(policy_path)
    label_dataset = _load_json_object(label_path)
    scoring = _load_json_object(scoring_path)
    first_surface = _load_json_object(first_surface_path)
    second_surface = _load_json_object(second_surface_path)

    expansion_metadata = _validate_expansion_plan(expansion)
    audit_plan_metadata = _validate_generalization_plan(audit_plan)
    policy_metadata, thresholds = _validate_policy(policy)
    _validate_label_dataset(label_dataset)
    if not conflict_path.exists():
        raise MLShadowScorerSecondHybridCandidatePlanError(f"conflict policy does not exist: {conflict_path}")
    scoring_metadata = _validate_offline_scoring_v3(scoring)
    first_surface_metadata = _validate_first_surface(first_surface)
    second_surface_metadata = _validate_second_surface(second_surface)

    old_ids = _old_217_ids(scoring)
    first_ids = _first_surface_ids(first_surface)
    underpowered_context = _underpowered_overlap_context(second_surface)
    candidates = _dedupe_selected_candidates(
        list(corpus_candidate_plan.get("selected_candidates") or []),
        old_217_ids=old_ids,
        first_surface_ids=first_ids,
        underpowered_full_ids=set(underpowered_context["underpowered_ids"]),
        underpowered_preview_ids=set(underpowered_context["preview_ids"]),
        full_underpowered_overlap_available=bool(underpowered_context["full_underpowered_overlap_available"]),
    )
    selected_ids = [str(row["canonical_openalex_work_id"]) for row in candidates]
    selected_sha = _work_set_sha256(selected_ids) if selected_ids else None
    old_overlap_ids = sorted(set(selected_ids).intersection(old_ids))
    first_overlap_ids = sorted(set(selected_ids).intersection(first_ids))
    confirmatory_ids = [
        str(row["canonical_openalex_work_id"])
        for row in candidates
        if row.get("confirmatory_metric_candidate_after_exclusions") is True
    ]
    if underpowered_context["full_underpowered_overlap_available"]:
        underpowered_overlap_ids = sorted(set(selected_ids).intersection(set(underpowered_context["underpowered_ids"])))
        preview_overlap_ids: list[str] = []
    else:
        underpowered_overlap_ids = []
        preview_overlap_ids = sorted(set(selected_ids).intersection(set(underpowered_context["preview_ids"])))
    minimum_confirmatory = int(thresholds.get("minimum_candidate_work_count") or 100)
    candidate_threshold_plausibly_met = len(confirmatory_ids) >= minimum_confirmatory
    expected_next_stage = (
        "ingest_second_hybrid_candidate_plan_as_snapshot_v1"
        if candidate_threshold_plausibly_met
        else "revise_second_candidate_plan_queries"
    )
    bucket_summary = _bucket_summary(candidates)
    current_blocker = expansion["current_blocker_summary"]

    inputs = [
        _input_record("second_candidate_source_expansion_plan", expansion_path, repo_root=root),
        _input_record("generalization_audit_plan", audit_plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
        _input_record("offline_production_candidate_scoring_v3", scoring_path, repo_root=root),
        _input_record("first_validated_surface", first_surface_path, repo_root=root),
        _input_record("generalization_second_surface", second_surface_path, repo_root=root),
    ]
    contact = _contact_provenance(mailto=mailto, corpus_plan=corpus_candidate_plan)
    readiness_estimate: dict[str, Any] = {
        "selected_total": len(candidates),
        "planned_candidate_work_set_sha256": selected_sha,
        "estimated_overlap_with_old_217": len(old_overlap_ids),
        "estimated_overlap_with_first_validated_surface": len(first_overlap_ids),
        "full_underpowered_overlap_available": bool(underpowered_context["full_underpowered_overlap_available"]),
        "underpowered_candidate_pool_work_count": underpowered_context["underpowered_candidate_pool_work_count"],
        "underpowered_source_overlap_preview_count": underpowered_context["underpowered_source_overlap_preview_count"],
        "estimated_confirmatory_eligible_after_exclusions": len(confirmatory_ids),
        "candidate_threshold_plausibly_met": candidate_threshold_plausibly_met,
        "minimum_confirmatory_eligible": minimum_confirmatory,
        "expected_next_stage": expected_next_stage,
    }
    if underpowered_context["full_underpowered_overlap_available"]:
        readiness_estimate["estimated_overlap_with_underpowered_source"] = len(underpowered_overlap_ids)
    else:
        readiness_estimate["estimated_overlap_with_underpowered_source_preview"] = len(preview_overlap_ids)
        readiness_estimate["estimated_new_confirmatory_outside_underpowered_preview"] = len(
            set(confirmatory_ids).difference(preview_overlap_ids)
        )

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
            "first_validated_surface_overlap": row.get("first_validated_surface_overlap"),
            "underpowered_source_overlap": row.get("underpowered_source_overlap"),
            "underpowered_overlap_basis": row.get("underpowered_overlap_basis"),
            "confirmatory_metric_candidate_after_exclusions": row.get("confirmatory_metric_candidate_after_exclusions"),
            "negative_or_borderline_candidate": row.get("negative_or_borderline_candidate"),
            "label_used_for_selection": False,
        }
        for row in candidates
    ]

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "source_expansion_plan_version": expansion_metadata.get("plan_version"),
            "source_generalization_plan_version": audit_plan_metadata.get("plan_version"),
            "source_fresh_surface_policy_version": policy_metadata.get("policy_version"),
            "source_label_dataset_version": label_dataset.get("dataset_version") or _get(label_dataset, "metadata.dataset_version"),
            "source_offline_scoring_version": scoring_metadata.get("experiment_version"),
            "source_first_surface_version": first_surface_metadata.get("surface_version"),
            "source_second_surface_version": second_surface_metadata.get("surface_version"),
            "openalex_contact_provenance": contact,
            "caveats": list(CAVEATS),
        },
        "second_hybrid_candidate_plan_defined": True,
        "generalization_audit_executed": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "planning_context": {
            "best_existing_source": current_blocker.get("best_distinct_ranking_run_id") or UNDERPOWERED_RANKING_RUN_ID,
            "best_existing_confirmatory_eligible": int(current_blocker.get("best_confirmatory_eligible_work_count") or 0),
            "candidate_gap": int(current_blocker.get("candidate_gap") or 0),
            "minimum_confirmatory_eligible": minimum_confirmatory,
            "first_validated_surface_sha256": FIRST_VALIDATED_SURFACE_SHA,
            "first_validated_ranking_run_id": FIRST_VALIDATED_RANKING_RUN_ID,
            "underpowered_source_sha256": UNDERPOWERED_SOURCE_SHA,
            "old_217_eval_sha256": OLD_217_EVAL_SHA,
            "target_min": target_min,
            "target_max": target_max,
            "full_underpowered_overlap_available": bool(underpowered_context["full_underpowered_overlap_available"]),
            "buffer_rationale": "target-min 180 buffers old/first-surface overlap, preview-limited underpowered overlap, hydration attrition, ranking filters, and later exclusions while aiming to leave at least 100 confirmatory-eligible works.",
        },
        "candidate_selection": {
            "selected_total": len(candidates),
            "target_min": target_min,
            "target_max": target_max,
            "selected_candidates": selected_candidates,
            "planned_candidate_work_set_sha256": selected_sha,
            "candidate_threshold_plausibly_met": candidate_threshold_plausibly_met,
            "source_corpus_v2_selected_total": corpus_candidate_plan.get("selected_total"),
            "source_corpus_v2_dedup_statistics": corpus_candidate_plan.get("dedup_statistics"),
            "label_dataset_used_for_selection": False,
        },
        "bucket_summary": bucket_summary,
        "overlap_estimates": {
            "old_217_overlap_work_ids_preview": old_overlap_ids[:25],
            "first_validated_surface_overlap_work_ids_preview": first_overlap_ids[:25],
            "underpowered_overlap_work_ids_preview": (underpowered_overlap_ids or preview_overlap_ids)[:25],
            **readiness_estimate,
        },
        "readiness_estimate": readiness_estimate,
        "learned_probability_followup": {
            "full_audit_embedding_probability_work_coverage_required": True,
            "approved_path": "approved upstream frozen ml-offline-audit-embedding-scorer-v2 application to pre-existing embeddings",
            "this_plan_generates_embeddings_or_probabilities": False,
        },
        "labeling_followup": {
            "pool_ge_100_does_not_imply_label_ready": True,
            "minimum_confirmatory_labeled_work_count": thresholds["minimum_confirmatory_labeled_work_count"],
            "minimum_confirmatory_positive_work_count": thresholds["minimum_confirmatory_positive_work_count"],
            "minimum_confirmatory_negative_work_count": thresholds["minimum_confirmatory_negative_work_count"],
            "minimum_distinct_negative_work_count": thresholds["minimum_distinct_negative_work_count"],
            "minimum_confirmatory_label_coverage_rate": thresholds["minimum_confirmatory_label_coverage_rate"],
            "labels_metric_only_never_scoring_features": True,
        },
        "openalex_contact_provenance": contact,
        "blocked_actions": [
            "database_access",
            "database_writes",
            "snapshot_creation",
            "ranking_run_creation",
            "embedding_generation",
            "learned_scorer_application",
            "scorer_execution",
            "label_ingest",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "missing_second_hybrid_candidate_plan_v1": False,
            "missing_second_fresh_candidate_source": True,
            "missing_generalization_audit_on_second_surface": True,
            "missing_generalization_audit_gates": True,
            "missing_online_shadow_implementation_disabled_by_default": True,
            "missing_shadow_runtime_isolation_verification": True,
            "missing_production_readiness_authorization": True,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "recommended_next_stage": expected_next_stage,
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}" if abs(value) < 1 else f"{value:.2f}"
    return str(value)


def markdown_from_ml_shadow_scorer_second_hybrid_candidate_plan(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    context = payload["planning_context"]
    readiness = payload["readiness_estimate"]
    selection = payload["candidate_selection"]
    lines = [
        f"# ML Shadow Scorer v1 Second Hybrid Candidate Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This is a dry-run OpenAlex candidate acquisition plan for a second fresh surface. It does not ingest, write a database, rank, embed, apply a learned scorer, execute shadow scoring, or authorize production.",
        "",
        f"- Selected candidates: {selection['selected_total']}",
        f"- Estimated confirmatory-eligible after old/first-surface exclusions: {readiness['estimated_confirmatory_eligible_after_exclusions']}",
        f"- Candidate threshold plausibly met: {readiness['candidate_threshold_plausibly_met']}",
        f"- Expected next stage: `{readiness['expected_next_stage']}`",
        f"- Shadow scoring allowed: {payload['shadow_and_production_blockers']['shadow_scoring_allowed']}",
        f"- Production default allowed: {payload['shadow_and_production_blockers']['production_default_allowed']}",
        "",
        "## Planning Context",
        "",
        f"The best existing distinct source is `{context['best_existing_source']}` with {context['best_existing_confirmatory_eligible']} confirmatory-eligible works, leaving a gap of {context['candidate_gap']} against the policy minimum of {context['minimum_confirmatory_eligible']}.",
        "",
        "## Candidate Plan Size And SHA",
        "",
        f"- Selected total: {selection['selected_total']} (target range {context['target_min']}-{context['target_max']})",
        f"- Planned candidate work-set SHA: `{selection['planned_candidate_work_set_sha256']}`",
        "",
        "## Overlap Estimates",
        "",
        f"- Old 217 overlap estimate: {readiness['estimated_overlap_with_old_217']}",
        f"- First validated surface overlap estimate: {readiness['estimated_overlap_with_first_validated_surface']}",
        f"- Full underpowered overlap available: {readiness['full_underpowered_overlap_available']}",
    ]
    if readiness.get("full_underpowered_overlap_available"):
        lines.append(f"- Underpowered overlap estimate: {readiness.get('estimated_overlap_with_underpowered_source')}")
    else:
        lines.append(
            f"- Underpowered preview overlap estimate: {readiness.get('estimated_overlap_with_underpowered_source_preview')}"
        )
    lines.extend(["", "## Bucket Composition", "", "| Rollup | Selected | Confirmatory after exclusions |", "| --- | ---: | ---: |"])
    for rollup in payload["bucket_summary"]["rollups"].values():
        lines.append(
            f"| `{rollup['rollup_bucket']}` | {rollup['selected_count']} | {rollup['confirmatory_after_exclusions_count']} |"
        )
    lines.extend(["", "### Raw Buckets", "", "| Bucket | Selected | Confirmatory after exclusions | Old overlap | First-surface overlap | Borderline intent |", "| --- | ---: | ---: | ---: | ---: | --- |"])
    for bucket in payload["bucket_summary"]["by_bucket"]:
        lines.append(
            f"| `{bucket['bucket_id']}` | {bucket['selected_count']} | {bucket['confirmatory_after_exclusions_count']} | "
            f"{bucket['old_217_overlap_count']} | {bucket['first_validated_surface_overlap_count']} | {bucket['negative_or_borderline_intent']} |"
        )
    if payload["bucket_summary"].get("shortfall_report"):
        lines.extend(["", f"Shortfall: `{payload['bucket_summary']['shortfall_report']['shortfall_type']}`"])
    lines.extend(
        [
            "",
            "## Follow-Ups",
            "",
            "- Learned probability coverage must come later from an approved frozen scorer application to pre-existing embeddings.",
            "- Labels may still block future audit execution; labels are metric-only and never scoring features.",
            "",
            "## Not Ingest / Not Runtime / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_second_hybrid_candidate_plan(
    *,
    second_candidate_source_expansion_plan_path: Path,
    generalization_audit_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    offline_production_candidate_scoring_v3_path: Path,
    first_validated_surface_path: Path,
    generalization_second_surface_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    target_min: int = DEFAULT_TARGET_MIN,
    target_max: int = DEFAULT_TARGET_MAX,
    mailto: str | None = None,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    if target_min < 1 or target_max < target_min:
        raise MLShadowScorerSecondHybridCandidatePlanError("target-min/target-max are invalid")
    contact_mode = "cli" if (mailto or "").strip() else ("env" if (os.environ.get("OPENALEX_MAILTO") or "").strip() else "api_key_only")
    contact_provided = bool((mailto or "").strip() or (os.environ.get("OPENALEX_MAILTO") or "").strip())
    try:
        resolved_mailto = resolve_corpus_expansion_preview_mailto(mailto=mailto or "", mock_openalex=False)
    except ValueError as exc:
        raise MLShadowScorerSecondHybridCandidatePlanError(
            "ml-shadow-scorer-second-hybrid-candidate-plan live OpenAlex mode requires OPENALEX_API_KEY and/or contact: set OPENALEX_API_KEY, pass --mailto, or set OPENALEX_MAILTO"
        ) from exc
    per_bucket_limit = min(max(target_max, target_min, 100), 500)
    with _target_aware_bucket_caps(target_max):
        corpus_plan = run_corpus_v2_candidate_plan(
            policy=CorpusPolicy(),
            mailto=resolved_mailto,
            contact_mode=contact_mode,
            contact_provided=contact_provided,
            per_bucket_limit=per_bucket_limit,
            target_min=target_min,
            target_max=target_max,
            mock_openalex=False,
        )
    payload = build_ml_shadow_scorer_second_hybrid_candidate_plan_payload(
        second_candidate_source_expansion_plan_path=second_candidate_source_expansion_plan_path,
        generalization_audit_plan_path=generalization_audit_plan_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        offline_production_candidate_scoring_v3_path=offline_production_candidate_scoring_v3_path,
        first_validated_surface_path=first_validated_surface_path,
        generalization_second_surface_path=generalization_second_surface_path,
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
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_hybrid_candidate_plan(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLShadowScorerSecondHybridCandidatePlanError",
    "NEGATIVE_OR_BORDERLINE_BUCKET_IDS",
    "build_ml_shadow_scorer_second_hybrid_candidate_plan_payload",
    "markdown_from_ml_shadow_scorer_second_hybrid_candidate_plan",
    "write_ml_shadow_scorer_second_hybrid_candidate_plan",
    "_work_set_sha256",
]
