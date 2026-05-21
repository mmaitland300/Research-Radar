"""Disabled-by-default ml-shadow-scorer-v1 implementation audit.

This module implements the frozen rank-fusion formula and verifies it by
replaying the committed fresh validation candidate rows. It does not query a
database, call external services, train, generate embeddings, run ranking,
ingest labels, or enable shadow execution.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence as SequenceABC
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_hybrid_scorer_offline_experiment import _rank_percentiles
from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_v1_implementation"
IMPLEMENTATION_VERSION = "ml-shadow-scorer-v1-implementation"
IMPLEMENTATION_MODE = "audit_replay_only"

SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
VALIDATION_ARTIFACT_TYPE = "ml_hybrid_validation_on_fresh_surface"
VALIDATION_VERSION = "ml-hybrid-validation-on-fresh-surface-v1"
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"

SCORER_ID = "ml-shadow-scorer-v1"
FORMULA_ID = "hybrid_rank_mean_50_50"
FORMULA_LITERAL = "score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)"
RANKING_RUN_ID = "rank-9f4b2a2084"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-fresh-hybrid-v1-20260518"
EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
EXPECTED_CANDIDATE_POOL_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
EXPECTED_REPLAY_ROW_COUNT = 358
REPLAY_TOLERANCE = 1e-12

CAVEATS = (
    "Implementation audit only; shadow execution remains disabled.",
    "No database access, scoring rerun, embedding generation, ranking run, label ingest, API/web change, or production change.",
    "Replay uses validation['candidate_work_scores'] exactly.",
    "Labels in validation rows are ignored by scoring.",
)


class MLShadowScorerV1Error(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerV1Error(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerV1Error(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerV1Error(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerV1Error(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()}))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def compute_rank_percentiles(values: Sequence[Any]) -> list[float]:
    """Return rank percentiles aligned to values using the frozen policy."""

    rows = [{"canonical_openalex_work_id": f"__row_{index}", "score": value} for index, value in enumerate(values)]
    ranked = _rank_percentiles(rows, "score")
    return [ranked[f"__row_{index}"] for index in range(len(values))]


def validate_shadow_scorer_inputs(candidate_work_scores: Sequence[Mapping[str, Any]]) -> None:
    if not isinstance(candidate_work_scores, SequenceABC) or isinstance(candidate_work_scores, (str, bytes)):
        raise MLShadowScorerV1Error("candidate_work_scores must be a sequence of objects")
    seen: set[str] = set()
    for index, row in enumerate(candidate_work_scores):
        if not isinstance(row, Mapping):
            raise MLShadowScorerV1Error(f"candidate_work_scores[{index}] must be an object")
        work_id = str(row.get("canonical_openalex_work_id") or "").strip()
        if not work_id:
            raise MLShadowScorerV1Error(f"candidate_work_scores[{index}] missing canonical_openalex_work_id")
        if work_id in seen:
            raise MLShadowScorerV1Error(f"duplicate canonical_openalex_work_id in candidate_work_scores: {work_id}")
        seen.add(work_id)
        if _float_or_none(row.get("final_score")) is None:
            raise MLShadowScorerV1Error(f"candidate row {work_id} missing final_score")
        if _float_or_none(row.get("audit_embedding_probability_work")) is None:
            raise MLShadowScorerV1Error(f"candidate row {work_id} missing audit_embedding_probability_work")


def compute_shadow_score_rows(candidate_work_scores: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    validate_shadow_scorer_inputs(candidate_work_scores)
    candidates = [dict(row) for row in candidate_work_scores]
    final_rank_pct = _rank_percentiles(candidates, "final_score")
    learned_rank_pct = _rank_percentiles(candidates, "audit_embedding_probability_work")

    out: list[dict[str, Any]] = []
    for row in candidates:
        work_id = str(row["canonical_openalex_work_id"])
        final_rank = float(final_rank_pct[work_id])
        learned_rank = float(learned_rank_pct[work_id])
        score = 0.5 * final_rank + 0.5 * learned_rank
        out.append(
            {
                "canonical_openalex_work_id": work_id,
                "title": row.get("title"),
                "final_score": float(row["final_score"]),
                "audit_embedding_probability_work": float(row["audit_embedding_probability_work"]),
                "final_score_rank_pct": final_rank,
                "audit_embedding_probability_rank_pct": learned_rank,
                "ml_shadow_scorer_v1_score": score,
                "ranking_run_id": row.get("ranking_run_id"),
                "family": row.get("family"),
                "heuristic_rank": row.get("heuristic_rank"),
            }
        )
    return out


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="shadow-scorer-spec")
    if metadata.get("artifact_type") != SPEC_ARTIFACT_TYPE:
        raise MLShadowScorerV1Error("spec metadata.artifact_type must be ml_shadow_scorer_spec")
    if metadata.get("spec_version") != SPEC_VERSION:
        raise MLShadowScorerV1Error("spec metadata.spec_version must be ml-shadow-scorer-v1-spec")
    if payload.get("spec_ready_for_implementation") is not True:
        raise MLShadowScorerV1Error("spec_ready_for_implementation must be true")
    if payload.get("recommended_next_stage") != "implement_ml_shadow_scorer_v1_disabled_by_default":
        raise MLShadowScorerV1Error("spec recommended_next_stage must be implement_ml_shadow_scorer_v1_disabled_by_default")
    if payload.get("shadow_scoring_allowed") is not False:
        raise MLShadowScorerV1Error("spec shadow_scoring_allowed must be false")
    if payload.get("production_default_allowed") is not False:
        raise MLShadowScorerV1Error("spec production_default_allowed must be false")
    if _get(payload, "scoring_formula.formula_id") != FORMULA_ID:
        raise MLShadowScorerV1Error("spec formula id must be hybrid_rank_mean_50_50")
    if _get(payload, "scoring_formula.scoring_formula_literal") != FORMULA_LITERAL:
        raise MLShadowScorerV1Error("spec formula literal mismatch")
    components = _get(payload, "scoring_formula.components")
    expected = {
        "final_score_rank_pct": ("rank_pct(final_score)", 0.5),
        "audit_embedding_probability_rank_pct": ("rank_pct(audit_embedding_probability_work)", 0.5),
    }
    if not isinstance(components, list) or len(components) != 2:
        raise MLShadowScorerV1Error("spec formula must contain exactly two 0.5 weighted components")
    for component in components:
        if not isinstance(component, Mapping):
            raise MLShadowScorerV1Error("spec formula component must be an object")
        name = str(component.get("name") or "")
        source_weight = expected.get(name)
        if source_weight is None:
            raise MLShadowScorerV1Error("spec formula component names mismatch")
        source, weight = source_weight
        if component.get("source") != source or _float_or_none(component.get("weight")) != weight:
            raise MLShadowScorerV1Error("spec formula weights exactly 0.5 final rank pct + 0.5 learned rank pct required")
    return metadata


def _validate_validation(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    metadata = _metadata(payload, name="hybrid-validation-on-fresh-surface")
    if metadata.get("artifact_type") != VALIDATION_ARTIFACT_TYPE:
        raise MLShadowScorerV1Error("validation metadata.artifact_type must be ml_hybrid_validation_on_fresh_surface")
    if metadata.get("validation_version") != VALIDATION_VERSION:
        raise MLShadowScorerV1Error("validation metadata.validation_version must be ml-hybrid-validation-on-fresh-surface-v1")
    rows = payload.get("candidate_work_scores")
    if not isinstance(rows, list):
        raise MLShadowScorerV1Error("validation candidate_work_scores must exist")
    if len(rows) != EXPECTED_REPLAY_ROW_COUNT:
        raise MLShadowScorerV1Error(f"validation candidate_work_scores must have {EXPECTED_REPLAY_ROW_COUNT} rows")
    for row in rows:
        if not isinstance(row, Mapping):
            raise MLShadowScorerV1Error("validation candidate_work_scores rows must be objects")
    return metadata, rows


def _validate_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-surface")
    if metadata.get("artifact_type") != SURFACE_ARTIFACT_TYPE:
        raise MLShadowScorerV1Error("fresh surface metadata.artifact_type must be ml_fresh_eval_surface_hybrid")
    if metadata.get("surface_version") != SURFACE_VERSION:
        raise MLShadowScorerV1Error("fresh surface metadata.surface_version must be ml-fresh-eval-surface-hybrid-v1")
    return metadata


def _cross_check_provenance(
    *,
    spec_payload: Mapping[str, Any],
    validation_payload: Mapping[str, Any],
    surface_payload: Mapping[str, Any],
) -> dict[str, Any]:
    validation_metadata = _metadata(validation_payload, name="hybrid-validation-on-fresh-surface")
    source = surface_payload.get("candidate_source")
    if not isinstance(source, Mapping):
        raise MLShadowScorerV1Error("fresh surface candidate_source must be present")
    surface_candidate_sha = _get(surface_payload, "candidate_pool.candidate_work_set_sha256")
    checks = {
        "ranking_run_id_matches": validation_metadata.get("ranking_run_id") == RANKING_RUN_ID
        and _get(validation_payload, "validation_scope.ranking_run_id") == RANKING_RUN_ID
        and source.get("ranking_run_id") == RANKING_RUN_ID
        and _get(spec_payload, "cross_artifact_provenance_checks.ranking_run_id") == RANKING_RUN_ID,
        "family_matches": validation_metadata.get("family") == FAMILY
        and _get(validation_payload, "validation_scope.family") == FAMILY
        and source.get("family") == FAMILY
        and _get(spec_payload, "cross_artifact_provenance_checks.family") == FAMILY,
        "corpus_snapshot_version_matches": validation_metadata.get("corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION
        and _get(validation_payload, "validation_scope.corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION
        and source.get("corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION
        and _get(spec_payload, "cross_artifact_provenance_checks.corpus_snapshot_version") == CORPUS_SNAPSHOT_VERSION,
        "embedding_version_matches": validation_metadata.get("embedding_version") == EMBEDDING_VERSION
        and _get(validation_payload, "validation_scope.embedding_version") == EMBEDDING_VERSION
        and _get(spec_payload, "cross_artifact_provenance_checks.embedding_version") == EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256_matches": validation_metadata.get("candidate_pool_work_set_sha256") == EXPECTED_CANDIDATE_POOL_SHA
        and surface_candidate_sha == EXPECTED_CANDIDATE_POOL_SHA
        and _get(spec_payload, "metadata.candidate_pool_work_set_sha256") == EXPECTED_CANDIDATE_POOL_SHA,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerV1Error(f"cross-artifact provenance checks failed: {failed}")
    return {
        **checks,
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
    }


def _percentile(values: Sequence[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * pct
    lower = int(pos)
    upper = min(lower + 1, len(ordered) - 1)
    weight = pos - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _score_distribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = [float(row["ml_shadow_scorer_v1_score"]) for row in rows]
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "p25": _percentile(values, 0.25),
        "median": _percentile(values, 0.5),
        "p75": _percentile(values, 0.75),
        "max": max(values) if values else None,
        "mean": (sum(values) / len(values)) if values else None,
    }


def _top_k_preview(rows: Sequence[Mapping[str, Any]], *, k: int = 20) -> list[dict[str, Any]]:
    top_by_final = {
        str(row["canonical_openalex_work_id"])
        for row in sorted(rows, key=lambda row: (-float(row["final_score"]), str(row["canonical_openalex_work_id"])))[:k]
    }
    top = sorted(
        rows,
        key=lambda row: (-float(row["ml_shadow_scorer_v1_score"]), str(row["canonical_openalex_work_id"])),
    )[:k]
    return [
        {
            "canonical_openalex_work_id": row["canonical_openalex_work_id"],
            "title": row.get("title"),
            "ml_shadow_scorer_v1_score": row["ml_shadow_scorer_v1_score"],
            "final_score_rank_pct": row["final_score_rank_pct"],
            "audit_embedding_probability_rank_pct": row["audit_embedding_probability_rank_pct"],
            "heuristic_rank": row.get("heuristic_rank"),
            "in_top_k_by_final_score": row["canonical_openalex_work_id"] in top_by_final,
        }
        for row in top
    ]


def _replay_summary(validation_rows: Sequence[Mapping[str, Any]], scored_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    validation_by_work = {str(row["canonical_openalex_work_id"]): row for row in validation_rows}
    max_score_delta = 0.0
    max_rank_delta = 0.0
    mismatches: list[dict[str, Any]] = []
    for row in scored_rows:
        work_id = str(row["canonical_openalex_work_id"])
        original = validation_by_work[work_id]
        expected_score = _float_or_none(_get(original, "arm_scores.hybrid_rank_mean_50_50"))
        expected_final_rank = _float_or_none(original.get("final_score_rank_pct"))
        expected_learned_rank = _float_or_none(original.get("audit_embedding_probability_rank_pct"))
        if expected_score is None or expected_final_rank is None or expected_learned_rank is None:
            raise MLShadowScorerV1Error(f"validation row {work_id} missing replay target fields")
        score_delta = abs(float(row["ml_shadow_scorer_v1_score"]) - expected_score)
        final_rank_delta = abs(float(row["final_score_rank_pct"]) - expected_final_rank)
        learned_rank_delta = abs(float(row["audit_embedding_probability_rank_pct"]) - expected_learned_rank)
        rank_delta = max(final_rank_delta, learned_rank_delta)
        max_score_delta = max(max_score_delta, score_delta)
        max_rank_delta = max(max_rank_delta, rank_delta)
        if score_delta > REPLAY_TOLERANCE or rank_delta > REPLAY_TOLERANCE:
            mismatches.append(
                {
                    "canonical_openalex_work_id": work_id,
                    "score_delta": score_delta,
                    "max_rank_pct_delta": rank_delta,
                }
            )
    candidate_sha = _work_set_sha256([str(row["canonical_openalex_work_id"]) for row in scored_rows])
    missing_learned = sum(1 for row in validation_rows if _float_or_none(row.get("audit_embedding_probability_work")) is None)
    implementation_matches_validation_replay = (
        len(validation_rows) == EXPECTED_REPLAY_ROW_COUNT
        and missing_learned == 0
        and candidate_sha == EXPECTED_CANDIDATE_POOL_SHA
        and max_score_delta <= REPLAY_TOLERANCE
        and max_rank_delta <= REPLAY_TOLERANCE
        and len(mismatches) == 0
    )
    return {
        "validation_primary_arm_id": FORMULA_ID,
        "candidate_work_scores_count": len(validation_rows),
        "candidate_pool_work_set_sha256": candidate_sha,
        "max_abs_score_delta": max_score_delta,
        "max_abs_rank_pct_delta": max_rank_delta,
        "mismatched_work_count": len(mismatches),
        "mismatched_work_preview": mismatches[:10],
        "replay_tolerance": REPLAY_TOLERANCE,
        "implementation_matches_validation_replay": implementation_matches_validation_replay,
        "missing_learned_probability_count": missing_learned,
    }


def build_ml_shadow_scorer_v1_audit_payload(
    *,
    shadow_scorer_spec_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    fresh_eval_surface_path: Path,
    output_implementation_version: str = IMPLEMENTATION_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    validation_path = Path(hybrid_validation_on_fresh_surface_path).resolve()
    surface_path = Path(fresh_eval_surface_path).resolve()

    spec_payload = _load_json_object(spec_path)
    validation_payload = _load_json_object(validation_path)
    surface_payload = _load_json_object(surface_path)

    spec_metadata = _validate_spec(spec_payload)
    validation_metadata, validation_rows = _validate_validation(validation_payload)
    surface_metadata = _validate_surface(surface_payload)
    provenance = _cross_check_provenance(
        spec_payload=spec_payload,
        validation_payload=validation_payload,
        surface_payload=surface_payload,
    )
    validate_shadow_scorer_inputs(validation_rows)
    scored_rows = compute_shadow_score_rows(validation_rows)
    replay = _replay_summary(validation_rows, scored_rows)
    implementation_matches_spec = bool(
        replay["implementation_matches_validation_replay"]
        and replay["candidate_work_scores_count"] == EXPECTED_REPLAY_ROW_COUNT
        and replay["missing_learned_probability_count"] == 0
        and replay["candidate_pool_work_set_sha256"] == EXPECTED_CANDIDATE_POOL_SHA
        and replay["mismatched_work_count"] == 0
    )
    inputs = [
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("hybrid_validation_on_fresh_surface", validation_path, repo_root=root),
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
    ]
    learned_coverage_count = len(scored_rows) - int(replay["missing_learned_probability_count"])

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "implementation_version": output_implementation_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "implementation_mode": IMPLEMENTATION_MODE,
            "shadow_execution_enabled": False,
            "source_spec_version": spec_metadata.get("spec_version"),
            "source_validation_version": validation_metadata.get("validation_version"),
            "source_surface_version": surface_metadata.get("surface_version"),
            "candidate_pool_work_set_sha256": replay["candidate_pool_work_set_sha256"],
            "caveats": list(CAVEATS),
        },
        "implementation_status": {
            "implemented": True,
            "disabled_by_default": True,
            "implementation_matches_spec": implementation_matches_spec,
            "implementation_matches_validation_replay": replay["implementation_matches_validation_replay"],
            "formula_id": FORMULA_ID,
            "candidate_pool_size": len(scored_rows),
            "learned_probability_coverage_count": learned_coverage_count,
            "missing_learned_probability_count": replay["missing_learned_probability_count"],
            "missing_ml_shadow_scorer_v1_implementation": False,
        },
        "spec_contract_summary": {
            "scorer_id": SCORER_ID,
            "formula_id": FORMULA_ID,
            "formula": FORMULA_LITERAL,
            "rank_percentile_policy": {
                "higher_raw_score_is_better": True,
                "ties": "average rank",
                "n_equals_1_behavior": "rank_pct = 1.0",
                "otherwise": "rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0))",
                "scope": "full candidate pool for that scoring run",
            },
        },
        "audit_replay_summary": replay,
        "cross_artifact_provenance_checks": provenance,
        "score_distribution": _score_distribution(scored_rows),
        "top_k_preview": _top_k_preview(scored_rows, k=20),
        "blocked_actions": [
            "shadow_execution_enablement",
            "production_default_change",
            "api_web_change",
            "ranking_run",
            "training",
            "embedding_generation",
            "label_ingest",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "production_default_changed": False,
            "api_web_changed": False,
            "missing_ml_shadow_scorer_v1_implementation": False,
            "missing_shadow_execution_readiness_gates": True,
            "missing_shadow_output_isolation_check": True,
        },
        "recommended_next_stage": "draft_ml_shadow_scorer_v1_execution_readiness_gates",
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def markdown_from_ml_shadow_scorer_v1_audit(payload: Mapping[str, Any]) -> str:
    status = payload["implementation_status"]
    replay = payload["audit_replay_summary"]
    distribution = payload["score_distribution"]
    lines = [
        f"# ML Shadow Scorer v1 Implementation Audit ({payload['metadata']['implementation_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact implements the frozen `ml-shadow-scorer-v1` formula and verifies it by exact replay against `validation[\"candidate_work_scores\"]`. Shadow execution remains disabled by default.",
        "",
        f"- Implementation matches spec: {status['implementation_matches_spec']}",
        f"- Implementation matches validation replay: {status['implementation_matches_validation_replay']}",
        f"- Candidate pool size: {status['candidate_pool_size']}",
        f"- Learned probability coverage: {status['learned_probability_coverage_count']} / {status['candidate_pool_size']}",
        f"- Shadow execution enabled: {payload['metadata']['shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Formula Implementation",
        "",
        f"- `{payload['spec_contract_summary']['formula']}`",
        "- The implementation ignores any label fields present in replay rows.",
        "",
        "## Replay Input Path",
        "",
        "- `validation[\"candidate_work_scores\"]` from `ml-hybrid-validation-on-fresh-surface-v1.json`.",
        "",
        "## Rank Percentile Policy",
        "",
        "- Higher raw score is better.",
        "- Ties use average rank.",
        "- If n == 1, rank_pct = 1.0.",
        "- Otherwise rank_pct = 1.0 - ((average_rank - 1.0) / (n - 1.0)).",
        "",
        "## Exact Replay Result",
        "",
        f"- Candidate work scores count: {replay['candidate_work_scores_count']}",
        f"- Candidate pool SHA: `{replay['candidate_pool_work_set_sha256']}`",
        f"- Max absolute score delta: {_fmt(replay['max_abs_score_delta'])}",
        f"- Max absolute rank percentile delta: {_fmt(replay['max_abs_rank_pct_delta'])}",
        f"- Mismatched work count: {replay['mismatched_work_count']}",
        f"- Replay tolerance: {replay['replay_tolerance']}",
        "",
        "## Score Distribution",
        "",
        f"- Min / p25 / median / p75 / max: {_fmt(distribution['min'])} / {_fmt(distribution['p25'])} / {_fmt(distribution['median'])} / {_fmt(distribution['p75'])} / {_fmt(distribution['max'])}",
        f"- Mean: {_fmt(distribution['mean'])}",
        "",
        "## Top-K Preview",
        "",
        "| Rank | Work | Score | Final rank pct | Learned rank pct | Heuristic rank |",
        "| ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    for index, row in enumerate(payload["top_k_preview"][:10], start=1):
        lines.append(
            f"| {index} | `{row['canonical_openalex_work_id']}` | {_fmt(row['ml_shadow_scorer_v1_score'])} | "
            f"{_fmt(row['final_score_rank_pct'])} | {_fmt(row['audit_embedding_probability_rank_pct'])} | {row.get('heuristic_rank')} |"
        )
    lines.extend(
        [
            "",
            "## Disabled By Default",
            "",
            "- `shadow_execution_enabled` is false.",
            "- Shadow scoring is not allowed by this artifact.",
            "- Production default is not allowed by this artifact.",
            "",
            "## Not Shadow Execution / Not Production",
            "",
            "- No shadow execution is enabled.",
            "- No production/API/web/ranking/default behavior is changed.",
            "- Execution readiness gates remain required before any shadow run.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_v1_audit(
    *,
    shadow_scorer_spec_path: Path,
    hybrid_validation_on_fresh_surface_path: Path,
    fresh_eval_surface_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    implementation_version: str = IMPLEMENTATION_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_v1_audit_payload(
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        hybrid_validation_on_fresh_surface_path=hybrid_validation_on_fresh_surface_path,
        fresh_eval_surface_path=fresh_eval_surface_path,
        output_implementation_version=implementation_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_v1_audit(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "IMPLEMENTATION_VERSION",
    "MLShadowScorerV1Error",
    "compute_rank_percentiles",
    "compute_shadow_score_rows",
    "validate_shadow_scorer_inputs",
    "build_ml_shadow_scorer_v1_audit_payload",
    "markdown_from_ml_shadow_scorer_v1_audit",
    "write_ml_shadow_scorer_v1_audit",
]
