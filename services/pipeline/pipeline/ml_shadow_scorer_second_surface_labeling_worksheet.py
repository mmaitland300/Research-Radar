"""Manual labeling worksheet for the second shadow-generalization surface.

This command creates a reviewer-blank CSV and context sidecar for all
confirmatory-eligible works on the selected second shadow-generalization
surface. It reads committed audit artifacts and performs SELECT-only local DB
queries. It does not ingest labels, rank, embed, score, or authorize
shadow/production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.ml_shadow_scorer_generalization_second_surface import (
    MLShadowScorerGeneralizationSecondSurfaceError,
    OLD_217_EVAL_SHA,
    SURFACE_VERSION as SECOND_SURFACE_VERSION,
    _database_url_from_env,
    _execute_select,
    _first_surface_ids,
    _old_eval_ids_from_v3,
    assert_local_database_url as _assert_second_surface_local_database_url,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_labeling_worksheet"
WORKSHEET_VERSION = "ml-shadow-scorer-second-surface-labeling-worksheet-v1"
DEFAULT_REVIEW_POOL_VARIANT = "ml_shadow_scorer_second_surface_generalization_v1"
DEFAULT_SEED = 20260522
DEFAULT_REQUESTED_ROWS = 0

DISCOVERY_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
LABEL_DATASET_VERSION = "ml-label-dataset-v10"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
EXPECTED_RANKING_RUN_ID = "rank-83787b91ef"
EXPECTED_CANDIDATE_SHA = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT = 168
EXPECTED_NEXT_STAGE = "create_second_surface_labeling_plan_for_shadow_generalization_v1"
TARGET = "good_or_acceptable"

CSV_COLUMNS = (
    "row_id",
    "worksheet_version",
    "review_pool_variant",
    "paper_id",
    "openalex_work_id",
    "work_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "abstract_preview",
    "sample_reason",
    "ranking_run_id",
    "family",
    "final_score",
    "rank_in_family",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

BLANK_LABEL_COLUMNS = ("relevance_label", "novelty_label", "bridge_like_label", "reviewer_notes")
ALLOWED_LABEL_SETS = {
    "relevance_label": ["good", "acceptable", "miss", "irrelevant"],
    "novelty_label": ["surprising", "useful", "obvious", "not_useful", "neither"],
    "bridge_like_label": ["yes", "partial", "no", "not_applicable"],
}

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

CAVEATS = (
    "Manual labeling worksheet only; no labels are ingested by this command.",
    "Rows are future second-surface confirmatory eval candidates, not validation results.",
    "Old 217-work and first validated surface overlaps are excluded.",
    "Existing v10-labeled canonical works are excluded.",
    "Reviewer label columns are intentionally blank.",
    "No database writes, ranking, embeddings, learned probability generation, scorer execution, shadow, or production authorization.",
)


class MLShadowScorerSecondSurfaceLabelingWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return dict(_assert_second_surface_local_database_url(database_url))
    except MLShadowScorerGeneralizationSecondSurfaceError as exc:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(str(exc), code=exc.code) from exc


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _canonical_work_id_from_label(row: Mapping[str, Any]) -> str | None:
    for field in ("work_id", "openalex_work_id", "paper_id", "canonical_openalex_work_id"):
        canonical = _canonical_from_value(row.get(field))
        if canonical:
            return canonical
    return None


def stable_row_id(*, worksheet_version: str, seed: int, canonical_openalex_work_id: str) -> str:
    raw = f"{worksheet_version}|{seed}|{canonical_openalex_work_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _validate_discovery(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-second-surface")
    if metadata.get("artifact_type") != DISCOVERY_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("discovery metadata.artifact_type mismatch")
    if metadata.get("surface_version") != SECOND_SURFACE_VERSION:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            "surface_version must be ml-shadow-scorer-v1-generalization-second-surface-v1"
        )
    if _get(payload, "discovery_summary.status") != "selected_needs_labels":
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("discovery status must be selected_needs_labels")
    if _get(payload, "discovery_summary.recommended_next_stage") != EXPECTED_NEXT_STAGE:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            f"discovery recommended_next_stage must be {EXPECTED_NEXT_STAGE}"
        )
    if _get(payload, "selected_second_surface.ranking_run_id") != EXPECTED_RANKING_RUN_ID:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            f"selected_second_surface.ranking_run_id must be {EXPECTED_RANKING_RUN_ID}"
        )
    if _get(payload, "selected_second_surface.candidate_pool_work_set_sha256") != EXPECTED_CANDIDATE_SHA:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("selected candidate_pool_work_set_sha256 mismatch")
    if _get(payload, "selected_second_surface.confirmatory_metric_eligible_work_count") != EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            "selected confirmatory_metric_eligible_work_count must be 168"
        )
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    version = payload.get("dataset_version") or _get(payload, "metadata.dataset_version")
    if version != LABEL_DATASET_VERSION:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            f"label dataset version must be {LABEL_DATASET_VERSION}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("fresh surface policy artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            "fresh surface policy_version must be ml-fresh-eval-surface-policy-hybrid-v1"
        )
    return metadata


def _validate_offline_scoring(payload: Mapping[str, Any]) -> None:
    metadata = _metadata(payload, name="offline-production-candidate-scoring-v3")
    if metadata.get("eval_work_set_sha256") != OLD_217_EVAL_SHA:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            f"offline scoring metadata.eval_work_set_sha256 must be {OLD_217_EVAL_SHA}"
        )


def _explicit_label_groups(label_rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in label_rows:
        if not row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()}):
            continue
        canonical = _canonical_work_id_from_label(row)
        if canonical:
            groups.setdefault(canonical, []).append(dict(row))
    return groups


def _label_summary(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "row_id": row.get("row_id"),
                TARGET: row.get(TARGET),
                "relevance_label": row.get("relevance_label"),
                "novelty_label": row.get("novelty_label"),
                "bridge_like_label": row.get("bridge_like_label"),
                "review_pool_variant": row.get("review_pool_variant"),
            }
        )
    return out


def _float_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_float(value: Any) -> str:
    number = _float_value(value)
    if number is None:
        return ""
    return f"{number:.6f}".rstrip("0").rstrip(".")


def _abstract_preview(value: Any, *, max_chars: int = 360) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _topic_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
        return _topic_text(parsed)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        names: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                name = item.get("display_name") or item.get("name") or item.get("topic_name")
            else:
                name = item
            if name:
                names.append(str(name))
        return ";".join(names)
    return str(value)


def _sample_reason_for_index(index: int, total: int) -> str:
    if total <= 0:
        return "second_surface_score_spread"
    top_boundary = int(total * 0.40)
    middle_boundary = int(total * 0.65)
    if index >= middle_boundary:
        return "second_surface_low_score_negative_candidate"
    if index >= top_boundary:
        return "second_surface_score_boundary"
    stride = max(1, total // 20)
    if index % stride == 0:
        return "second_surface_score_spread"
    return "second_surface_high_score_candidate"


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[float, int, str]:
    score = _float_value(row.get("final_score"))
    rank = row.get("heuristic_rank")
    try:
        rank_int = int(rank)
    except (TypeError, ValueError):
        rank_int = 10**9
    return (-(score if score is not None else -1e18), rank_int, str(row.get("canonical_openalex_work_id") or ""))


def _query_candidate_rows(conn: Any, *, ranking_run_id: str, family: str) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(
            cur,
            """
            SELECT
                ps.ranking_run_id,
                ps.work_id AS internal_work_id,
                ps.recommendation_family,
                ps.final_score,
                w.openalex_id,
                w.title,
                w.year,
                w.citation_count,
                w.source_slug,
                w.abstract,
                w.corpus_snapshot_version,
                NULL::json AS topics
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            WHERE ps.ranking_run_id = %s
              AND ps.recommendation_family = %s
            ORDER BY ps.final_score DESC NULLS LAST, ps.work_id ASC
            """,
            (ranking_run_id, family),
        )
        raw_rows = [dict(row) for row in cur.fetchall()]
    topic_map = _query_topics_by_work(conn, work_ids=[row.get("internal_work_id") for row in raw_rows])
    out: list[dict[str, Any]] = []
    for index, row in enumerate(raw_rows, start=1):
        canonical = _canonical_from_value(row.get("openalex_id"))
        if not canonical:
            continue
        out.append(
            {
                "ranking_run_id": ranking_run_id,
                "family": row.get("recommendation_family") or family,
                "heuristic_rank": index,
                "internal_work_id": row.get("internal_work_id"),
                "canonical_openalex_work_id": canonical,
                "openalex_id": row.get("openalex_id") or canonical,
                "title": row.get("title"),
                "year": row.get("year"),
                "citation_count": row.get("citation_count"),
                "source_slug": row.get("source_slug"),
                "topics": topic_map.get(int(row["internal_work_id"])) if row.get("internal_work_id") is not None else row.get("topics"),
                "abstract": row.get("abstract"),
                "corpus_snapshot_version": row.get("corpus_snapshot_version"),
                "final_score": _float_value(row.get("final_score")),
            }
        )
    return out


def _query_topics_by_work(conn: Any, *, work_ids: Sequence[Any]) -> dict[int, list[str]]:
    ids = [int(work_id) for work_id in work_ids if work_id is not None]
    if not ids:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(
            cur,
            """
            SELECT sub.work_id, json_agg(sub.topic_name ORDER BY sub.max_score DESC NULLS LAST, sub.topic_name ASC) AS topics
            FROM (
                SELECT wt.work_id, t.name AS topic_name, MAX(wt.score) AS max_score
                FROM work_topics wt
                JOIN topics t ON t.id = wt.topic_id
                WHERE wt.work_id = ANY(%s)
                GROUP BY wt.work_id, t.name
            ) sub
            GROUP BY sub.work_id
            """,
            (ids,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    out: dict[int, list[str]] = {}
    for row in rows:
        work_id = row.get("work_id")
        topics = row.get("topics")
        if work_id is None:
            continue
        if isinstance(topics, str):
            try:
                parsed = json.loads(topics)
            except json.JSONDecodeError:
                parsed = [topics]
            topics = parsed
        if isinstance(topics, Sequence) and not isinstance(topics, (str, bytes, bytearray)):
            out[int(work_id)] = [str(topic) for topic in topics if topic]
    return out


def _worksheet_row(
    row: Mapping[str, Any],
    *,
    row_id: str,
    worksheet_version: str,
    review_pool_variant: str,
    sample_reason: str,
) -> dict[str, str]:
    canonical = str(row["canonical_openalex_work_id"])
    openalex_id = str(row.get("openalex_id") or f"https://openalex.org/{canonical}")
    return {
        "row_id": row_id,
        "worksheet_version": worksheet_version,
        "review_pool_variant": review_pool_variant,
        "paper_id": openalex_id,
        "openalex_work_id": canonical,
        "work_id": canonical,
        "title": str(row.get("title") or ""),
        "year": "" if row.get("year") is None else str(row.get("year")),
        "citation_count": "" if row.get("citation_count") is None else str(row.get("citation_count")),
        "source_slug": str(row.get("source_slug") or ""),
        "topics": _topic_text(row.get("topics")),
        "abstract_preview": _abstract_preview(row.get("abstract")),
        "sample_reason": sample_reason,
        "ranking_run_id": str(row.get("ranking_run_id") or ""),
        "family": str(row.get("family") or ""),
        "final_score": _format_float(row.get("final_score")),
        "rank_in_family": "" if row.get("heuristic_rank") is None else str(row.get("heuristic_rank")),
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def render_worksheet_csv(rows: Sequence[Mapping[str, str]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(CSV_COLUMNS), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})
    return out.getvalue()


def _threshold_gaps(discovery_payload: Mapping[str, Any], policy_payload: Mapping[str, Any] | None) -> dict[str, Any]:
    thresholds = discovery_payload.get("threshold_check")
    if not isinstance(thresholds, Mapping):
        thresholds = {}
    out: dict[str, Any] = {}
    for key, item in thresholds.items():
        if not isinstance(item, Mapping):
            continue
        observed = item.get("observed")
        threshold = item.get("threshold")
        deficit = 0
        if isinstance(observed, (int, float)) and isinstance(threshold, (int, float)):
            deficit = max(0, threshold - observed)
        out[str(key)] = {
            "observed": observed,
            "threshold": threshold,
            "passed": item.get("passed"),
            "deficit": deficit,
        }
    policy_thresholds = _get(policy_payload or {}, "label_policy.minimum_confirmatory_label_thresholds")
    if isinstance(policy_thresholds, Mapping):
        out["policy_minimum_confirmatory_label_thresholds"] = dict(policy_thresholds)
    return out


def _connect_readonly(database_url: str) -> Any:
    return psycopg.connect(database_url, autocommit=True, options="-c default_transaction_read_only=on")


def build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
    conn: Any,
    *,
    generalization_second_surface_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    offline_production_candidate_scoring_v3_path: Path,
    first_validated_surface_path: Path,
    fresh_surface_policy_path: Path | None = None,
    requested_rows: int = DEFAULT_REQUESTED_ROWS,
    worksheet_version: str = WORKSHEET_VERSION,
    review_pool_variant: str = DEFAULT_REVIEW_POOL_VARIANT,
    seed: int = DEFAULT_SEED,
    database_url: str | None = None,
    repo_root: Path | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any], str]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    discovery_path = Path(generalization_second_surface_path).resolve()
    labels_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()
    scoring_path = Path(offline_production_candidate_scoring_v3_path).resolve()
    first_surface_path = Path(first_validated_surface_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve() if fresh_surface_policy_path else None

    if int(requested_rows) < 0:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("--requested-rows must be non-negative")
    if not str(review_pool_variant or "").strip():
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("review_pool_variant must not be blank")
    if not conflict_path.exists():
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(f"conflict policy does not exist: {conflict_path}")
    if database_url:
        _assert_local_database_url(database_url)

    discovery_payload = _load_json_object(discovery_path)
    label_payload = _load_json_object(labels_path)
    scoring_payload = _load_json_object(scoring_path)
    first_surface_payload = _load_json_object(first_surface_path)
    policy_payload = _load_json_object(policy_path) if policy_path else None
    _validate_discovery(discovery_payload)
    label_rows = _validate_label_dataset(label_payload)
    _validate_offline_scoring(scoring_payload)
    if policy_payload is not None:
        _validate_policy(policy_payload)

    selected = discovery_payload["selected_second_surface"]
    ranking_run_id = str(selected["ranking_run_id"])
    family = str(selected["family"])
    old_217_ids = _old_eval_ids_from_v3(scoring_payload)
    first_surface_ids = _first_surface_ids(first_surface_payload)
    explicit_labels = _explicit_label_groups(label_rows)

    all_rows = _query_candidate_rows(conn, ranking_run_id=ranking_run_id, family=family)
    if len({row["canonical_openalex_work_id"] for row in all_rows}) != int(selected["candidate_pool_work_count"]):
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError("DB candidate pool count does not match discovery")

    row_ids = {str(row["canonical_openalex_work_id"]) for row in all_rows}
    old_overlap_ids = row_ids.intersection(old_217_ids)
    first_overlap_ids = row_ids.intersection(first_surface_ids)
    prior_overlap_union = old_overlap_ids.union(first_overlap_ids)
    eligible: list[dict[str, Any]] = []
    excluded_existing_label_ids: set[str] = set()
    for row in all_rows:
        canonical = str(row["canonical_openalex_work_id"])
        if canonical in prior_overlap_union:
            continue
        if canonical in explicit_labels:
            excluded_existing_label_ids.add(canonical)
            continue
        eligible.append(row)

    expected_eligible = int(selected["confirmatory_metric_eligible_work_count"])
    if len(eligible) != expected_eligible and not excluded_existing_label_ids:
        raise MLShadowScorerSecondSurfaceLabelingWorksheetError(
            f"eligible worksheet row count mismatch: expected {expected_eligible}, found {len(eligible)}"
        )
    selected_rows = sorted(eligible, key=_candidate_sort_key)
    requested = int(requested_rows)
    if requested > 0 and requested < len(selected_rows):
        selected_rows = selected_rows[:requested]

    worksheet_rows: list[dict[str, str]] = []
    sidecar_rows: list[dict[str, Any]] = []
    for index, row in enumerate(selected_rows):
        canonical = str(row["canonical_openalex_work_id"])
        row_id = stable_row_id(
            worksheet_version=worksheet_version,
            seed=int(seed),
            canonical_openalex_work_id=canonical,
        )
        sample_reason = _sample_reason_for_index(index, len(selected_rows))
        csv_row = _worksheet_row(
            row,
            row_id=row_id,
            worksheet_version=worksheet_version,
            review_pool_variant=str(review_pool_variant),
            sample_reason=sample_reason,
        )
        worksheet_rows.append(csv_row)
        sidecar_rows.append(
            {
                "row_id": row_id,
                "canonical_openalex_work_id": canonical,
                "internal_work_id": row.get("internal_work_id"),
                "paper_id": csv_row["paper_id"],
                "ranking_run_id": ranking_run_id,
                "family": family,
                "rank_in_family": csv_row["rank_in_family"],
                "heuristic_rank": row.get("heuristic_rank"),
                "final_score": row.get("final_score"),
                "title": csv_row["title"],
                "year": csv_row["year"],
                "citation_count": csv_row["citation_count"],
                "source_slug": csv_row["source_slug"],
                "topics": csv_row["topics"],
                "abstract_preview": csv_row["abstract_preview"],
                "sample_reason": sample_reason,
                "old_217_overlap": False,
                "first_validated_surface_overlap": False,
                "existing_v10_labels": _label_summary(explicit_labels.get(canonical, [])),
                "split_intent": "future_confirmatory_eval_only",
            }
        )

    inputs = [
        _input_record("generalization_second_surface", discovery_path, repo_root=root),
        _input_record("label_dataset", labels_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
        _input_record("offline_production_candidate_scoring_v3", scoring_path, repo_root=root),
        _input_record("first_validated_surface", first_surface_path, repo_root=root),
    ]
    if policy_path is not None:
        inputs.append(_input_record("fresh_surface_policy", policy_path, repo_root=root))

    reason_counts = dict(Counter(row["sample_reason"] for row in worksheet_rows))
    context_payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "worksheet_version": worksheet_version,
            "review_pool_variant": str(review_pool_variant),
            "seed": int(seed),
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": (
                _assert_local_database_url(database_url).get("database_target_redacted")
                if database_url
                else None
            ),
            "database_access": "read_only_select",
            "label_sets": ALLOWED_LABEL_SETS,
            "caveats": list(CAVEATS),
        },
        "discovery_provenance": {
            "ranking_run_id": ranking_run_id,
            "family": family,
            "corpus_snapshot_version": selected.get("corpus_snapshot_version"),
            "embedding_version": selected.get("embedding_version"),
            "candidate_pool_work_count": selected.get("candidate_pool_work_count"),
            "candidate_pool_work_set_sha256": selected.get("candidate_pool_work_set_sha256"),
            "confirmatory_metric_eligible_work_count": selected.get("confirmatory_metric_eligible_work_count"),
            "old_217_overlap_count": _get(discovery_payload, "overlap_report.old_217_overlap_count"),
            "rank_9f4b2a2084_overlap_count": _get(discovery_payload, "overlap_report.rank_9f4b2a2084_overlap_count"),
            "combined_prior_surface_overlap_count": _get(
                discovery_payload,
                "overlap_report.combined_prior_surface_overlap_count",
            ),
        },
        "selection_summary": {
            "requested_rows": requested,
            "selected_row_count": len(worksheet_rows),
            "candidate_pool_work_count": len(all_rows),
            "excluded_old_217_count": len(old_overlap_ids),
            "excluded_first_surface_count": len(first_overlap_ids),
            "excluded_prior_overlap_union_count": len(prior_overlap_union),
            "excluded_existing_v10_label_count": len(excluded_existing_label_ids),
            "filter_difference_note": (
                "selected_row_count is below discovery confirmatory count because existing explicit v10 labels were excluded"
                if excluded_existing_label_ids
                else None
            ),
            "sample_reason_breakdown": reason_counts,
            "ordered_by": ["final_score desc", "heuristic_rank asc", "canonical_openalex_work_id asc"],
        },
        "threshold_gaps_before_labeling": _threshold_gaps(discovery_payload, policy_payload),
        "row_id_policy": {
            "formula": "sha256(f\"{worksheet_version}|{seed}|{canonical_openalex_work_id}\")",
            "csv_row_id_set_equals_sidecar_row_id_set": True,
        },
        "rows": sidecar_rows,
        "recommended_next_stage": "manual_label_shadow_generalization_second_surface_worksheet_v1",
        "manual_followup": [
            "Fill relevance_label, novelty_label, bridge_like_label, and reviewer_notes in the CSV.",
            "Future task ingests the dated labeled CSV into ml-label-dataset-v11.",
            "Rerun ml-shadow-scorer-generalization-second-surface pinned to rank-83787b91ef.",
            "If labels pass and learned probability remains missing, create the learned probability coverage plan.",
        ],
        "blocked_actions": [
            "label_ingest",
            "database_writes",
            "ranking",
            "embeddings",
            "learned_probability_generation",
            "scorer_execution",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "runtime_implementation_authorized": False,
        },
        "caveats": list(CAVEATS),
    }
    return worksheet_rows, context_payload, markdown_from_context(context_payload)


def markdown_from_context(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    provenance = payload["discovery_provenance"]
    selection = payload["selection_summary"]
    gaps = payload["threshold_gaps_before_labeling"]
    lines = [
        f"# Shadow Generalization Second Surface Labeling Worksheet ({metadata['worksheet_version']})",
        "",
        "## Executive Summary",
        "",
        "This worksheet contains reviewer-blank rows for all confirmatory-eligible works on the selected second shadow-generalization surface. It is for manual labels only; it does not ingest labels, score, rank, embed, or authorize shadow/production.",
        "",
        f"- Ranking run: `{provenance['ranking_run_id']}`",
        f"- Family: `{provenance['family']}`",
        f"- Snapshot: `{provenance['corpus_snapshot_version']}`",
        f"- Candidate pool: {provenance['candidate_pool_work_count']}",
        f"- Candidate SHA: `{provenance['candidate_pool_work_set_sha256']}`",
        f"- Prior-overlap union excluded: {selection['excluded_prior_overlap_union_count']}",
        f"- Selected worksheet rows: {selection['selected_row_count']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Why This Worksheet Exists",
        "",
        "The second surface has enough confirmatory-eligible works after exclusions, but label coverage is 0/168. Manual labels are needed before discovery can advance toward learned-probability coverage and a later generalization audit.",
        "",
        "## Selection Policy",
        "",
        "- Universe: `paper_scores` joined to `works` for `rank-83787b91ef` / `emerging`.",
        "- Excluded old-217 eval works and first validated surface works.",
        "- Excluded existing explicit v10 labels.",
        "- Ordered label-blind by final_score descending, heuristic rank ascending, canonical OpenAlex ID ascending.",
        "- Review columns are intentionally blank.",
        "",
        "## Sample Reason Breakdown",
        "",
    ]
    for reason, count in sorted(selection["sample_reason_breakdown"].items()):
        lines.append(f"- `{reason}`: {count}")
    lines.extend(
        [
            "",
            "## Thresholds Needed",
            "",
            "| Threshold | Observed | Required | Deficit | Passed |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for key, item in gaps.items():
        if not isinstance(item, Mapping) or key == "policy_minimum_confirmatory_label_thresholds":
            continue
        lines.append(
            f"| `{key}` | {item.get('observed')} | {item.get('threshold')} | {item.get('deficit')} | {item.get('passed')} |"
        )
    lines.extend(
        [
            "",
            "## Rubric Reminder",
            "",
            "- relevance_label: good, acceptable, miss, irrelevant",
            "- novelty_label: surprising, useful, obvious, not_useful, neither",
            "- bridge_like_label: yes, partial, no, not_applicable",
            "- reviewer_notes: required free text in the later labeled CSV",
            "",
            "## Not Ingest / Not Ranking / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    lines.extend(
        [
            "",
            "## Manual Follow-Up",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["manual_followup"])
    lines.append("")
    return "\n".join(lines)


def write_ml_shadow_scorer_second_surface_labeling_worksheet(
    *,
    generalization_second_surface_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    offline_production_candidate_scoring_v3_path: Path,
    first_validated_surface_path: Path,
    output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    fresh_surface_policy_path: Path | None = None,
    database_url: str | None = None,
    requested_rows: int = DEFAULT_REQUESTED_ROWS,
    worksheet_version: str = WORKSHEET_VERSION,
    review_pool_variant: str = DEFAULT_REVIEW_POOL_VARIANT,
    seed: int = DEFAULT_SEED,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    db_url = database_url or _database_url_from_env()
    _assert_local_database_url(db_url)
    with _connect_readonly(db_url) as conn:
        worksheet_rows, context_payload, markdown = build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
            conn,
            generalization_second_surface_path=generalization_second_surface_path,
            label_dataset_path=label_dataset_path,
            conflict_policy_path=conflict_policy_path,
            offline_production_candidate_scoring_v3_path=offline_production_candidate_scoring_v3_path,
            first_validated_surface_path=first_validated_surface_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            requested_rows=requested_rows,
            worksheet_version=worksheet_version,
            review_pool_variant=review_pool_variant,
            seed=seed,
            database_url=db_url,
            repo_root=repo_root,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_worksheet_csv(worksheet_rows), encoding="utf-8", newline="")
    context_output_path.parent.mkdir(parents=True, exist_ok=True)
    context_output_path.write_text(json.dumps(context_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown, encoding="utf-8", newline="\n")
    return context_payload


__all__ = [
    "ARTIFACT_TYPE",
    "WORKSHEET_VERSION",
    "MLShadowScorerSecondSurfaceLabelingWorksheetError",
    "build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads",
    "render_worksheet_csv",
    "stable_row_id",
    "write_ml_shadow_scorer_second_surface_labeling_worksheet",
]
