"""Fresh hybrid positive top-up manual-labeling worksheet generator.

This command creates reviewer-blank CSV rows plus a context sidecar for the
remaining unlabeled fresh hybrid confirmatory works when only the positive
work-count threshold is short. It does not ingest labels, score, rank, train,
embed, validate hybrids, or authorize shadow/prod.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_eval_positive_topup_worksheet_hybrid"
WORKSHEET_VERSION = "ml-fresh-eval-positive-topup-worksheet-hybrid-v1"
DEFAULT_REVIEW_POOL_VARIANT = "ml_fresh_hybrid_positive_topup_v1"
DEFAULT_SEED = 20260519
DEFAULT_REQUESTED_ROWS = 0
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v9"
POSITIVE_THRESHOLD_KEY = "minimum_confirmatory_positive_work_count"
SAMPLE_REASON = "fresh_hybrid_positive_topup"

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

REVIEW_COLUMNS = ("relevance_label", "novelty_label", "bridge_like_label", "reviewer_notes")

ALLOWED_LABEL_SETS = {
    "relevance_label": ["good", "acceptable", "miss", "irrelevant"],
    "novelty_label": ["surprising", "useful", "obvious", "not_useful", "neither"],
    "bridge_like_label": ["yes", "partial", "no", "not_applicable"],
}

CAVEATS = (
    "Manual labeling worksheet only; no labels are ingested by this command.",
    "This is not validation and does not complete confirmatory hybrid validation.",
    "Rows are selected only from unlabeled confirmatory-eligible fresh-surface works.",
    "Ordering is label-blind: final_score descending, heuristic_rank ascending, canonical work id ascending.",
    "No scoring, ranking, training, embeddings, shadow, or production authorization.",
)

BLOCKED_ACTIONS = (
    "label_ingest",
    "hybrid_validation",
    "scoring",
    "ranking",
    "training",
    "embeddings",
    "shadow",
    "production_default",
)

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)


class MLFreshEvalPositiveTopupWorksheetHybridError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshEvalPositiveTopupWorksheetHybridError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshEvalPositiveTopupWorksheetHybridError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshEvalPositiveTopupWorksheetHybridError(f"{name} JSON missing metadata object")
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
        raise MLFreshEvalPositiveTopupWorksheetHybridError(f"Input {name} does not exist: {path}")
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
    for field in ("work_id", "openalex_work_id", "paper_id"):
        canonical = _canonical_from_value(row.get(field))
        if canonical:
            return canonical
    return None


def stable_row_id(*, worksheet_version: str, seed: int, canonical_openalex_work_id: str) -> str:
    raw = f"{worksheet_version}|{seed}|{canonical_openalex_work_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _float_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
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


def _validate_surface(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-surface")
    if metadata.get("artifact_type") != SURFACE_ARTIFACT_TYPE:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("fresh surface metadata.artifact_type mismatch")
    if metadata.get("surface_version") != SURFACE_VERSION:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("surface_version must be ml-fresh-eval-surface-hybrid-v1")
    if metadata.get("label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("fresh surface must use label_dataset_version ml-label-dataset-v9")
    expected_version = metadata.get("expected_label_dataset_version")
    if expected_version is not None and expected_version != LABEL_DATASET_VERSION:
        raise MLFreshEvalPositiveTopupWorksheetHybridError(
            "fresh surface expected_label_dataset_version must be ml-label-dataset-v9"
        )
    if metadata.get("status") != "materialized_needs_labels":
        raise MLFreshEvalPositiveTopupWorksheetHybridError("fresh surface status must be materialized_needs_labels")
    if payload.get("ready_for_hybrid_validation_scoring") is not False:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("ready_for_hybrid_validation_scoring must be false")

    thresholds = payload.get("threshold_check")
    if not isinstance(thresholds, Mapping):
        raise MLFreshEvalPositiveTopupWorksheetHybridError("fresh surface threshold_check missing")
    positive = thresholds.get(POSITIVE_THRESHOLD_KEY)
    if not isinstance(positive, Mapping):
        raise MLFreshEvalPositiveTopupWorksheetHybridError("positive threshold row missing")
    if positive.get("passed") is not False:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("positive threshold must be the remaining failing threshold")
    failing = [
        key
        for key, row in thresholds.items()
        if key != POSITIVE_THRESHOLD_KEY and isinstance(row, Mapping) and row.get("passed") is not True
    ]
    if failing:
        raise MLFreshEvalPositiveTopupWorksheetHybridError(
            f"only positive threshold may fail; also failing: {', '.join(sorted(map(str, failing)))}"
        )

    observed = _float_value(positive.get("observed"))
    threshold = _float_value(positive.get("threshold"))
    if observed is None or threshold is None:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("positive threshold observed/threshold must be numeric")
    deficit = max(0, int(math.ceil(threshold - observed)))
    work_level_positive = _get(payload, "label_coverage.work_level.confirmatory_positive_work_count")
    if _float_value(work_level_positive) != observed:
        raise MLFreshEvalPositiveTopupWorksheetHybridError(
            "label_coverage.work_level.confirmatory_positive_work_count does not match positive threshold observed"
        )
    return {
        "observed": int(observed) if observed.is_integer() else observed,
        "threshold": int(threshold) if threshold.is_integer() else threshold,
        "passed": False,
        "deficit": deficit,
    }


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("label dataset_version must be ml-label-dataset-v9")
    if not isinstance(payload.get("rows"), list):
        raise MLFreshEvalPositiveTopupWorksheetHybridError("label dataset missing rows array")


def _label_rows_by_work(payload: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    _validate_label_dataset(payload)
    out: dict[str, list[dict[str, Any]]] = {}
    rows = payload.get("rows")
    assert isinstance(rows, list)
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        string_row = {str(k): "" if v is None else str(v) for k, v in row.items()}
        if not row_has_explicit_label(string_row):
            continue
        canonical = _canonical_work_id_from_label(row)
        if canonical:
            out.setdefault(canonical, []).append(dict(row))
    return out


def _candidate_rows(surface_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _get(surface_payload, "candidate_pool.candidate_rows")
    if not isinstance(rows, list):
        raise MLFreshEvalPositiveTopupWorksheetHybridError("fresh surface candidate_pool.candidate_rows missing")
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        canonical = _canonical_from_value(row.get("canonical_openalex_work_id") or row.get("openalex_id"))
        if canonical:
            normalized = dict(row)
            normalized["canonical_openalex_work_id"] = canonical
            out.append(normalized)
    return out


def _label_summary(labels: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for row in labels:
        summary.append(
            {
                "row_id": row.get("row_id"),
                "dataset_version": row.get("dataset_version"),
                "review_pool_variant": row.get("review_pool_variant"),
                "relevance_label": row.get("relevance_label"),
                "novelty_label": row.get("novelty_label"),
                "bridge_like_label": row.get("bridge_like_label"),
                "good_or_acceptable": row.get("good_or_acceptable"),
            }
        )
    return summary


def _unlabeled_confirmatory_universe(
    surface_payload: Mapping[str, Any],
    label_groups: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[list[dict[str, Any]], int]:
    candidates = _candidate_rows(surface_payload)
    eligible_by_work: dict[str, dict[str, Any]] = {}
    duplicate_eligible_count = 0
    for row in candidates:
        canonical = str(row["canonical_openalex_work_id"])
        if row.get("confirmatory_metric_eligible") is not True:
            continue
        if row.get("previous_eval_overlap") is True:
            raise MLFreshEvalPositiveTopupWorksheetHybridError(
                f"confirmatory eligible row is marked previous_eval_overlap: {canonical}"
            )
        if canonical in label_groups:
            continue
        if canonical in eligible_by_work:
            duplicate_eligible_count += 1
            continue
        eligible_by_work[canonical] = row
    universe = sorted(eligible_by_work.values(), key=_positive_priority_sort_key)
    if not universe:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("unlabeled confirmatory universe is empty")
    return universe, duplicate_eligible_count


def _positive_priority_sort_key(row: Mapping[str, Any]) -> tuple[float, int, str]:
    score = _float_value(row.get("final_score"))
    rank = _int_value(row.get("heuristic_rank"))
    canonical = str(row.get("canonical_openalex_work_id") or "")
    return (-(score if score is not None else float("-inf")), rank if rank is not None else 10**12, canonical)


def _threshold_table(surface_payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    thresholds = surface_payload.get("threshold_check")
    if not isinstance(thresholds, Mapping):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, row in thresholds.items():
        if not isinstance(row, Mapping):
            continue
        observed = row.get("observed")
        threshold = row.get("threshold")
        deficit: Any = None
        if key == "minimum_confirmatory_label_coverage_rate":
            eligible = _int_value(_get(surface_payload, "confirmatory_eligibility.confirmatory_metric_eligible_work_count")) or 0
            labeled = _int_value(_get(surface_payload, "label_coverage.work_level.confirmatory_labeled_work_count")) or 0
            needed = math.ceil(float(threshold or 0) * eligible)
            deficit = max(0, needed - labeled)
        elif isinstance(observed, (int, float)) and isinstance(threshold, (int, float)):
            deficit = max(0, threshold - observed)
            if isinstance(deficit, float) and deficit.is_integer():
                deficit = int(deficit)
        out[str(key)] = {
            "observed": observed,
            "threshold": threshold,
            "deficit": deficit,
            "passed": bool(row.get("passed")),
        }
    return out


def _source_surface_summary(surface_payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ranking_run_id": _get(surface_payload, "candidate_source.ranking_run_id"),
        "family": _get(surface_payload, "candidate_source.family"),
        "snapshot_version": _get(surface_payload, "candidate_source.corpus_snapshot_version"),
        "candidate_work_count": _get(surface_payload, "candidate_pool.candidate_work_count"),
        "candidate_work_set_sha256": _get(surface_payload, "candidate_pool.candidate_work_set_sha256"),
        "confirmatory_eligible_work_count": _get(
            surface_payload,
            "confirmatory_eligibility.confirmatory_metric_eligible_work_count",
        ),
        "excluded_previous_eval_overlap_count": _get(
            surface_payload,
            "disallowed_overlap_report.excluded_previous_eval_overlap_count",
        ),
        "labeled_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_labeled_work_count"),
        "unlabeled_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_unlabeled_work_count"),
        "positive_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_positive_work_count"),
        "negative_work_count": _get(surface_payload, "label_coverage.work_level.confirmatory_negative_work_count"),
        "distinct_negative_work_count": _get(surface_payload, "label_coverage.work_level.distinct_negative_work_count"),
        "label_coverage_rate": _get(surface_payload, "label_coverage.work_level.label_coverage_rate"),
    }


def _worksheet_row(
    row: Mapping[str, Any],
    *,
    row_id: str,
    worksheet_version: str,
    review_pool_variant: str,
) -> dict[str, str]:
    canonical = str(row["canonical_openalex_work_id"])
    openalex_id = str(row.get("openalex_id") or f"https://openalex.org/{canonical}")
    year = row.get("year")
    citation_count = row.get("citation_count")
    return {
        "row_id": row_id,
        "worksheet_version": worksheet_version,
        "review_pool_variant": review_pool_variant,
        "paper_id": openalex_id,
        "openalex_work_id": canonical,
        "work_id": canonical,
        "title": str(row.get("title") or ""),
        "year": "" if year is None else str(year),
        "citation_count": "" if citation_count is None else str(citation_count),
        "source_slug": str(row.get("source_slug") or ""),
        "topics": str(row.get("topics") or ""),
        "abstract_preview": _abstract_preview(row.get("abstract") or row.get("abstract_preview")),
        "sample_reason": SAMPLE_REASON,
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


def build_ml_fresh_eval_positive_topup_worksheet_hybrid_payloads(
    *,
    fresh_eval_surface_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    requested_rows: int = DEFAULT_REQUESTED_ROWS,
    seed: int = DEFAULT_SEED,
    worksheet_version: str = WORKSHEET_VERSION,
    review_pool_variant: str = DEFAULT_REVIEW_POOL_VARIANT,
    repo_root: Path | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any], str]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    surface_path = Path(fresh_eval_surface_path).resolve()
    labels_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    surface_payload = _load_json_object(surface_path)
    label_payload = _load_json_object(labels_path)
    positive_threshold = _validate_surface(surface_payload)
    label_groups = _label_rows_by_work(label_payload)
    if not conflict_path.exists():
        raise MLFreshEvalPositiveTopupWorksheetHybridError(f"conflict policy does not exist: {conflict_path}")
    if int(requested_rows) < 0:
        raise MLFreshEvalPositiveTopupWorksheetHybridError("--requested-rows must be non-negative")
    if not str(review_pool_variant or "").strip():
        raise MLFreshEvalPositiveTopupWorksheetHybridError("review_pool_variant must not be blank")

    universe, duplicate_eligible_count = _unlabeled_confirmatory_universe(surface_payload, label_groups)
    target = len(universe) if int(requested_rows) == 0 else min(int(requested_rows), len(universe))
    selected = universe[:target]

    worksheet_rows: list[dict[str, str]] = []
    sidecar_rows: list[dict[str, Any]] = []
    for row in selected:
        canonical = str(row["canonical_openalex_work_id"])
        row_id = stable_row_id(
            worksheet_version=worksheet_version,
            seed=int(seed),
            canonical_openalex_work_id=canonical,
        )
        csv_row = _worksheet_row(
            row,
            row_id=row_id,
            worksheet_version=worksheet_version,
            review_pool_variant=str(review_pool_variant),
        )
        worksheet_rows.append(csv_row)
        sidecar_rows.append(
            {
                "row_id": row_id,
                "canonical_openalex_work_id": canonical,
                "paper_id": csv_row["paper_id"],
                "openalex_work_id": csv_row["openalex_work_id"],
                "work_id": csv_row["work_id"],
                "ranking_run_id": csv_row["ranking_run_id"],
                "family": csv_row["family"],
                "rank_in_family": csv_row["rank_in_family"],
                "heuristic_rank": row.get("heuristic_rank"),
                "final_score": row.get("final_score"),
                "semantic_score": row.get("semantic_score"),
                "citation_velocity_score": row.get("citation_velocity_score"),
                "topic_growth_score": row.get("topic_growth_score"),
                "bridge_score": row.get("bridge_score"),
                "bridge_eligible": row.get("bridge_eligible"),
                "confirmatory_metric_eligible": row.get("confirmatory_metric_eligible"),
                "previous_eval_overlap": row.get("previous_eval_overlap"),
                "sample_reason": SAMPLE_REASON,
                "title": csv_row["title"],
                "year": csv_row["year"],
                "citation_count": csv_row["citation_count"],
                "source_slug": csv_row["source_slug"],
                "topics": csv_row["topics"],
                "abstract_preview": csv_row["abstract_preview"],
                "existing_v9_labels_for_same_work": _label_summary(label_groups.get(canonical, [])),
                "candidate_row": dict(row),
                "split_intent": "future_confirmatory_eval_only",
            }
        )

    csv_ids = {row["row_id"] for row in worksheet_rows}
    sidecar_ids = {row["row_id"] for row in sidecar_rows}
    parity = csv_ids == sidecar_ids

    inputs = [
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
        _input_record("label_dataset", labels_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    source_summary = _source_surface_summary(surface_payload)
    threshold_table = _threshold_table(surface_payload)
    context_payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "worksheet_version": worksheet_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "seed": int(seed),
            "requested_rows": int(requested_rows),
            "achieved_rows": len(worksheet_rows),
            "selected_rows": len(worksheet_rows),
            "review_pool_variant": str(review_pool_variant),
            "source_surface_summary": source_summary,
            "positive_threshold_before_labeling": positive_threshold,
            "positive_deficit_before_labeling": positive_threshold["deficit"],
            "unlabeled_confirmatory_universe_size": len(universe),
            "duplicate_unlabeled_confirmatory_candidates_ignored": duplicate_eligible_count,
            "shortfall_count": max(0, target - len(worksheet_rows)),
            "threshold_check_before_labeling": threshold_table,
            "label_sets": ALLOWED_LABEL_SETS,
            "caveats": list(CAVEATS),
        },
        "selection_policy": {
            "universe": "confirmatory_metric_eligible rows from fresh surface with no explicit ml-label-dataset-v9 label",
            "exclude_previous_eval_overlap": True,
            "exclude_existing_v9_labeled_works": True,
            "include_all_remaining_by_default": True,
            "requested_rows_zero_means_all": True,
            "ordering": [
                "final_score descending",
                "heuristic_rank ascending",
                "canonical_openalex_work_id ascending",
            ],
            "sample_reason": SAMPLE_REASON,
            "label_blind_priority": True,
        },
        "row_id_policy": {
            "formula": "sha256(f\"{worksheet_version}|{seed}|{canonical_openalex_work_id}\")",
            "uses_topup_worksheet_version": True,
            "csv_row_id_set_equals_sidecar_row_id_set": parity,
        },
        "rows": sidecar_rows,
        "blocked_actions": list(BLOCKED_ACTIONS),
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }
    markdown = markdown_from_context(context_payload)
    return worksheet_rows, context_payload, markdown


def markdown_from_context(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    source = metadata["source_surface_summary"]
    threshold_rows = metadata["threshold_check_before_labeling"]
    positive = metadata["positive_threshold_before_labeling"]
    lines = [
        f"# Fresh Hybrid Positive Top-Up Worksheet ({metadata['worksheet_version']})",
        "",
        "## Executive Summary",
        "",
        "This reviewer-blank worksheet targets the remaining positive-label shortfall on the fresh hybrid eval surface. It does not ingest labels, run validation, or authorize shadow/production use.",
        "",
        f"- Ranking run: `{source['ranking_run_id']}`",
        f"- Family: `{source['family']}`",
        f"- Snapshot: `{source['snapshot_version']}`",
        f"- Confirmatory eligible works: {source['confirmatory_eligible_work_count']}",
        f"- Labeled / unlabeled works before top-up: {source['labeled_work_count']} / {source['unlabeled_work_count']}",
        f"- Positive / negative / distinct negative works before top-up: {source['positive_work_count']} / {source['negative_work_count']} / {source['distinct_negative_work_count']}",
        f"- Positive work threshold: {positive['observed']} / {positive['threshold']} (deficit {positive['deficit']})",
        f"- Requested / generated worksheet rows: {metadata['requested_rows']} / {metadata['achieved_rows']}",
        "",
        "## Why This Worksheet Exists",
        "",
        f"The only positive threshold short by policy is `{POSITIVE_THRESHOLD_KEY}`.",
        f"All policy thresholds except `{POSITIVE_THRESHOLD_KEY}` pass. The current surface needs at least {positive['deficit']} additional `good` or `acceptable` works to reach the 50 work-level positive threshold.",
        "",
        "## Thresholds Before Top-Up",
        "",
        "| Threshold | Observed | Required | Deficit | Passed |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for key, row in threshold_rows.items():
        lines.append(
            f"| `{key}` | {row.get('observed')} | {row.get('threshold')} | {row.get('deficit')} | {row.get('passed')} |"
        )
    lines.extend(
        [
            "",
            "## Rows And Ordering",
            "",
            f"- Unlabeled confirmatory-eligible universe: {metadata['unlabeled_confirmatory_universe_size']}",
            f"- Rows generated: {metadata['achieved_rows']}",
            "- Selection excludes old-surface overlaps and works already explicitly labeled in `ml-label-dataset-v9`.",
            "- Ordering is label-blind: `final_score` descending, then `heuristic_rank` ascending, then canonical work id ascending.",
            "- Every row uses `sample_reason = fresh_hybrid_positive_topup`.",
            "",
            "## Labeling Instructions",
            "",
            f"To reach the policy floor, the future ingest must add at least {positive['deficit']} new `good` or `acceptable` works among these rows. Filling all rows is useful, but readiness depends on work-level positives, not merely the number of completed labels.",
            "",
            "Allowed label values:",
            "",
            "- relevance_label: good, acceptable, miss, irrelevant",
            "- novelty_label: surprising, useful, obvious, not_useful, neither",
            "- bridge_like_label: yes, partial, no, not_applicable",
            "",
            "## Worksheet Only / Not Validation / No Shadow-Prod",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            "Save a dated labeled CSV, then use a future explicit v10 or dedicated top-up label ingest. Rematerialize the fresh surface after ingest; run hybrid validation only if `ready_for_hybrid_validation_scoring` becomes true.",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_fresh_eval_positive_topup_worksheet_hybrid(
    *,
    fresh_eval_surface_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    requested_rows: int = DEFAULT_REQUESTED_ROWS,
    seed: int = DEFAULT_SEED,
    worksheet_version: str = WORKSHEET_VERSION,
    review_pool_variant: str = DEFAULT_REVIEW_POOL_VARIANT,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    worksheet_rows, context_payload, markdown = build_ml_fresh_eval_positive_topup_worksheet_hybrid_payloads(
        fresh_eval_surface_path=fresh_eval_surface_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        requested_rows=requested_rows,
        seed=seed,
        worksheet_version=worksheet_version,
        review_pool_variant=review_pool_variant,
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
    "MLFreshEvalPositiveTopupWorksheetHybridError",
    "build_ml_fresh_eval_positive_topup_worksheet_hybrid_payloads",
    "render_worksheet_csv",
    "stable_row_id",
    "write_ml_fresh_eval_positive_topup_worksheet_hybrid",
]
