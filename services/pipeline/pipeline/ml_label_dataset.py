"""Build a versioned manual-label dataset JSON (+ Markdown) from audit review CSVs.

Read-only: no database, no ranking. Intended for offline experiment scaffolding only.
"""

from __future__ import annotations

import csv
import copy
import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DATASET_VERSION = "ml-label-dataset-v1"
DEFAULT_DATASET_VERSION = DATASET_VERSION
LABEL_FIELDS = ("relevance_label", "novelty_label", "bridge_like_label")

VERBATIM_CAVEATS = (
    "This is not validation.",
    "Blind snapshot labels reduce but do not eliminate selection bias.",
    "All rows remain audit_only.",
    "No production ranking change is supported.",
)

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

DERIVED_TARGET_FIELDS = ("good_or_acceptable", "surprising_or_useful", "bridge_like_yes_or_partial")

BLIND_REVIEW_POOL_VARIANT = "ml_blind_snapshot_audit"
BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION = "ml-blind-snapshot-review-v2"
BLIND_SNAPSHOT_REVIEW_V2_EXPECTED_ROWS = 60
BLIND_SNAPSHOT_REVIEW_V2_REVIEW_COLUMNS = {
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
}
HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION = "ml-hard-negative-review-v1"
HARD_NEGATIVE_REVIEW_V1_EXPECTED_ROWS = 7
HARD_NEGATIVE_REVIEW_POOL_VARIANT = "ml_hard_negative_audit"
EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION = "ml-external-near-miss-review-v1"
EXTERNAL_NEAR_MISS_REVIEW_V1_EXPECTED_ROWS = 60
EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT = "ml_external_near_miss_audit"
TRANSFER_GAP_REVIEW_V1_WORKSHEET_VERSION = "ml-transfer-gap-review-v1"
TRANSFER_GAP_REVIEW_POOL_VARIANT = "ml_transfer_gap_audit"
TRANSFER_GAP_REVIEW_V1_CONTEXT_ARTIFACT_TYPE = "ml_transfer_gap_review_v1_context"
FRESH_HYBRID_WORKSHEET_VERSION = "ml-fresh-eval-labeling-worksheet-hybrid-v1"
FRESH_HYBRID_REVIEW_POOL_VARIANT = "ml_fresh_hybrid_eval_v1"
FRESH_HYBRID_CONTEXT_ARTIFACT_TYPE = "ml_fresh_eval_labeling_worksheet_hybrid"
FRESH_HYBRID_SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
ALLOWED_RELEVANCE_LABELS = {"good", "acceptable", "miss", "irrelevant"}
ALLOWED_NOVELTY_LABELS = {"surprising", "useful", "obvious", "not_useful", "neither"}
ALLOWED_BRIDGE_LIKE_LABELS = {"yes", "partial", "no", "not_applicable"}
# Worksheet/context fields preserved for blind-snapshot rows so future diagnostics can read
# sample provenance and ranking-context family scores/ranks without inferring labels.
BLIND_CONTEXT_FIELDS = (
    "worksheet_version",
    "sample_seed",
    "sample_reason",
    "cluster_id",
    "topics",
    "abstract_preview",
    "ranking_context_family_scores_json",
    "ranking_context_family_ranks_json",
    "openalex_work_id",
    "internal_work_id",
)


class MLLabelDatasetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _norm_ws(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _emptyish(value: str | None) -> bool:
    return _norm_ws(value) == ""


def _norm_label_token(value: str | None) -> str:
    return _norm_ws(value).lower()


def paper_id_to_work_id(paper_id: str | None) -> str | None:
    if not paper_id:
        return None
    m = _WORK_ID_RE.search(str(paper_id).strip())
    if not m:
        return None
    return m.group(1)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _fieldnames_set(fieldnames: Iterable[str] | None) -> set[str]:
    if not fieldnames:
        return set()
    return {str(n).strip() for n in fieldnames if n and str(n).strip()}


def worksheet_infer_bridge_family_from_context(rel_path: str, fieldnames: Iterable[str] | None) -> bool:
    """True when worksheet is a known bridge delta/objective review CSV without a family column."""
    names = _fieldnames_set(fieldnames)
    if "family" in names:
        return False
    base = Path(rel_path).name.lower()
    if base.startswith("bridge_weight_experiment_") and "delta_review" in base:
        return True
    if base.startswith("bridge_objective_") and ("delta" in base or "one_row_review" in base):
        return True
    return False


def worksheet_has_label_schema(fieldnames: Iterable[str] | None) -> bool:
    if not fieldnames:
        return False
    names = {n.strip() for n in fieldnames if n}
    if "paper_id" not in names:
        return False
    return all(k in names for k in LABEL_FIELDS)


def row_has_explicit_label(row: dict[str, str]) -> bool:
    return any(not _emptyish(row.get(k)) for k in LABEL_FIELDS)


def good_or_acceptable(relevance_label: str | None) -> bool | None:
    t = _norm_label_token(relevance_label)
    if t == "":
        return None
    if t in {"good", "acceptable"}:
        return True
    if t in {"miss", "irrelevant"}:
        return False
    return None


def surprising_or_useful(novelty_label: str | None) -> bool | None:
    t = _norm_label_token(novelty_label)
    if t == "":
        return None
    if t in {"surprising", "useful"}:
        return True
    if t in {"obvious", "not_useful", "neither"}:
        return False
    return None


def bridge_like_yes_or_partial(bridge_like_label: str | None) -> bool | None:
    t = _norm_label_token(bridge_like_label)
    if t == "":
        return None
    if t in {"yes", "partial"}:
        return True
    if t == "no":
        return False
    if t in {"not_applicable", "not applicable"}:
        return None
    return None


def label_completeness_count(row: dict[str, str]) -> int:
    return sum(1 for k in LABEL_FIELDS if not _emptyish(row.get(k)))


def stable_row_id(
    *,
    source_rel: str,
    source_row_number: int,
    paper_id: str,
    ranking_run_id: str | None,
    rank_key: str | None,
    experiment_rank: str | None,
) -> str:
    payload = "\t".join(
        [
            source_rel,
            str(source_row_number),
            paper_id,
            ranking_run_id or "",
            rank_key or "",
            experiment_rank or "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_ranking_run_id(row: dict[str, str]) -> str | None:
    exp = _norm_ws(row.get("experiment_ranking_run_id"))
    ctx = _norm_ws(row.get("ranking_run_id_context"))
    base = _norm_ws(row.get("ranking_run_id"))
    if exp:
        return exp
    if ctx:
        return ctx
    if base:
        return base
    return None


def _resolve_rank_fields(row: dict[str, str]) -> tuple[str | None, str | None]:
    r = _norm_ws(row.get("rank"))
    if not r:
        r = _norm_ws(row.get("family_rank"))
    er = _norm_ws(row.get("experiment_rank"))
    rank_out = r or None
    exp_rank_out = er or None
    return rank_out, exp_rank_out


def _malformed_reason(row: dict[str, str]) -> str | None:
    pid = _norm_ws(row.get("paper_id"))
    if not pid:
        return "missing_paper_id"
    return None


@dataclass
class ParsedWorksheet:
    rel_path: str
    abs_path: Path
    sha256: str
    data_row_count: int
    skipped_blank_rows: int
    included_rows: list[dict[str, Any]]
    skipped_malformed: list[dict[str, Any]]


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        text = f.read()
    lines = text.splitlines()
    if not lines:
        return [], []
    reader = csv.DictReader(lines)
    fieldnames = reader.fieldnames or []
    rows: list[dict[str, str]] = []
    for r in reader:
        rows.append({k: (v if v is not None else "") for k, v in r.items()})
    return list(fieldnames), rows


def parse_manual_review_worksheet(
    csv_path: Path,
    *,
    repo_root: Path,
    dataset_version: str = DEFAULT_DATASET_VERSION,
) -> ParsedWorksheet | None:
    rel = csv_path.resolve().relative_to(repo_root.resolve()).as_posix()
    digest = sha256_file(csv_path)
    fieldnames, raw_rows = _read_csv_rows(csv_path)
    if not worksheet_has_label_schema(fieldnames):
        return None
    infer_bridge_family = worksheet_infer_bridge_family_from_context(rel, fieldnames)

    skipped_blank = 0
    included: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    # source_row_number: 1-based line index (header line = 1 in CSV convention for spreadsheets)
    for i, row in enumerate(raw_rows, start=2):
        if not row_has_explicit_label(row):
            skipped_blank += 1
            continue
        reason = _malformed_reason(row)
        if reason:
            malformed.append({"source_row_number": i, "reason": reason, "row": row})
            continue
        ranking_run_id = _resolve_ranking_run_id(row)
        rank_val, experiment_rank = _resolve_rank_fields(row)
        paper_id = _norm_ws(row.get("paper_id"))
        work_col = _norm_ws(row.get("work_id"))
        work_id = work_col or paper_id_to_work_id(paper_id)
        row_id = stable_row_id(
            source_rel=rel,
            source_row_number=i,
            paper_id=paper_id,
            ranking_run_id=ranking_run_id,
            rank_key=rank_val,
            experiment_rank=experiment_rank,
        )
        rel_l = _norm_ws(row.get("relevance_label")) or None
        nov_l = _norm_ws(row.get("novelty_label")) or None
        br_l = _norm_ws(row.get("bridge_like_label")) or None
        notes = _norm_ws(row.get("reviewer_notes")) or None
        names = _fieldnames_set(fieldnames)
        has_family_col = "family" in names
        raw_family = _norm_ws(row.get("family")) if has_family_col else ""
        family_inferred = False
        if raw_family:
            family: str | None = raw_family
        elif infer_bridge_family:
            family = "bridge"
            family_inferred = True
        else:
            family = None
        out: dict[str, Any] = {
            "dataset_version": dataset_version,
            "row_id": row_id,
            "paper_id": paper_id,
            "work_id": work_id,
            "title": _norm_ws(row.get("title")) or None,
            "ranking_run_id": ranking_run_id,
            "ranking_version": _norm_ws(row.get("ranking_version")) or None,
            "corpus_snapshot_version": _norm_ws(row.get("corpus_snapshot_version")) or None,
            "family": family,
            "review_pool_variant": _norm_ws(row.get("review_pool_variant")) or None,
            "rank": rank_val,
            "experiment_rank": experiment_rank,
            "source_worksheet_path": rel,
            "source_worksheet_sha256": digest,
            "source_row_number": i,
            "relevance_label": rel_l,
            "novelty_label": nov_l,
            "bridge_like_label": br_l,
            "reviewer_notes": notes,
            "label_provenance": "manual_review_worksheet_csv",
            "split": "audit_only",
            "good_or_acceptable": good_or_acceptable(rel_l),
            "surprising_or_useful": surprising_or_useful(nov_l),
            "bridge_like_yes_or_partial": bridge_like_yes_or_partial(br_l),
        }
        if family_inferred:
            out["family_inferred"] = True
        if _norm_ws(row.get("review_pool_variant")) == BLIND_REVIEW_POOL_VARIANT:
            for ctx_field in BLIND_CONTEXT_FIELDS:
                if ctx_field in names:
                    out[ctx_field] = _norm_ws(row.get(ctx_field)) or None
        included.append(out)
    return ParsedWorksheet(rel, csv_path, digest, len(raw_rows), skipped_blank, included, malformed)


def discover_manual_review_csvs(manual_review_dir: Path) -> list[Path]:
    if not manual_review_dir.is_dir():
        return []
    paths = sorted(manual_review_dir.glob("*.csv"))
    return [p for p in paths if p.is_file()]


def _repo_relative(path: Path, *, repo_root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = path.resolve()
    if not resolved.exists():
        raise MLLabelDatasetError(f"required input not found: {path}")
    return {
        "name": name,
        "path": _repo_relative(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_blind_snapshot_v2_row_id(
    *,
    worksheet_version: str,
    sample_seed: int | str,
    paper_id: str,
) -> str:
    payload = f"{worksheet_version}|{sample_seed}|{paper_id}"
    return _sha256_text(payload)


def stable_hard_negative_v1_row_id(
    *,
    worksheet_version: str,
    sample_seed: int | str,
    paper_id: str,
) -> str:
    payload = f"{worksheet_version}|{sample_seed}|{paper_id}"
    return _sha256_text(payload)


def stable_external_near_miss_v1_row_id(
    *,
    worksheet_version: str,
    sample_seed: int | str,
    paper_id: str,
) -> str:
    payload = f"{worksheet_version}|{sample_seed}|{paper_id}"
    return _sha256_text(payload)


def _raw_csv_or_none(row: dict[str, str], field: str) -> str | None:
    value = row.get(field)
    if value is None or value == "":
        return None
    return value


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise MLLabelDatasetError(f"Failed to load JSON {path}: {e}") from e
    if not isinstance(raw, dict):
        raise MLLabelDatasetError(f"Expected JSON object in {path}")
    return raw


def _read_v2_sidecar_rows(context_sidecar_path: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    payload = _load_json_object(context_sidecar_path)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLabelDatasetError(f"{context_sidecar_path} missing rows array")
    by_id: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise MLLabelDatasetError(f"{context_sidecar_path} sidecar row {idx} is not an object")
        row_id = _norm_ws(row.get("row_id"))
        if not row_id:
            raise MLLabelDatasetError(f"{context_sidecar_path} sidecar row {idx} has blank row_id")
        if row_id in by_id:
            raise MLLabelDatasetError(f"{context_sidecar_path} has duplicate row_id {row_id}")
        by_id[row_id] = row
    return payload, by_id


def _index_transfer_gap_sidecar_rows(
    *,
    context_sidecar_path: Path,
    rows_value: Any,
    top_level: bool = False,
) -> dict[str, dict[str, Any]]:
    if isinstance(rows_value, list):
        by_id: dict[str, dict[str, Any]] = {}
        for idx, row in enumerate(rows_value, start=1):
            if not isinstance(row, dict):
                raise MLLabelDatasetError(f"{context_sidecar_path} sidecar row {idx} is not an object")
            row_id = _norm_ws(row.get("row_id"))
            if not row_id:
                raise MLLabelDatasetError(f"{context_sidecar_path} sidecar row {idx} has blank row_id")
            if row_id in by_id:
                raise MLLabelDatasetError(f"{context_sidecar_path} has duplicate row_id {row_id}")
            by_id[row_id] = row
        return by_id

    if isinstance(rows_value, dict):
        by_id = {}
        skipped_keys = {"artifact_type", "generated_at", "provenance", "schema", "metadata", "caveats"}
        items = rows_value.items()
        if top_level:
            items = ((k, v) for k, v in rows_value.items() if k not in skipped_keys)
        for key, row in items:
            if not isinstance(row, dict):
                if top_level:
                    continue
                raise MLLabelDatasetError(f"{context_sidecar_path} sidecar map value for {key!r} is not an object")
            key_id = _norm_ws(key)
            row_id = _norm_ws(row.get("row_id")) or key_id
            if not row_id:
                raise MLLabelDatasetError(f"{context_sidecar_path} sidecar map entry {key!r} has blank row_id")
            if key_id and key_id != row_id:
                raise MLLabelDatasetError(
                    f"{context_sidecar_path} sidecar map key {key_id!r} does not match row_id {row_id!r}"
                )
            if row_id in by_id:
                raise MLLabelDatasetError(f"{context_sidecar_path} has duplicate row_id {row_id}")
            by_id[row_id] = row
        if by_id:
            return by_id

    raise MLLabelDatasetError(f"{context_sidecar_path} missing transfer-gap sidecar rows")


def _read_transfer_gap_sidecar_rows(context_sidecar_path: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    payload = _load_json_object(context_sidecar_path)
    artifact_type = _norm_ws(payload.get("artifact_type"))
    if artifact_type != TRANSFER_GAP_REVIEW_V1_CONTEXT_ARTIFACT_TYPE:
        raise MLLabelDatasetError(
            f"{context_sidecar_path} artifact_type={artifact_type!r} does not match "
            f"{TRANSFER_GAP_REVIEW_V1_CONTEXT_ARTIFACT_TYPE!r}"
        )
    if "rows" in payload:
        by_id = _index_transfer_gap_sidecar_rows(context_sidecar_path=context_sidecar_path, rows_value=payload["rows"])
    else:
        by_id = _index_transfer_gap_sidecar_rows(
            context_sidecar_path=context_sidecar_path,
            rows_value=payload,
            top_level=True,
        )
    return payload, by_id


def _read_fresh_hybrid_sidecar_rows(context_sidecar_path: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    payload = _load_json_object(context_sidecar_path)
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise MLLabelDatasetError(f"{context_sidecar_path} missing metadata object")
    artifact_type = _norm_ws(metadata.get("artifact_type"))
    if artifact_type != FRESH_HYBRID_CONTEXT_ARTIFACT_TYPE:
        raise MLLabelDatasetError(
            f"{context_sidecar_path} metadata.artifact_type={artifact_type!r} does not match "
            f"{FRESH_HYBRID_CONTEXT_ARTIFACT_TYPE!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLabelDatasetError(f"{context_sidecar_path} missing rows array")
    by_id: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise MLLabelDatasetError(f"{context_sidecar_path} fresh-hybrid sidecar row {idx} is not an object")
        row_id = _norm_ws(row.get("row_id"))
        if not row_id:
            raise MLLabelDatasetError(f"{context_sidecar_path} fresh-hybrid sidecar row {idx} has blank row_id")
        if row_id in by_id:
            raise MLLabelDatasetError(f"{context_sidecar_path} has duplicate row_id {row_id}")
        by_id[row_id] = row
    return payload, by_id


def _require_labeled_v2_fields(fieldnames: list[str], *, path: Path) -> None:
    required = {
        "row_id",
        "worksheet_version",
        "review_pool_variant",
        "paper_id",
        "openalex_work_id",
        "work_id",
        "title",
        "relevance_label",
        "novelty_label",
        "bridge_like_label",
        "reviewer_notes",
    }
    missing = sorted(required - set(fieldnames))
    if missing:
        raise MLLabelDatasetError(f"{path} missing required columns: {', '.join(missing)}")


def _validate_nonempty_allowed_labels(row: dict[str, str], *, source_row_number: int) -> None:
    checks = (
        ("relevance_label", ALLOWED_RELEVANCE_LABELS),
        ("novelty_label", ALLOWED_NOVELTY_LABELS),
        ("bridge_like_label", ALLOWED_BRIDGE_LIKE_LABELS),
    )
    for field, allowed in checks:
        value = _norm_label_token(row.get(field))
        if not value:
            raise MLLabelDatasetError(f"labeled CSV row {source_row_number} has blank {field}")
        if value not in allowed:
            raise MLLabelDatasetError(
                f"labeled CSV row {source_row_number} has unsupported {field}={row.get(field)!r}"
            )
    if not _norm_ws(row.get("reviewer_notes")):
        raise MLLabelDatasetError(f"labeled CSV row {source_row_number} has blank reviewer_notes")


def _norm_path_text(value: Any) -> str:
    return _norm_ws(value).replace("\\", "/").lstrip("./")


def _validate_sidecar_base_dataset_provenance(
    *,
    sidecar_provenance: dict[str, Any],
    base_path: Path,
    repo_root: Path,
) -> tuple[str, str]:
    base_sha = sha256_file(base_path)
    sidecar_base_sha = _norm_ws(sidecar_provenance.get("label_dataset_sha256"))
    if not sidecar_base_sha:
        raise MLLabelDatasetError("external near-miss sidecar missing provenance.label_dataset_sha256")
    if sidecar_base_sha != base_sha:
        raise MLLabelDatasetError(
            f"sidecar label_dataset_sha256 does not match base dataset SHA; sidecar={sidecar_base_sha}, base={base_sha}"
        )

    sidecar_base_path = _norm_path_text(sidecar_provenance.get("label_dataset_path"))
    if not sidecar_base_path:
        raise MLLabelDatasetError("external near-miss sidecar missing provenance.label_dataset_path")
    base_rel = _repo_relative(base_path, repo_root=repo_root)
    allowed = {
        _norm_path_text(base_rel),
        _norm_path_text(base_path.resolve().as_posix()),
        _norm_path_text(base_path.name),
    }
    if sidecar_base_path not in allowed and not sidecar_base_path.endswith("/" + _norm_path_text(base_rel)):
        raise MLLabelDatasetError(
            "external near-miss sidecar label_dataset_path does not point to the base dataset; "
            f"sidecar={sidecar_provenance.get('label_dataset_path')!r}, base={base_rel!r}"
        )
    return base_sha, sidecar_base_sha


def _validate_labeled_matches_blank_template(
    *,
    blank_path: Path,
    blank_fieldnames: list[str],
    blank_rows: list[dict[str, str]],
    labeled_path: Path,
    labeled_fieldnames: list[str],
    labeled_rows: list[dict[str, str]],
) -> None:
    _require_labeled_v2_fields(blank_fieldnames, path=blank_path)
    _require_labeled_v2_fields(labeled_fieldnames, path=labeled_path)

    blank_by_id: dict[str, dict[str, str]] = {}
    labeled_by_id: dict[str, dict[str, str]] = {}
    for row in blank_rows:
        rid = _norm_ws(row.get("row_id"))
        if not rid:
            raise MLLabelDatasetError(f"{blank_path} contains a row with blank row_id")
        if rid in blank_by_id:
            raise MLLabelDatasetError(f"{blank_path} contains duplicate row_id {rid}")
        blank_by_id[rid] = row
    for row in labeled_rows:
        rid = _norm_ws(row.get("row_id"))
        if not rid:
            raise MLLabelDatasetError(f"{labeled_path} contains a row with blank row_id")
        if rid in labeled_by_id:
            raise MLLabelDatasetError(f"{labeled_path} contains duplicate row_id {rid}")
        labeled_by_id[rid] = row

    if set(blank_by_id) != set(labeled_by_id):
        missing = sorted(set(blank_by_id) - set(labeled_by_id))
        extra = sorted(set(labeled_by_id) - set(blank_by_id))
        raise MLLabelDatasetError(
            f"v2 labeled CSV row_id set differs from blank template; missing={missing[:5]}, extra={extra[:5]}"
        )

    comparable_fields = (set(blank_fieldnames) | set(labeled_fieldnames)) - BLIND_SNAPSHOT_REVIEW_V2_REVIEW_COLUMNS
    for row_id in sorted(labeled_by_id):
        blank = blank_by_id[row_id]
        labeled = labeled_by_id[row_id]
        for field in sorted(comparable_fields):
            if _norm_ws(blank.get(field)) != _norm_ws(labeled.get(field)):
                raise MLLabelDatasetError(
                    "v2 labeled CSV changed non-review template field "
                    f"{field!r} for row_id={row_id}: blank={blank.get(field)!r}, labeled={labeled.get(field)!r}"
                )


def _assemble_dataset_payload_from_rows(
    *,
    dataset_version: str,
    generated_at: str,
    source_worksheets: list[str],
    source_sha256: dict[str, str],
    all_rows: list[dict[str, Any]],
    manual_review_dir_rel: str,
    row_counts_by_source: dict[str, int],
    included_by_source: dict[str, int],
    blank_rows_by_source: dict[str, int],
    skipped_blank_worksheets: list[str],
    skipped_malformed_rows: list[dict[str, Any]],
    extra_metadata: dict[str, Any] | None = None,
    extra_caveats: list[str] | None = None,
) -> dict[str, Any]:
    source_worksheets_sorted = sorted(set(source_worksheets))

    by_family: Counter[str] = Counter()
    by_review_pool_variant: Counter[str] = Counter()
    for r in all_rows:
        fam = r.get("family")
        key = str(fam) if fam is not None else "(null)"
        by_family[key] += 1
        pool = r.get("review_pool_variant")
        pool_key = str(pool) if pool is not None and str(pool).strip() else "(null)"
        by_review_pool_variant[pool_key] += 1

    inferred_family_count = sum(1 for r in all_rows if r.get("family_inferred") is True)
    inferred_family_by_source: Counter[str] = Counter()
    for r in all_rows:
        if r.get("family_inferred") is True:
            inferred_family_by_source[str(r["source_worksheet_path"])] += 1

    completeness: Counter[str] = Counter()
    unmapped_label_warnings: list[str] = []
    for r in all_rows:
        n = sum(1 for k in LABEL_FIELDS if r.get(k))
        completeness[str(n)] += 1
        source = str(r.get("source_worksheet_path", ""))
        line = r.get("source_row_number", "?")
        if good_or_acceptable(r.get("relevance_label")) is None and r.get("relevance_label"):
            unmapped_label_warnings.append(
                f"{source} row {line}: unmapped relevance_label={r.get('relevance_label')!r}"
            )
        if surprising_or_useful(r.get("novelty_label")) is None and r.get("novelty_label"):
            unmapped_label_warnings.append(
                f"{source} row {line}: unmapped novelty_label={r.get('novelty_label')!r}"
            )
        if bridge_like_yes_or_partial(r.get("bridge_like_label")) is None and r.get("bridge_like_label"):
            t = _norm_label_token(r.get("bridge_like_label"))
            if t not in {"", "not_applicable", "not applicable"}:
                unmapped_label_warnings.append(
                    f"{source} row {line}: unmapped bridge_like_label={r.get('bridge_like_label')!r}"
                )

    paper_to_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in all_rows:
        paper_to_rows[str(r["paper_id"])].append(r)

    duplicate_paper_report: dict[str, Any] = {
        "duplicate_paper_id_count": sum(1 for _pid, lst in paper_to_rows.items() if len(lst) > 1),
        "duplicate_paper_ids": sorted(pid for pid, lst in paper_to_rows.items() if len(lst) > 1),
        "observations_per_paper_id": {pid: len(lst) for pid, lst in paper_to_rows.items() if len(lst) > 1},
    }

    conflicts: list[dict[str, Any]] = []
    for field in LABEL_FIELDS:
        for pid, lst in paper_to_rows.items():
            by_norm: dict[str, list[str]] = defaultdict(list)
            for r in lst:
                v = r.get(field)
                if v is None or v == "":
                    continue
                nv = _norm_label_token(str(v))
                if nv:
                    by_norm[nv].append(str(r["row_id"]))
            if len(by_norm) > 1:
                conflicts.append(
                    {
                        "paper_id": pid,
                        "field": field,
                        "distinct_normalized_values": sorted(by_norm.keys()),
                        "row_ids_by_normalized_value": {k: v for k, v in sorted(by_norm.items())},
                    }
                )

    derived_conflicts: list[dict[str, Any]] = []
    for pid, lst in paper_to_rows.items():
        for field in DERIVED_TARGET_FIELDS:
            true_ids: list[str] = []
            false_ids: list[str] = []
            for r in lst:
                v = r.get(field)
                if v is True:
                    true_ids.append(str(r["row_id"]))
                elif v is False:
                    false_ids.append(str(r["row_id"]))
            if true_ids and false_ids:
                derived_conflicts.append(
                    {
                        "paper_id": pid,
                        "field": field,
                        "true_row_ids": sorted(true_ids),
                        "false_row_ids": sorted(false_ids),
                    }
                )

    caveats = list(VERBATIM_CAVEATS)
    if unmapped_label_warnings:
        caveats.append("Some rows contain label strings outside the expected closed sets; derived targets are null for those.")
    if skipped_malformed_rows:
        caveats.append(f"Skipped {len(skipped_malformed_rows)} malformed labeled rows (see skipped_malformed_rows).")
    if extra_caveats:
        caveats.extend(extra_caveats)

    metadata: dict[str, Any] = {
        "manual_review_dir": manual_review_dir_rel,
        "row_counts_by_source": {k: row_counts_by_source[k] for k in sorted(row_counts_by_source)},
        "included_labeled_row_counts_by_source": {k: included_by_source.get(k, 0) for k in sorted(included_by_source)},
        "skipped_blank_row_counts_by_source": {k: blank_rows_by_source[k] for k in sorted(blank_rows_by_source)},
        "skipped_blank_worksheets": sorted(set(skipped_blank_worksheets)),
        "row_counts_by_family": dict(by_family),
        "row_counts_by_review_pool_variant": dict(by_review_pool_variant),
        "row_counts_by_label_completeness": dict(completeness),
        "duplicate_paper_id_report": duplicate_paper_report,
        "conflicting_label_report": {
            "conflicting_label_count": len(conflicts),
            "conflicts": conflicts,
        },
        "derived_target_conflict_report": {
            "derived_target_conflict_count": len(derived_conflicts),
            "conflicts": derived_conflicts,
        },
        "inferred_family_count": inferred_family_count,
        "inferred_family_by_source": dict(sorted(inferred_family_by_source.items())),
        "skipped_malformed_rows": skipped_malformed_rows,
        "total_explicit_labeled_rows": len(all_rows),
        "total_blank_rows_skipped": sum(blank_rows_by_source.values()),
    }
    if extra_metadata:
        metadata.update(extra_metadata)

    sha_out = {k: source_sha256[k] for k in sorted(source_sha256) if k in source_worksheets_sorted}
    return {
        "dataset_version": dataset_version,
        "generated_at": generated_at,
        "caveats": caveats,
        "source_worksheets": source_worksheets_sorted,
        "source_worksheet_sha256": sha_out,
        "rows": all_rows,
        "metadata": metadata,
    }


def build_ml_label_dataset(
    *,
    repo_root: Path,
    manual_review_dir: Path | None = None,
    dataset_version: str | None = None,
) -> dict[str, Any]:
    root = repo_root.resolve()
    mdir = (manual_review_dir or (root / "docs" / "audit" / "manual-review")).resolve()
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    ver = dataset_version or DEFAULT_DATASET_VERSION
    csv_paths = discover_manual_review_csvs(mdir)
    skipped_blank_worksheets: list[str] = []
    source_sha256: dict[str, str] = {}
    row_counts_by_source: dict[str, int] = {}
    included_by_source: dict[str, int] = {}
    blank_rows_by_source: dict[str, int] = {}
    all_rows: list[dict[str, Any]] = []
    unmapped_label_warnings: list[str] = []
    skipped_malformed_all: list[dict[str, Any]] = []
    source_worksheets_sorted: list[str] = []

    for p in csv_paths:
        pw = parse_manual_review_worksheet(p, repo_root=root, dataset_version=ver)
        if pw is None:
            continue
        source_worksheets_sorted.append(pw.rel_path)
        source_sha256[pw.rel_path] = pw.sha256
        row_counts_by_source[pw.rel_path] = pw.data_row_count
        included_by_source[pw.rel_path] = len(pw.included_rows)
        blank_rows_by_source[pw.rel_path] = pw.skipped_blank_rows
        if len(pw.included_rows) == 0:
            skipped_blank_worksheets.append(pw.rel_path)
        for m in pw.skipped_malformed:
            skipped_malformed_all.append({**m, "source_worksheet_path": pw.rel_path})
        for row in pw.included_rows:
            if good_or_acceptable(row.get("relevance_label")) is None and row.get("relevance_label"):
                unmapped_label_warnings.append(
                    f"{pw.rel_path} row {row['source_row_number']}: unmapped relevance_label={row.get('relevance_label')!r}"
                )
            if surprising_or_useful(row.get("novelty_label")) is None and row.get("novelty_label"):
                unmapped_label_warnings.append(
                    f"{pw.rel_path} row {row['source_row_number']}: unmapped novelty_label={row.get('novelty_label')!r}"
                )
            if bridge_like_yes_or_partial(row.get("bridge_like_label")) is None and row.get("bridge_like_label"):
                t = _norm_label_token(row.get("bridge_like_label"))
                if t not in {"", "not_applicable", "not applicable"}:
                    unmapped_label_warnings.append(
                        f"{pw.rel_path} row {row['source_row_number']}: unmapped bridge_like_label={row.get('bridge_like_label')!r}"
                    )
        all_rows.extend(pw.included_rows)

    source_worksheets_sorted = sorted(set(source_worksheets_sorted))

    by_family: Counter[str] = Counter()
    for r in all_rows:
        fam = r.get("family")
        key = str(fam) if fam is not None else "(null)"
        by_family[key] += 1

    inferred_family_count = sum(1 for r in all_rows if r.get("family_inferred") is True)
    inferred_family_by_source: Counter[str] = Counter()
    for r in all_rows:
        if r.get("family_inferred") is True:
            inferred_family_by_source[str(r["source_worksheet_path"])] += 1

    completeness: Counter[str] = Counter()
    for r in all_rows:
        n = sum(1 for k in LABEL_FIELDS if r.get(k))
        completeness[str(n)] += 1

    paper_to_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in all_rows:
        paper_to_rows[str(r["paper_id"])].append(r)

    duplicate_paper_report: dict[str, Any] = {
        "duplicate_paper_id_count": sum(1 for pid, lst in paper_to_rows.items() if len(lst) > 1),
        "duplicate_paper_ids": sorted(pid for pid, lst in paper_to_rows.items() if len(lst) > 1),
        "observations_per_paper_id": {pid: len(lst) for pid, lst in paper_to_rows.items() if len(lst) > 1},
    }

    conflicts: list[dict[str, Any]] = []

    def _collect_conflicts(field: str) -> None:
        for pid, lst in paper_to_rows.items():
            by_norm: dict[str, list[str]] = defaultdict(list)
            for r in lst:
                v = r.get(field)
                if v is None or v == "":
                    continue
                nv = _norm_label_token(str(v))
                if not nv:
                    continue
                by_norm[nv].append(str(r["row_id"]))
            if len(by_norm) > 1:
                conflicts.append(
                    {
                        "paper_id": pid,
                        "field": field,
                        "distinct_normalized_values": sorted(by_norm.keys()),
                        "row_ids_by_normalized_value": {k: v for k, v in sorted(by_norm.items())},
                    }
                )

    for f in LABEL_FIELDS:
        _collect_conflicts(f)

    derived_conflicts: list[dict[str, Any]] = []
    for pid, lst in paper_to_rows.items():
        for field in DERIVED_TARGET_FIELDS:
            true_ids: list[str] = []
            false_ids: list[str] = []
            for r in lst:
                v = r.get(field)
                if v is True:
                    true_ids.append(str(r["row_id"]))
                elif v is False:
                    false_ids.append(str(r["row_id"]))
            if true_ids and false_ids:
                derived_conflicts.append(
                    {
                        "paper_id": pid,
                        "field": field,
                        "true_row_ids": sorted(true_ids),
                        "false_row_ids": sorted(false_ids),
                    }
                )

    caveats = list(VERBATIM_CAVEATS)
    if unmapped_label_warnings:
        caveats.append("Some rows contain label strings outside the expected closed sets; derived targets are null for those.")
    if skipped_malformed_all:
        caveats.append(f"Skipped {len(skipped_malformed_all)} malformed labeled rows (see skipped_malformed_rows).")

    sha_out = {k: source_sha256[k] for k in sorted(source_sha256) if k in source_worksheets_sorted}
    return {
        "dataset_version": ver,
        "generated_at": generated_at,
        "caveats": caveats,
        "source_worksheets": source_worksheets_sorted,
        "source_worksheet_sha256": sha_out,
        "rows": all_rows,
        "metadata": {
            "manual_review_dir": mdir.relative_to(root).as_posix(),
            "row_counts_by_source": {k: row_counts_by_source[k] for k in sorted(row_counts_by_source)},
            "included_labeled_row_counts_by_source": {k: included_by_source.get(k, 0) for k in sorted(included_by_source)},
            "skipped_blank_row_counts_by_source": {k: blank_rows_by_source[k] for k in sorted(blank_rows_by_source)},
            "skipped_blank_worksheets": sorted(set(skipped_blank_worksheets)),
            "row_counts_by_family": dict(by_family),
            "row_counts_by_label_completeness": dict(completeness),
            "duplicate_paper_id_report": duplicate_paper_report,
            "conflicting_label_report": {
                "conflicting_label_count": len(conflicts),
                "conflicts": conflicts,
            },
            "derived_target_conflict_report": {
                "derived_target_conflict_count": len(derived_conflicts),
                "conflicts": derived_conflicts,
            },
            "inferred_family_count": inferred_family_count,
            "inferred_family_by_source": dict(sorted(inferred_family_by_source.items())),
            "skipped_malformed_rows": skipped_malformed_all,
            "total_explicit_labeled_rows": len(all_rows),
            "total_blank_rows_skipped": sum(blank_rows_by_source.values()),
        },
    }


def build_ml_label_dataset_v5_reviewer_blind_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    dataset_version: str = "ml-label-dataset-v5",
) -> dict[str, Any]:
    """Build v5 as v4 rows unchanged plus the validated reviewer-blind v2 labeled slice."""
    root = repo_root.resolve()
    base_path = base_dataset_path.resolve()
    blank_path = blank_worksheet_path.resolve()
    labeled_path = labeled_worksheet_path.resolve()
    sidecar_path = context_sidecar_path.resolve()
    for path in (base_path, blank_path, labeled_path, sidecar_path):
        if not path.is_file():
            raise MLLabelDatasetError(f"required input not found: {path}")

    base_payload = _load_json_object(base_path)
    base_rows_raw = base_payload.get("rows")
    if not isinstance(base_rows_raw, list):
        raise MLLabelDatasetError(f"{base_path} missing rows array")
    base_rows: list[dict[str, Any]] = copy.deepcopy(base_rows_raw)

    blank_fieldnames, blank_rows = _read_csv_rows(blank_path)
    labeled_fieldnames, labeled_rows = _read_csv_rows(labeled_path)
    if len(labeled_rows) != BLIND_SNAPSHOT_REVIEW_V2_EXPECTED_ROWS:
        raise MLLabelDatasetError(
            f"expected {BLIND_SNAPSHOT_REVIEW_V2_EXPECTED_ROWS} v2 labeled rows, found {len(labeled_rows)}"
        )

    _validate_labeled_matches_blank_template(
        blank_path=blank_path,
        blank_fieldnames=blank_fieldnames,
        blank_rows=blank_rows,
        labeled_path=labeled_path,
        labeled_fieldnames=labeled_fieldnames,
        labeled_rows=labeled_rows,
    )

    sidecar_payload, sidecar_by_id = _read_v2_sidecar_rows(sidecar_path)
    labeled_ids = {_norm_ws(r.get("row_id")) for r in labeled_rows}
    if set(sidecar_by_id) != labeled_ids:
        missing = sorted(labeled_ids - set(sidecar_by_id))
        extra = sorted(set(sidecar_by_id) - labeled_ids)
        raise MLLabelDatasetError(
            f"v2 sidecar row_id set differs from labeled CSV; missing={missing[:5]}, extra={extra[:5]}"
        )

    base_sha = sha256_file(base_path)
    sidecar_provenance = sidecar_payload.get("provenance") if isinstance(sidecar_payload.get("provenance"), dict) else {}
    sidecar_base_sha = _norm_ws(sidecar_provenance.get("label_dataset_sha256")) if isinstance(sidecar_provenance, dict) else ""
    if sidecar_base_sha and sidecar_base_sha != base_sha:
        raise MLLabelDatasetError(
            f"sidecar label_dataset_sha256 does not match base dataset SHA; sidecar={sidecar_base_sha}, base={base_sha}"
        )
    sidecar_ws_version = _norm_ws(sidecar_provenance.get("worksheet_version")) if isinstance(sidecar_provenance, dict) else ""
    if sidecar_ws_version and sidecar_ws_version != BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION:
        raise MLLabelDatasetError(
            f"sidecar worksheet_version={sidecar_ws_version!r} does not match {BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION!r}"
        )

    source_rel = _repo_relative(labeled_path, repo_root=root)
    source_sha = sha256_file(labeled_path)
    v2_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_row_number, row in enumerate(labeled_rows, start=2):
        row_id = _norm_ws(row.get("row_id"))
        if row_id in seen:
            raise MLLabelDatasetError(f"duplicate v2 labeled row_id {row_id}")
        seen.add(row_id)
        _validate_nonempty_allowed_labels(row, source_row_number=source_row_number)

        worksheet_version = _norm_ws(row.get("worksheet_version"))
        if worksheet_version != BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION:
            raise MLLabelDatasetError(
                f"v2 labeled row {source_row_number} has worksheet_version={worksheet_version!r}"
            )
        review_pool_variant = _norm_ws(row.get("review_pool_variant"))
        if review_pool_variant != BLIND_REVIEW_POOL_VARIANT:
            raise MLLabelDatasetError(
                f"v2 labeled row {source_row_number} has review_pool_variant={review_pool_variant!r}"
            )

        paper_id = _norm_ws(row.get("paper_id"))
        openalex_work_id = _norm_ws(row.get("openalex_work_id"))
        work_id = _norm_ws(row.get("work_id"))
        expected_work_id = paper_id_to_work_id(paper_id)
        if not expected_work_id:
            raise MLLabelDatasetError(f"v2 labeled row {source_row_number} has non-OpenAlex paper_id={paper_id!r}")
        if work_id != expected_work_id or openalex_work_id != expected_work_id:
            raise MLLabelDatasetError(
                f"v2 labeled row {source_row_number} must keep OpenAlex W token in work_id/openalex_work_id"
            )
        if work_id.isdigit():
            raise MLLabelDatasetError(f"v2 labeled row {source_row_number} has numeric work_id={work_id!r}")

        context_row = copy.deepcopy(sidecar_by_id[row_id])
        if _norm_ws(context_row.get("paper_id")) != paper_id:
            raise MLLabelDatasetError(f"v2 sidecar paper_id mismatch for row_id={row_id}")
        if _norm_ws(context_row.get("openalex_work_id")) != openalex_work_id:
            raise MLLabelDatasetError(f"v2 sidecar openalex_work_id mismatch for row_id={row_id}")

        sample_seed = context_row.get("sample_seed", sidecar_provenance.get("sample_seed"))
        expected_row_id = stable_blind_snapshot_v2_row_id(
            worksheet_version=worksheet_version,
            sample_seed=sample_seed,
            paper_id=paper_id,
        )
        if row_id != expected_row_id:
            raise MLLabelDatasetError(
                f"v2 labeled row {source_row_number} row_id does not match worksheet_version|sample_seed|paper_id"
            )

        rel_l = _norm_ws(row.get("relevance_label")) or None
        nov_l = _norm_ws(row.get("novelty_label")) or None
        br_l = _norm_ws(row.get("bridge_like_label")) or None
        notes = _norm_ws(row.get("reviewer_notes")) or None
        ranking_run_id = _norm_ws(context_row.get("ranking_run_id") or sidecar_provenance.get("ranking_run_id")) or None
        out: dict[str, Any] = {
            "dataset_version": dataset_version,
            "row_id": row_id,
            "paper_id": paper_id,
            "work_id": work_id,
            "title": _norm_ws(row.get("title")) or None,
            "year": _norm_ws(row.get("year")) or None,
            "citation_count": _norm_ws(row.get("citation_count")) or None,
            "source_slug": _norm_ws(row.get("source_slug")) or None,
            "ranking_run_id": ranking_run_id,
            "ranking_version": None,
            "corpus_snapshot_version": context_row.get("corpus_snapshot_version") or sidecar_provenance.get("corpus_snapshot_version"),
            "embedding_version": context_row.get("embedding_version") or sidecar_provenance.get("embedding_version"),
            "cluster_version": context_row.get("cluster_version") or sidecar_provenance.get("cluster_version"),
            "family": None,
            "review_pool_variant": review_pool_variant,
            "rank": None,
            "experiment_rank": None,
            "source_worksheet_path": source_rel,
            "source_worksheet_sha256": source_sha,
            "source_row_number": source_row_number,
            "relevance_label": rel_l,
            "novelty_label": nov_l,
            "bridge_like_label": br_l,
            "reviewer_notes": notes,
            "label_provenance": "manual_review_worksheet_csv",
            "split": "audit_only",
            "good_or_acceptable": good_or_acceptable(rel_l),
            "surprising_or_useful": surprising_or_useful(nov_l),
            "bridge_like_yes_or_partial": bridge_like_yes_or_partial(br_l),
            "worksheet_version": worksheet_version,
            "sample_seed": sample_seed,
            "sample_reason": context_row.get("sample_reason") or _norm_ws(row.get("sample_reason")) or None,
            "cluster_id": context_row.get("cluster_id") or _norm_ws(row.get("cluster_id")) or None,
            "topics": _norm_ws(row.get("topics")) or None,
            "abstract_preview": _norm_ws(row.get("abstract_preview")) or None,
            "openalex_work_id": openalex_work_id,
            "internal_work_id": context_row.get("internal_work_id"),
            "ranking_context_family_scores_json": context_row.get("ranking_context_family_scores_json"),
            "ranking_context_family_ranks_json": context_row.get("ranking_context_family_ranks_json"),
            "blind_snapshot_context": context_row,
        }
        v2_rows.append(out)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metadata_in = base_payload.get("metadata") if isinstance(base_payload.get("metadata"), dict) else {}
    source_worksheets = list(base_payload.get("source_worksheets") or [])
    source_worksheets.append(source_rel)
    source_sha256 = dict(base_payload.get("source_worksheet_sha256") or {})
    source_sha256[source_rel] = source_sha
    row_counts_by_source = dict(metadata_in.get("row_counts_by_source") or {})
    row_counts_by_source[source_rel] = len(labeled_rows)
    included_by_source = dict(metadata_in.get("included_labeled_row_counts_by_source") or {})
    included_by_source[source_rel] = len(v2_rows)
    blank_rows_by_source = dict(metadata_in.get("skipped_blank_row_counts_by_source") or {})
    blank_rows_by_source[source_rel] = 0
    skipped_blank_worksheets = list(metadata_in.get("skipped_blank_worksheets") or [])
    skipped_malformed_rows = copy.deepcopy(metadata_in.get("skipped_malformed_rows") or [])
    manual_review_dir_rel = str(metadata_in.get("manual_review_dir") or "docs/audit/manual-review")

    extra_metadata = {
        "reviewer_blind_v2_ingest": {
            "base_dataset_path": _repo_relative(base_path, repo_root=root),
            "base_dataset_sha256": base_sha,
            "blank_template_path": _repo_relative(blank_path, repo_root=root),
            "blank_template_sha256": sha256_file(blank_path),
            "labeled_worksheet_path": source_rel,
            "labeled_worksheet_sha256": source_sha,
            "context_sidecar_path": _repo_relative(sidecar_path, repo_root=root),
            "context_sidecar_sha256": sha256_file(sidecar_path),
            "worksheet_version": BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION,
            "review_pool_variant": BLIND_REVIEW_POOL_VARIANT,
            "canonical_row_id_source": "v2 labeled CSV row_id column",
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "base_row_count": len(base_rows),
            "v2_rows_appended": len(v2_rows),
            "sidecar_row_ids_matched": True,
            "blank_template_identity_columns_matched": True,
        }
    }
    extra_caveats = [
        "Reviewer-blind v2 hidden score/rank context is preserved for audit provenance only and is not label evidence."
    ]
    return _assemble_dataset_payload_from_rows(
        dataset_version=dataset_version,
        generated_at=generated_at,
        source_worksheets=source_worksheets,
        source_sha256=source_sha256,
        all_rows=base_rows + v2_rows,
        manual_review_dir_rel=manual_review_dir_rel,
        row_counts_by_source=row_counts_by_source,
        included_by_source=included_by_source,
        blank_rows_by_source=blank_rows_by_source,
        skipped_blank_worksheets=skipped_blank_worksheets,
        skipped_malformed_rows=skipped_malformed_rows,
        extra_metadata=extra_metadata,
        extra_caveats=extra_caveats,
    )


def build_ml_label_dataset_v6_hard_negative_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    dataset_version: str = "ml-label-dataset-v6",
) -> dict[str, Any]:
    """Build v6 as v5 rows unchanged plus the validated hard-negative labeled slice."""
    root = repo_root.resolve()
    base_path = base_dataset_path.resolve()
    blank_path = blank_worksheet_path.resolve()
    labeled_path = labeled_worksheet_path.resolve()
    sidecar_path = context_sidecar_path.resolve()
    for path in (base_path, blank_path, labeled_path, sidecar_path):
        if not path.is_file():
            raise MLLabelDatasetError(f"required input not found: {path}")

    base_payload = _load_json_object(base_path)
    base_rows_raw = base_payload.get("rows")
    if not isinstance(base_rows_raw, list):
        raise MLLabelDatasetError(f"{base_path} missing rows array")
    base_rows: list[dict[str, Any]] = copy.deepcopy(base_rows_raw)

    blank_fieldnames, blank_rows = _read_csv_rows(blank_path)
    labeled_fieldnames, labeled_rows = _read_csv_rows(labeled_path)
    if len(labeled_rows) != HARD_NEGATIVE_REVIEW_V1_EXPECTED_ROWS:
        raise MLLabelDatasetError(
            f"expected {HARD_NEGATIVE_REVIEW_V1_EXPECTED_ROWS} hard-negative labeled rows, found {len(labeled_rows)}"
        )

    _validate_labeled_matches_blank_template(
        blank_path=blank_path,
        blank_fieldnames=blank_fieldnames,
        blank_rows=blank_rows,
        labeled_path=labeled_path,
        labeled_fieldnames=labeled_fieldnames,
        labeled_rows=labeled_rows,
    )

    sidecar_payload, sidecar_by_id = _read_v2_sidecar_rows(sidecar_path)
    labeled_ids = {_norm_ws(r.get("row_id")) for r in labeled_rows}
    if set(sidecar_by_id) != labeled_ids:
        missing = sorted(labeled_ids - set(sidecar_by_id))
        extra = sorted(set(sidecar_by_id) - labeled_ids)
        raise MLLabelDatasetError(
            f"hard-negative sidecar row_id set differs from labeled CSV; missing={missing[:5]}, extra={extra[:5]}"
        )

    base_sha = sha256_file(base_path)
    sidecar_provenance = sidecar_payload.get("provenance") if isinstance(sidecar_payload.get("provenance"), dict) else {}
    sidecar_base_sha = _norm_ws(sidecar_provenance.get("label_dataset_sha256")) if isinstance(sidecar_provenance, dict) else ""
    if sidecar_base_sha and sidecar_base_sha != base_sha:
        raise MLLabelDatasetError(
            f"sidecar label_dataset_sha256 does not match base dataset SHA; sidecar={sidecar_base_sha}, base={base_sha}"
        )
    sidecar_ws_version = _norm_ws(sidecar_provenance.get("worksheet_version")) if isinstance(sidecar_provenance, dict) else ""
    if sidecar_ws_version and sidecar_ws_version != HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION:
        raise MLLabelDatasetError(
            f"sidecar worksheet_version={sidecar_ws_version!r} does not match {HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION!r}"
        )

    source_rel = _repo_relative(labeled_path, repo_root=root)
    source_sha = sha256_file(labeled_path)
    hn_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_row_number, row in enumerate(labeled_rows, start=2):
        row_id = _norm_ws(row.get("row_id"))
        if row_id in seen:
            raise MLLabelDatasetError(f"duplicate hard-negative labeled row_id {row_id}")
        seen.add(row_id)
        _validate_nonempty_allowed_labels(row, source_row_number=source_row_number)

        worksheet_version = _norm_ws(row.get("worksheet_version"))
        if worksheet_version != HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION:
            raise MLLabelDatasetError(
                f"hard-negative labeled row {source_row_number} has worksheet_version={worksheet_version!r}"
            )
        review_pool_variant = _norm_ws(row.get("review_pool_variant"))
        if review_pool_variant != HARD_NEGATIVE_REVIEW_POOL_VARIANT:
            raise MLLabelDatasetError(
                f"hard-negative labeled row {source_row_number} has review_pool_variant={review_pool_variant!r}"
            )

        paper_id = _norm_ws(row.get("paper_id"))
        openalex_work_id = _norm_ws(row.get("openalex_work_id"))
        work_id = _norm_ws(row.get("work_id"))
        expected_work_id = paper_id_to_work_id(paper_id)
        if not expected_work_id:
            raise MLLabelDatasetError(
                f"hard-negative labeled row {source_row_number} has non-OpenAlex paper_id={paper_id!r}"
            )
        if work_id != expected_work_id or openalex_work_id != expected_work_id:
            raise MLLabelDatasetError(
                f"hard-negative labeled row {source_row_number} must keep OpenAlex W token in work_id/openalex_work_id"
            )
        if work_id.isdigit():
            raise MLLabelDatasetError(f"hard-negative labeled row {source_row_number} has numeric work_id={work_id!r}")

        context_row = copy.deepcopy(sidecar_by_id[row_id])
        if _norm_ws(context_row.get("paper_id")) != paper_id:
            raise MLLabelDatasetError(f"hard-negative sidecar paper_id mismatch for row_id={row_id}")
        if _norm_ws(context_row.get("openalex_work_id")) != openalex_work_id:
            raise MLLabelDatasetError(f"hard-negative sidecar openalex_work_id mismatch for row_id={row_id}")

        sample_seed = context_row.get("sample_seed", sidecar_provenance.get("sample_seed"))
        expected_row_id = stable_hard_negative_v1_row_id(
            worksheet_version=worksheet_version,
            sample_seed=sample_seed,
            paper_id=paper_id,
        )
        if row_id != expected_row_id:
            raise MLLabelDatasetError(
                f"hard-negative labeled row {source_row_number} row_id does not match worksheet_version|sample_seed|paper_id"
            )

        rel_l = _norm_ws(row.get("relevance_label")) or None
        nov_l = _norm_ws(row.get("novelty_label")) or None
        br_l = _norm_ws(row.get("bridge_like_label")) or None
        notes = _norm_ws(row.get("reviewer_notes")) or None
        ranking_run_id = _norm_ws(context_row.get("ranking_run_id") or sidecar_provenance.get("ranking_run_id")) or None
        out: dict[str, Any] = {
            "dataset_version": dataset_version,
            "row_id": row_id,
            "paper_id": paper_id,
            "work_id": work_id,
            "title": _norm_ws(row.get("title")) or None,
            "year": _norm_ws(row.get("year")) or None,
            "citation_count": _norm_ws(row.get("citation_count")) or None,
            "source_slug": _norm_ws(row.get("source_slug")) or None,
            "ranking_run_id": ranking_run_id,
            "ranking_version": None,
            "corpus_snapshot_version": context_row.get("corpus_snapshot_version") or sidecar_provenance.get("corpus_snapshot_version"),
            "embedding_version": context_row.get("embedding_version") or sidecar_provenance.get("embedding_version"),
            "cluster_version": context_row.get("cluster_version") or sidecar_provenance.get("cluster_version"),
            "family": _norm_ws(row.get("family")) or None if "family" in _fieldnames_set(labeled_fieldnames) else None,
            "review_pool_variant": review_pool_variant,
            "rank": None,
            "experiment_rank": None,
            "source_worksheet_path": source_rel,
            "source_worksheet_sha256": source_sha,
            "source_row_number": source_row_number,
            "relevance_label": rel_l,
            "novelty_label": nov_l,
            "bridge_like_label": br_l,
            "reviewer_notes": notes,
            "label_provenance": "manual_review_worksheet_csv",
            "split": "audit_only",
            "good_or_acceptable": good_or_acceptable(rel_l),
            "surprising_or_useful": surprising_or_useful(nov_l),
            "bridge_like_yes_or_partial": bridge_like_yes_or_partial(br_l),
            "worksheet_version": worksheet_version,
            "sample_seed": sample_seed,
            "sample_reason": context_row.get("sample_reason") or _norm_ws(row.get("sample_reason")) or None,
            "cluster_id": context_row.get("cluster_id") or _norm_ws(row.get("cluster_id")) or None,
            "topics": _norm_ws(row.get("topics")) or None,
            "abstract_preview": _norm_ws(row.get("abstract_preview")) or None,
            "openalex_work_id": openalex_work_id,
            "internal_work_id": context_row.get("internal_work_id"),
            "hard_negative_context": context_row,
        }
        hn_rows.append(out)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metadata_in = base_payload.get("metadata") if isinstance(base_payload.get("metadata"), dict) else {}
    source_worksheets = list(base_payload.get("source_worksheets") or [])
    source_worksheets.append(source_rel)
    source_sha256 = dict(base_payload.get("source_worksheet_sha256") or {})
    source_sha256[source_rel] = source_sha
    row_counts_by_source = dict(metadata_in.get("row_counts_by_source") or {})
    row_counts_by_source[source_rel] = len(labeled_rows)
    included_by_source = dict(metadata_in.get("included_labeled_row_counts_by_source") or {})
    included_by_source[source_rel] = len(hn_rows)
    blank_rows_by_source = dict(metadata_in.get("skipped_blank_row_counts_by_source") or {})
    blank_rows_by_source[source_rel] = 0
    skipped_blank_worksheets = list(metadata_in.get("skipped_blank_worksheets") or [])
    skipped_malformed_rows = copy.deepcopy(metadata_in.get("skipped_malformed_rows") or [])
    manual_review_dir_rel = str(metadata_in.get("manual_review_dir") or "docs/audit/manual-review")

    previous_ingests = {}
    if "reviewer_blind_v2_ingest" in metadata_in:
        previous_ingests["previous_reviewer_blind_v2_ingest"] = copy.deepcopy(metadata_in["reviewer_blind_v2_ingest"])

    extra_metadata = {
        **previous_ingests,
        "hard_negative_v1_ingest": {
            "base_dataset_path": _repo_relative(base_path, repo_root=root),
            "base_dataset_sha256": base_sha,
            "blank_template_path": _repo_relative(blank_path, repo_root=root),
            "blank_template_sha256": sha256_file(blank_path),
            "labeled_worksheet_path": source_rel,
            "labeled_worksheet_sha256": source_sha,
            "context_sidecar_path": _repo_relative(sidecar_path, repo_root=root),
            "context_sidecar_sha256": sha256_file(sidecar_path),
            "worksheet_version": HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION,
            "review_pool_variant": HARD_NEGATIVE_REVIEW_POOL_VARIANT,
            "canonical_row_id_source": "hard-negative labeled CSV row_id column",
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "base_row_count": len(base_rows),
            "hard_negative_rows_appended": len(hn_rows),
            "sidecar_row_ids_matched": True,
            "blank_template_identity_columns_matched": True,
        },
    }
    extra_caveats = [
        "Hard-negative hidden score/rank context is preserved for audit provenance only and is not label evidence.",
        "The hard-negative review pool remains distinct from blind snapshot rows unless a later experiment explicitly pools it.",
    ]
    return _assemble_dataset_payload_from_rows(
        dataset_version=dataset_version,
        generated_at=generated_at,
        source_worksheets=source_worksheets,
        source_sha256=source_sha256,
        all_rows=base_rows + hn_rows,
        manual_review_dir_rel=manual_review_dir_rel,
        row_counts_by_source=row_counts_by_source,
        included_by_source=included_by_source,
        blank_rows_by_source=blank_rows_by_source,
        skipped_blank_worksheets=skipped_blank_worksheets,
        skipped_malformed_rows=skipped_malformed_rows,
        extra_metadata=extra_metadata,
        extra_caveats=extra_caveats,
    )


def build_ml_label_dataset_v7_external_near_miss_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    conflict_policy_path: Path,
    dataset_version: str = "ml-label-dataset-v7",
) -> dict[str, Any]:
    """Build v7 as v6 rows unchanged plus the validated external near-miss labeled slice."""
    root = repo_root.resolve()
    base_path = base_dataset_path.resolve()
    blank_path = blank_worksheet_path.resolve()
    labeled_path = labeled_worksheet_path.resolve()
    sidecar_path = context_sidecar_path.resolve()
    conflict_path = conflict_policy_path.resolve()
    for path in (base_path, blank_path, labeled_path, sidecar_path, conflict_path):
        if not path.is_file():
            raise MLLabelDatasetError(f"required input not found: {path}")

    base_payload = _load_json_object(base_path)
    base_rows_raw = base_payload.get("rows")
    if not isinstance(base_rows_raw, list):
        raise MLLabelDatasetError(f"{base_path} missing rows array")
    base_rows: list[dict[str, Any]] = copy.deepcopy(base_rows_raw)

    blank_fieldnames, blank_rows = _read_csv_rows(blank_path)
    labeled_fieldnames, labeled_rows = _read_csv_rows(labeled_path)
    if len(labeled_rows) != EXTERNAL_NEAR_MISS_REVIEW_V1_EXPECTED_ROWS:
        raise MLLabelDatasetError(
            f"expected {EXTERNAL_NEAR_MISS_REVIEW_V1_EXPECTED_ROWS} external near-miss labeled rows, "
            f"found {len(labeled_rows)}"
        )

    _validate_labeled_matches_blank_template(
        blank_path=blank_path,
        blank_fieldnames=blank_fieldnames,
        blank_rows=blank_rows,
        labeled_path=labeled_path,
        labeled_fieldnames=labeled_fieldnames,
        labeled_rows=labeled_rows,
    )

    sidecar_payload, sidecar_by_id = _read_v2_sidecar_rows(sidecar_path)
    labeled_ids = {_norm_ws(r.get("row_id")) for r in labeled_rows}
    if set(sidecar_by_id) != labeled_ids:
        missing = sorted(labeled_ids - set(sidecar_by_id))
        extra = sorted(set(sidecar_by_id) - labeled_ids)
        raise MLLabelDatasetError(
            f"external near-miss sidecar row_id set differs from labeled CSV; missing={missing[:5]}, extra={extra[:5]}"
        )

    sidecar_provenance = sidecar_payload.get("provenance") if isinstance(sidecar_payload.get("provenance"), dict) else {}
    base_sha, sidecar_base_sha = _validate_sidecar_base_dataset_provenance(
        sidecar_provenance=sidecar_provenance,
        base_path=base_path,
        repo_root=root,
    )
    sidecar_ws_version = _norm_ws(sidecar_provenance.get("worksheet_version"))
    if sidecar_ws_version and sidecar_ws_version != EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION:
        raise MLLabelDatasetError(
            f"sidecar worksheet_version={sidecar_ws_version!r} does not match "
            f"{EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION!r}"
        )
    sidecar_pool = _norm_ws(sidecar_provenance.get("review_pool_variant"))
    if sidecar_pool and sidecar_pool != EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT:
        raise MLLabelDatasetError(
            f"sidecar review_pool_variant={sidecar_pool!r} does not match {EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT!r}"
        )

    source_rel = _repo_relative(labeled_path, repo_root=root)
    source_sha = sha256_file(labeled_path)
    external_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    labeled_names = _fieldnames_set(labeled_fieldnames)
    for source_row_number, row in enumerate(labeled_rows, start=2):
        row_id = _norm_ws(row.get("row_id"))
        if row_id in seen:
            raise MLLabelDatasetError(f"duplicate external near-miss labeled row_id {row_id}")
        seen.add(row_id)
        _validate_nonempty_allowed_labels(row, source_row_number=source_row_number)

        worksheet_version = _norm_ws(row.get("worksheet_version"))
        if worksheet_version != EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION:
            raise MLLabelDatasetError(
                f"external near-miss labeled row {source_row_number} has worksheet_version={worksheet_version!r}"
            )
        review_pool_variant = _norm_ws(row.get("review_pool_variant"))
        if review_pool_variant != EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT:
            raise MLLabelDatasetError(
                f"external near-miss labeled row {source_row_number} has review_pool_variant={review_pool_variant!r}"
            )

        paper_id = _norm_ws(row.get("paper_id"))
        openalex_work_id = _norm_ws(row.get("openalex_work_id"))
        work_id = _norm_ws(row.get("work_id"))
        expected_work_id = paper_id_to_work_id(paper_id)
        if not expected_work_id:
            raise MLLabelDatasetError(
                f"external near-miss labeled row {source_row_number} has non-OpenAlex paper_id={paper_id!r}"
            )
        if work_id != expected_work_id or openalex_work_id != expected_work_id:
            raise MLLabelDatasetError(
                "external near-miss labeled row "
                f"{source_row_number} must keep OpenAlex W token in work_id/openalex_work_id"
            )
        if work_id.isdigit():
            raise MLLabelDatasetError(
                f"external near-miss labeled row {source_row_number} has numeric work_id={work_id!r}"
            )

        context_row = copy.deepcopy(sidecar_by_id[row_id])
        if _norm_ws(context_row.get("paper_id")) != paper_id:
            raise MLLabelDatasetError(f"external near-miss sidecar paper_id mismatch for row_id={row_id}")
        if _norm_ws(context_row.get("openalex_work_id")) != openalex_work_id:
            raise MLLabelDatasetError(f"external near-miss sidecar openalex_work_id mismatch for row_id={row_id}")

        sample_seed = context_row.get("sample_seed", sidecar_provenance.get("sample_seed"))
        expected_row_id = stable_external_near_miss_v1_row_id(
            worksheet_version=worksheet_version,
            sample_seed=sample_seed,
            paper_id=paper_id,
        )
        if row_id != expected_row_id:
            raise MLLabelDatasetError(
                "external near-miss labeled row "
                f"{source_row_number} row_id does not match worksheet_version|sample_seed|paper_id"
            )

        rel_l = _norm_ws(row.get("relevance_label")) or None
        nov_l = _norm_ws(row.get("novelty_label")) or None
        br_l = _norm_ws(row.get("bridge_like_label")) or None
        notes = _norm_ws(row.get("reviewer_notes")) or None
        family = _norm_ws(row.get("family")) or None if "family" in labeled_names else None
        out: dict[str, Any] = {
            "dataset_version": dataset_version,
            "row_id": row_id,
            "paper_id": paper_id,
            "work_id": work_id,
            "title": _norm_ws(row.get("title")) or None,
            "year": _norm_ws(row.get("year")) or None,
            "citation_count": _norm_ws(row.get("citation_count")) or None,
            "source_slug": _norm_ws(row.get("source_slug")) or None,
            "ranking_run_id": None,
            "ranking_version": None,
            "corpus_snapshot_version": None,
            "embedding_version": None,
            "cluster_version": None,
            "family": family,
            "review_pool_variant": review_pool_variant,
            "rank": None,
            "experiment_rank": None,
            "source_worksheet_path": source_rel,
            "source_worksheet_sha256": source_sha,
            "source_row_number": source_row_number,
            "relevance_label": rel_l,
            "novelty_label": nov_l,
            "bridge_like_label": br_l,
            "reviewer_notes": notes,
            "label_provenance": "manual_review_worksheet_csv",
            "split": "audit_only",
            "good_or_acceptable": good_or_acceptable(rel_l),
            "surprising_or_useful": surprising_or_useful(nov_l),
            "bridge_like_yes_or_partial": bridge_like_yes_or_partial(br_l),
            "worksheet_version": worksheet_version,
            "sample_seed": sample_seed,
            "sample_reason": context_row.get("sample_reason") or _norm_ws(row.get("sample_reason")) or None,
            "cluster_id": context_row.get("cluster_id") or _norm_ws(row.get("cluster_id")) or None,
            "topics": _norm_ws(row.get("topics")) or None,
            "abstract_preview": _norm_ws(row.get("abstract_preview")) or None,
            "openalex_work_id": openalex_work_id,
            "internal_work_id": context_row.get("internal_work_id"),
            "external_near_miss_context": context_row,
        }
        external_rows.append(out)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metadata_in = base_payload.get("metadata") if isinstance(base_payload.get("metadata"), dict) else {}
    source_worksheets = list(base_payload.get("source_worksheets") or [])
    source_worksheets.append(source_rel)
    source_sha256 = dict(base_payload.get("source_worksheet_sha256") or {})
    source_sha256[source_rel] = source_sha
    row_counts_by_source = dict(metadata_in.get("row_counts_by_source") or {})
    row_counts_by_source[source_rel] = len(labeled_rows)
    included_by_source = dict(metadata_in.get("included_labeled_row_counts_by_source") or {})
    included_by_source[source_rel] = len(external_rows)
    blank_rows_by_source = dict(metadata_in.get("skipped_blank_row_counts_by_source") or {})
    blank_rows_by_source[source_rel] = 0
    skipped_blank_worksheets = list(metadata_in.get("skipped_blank_worksheets") or [])
    skipped_malformed_rows = copy.deepcopy(metadata_in.get("skipped_malformed_rows") or [])
    manual_review_dir_rel = str(metadata_in.get("manual_review_dir") or "docs/audit/manual-review")

    previous_ingests = {
        key: copy.deepcopy(value)
        for key, value in metadata_in.items()
        if key.endswith("_ingest") or key.startswith("previous_")
    }
    extra_metadata = {
        **previous_ingests,
        "external_near_miss_v1_ingest": {
            "base_dataset_path": _repo_relative(base_path, repo_root=root),
            "base_dataset_sha256": base_sha,
            "blank_template_path": _repo_relative(blank_path, repo_root=root),
            "blank_template_sha256": sha256_file(blank_path),
            "labeled_worksheet_path": source_rel,
            "labeled_worksheet_sha256": source_sha,
            "context_sidecar_path": _repo_relative(sidecar_path, repo_root=root),
            "context_sidecar_sha256": sha256_file(sidecar_path),
            "conflict_policy_path": _repo_relative(conflict_path, repo_root=root),
            "conflict_policy_sha256": sha256_file(conflict_path),
            "worksheet_version": EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION,
            "review_pool_variant": EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT,
            "canonical_row_id_source": "external near-miss labeled CSV row_id column",
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "base_row_count": len(base_rows),
            "external_near_miss_rows_appended": len(external_rows),
            "sidecar_label_dataset_sha256": sidecar_base_sha,
            "validation_summary": {
                "expected_labeled_rows": EXTERNAL_NEAR_MISS_REVIEW_V1_EXPECTED_ROWS,
                "labeled_rows_found": len(labeled_rows),
                "sidecar_row_ids_matched": True,
                "blank_template_identity_columns_matched": True,
                "sidecar_base_dataset_sha256_matched": True,
                "sidecar_base_dataset_path_matched": True,
                "non_review_columns_unchanged": True,
                "review_columns_required_non_empty": True,
            },
        },
    }
    extra_caveats = [
        "External near-miss context is preserved for audit provenance only and is not label evidence.",
        "The external near-miss review pool remains distinct from blind snapshot and hard-negative pools unless a later experiment explicitly pools it.",
    ]
    return _assemble_dataset_payload_from_rows(
        dataset_version=dataset_version,
        generated_at=generated_at,
        source_worksheets=source_worksheets,
        source_sha256=source_sha256,
        all_rows=base_rows + external_rows,
        manual_review_dir_rel=manual_review_dir_rel,
        row_counts_by_source=row_counts_by_source,
        included_by_source=included_by_source,
        blank_rows_by_source=blank_rows_by_source,
        skipped_blank_worksheets=skipped_blank_worksheets,
        skipped_malformed_rows=skipped_malformed_rows,
        extra_metadata=extra_metadata,
        extra_caveats=extra_caveats,
    )


def _appended_label_distribution(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    def _bool_key(value: Any) -> str:
        if value is True:
            return "true"
        if value is False:
            return "false"
        return "null"

    out: dict[str, dict[str, int]] = {}
    for field in LABEL_FIELDS:
        counts: Counter[str] = Counter()
        for row in rows:
            value = row.get(field)
            key = _norm_label_token(str(value)) if value is not None else ""
            counts[key or "(null)"] += 1
        out[field] = dict(sorted(counts.items()))
    for field in DERIVED_TARGET_FIELDS:
        counts = Counter(_bool_key(row.get(field)) for row in rows)
        out[field] = {key: counts.get(key, 0) for key in ("true", "false", "null")}
    return out


def build_ml_label_dataset_v8_transfer_gap_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    conflict_policy_path: Path,
    dataset_version: str = "ml-label-dataset-v8",
) -> dict[str, Any]:
    """Build v8 as v7 rows unchanged plus the validated transfer-gap labeled slice."""
    root = repo_root.resolve()
    base_path = base_dataset_path.resolve()
    blank_path = blank_worksheet_path.resolve()
    labeled_path = labeled_worksheet_path.resolve()
    sidecar_path = context_sidecar_path.resolve()
    conflict_path = conflict_policy_path.resolve()
    for path in (base_path, blank_path, labeled_path, sidecar_path, conflict_path):
        if not path.is_file():
            raise MLLabelDatasetError(f"required input not found: {path}")

    base_payload = _load_json_object(base_path)
    base_metadata = base_payload.get("metadata") if isinstance(base_payload.get("metadata"), dict) else {}
    base_version = _norm_ws(base_payload.get("dataset_version") or base_metadata.get("dataset_version"))
    if base_version != "ml-label-dataset-v7":
        raise MLLabelDatasetError(f"{base_path} dataset_version={base_version!r}; expected 'ml-label-dataset-v7'")
    base_rows_raw = base_payload.get("rows")
    if not isinstance(base_rows_raw, list):
        raise MLLabelDatasetError(f"{base_path} missing rows array")
    base_rows: list[dict[str, Any]] = copy.deepcopy(base_rows_raw)

    blank_fieldnames, blank_rows = _read_csv_rows(blank_path)
    labeled_fieldnames, labeled_rows = _read_csv_rows(labeled_path)
    if not labeled_rows:
        raise MLLabelDatasetError(f"{labeled_path} has no labeled transfer-gap data rows")

    _validate_labeled_matches_blank_template(
        blank_path=blank_path,
        blank_fieldnames=blank_fieldnames,
        blank_rows=blank_rows,
        labeled_path=labeled_path,
        labeled_fieldnames=labeled_fieldnames,
        labeled_rows=labeled_rows,
    )

    sidecar_payload, sidecar_by_id = _read_transfer_gap_sidecar_rows(sidecar_path)
    labeled_ids = {_norm_ws(r.get("row_id")) for r in labeled_rows}
    if set(sidecar_by_id) != labeled_ids:
        missing = sorted(labeled_ids - set(sidecar_by_id))
        extra = sorted(set(sidecar_by_id) - labeled_ids)
        raise MLLabelDatasetError(
            f"transfer-gap sidecar row_id set differs from labeled CSV; missing={missing[:5]}, extra={extra[:5]}"
        )

    sidecar_provenance = sidecar_payload.get("provenance") if isinstance(sidecar_payload.get("provenance"), dict) else {}
    sidecar_ws_version = _norm_ws(sidecar_provenance.get("worksheet_version"))
    if sidecar_ws_version and sidecar_ws_version != TRANSFER_GAP_REVIEW_V1_WORKSHEET_VERSION:
        raise MLLabelDatasetError(
            f"transfer-gap sidecar worksheet_version={sidecar_ws_version!r} does not match "
            f"{TRANSFER_GAP_REVIEW_V1_WORKSHEET_VERSION!r}"
        )
    sidecar_pool = _norm_ws(sidecar_provenance.get("review_pool_variant"))
    if sidecar_pool and sidecar_pool != TRANSFER_GAP_REVIEW_POOL_VARIANT:
        raise MLLabelDatasetError(
            f"transfer-gap sidecar review_pool_variant={sidecar_pool!r} does not match "
            f"{TRANSFER_GAP_REVIEW_POOL_VARIANT!r}"
        )

    source_rel = _repo_relative(labeled_path, repo_root=root)
    source_sha = sha256_file(labeled_path)
    transfer_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_row_number, row in enumerate(labeled_rows, start=2):
        row_id = _norm_ws(row.get("row_id"))
        if row_id in seen:
            raise MLLabelDatasetError(f"duplicate transfer-gap labeled row_id {row_id}")
        seen.add(row_id)
        _validate_nonempty_allowed_labels(row, source_row_number=source_row_number)

        worksheet_version = _norm_ws(row.get("worksheet_version"))
        if worksheet_version != TRANSFER_GAP_REVIEW_V1_WORKSHEET_VERSION:
            raise MLLabelDatasetError(
                f"transfer-gap labeled row {source_row_number} has worksheet_version={worksheet_version!r}"
            )
        review_pool_variant = _norm_ws(row.get("review_pool_variant"))
        if review_pool_variant != TRANSFER_GAP_REVIEW_POOL_VARIANT:
            raise MLLabelDatasetError(
                f"transfer-gap labeled row {source_row_number} has review_pool_variant={review_pool_variant!r}"
            )

        paper_id = _norm_ws(row.get("paper_id"))
        openalex_work_id = _norm_ws(row.get("openalex_work_id"))
        work_id = _norm_ws(row.get("work_id"))
        expected_work_id = paper_id_to_work_id(paper_id)
        if not expected_work_id:
            raise MLLabelDatasetError(
                f"transfer-gap labeled row {source_row_number} has non-OpenAlex paper_id={paper_id!r}"
            )
        if work_id != expected_work_id or openalex_work_id != expected_work_id:
            raise MLLabelDatasetError(
                f"transfer-gap labeled row {source_row_number} must keep OpenAlex W token in work_id/openalex_work_id"
            )
        if work_id.isdigit():
            raise MLLabelDatasetError(f"transfer-gap labeled row {source_row_number} has numeric work_id={work_id!r}")

        context_row = copy.deepcopy(sidecar_by_id[row_id])
        context_paper_id = _norm_ws(context_row.get("paper_id"))
        if context_paper_id and context_paper_id != paper_id:
            raise MLLabelDatasetError(f"transfer-gap sidecar paper_id mismatch for row_id={row_id}")
        context_openalex_work_id = _norm_ws(context_row.get("openalex_work_id"))
        if context_openalex_work_id and context_openalex_work_id != openalex_work_id:
            raise MLLabelDatasetError(f"transfer-gap sidecar openalex_work_id mismatch for row_id={row_id}")
        context_work_id = _norm_ws(context_row.get("work_id"))
        if context_work_id and context_work_id != work_id:
            raise MLLabelDatasetError(f"transfer-gap sidecar work_id mismatch for row_id={row_id}")

        rel_l = _raw_csv_or_none(row, "relevance_label")
        nov_l = _raw_csv_or_none(row, "novelty_label")
        br_l = _raw_csv_or_none(row, "bridge_like_label")
        notes = _raw_csv_or_none(row, "reviewer_notes")
        sample_seed = context_row.get("sample_seed", sidecar_provenance.get("sample_seed"))
        family = _norm_ws(context_row.get("family")) or None
        out: dict[str, Any] = {
            "dataset_version": dataset_version,
            "row_id": row_id,
            "paper_id": paper_id,
            "work_id": work_id,
            "title": _norm_ws(row.get("title")) or None,
            "year": _norm_ws(row.get("year")) or None,
            "citation_count": _norm_ws(row.get("citation_count")) or None,
            "source_slug": _norm_ws(row.get("source_slug")) or None,
            "ranking_run_id": None,
            "ranking_version": None,
            "corpus_snapshot_version": None,
            "embedding_version": None,
            "cluster_version": None,
            "family": family,
            "review_pool_variant": review_pool_variant,
            "rank": None,
            "experiment_rank": None,
            "source_worksheet_path": source_rel,
            "source_worksheet_sha256": source_sha,
            "source_row_number": source_row_number,
            "relevance_label": rel_l,
            "novelty_label": nov_l,
            "bridge_like_label": br_l,
            "reviewer_notes": notes,
            "label_provenance": "manual_review_worksheet_csv",
            "split": "audit_only",
            "good_or_acceptable": good_or_acceptable(rel_l),
            "surprising_or_useful": surprising_or_useful(nov_l),
            "bridge_like_yes_or_partial": bridge_like_yes_or_partial(br_l),
            "worksheet_version": worksheet_version,
            "sample_seed": sample_seed,
            "sample_reason": _norm_ws(row.get("sample_reason")) or context_row.get("sample_reason") or None,
            "cluster_id": _norm_ws(row.get("cluster_id")) or context_row.get("cluster_id") or None,
            "topics": _norm_ws(row.get("topics")) or None,
            "abstract_preview": _norm_ws(row.get("abstract_preview")) or None,
            "openalex_work_id": openalex_work_id,
            "internal_work_id": None,
            "transfer_gap_context": context_row,
        }
        transfer_rows.append(out)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    source_worksheets = list(base_payload.get("source_worksheets") or [])
    source_worksheets.append(source_rel)
    source_sha256 = dict(base_payload.get("source_worksheet_sha256") or {})
    source_sha256[source_rel] = source_sha
    row_counts_by_source = dict(base_metadata.get("row_counts_by_source") or {})
    row_counts_by_source[source_rel] = len(labeled_rows)
    included_by_source = dict(base_metadata.get("included_labeled_row_counts_by_source") or {})
    included_by_source[source_rel] = len(transfer_rows)
    blank_rows_by_source = dict(base_metadata.get("skipped_blank_row_counts_by_source") or {})
    blank_rows_by_source[source_rel] = 0
    skipped_blank_worksheets = list(base_metadata.get("skipped_blank_worksheets") or [])
    skipped_malformed_rows = copy.deepcopy(base_metadata.get("skipped_malformed_rows") or [])
    manual_review_dir_rel = str(base_metadata.get("manual_review_dir") or "docs/audit/manual-review")

    previous_ingests = {
        key: copy.deepcopy(value)
        for key, value in base_metadata.items()
        if key.endswith("_ingest") or key.startswith("previous_")
    }
    sidecar_inputs = sidecar_provenance.get("inputs") if isinstance(sidecar_provenance.get("inputs"), list) else []
    sidecar_label_dataset_sha256 = None
    for item in sidecar_inputs:
        if isinstance(item, dict) and item.get("name") == "label_dataset":
            sidecar_label_dataset_sha256 = item.get("sha256")
            break

    base_sha = sha256_file(base_path)
    extra_metadata = {
        **previous_ingests,
        "dataset_version": dataset_version,
        "previous_dataset_version": base_version,
        "previous_dataset_path": _repo_relative(base_path, repo_root=root),
        "previous_dataset_sha256": base_sha,
        "transfer_gap_v1_ingest": {
            "base_dataset_path": _repo_relative(base_path, repo_root=root),
            "base_dataset_sha256": base_sha,
            "blank_template_path": _repo_relative(blank_path, repo_root=root),
            "blank_template_sha256": sha256_file(blank_path),
            "labeled_worksheet_path": source_rel,
            "labeled_worksheet_sha256": source_sha,
            "context_sidecar_path": _repo_relative(sidecar_path, repo_root=root),
            "context_sidecar_sha256": sha256_file(sidecar_path),
            "conflict_policy_path": _repo_relative(conflict_path, repo_root=root),
            "conflict_policy_sha256": sha256_file(conflict_path),
            "worksheet_version": TRANSFER_GAP_REVIEW_V1_WORKSHEET_VERSION,
            "review_pool_variant": TRANSFER_GAP_REVIEW_POOL_VARIANT,
            "row_count_appended": len(transfer_rows),
            "base_row_count": len(base_rows),
            "row_id_policy": "CSV canonical; match sidecar after normalizing rows[] or map",
            "source_row_number_convention": "physical CSV line including header; first data row = 2",
            "label_distribution": _appended_label_distribution(transfer_rows),
            "sidecar_context_fields_preserved": "entire normalized sidecar row object preserved verbatim under transfer_gap_context",
            "sidecar_label_dataset_sha256": sidecar_label_dataset_sha256,
            "validation_summary": {
                "labeled_rows_found": len(labeled_rows),
                "sidecar_row_ids_matched": True,
                "blank_template_identity_columns_matched": True,
                "non_review_columns_unchanged": True,
                "review_columns_required_non_empty": True,
                "conflict_policy_recorded_as_provenance_only": True,
            },
        },
    }
    extra_caveats = [
        "Single-reviewer audit labels remain single-reviewer evidence unless a source artifact states otherwise.",
        "Transfer-gap labels are targeted audit evidence and are not representative validation samples.",
        "Transfer-gap rows remain audit_only and keep review_pool_variant=ml_transfer_gap_audit as a distinct pool.",
        "The nested transfer_gap_context is preserved for provenance only and is not label evidence.",
    ]
    return _assemble_dataset_payload_from_rows(
        dataset_version=dataset_version,
        generated_at=generated_at,
        source_worksheets=source_worksheets,
        source_sha256=source_sha256,
        all_rows=base_rows + transfer_rows,
        manual_review_dir_rel=manual_review_dir_rel,
        row_counts_by_source=row_counts_by_source,
        included_by_source=included_by_source,
        blank_rows_by_source=blank_rows_by_source,
        skipped_blank_worksheets=skipped_blank_worksheets,
        skipped_malformed_rows=skipped_malformed_rows,
        extra_metadata=extra_metadata,
        extra_caveats=extra_caveats,
    )


def build_ml_label_dataset_v9_fresh_hybrid_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    conflict_policy_path: Path,
    fresh_eval_surface_path: Path | None = None,
    dataset_version: str = "ml-label-dataset-v9",
) -> dict[str, Any]:
    """Build v9 as v8 rows unchanged plus the validated fresh-hybrid labeled worksheet."""
    from pipeline.ml_fresh_eval_labeling_worksheet_hybrid import stable_row_id as stable_fresh_hybrid_row_id

    root = repo_root.resolve()
    base_path = base_dataset_path.resolve()
    blank_path = blank_worksheet_path.resolve()
    labeled_path = labeled_worksheet_path.resolve()
    sidecar_path = context_sidecar_path.resolve()
    conflict_path = conflict_policy_path.resolve()
    input_paths = [base_path, blank_path, labeled_path, sidecar_path, conflict_path]
    surface_path = fresh_eval_surface_path.resolve() if fresh_eval_surface_path is not None else None
    if surface_path is not None:
        input_paths.append(surface_path)
    for path in input_paths:
        if not path.is_file():
            raise MLLabelDatasetError(f"required input not found: {path}")

    base_payload = _load_json_object(base_path)
    base_metadata = base_payload.get("metadata") if isinstance(base_payload.get("metadata"), dict) else {}
    base_version = _norm_ws(base_payload.get("dataset_version") or base_metadata.get("dataset_version"))
    if base_version != "ml-label-dataset-v8":
        raise MLLabelDatasetError(f"{base_path} dataset_version={base_version!r}; expected 'ml-label-dataset-v8'")
    base_rows_raw = base_payload.get("rows")
    if not isinstance(base_rows_raw, list):
        raise MLLabelDatasetError(f"{base_path} missing rows array")
    base_rows: list[dict[str, Any]] = copy.deepcopy(base_rows_raw)

    blank_fieldnames, blank_rows = _read_csv_rows(blank_path)
    labeled_fieldnames, labeled_rows = _read_csv_rows(labeled_path)
    if len(labeled_rows) != 120:
        raise MLLabelDatasetError(f"{labeled_path} must contain exactly 120 fresh-hybrid labeled data rows")

    _validate_labeled_matches_blank_template(
        blank_path=blank_path,
        blank_fieldnames=blank_fieldnames,
        blank_rows=blank_rows,
        labeled_path=labeled_path,
        labeled_fieldnames=labeled_fieldnames,
        labeled_rows=labeled_rows,
    )

    sidecar_payload, sidecar_by_id = _read_fresh_hybrid_sidecar_rows(sidecar_path)
    sidecar_metadata = sidecar_payload["metadata"]
    labeled_ids = {_norm_ws(r.get("row_id")) for r in labeled_rows}
    if set(sidecar_by_id) != labeled_ids:
        missing = sorted(labeled_ids - set(sidecar_by_id))
        extra = sorted(set(sidecar_by_id) - labeled_ids)
        raise MLLabelDatasetError(
            f"fresh-hybrid sidecar row_id set differs from labeled CSV; missing={missing[:5]}, extra={extra[:5]}"
        )

    sidecar_ws_version = _norm_ws(sidecar_metadata.get("worksheet_version"))
    if sidecar_ws_version != FRESH_HYBRID_WORKSHEET_VERSION:
        raise MLLabelDatasetError(
            f"fresh-hybrid sidecar worksheet_version={sidecar_ws_version!r} does not match "
            f"{FRESH_HYBRID_WORKSHEET_VERSION!r}"
        )
    sidecar_pool = _norm_ws(sidecar_metadata.get("review_pool_variant"))
    if sidecar_pool != FRESH_HYBRID_REVIEW_POOL_VARIANT:
        raise MLLabelDatasetError(
            f"fresh-hybrid sidecar review_pool_variant={sidecar_pool!r} does not match "
            f"{FRESH_HYBRID_REVIEW_POOL_VARIANT!r}"
        )
    seed = sidecar_metadata.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise MLLabelDatasetError("fresh-hybrid sidecar metadata.seed must be an integer")
    surface_summary = sidecar_metadata.get("source_surface_summary")
    if not isinstance(surface_summary, dict):
        raise MLLabelDatasetError("fresh-hybrid sidecar missing metadata.source_surface_summary")

    if surface_path is not None:
        surface_payload = _load_json_object(surface_path)
        surface_metadata = surface_payload.get("metadata")
        if not isinstance(surface_metadata, dict):
            raise MLLabelDatasetError(f"{surface_path} missing metadata object")
        if surface_metadata.get("surface_version") != FRESH_HYBRID_SURFACE_VERSION:
            raise MLLabelDatasetError(
                f"{surface_path} metadata.surface_version must be {FRESH_HYBRID_SURFACE_VERSION!r}"
            )
        candidate_pool = surface_payload.get("candidate_pool")
        surface_sha = _norm_ws(candidate_pool.get("candidate_work_set_sha256") if isinstance(candidate_pool, dict) else None)
        sidecar_surface_sha = _norm_ws(surface_summary.get("candidate_work_set_sha256"))
        if surface_sha != sidecar_surface_sha:
            raise MLLabelDatasetError(
                "fresh eval surface candidate_work_set_sha256 does not match fresh-hybrid sidecar source surface summary"
            )

    source_rel = _repo_relative(labeled_path, repo_root=root)
    source_sha = sha256_file(labeled_path)
    fresh_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_row_number, row in enumerate(labeled_rows, start=2):
        row_id = _norm_ws(row.get("row_id"))
        if row_id in seen:
            raise MLLabelDatasetError(f"duplicate fresh-hybrid labeled row_id {row_id}")
        seen.add(row_id)
        _validate_nonempty_allowed_labels(row, source_row_number=source_row_number)

        worksheet_version = _norm_ws(row.get("worksheet_version"))
        if worksheet_version != FRESH_HYBRID_WORKSHEET_VERSION:
            raise MLLabelDatasetError(
                f"fresh-hybrid labeled row {source_row_number} has worksheet_version={worksheet_version!r}"
            )
        review_pool_variant = _norm_ws(row.get("review_pool_variant"))
        if review_pool_variant != FRESH_HYBRID_REVIEW_POOL_VARIANT:
            raise MLLabelDatasetError(
                f"fresh-hybrid labeled row {source_row_number} has review_pool_variant={review_pool_variant!r}"
            )

        paper_id = _norm_ws(row.get("paper_id"))
        openalex_work_id = _norm_ws(row.get("openalex_work_id"))
        work_id = _norm_ws(row.get("work_id"))
        expected_work_id = paper_id_to_work_id(paper_id)
        if not expected_work_id:
            raise MLLabelDatasetError(
                f"fresh-hybrid labeled row {source_row_number} has non-OpenAlex paper_id={paper_id!r}"
            )
        if work_id != expected_work_id or openalex_work_id != expected_work_id:
            raise MLLabelDatasetError(
                f"fresh-hybrid labeled row {source_row_number} must keep OpenAlex W token in work_id/openalex_work_id"
            )
        if work_id.isdigit():
            raise MLLabelDatasetError(f"fresh-hybrid labeled row {source_row_number} has numeric work_id={work_id!r}")

        context_row = copy.deepcopy(sidecar_by_id[row_id])
        context_canonical = _norm_ws(context_row.get("canonical_openalex_work_id"))
        if context_canonical != expected_work_id:
            raise MLLabelDatasetError(f"fresh-hybrid sidecar canonical_openalex_work_id mismatch for row_id={row_id}")
        expected_row_id = stable_fresh_hybrid_row_id(
            worksheet_version=sidecar_ws_version,
            seed=seed,
            canonical_openalex_work_id=context_canonical,
        )
        if row_id != expected_row_id:
            raise MLLabelDatasetError(
                f"fresh-hybrid labeled row {source_row_number} row_id does not match worksheet_version|seed|canonical_openalex_work_id"
            )
        context_paper_id = _norm_ws(context_row.get("paper_id"))
        if context_paper_id and paper_id_to_work_id(context_paper_id) != expected_work_id:
            raise MLLabelDatasetError(f"fresh-hybrid sidecar paper_id mismatch for row_id={row_id}")

        rel_l = _raw_csv_or_none(row, "relevance_label")
        nov_l = _raw_csv_or_none(row, "novelty_label")
        br_l = _raw_csv_or_none(row, "bridge_like_label")
        notes = _raw_csv_or_none(row, "reviewer_notes")
        rank_in_family = row.get("rank_in_family", "")
        out: dict[str, Any] = {
            "dataset_version": dataset_version,
            "row_id": row_id,
            "paper_id": paper_id,
            "work_id": work_id,
            "title": row.get("title", ""),
            "year": row.get("year", ""),
            "citation_count": row.get("citation_count", ""),
            "source_slug": row.get("source_slug", ""),
            "topics": row.get("topics", ""),
            "abstract_preview": row.get("abstract_preview", ""),
            "ranking_run_id": row.get("ranking_run_id", ""),
            "ranking_version": None,
            "corpus_snapshot_version": surface_summary.get("snapshot_version"),
            "embedding_version": None,
            "cluster_version": None,
            "family": row.get("family", ""),
            "review_pool_variant": review_pool_variant,
            "rank": rank_in_family,
            "rank_in_family": rank_in_family,
            "experiment_rank": None,
            "final_score": row.get("final_score", ""),
            "sample_reason": row.get("sample_reason", ""),
            "source_worksheet_path": source_rel,
            "source_worksheet_sha256": source_sha,
            "source_row_number": source_row_number,
            "relevance_label": rel_l,
            "novelty_label": nov_l,
            "bridge_like_label": br_l,
            "reviewer_notes": notes,
            "label_provenance": "manual_review_worksheet_csv",
            "split": "audit_only",
            "good_or_acceptable": good_or_acceptable(rel_l),
            "surprising_or_useful": surprising_or_useful(nov_l),
            "bridge_like_yes_or_partial": bridge_like_yes_or_partial(br_l),
            "worksheet_version": worksheet_version,
            "sample_seed": seed,
            "openalex_work_id": openalex_work_id,
            "fresh_hybrid_context": context_row,
        }
        fresh_rows.append(out)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    source_worksheets = list(base_payload.get("source_worksheets") or [])
    source_worksheets.append(source_rel)
    source_sha256 = dict(base_payload.get("source_worksheet_sha256") or {})
    source_sha256[source_rel] = source_sha
    row_counts_by_source = dict(base_metadata.get("row_counts_by_source") or {})
    row_counts_by_source[source_rel] = len(labeled_rows)
    included_by_source = dict(base_metadata.get("included_labeled_row_counts_by_source") or {})
    included_by_source[source_rel] = len(fresh_rows)
    blank_rows_by_source = dict(base_metadata.get("skipped_blank_row_counts_by_source") or {})
    blank_rows_by_source[source_rel] = 0
    skipped_blank_worksheets = list(base_metadata.get("skipped_blank_worksheets") or [])
    skipped_malformed_rows = copy.deepcopy(base_metadata.get("skipped_malformed_rows") or [])
    manual_review_dir_rel = str(base_metadata.get("manual_review_dir") or "docs/audit/manual-review")

    previous_ingests = {
        key: copy.deepcopy(value)
        for key, value in base_metadata.items()
        if key.endswith("_ingest") or key.startswith("previous_")
    }
    base_sha = sha256_file(base_path)
    blank_sha = sha256_file(blank_path)
    sidecar_sha = sha256_file(sidecar_path)
    conflict_sha = sha256_file(conflict_path)
    inputs = [
        _input_record("base_dataset", base_path, repo_root=root),
        _input_record("blank_worksheet", blank_path, repo_root=root),
        _input_record("labeled_worksheet", labeled_path, repo_root=root),
        _input_record("context_sidecar", sidecar_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    fresh_eval_surface_input: dict[str, str] | None = None
    if surface_path is not None:
        fresh_eval_surface_input = _input_record("fresh_eval_surface", surface_path, repo_root=root)
        inputs.append(fresh_eval_surface_input)

    label_distribution = _appended_label_distribution(fresh_rows)
    good_positive = sum(1 for row in fresh_rows if row.get("good_or_acceptable") is True)
    good_negative = sum(1 for row in fresh_rows if row.get("good_or_acceptable") is False)
    extra_metadata = {
        **previous_ingests,
        "dataset_version": dataset_version,
        "previous_dataset_version": base_version,
        "previous_dataset_path": _repo_relative(base_path, repo_root=root),
        "previous_dataset_sha256": base_sha,
        "inputs": inputs,
        "fresh_hybrid_v1_ingest": {
            "row_count_appended": len(fresh_rows),
            "base_row_count": len(base_rows),
            "worksheet_version": FRESH_HYBRID_WORKSHEET_VERSION,
            "review_pool_variant": FRESH_HYBRID_REVIEW_POOL_VARIANT,
            "row_id_policy": {
                "source": "CSV canonical; sidecar parity required",
                "formula": "sha256(f\"{worksheet_version}|{seed}|{canonical_openalex_work_id}\")",
                "stable_row_id_formula_validated": True,
                "csv_row_id_set_equals_sidecar_row_id_set": True,
            },
            "source_row_number_convention": "physical CSV line including header; first data row = 2",
            "label_distribution": label_distribution,
            "good_or_acceptable_positive_count": good_positive,
            "good_or_acceptable_negative_count": good_negative,
            "fresh_hybrid_context_fields_preserved": "entire sidecar row object preserved verbatim under fresh_hybrid_context",
            "blank_worksheet_path": _repo_relative(blank_path, repo_root=root),
            "blank_worksheet_sha256": blank_sha,
            "labeled_worksheet_path": source_rel,
            "labeled_worksheet_sha256": source_sha,
            "context_sidecar_path": _repo_relative(sidecar_path, repo_root=root),
            "context_sidecar_sha256": sidecar_sha,
            "conflict_policy_path": _repo_relative(conflict_path, repo_root=root),
            "conflict_policy_sha256": conflict_sha,
            "fresh_eval_surface_path": fresh_eval_surface_input["path"] if fresh_eval_surface_input else None,
            "fresh_eval_surface_sha256": fresh_eval_surface_input["sha256"] if fresh_eval_surface_input else None,
            "previous_dataset_version": base_version,
            "previous_dataset_path": _repo_relative(base_path, repo_root=root),
            "previous_dataset_sha256": base_sha,
            "source_surface_summary": copy.deepcopy(surface_summary),
            "validation_summary": {
                "labeled_rows_found": len(labeled_rows),
                "blank_and_labeled_row_id_sets_matched": True,
                "non_review_columns_unchanged": True,
                "review_columns_required_non_empty": True,
                "closed_label_sets_validated": True,
                "sidecar_row_ids_matched": True,
                "stable_row_id_formula_validated": True,
                "fresh_eval_surface_sha_matched": surface_path is not None,
                "conflict_policy_recorded_as_provenance_only": True,
            },
        },
    }
    extra_caveats = [
        "Fresh hybrid labels are single-reviewer audit labels.",
        "Fresh hybrid rows remain audit_only and do not define a train/eval split.",
        "This dataset versioning step is not validation and does not run ranking, scoring, training, or embeddings.",
        "No production or API behavior changes are authorized by this artifact.",
    ]
    return _assemble_dataset_payload_from_rows(
        dataset_version=dataset_version,
        generated_at=generated_at,
        source_worksheets=source_worksheets,
        source_sha256=source_sha256,
        all_rows=base_rows + fresh_rows,
        manual_review_dir_rel=manual_review_dir_rel,
        row_counts_by_source=row_counts_by_source,
        included_by_source=included_by_source,
        blank_rows_by_source=blank_rows_by_source,
        skipped_blank_worksheets=skipped_blank_worksheets,
        skipped_malformed_rows=skipped_malformed_rows,
        extra_metadata=extra_metadata,
        extra_caveats=extra_caveats,
    )


def markdown_from_ml_label_dataset(payload: dict[str, Any]) -> str:
    meta = payload["metadata"]
    dup = meta["duplicate_paper_id_report"]
    conf = meta["conflicting_label_report"]
    dconf = meta["derived_target_conflict_report"]
    inferred_n = meta.get("inferred_family_count", 0)
    v5_ingest = meta.get("reviewer_blind_v2_ingest")
    v6_ingest = meta.get("hard_negative_v1_ingest")
    v7_ingest = meta.get("external_near_miss_v1_ingest")
    v8_ingest = meta.get("transfer_gap_v1_ingest")
    v9_ingest = meta.get("fresh_hybrid_v1_ingest")
    if isinstance(v9_ingest, dict):
        regenerate_command = (
            "Machine-readable export: regenerate via `python -m pipeline.cli "
            "ml-label-dataset-v9-fresh-hybrid-ingest "
            f"--base-dataset {v9_ingest['previous_dataset_path']} "
            f"--blank-worksheet {v9_ingest['blank_worksheet_path']} "
            f"--labeled-worksheet {v9_ingest['labeled_worksheet_path']} "
            f"--context-sidecar {v9_ingest['context_sidecar_path']} "
            f"--conflict-policy {v9_ingest['conflict_policy_path']} --output <path>.json`."
        )
    elif isinstance(v8_ingest, dict):
        regenerate_command = (
            "Machine-readable export: regenerate via `python -m pipeline.cli "
            "ml-label-dataset-v8-transfer-gap-ingest "
            f"--base-dataset {v8_ingest['base_dataset_path']} "
            f"--blank-worksheet {v8_ingest['blank_template_path']} "
            f"--labeled-worksheet {v8_ingest['labeled_worksheet_path']} "
            f"--context-sidecar {v8_ingest['context_sidecar_path']} "
            f"--conflict-policy {v8_ingest['conflict_policy_path']} --output <path>.json`."
        )
    elif isinstance(v7_ingest, dict):
        regenerate_command = (
            "Machine-readable export: regenerate via `python -m pipeline.cli "
            "ml-label-dataset-v7-external-near-miss-ingest "
            f"--base-dataset {v7_ingest['base_dataset_path']} "
            f"--blank-worksheet {v7_ingest['blank_template_path']} "
            f"--labeled-worksheet {v7_ingest['labeled_worksheet_path']} "
            f"--context-sidecar {v7_ingest['context_sidecar_path']} "
            f"--conflict-policy {v7_ingest['conflict_policy_path']} --output <path>.json`."
        )
    elif isinstance(v6_ingest, dict):
        regenerate_command = (
            "Machine-readable export: regenerate via `python -m pipeline.cli "
            "ml-label-dataset-v6-hard-negative-ingest "
            f"--base-dataset {v6_ingest['base_dataset_path']} "
            f"--blank-worksheet {v6_ingest['blank_template_path']} "
            f"--labeled-worksheet {v6_ingest['labeled_worksheet_path']} "
            f"--context-sidecar {v6_ingest['context_sidecar_path']} --output <path>.json`."
        )
    elif isinstance(v5_ingest, dict):
        regenerate_command = (
            "Machine-readable export: regenerate via `python -m pipeline.cli "
            "ml-label-dataset-v5-reviewer-blind-ingest "
            f"--base-dataset {v5_ingest['base_dataset_path']} "
            f"--blank-worksheet {v5_ingest['blank_template_path']} "
            f"--labeled-worksheet {v5_ingest['labeled_worksheet_path']} "
            f"--context-sidecar {v5_ingest['context_sidecar_path']} --output <path>.json`."
        )
    else:
        regenerate_command = (
            "Machine-readable export: regenerate via `python -m pipeline.cli "
            f"ml-label-dataset --dataset-version {payload['dataset_version']} --output <path>.json`."
        )
    lines = [
        f"# Manual label dataset ({payload['dataset_version']})",
        "",
        "## What this dataset is",
        "",
        "A versioned export of **explicit manual reviewer labels** taken from Research Radar **offline audit CSV worksheets** "
        "under `docs/audit/manual-review/`. Each row is one labeled observation of one paper in a specific ranking or "
        "experiment-review context, with file-level provenance (path, SHA-256, spreadsheet row number). "
        "It exists so future work can run **offline** ranking or learning-to-rank experiments with measurable labels that were "
        "**not invented for ML**.",
        "",
        "## What this dataset is not",
        "",
        "- It is **not** model training output and **not** an automated relevance oracle.",
        "- It is **not** a substitute for live product metrics.",
        "- It does **not** define train/dev/test partitions (see `split`).",
        "",
        "## Label sources",
        "",
        "Worksheets are CSV exports produced during manual audit. Only rows with at least one non-empty value among "
        "`relevance_label`, `novelty_label`, or `bridge_like_label` are included. Free-text `reviewer_notes` alone does not qualify.",
        "",
        "### Source files",
        "",
        *[f"- `{p}`" for p in payload["source_worksheets"]],
        "",
        "### Skipped blank worksheets",
        "",
        (
            "None: every worksheet with a label schema contributed at least one labeled row."
            if not meta["skipped_blank_worksheets"]
            else "\n".join(f"- `{p}`" for p in meta["skipped_blank_worksheets"])
        ),
        "",
        "## Derived targets",
        "",
        "These are **deterministic functions** of the three manual label columns only (no inference from scores or titles):",
        "",
        "| Column | Rule |",
        "|--------|------|",
        "| `good_or_acceptable` | `true` if `relevance_label` is one of good, acceptable; `false` if one of miss, irrelevant; else `null` |",
        "| `surprising_or_useful` | `true` if `novelty_label` is one of surprising, useful; `false` if one of obvious, not_useful, neither; else `null` |",
        "| `bridge_like_yes_or_partial` | `true` if `bridge_like_label` is one of yes, partial; `false` if `no`; `null` if missing, empty, `not_applicable`, or unknown token |",
        "",
        "## Known biases",
        "",
        "- **Single reviewer** per audit pass unless a source file states otherwise.",
        "- **Top-k / worksheet selection**: labels exist for papers that reached audit worksheets, not a random sample of the corpus.",
        "- **Family-specific contexts** (bridge, emerging, undercited, experiment deltas) are not interchangeable without careful experimental design.",
        "- **Transfer-gap targeted samples** are not representative validation data.",
        "",
        "## Family inference (worksheet context)",
        "",
        "Some bridge experiment review CSVs (weight delta review, objective delta / eligibility delta / one-row review) "
        "do not include a `family` column. For those files only, `family` is set to **`bridge`** from worksheet naming "
        "convention so downstream joins can treat rows like other bridge-family audits. "
        "This does **not** change any reviewer label columns.",
        "",
        f"- **Rows with inferred `family`:** {inferred_n} (per-source counts: `metadata.inferred_family_by_source`).",
        "",
        "## Blind snapshot context fields",
        "",
        "Rows from worksheets with `review_pool_variant=ml_blind_snapshot_audit` keep `family=null` (these papers were "
        "**not** sampled from a recommendation family's top-k). To support a blind-source family-context diagnostic, "
        "these rows additionally preserve worksheet-level context when the worksheet provides it: "
        "`worksheet_version`, `sample_seed`, `sample_reason`, `cluster_id`, `topics`, `abstract_preview`, "
        "`ranking_context_family_scores_json`, `ranking_context_family_ranks_json`, `openalex_work_id`, "
        "and `internal_work_id`. Reviewer-blind v2 rows also keep the full sidecar row in nested "
        "`blind_snapshot_context`, keyed by the canonical worksheet `row_id`, so hidden score/rank provenance is "
        "not lost when the reviewer CSV intentionally omits it. These context fields are **not labels** and must "
        "not be treated as family-selected ranking outputs.",
        "",
        "## Hard-negative context fields",
        "",
        "Rows from worksheets with `review_pool_variant=ml_hard_negative_audit` remain a distinct audit pool for "
        "negative and near-miss relevance-boundary labels. They keep `family=null` unless a worksheet explicitly "
        "provides a family column, and preserve sidecar provenance in nested `hard_negative_context`. That context "
        "may include hidden score/rank features and selection signals, but derived targets are still computed only "
        "from explicit manual labels.",
        "",
        "## External near-miss context fields",
        "",
        "Rows from worksheets with `review_pool_variant=ml_external_near_miss_audit` remain a distinct audit pool for "
        "negative-boundary labels acquired outside the current curated corpus snapshot. They have `split=audit_only`, "
        "`family=null` unless a worksheet explicitly provides a family column, and no top-level ranking or corpus snapshot "
        "identity because they were not sampled from a persisted ranking run. The nested `external_near_miss_context` "
        "preserves OpenAlex query provenance, outside-217 exclusion checks, and reviewer-hidden acquisition metadata. "
        "Those context fields are **not labels** and must not be pooled with blind or hard-negative rows unless a later "
        "experiment explicitly says so.",
        "",
        "## Transfer-gap context fields",
        "",
        "Rows from worksheets with `review_pool_variant=ml_transfer_gap_audit` remain a distinct targeted audit pool for "
        "transfer-gap and sparse-pool labeling. They have `split=audit_only`, keep `family=null` unless the sidecar "
        "explicitly maps a family field, and preserve the full row-id keyed sidecar object under nested "
        "`transfer_gap_context`. The sidecar may describe gap priority, target hint, source query, or old evidence "
        "pool being addressed; those context fields are **not labels** and do not imply production ranking readiness.",
        "",
        "## Fresh hybrid eval context fields",
        "",
        "Rows from worksheets with `review_pool_variant=ml_fresh_hybrid_eval_v1` are manual labels for the fresh "
        "hybrid confirmatory path. They have `split=audit_only`, preserve ranking-run context from the worksheet, "
        "and keep the full row-id keyed sidecar object under nested `fresh_hybrid_context`. The sidecar provides "
        "candidate-surface provenance only; it is not label evidence and does not authorize validation, shadow, or production.",
        "",
        *(
            [
                "### Fresh hybrid v1 ingest",
                "",
                f"- **Rows appended:** {v9_ingest['row_count_appended']}",
                "- **Legacy rows:** copied from v8 unchanged field-for-field, including their existing per-row `dataset_version` values.",
                f"- **Source row numbering:** {v9_ingest['source_row_number_convention']}.",
                f"- **Raw relevance distribution:** `{v9_ingest['label_distribution']['relevance_label']}`",
                f"- **Raw novelty distribution:** `{v9_ingest['label_distribution']['novelty_label']}`",
                f"- **Raw bridge-like distribution:** `{v9_ingest['label_distribution']['bridge_like_label']}`",
                f"- **good_or_acceptable positives / negatives:** {v9_ingest['good_or_acceptable_positive_count']} / {v9_ingest['good_or_acceptable_negative_count']}",
                "- **Next step:** after a follow-up accepts `ml-label-dataset-v9`, rerun `ml-fresh-eval-surface-hybrid-materialize` with `--label-dataset ../../docs/audit/ml-label-dataset-v9.json` to measure remaining policy thresholds.",
                "",
            ]
            if isinstance(v9_ingest, dict)
            else []
        ),
        *(
            [
                "### Transfer-gap v1 ingest",
                "",
                f"- **Rows appended:** {v8_ingest['row_count_appended']}",
                "- **Legacy rows:** copied from v7 unchanged field-for-field, including their existing per-row `dataset_version` values.",
                f"- **Source row numbering:** {v8_ingest.get('source_row_number_convention', 'physical CSV line including header; first data row = 2')}.",
                f"- **Raw relevance distribution:** `{v8_ingest.get('label_distribution', {}).get('relevance_label', {})}`",
                f"- **Raw novelty distribution:** `{v8_ingest.get('label_distribution', {}).get('novelty_label', {})}`",
                f"- **Raw bridge-like distribution:** `{v8_ingest.get('label_distribution', {}).get('bridge_like_label', {})}`",
                "",
            ]
            if isinstance(v8_ingest, dict)
            else []
        ),
        "## Duplicate and conflicting labels",
        "",
        f"- **Duplicate `paper_id` count** (papers with more than one retained row): {dup['duplicate_paper_id_count']}",
        f"- **Conflicting raw label groups** (same `paper_id`, same label field, multiple distinct non-empty values): {conf['conflicting_label_count']}",
        "",
        "**Duplicate rows:** the same `paper_id` may appear in multiple worksheets or ranks. Each row remains a **separate "
        "labeled observation**; nothing in this export merges or collapses duplicates - use `row_id` and provenance fields "
        "when designing offline baselines.",
        "",
        "## Derived target conflicts",
        "",
        "For each derived boolean target (`good_or_acceptable`, `surprising_or_useful`, `bridge_like_yes_or_partial`), "
        "we group by `paper_id` and compare non-null values only. A conflict is recorded when the same paper has **both** "
        "`true` and `false` for that target across rows (e.g. `surprising` vs `obvious` both map into `surprising_or_useful` "
        "and therefore do **not** count as a conflict on that target).",
        "",
        f"- **Derived target conflict count:** {dconf['derived_target_conflict_count']}",
        "",
        "## Skipped blank rows",
        "",
        f"Total data rows skipped for blank label scaffold: **{meta['total_blank_rows_skipped']}** "
        "(per-source counts are in JSON metadata `skipped_blank_row_counts_by_source`).",
        "",
        "## Split field (`audit_only`)",
        "",
        "Every row has `split: \"audit_only\"` to mark that these observations come from **audit worksheets**, not from a "
        "deliberately constructed ML split. Future experiments must assign splits explicitly to avoid leakage.",
        "",
        "## Using this in future offline experiments",
        "",
        "- Join rows to frozen ranking outputs or corpus snapshots using `ranking_run_id`, `ranking_version`, `corpus_snapshot_version`, "
        "`paper_id` / `work_id`, and ranks as appropriate.",
        "- Treat duplicate `paper_id` entries as **separate contexts** unless you define an aggregation policy.",
        "- Use derived targets only when the corresponding raw label is in the documented closed sets.",
        "",
        "## Caveats (verbatim)",
        "",
        *[f"> {c}\n" for c in (payload.get("caveats") or VERBATIM_CAVEATS)],
        "",
        "## JSON artifact",
        "",
        regenerate_command,
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_ml_label_dataset(
    *,
    repo_root: Path,
    json_path: Path,
    markdown_path: Path | None,
    manual_review_dir: Path | None = None,
    dataset_version: str | None = None,
) -> dict[str, Any]:
    payload = build_ml_label_dataset(
        repo_root=repo_root,
        manual_review_dir=manual_review_dir,
        dataset_version=dataset_version,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_dataset(payload), encoding="utf-8")
    return payload


def write_ml_label_dataset_v5_reviewer_blind_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    dataset_version: str = "ml-label-dataset-v5",
) -> dict[str, Any]:
    payload = build_ml_label_dataset_v5_reviewer_blind_ingest(
        repo_root=repo_root,
        base_dataset_path=base_dataset_path,
        blank_worksheet_path=blank_worksheet_path,
        labeled_worksheet_path=labeled_worksheet_path,
        context_sidecar_path=context_sidecar_path,
        dataset_version=dataset_version,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_dataset(payload), encoding="utf-8")
    return payload


def write_ml_label_dataset_v6_hard_negative_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    dataset_version: str = "ml-label-dataset-v6",
) -> dict[str, Any]:
    payload = build_ml_label_dataset_v6_hard_negative_ingest(
        repo_root=repo_root,
        base_dataset_path=base_dataset_path,
        blank_worksheet_path=blank_worksheet_path,
        labeled_worksheet_path=labeled_worksheet_path,
        context_sidecar_path=context_sidecar_path,
        dataset_version=dataset_version,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_dataset(payload), encoding="utf-8")
    return payload


def write_ml_label_dataset_v7_external_near_miss_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    conflict_policy_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    dataset_version: str = "ml-label-dataset-v7",
) -> dict[str, Any]:
    payload = build_ml_label_dataset_v7_external_near_miss_ingest(
        repo_root=repo_root,
        base_dataset_path=base_dataset_path,
        blank_worksheet_path=blank_worksheet_path,
        labeled_worksheet_path=labeled_worksheet_path,
        context_sidecar_path=context_sidecar_path,
        conflict_policy_path=conflict_policy_path,
        dataset_version=dataset_version,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_dataset(payload), encoding="utf-8")
    return payload


def write_ml_label_dataset_v8_transfer_gap_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    conflict_policy_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    dataset_version: str = "ml-label-dataset-v8",
) -> dict[str, Any]:
    payload = build_ml_label_dataset_v8_transfer_gap_ingest(
        repo_root=repo_root,
        base_dataset_path=base_dataset_path,
        blank_worksheet_path=blank_worksheet_path,
        labeled_worksheet_path=labeled_worksheet_path,
        context_sidecar_path=context_sidecar_path,
        conflict_policy_path=conflict_policy_path,
        dataset_version=dataset_version,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_dataset(payload), encoding="utf-8")
    return payload


def write_ml_label_dataset_v9_fresh_hybrid_ingest(
    *,
    repo_root: Path,
    base_dataset_path: Path,
    blank_worksheet_path: Path,
    labeled_worksheet_path: Path,
    context_sidecar_path: Path,
    conflict_policy_path: Path,
    json_path: Path,
    markdown_path: Path | None,
    fresh_eval_surface_path: Path | None = None,
    dataset_version: str = "ml-label-dataset-v9",
) -> dict[str, Any]:
    payload = build_ml_label_dataset_v9_fresh_hybrid_ingest(
        repo_root=repo_root,
        base_dataset_path=base_dataset_path,
        blank_worksheet_path=blank_worksheet_path,
        labeled_worksheet_path=labeled_worksheet_path,
        context_sidecar_path=context_sidecar_path,
        conflict_policy_path=conflict_policy_path,
        fresh_eval_surface_path=fresh_eval_surface_path,
        dataset_version=dataset_version,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_dataset(payload), encoding="utf-8")
    return payload
