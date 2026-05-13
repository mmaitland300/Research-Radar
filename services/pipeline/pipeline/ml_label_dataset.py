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
            raise MLLabelDatasetError(f"v2 labeled CSV row {source_row_number} has blank {field}")
        if value not in allowed:
            raise MLLabelDatasetError(
                f"v2 labeled CSV row {source_row_number} has unsupported {field}={row.get(field)!r}"
            )
    if not _norm_ws(row.get("reviewer_notes")):
        raise MLLabelDatasetError(f"v2 labeled CSV row {source_row_number} has blank reviewer_notes")


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


def markdown_from_ml_label_dataset(payload: dict[str, Any]) -> str:
    meta = payload["metadata"]
    dup = meta["duplicate_paper_id_report"]
    conf = meta["conflicting_label_report"]
    dconf = meta["derived_target_conflict_report"]
    inferred_n = meta.get("inferred_family_count", 0)
    v5_ingest = meta.get("reviewer_blind_v2_ingest")
    if isinstance(v5_ingest, dict):
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
        *[f"> {c}\n" for c in VERBATIM_CAVEATS],
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
