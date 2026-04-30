"""Build a versioned manual-label dataset JSON (+ Markdown) from audit review CSVs.

Read-only: no database, no ranking. Intended for offline experiment scaffolding only.
"""

from __future__ import annotations

import csv
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


def markdown_from_ml_label_dataset(payload: dict[str, Any]) -> str:
    meta = payload["metadata"]
    dup = meta["duplicate_paper_id_report"]
    conf = meta["conflicting_label_report"]
    dconf = meta["derived_target_conflict_report"]
    inferred_n = meta.get("inferred_family_count", 0)
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
        "and `internal_work_id`. These context fields are **not labels** and must not be treated as family-selected "
        "ranking outputs.",
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
        f"Machine-readable export: regenerate via `python -m pipeline.cli ml-label-dataset --dataset-version {payload['dataset_version']} --output <path>.json`.",
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
