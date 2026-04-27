"""Validate and summarize a filled recommendation review worksheet CSV (human labels only)."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.recommendation_review_worksheet import WORKSHEET_COLUMNS

RELEVANCE_ALLOWED: frozenset[str] = frozenset(
    {"good", "acceptable", "miss", "irrelevant"}
)
NOVELTY_ALLOWED: frozenset[str] = frozenset(
    {"obvious", "useful", "surprising", "not_useful"}
)
BRIDGE_LIKE_ALLOWED: frozenset[str] = frozenset(
    {"yes", "partial", "no", "not_applicable"}
)


class ReviewSummaryError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class _RowIssue:
    data_row: int
    paper_id: str
    column: str
    detail: str


def _norm_cell(raw: str | None) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def _load_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ReviewSummaryError(f"Input file not found: {path}", code=2)
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ReviewSummaryError("CSV has no header row.", code=2)
    header = [h.strip() if h else "" for h in reader.fieldnames]
    missing = [c for c in WORKSHEET_COLUMNS if c not in header]
    if missing:
        raise ReviewSummaryError(
            f"Worksheet is missing required columns: {', '.join(missing)}",
            code=2,
        )
    out: list[dict[str, str]] = []
    for row in reader:
        if row is None:
            continue
        if not any(_norm_cell(v) for k, v in row.items() if k):
            continue
        out.append({c: _norm_cell(row.get(c, "")) for c in WORKSHEET_COLUMNS})
    if not out:
        raise ReviewSummaryError("No data rows in worksheet.", code=2)
    return out


def _validate_label(
    col: str,
    value: str,
    allowed: frozenset[str],
    *,
    data_row: int,
    paper_id: str,
) -> _RowIssue | None:
    if not value:
        return _RowIssue(
            data_row=data_row,
            paper_id=paper_id,
            column=col,
            detail="blank or whitespace-only",
        )
    if value not in allowed:
        return _RowIssue(
            data_row=data_row,
            paper_id=paper_id,
            column=col,
            detail=f"invalid value {value!r} (expected one of: {', '.join(sorted(allowed))})",
        )
    return None


def _collect_issues(rows: list[dict[str, str]]) -> list[_RowIssue]:
    issues: list[_RowIssue] = []
    for i, r in enumerate(rows, start=1):
        pid = r.get("paper_id", "") or "(no paper_id)"
        for col, allowed in (
            ("relevance_label", RELEVANCE_ALLOWED),
            ("novelty_label", NOVELTY_ALLOWED),
            ("bridge_like_label", BRIDGE_LIKE_ALLOWED),
        ):
            v = r.get(col, "")
            p = _validate_label(col, v, allowed, data_row=i, paper_id=pid)
            if p:
                issues.append(p)
    return issues


def _format_issue_line(it: _RowIssue) -> str:
    return (
        f"data row {it.data_row} (paper_id={it.paper_id!r}): "
        f"column {it.column!r} is {it.detail}"
    )


def _unique_values(rows: list[dict[str, str]], col: str) -> list[str]:
    seen: dict[str, None] = {}
    for r in rows:
        v = r.get(col, "")
        if v and v not in seen:
            seen[v] = None
    return sorted(seen.keys())


def build_recommendation_review_summary(
    rows: list[dict[str, str]],
    *,
    input_path: Path,
    allow_incomplete: bool,
) -> dict[str, Any]:
    issues = _collect_issues(rows)
    is_complete = len(issues) == 0
    if not is_complete and not allow_incomplete:
        lines = "\n".join(_format_issue_line(x) for x in issues)
        msg = f"Worksheet label validation failed ({len(issues)} issue(s)):\n{lines}"
        raise ReviewSummaryError(msg, code=2)

    warnings: list[str] = []
    if not is_complete and allow_incomplete:
        warnings.append(
            f"incomplete or invalid labels on {len(issues)} field(s); "
            "is_complete is false; metrics use all rows and may be misleading"
        )
    rids = _unique_values(rows, "ranking_run_id")
    if len(rids) > 1:
        warnings.append(
            "multiple ranking_run_id values observed: " + ", ".join(rids)
        )
    snaps = _unique_values(rows, "corpus_snapshot_version")
    if len(snaps) > 1:
        warnings.append(
            "multiple corpus_snapshot_version values observed: " + ", ".join(snaps)
        )
    fams = _unique_values(rows, "family")
    if len(fams) > 1:
        warnings.append("multiple family values observed: " + ", ".join(fams))
    embs = _unique_values(rows, "embedding_version")
    if len(embs) > 1:
        warnings.append(
            "multiple embedding_version values observed: " + ", ".join(embs)
        )
    clusts = _unique_values(rows, "cluster_version")
    if len(clusts) > 1:
        warnings.append(
            "multiple cluster_version values observed: " + ", ".join(clusts)
        )

    row_count = len(rows)

    def count_map(allowed: frozenset[str], col: str) -> dict[str, int]:
        return {a: sum(1 for r in rows if r.get(col) == a) for a in sorted(allowed)}

    label_counts = {
        "relevance_label": count_map(RELEVANCE_ALLOWED, "relevance_label"),
        "novelty_label": count_map(NOVELTY_ALLOWED, "novelty_label"),
        "bridge_like_label": count_map(BRIDGE_LIKE_ALLOWED, "bridge_like_label"),
    }

    rel_good = sum(1 for r in rows if r.get("relevance_label") == "good")
    rel_goa = sum(
        1
        for r in rows
        if r.get("relevance_label") in ("good", "acceptable")
    )
    novel_su = sum(
        1
        for r in rows
        if r.get("novelty_label") in ("surprising", "useful")
    )
    bl_num = sum(
        1
        for r in rows
        if r.get("bridge_like_label") in ("yes", "partial")
    )
    bl_denom = sum(
        1 for r in rows if r.get("bridge_like_label") != "not_applicable"
    )
    bridge_share: float | None
    if bl_denom == 0:
        bridge_share = None
    else:
        bridge_share = bl_num / bl_denom

    metrics: dict[str, Any] = {
        "precision_at_k_good_only": rel_good / row_count,
        "precision_at_k_good_or_acceptable": rel_goa / row_count,
        "bridge_like_yes_or_partial_share": bridge_share,
        "surprising_or_useful_share": novel_su / row_count,
    }

    return {
        "input_path": str(input_path.resolve()),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": row_count,
        "is_complete": is_complete,
        "ranking_run_id": rids,
        "ranking_version": _unique_values(rows, "ranking_version"),
        "corpus_snapshot_version": snaps,
        "embedding_version": _unique_values(rows, "embedding_version"),
        "cluster_version": _unique_values(rows, "cluster_version"),
        "family": fams,
        "label_counts": label_counts,
        "metrics": metrics,
        "warnings": warnings,
    }


def _markdown_from_summary(summary: dict[str, Any]) -> str:
    lines: list[str] = [
        "# Recommendation review summary",
        "",
        f"- **Input:** `{summary['input_path']}`",
        f"- **Generated (UTC):** {summary['generated_at']}",
        f"- **Rows:** {summary['row_count']}",
        f"- **Complete labels:** {summary['is_complete']}",
        "",
        "## Provenance (observed distinct values)",
        "",
    ]
    for key in (
        "ranking_run_id",
        "ranking_version",
        "corpus_snapshot_version",
        "embedding_version",
        "cluster_version",
        "family",
    ):
        lines.append(f"- **{key}:** {', '.join(summary.get(key) or ['(empty)'])}")
    lines.extend(
        [
            "",
            "## Label counts (human labels only)",
            "",
        ]
    )
    for col, m in summary["label_counts"].items():
        lines.append(f"### {col}")
        for k, v in m.items():
            lines.append(f"- `{k}`: {v}")
        lines.append("")
    m = summary["metrics"]
    lines.extend(
        [
            "## Metrics",
            "",
            f"- **precision_at_k_good_only:** {m['precision_at_k_good_only']!r}",
            f"- **precision_at_k_good_or_acceptable:** {m['precision_at_k_good_or_acceptable']!r}",
            f"- **bridge_like_yes_or_partial_share:** {m['bridge_like_yes_or_partial_share']!r}",
            f"- **surprising_or_useful_share:** {m['surprising_or_useful_share']!r}",
            "",
        ]
    )
    w = summary.get("warnings") or []
    if w:
        lines.append("## Warnings")
        lines.append("")
        for x in w:
            lines.append(f"- {x}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def read_worksheet_path(path: Path) -> list[dict[str, str]]:
    """Read and normalize rows from a worksheet CSV (validates header)."""
    return _load_rows(path)


def run_recommendation_review_summary(
    *,
    input_path: Path,
    output_path: Path,
    allow_incomplete: bool,
    markdown_path: Path | None,
) -> None:
    rows = read_worksheet_path(input_path)
    summary = build_recommendation_review_summary(
        rows, input_path=input_path, allow_incomplete=allow_incomplete
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            _markdown_from_summary(summary), encoding="utf-8", newline="\n"
        )


__all__ = [
    "BRIDGE_LIKE_ALLOWED",
    "NOVELTY_ALLOWED",
    "RELEVANCE_ALLOWED",
    "ReviewSummaryError",
    "build_recommendation_review_summary",
    "read_worksheet_path",
    "run_recommendation_review_summary",
]
