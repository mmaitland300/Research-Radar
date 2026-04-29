"""Validate and summarize a completed bridge-weight delta review worksheet CSV."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.bridge_weight_experiment_delta_worksheet import DELTA_WORKSHEET_COLUMNS
from pipeline.recommendation_review_summary import (
    BRIDGE_LIKE_ALLOWED,
    NOVELTY_ALLOWED,
    RELEVANCE_ALLOWED,
)

EXPECTED_BASELINE_RANKING_RUN_ID = "rank-ee2ba6c816"
EXPECTED_EXPERIMENT_RANKING_RUN_ID = "rank-bc1123e00c"
EXPECTED_ROW_COUNT = 4


class BridgeWeightExperimentDeltaSummaryError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _norm(s: str | None) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _load_delta_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise BridgeWeightExperimentDeltaSummaryError(f"Input file not found: {path}", code=2)
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise BridgeWeightExperimentDeltaSummaryError("CSV has no header row.", code=2)
    header = [h.strip() if h else "" for h in reader.fieldnames]
    missing = [c for c in DELTA_WORKSHEET_COLUMNS if c not in header]
    if missing:
        raise BridgeWeightExperimentDeltaSummaryError(
            f"Delta worksheet is missing required columns: {', '.join(missing)}",
            code=2,
        )
    rows: list[dict[str, str]] = []
    for row in reader:
        if row is None:
            continue
        if not any(_norm(v) for k, v in row.items() if k):
            continue
        rows.append({c: _norm(row.get(c, "")) for c in DELTA_WORKSHEET_COLUMNS})
    return rows


def _validate_label(col: str, value: str, allowed: frozenset[str], *, row: int, paper_id: str) -> None:
    if not value:
        raise BridgeWeightExperimentDeltaSummaryError(
            f"data row {row} (paper_id={paper_id!r}): column {col!r} is blank",
            code=2,
        )
    if value not in allowed:
        raise BridgeWeightExperimentDeltaSummaryError(
            f"data row {row} (paper_id={paper_id!r}): column {col!r} has invalid value {value!r} "
            f"(expected one of: {', '.join(sorted(allowed))})",
            code=2,
        )


def build_bridge_weight_experiment_delta_summary_payload(
    rows: list[dict[str, str]],
    *,
    input_path: Path,
) -> dict[str, Any]:
    if len(rows) != EXPECTED_ROW_COUNT:
        raise BridgeWeightExperimentDeltaSummaryError(
            f"Expected exactly {EXPECTED_ROW_COUNT} data rows, found {len(rows)}.",
            code=2,
        )
    for i, r in enumerate(rows, start=1):
        pid = r.get("paper_id") or f"(row {i})"
        b = r.get("baseline_ranking_run_id", "")
        e = r.get("experiment_ranking_run_id", "")
        if b != EXPECTED_BASELINE_RANKING_RUN_ID:
            raise BridgeWeightExperimentDeltaSummaryError(
                f"data row {i} (paper_id={pid!r}): baseline_ranking_run_id must be "
                f"{EXPECTED_BASELINE_RANKING_RUN_ID!r}, got {b!r}.",
                code=2,
            )
        if e != EXPECTED_EXPERIMENT_RANKING_RUN_ID:
            raise BridgeWeightExperimentDeltaSummaryError(
                f"data row {i} (paper_id={pid!r}): experiment_ranking_run_id must be "
                f"{EXPECTED_EXPERIMENT_RANKING_RUN_ID!r}, got {e!r}.",
                code=2,
            )
        _validate_label("relevance_label", r.get("relevance_label", ""), RELEVANCE_ALLOWED, row=i, paper_id=pid)
        _validate_label("novelty_label", r.get("novelty_label", ""), NOVELTY_ALLOWED, row=i, paper_id=pid)
        _validate_label("bridge_like_label", r.get("bridge_like_label", ""), BRIDGE_LIKE_ALLOWED, row=i, paper_id=pid)

    n = len(rows)
    good_count = sum(1 for r in rows if r.get("relevance_label") == "good")
    acceptable_count = sum(1 for r in rows if r.get("relevance_label") == "acceptable")
    good_or_acceptable_count = sum(
        1 for r in rows if r.get("relevance_label") in ("good", "acceptable")
    )
    useful_or_surprising_count = sum(
        1 for r in rows if r.get("novelty_label") in ("useful", "surprising")
    )
    bridge_like_yes_or_partial_count = sum(
        1 for r in rows if r.get("bridge_like_label") in ("yes", "partial")
    )
    miss_or_irrelevant_count = sum(
        1 for r in rows if r.get("relevance_label") in ("miss", "irrelevant")
    )
    bridge_like_no_count = sum(1 for r in rows if r.get("bridge_like_label") == "no")

    good_or_acceptable_share = good_or_acceptable_count / n
    useful_or_surprising_share = useful_or_surprising_count / n
    bridge_like_yes_or_partial_share = bridge_like_yes_or_partial_count / n

    delta_quality_pass = good_or_acceptable_share >= 0.75
    delta_bridge_like_pass = bridge_like_yes_or_partial_share >= 0.75
    experiment_quality_gate_pass = delta_quality_pass and delta_bridge_like_pass

    if experiment_quality_gate_pass:
        decision_text = (
            "The 0.05 bridge-weight experiment preserved quality on moved-in rows; "
            "candidate for a second gated experiment, not default."
        )
    else:
        decision_text = (
            "Delta quality gates did not both pass; do not treat this run as justification "
            "for a second experiment or any weight increase without further review."
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path.resolve()),
        "review_kind": "bridge_weight_experiment_delta_only",
        "row_count": n,
        "baseline_ranking_run_id": EXPECTED_BASELINE_RANKING_RUN_ID,
        "experiment_ranking_run_id": EXPECTED_EXPERIMENT_RANKING_RUN_ID,
        "metrics": {
            "good_count": good_count,
            "acceptable_count": acceptable_count,
            "good_or_acceptable_count": good_or_acceptable_count,
            "useful_or_surprising_count": useful_or_surprising_count,
            "bridge_like_yes_or_partial_count": bridge_like_yes_or_partial_count,
            "miss_or_irrelevant_count": miss_or_irrelevant_count,
            "bridge_like_no_count": bridge_like_no_count,
            "good_or_acceptable_share": good_or_acceptable_share,
            "useful_or_surprising_share": useful_or_surprising_share,
            "bridge_like_yes_or_partial_share": bridge_like_yes_or_partial_share,
        },
        "gates": {
            "delta_quality_pass": delta_quality_pass,
            "delta_bridge_like_pass": delta_bridge_like_pass,
            "experiment_quality_gate_pass": experiment_quality_gate_pass,
        },
        "decision": {
            "text": decision_text,
            "ready_for_default": False,
        },
        "caveats": [
            "This is not validation of bridge ranking or of any default bridge weight.",
            "This summary reflects only a 4-row delta review of moved-in eligible bridge top-20 rows.",
            "ready_for_default remains false.",
            "No further weight increase until this artifact is reviewed in context with the full pipeline evidence.",
        ],
    }


def markdown_from_bridge_weight_experiment_delta_summary(payload: dict[str, Any]) -> str:
    m = payload.get("metrics") or {}
    g = payload.get("gates") or {}
    d = payload.get("decision") or {}
    caveats = payload.get("caveats") or []
    lines = [
        "# Bridge weight experiment delta review summary",
        "",
        "This artifact does **not** validate bridge ranking and does **not** justify making `0.05` the default.",
        "",
        "## Provenance",
        "",
        f"- **Generated (UTC):** `{payload.get('generated_at', '')}`",
        f"- **Input:** `{payload.get('input_path', '')}`",
        f"- **Review kind:** `{payload.get('review_kind', '')}`",
        f"- **row_count:** `{payload.get('row_count')}`",
        f"- **baseline_ranking_run_id:** `{payload.get('baseline_ranking_run_id')}`",
        f"- **experiment_ranking_run_id:** `{payload.get('experiment_ranking_run_id')}`",
        "",
        "## Metrics",
        "",
        f"- **good_count:** `{m.get('good_count')}`",
        f"- **acceptable_count:** `{m.get('acceptable_count')}`",
        f"- **good_or_acceptable_count:** `{m.get('good_or_acceptable_count')}`",
        f"- **useful_or_surprising_count:** `{m.get('useful_or_surprising_count')}`",
        f"- **bridge_like_yes_or_partial_count:** `{m.get('bridge_like_yes_or_partial_count')}`",
        f"- **miss_or_irrelevant_count:** `{m.get('miss_or_irrelevant_count')}`",
        f"- **bridge_like_no_count:** `{m.get('bridge_like_no_count')}`",
        f"- **good_or_acceptable_share:** `{m.get('good_or_acceptable_share')}`",
        f"- **useful_or_surprising_share:** `{m.get('useful_or_surprising_share')}`",
        f"- **bridge_like_yes_or_partial_share:** `{m.get('bridge_like_yes_or_partial_share')}`",
        "",
        "## Gates",
        "",
        f"- **delta_quality_pass:** `{g.get('delta_quality_pass')}` (good_or_acceptable_share ≥ 0.75)",
        f"- **delta_bridge_like_pass:** `{g.get('delta_bridge_like_pass')}` (bridge_like_yes_or_partial_share ≥ 0.75)",
        f"- **experiment_quality_gate_pass:** `{g.get('experiment_quality_gate_pass')}`",
        "",
        "## Decision",
        "",
        f"{d.get('text', '')}",
        "",
        f"- **ready_for_default:** `{d.get('ready_for_default')}` (must remain false)",
        "",
        "## Caveats",
        "",
    ]
    for c in caveats:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_weight_experiment_delta_summary(
    *,
    input_path: Path,
    output_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    rows = _load_delta_rows(input_path)
    payload = build_bridge_weight_experiment_delta_summary_payload(rows, input_path=input_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            markdown_from_bridge_weight_experiment_delta_summary(payload),
            encoding="utf-8",
            newline="\n",
        )
    return payload


__all__ = [
    "BridgeWeightExperimentDeltaSummaryError",
    "EXPECTED_BASELINE_RANKING_RUN_ID",
    "EXPECTED_EXPERIMENT_RANKING_RUN_ID",
    "EXPECTED_ROW_COUNT",
    "build_bridge_weight_experiment_delta_summary_payload",
    "markdown_from_bridge_weight_experiment_delta_summary",
    "run_bridge_weight_experiment_delta_summary",
]
