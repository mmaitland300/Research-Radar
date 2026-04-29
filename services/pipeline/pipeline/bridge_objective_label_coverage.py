"""Label coverage correction for objective-experiment moved-in bridge rows."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class BridgeObjectiveLabelCoverageError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


ONE_ROW_REVIEW_COLUMNS: tuple[str, ...] = (
    "baseline_ranking_run_id",
    "experiment_ranking_run_id",
    "experiment_rank",
    "work_id",
    "paper_id",
    "title",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)


def _normalize_paper_id(value: Any) -> str:
    return str(value or "").strip().lower()


def _parse_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise BridgeObjectiveLabelCoverageError(f"{label} not found: {path}", code=2)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise BridgeObjectiveLabelCoverageError(f"{label} is not valid JSON: {path}", code=2) from e
    if not isinstance(parsed, dict):
        raise BridgeObjectiveLabelCoverageError(f"{label} must contain a JSON object: {path}", code=2)
    return parsed


def _read_csv_rows(path: Path, *, label: str) -> tuple[list[dict[str, str]], list[str]]:
    if not path.is_file():
        raise BridgeObjectiveLabelCoverageError(f"{label} not found: {path}", code=2)
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise BridgeObjectiveLabelCoverageError(f"{label} has no CSV header: {path}", code=2)
        rows = [{str(k): str(v or "") for k, v in row.items()} for row in reader]
        return rows, [str(x) for x in reader.fieldnames]


def _rows_from_objective_comparison(comparison: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    quality = comparison.get("quality_risk")
    bridge = comparison.get("bridge_top_k_comparison")
    if not isinstance(quality, dict):
        raise BridgeObjectiveLabelCoverageError("comparison artifact missing quality_risk", code=2)
    if not isinstance(bridge, dict):
        raise BridgeObjectiveLabelCoverageError("comparison artifact missing bridge_top_k_comparison", code=2)
    rows = quality.get("unlabeled_new_experiment_eligible_top_k_rows")
    if not isinstance(rows, list):
        rows = quality.get("experiment_eligible_top_k_not_in_labeled_baseline_rows")
    if not isinstance(rows, list):
        raise BridgeObjectiveLabelCoverageError("comparison artifact missing moved-in eligible row list", code=2)
    moved_rows = [dict(r) for r in rows if isinstance(r, dict)]
    baseline_ids = bridge.get("baseline_eligible_bridge_top_k_ids")
    experiment_ids = bridge.get("experiment_eligible_bridge_top_k_ids")
    mismatches: list[str] = []
    if isinstance(baseline_ids, list) and isinstance(experiment_ids, list):
        moved_work_ids = sorted(set(int(x) for x in experiment_ids) - set(int(x) for x in baseline_ids))
        moved_from_rows = sorted(
            x
            for x in (_parse_int(r.get("work_id")) for r in moved_rows)
            if x is not None
        )
        if moved_work_ids != moved_from_rows:
            mismatches.append(
                "moved-in work_id diff from eligible_top_k ids does not match quality_risk row list: "
                f"diff={moved_work_ids}, rows={moved_from_rows}"
            )
    return moved_rows, mismatches


def build_objective_label_coverage_payload(
    *,
    comparison_path: Path,
    baseline_worksheet_path: Path,
    prior_delta_worksheet_path: Path,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    comparison = _read_json_object(comparison_path, label="objective comparison artifact")
    prov = comparison.get("provenance")
    if not isinstance(prov, dict):
        raise BridgeObjectiveLabelCoverageError("comparison artifact missing provenance", code=2)
    baseline = prov.get("baseline")
    experiment = prov.get("experiment")
    if not isinstance(baseline, dict) or not isinstance(experiment, dict):
        raise BridgeObjectiveLabelCoverageError("comparison artifact missing baseline/experiment provenance", code=2)
    baseline_ranking_run_id = str(baseline.get("ranking_run_id") or "").strip()
    experiment_ranking_run_id = str(experiment.get("ranking_run_id") or "").strip()
    if not baseline_ranking_run_id or not experiment_ranking_run_id:
        raise BridgeObjectiveLabelCoverageError("comparison artifact missing ranking_run_id values", code=2)

    moved_rows, moved_row_mismatches = _rows_from_objective_comparison(comparison)
    baseline_rows, baseline_fields = _read_csv_rows(baseline_worksheet_path, label="baseline worksheet")
    prior_rows, prior_fields = _read_csv_rows(prior_delta_worksheet_path, label="prior delta worksheet")
    if "paper_id" not in baseline_fields or "paper_id" not in prior_fields:
        raise BridgeObjectiveLabelCoverageError("label-source worksheets must include paper_id", code=2)

    known_by_pid: dict[str, str] = {}
    known_by_wid: dict[int, str] = {}
    mismatches: list[str] = list(moved_row_mismatches)

    for source_name, rows in (
        ("baseline_bridge_eligible_top20", baseline_rows),
        ("prior_bridge_weight_delta_review", prior_rows),
    ):
        for row in rows:
            pid = _normalize_paper_id(row.get("paper_id"))
            if pid and pid not in known_by_pid:
                known_by_pid[pid] = source_name
            wid = _parse_int(row.get("work_id"))
            if wid is not None and wid not in known_by_wid:
                known_by_wid[wid] = source_name

    moved_in_count = len(moved_rows)
    already_labeled_ids: list[str] = []
    unlabeled_ids: list[str] = []
    label_source_by_paper_id: dict[str, str] = {}
    missing_identifier_rows: list[dict[str, Any]] = []
    one_row_review: list[dict[str, str]] = []

    for row in moved_rows:
        pid_raw = str(row.get("paper_id") or "").strip()
        pid = _normalize_paper_id(pid_raw)
        wid = _parse_int(row.get("work_id"))
        source = known_by_pid.get(pid)
        if source is None and wid is not None:
            source = known_by_wid.get(wid)
        if not pid and wid is None:
            missing_identifier_rows.append({"rank": row.get("rank"), "title": row.get("title")})
            mismatches.append(f"moved-in row missing both paper_id and work_id at rank={row.get('rank')}")
            continue
        canonical_id = pid_raw or f"work_id:{wid}"
        if source is not None:
            already_labeled_ids.append(canonical_id)
            if pid_raw:
                label_source_by_paper_id[pid_raw] = source
            continue
        unlabeled_ids.append(canonical_id)
        one_row_review.append(
            {
                "baseline_ranking_run_id": baseline_ranking_run_id,
                "experiment_ranking_run_id": experiment_ranking_run_id,
                "experiment_rank": str(row.get("rank") or ""),
                "work_id": str(wid if wid is not None else ""),
                "paper_id": pid_raw,
                "title": str(row.get("title") or ""),
                "relevance_label": "",
                "novelty_label": "",
                "bridge_like_label": "",
                "reviewer_notes": "",
            }
        )

    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "bridge_objective_label_coverage",
        "provenance": {
            "objective_experiment_comparison_path": str(comparison_path),
            "objective_experiment_ranking_run_id": experiment_ranking_run_id,
            "baseline_ranking_run_id": baseline_ranking_run_id,
            "known_label_sources": [
                {"name": "baseline_bridge_eligible_top20", "path": str(baseline_worksheet_path)},
                {"name": "prior_bridge_weight_delta_review", "path": str(prior_delta_worksheet_path)},
            ],
        },
        "summary": {
            "moved_in_count_relative_to_baseline": moved_in_count,
            "moved_in_already_labeled_count": len(already_labeled_ids),
            "truly_unlabeled_moved_in_count": len(unlabeled_ids),
        },
        "moved_in": {
            "already_labeled_moved_in_ids": already_labeled_ids,
            "truly_unlabeled_moved_in_ids": unlabeled_ids,
            "label_source_by_paper_id": label_source_by_paper_id,
        },
        "diagnostics": {
            "id_mismatches_or_missing_identifiers": mismatches,
            "missing_identifier_rows": missing_identifier_rows,
        },
        "guardrails": {
            "diagnostic_only": True,
            "validation_claim": False,
            "ready_for_default": False,
            "requires_manual_labeling_before_quality_rollup": bool(unlabeled_ids),
        },
    }
    return payload, one_row_review


def render_one_row_review_csv(rows: list[dict[str, str]]) -> str:
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=list(ONE_ROW_REVIEW_COLUMNS))
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in ONE_ROW_REVIEW_COLUMNS})
    return buf.getvalue()


def markdown_from_objective_label_coverage(payload: dict[str, Any]) -> str:
    prov = payload.get("provenance", {})
    summary = payload.get("summary", {})
    moved = payload.get("moved_in", {})
    lines = [
        "# Bridge objective experiment label coverage correction",
        "",
        "Diagnostic only, not validation.",
        "No ranking/default/product claim is made by this artifact.",
        "This artifact corrects label coverage accounting only.",
        "Objective quality rollup should happen only after the remaining row is manually labeled.",
        "",
        "## Provenance",
        "",
        f"- objective_experiment_ranking_run_id: `{prov.get('objective_experiment_ranking_run_id')}`",
        f"- baseline_ranking_run_id: `{prov.get('baseline_ranking_run_id')}`",
        "",
        "## Coverage counts",
        "",
        f"- moved_in_count_relative_to_baseline: `{summary.get('moved_in_count_relative_to_baseline')}`",
        f"- moved_in_already_labeled_count: `{summary.get('moved_in_already_labeled_count')}`",
        f"- truly_unlabeled_moved_in_count: `{summary.get('truly_unlabeled_moved_in_count')}`",
        "",
        "## IDs",
        "",
        f"- already_labeled_moved_in_ids: `{moved.get('already_labeled_moved_in_ids')}`",
        f"- truly_unlabeled_moved_in_ids: `{moved.get('truly_unlabeled_moved_in_ids')}`",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def run_bridge_objective_label_coverage(
    *,
    comparison_path: Path,
    baseline_worksheet_path: Path,
    prior_delta_worksheet_path: Path,
    output_json_path: Path,
    output_markdown_path: Path,
    output_review_csv_path: Path,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    payload, review_rows = build_objective_label_coverage_payload(
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_worksheet_path,
        prior_delta_worksheet_path=prior_delta_worksheet_path,
    )
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    output_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    output_markdown_path.write_text(markdown_from_objective_label_coverage(payload), encoding="utf-8", newline="\n")
    output_review_csv_path.parent.mkdir(parents=True, exist_ok=True)
    output_review_csv_path.write_text(render_one_row_review_csv(review_rows), encoding="utf-8", newline="")
    return payload, review_rows


__all__ = [
    "BridgeObjectiveLabelCoverageError",
    "build_objective_label_coverage_payload",
    "markdown_from_objective_label_coverage",
    "render_one_row_review_csv",
    "run_bridge_objective_label_coverage",
]
