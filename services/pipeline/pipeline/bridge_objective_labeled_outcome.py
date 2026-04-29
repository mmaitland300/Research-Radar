"""Labeled outcome rollup for bridge objective experiments (same weight, changed eligibility)."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.bridge_objective_label_coverage import ONE_ROW_REVIEW_COLUMNS
from pipeline.recommendation_review_summary import (
    BRIDGE_LIKE_ALLOWED,
    NOVELTY_ALLOWED,
    RELEVANCE_ALLOWED,
)


class BridgeObjectiveLabeledOutcomeError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _norm(value: str | None) -> str:
    return str(value or "").strip()


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise BridgeObjectiveLabeledOutcomeError(f"{label} not found: {path}", code=2)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BridgeObjectiveLabeledOutcomeError(f"{label} is not valid JSON: {path}", code=2) from exc
    if not isinstance(parsed, dict):
        raise BridgeObjectiveLabeledOutcomeError(f"{label} must be a JSON object: {path}", code=2)
    return parsed


def _validate_label_value(col: str, value: str, allowed: frozenset[str], *, ctx: str) -> None:
    if not value:
        raise BridgeObjectiveLabeledOutcomeError(f"{ctx}: column {col!r} is blank", code=2)
    if value not in allowed:
        raise BridgeObjectiveLabeledOutcomeError(
            f"{ctx}: column {col!r} has invalid value {value!r} (expected one of: {', '.join(sorted(allowed))})",
            code=2,
        )


def _read_csv(path: Path, *, label: str) -> tuple[list[dict[str, str]], list[str]]:
    if not path.is_file():
        raise BridgeObjectiveLabeledOutcomeError(f"{label} not found: {path}", code=2)
    text = path.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise BridgeObjectiveLabeledOutcomeError(f"{label} has no header: {path}", code=2)
    rows = [{str(k or "").strip(): _norm(v) for k, v in row.items()} for row in reader if row is not None]
    return rows, [str(h or "").strip() for h in reader.fieldnames]


@dataclass(frozen=True)
class _LabelTriple:
    relevance_label: str
    novelty_label: str
    bridge_like_label: str


def _load_baseline_rows(path: Path, *, expected_ranking_run_id: str) -> list[dict[str, str]]:
    rows, fields = _read_csv(path, label="baseline worksheet")
    required = {"ranking_run_id", "rank", "paper_id", "relevance_label", "novelty_label", "bridge_like_label"}
    missing = required - set(fields)
    if missing:
        raise BridgeObjectiveLabeledOutcomeError(f"{path}: missing columns: {sorted(missing)}", code=2)
    if len(rows) != 20:
        raise BridgeObjectiveLabeledOutcomeError(f"{path}: expected exactly 20 rows, got {len(rows)}", code=2)
    for i, row in enumerate(rows, start=1):
        ctx = f"{path} row {i} paper_id={row.get('paper_id')!r}"
        if row.get("ranking_run_id") != expected_ranking_run_id:
            raise BridgeObjectiveLabeledOutcomeError(
                f"{ctx}: ranking_run_id must be {expected_ranking_run_id!r}", code=2
            )
        _validate_label_value("relevance_label", row.get("relevance_label", ""), RELEVANCE_ALLOWED, ctx=ctx)
        _validate_label_value("novelty_label", row.get("novelty_label", ""), NOVELTY_ALLOWED, ctx=ctx)
        _validate_label_value("bridge_like_label", row.get("bridge_like_label", ""), BRIDGE_LIKE_ALLOWED, ctx=ctx)
    return sorted(
        rows,
        key=lambda r: int(r.get("rank") or "0"),
    )


def _load_prior_delta_rows(path: Path, *, baseline_ranking_run_id: str) -> list[dict[str, str]]:
    rows, fields = _read_csv(path, label="prior delta worksheet")
    required = {
        "baseline_ranking_run_id",
        "paper_id",
        "relevance_label",
        "novelty_label",
        "bridge_like_label",
    }
    missing = required - set(fields)
    if missing:
        raise BridgeObjectiveLabeledOutcomeError(f"{path}: missing columns: {sorted(missing)}", code=2)
    for i, row in enumerate(rows, start=1):
        ctx = f"{path} row {i} paper_id={row.get('paper_id')!r}"
        if row.get("baseline_ranking_run_id") != baseline_ranking_run_id:
            raise BridgeObjectiveLabeledOutcomeError(
                f"{ctx}: baseline_ranking_run_id must be {baseline_ranking_run_id!r}", code=2
            )
        _validate_label_value("relevance_label", row.get("relevance_label", ""), RELEVANCE_ALLOWED, ctx=ctx)
        _validate_label_value("novelty_label", row.get("novelty_label", ""), NOVELTY_ALLOWED, ctx=ctx)
        _validate_label_value("bridge_like_label", row.get("bridge_like_label", ""), BRIDGE_LIKE_ALLOWED, ctx=ctx)
    return rows


def _load_objective_delta_rows(
    path: Path,
    *,
    baseline_ranking_run_id: str,
    experiment_ranking_run_id: str,
) -> list[dict[str, str]]:
    rows, fields = _read_csv(path, label="objective one-row worksheet")
    missing = [c for c in ONE_ROW_REVIEW_COLUMNS if c not in fields]
    if missing:
        raise BridgeObjectiveLabeledOutcomeError(f"{path}: missing columns: {missing}", code=2)
    if len(rows) != 1:
        raise BridgeObjectiveLabeledOutcomeError(f"{path}: expected exactly 1 row, got {len(rows)}", code=2)
    row = rows[0]
    ctx = f"{path} row 1 paper_id={row.get('paper_id')!r}"
    if row.get("baseline_ranking_run_id") != baseline_ranking_run_id:
        raise BridgeObjectiveLabeledOutcomeError(
            f"{ctx}: baseline_ranking_run_id must be {baseline_ranking_run_id!r}", code=2
        )
    if row.get("experiment_ranking_run_id") != experiment_ranking_run_id:
        raise BridgeObjectiveLabeledOutcomeError(
            f"{ctx}: experiment_ranking_run_id must be {experiment_ranking_run_id!r}", code=2
        )
    _validate_label_value("relevance_label", row.get("relevance_label", ""), RELEVANCE_ALLOWED, ctx=ctx)
    _validate_label_value("novelty_label", row.get("novelty_label", ""), NOVELTY_ALLOWED, ctx=ctx)
    _validate_label_value("bridge_like_label", row.get("bridge_like_label", ""), BRIDGE_LIKE_ALLOWED, ctx=ctx)
    return rows


def _build_label_map(*, baseline_rows: list[dict[str, str]], prior_delta_rows: list[dict[str, str]], objective_delta_rows: list[dict[str, str]]) -> dict[str, _LabelTriple]:
    label_map: dict[str, _LabelTriple] = {}
    for source_rows in (baseline_rows, prior_delta_rows, objective_delta_rows):
        for row in source_rows:
            pid = _norm(row.get("paper_id"))
            if not pid:
                raise BridgeObjectiveLabeledOutcomeError("label row missing paper_id", code=2)
            triple = _LabelTriple(
                relevance_label=row["relevance_label"],
                novelty_label=row["novelty_label"],
                bridge_like_label=row["bridge_like_label"],
            )
            existing = label_map.get(pid)
            if existing is not None and existing != triple:
                raise BridgeObjectiveLabeledOutcomeError(
                    f"conflicting labels for paper_id={pid!r} across label sources", code=2
                )
            label_map[pid] = triple
    return label_map


def _compute_metrics(labels: list[_LabelTriple]) -> dict[str, Any]:
    n = len(labels)
    if n == 0:
        raise BridgeObjectiveLabeledOutcomeError("cannot compute metrics from empty label set", code=2)
    good_or_acceptable = sum(1 for l in labels if l.relevance_label in {"good", "acceptable"})
    yes_or_partial = sum(1 for l in labels if l.bridge_like_label in {"yes", "partial"})
    return {
        "row_count": n,
        "good_or_acceptable_count": good_or_acceptable,
        "good_or_acceptable_share": good_or_acceptable / n,
        "bridge_like_yes_or_partial_count": yes_or_partial,
        "bridge_like_yes_or_partial_share": yes_or_partial / n,
    }


def build_bridge_objective_labeled_outcome_payload(
    *,
    baseline_worksheet_path: Path,
    prior_delta_worksheet_path: Path,
    objective_delta_worksheet_path: Path,
    objective_comparison_path: Path,
) -> dict[str, Any]:
    comparison = _read_json(objective_comparison_path, label="objective experiment comparison")
    if comparison.get("review_kind") != "bridge_objective_experiment_compare":
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact is not bridge_objective_experiment_compare", code=2)
    prov = comparison.get("provenance")
    if not isinstance(prov, dict):
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact missing provenance", code=2)
    baseline = prov.get("baseline")
    experiment = prov.get("experiment")
    if not isinstance(baseline, dict) or not isinstance(experiment, dict):
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact missing baseline/experiment provenance", code=2)
    baseline_rid = _norm(baseline.get("ranking_run_id"))
    experiment_rid = _norm(experiment.get("ranking_run_id"))
    if not baseline_rid or not experiment_rid:
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact missing ranking_run_id values", code=2)

    baseline_rows = _load_baseline_rows(baseline_worksheet_path, expected_ranking_run_id=baseline_rid)
    prior_delta_rows = _load_prior_delta_rows(prior_delta_worksheet_path, baseline_ranking_run_id=baseline_rid)
    objective_delta_rows = _load_objective_delta_rows(
        objective_delta_worksheet_path,
        baseline_ranking_run_id=baseline_rid,
        experiment_ranking_run_id=experiment_rid,
    )
    label_map = _build_label_map(
        baseline_rows=baseline_rows,
        prior_delta_rows=prior_delta_rows,
        objective_delta_rows=objective_delta_rows,
    )

    bridge_comp = comparison.get("bridge_top_k_comparison")
    if not isinstance(bridge_comp, dict):
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact missing bridge_top_k_comparison", code=2)
    baseline_eligible_work_ids = bridge_comp.get("baseline_eligible_bridge_top_k_ids")
    experiment_eligible_work_ids = bridge_comp.get("experiment_eligible_bridge_top_k_ids")
    if not isinstance(baseline_eligible_work_ids, list) or not isinstance(experiment_eligible_work_ids, list):
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact missing eligible bridge top-k work_ids", code=2)
    if len(baseline_eligible_work_ids) != 20 or len(experiment_eligible_work_ids) != 20:
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact eligible bridge top-k lists must each be length 20", code=2)

    baseline_wid_to_pid: dict[int, str] = {}
    for wid, row in zip([int(x) for x in baseline_eligible_work_ids], baseline_rows, strict=True):
        pid = _norm(row.get("paper_id"))
        if not pid:
            raise BridgeObjectiveLabeledOutcomeError("baseline worksheet row missing paper_id", code=2)
        baseline_wid_to_pid[wid] = pid

    moved_rows = ((comparison.get("quality_risk") or {}).get("unlabeled_new_experiment_eligible_top_k_rows") or [])
    moved_wid_to_pid: dict[int, str] = {}
    if isinstance(moved_rows, list):
        for row in moved_rows:
            if not isinstance(row, dict):
                continue
            wid = row.get("work_id")
            pid = _norm(row.get("paper_id"))
            if isinstance(wid, int) and pid:
                moved_wid_to_pid[wid] = pid

    experiment_labels: list[_LabelTriple] = []
    missing_experiment_ids: list[int] = []
    for wid in [int(x) for x in experiment_eligible_work_ids]:
        pid = baseline_wid_to_pid.get(wid) or moved_wid_to_pid.get(wid)
        if not pid:
            missing_experiment_ids.append(wid)
            continue
        triple = label_map.get(pid)
        if triple is None:
            missing_experiment_ids.append(wid)
            continue
        experiment_labels.append(triple)
    if missing_experiment_ids:
        raise BridgeObjectiveLabeledOutcomeError(
            f"missing labels for experiment eligible work_ids: {sorted(set(missing_experiment_ids))}", code=2
        )

    baseline_labels = [_LabelTriple(r["relevance_label"], r["novelty_label"], r["bridge_like_label"]) for r in baseline_rows]
    baseline_metrics = _compute_metrics(baseline_labels)
    experiment_metrics = _compute_metrics(experiment_labels)

    distinctness = comparison.get("distinctness")
    if not isinstance(distinctness, dict):
        raise BridgeObjectiveLabeledOutcomeError("comparison artifact missing distinctness block", code=2)
    baseline_j = distinctness.get("baseline_eligible_bridge_vs_emerging_jaccard")
    experiment_j = distinctness.get("experiment_eligible_bridge_vs_emerging_jaccard")
    if not isinstance(baseline_j, (int, float)) or not isinstance(experiment_j, (int, float)):
        raise BridgeObjectiveLabeledOutcomeError("distinctness block missing jaccard values", code=2)
    baseline_jf = float(baseline_j)
    experiment_jf = float(experiment_j)

    quality_preserved = experiment_metrics["good_or_acceptable_share"] >= baseline_metrics["good_or_acceptable_share"] - 0.05
    bridge_like_preserved = (
        experiment_metrics["bridge_like_yes_or_partial_share"] >= baseline_metrics["bridge_like_yes_or_partial_share"] - 0.05
    )
    distinctness_improves = experiment_jf < baseline_jf
    recommend = bool(quality_preserved and bridge_like_preserved and distinctness_improves)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_kind": "bridge_objective_labeled_outcome",
        "provenance": {
            "baseline_ranking_run_id": baseline_rid,
            "experiment_ranking_run_id": experiment_rid,
            "baseline_worksheet_path": str(baseline_worksheet_path),
            "prior_delta_worksheet_path": str(prior_delta_worksheet_path),
            "objective_delta_worksheet_path": str(objective_delta_worksheet_path),
            "objective_comparison_path": str(objective_comparison_path),
            "bridge_eligibility_mode_baseline": str(baseline.get("bridge_eligibility_mode") or ""),
            "bridge_eligibility_mode_experiment": str(experiment.get("bridge_eligibility_mode") or ""),
            "bridge_weight_for_family_bridge_baseline": baseline.get("bridge_weight_for_family_bridge"),
            "bridge_weight_for_family_bridge_experiment": experiment.get("bridge_weight_for_family_bridge"),
            "corpus_snapshot_version": str(baseline.get("corpus_snapshot_version") or ""),
            "embedding_version": str(baseline.get("embedding_version") or ""),
            "cluster_version": str(baseline.get("cluster_version") or ""),
        },
        "coverage": {
            "label_map_paper_id_count": len(label_map),
            "baseline_labeled_count": len(baseline_rows),
            "prior_delta_labeled_count": len(prior_delta_rows),
            "objective_delta_labeled_count": len(objective_delta_rows),
        },
        "baseline": baseline_metrics,
        "experiment": experiment_metrics,
        "distinctness": {
            "baseline_eligible_bridge_vs_emerging_jaccard": baseline_jf,
            "experiment_eligible_bridge_vs_emerging_jaccard": experiment_jf,
            "improves": distinctness_improves,
        },
        "decision": {
            "quality_preserved_under_new_mode": quality_preserved,
            "bridge_like_preserved_under_new_mode": bridge_like_preserved,
            "distinctness_improves": distinctness_improves,
            "recommend_persistent_overlap_exclusion_as_experimental_arm": recommend,
            "ready_for_default": False,
        },
        "caveats": [
            "This is not validation of bridge ranking quality.",
            "Single-reviewer, top-20, offline audit material only.",
            "Persistent-overlap exclusion is corpus-snapshot-specific (source-snapshot-v2-candidate-plan-20260428); the rule must not become default without rederivation on the active snapshot.",
        ],
    }


def markdown_from_bridge_objective_labeled_outcome(payload: dict[str, Any]) -> str:
    b = payload.get("baseline", {})
    e = payload.get("experiment", {})
    d = payload.get("distinctness", {})
    dec = payload.get("decision", {})
    prov = payload.get("provenance", {})
    lines = [
        "# Bridge objective labeled outcome",
        "",
        "Diagnostic only; this does not validate bridge ranking quality and does not justify default changes.",
        "",
        "## Provenance",
        "",
        f"- baseline_ranking_run_id: `{prov.get('baseline_ranking_run_id')}`",
        f"- experiment_ranking_run_id: `{prov.get('experiment_ranking_run_id')}`",
        f"- baseline bridge_eligibility_mode: `{prov.get('bridge_eligibility_mode_baseline')}`",
        f"- experiment bridge_eligibility_mode: `{prov.get('bridge_eligibility_mode_experiment')}`",
        "",
        "## Shares",
        "",
        f"- baseline good_or_acceptable_share: `{b.get('good_or_acceptable_share')}`",
        f"- experiment good_or_acceptable_share: `{e.get('good_or_acceptable_share')}`",
        f"- baseline bridge_like_yes_or_partial_share: `{b.get('bridge_like_yes_or_partial_share')}`",
        f"- experiment bridge_like_yes_or_partial_share: `{e.get('bridge_like_yes_or_partial_share')}`",
        "",
        "## Distinctness",
        "",
        f"- baseline eligible_bridge_vs_emerging_jaccard: `{d.get('baseline_eligible_bridge_vs_emerging_jaccard')}`",
        f"- experiment eligible_bridge_vs_emerging_jaccard: `{d.get('experiment_eligible_bridge_vs_emerging_jaccard')}`",
        "",
        "## Gates",
        "",
        f"- quality_preserved_under_new_mode: `{dec.get('quality_preserved_under_new_mode')}`",
        f"- bridge_like_preserved_under_new_mode: `{dec.get('bridge_like_preserved_under_new_mode')}`",
        f"- distinctness_improves: `{dec.get('distinctness_improves')}`",
        f"- recommend_persistent_overlap_exclusion_as_experimental_arm: `{dec.get('recommend_persistent_overlap_exclusion_as_experimental_arm')}`",
        f"- ready_for_default: `{dec.get('ready_for_default')}`",
        "",
        "## Caveats",
        "",
    ]
    for caveat in payload.get("caveats", []):
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def run_bridge_objective_labeled_outcome(
    *,
    baseline_worksheet_path: Path,
    prior_delta_worksheet_path: Path,
    objective_delta_worksheet_path: Path,
    objective_comparison_path: Path,
    output_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_bridge_objective_labeled_outcome_payload(
        baseline_worksheet_path=baseline_worksheet_path,
        prior_delta_worksheet_path=prior_delta_worksheet_path,
        objective_delta_worksheet_path=objective_delta_worksheet_path,
        objective_comparison_path=objective_comparison_path,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_bridge_objective_labeled_outcome(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "BridgeObjectiveLabeledOutcomeError",
    "build_bridge_objective_labeled_outcome_payload",
    "markdown_from_bridge_objective_labeled_outcome",
    "run_bridge_objective_labeled_outcome",
]
