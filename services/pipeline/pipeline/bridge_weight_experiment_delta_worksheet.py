"""Delta worksheet for unlabeled moved-in bridge-weight experiment rows."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.recommendation_review_worksheet import _fmt_float, _topic_names_from_json


class BridgeWeightExperimentDeltaWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


DELTA_WORKSHEET_COLUMNS: tuple[str, ...] = (
    "baseline_ranking_run_id",
    "experiment_ranking_run_id",
    "experiment_rank",
    "paper_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "final_score",
    "bridge_score",
    "reason_short",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise BridgeWeightExperimentDeltaWorksheetError(f"{label} not found: {path}", code=2)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise BridgeWeightExperimentDeltaWorksheetError(f"{label} is not valid JSON: {path}", code=2) from e
    if not isinstance(parsed, dict):
        raise BridgeWeightExperimentDeltaWorksheetError(f"{label} must contain a JSON object: {path}", code=2)
    return parsed


def _read_baseline_paper_ids(path: Path, *, expected_ranking_run_id: str) -> set[str]:
    if not path.is_file():
        raise BridgeWeightExperimentDeltaWorksheetError(f"baseline worksheet not found: {path}", code=2)
    out: set[str] = set()
    seen_run_ids: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise BridgeWeightExperimentDeltaWorksheetError(f"baseline worksheet has no header: {path}", code=2)
        missing = {"paper_id", "ranking_run_id"} - set(reader.fieldnames)
        if missing:
            raise BridgeWeightExperimentDeltaWorksheetError(
                f"baseline worksheet missing required columns {sorted(missing)}: {path}",
                code=2,
            )
        for row in reader:
            rrid = (row.get("ranking_run_id") or "").strip()
            if rrid:
                seen_run_ids.add(rrid)
            pid = (row.get("paper_id") or "").strip()
            if pid:
                out.add(pid)
    if seen_run_ids != {expected_ranking_run_id}:
        raise BridgeWeightExperimentDeltaWorksheetError(
            "baseline worksheet ranking_run_id does not match comparison artifact "
            f"(expected {expected_ranking_run_id!r}, found {sorted(seen_run_ids)!r})",
            code=2,
        )
    return out


def _validate_comparison_artifact(
    comparison: dict[str, Any],
    *,
    baseline_ranking_run_id: str | None,
    experiment_ranking_run_id: str | None,
) -> tuple[str, str, list[dict[str, Any]]]:
    prov = comparison.get("provenance")
    if not isinstance(prov, dict):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact missing provenance", code=2)
    baseline = prov.get("baseline")
    experiment = prov.get("experiment")
    if not isinstance(baseline, dict) or not isinstance(experiment, dict):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact missing baseline/experiment provenance", code=2)
    base_id = str(baseline.get("ranking_run_id") or "")
    exp_id = str(experiment.get("ranking_run_id") or "")
    if not base_id or not exp_id:
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact missing ranking_run_id values", code=2)
    if baseline_ranking_run_id is not None and baseline_ranking_run_id != base_id:
        raise BridgeWeightExperimentDeltaWorksheetError("baseline ranking_run_id argument mismatches comparison artifact", code=2)
    if experiment_ranking_run_id is not None and experiment_ranking_run_id != exp_id:
        raise BridgeWeightExperimentDeltaWorksheetError("experiment ranking_run_id argument mismatches comparison artifact", code=2)

    same_stack = comparison.get("same_stack_check")
    if not isinstance(same_stack, dict):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact missing same_stack_check", code=2)
    if comparison.get("review_kind") == "bridge_objective_experiment_compare":
        required_true = (
            "same_corpus_snapshot_version",
            "same_embedding_version",
            "same_cluster_version",
            "same_bridge_weight_for_family_bridge",
        )
        if not all(bool(same_stack.get(k)) for k in required_true):
            raise BridgeWeightExperimentDeltaWorksheetError(
                "objective experiment comparison did not pass same-stack checks", code=2
            )
        if not bool(same_stack.get("bridge_eligibility_modes_differ")):
            raise BridgeWeightExperimentDeltaWorksheetError(
                "objective experiment comparison requires differing bridge_eligibility_mode", code=2
            )
    elif not all(bool(v) for v in same_stack.values()):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact did not pass same-stack checks", code=2)

    quality = comparison.get("quality_risk")
    if not isinstance(quality, dict):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact missing quality_risk", code=2)
    rows = quality.get("experiment_eligible_top_k_not_in_labeled_baseline_rows")
    if not isinstance(rows, list):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact missing unlabeled experiment rows", code=2)
    expected_count = quality.get("unlabeled_experiment_eligible_top_k_count")
    if isinstance(expected_count, int) and expected_count != len(rows):
        raise BridgeWeightExperimentDeltaWorksheetError("comparison artifact unlabeled row count is inconsistent", code=2)
    return base_id, exp_id, [dict(r) for r in rows if isinstance(r, dict)]


def _validate_diagnostics_artifact(
    diagnostics: dict[str, Any],
    *,
    experiment_ranking_run_id: str,
    work_ids: set[int],
) -> None:
    prov = diagnostics.get("provenance")
    if not isinstance(prov, dict) or prov.get("ranking_run_id") != experiment_ranking_run_id:
        raise BridgeWeightExperimentDeltaWorksheetError(
            "diagnostics artifact ranking_run_id does not match experiment ranking_run_id",
            code=2,
        )
    overlap = diagnostics.get("overlap_detail")
    eligible_ids = overlap.get("eligible_bridge_top_k_ids") if isinstance(overlap, dict) else None
    if not isinstance(eligible_ids, list):
        raise BridgeWeightExperimentDeltaWorksheetError("diagnostics artifact missing eligible_bridge_top_k_ids", code=2)
    eligible_set = {int(x) for x in eligible_ids}
    missing = sorted(work_ids - eligible_set)
    if missing:
        raise BridgeWeightExperimentDeltaWorksheetError(
            f"comparison rows missing from diagnostics eligible top-k ids: {missing}",
            code=2,
        )


def _fetch_experiment_bridge_rows(
    conn: psycopg.Connection,
    *,
    experiment_ranking_run_id: str,
    work_ids: Sequence[int],
) -> dict[int, dict[str, Any]]:
    if not work_ids:
        return {}
    rows = conn.execute(
        """
        SELECT *
        FROM (
            SELECT
                ROW_NUMBER() OVER (ORDER BY ps.final_score DESC, ps.work_id ASC) AS experiment_rank,
                ps.work_id,
                w.openalex_id AS paper_id,
                w.title,
                w.year,
                w.citation_count,
                w.source_slug,
                COALESCE(topic_agg.topics, '[]'::json) AS topics,
                ps.final_score,
                ps.bridge_score,
                ps.reason_short,
                ps.bridge_eligible
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            LEFT JOIN LATERAL (
                SELECT json_agg(sub.topic_name ORDER BY sub.score DESC, sub.topic_name ASC) AS topics
                FROM (
                    SELECT t.name AS topic_name, wt.score AS score
                    FROM work_topics wt
                    JOIN topics t ON t.id = wt.topic_id
                    WHERE wt.work_id = w.id
                    ORDER BY wt.score DESC, t.name ASC
                    LIMIT 3
                ) sub
            ) topic_agg ON TRUE
            WHERE ps.ranking_run_id = %s
              AND ps.recommendation_family = 'bridge'
              AND ps.bridge_eligible IS TRUE
        ) ranked
        WHERE ranked.work_id = ANY(%s)
        ORDER BY ranked.experiment_rank ASC
        """,
        (experiment_ranking_run_id, list(work_ids)),
    ).fetchall()
    return {int(r["work_id"]): dict(r) for r in rows}


def build_bridge_weight_experiment_delta_rows(
    conn: psycopg.Connection,
    *,
    comparison_path: Path,
    baseline_worksheet_path: Path,
    diagnostics_path: Path,
    baseline_ranking_run_id: str | None = None,
    experiment_ranking_run_id: str | None = None,
) -> list[dict[str, str]]:
    comparison = _read_json_object(comparison_path, label="comparison artifact")
    diagnostics = _read_json_object(diagnostics_path, label="diagnostics artifact")
    base_id, exp_id, comparison_rows = _validate_comparison_artifact(
        comparison,
        baseline_ranking_run_id=baseline_ranking_run_id,
        experiment_ranking_run_id=experiment_ranking_run_id,
    )
    baseline_paper_ids = _read_baseline_paper_ids(baseline_worksheet_path, expected_ranking_run_id=base_id)

    delta_specs: list[dict[str, Any]] = []
    for row in comparison_rows:
        pid = str(row.get("paper_id") or "").strip()
        if not pid or pid in baseline_paper_ids:
            continue
        try:
            work_id = int(row["work_id"])
            rank = int(row["rank"])
        except (KeyError, TypeError, ValueError) as e:
            raise BridgeWeightExperimentDeltaWorksheetError(
                "comparison artifact unlabeled row missing integer work_id/rank",
                code=2,
            ) from e
        delta_specs.append({"work_id": work_id, "rank": rank, "paper_id": pid})
    delta_specs.sort(key=lambda r: int(r["rank"]))

    work_ids = {int(r["work_id"]) for r in delta_specs}
    _validate_diagnostics_artifact(diagnostics, experiment_ranking_run_id=exp_id, work_ids=work_ids)
    db_rows = _fetch_experiment_bridge_rows(conn, experiment_ranking_run_id=exp_id, work_ids=[int(x) for x in work_ids])

    out: list[dict[str, str]] = []
    for spec in delta_specs:
        work_id = int(spec["work_id"])
        row = db_rows.get(work_id)
        if row is None:
            raise BridgeWeightExperimentDeltaWorksheetError(
                f"experiment bridge eligible row not found in database for work_id={work_id}",
                code=2,
            )
        if bool(row.get("bridge_eligible")) is not True:
            raise BridgeWeightExperimentDeltaWorksheetError(
                f"experiment row is not bridge_eligible for work_id={work_id}",
                code=2,
            )
        if int(row["experiment_rank"]) != int(spec["rank"]):
            raise BridgeWeightExperimentDeltaWorksheetError(
                f"experiment rank mismatch for work_id={work_id}: "
                f"artifact={spec['rank']} database={row['experiment_rank']}",
                code=2,
            )
        if str(row.get("paper_id") or "") != str(spec["paper_id"]):
            raise BridgeWeightExperimentDeltaWorksheetError(
                f"paper_id mismatch for work_id={work_id}",
                code=2,
            )
        topics = _topic_names_from_json(row.get("topics"))
        out.append(
            {
                "baseline_ranking_run_id": base_id,
                "experiment_ranking_run_id": exp_id,
                "experiment_rank": str(int(row["experiment_rank"])),
                "paper_id": str(row["paper_id"]),
                "title": str(row["title"] or ""),
                "year": str(int(row["year"]) if row.get("year") is not None else ""),
                "citation_count": str(int(row["citation_count"] or 0)),
                "source_slug": str(row["source_slug"] or ""),
                "topics": ";".join(topics) if topics else "",
                "final_score": _fmt_float(float(row["final_score"]) if row.get("final_score") is not None else None),
                "bridge_score": _fmt_float(float(row["bridge_score"]) if row.get("bridge_score") is not None else None),
                "reason_short": str(row["reason_short"] or ""),
                "relevance_label": "",
                "novelty_label": "",
                "bridge_like_label": "",
                "reviewer_notes": "",
            }
        )
    return out


def render_delta_worksheet_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=DELTA_WORKSHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({c: row.get(c, "") for c in DELTA_WORKSHEET_COLUMNS})
    return buf.getvalue()


def write_bridge_weight_experiment_delta_worksheet(
    *,
    comparison_path: Path,
    baseline_worksheet_path: Path,
    diagnostics_path: Path,
    output_path: Path,
    database_url: str | None,
    baseline_ranking_run_id: str | None = None,
    experiment_ranking_run_id: str | None = None,
) -> list[dict[str, str]]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=30) as conn:
        rows = build_bridge_weight_experiment_delta_rows(
            conn,
            comparison_path=comparison_path,
            baseline_worksheet_path=baseline_worksheet_path,
            diagnostics_path=diagnostics_path,
            baseline_ranking_run_id=baseline_ranking_run_id,
            experiment_ranking_run_id=experiment_ranking_run_id,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_delta_worksheet_csv(rows), encoding="utf-8", newline="")
    return rows


__all__ = [
    "BridgeWeightExperimentDeltaWorksheetError",
    "DELTA_WORKSHEET_COLUMNS",
    "build_bridge_weight_experiment_delta_rows",
    "render_delta_worksheet_csv",
    "write_bridge_weight_experiment_delta_worksheet",
]
