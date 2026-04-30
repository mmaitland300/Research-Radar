"""Read-only label coverage and offline-baseline readiness across ranking_run_id slices (no training)."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.repo_paths import portable_repo_path
from pipeline.ml_offline_baseline_eval import (
    VALID_FAMILIES,
    TARGET_FIELDS,
    _build_score_lookups,
    fetch_paper_scores_with_openalex,
    join_label_row_to_score,
    load_label_dataset,
    sha256_file,
)

CAVEATS = (
    "This is not validation.",
    "Blind snapshot labels reduce but do not eliminate selection bias.",
    "All rows remain audit_only.",
    "No production ranking change is supported.",
)


class MLLabelReadinessMatrixError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def filter_audit_only_rows(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    """All rows with split=audit_only; dedupe exact duplicate row_id (keep first)."""
    rows_in = payload.get("rows")
    if not isinstance(rows_in, list):
        raise MLLabelReadinessMatrixError("label dataset missing 'rows' array")
    out: list[dict[str, Any]] = []
    seen_rid: set[str] = set()
    dup_skip = 0
    for r in rows_in:
        if not isinstance(r, dict):
            continue
        if str(r.get("split", "")) != "audit_only":
            continue
        rid = str(r.get("row_id", ""))
        if rid and rid in seen_rid:
            dup_skip += 1
            continue
        if rid:
            seen_rid.add(rid)
        out.append(r)
    return out, dup_skip


def fetch_run_db_snapshot(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT ranking_run_id, status
            FROM ranking_runs
            WHERE ranking_run_id = %s
            """,
            (ranking_run_id,),
        )
        row = cur.fetchone()
    if row is None:
        return {
            "ranking_run_exists": False,
            "ranking_run_succeeded": False,
            "ranking_run_status": None,
            "paper_scores_row_count": 0,
        }
    status = str(row.get("status") or "")
    succeeded = status == "succeeded"
    n_scores = 0
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT COUNT(*)::bigint AS n FROM paper_scores WHERE ranking_run_id = %s",
            (ranking_run_id,),
        )
        cr = cur.fetchone()
        if cr is not None:
            n_scores = int(cr["n"])
    return {
        "ranking_run_exists": True,
        "ranking_run_succeeded": succeeded,
        "ranking_run_status": status,
        "paper_scores_row_count": n_scores,
    }


def _target_value_counts(rows: list[dict[str, Any]], target: str) -> tuple[int, int, int]:
    pos = neg = null = 0
    for r in rows:
        v = r.get(target)
        if v is True:
            pos += 1
        elif v is False:
            neg += 1
        else:
            null += 1
    return pos, neg, null


def _duplicate_paper_id_count(rows: list[dict[str, Any]]) -> int:
    by_pid: dict[str, int] = defaultdict(int)
    for r in rows:
        pid = str(r.get("paper_id") or "")
        if pid:
            by_pid[pid] += 1
    return sum(1 for _pid, c in by_pid.items() if c > 1)


def _derived_target_conflict_count(rows: list[dict[str, Any]], target: str) -> int:
    by_pid: dict[str, set[bool]] = defaultdict(set)
    for r in rows:
        pid = str(r.get("paper_id") or "")
        if not pid:
            continue
        v = r.get(target)
        if isinstance(v, bool):
            by_pid[pid].add(v)
    return sum(1 for _pid, vals in by_pid.items() if len(vals) > 1)


def _readiness_flags(pos: int, neg: int) -> dict[str, bool]:
    return {
        "has_both_classes": pos > 0 and neg > 0,
        "enough_for_diagnostic_auc": pos >= 3 and neg >= 3,
        "enough_for_tiny_baseline": pos >= 10 and neg >= 10,
    }


def _review_pool_variant_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for r in rows:
        key = str(r.get("review_pool_variant") or "(null)")
        out[key] += 1
    return dict(sorted(out.items()))


def _score_cache_for_run(
    conn: psycopg.Connection, *, ranking_run_id: str, succeeded: bool
) -> tuple[dict[tuple[str, int], dict], dict[tuple[str, str], dict]] | None:
    if not succeeded:
        return None
    score_rows = fetch_paper_scores_with_openalex(conn, ranking_run_id=ranking_run_id)
    return _build_score_lookups(score_rows)


def build_ml_label_readiness_matrix_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
) -> dict[str, Any]:
    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLLabelReadinessMatrixError(f"label dataset not found: {path}")
    label_sha = sha256_file(path)
    raw = load_label_dataset(path)
    label_version = str(raw.get("dataset_version", ""))

    rows, dup_global = filter_audit_only_rows(raw)

    run_ids = sorted({str(r.get("ranking_run_id") or "") for r in rows if r.get("ranking_run_id")})
    run_ids = [r for r in run_ids if r]

    run_snapshots: dict[str, dict[str, Any]] = {}
    score_lookups: dict[str, tuple[dict[tuple[str, int], dict], dict[tuple[str, str], dict]] | None] = {}
    for rid in run_ids:
        snap = fetch_run_db_snapshot(conn, ranking_run_id=rid)
        run_snapshots[rid] = snap
        score_lookups[rid] = _score_cache_for_run(
            conn, ranking_run_id=rid, succeeded=bool(snap.get("ranking_run_succeeded"))
        )

    group_keys: list[tuple[str, str, str]] = []
    for rid in run_ids:
        for fam in sorted({str(r.get("family") or "") for r in rows if str(r.get("ranking_run_id") or "") == rid}):
            for tgt in TARGET_FIELDS:
                group_keys.append((rid, fam, tgt))

    groups_out: list[dict[str, Any]] = []
    for rid, fam, target in group_keys:
        rows_g = [
            r
            for r in rows
            if str(r.get("ranking_run_id") or "") == rid and str(r.get("family") or "") == fam
        ]
        if not rows_g:
            continue
        pos, neg, null_n = _target_value_counts(rows_g, target)
        dup_pid = _duplicate_paper_id_count(rows_g)
        dconf = _derived_target_conflict_count(rows_g, target)
        snap = run_snapshots[rid]
        lookups = score_lookups.get(rid)
        joinable = 0
        if lookups is not None:
            by_w, by_tok = lookups
            joinable = sum(1 for r in rows_g if join_label_row_to_score(r, by_w, by_tok) is not None)
        missing_score = len(rows_g) - joinable
        flags = _readiness_flags(pos, neg)
        groups_out.append(
            {
                "ranking_run_id": rid,
                "family": fam if fam else None,
                "target": target,
                "total_labeled_rows": len(rows_g),
                "positive_count": pos,
                "negative_count": neg,
                "null_target_count": null_n,
                "review_pool_variant_counts": _review_pool_variant_counts(rows_g),
                "duplicate_paper_id_count": dup_pid,
                "derived_target_conflict_count": dconf,
                "ranking_run_exists": snap["ranking_run_exists"],
                "ranking_run_succeeded": snap["ranking_run_succeeded"],
                "ranking_run_status": snap["ranking_run_status"],
                "paper_scores_row_count": snap["paper_scores_row_count"],
                "paper_scores_joinable_count": joinable,
                "missing_score_count": missing_score,
                "readiness": flags,
            }
        )

    groups_out.sort(key=lambda g: (g["ranking_run_id"], g["family"] or "", g["target"]))
    source_slice_summary = [
        {
            "ranking_run_id": g["ranking_run_id"],
            "family": g["family"],
            "target": g["target"],
            "positive_count": g["positive_count"],
            "negative_count": g["negative_count"],
            "null_count": g["null_target_count"],
            "has_both_classes": g["readiness"]["has_both_classes"],
            "enough_for_diagnostic_auc": g["readiness"]["enough_for_diagnostic_auc"],
            "enough_for_tiny_baseline": g["readiness"]["enough_for_tiny_baseline"],
            "review_pool_variant_counts": g["review_pool_variant_counts"],
        }
        for g in groups_out
    ]

    eval_candidates = [
        g
        for g in groups_out
        if g["readiness"]["has_both_classes"]
        and g.get("ranking_run_succeeded")
        and g.get("family")
        and str(g["family"]) in VALID_FAMILIES
    ]
    labeling_needed = [
        g
        for g in groups_out
        if (g["total_labeled_rows"] > 0 and not g["readiness"]["has_both_classes"])
        or (g["readiness"]["has_both_classes"] and not g["readiness"]["enough_for_diagnostic_auc"])
    ]

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_label_readiness_matrix",
        "generated_at": generated_at,
        "provenance": {
            "label_dataset_path": portable_repo_path(path),
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
            "duplicate_row_id_skipped_globally": dup_global,
        },
        "caveats": list(CAVEATS),
        "run_snapshots": run_snapshots,
        "groups": groups_out,
        "source_slice_summary": source_slice_summary,
        "recommendation": {
            "run_ml_offline_baseline_eval_for": sorted(
                {g["ranking_run_id"] for g in eval_candidates if g["readiness"]["has_both_classes"]}
            ),
            "targeted_labeling_suggested_for_groups": len(labeling_needed),
            "notes": (
                "Run `ml-offline-baseline-eval` for each succeeded `ranking_run_id` that appears under "
                "`run_ml_offline_baseline_eval_for` once you care about score-aligned metrics for those slices. "
                "For groups without both classes or below diagnostic counts, prioritize **targeted worksheets** "
                "(explicit negatives / contrastive rows) before expecting stable AUC or tiny baselines."
            ),
        },
    }


def markdown_from_ml_label_readiness_matrix(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    rec = payload["recommendation"]
    lines = [
        "# ML label readiness matrix",
        "",
        "Read-only summary of **manual label coverage** and **offline baseline readiness** by "
        "`ranking_run_id`, `family`, and derived target. Uses `ml-label-dataset` plus **`ranking_runs` / `paper_scores`** "
        "(read-only). No model training.",
        "",
        "## Provenance",
        "",
        f"- **label_dataset_path:** `{prov.get('label_dataset_path')}`",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        f"- **duplicate_row_id_skipped (global):** {prov.get('duplicate_row_id_skipped_globally', 0)}",
        f"- **generated_at:** `{payload.get('generated_at')}`",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        "## Recommendation",
        "",
        rec.get("notes", ""),
        "",
        f"- **Runs with both classes (candidates for `ml-offline-baseline-eval`):** "
        f"{', '.join(f'`{x}`' for x in rec.get('run_ml_offline_baseline_eval_for', [])) or '*(none)*'}",
        "",
        f"- **Groups needing richer / contrastive labeling (heuristic):** {rec.get('targeted_labeling_suggested_for_groups', 0)}",
        "",
        "## Run snapshots (DB)",
        "",
        "See JSON `run_snapshots` for `ranking_run_exists`, `ranking_run_succeeded`, `ranking_run_status`, "
        "and `paper_scores_row_count` per `ranking_run_id`.",
        "",
        "## Groups (detail)",
        "",
        "See JSON `groups` for per (`ranking_run_id`, `family`, `target`) counts, join coverage, conflicts, readiness flags, "
        "and `review_pool_variant_counts`.",
        "",
        "## Source-slice summary",
        "",
        "See JSON `source_slice_summary` for per-slice diagnostics (`positive_count`, `negative_count`, `null_count`, "
        "`has_both_classes`, `enough_for_diagnostic_auc`, `enough_for_tiny_baseline`) plus `review_pool_variant_counts`.",
        "",
    ]
    return "\n".join(lines).rstrip() + "\n"


def write_ml_label_readiness_matrix(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_label_readiness_matrix_payload(conn, label_dataset_path=label_dataset_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_label_readiness_matrix(payload), encoding="utf-8")
    return payload


def run_ml_label_readiness_matrix_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_label_readiness_matrix(
            conn,
            label_dataset_path=label_dataset_path,
            json_path=output_json,
            markdown_path=markdown_output,
        )
