"""Offline audit: rank disagreements between OOF learned_full logits and heuristic final_score (emerging only)."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg

from pipeline.ml_offline_baseline_eval import _parse_config_json, load_label_dataset, sha256_file
from pipeline.ml_tiny_baseline import (
    EMERGING_FAMILY,
    ALLOWED_TARGETS,
    MLTinyBaselineError,
    _float_or_none,
    collect_joined_emerging_rows_dual_bool_targets,
    compute_oof_learned_logits_learned_full,
    fetch_ranking_run_row,
)
from pipeline.recommendation_review_worksheet import cluster_version_from_config

TARGET_ORDER = ("good_or_acceptable", "surprising_or_useful")

DISAGREEMENT_CAVEATS = (
    "This is an offline disagreement audit, not validation of production ranking.",
    "OOF logits come from the same stratified folds as ml-tiny-baseline learned_full; ranks are within this labeled slice only.",
    "Labels are single-reviewer audit labels with ranking-selection bias.",
    "Results must not change production ranking defaults.",
    "No train/dev/test split is created by this artifact beyond the documented cross-fitting used for OOF scores.",
)


class MLTinyBaselineDisagreementError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _tie_key(row: dict[str, Any]) -> str:
    w = row.get("work_id")
    if w is not None and str(w).strip():
        return str(w).strip()
    return str(row.get("paper_id", ""))


def ordinal_rank_descending(scores: Sequence[float], tie_keys: Sequence[str]) -> list[int]:
    """1 = best (highest score); ties broken by tie_keys ascending."""
    n = len(scores)
    order = sorted(range(n), key=lambda i: (-scores[i], tie_keys[i]))
    ranks = [0] * n
    for pos, idx in enumerate(order, start=1):
        ranks[idx] = pos
    return ranks


def _build_target_audit(
    rows: list[dict[str, Any]],
    *,
    target: str,
    oof_logits: list[float],
    top_n: int,
) -> dict[str, Any]:
    n = len(rows)
    final_scores = [_float_or_none(rows[i].get("final_score")) or 0.0 for i in range(n)]
    keys = [_tie_key(rows[i]) for i in range(n)]
    r_heur = ordinal_rank_descending(final_scores, keys)
    r_learn = ordinal_rank_descending(oof_logits, keys)
    # positive => learned ranks higher (smaller rank number) than heuristic => promoted by learned ordering
    deltas = [r_heur[i] - r_learn[i] for i in range(n)]

    detail_rows: list[dict[str, Any]] = []
    for i in range(n):
        d = deltas[i]
        if d > 0:
            bucket = "promoted_by_learned_ordering"
        elif d < 0:
            bucket = "demoted_by_learned_ordering"
        else:
            bucket = "tie"
        detail_rows.append(
            {
                "row_id": str(rows[i].get("row_id", "")),
                "work_id": rows[i].get("work_id"),
                "paper_id": rows[i].get("paper_id"),
                "title": rows[i].get("title"),
                "final_score": final_scores[i],
                "oof_learned_linear_logit": oof_logits[i],
                "rank_by_final_score": r_heur[i],
                "rank_by_oof_learned_logit": r_learn[i],
                "rank_delta_heuristic_minus_learned": d,
                "disagreement_bucket": bucket,
                "good_or_acceptable": rows[i].get("good_or_acceptable"),
                "surprising_or_useful": rows[i].get("surprising_or_useful"),
            }
        )

    promoted = [r for r in detail_rows if r["disagreement_bucket"] == "promoted_by_learned_ordering"]
    demoted = [r for r in detail_rows if r["disagreement_bucket"] == "demoted_by_learned_ordering"]
    promoted.sort(key=lambda r: (-r["rank_delta_heuristic_minus_learned"], str(r.get("row_id", ""))))
    demoted.sort(key=lambda r: (r["rank_delta_heuristic_minus_learned"], str(r.get("row_id", ""))))

    return {
        "target": target,
        "n_rows": n,
        "promoted_count": len(promoted),
        "demoted_count": len(demoted),
        "tie_count": sum(1 for r in detail_rows if r["disagreement_bucket"] == "tie"),
        "top_promotions_by_abs_rank_delta": promoted[:top_n],
        "top_demotions_by_abs_rank_delta": demoted[:top_n],
        "all_rows": detail_rows,
        "interpretation_note": (
            "rank_delta_heuristic_minus_learned > 0 means the paper moves up under OOF learned ordering vs final_score "
            "(smaller rank is better). This answers whether the learned model would reorder this slice differently "
            "from the heuristic composite; it does not prove better recommendations in production."
        ),
    }


def build_ml_tiny_baseline_disagreement_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    targets: tuple[str, ...],
    top_n: int,
) -> dict[str, Any]:
    rid = ranking_run_id.strip()
    if not rid:
        raise MLTinyBaselineDisagreementError("ranking_run_id must be non-empty")
    if family.strip().lower() != EMERGING_FAMILY:
        raise MLTinyBaselineDisagreementError(
            f"ml-tiny-baseline-disagreement supports only family={EMERGING_FAMILY!r}, not {family!r}",
        )
    for t in targets:
        if t not in ALLOWED_TARGETS:
            raise MLTinyBaselineDisagreementError(f"unknown target {t!r}")

    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLTinyBaselineDisagreementError(f"label dataset not found: {path}")
    label_sha = sha256_file(path)
    raw = load_label_dataset(path)
    label_version = str(raw.get("dataset_version", ""))

    run_row = fetch_ranking_run_row(conn, ranking_run_id=rid)
    cfg = _parse_config_json(run_row.get("config_json"))
    cluster_version = cluster_version_from_config(cfg) or ""

    try:
        joined, join_meta = collect_joined_emerging_rows_dual_bool_targets(
            conn, label_dataset_path=path, ranking_run_id=rid
        )
    except MLTinyBaselineError as e:
        raise MLTinyBaselineDisagreementError(str(e), code=e.code) from e
    if not joined:
        raise MLTinyBaselineDisagreementError("no joined emerging rows with both targets boolean")

    targets_out: dict[str, Any] = {}
    for tgt in targets:
        try:
            oof = compute_oof_learned_logits_learned_full(joined, target=tgt)
        except MLTinyBaselineError as e:
            raise MLTinyBaselineDisagreementError(str(e), code=e.code) from e
        targets_out[tgt] = _build_target_audit(joined, target=tgt, oof_logits=oof, top_n=top_n)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_tiny_baseline_disagreement",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": rid,
            "ranking_version": str(run_row.get("ranking_version", "")),
            "corpus_snapshot_version": str(run_row.get("corpus_snapshot_version", "")),
            "embedding_version": str(run_row.get("embedding_version", "")),
            "cluster_version": cluster_version,
            "label_dataset_path": path.as_posix(),
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
            "family": EMERGING_FAMILY,
            "targets": list(targets),
            "top_n_listings": top_n,
        },
        "caveats": list(DISAGREEMENT_CAVEATS),
        "join_summary": join_meta,
        "targets": targets_out,
    }


def markdown_from_ml_tiny_baseline_disagreement(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    lines = [
        "# Tiny baseline disagreement audit (emerging)",
        "",
        "Compares **within-slice** ordering by persisted `final_score` vs **OOF learned_full linear logits** "
        "(same folds as `ml-tiny-baseline`).",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **targets:** {', '.join(prov.get('targets') or [])}",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **top_n listings:** `{prov.get('top_n_listings')}`",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
    ]
    for tgt, block in payload.get("targets", {}).items():
        lines.extend(
            [
                f"## Target `{tgt}`",
                "",
                f"- **n_rows:** `{block.get('n_rows')}`",
                f"- **promoted / demoted / tie:** `{block.get('promoted_count')}` / `{block.get('demoted_count')}` / `{block.get('tie_count')}`",
                "",
                "### Top promotions (learned ranks higher than final_score)",
                "",
            ]
        )
        for r in block.get("top_promotions_by_abs_rank_delta", []):
            lines.append(
                f"- `{r.get('work_id')}` Δrank={r.get('rank_delta_heuristic_minus_learned')} "
                f"final={r.get('final_score'):.6g} logit={r.get('oof_learned_linear_logit'):.6g} — "
                f"{str(r.get('title') or '')[:120]}"
            )
        lines.extend(["", "### Top demotions", ""])
        for r in block.get("top_demotions_by_abs_rank_delta", []):
            lines.append(
                f"- `{r.get('work_id')}` Δrank={r.get('rank_delta_heuristic_minus_learned')} "
                f"final={r.get('final_score'):.6g} logit={r.get('oof_learned_linear_logit'):.6g} — "
                f"{str(r.get('title') or '')[:120]}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_ml_tiny_baseline_disagreement(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    targets: tuple[str, ...],
    top_n: int,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_tiny_baseline_disagreement_payload(
        conn,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        family=family,
        targets=targets,
        top_n=top_n,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_ml_tiny_baseline_disagreement(payload), encoding="utf-8")
    return payload


def run_ml_tiny_baseline_disagreement_cli(
    *,
    database_url: str,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    targets: tuple[str, ...],
    top_n: int,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    with psycopg.connect(database_url) as conn:
        write_ml_tiny_baseline_disagreement(
            conn,
            label_dataset_path=label_dataset_path,
            ranking_run_id=ranking_run_id,
            family=family,
            targets=targets,
            top_n=top_n,
            json_path=output_json,
            markdown_path=markdown_output,
        )
