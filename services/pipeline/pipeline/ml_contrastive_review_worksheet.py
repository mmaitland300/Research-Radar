"""Contrastive offline audit worksheet: sample persisted paper_scores rows for manual labeling (read-only DB)."""

from __future__ import annotations

import csv
import io
import json
import statistics
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_label_dataset import LABEL_FIELDS, paper_id_to_work_id
from pipeline.ml_offline_baseline_eval import load_label_dataset as load_label_dataset_json
from pipeline.recommendation_review_worksheet import (
    _topic_names_from_json,
    cluster_version_from_config,
    format_bridge_eligible_for_csv,
)

REVIEW_POOL_VARIANT = "ml_contrastive_offline_audit"

ALLOWED_SAMPLE_REASONS: frozenset[str] = frozenset(
    {
        "lower_rank_window",
        "median_borderline",
        "weak_family_signal",
        "bridge_ineligible",
        "label_incomplete",
        "fallback_deterministic_fill",
    }
)

FAMILY_ORDER: tuple[str, ...] = ("bridge", "emerging", "undercited")

CSV_COLUMNS: tuple[str, ...] = (
    "ranking_run_id",
    "ranking_version",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
    "family",
    "review_pool_variant",
    "family_rank",
    "paper_id",
    "work_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "final_score",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "bridge_score",
    "diversity_penalty",
    "bridge_eligible",
    "reason_short",
    "sample_reason",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

VERBATIM_MARKDOWN_CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation of ranking quality.",
    "Rows are selected from a persisted ranking run to improve contrastive label coverage.",
    "Labels must be filled manually; missing labels must not be inferred.",
    "Do not create train/dev/test splits from this worksheet until a later explicit split policy exists.",
)


class MLContrastiveReviewWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _fmt_float(v: float | None) -> str:
    if v is None:
        return ""
    return f"{v:.9g}"


def _norm_ws(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def paper_key_family(openalex_id: str | None, family: str) -> tuple[str, str]:
    """Stable key for label exclusion: (family, uppercase W token)."""
    pid = _norm_ws(openalex_id)
    wt = paper_id_to_work_id(pid) if pid else None
    if not wt:
        return (family, "")
    return (family, wt.upper())


def build_label_key_sets(
    payload: dict[str, Any],
    *,
    ranking_run_id: str,
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """Returns (complete_keys, incomplete_only_keys) for audit_only rows for this run."""
    rows_in = payload.get("rows")
    if not isinstance(rows_in, list):
        raise MLContrastiveReviewWorksheetError("label dataset missing 'rows' array")
    rid = str(ranking_run_id).strip()
    complete: set[tuple[str, str]] = set()
    incomplete: set[tuple[str, str]] = set()
    for r in rows_in:
        if not isinstance(r, dict):
            continue
        if str(r.get("split", "")) != "audit_only":
            continue
        if str(r.get("ranking_run_id", "")) != rid:
            continue
        fam = str(r.get("family", "")).strip()
        paper_id = r.get("paper_id")
        key = paper_key_family(str(paper_id) if paper_id else None, fam)
        if key[1] == "":
            continue
        rel, nov, br = (_norm_ws(r.get("relevance_label")), _norm_ws(r.get("novelty_label")), _norm_ws(r.get("bridge_like_label")))
        if rel and nov and br:
            complete.add(key)
            incomplete.discard(key)
        else:
            if key not in complete:
                incomplete.add(key)
    return complete, incomplete


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            p = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(p) if isinstance(p, dict) else {}
    return {}


def assert_succeeded_ranking_run(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        raise MLContrastiveReviewWorksheetError(f"ranking_run_id not found: {ranking_run_id!r}")
    if str(row["status"]) != "succeeded":
        raise MLContrastiveReviewWorksheetError(
            f"ranking run {ranking_run_id!r} is not succeeded (status={row['status']!r}).",
        )
    return dict(row)


def fetch_family_scored_rows(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    family: str,
) -> list[dict[str, Any]]:
    q = """
        SELECT
            ROW_NUMBER() OVER (ORDER BY ps.final_score DESC, ps.work_id ASC) AS family_rank,
            w.openalex_id AS paper_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            COALESCE(topic_agg.topics, '[]'::json) AS topics,
            ps.final_score,
            ps.reason_short,
            ps.semantic_score,
            ps.citation_velocity_score,
            ps.topic_growth_score,
            ps.bridge_score,
            ps.diversity_penalty,
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
          AND ps.recommendation_family = %s
        ORDER BY ps.final_score DESC, ps.work_id ASC
    """
    return list(conn.execute(q, (ranking_run_id, family)).fetchall())


def _median(vals: Sequence[float]) -> float | None:
    if not vals:
        return None
    return float(statistics.median(vals))


def _quantile_sorted(sorted_vals: Sequence[float], q: float) -> float | None:
    """q in [0,1]; linear interpolation on sorted sequence."""
    if not sorted_vals:
        return None
    n = len(sorted_vals)
    if n == 1:
        return float(sorted_vals[0])
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return float(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac)


@dataclass(frozen=True)
class ContrastiveCandidate:
    family: str
    family_rank: int
    paper_id: str
    work_token: str
    final_score: float
    semantic_score: float | None
    citation_velocity_score: float | None
    topic_growth_score: float | None
    bridge_score: float | None
    diversity_penalty: float | None
    bridge_eligible: bool | None
    reason_short: str
    title: str
    year: int | None
    citation_count: int
    source_slug: str
    topics_raw: Any


def raw_row_to_candidate(family: str, row: dict[str, Any]) -> ContrastiveCandidate | None:
    pid = str(row.get("paper_id") or "")
    wt = paper_id_to_work_id(pid)
    if not wt:
        return None
    fs = row.get("final_score")
    if fs is None:
        return None
    return ContrastiveCandidate(
        family=family,
        family_rank=int(row["family_rank"]),
        paper_id=pid,
        work_token=wt.upper(),
        final_score=float(fs),
        semantic_score=float(row["semantic_score"]) if row.get("semantic_score") is not None else None,
        citation_velocity_score=float(row["citation_velocity_score"])
        if row.get("citation_velocity_score") is not None
        else None,
        topic_growth_score=float(row["topic_growth_score"]) if row.get("topic_growth_score") is not None else None,
        bridge_score=float(row["bridge_score"]) if row.get("bridge_score") is not None else None,
        diversity_penalty=float(row["diversity_penalty"]) if row.get("diversity_penalty") is not None else None,
        bridge_eligible=None if row.get("bridge_eligible") is None else bool(row["bridge_eligible"]),
        reason_short=str(row.get("reason_short") or ""),
        title=str(row.get("title") or ""),
        year=int(row["year"]) if row.get("year") is not None else None,
        citation_count=int(row["citation_count"] or 0),
        source_slug=str(row["source_slug"] or "") if row.get("source_slug") is not None else "",
        topics_raw=row.get("topics"),
    )


def select_contrastive_for_family(
    family: str,
    candidates: Sequence[ContrastiveCandidate],
    *,
    per_family: int,
    complete_keys: set[tuple[str, str]],
    incomplete_keys: set[tuple[str, str]],
) -> list[tuple[ContrastiveCandidate, str]]:
    """Deterministic contrastive sample up to per_family rows."""
    if per_family < 1:
        raise MLContrastiveReviewWorksheetError("--per-family must be at least 1")
    pool = [c for c in candidates if (family, c.work_token) not in complete_keys]
    # Stable ordering for downstream buckets
    pool_sorted = sorted(pool, key=lambda c: (c.family_rank, c.work_token))

    eligible_li = [c for c in pool_sorted if (family, c.work_token) in incomplete_keys]
    lower_win = [c for c in pool_sorted if 40 <= c.family_rank <= 80]

    scores = [c.final_score for c in pool_sorted]
    med = _median(scores)
    borderline_sorted = sorted(pool_sorted, key=lambda c: (abs(c.final_score - med) if med is not None else 0.0, c.family_rank, c.work_token))

    used: set[str] = set()
    out: list[tuple[ContrastiveCandidate, str]] = []

    def take_from(
        seq: Iterable[ContrastiveCandidate],
        reason_fn: Callable[[ContrastiveCandidate], str],
        *,
        max_take: int | None = None,
    ) -> None:
        nonlocal out
        seq_list = list(seq)
        taken_bucket = 0
        for c in sorted(seq_list, key=lambda x: (x.family_rank, x.work_token)):
            if len(out) >= per_family:
                return
            if max_take is not None and taken_bucket >= max_take:
                return
            if c.work_token in used:
                continue
            reason = reason_fn(c)
            if reason not in ALLOWED_SAMPLE_REASONS:
                raise MLContrastiveReviewWorksheetError(f"internal error: bad sample_reason {reason!r}")
            out.append((c, reason))
            used.add(c.work_token)
            taken_bucket += 1

    # Bucket caps keep contrastive diversity: rank-window pools can be large enough to starve other buckets.
    lr_cap = max(4, min(len(lower_win), per_family // 3 + bool(per_family % 3)))
    mb_cap = max(3, per_family // 4)

    # 1) label_incomplete
    take_from(eligible_li, lambda _c: "label_incomplete")

    # 2) lower_rank_window (capped)
    take_from(lower_win, lambda _c: "lower_rank_window", max_take=lr_cap)

    # 3) median_borderline — closest scores to median first (capped)
    if med is not None:
        ordered_border = list(borderline_sorted)
        take_from(ordered_border, lambda _c: "median_borderline", max_take=mb_cap)

    # 4) weak family signal / bridge_ineligible
    def bridge_reason(c: ContrastiveCandidate) -> str:
        if c.bridge_eligible is False:
            return "bridge_ineligible"
        return "weak_family_signal"

    sums_sorted = sorted((c.citation_velocity_score or 0.0) + (c.topic_growth_score or 0.0) for c in pool_sorted)
    sum_q25 = _quantile_sorted(sums_sorted, 0.25)

    cites_sorted = sorted(c.citation_count for c in pool_sorted)
    cite_q75 = _quantile_sorted(cites_sorted, 0.75)

    weak_pool: list[ContrastiveCandidate]
    if family == "bridge":
        bridge_scores = sorted(c.bridge_score for c in pool_sorted if c.bridge_score is not None)
        bq25 = _quantile_sorted(bridge_scores, 0.25) if bridge_scores else None

        def is_bridge_weak(c: ContrastiveCandidate) -> bool:
            if c.bridge_eligible is False:
                return True
            if c.bridge_score is not None and bq25 is not None and c.bridge_score <= bq25:
                return True
            return False

        weak_pool = [c for c in pool_sorted if is_bridge_weak(c)]
        weak_sorted = sorted(weak_pool, key=lambda c: (c.family_rank, c.work_token))
        take_from(
            weak_sorted,
            bridge_reason,
        )
    elif family == "emerging":

        def is_emerging_weak(c: ContrastiveCandidate) -> bool:
            s = (c.citation_velocity_score or 0.0) + (c.topic_growth_score or 0.0)
            return sum_q25 is not None and s <= sum_q25

        weak_pool = [c for c in pool_sorted if is_emerging_weak(c)]
        take_from(sorted(weak_pool, key=lambda c: (c.family_rank, c.work_token)), lambda _c: "weak_family_signal")
    else:
        # undercited
        weak_pool = [c for c in pool_sorted if cite_q75 is not None and c.citation_count >= cite_q75]
        take_from(sorted(weak_pool, key=lambda c: (c.family_rank, c.work_token)), lambda _c: "weak_family_signal")

    # 5) fallback: lowest ranks first within remaining (high family_rank number = worse rank)
    remaining = [c for c in sorted(pool_sorted, key=lambda c: (-c.family_rank, c.work_token)) if c.work_token not in used]
    take_from(remaining, lambda _c: "fallback_deterministic_fill")

    return out[:per_family]


def row_dict_to_csv_row(
    *,
    run: dict[str, Any],
    cluster_ver: str,
    selected: Sequence[tuple[ContrastiveCandidate, str]],
) -> list[dict[str, str]]:
    prov = {
        "ranking_run_id": str(run["ranking_run_id"]),
        "ranking_version": str(run["ranking_version"]),
        "corpus_snapshot_version": str(run["corpus_snapshot_version"]),
        "embedding_version": str(run["embedding_version"]),
        "cluster_version": cluster_ver,
        "review_pool_variant": REVIEW_POOL_VARIANT,
    }
    rows: list[dict[str, str]] = []
    for cand, sample_reason in selected:
        topics_list = _topic_names_from_json(cand.topics_raw)
        topics_str = ";".join(topics_list) if topics_list else ""
        be_out = format_bridge_eligible_for_csv(cand.bridge_eligible)
        rows.append(
            {
                **prov,
                "family": cand.family,
                "family_rank": str(cand.family_rank),
                "paper_id": cand.paper_id,
                "work_id": cand.work_token,
                "title": cand.title,
                "year": str(int(cand.year)) if cand.year is not None else "",
                "citation_count": str(int(cand.citation_count)),
                "source_slug": cand.source_slug,
                "topics": topics_str,
                "final_score": _fmt_float(cand.final_score),
                "semantic_score": _fmt_float(cand.semantic_score),
                "citation_velocity_score": _fmt_float(cand.citation_velocity_score),
                "topic_growth_score": _fmt_float(cand.topic_growth_score),
                "bridge_score": _fmt_float(cand.bridge_score),
                "diversity_penalty": _fmt_float(cand.diversity_penalty),
                "bridge_eligible": be_out,
                "reason_short": cand.reason_short,
                "sample_reason": sample_reason,
                "relevance_label": "",
                "novelty_label": "",
                "bridge_like_label": "",
                "reviewer_notes": "",
            }
        )
    return rows


def render_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in CSV_COLUMNS})
    return buf.getvalue()


def markdown_report(
    *,
    ranking_run_id: str,
    run: dict[str, Any],
    cluster_ver: str,
    label_dataset_path: Path,
    selected_by_family: dict[str, list[tuple[ContrastiveCandidate, str]]],
    duplicate_notes: Sequence[str],
) -> str:
    lines: list[str] = []
    lines.append("# ML contrastive offline audit worksheet")
    lines.append("")
    lines.append("## Purpose")
    lines.append("")
    lines.append(
        "This artifact lists manually reviewable candidate rows drawn from a **single persisted ranking run** "
        "to expand **contrastive offline audit label coverage** (especially sparse negatives and uncertain rows). "
        "It does **not** train or evaluate a learned model by itself."
    )
    lines.append("")
    lines.append("## Selection strategy")
    lines.append("")
    lines.append(
        "- Exclude papers that already have **all three** manual label columns filled for this run + family in the "
        "audit slice of the label dataset."
    )
    lines.append(
        "- Prefer ranks **40–80**, score values **near the family median**, and **weak family-specific signals** "
        "(including bridge ineligibility where applicable), then fill remaining slots deterministically."
    )
    lines.append(
        "- Rows with **incomplete** prior labels in the dataset may appear with `sample_reason=label_incomplete`; "
        "label columns are left blank for a fresh pass."
    )
    lines.append("")
    lines.append("## Row counts by family")
    lines.append("")
    lines.append("| family | rows |")
    lines.append("| --- | ---: |")
    for fam in FAMILY_ORDER:
        n = len(selected_by_family.get(fam, []))
        lines.append(f"| `{fam}` | {n} |")
    lines.append("")
    lines.append("## Row counts by sample_reason")
    lines.append("")
    ctr: Counter[str] = Counter()
    for fam in FAMILY_ORDER:
        for _c, sr in selected_by_family.get(fam, []):
            ctr[sr] += 1
    lines.append("| sample_reason | rows |")
    lines.append("| --- | ---: |")
    for sr in sorted(ctr.keys()):
        lines.append(f"| `{sr}` | {ctr[sr]} |")
    lines.append("")
    lines.append("## Caveats")
    lines.append("")
    for c in VERBATIM_MARKDOWN_CAVEATS:
        lines.append(f"- {c}")
    lines.append("")
    lines.append(
        "- Train/dev/test split policy is intentionally deferred; do not derive splits from this worksheet alone."
    )
    lines.append("")
    lines.append("## Provenance")
    lines.append("")
    lines.append(f"- **ranking_run_id:** `{ranking_run_id}`")
    lines.append(f"- **ranking_version:** `{run.get('ranking_version')}`")
    lines.append(f"- **corpus_snapshot_version:** `{run.get('corpus_snapshot_version')}`")
    lines.append(f"- **embedding_version:** `{run.get('embedding_version')}`")
    lines.append(f"- **cluster_version:** `{cluster_ver}`")
    try:
        ds_disp = label_dataset_path.resolve().as_posix()
    except OSError:
        ds_disp = label_dataset_path.as_posix()
    lines.append(f"- **label_dataset:** `{ds_disp}`")
    lines.append(f"- **generated_at:** `{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}`")
    lines.append("")
    if duplicate_notes:
        lines.append("## Duplicate paper notes")
        lines.append("")
        for note in duplicate_notes:
            lines.append(f"- {note}")
        lines.append("")
    return "\n".join(lines)


def build_contrastive_worksheet(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    ranking_run_id: str,
    per_family: int,
) -> tuple[str, str, dict[str, Any]]:
    """Returns (csv_text, markdown_text, debug_payload)."""
    rid = str(ranking_run_id).strip()
    if not rid:
        raise MLContrastiveReviewWorksheetError("--ranking-run-id is required and must not be blank")
    if not label_dataset_path.is_file():
        raise MLContrastiveReviewWorksheetError(f"label dataset not found: {label_dataset_path}")

    payload = load_label_dataset_json(label_dataset_path)
    complete_keys, incomplete_keys = build_label_key_sets(payload, ranking_run_id=rid)

    dsn = database_url or database_url_from_env()
    selected_by_family: dict[str, list[tuple[ContrastiveCandidate, str]]] = {}
    all_csv_rows: list[dict[str, str]] = []
    dup_notes: list[str] = []

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        run = assert_succeeded_ranking_run(conn, ranking_run_id=rid)
        cfg = _parse_config_json(run.get("config_json"))
        cluster_ver = cluster_version_from_config(cfg) or ""

        seen_tokens_global: set[str] = set()
        for family in FAMILY_ORDER:
            raw = fetch_family_scored_rows(conn, ranking_run_id=rid, family=family)
            candidates: list[ContrastiveCandidate] = []
            for rw in raw:
                cc = raw_row_to_candidate(family, dict(rw))
                if cc is None:
                    continue
                candidates.append(cc)
            sel = select_contrastive_for_family(
                family,
                candidates,
                per_family=per_family,
                complete_keys=complete_keys,
                incomplete_keys=incomplete_keys,
            )
            dup_msg = (
                "`{tok}` appears in more than one family row in this worksheet (same persisted pool edge case)."
            )
            for c, _sr in sel:
                if c.work_token in seen_tokens_global:
                    note = dup_msg.format(tok=c.work_token)
                    if note not in dup_notes:
                        dup_notes.append(note)
                seen_tokens_global.add(c.work_token)
            selected_by_family[family] = sel
            all_csv_rows.extend(row_dict_to_csv_row(run=run, cluster_ver=cluster_ver, selected=sel))

    md = markdown_report(
        ranking_run_id=rid,
        run=run,
        cluster_ver=cluster_ver,
        label_dataset_path=label_dataset_path,
        selected_by_family=selected_by_family,
        duplicate_notes=dup_notes,
    )
    csv_text = render_csv(all_csv_rows)
    debug = {
        "ranking_run_id": rid,
        "rows_written": len(all_csv_rows),
        "per_family": {f: len(selected_by_family.get(f, [])) for f in FAMILY_ORDER},
    }
    return csv_text, md, debug


def run_ml_contrastive_review_worksheet_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    ranking_run_id: str,
    output_csv: Path,
    markdown_output: Path,
    per_family: int,
) -> None:
    csv_text, md_text, _dbg = build_contrastive_worksheet(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        per_family=per_family,
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    output_csv.write_text(csv_text, encoding="utf-8", newline="")
    markdown_output.write_text(md_text, encoding="utf-8", newline="")


__all__ = [
    "ALLOWED_SAMPLE_REASONS",
    "CSV_COLUMNS",
    "ContrastiveCandidate",
    "LABEL_FIELDS",
    "MLContrastiveReviewWorksheetError",
    "REVIEW_POOL_VARIANT",
    "VERBATIM_MARKDOWN_CAVEATS",
    "build_contrastive_worksheet",
    "build_label_key_sets",
    "fetch_family_scored_rows",
    "paper_key_family",
    "render_csv",
    "run_ml_contrastive_review_worksheet_cli",
    "select_contrastive_for_family",
]
