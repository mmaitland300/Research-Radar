"""Targeted manual review worksheet for label gaps (read-only DB; one family, one persisted run)."""

from __future__ import annotations

import csv
import io
import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_contrastive_review_worksheet import (
    ContrastiveCandidate,
    MLContrastiveReviewWorksheetError,
    assert_succeeded_ranking_run,
    build_label_key_sets,
    fetch_family_scored_rows,
    raw_row_to_candidate,
)
from pipeline.ml_offline_baseline_eval import load_label_dataset as load_label_dataset_json
from pipeline.repo_paths import portable_repo_path
from pipeline.recommendation_review_worksheet import (
    _topic_names_from_json,
    cluster_version_from_config,
    format_bridge_eligible_for_csv,
)

REVIEW_POOL_VARIANT = "ml_emerging_target_gap_audit"

TARGET_GAP_CHOICES: frozenset[str] = frozenset({"good_or_acceptable", "surprising_or_useful"})
FAMILY_EMERGING = "emerging"

GAP_SAMPLE_REASONS: frozenset[str] = frozenset(
    {
        "emerging_bottom_rank_tail",
        "weak_emerging_signal",
        "low_topic_growth",
        "low_citation_velocity",
        "off_slice_topic_metadata",
        "fallback_deterministic_fill",
    }
)

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
    "bridge_eligible",
    "diversity_penalty",
    "reason_short",
    "sample_reason",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

VERBATIM_GAP_CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation of ranking quality.",
    "Rows are selected from a persisted ranking run to improve emerging-family contrastive label coverage.",
    "Labels must be filled manually; missing labels must not be inferred.",
    "Do not create train/dev/test splits from this worksheet until a later explicit split policy exists.",
)


class MLTargetedGapReviewWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _fmt_float(v: float | None) -> str:
    if v is None:
        return ""
    return f"{v:.9g}"


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


@dataclass(frozen=True)
class EmergingRow:
    """Minimal scored emerging row for gap sampling (mirrors contrastive candidate fields)."""

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


def _candidate_to_emerging_row(c: ContrastiveCandidate) -> EmergingRow | None:
    """Adapt ContrastiveCandidate to EmergingRow."""
    if c.family != FAMILY_EMERGING:
        return None
    return EmergingRow(
        family_rank=c.family_rank,
        paper_id=c.paper_id,
        work_token=c.work_token,
        final_score=c.final_score,
        semantic_score=c.semantic_score,
        citation_velocity_score=c.citation_velocity_score,
        topic_growth_score=c.topic_growth_score,
        bridge_score=c.bridge_score,
        diversity_penalty=c.diversity_penalty,
        bridge_eligible=c.bridge_eligible,
        reason_short=c.reason_short,
        title=c.title,
        year=c.year,
        citation_count=c.citation_count,
        source_slug=c.source_slug,
        topics_raw=c.topics_raw,
    )


def select_emerging_gap_rows(
    pool: Sequence[EmergingRow],
    *,
    complete_keys: set[tuple[str, str]],
    limit: int,
) -> list[tuple[EmergingRow, str]]:
    if limit < 1 or limit > 200:
        raise MLTargetedGapReviewWorksheetError("--limit must be between 1 and 200")
    fam = FAMILY_EMERGING
    eligible = sorted(
        [c for c in pool if (fam, c.work_token) not in complete_keys],
        key=lambda c: (c.family_rank, c.work_token),
    )
    if not eligible:
        return []

    used: set[str] = set()
    out: list[tuple[EmergingRow, str]] = []

    def take_from(
        seq: Sequence[EmergingRow],
        reason: str,
        *,
        max_take: int,
        preserve_order: bool = False,
    ) -> None:
        nonlocal out
        if reason not in GAP_SAMPLE_REASONS:
            raise MLTargetedGapReviewWorksheetError(f"internal error: bad sample_reason {reason!r}")
        seq_list = list(seq)
        ordered = seq_list if preserve_order else sorted(seq_list, key=lambda x: (x.family_rank, x.work_token))
        taken = 0
        for c in ordered:
            if len(out) >= limit or taken >= max_take:
                return
            if c.work_token in used:
                continue
            out.append((c, reason))
            used.add(c.work_token)
            taken += 1

    L = limit
    # Deterministic bucket caps (sum may exceed L; take_from stops at limit).
    tail_cap = max(4, min(L, (L * 2 + 4) // 5))
    weak_cap = max(3, min(L - tail_cap, (L + 3) // 5))
    tg_cap = max(2, min(L, (L + 4) // 6))
    cv_cap = max(2, min(L, (L + 4) // 6))
    off_cap = max(2, min(L, (L + 3) // 7))

    tail_order = sorted(eligible, key=lambda c: (-c.family_rank, c.work_token))
    take_from(tail_order, "emerging_bottom_rank_tail", max_take=tail_cap, preserve_order=True)

    weak_order = sorted(
        eligible,
        key=lambda c: (
            c.final_score,
            c.citation_velocity_score if c.citation_velocity_score is not None else 0.0,
            c.topic_growth_score if c.topic_growth_score is not None else 0.0,
            c.family_rank,
            c.work_token,
        ),
    )
    take_from(weak_order, "weak_emerging_signal", max_take=weak_cap, preserve_order=True)

    tg_order = sorted(
        eligible,
        key=lambda c: (
            c.topic_growth_score if c.topic_growth_score is not None else 0.0,
            c.family_rank,
            c.work_token,
        ),
    )
    take_from(tg_order, "low_topic_growth", max_take=tg_cap, preserve_order=True)

    cv_order = sorted(
        eligible,
        key=lambda c: (
            c.citation_velocity_score if c.citation_velocity_score is not None else 0.0,
            c.family_rank,
            c.work_token,
        ),
    )
    take_from(cv_order, "low_citation_velocity", max_take=cv_cap, preserve_order=True)

    def off_key(c: EmergingRow) -> tuple[int, float, int, str]:
        n_topics = len(_topic_names_from_json(c.topics_raw))
        empty_first = 0 if n_topics == 0 else 1
        sem = c.semantic_score if c.semantic_score is not None else 999.0
        return (empty_first, sem, c.family_rank, c.work_token)

    off_order = sorted(eligible, key=off_key)
    take_from(off_order, "off_slice_topic_metadata", max_take=off_cap, preserve_order=True)

    fb_order = sorted(eligible, key=lambda c: (-c.family_rank, c.work_token))
    take_from(fb_order, "fallback_deterministic_fill", max_take=L, preserve_order=True)

    return out[:limit]


def _rows_to_csv_dicts(
    *,
    run: dict[str, Any],
    cluster_ver: str,
    target_gap: str,
    selected: Sequence[tuple[EmergingRow, str]],
) -> list[dict[str, str]]:
    prov = {
        "ranking_run_id": str(run["ranking_run_id"]),
        "ranking_version": str(run["ranking_version"]),
        "corpus_snapshot_version": str(run["corpus_snapshot_version"]),
        "embedding_version": str(run["embedding_version"]),
        "cluster_version": cluster_ver,
        "review_pool_variant": f"{REVIEW_POOL_VARIANT}:{target_gap}",
        "family": FAMILY_EMERGING,
    }
    rows_out: list[dict[str, str]] = []
    for cand, sample_reason in selected:
        topics_list = _topic_names_from_json(cand.topics_raw)
        topics_str = ";".join(topics_list) if topics_list else ""
        be_out = format_bridge_eligible_for_csv(cand.bridge_eligible)
        rows_out.append(
            {
                **prov,
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
                "bridge_eligible": be_out,
                "diversity_penalty": _fmt_float(cand.diversity_penalty),
                "reason_short": cand.reason_short,
                "sample_reason": sample_reason,
                "relevance_label": "",
                "novelty_label": "",
                "bridge_like_label": "",
                "reviewer_notes": "",
            }
        )
    return rows_out


def render_gap_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in CSV_COLUMNS})
    return buf.getvalue()


def markdown_gap_report(
    *,
    ranking_run_id: str,
    target_gap: str,
    run: dict[str, Any],
    cluster_ver: str,
    label_dataset_path: Path,
    selected: Sequence[tuple[EmergingRow, str]],
) -> str:
    ctr: Counter[str] = Counter(sr for _c, sr in selected)
    lines = [
        "# ML targeted gap review worksheet (emerging)",
        "",
        "## Purpose",
        "",
        "This worksheet lists **emerging-family** candidate rows from one **persisted ranking run** chosen to grow "
        "**contrastive offline audit labels**, especially **explicit negatives** for `good_or_acceptable` (relevance) "
        "and `surprising_or_useful` (novelty). It is **not** model training output.",
        "",
        f"**Target gap focus for this export:** `{target_gap}` (use label columns to capture both relevance and novelty; "
        "the gap name names the primary coverage hole in the v2 readiness matrix).",
        "",
        "## Why emerging negatives are the current bottleneck",
        "",
        "The v2 label dataset and readiness matrix show **emerging** slices with **few or no negative** derived targets "
        "for relevance and/or novelty on `rank-ee2ba6c816`, while bridge and undercited already carry more contrast. "
        "A meaningful offline learned baseline would stay premature until emerging has **miss / irrelevant** and "
        "**not_useful / obvious** (or `neither`) examples from the **same** `paper_scores` pool.",
        "",
        "## Selection strategy",
        "",
        "- **Exclude** papers already **fully** labeled for this `ranking_run_id` + `emerging` + `paper_id` in the "
        "audit slice of the provided label dataset (all three label columns non-empty).",
        "- **Prefer** bottom-of-list ranks (tail), **low final_score**, **low topic_growth_score**, **low citation_velocity_score**, "
        "and rows with **sparse topic metadata** or **low semantic_score**, then **deterministic fallback** fill.",
        "- **Reviewer guidance for `bridge_like_label`:** leave blank in the CSV until review; when filling, use "
        "`not_applicable` unless you are **intentionally** judging bridge-like behavior for this emerging row.",
        "",
        "## Row count by sample_reason",
        "",
        "| sample_reason | rows |",
        "| --- | ---: |",
        *[f"| `{k}` | {ctr[k]} |" for k in sorted(ctr.keys())],
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{ranking_run_id}`",
        f"- **target_gap:** `{target_gap}`",
        f"- **ranking_version:** `{run.get('ranking_version')}`",
        f"- **corpus_snapshot_version:** `{run.get('corpus_snapshot_version')}`",
        f"- **embedding_version:** `{run.get('embedding_version')}`",
        f"- **cluster_version:** `{cluster_ver}`",
    ]
    lines.extend(
        [
            f"- **label_dataset:** `{portable_repo_path(label_dataset_path)}`",
            f"- **generated_at:** `{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}`",
            "",
            "## Caveats",
            "",
            *[f"- {c}" for c in VERBATIM_GAP_CAVEATS],
            "",
            "This worksheet supports **targeted contrastive audit labeling** only; do not treat it as ranking validation.",
            "",
        ]
    )
    return "\n".join(lines)


def build_targeted_gap_worksheet(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    target_gap: str,
    limit: int,
) -> tuple[str, str]:
    rid = str(ranking_run_id).strip()
    if not rid:
        raise MLTargetedGapReviewWorksheetError("--ranking-run-id is required and must not be blank")
    if family != FAMILY_EMERGING:
        raise MLTargetedGapReviewWorksheetError(f"--family must be {FAMILY_EMERGING!r} for this command")
    tg = str(target_gap).strip()
    if tg not in TARGET_GAP_CHOICES:
        raise MLTargetedGapReviewWorksheetError(
            f"--target-gap must be one of: {', '.join(sorted(TARGET_GAP_CHOICES))}"
        )
    if not label_dataset_path.is_file():
        raise MLTargetedGapReviewWorksheetError(f"label dataset not found: {label_dataset_path}")

    payload = load_label_dataset_json(label_dataset_path)
    try:
        complete_keys, _ = build_label_key_sets(payload, ranking_run_id=rid)
    except MLContrastiveReviewWorksheetError as e:
        raise MLTargetedGapReviewWorksheetError(str(e)) from e

    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        try:
            run = assert_succeeded_ranking_run(conn, ranking_run_id=rid)
        except MLContrastiveReviewWorksheetError as e:
            raise MLTargetedGapReviewWorksheetError(str(e)) from e
        cfg = _parse_config_json(run.get("config_json"))
        cluster_ver = cluster_version_from_config(cfg) or ""
        raw_rows = fetch_family_scored_rows(conn, ranking_run_id=rid, family=FAMILY_EMERGING)
        pool: list[EmergingRow] = []
        for rw in raw_rows:
            cc = raw_row_to_candidate(FAMILY_EMERGING, dict(rw))
            if cc is None:
                continue
            er = _candidate_to_emerging_row(cc)
            if er is not None:
                pool.append(er)

    selected = select_emerging_gap_rows(pool, complete_keys=complete_keys, limit=limit)
    csv_rows = _rows_to_csv_dicts(run=run, cluster_ver=cluster_ver, target_gap=tg, selected=selected)
    csv_text = render_gap_csv(csv_rows)
    md_text = markdown_gap_report(
        ranking_run_id=rid,
        target_gap=tg,
        run=run,
        cluster_ver=cluster_ver,
        label_dataset_path=label_dataset_path,
        selected=selected,
    )
    return csv_text, md_text


def run_ml_targeted_gap_review_worksheet_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    ranking_run_id: str,
    family: str,
    target_gap: str,
    output_csv: Path,
    markdown_output: Path,
    limit: int,
) -> None:
    csv_text, md_text = build_targeted_gap_worksheet(
        database_url=database_url,
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        family=family,
        target_gap=target_gap,
        limit=limit,
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    output_csv.write_text(csv_text, encoding="utf-8", newline="")
    markdown_output.write_text(md_text, encoding="utf-8", newline="")


__all__ = [
    "CSV_COLUMNS",
    "EmergingRow",
    "FAMILY_EMERGING",
    "GAP_SAMPLE_REASONS",
    "MLTargetedGapReviewWorksheetError",
    "REVIEW_POOL_VARIANT",
    "TARGET_GAP_CHOICES",
    "VERBATIM_GAP_CAVEATS",
    "build_targeted_gap_worksheet",
    "render_gap_csv",
    "run_ml_targeted_gap_review_worksheet_cli",
    "select_emerging_gap_rows",
]
