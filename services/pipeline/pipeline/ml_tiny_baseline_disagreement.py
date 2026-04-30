"""Offline audit: rank disagreements between OOF learned_full logits and heuristic final_score (emerging only)."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import psycopg

from pipeline.ml_offline_baseline_eval import _parse_config_json, load_label_dataset, normalize_w_token, sha256_file
from pipeline.repo_paths import portable_repo_path
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
JUDGMENT_BUCKETS = (
    "promoted_positive",
    "promoted_negative",
    "demoted_positive",
    "demoted_negative",
    "stable_positive",
    "stable_negative",
)

DISAGREEMENT_CAVEATS = (
    "This is an offline disagreement audit, not validation of production ranking.",
    "OOF logits come from the same stratified folds as ml-tiny-baseline learned_full; ranks are within this labeled slice only.",
    "Labels are single-reviewer audit labels with ranking-selection bias.",
    "Worksheet-driven sampling (gap/tail pools, contrastive expansions) can align labels with rank-driven slices; "
    "bucket counts are for inspection only, not evidence that learned ranking improves product quality.",
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


def _openalex_work_id(row: dict[str, Any]) -> str | None:
    for key in ("paper_id", "openalex_id", "work_id"):
        token = normalize_w_token(str(row.get(key) or ""))
        if token:
            return token
    return None


def _movement_bucket(delta: int) -> str:
    if delta > 0:
        return "promoted_by_learned_ordering"
    if delta < 0:
        return "demoted_by_learned_ordering"
    return "stable_by_learned_ordering"


def _judgment_bucket(delta: int, *, is_positive: bool) -> str:
    if delta > 0:
        return "promoted_positive" if is_positive else "promoted_negative"
    if delta < 0:
        return "demoted_positive" if is_positive else "demoted_negative"
    return "stable_positive" if is_positive else "stable_negative"


def ordinal_rank_descending(scores: Sequence[float], tie_keys: Sequence[str]) -> list[int]:
    """1 = best (highest score); ties broken by tie_keys ascending."""
    n = len(scores)
    order = sorted(range(n), key=lambda i: (-scores[i], tie_keys[i]))
    ranks = [0] * n
    for pos, idx in enumerate(order, start=1):
        ranks[idx] = pos
    return ranks


def _bucket_counts(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    return {bucket: sum(1 for r in rows if r.get("disagreement_bucket") == bucket) for bucket in JUDGMENT_BUCKETS}


def _interpretation_from_bucket_counts(counts: dict[str, int]) -> str:
    """Neutral summary of judgment buckets; avoids product-quality claims."""
    useful = int(counts.get("promoted_positive", 0)) + int(counts.get("demoted_negative", 0))
    harmful = int(counts.get("promoted_negative", 0)) + int(counts.get("demoted_positive", 0))
    return (
        "Judgment-aware movement vs labels on this slice: "
        f"useful_promotions+demotions={useful}, harmful_promotions+demotions={harmful} "
        "(useful = promoted_positive + demoted_negative; harmful = promoted_negative + demoted_positive). "
        "Offline inspection counts only; not validation and not evidence of product-quality improvement."
    )


def _count_nonempty_field(rows: Sequence[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        raw = str(row.get(field) or "").strip()
        key = raw if raw else "(missing)"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def selection_bias_disclosure_from_joined_rows(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Counts for reviewer context; does not remove selection bias from the experiment."""
    by_variant = _count_nonempty_field(rows, "review_pool_variant")
    by_path = _count_nonempty_field(rows, "source_worksheet_path")
    sample_counts: dict[str, int] = {}
    for row in rows:
        sr = str(row.get("sample_reason") or "").strip()
        if sr:
            sample_counts[sr] = sample_counts.get(sr, 0) + 1
    sample_counts = dict(sorted(sample_counts.items(), key=lambda kv: (-kv[1], kv[0])))
    note = (
        "Audit worksheets often oversample tail or gap slices chosen by the prior ranking pipeline "
        "(for example emerging_bottom_rank_tail in gap-audit pools). Stratified OOF folds do not remove that "
        "selection bias. Bucket counts compare reordering within this labeled slice to manual labels only."
    )
    out: dict[str, Any] = {
        "row_counts_by_review_pool_variant": by_variant,
        "row_counts_by_source_worksheet_path": by_path,
        "note": note,
    }
    if sample_counts:
        out["row_counts_by_sample_reason"] = sample_counts
    return out


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
        positive = bool(rows[i].get(target))
        openalex_work_id = _openalex_work_id(rows[i])
        detail_rows.append(
            {
                "row_id": str(rows[i].get("row_id", "")),
                "openalex_work_id": openalex_work_id,
                "openalex_work_url": f"https://openalex.org/{openalex_work_id}" if openalex_work_id else None,
                "internal_work_id": rows[i].get("work_id"),
                "paper_id": rows[i].get("paper_id"),
                "title": rows[i].get("title"),
                "rank": rows[i].get("rank"),
                "family_rank": rows[i].get("_rank"),
                "experiment_rank": rows[i].get("experiment_rank"),
                "review_pool_variant": rows[i].get("review_pool_variant"),
                "source_worksheet_path": rows[i].get("source_worksheet_path"),
                "source_row_number": rows[i].get("source_row_number"),
                "relevance_label": rows[i].get("relevance_label"),
                "novelty_label": rows[i].get("novelty_label"),
                "bridge_like_label": rows[i].get("bridge_like_label"),
                "reviewer_notes": rows[i].get("reviewer_notes"),
                "final_score": final_scores[i],
                "oof_learned_linear_logit": oof_logits[i],
                "rank_by_final_score": r_heur[i],
                "rank_by_oof_learned_logit": r_learn[i],
                "rank_delta_heuristic_minus_learned": d,
                "movement_bucket": _movement_bucket(d),
                "disagreement_bucket": _judgment_bucket(d, is_positive=positive),
                "good_or_acceptable": rows[i].get("good_or_acceptable"),
                "surprising_or_useful": rows[i].get("surprising_or_useful"),
            }
        )

    promoted = [r for r in detail_rows if r["movement_bucket"] == "promoted_by_learned_ordering"]
    demoted = [r for r in detail_rows if r["movement_bucket"] == "demoted_by_learned_ordering"]
    promoted.sort(key=lambda r: (-r["rank_delta_heuristic_minus_learned"], str(r.get("row_id", ""))))
    demoted.sort(key=lambda r: (r["rank_delta_heuristic_minus_learned"], str(r.get("row_id", ""))))
    bucket_counts = _bucket_counts(detail_rows)

    return {
        "target": target,
        "n_rows": n,
        "promoted_count": len(promoted),
        "demoted_count": len(demoted),
        "stable_count": sum(1 for r in detail_rows if r["movement_bucket"] == "stable_by_learned_ordering"),
        "tie_count": sum(1 for r in detail_rows if r["movement_bucket"] == "stable_by_learned_ordering"),
        "judgment_bucket_counts": bucket_counts,
        "useful_promotion_count": bucket_counts["promoted_positive"],
        "harmful_promotion_count": bucket_counts["promoted_negative"],
        "useful_demotion_count": bucket_counts["demoted_negative"],
        "harmful_demotion_count": bucket_counts["demoted_positive"],
        "top_promotions_by_abs_rank_delta": promoted[:top_n],
        "top_demotions_by_abs_rank_delta": demoted[:top_n],
        "all_rows": detail_rows,
        "movement_definition": (
            "rank_delta_heuristic_minus_learned > 0 means the paper moves up under OOF learned ordering vs final_score "
            "(smaller rank is better). This answers whether the learned model would reorder this slice differently "
            "from the heuristic composite; it does not prove better recommendations in production."
        ),
        "interpretation_note": _interpretation_from_bucket_counts(bucket_counts),
    }


def _ascii_markdown_text(value: Any, *, max_len: int | None = None) -> str:
    text = "" if value is None else str(value)
    replacements = {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": " - ",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2026": "...",
        "\u00a0": " ",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = text.encode("ascii", "replace").decode("ascii")
    text = " ".join(text.split())
    if max_len is not None and len(text) > max_len:
        return text[: max(0, max_len - 3)].rstrip() + "..."
    return text


def _format_value(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    return _ascii_markdown_text(value)


def _format_disagreement_row(row: dict[str, Any]) -> str:
    title = _ascii_markdown_text(row.get("title"), max_len=110)
    notes = _ascii_markdown_text(row.get("reviewer_notes"), max_len=120)
    source = _ascii_markdown_text(row.get("source_worksheet_path"), max_len=110)
    label_bits = (
        f"relevance={_format_value(row.get('relevance_label'))}, "
        f"novelty={_format_value(row.get('novelty_label'))}, "
        f"bridge_like={_format_value(row.get('bridge_like_label'))}"
    )
    rank_bits = (
        f"family_rank={_format_value(row.get('family_rank'))}, "
        f"source_rank={_format_value(row.get('rank'))}"
    )
    return (
        f"- `{_format_value(row.get('openalex_work_id'))}` "
        f"rank_delta={row.get('rank_delta_heuristic_minus_learned')} "
        f"bucket={_format_value(row.get('disagreement_bucket'))} "
        f"final={row.get('final_score'):.6g} logit={row.get('oof_learned_linear_logit'):.6g}; "
        f"{label_bits}; {rank_bits}; "
        f"source={source or 'n/a'}; notes={notes or 'n/a'} - {title}"
    )


def _ascii_markdown_document(markdown: str) -> str:
    return "\n".join(_ascii_markdown_text(line) for line in markdown.splitlines()).rstrip() + "\n"


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
    disclosure = selection_bias_disclosure_from_joined_rows(joined)
    return {
        "artifact_type": "ml_tiny_baseline_disagreement",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": rid,
            "ranking_version": str(run_row.get("ranking_version", "")),
            "corpus_snapshot_version": str(run_row.get("corpus_snapshot_version", "")),
            "embedding_version": str(run_row.get("embedding_version", "")),
            "cluster_version": cluster_version,
            "label_dataset_path": portable_repo_path(path),
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
            "family": EMERGING_FAMILY,
            "targets": list(targets),
            "top_n_listings": top_n,
        },
        "caveats": list(DISAGREEMENT_CAVEATS),
        "join_summary": join_meta,
        "selection_bias_disclosure": disclosure,
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
        "## Selection bias disclosure",
        "",
        _ascii_markdown_text((payload.get("selection_bias_disclosure") or {}).get("note", "")),
        "",
    ]
    disc = payload.get("selection_bias_disclosure") or {}
    if disc.get("row_counts_by_review_pool_variant"):
        lines.append("### Row counts by `review_pool_variant`")
        lines.append("")
        for key, cnt in disc["row_counts_by_review_pool_variant"].items():
            lines.append(f"- `{_ascii_markdown_text(key)}`: `{cnt}`")
        lines.append("")
    if disc.get("row_counts_by_source_worksheet_path"):
        lines.append("### Row counts by `source_worksheet_path`")
        lines.append("")
        for key, cnt in disc["row_counts_by_source_worksheet_path"].items():
            lines.append(f"- `{_ascii_markdown_text(key)}`: `{cnt}`")
        lines.append("")
    if disc.get("row_counts_by_sample_reason"):
        lines.append("### Row counts by `sample_reason` (when present on worksheet rows)")
        lines.append("")
        for key, cnt in disc["row_counts_by_sample_reason"].items():
            lines.append(f"- `{_ascii_markdown_text(key)}`: `{cnt}`")
        lines.append("")
    for tgt, block in payload.get("targets", {}).items():
        counts = block.get("judgment_bucket_counts") or {}
        lines.extend(
            [
                f"## Target `{tgt}`",
                "",
                f"- **n_rows:** `{block.get('n_rows')}`",
                f"- **promoted / demoted / stable:** `{block.get('promoted_count')}` / `{block.get('demoted_count')}` / `{block.get('stable_count', block.get('tie_count'))}`",
                f"- **useful promotions:** `{block.get('useful_promotion_count')}` (`promoted_positive`)",
                f"- **harmful promotions:** `{block.get('harmful_promotion_count')}` (`promoted_negative`)",
                f"- **useful demotions:** `{block.get('useful_demotion_count')}` (`demoted_negative`)",
                f"- **harmful demotions:** `{block.get('harmful_demotion_count')}` (`demoted_positive`)",
                f"- **stable positives / negatives:** `{counts.get('stable_positive', 0)}` / `{counts.get('stable_negative', 0)}`",
                "",
                "### Interpretation",
                "",
                _ascii_markdown_text(block.get("interpretation_note")),
                "",
                "### Top promotions (learned ranks higher than final_score)",
                "",
            ]
        )
        for r in block.get("top_promotions_by_abs_rank_delta", []):
            lines.append(_format_disagreement_row(r))
        lines.extend(["", "### Top demotions", ""])
        for r in block.get("top_demotions_by_abs_rank_delta", []):
            lines.append(_format_disagreement_row(r))
        lines.append("")
    return _ascii_markdown_document("\n".join(lines))


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
