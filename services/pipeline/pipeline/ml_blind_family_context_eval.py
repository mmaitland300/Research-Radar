"""Read-only blind-source family-context diagnostic.

Evaluates how the heuristic ranking's per-family scores and ranks (carried as worksheet
context on `ml_blind_snapshot_audit` rows) behave on the blind manual labels. This is a
**diagnostic, not validation**: blind rows were not sampled from family top-k rankings,
the family scores/ranks are context fields rather than labels, and `family` stays `null`
on every row.

No DB access, no ranking, no model training, no DB writes.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.ml_label_dataset import BLIND_REVIEW_POOL_VARIANT
from pipeline.ml_offline_baseline_eval import (
    TARGET_FIELDS,
    VALID_FAMILIES,
    roc_auc_mann_whitney,
)

CAVEATS = (
    "This is not validation.",
    "Blind rows were not sampled from family top-k rankings.",
    "Family scores/ranks are context fields, not labels.",
    "Results must not change production ranking defaults.",
    "All rows remain audit_only.",
)


class MLBlindFamilyContextEvalError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _portable_dataset_path(path: Path) -> str:
    """Prefer repo-relative provenance paths to avoid local machine leakage."""
    try:
        repo_root = Path(__file__).resolve().parents[3]
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _load_label_dataset(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise MLBlindFamilyContextEvalError(f"Failed to load label dataset {path}: {e}") from e


def _parse_context_json(raw: Any) -> dict[str, Any]:
    """Parse a `ranking_context_family_*_json` field. Empty/None -> {}; bad JSON -> {}."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        return {}
    s = raw.strip()
    if not s:
        return {}
    try:
        parsed = json.loads(s)
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def filter_blind_rows(payload: dict[str, Any], *, ranking_run_id: str) -> tuple[list[dict[str, Any]], int]:
    """Audit_only blind-snapshot rows for the given ranking_run_id; dedupe by row_id."""
    rows_in = payload.get("rows")
    if not isinstance(rows_in, list):
        raise MLBlindFamilyContextEvalError("label dataset missing 'rows' array")
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    dup = 0
    for r in rows_in:
        if not isinstance(r, dict):
            continue
        if str(r.get("split", "")) != "audit_only":
            continue
        if str(r.get("review_pool_variant", "")) != BLIND_REVIEW_POOL_VARIANT:
            continue
        if str(r.get("ranking_run_id", "")) != ranking_run_id:
            continue
        rid = str(r.get("row_id", ""))
        if rid and rid in seen:
            dup += 1
            continue
        if rid:
            seen.add(rid)
        out.append(r)
    return out, dup


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def compute_family_context_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Per (family, target) summary using ranking_context_family_scores/ranks_json fields.

    `family` here means a key found inside the row's context JSON (e.g. 'bridge'),
    which is **not** an assigned recommendation family for the row.
    """
    families = sorted(VALID_FAMILIES)
    out: dict[str, Any] = {fam: {} for fam in families}
    family_label_class_counts: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: {t: {"positive": 0, "negative": 0, "null": 0} for t in TARGET_FIELDS}
    )

    for fam in families:
        for target in TARGET_FIELDS:
            pos_scores: list[float] = []
            neg_scores: list[float] = []
            pos_ranks: list[int] = []
            neg_ranks: list[int] = []
            pos_n = neg_n = null_n = 0
            with_score_n = 0
            with_rank_n = 0
            for r in rows:
                tval = r.get(target)
                if tval is True:
                    pos_n += 1
                elif tval is False:
                    neg_n += 1
                else:
                    null_n += 1
                fam_scores = _parse_context_json(r.get("ranking_context_family_scores_json"))
                fam_ranks = _parse_context_json(r.get("ranking_context_family_ranks_json"))
                score = _coerce_float(fam_scores.get(fam)) if fam in fam_scores else None
                rank = _coerce_int(fam_ranks.get(fam)) if fam in fam_ranks else None
                if score is not None:
                    with_score_n += 1
                if rank is not None:
                    with_rank_n += 1
                if tval is True:
                    if score is not None:
                        pos_scores.append(score)
                    if rank is not None:
                        pos_ranks.append(rank)
                elif tval is False:
                    if score is not None:
                        neg_scores.append(score)
                    if rank is not None:
                        neg_ranks.append(rank)

            family_label_class_counts[fam][target] = {
                "positive": pos_n,
                "negative": neg_n,
                "null": null_n,
            }

            score_pairs: list[tuple[float, bool]] = []
            for s in pos_scores:
                score_pairs.append((s, True))
            for s in neg_scores:
                score_pairs.append((s, False))
            auc = roc_auc_mann_whitney(score_pairs) if pos_scores and neg_scores else None

            out[fam][target] = {
                "positive_count": pos_n,
                "negative_count": neg_n,
                "null_count": null_n,
                "rows_with_family_score": with_score_n,
                "rows_with_family_rank": with_rank_n,
                "median_rank_positive": float(statistics.median(pos_ranks)) if pos_ranks else None,
                "median_rank_negative": float(statistics.median(neg_ranks)) if neg_ranks else None,
                "mean_family_score_positive": statistics.fmean(pos_scores) if pos_scores else None,
                "mean_family_score_negative": statistics.fmean(neg_scores) if neg_scores else None,
                "diagnostic_auc_family_score": auc,
            }
    return out


def build_blind_family_context_eval_payload(
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
) -> dict[str, Any]:
    path = label_dataset_path.resolve()
    path_for_provenance = _portable_dataset_path(path)
    if not path.is_file():
        raise MLBlindFamilyContextEvalError(f"label dataset not found: {path}")
    label_sha = sha256_file(path)
    raw_payload = _load_label_dataset(path)
    label_version = str(raw_payload.get("dataset_version", ""))

    blind_rows, dup_skip = filter_blind_rows(raw_payload, ranking_run_id=ranking_run_id)
    if not blind_rows:
        raise MLBlindFamilyContextEvalError(
            f"no audit_only blind-snapshot rows found in {path.as_posix()} for ranking_run_id={ranking_run_id!r}"
        )

    family_keys_seen: set[str] = set()
    rows_with_any_score = 0
    rows_with_any_rank = 0
    for r in blind_rows:
        s = _parse_context_json(r.get("ranking_context_family_scores_json"))
        rk = _parse_context_json(r.get("ranking_context_family_ranks_json"))
        if s:
            rows_with_any_score += 1
        if rk:
            rows_with_any_rank += 1
        family_keys_seen.update(str(k) for k in s.keys())
        family_keys_seen.update(str(k) for k in rk.keys())

    metrics = compute_family_context_metrics(blind_rows)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "artifact_type": "ml_blind_family_context_eval",
        "diagnostic_kind": "blind_source_family_context_diagnostic",
        "generated_at": generated_at,
        "provenance": {
            "ranking_run_id": ranking_run_id,
            "label_dataset_path": path_for_provenance,
            "label_dataset_version": label_version,
            "label_dataset_sha256": label_sha,
            "review_pool_variant": BLIND_REVIEW_POOL_VARIANT,
        },
        "caveats": list(CAVEATS),
        "blind_row_summary": {
            "blind_rows_included": len(blind_rows),
            "duplicate_row_id_skipped": dup_skip,
            "rows_with_family_scores_context": rows_with_any_score,
            "rows_with_family_ranks_context": rows_with_any_rank,
            "context_family_keys_seen": sorted(family_keys_seen),
            "all_rows_family_null": all(r.get("family") is None for r in blind_rows),
        },
        "metrics": {
            "by_family_context": metrics,
        },
    }


def markdown_from_blind_family_context_eval(payload: dict[str, Any]) -> str:
    prov = payload["provenance"]
    summary = payload["blind_row_summary"]
    metrics = payload["metrics"]["by_family_context"]
    lines = [
        "# Blind-source family-context diagnostic",
        "",
        "Read-only diagnostic that evaluates how the heuristic ranking's per-family **context "
        "scores and ranks** (carried as worksheet context fields on `ml_blind_snapshot_audit` rows) "
        "behave on the blind manual labels. **This is not validation.** Blind rows were drawn from "
        "a cluster-stratified blind sample of the corpus snapshot, **not** from any family's top-k "
        "ranking, and `family` stays `null` on every row in the dataset.",
        "",
        "## Provenance",
        "",
        f"- **ranking_run_id:** `{prov.get('ranking_run_id')}`",
        f"- **label_dataset_path:** `{prov.get('label_dataset_path')}`",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        f"- **review_pool_variant:** `{prov.get('review_pool_variant')}`",
        f"- **generated_at:** `{payload.get('generated_at')}`",
        "",
        "## Blind row summary",
        "",
        f"- **Blind rows included (audit_only, run match, after row_id dedupe):** {summary['blind_rows_included']}",
        f"- **Duplicate row_id rows skipped:** {summary['duplicate_row_id_skipped']}",
        f"- **Rows with any `ranking_context_family_scores_json`:** {summary['rows_with_family_scores_context']}",
        f"- **Rows with any `ranking_context_family_ranks_json`:** {summary['rows_with_family_ranks_context']}",
        f"- **Context family keys seen:** {', '.join(f'`{k}`' for k in summary['context_family_keys_seen']) or '*(none)*'}",
        f"- **All rows have `family=null`:** {summary['all_rows_family_null']}",
        "",
        "## What this diagnostic answers",
        "",
        "For each `(family_context, target)` pair, it reports row counts and how the family's context "
        "score/rank distributes across positive vs negative manual labels among the blind sample. "
        "AUC is reported **only when both classes exist**, and only as a **diagnostic** of the "
        "context score's ordering on this blind label set - not as production-validation evidence.",
        "",
        "## What this diagnostic is *not*",
        "",
        "- It is **not** validation of the production ranking.",
        "- It does **not** treat blind rows as family-selected ranking outputs.",
        "- It does **not** reassign `family`; rows remain `family=null`.",
        "- It does **not** infer labels from any context field.",
        "- It does **not** support changing production ranking defaults.",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
        "## Headline metrics (per family context)",
        "",
        "| family_context | target | positive | negative | null | median_rank_pos | median_rank_neg | mean_score_pos | mean_score_neg | diagnostic_auc |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for fam in sorted(metrics.keys()):
        for target in TARGET_FIELDS:
            m = metrics[fam].get(target, {})
            lines.append(
                "| `{fam}` | `{tgt}` | {pos} | {neg} | {null} | {mrp} | {mrn} | {msp} | {msn} | {auc} |".format(
                    fam=fam,
                    tgt=target,
                    pos=m.get("positive_count", 0),
                    neg=m.get("negative_count", 0),
                    null=m.get("null_count", 0),
                    mrp=_fmt_num(m.get("median_rank_positive")),
                    mrn=_fmt_num(m.get("median_rank_negative")),
                    msp=_fmt_num(m.get("mean_family_score_positive")),
                    msn=_fmt_num(m.get("mean_family_score_negative")),
                    auc=_fmt_num(m.get("diagnostic_auc_family_score")),
                )
            )
    lines.extend(
        [
            "",
            "See JSON `metrics.by_family_context` for full counts including `rows_with_family_score` "
            "and `rows_with_family_rank`.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _fmt_num(v: Any) -> str:
    if v is None:
        return "*null*"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int,)):
        return str(v)
    try:
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return str(v)


def write_blind_family_context_eval(
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    json_path: Path,
    markdown_path: Path | None,
) -> dict[str, Any]:
    payload = build_blind_family_context_eval_payload(
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_blind_family_context_eval(payload), encoding="utf-8")
    return payload


def run_ml_blind_family_context_eval_cli(
    *,
    label_dataset_path: Path,
    ranking_run_id: str,
    output_json: Path,
    markdown_output: Path | None,
) -> None:
    write_blind_family_context_eval(
        label_dataset_path=label_dataset_path,
        ranking_run_id=ranking_run_id,
        json_path=output_json,
        markdown_path=markdown_output,
    )
