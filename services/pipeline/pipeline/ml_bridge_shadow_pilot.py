"""Bridge shadow pilot: apply the frozen v2 ML model to all 528 Bridge candidates,
compute hybrid_bridge_score_50_50, and compare with the current Bridge top-20.

The "shadow" label means this ranking exists only as an audit artifact. Nothing is
served, no API or UI is changed, and no ranking tables are written.

What this module does:
  1. Load the frozen StandardScaler + LogisticRegression from the v2 scorer artifact.
  2. Query all Bridge family paper_scores rows for the clustering-enabled ranking run
     (rank-5a7efa5ca3) to get bridge_score, final_score, and the current family rank.
  3. Query 1536-dim embeddings for all 528 works.
  4. Score every work with the frozen model → ml_probability.
  5. Compute rank percentiles across all 528 for ml_probability and bridge_score.
  6. hybrid_bridge_score_50_50 = 0.5 * rank_pct(bridge_score) + 0.5 * rank_pct(ml_prob).
  7. Rank all 528 by hybrid score → hybrid_rank.
  8. Compare current top-20 vs hybrid top-20: record promoted and demoted papers.
  9. Produce four disagreement buckets for the follow-on labeling worksheet.
 10. Write the pilot artifact (JSON + Markdown) and a blank worksheet CSV with a
     context sidecar for human labeling.

SELECT-only DB access. No DB writes, no ranking writes, no serving changes.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.openalex_ids import normalize_w_token
from pipeline.repo_paths import portable_repo_path

PILOT_VERSION = "ml-bridge-shadow-pilot-v1"
ARTIFACT_TYPE = "ml_bridge_shadow_pilot"
WORKSHEET_VERSION = "ml-bridge-shadow-pilot-disagreements-v1"
WORKSHEET_ARTIFACT_TYPE = "ml_bridge_shadow_pilot_disagreements_context"
V2_SCORER_ARTIFACT_TYPE = "ml_offline_bridge_recommendable_scorer_v2"
V2_SCORER_VERSION = "ml-offline-bridge-recommendable-scorer-v2"
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
FAMILY = "bridge"
TOP_K = 20
MIN_BRIDGE_SCORE_COVERAGE = 0.90  # require ≥90% of candidates to have non-null bridge_score
DISAGREEMENT_BUCKET_LIMIT = 10    # max per bucket in the worksheet
EXPECTED_CANDIDATE_COUNT = 528

CAVEATS = (
    "This is not validation.",
    "ml_probability values come from a frozen full-fit model; they are in-sample for the "
    "100 labeled rows and out-of-sample for the remaining 428.",
    "Rank percentiles are computed across all 528 Bridge candidates (full-pool pilot scope).",
    "bridge_score comes from rank-5a7efa5ca3 (cluster-enabled run); it was NULL in rank-83787b91ef.",
    "No API, UI, serving, or ranking-table changes are made or authorized.",
    "This shadow pilot is a pre-serving diagnostic only.",
)

WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|merge|grant|revoke|vacuum|reindex|copy)\b"
)


class MLBridgeShadowPilotError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _execute_select(cur: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    stripped = sql.strip()
    lowered = stripped.lower()
    if not lowered.startswith("select"):
        raise MLBridgeShadowPilotError("DB safety violation: SQL must start with SELECT")
    if WRITE_SQL_RE.search(lowered):
        raise MLBridgeShadowPilotError("DB safety violation: SQL contains write/DDL verb")
    return cur.execute(sql, tuple(params or ()))


# ---------------------------------------------------------------------------
# Frozen scorer loading and inference
# ---------------------------------------------------------------------------

def _load_frozen_scorer(v2_artifact: Mapping[str, Any]) -> dict[str, Any]:
    if v2_artifact.get("artifact_type") != V2_SCORER_ARTIFACT_TYPE:
        raise MLBridgeShadowPilotError(
            f"v2 scorer artifact_type must be {V2_SCORER_ARTIFACT_TYPE!r}; "
            f"got {v2_artifact.get('artifact_type')!r}"
        )
    if v2_artifact.get("scorer_version") != V2_SCORER_VERSION:
        raise MLBridgeShadowPilotError(
            f"v2 scorer scorer_version must be {V2_SCORER_VERSION!r}; "
            f"got {v2_artifact.get('scorer_version')!r}"
        )
    frozen = v2_artifact.get("frozen_scorer")
    if not isinstance(frozen, dict):
        raise MLBridgeShadowPilotError("v2 scorer artifact missing frozen_scorer object")
    for key in ("scaler_mean", "scaler_scale", "coef", "intercept"):
        if frozen.get(key) is None:
            raise MLBridgeShadowPilotError(f"frozen_scorer missing required field {key!r}")
    mean = [float(v) for v in frozen["scaler_mean"]]
    scale = [float(v) for v in frozen["scaler_scale"]]
    coef = [float(v) for v in frozen["coef"]]
    intercept = float(frozen["intercept"])
    n = len(mean)
    if len(scale) != n or len(coef) != n:
        raise MLBridgeShadowPilotError(
            f"frozen_scorer dimension mismatch: mean={len(mean)}, scale={len(scale)}, coef={len(coef)}"
        )
    return {
        "scaler_mean": mean,
        "scaler_scale": scale,
        "coef": coef,
        "intercept": intercept,
        "embedding_dimensions": n,
        "embedding_version": frozen.get("embedding_version", EMBEDDING_VERSION),
    }


def _sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    exp_x = math.exp(x)
    return exp_x / (1.0 + exp_x)


def _score_with_frozen_model(
    vector: Sequence[float],
    *,
    scaler_mean: Sequence[float],
    scaler_scale: Sequence[float],
    coef: Sequence[float],
    intercept: float,
) -> float:
    n = len(scaler_mean)
    if len(vector) != n:
        raise MLBridgeShadowPilotError(
            f"embedding dimension {len(vector)} does not match frozen scorer dimension {n}"
        )
    log_odds = intercept
    for i in range(n):
        sc = scaler_scale[i]
        x_scaled = (vector[i] - scaler_mean[i]) / (sc if sc > 0 else 1.0)
        log_odds += coef[i] * x_scaled
    return _sigmoid(log_odds)


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def _fetch_bridge_candidates(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
) -> list[dict[str, Any]]:
    sql = """
        SELECT
            ps.work_id                                                        AS work_id_int,
            w.openalex_id,
            w.title,
            ps.bridge_score,
            ps.final_score,
            ROW_NUMBER() OVER (
                ORDER BY ps.final_score DESC, ps.work_id ASC
            )                                                                 AS current_family_rank
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = %s
        ORDER BY ps.final_score DESC, ps.work_id ASC
    """
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(cur, sql.strip(), (ranking_run_id, FAMILY))
        rows = cur.fetchall()
    if not rows:
        raise MLBridgeShadowPilotError(
            f"no bridge family rows found for ranking_run_id={ranking_run_id!r}"
        )
    return [dict(r) for r in rows]


def _fetch_embeddings(
    conn: psycopg.Connection,
    *,
    work_ids: list[int],
    embedding_version: str,
) -> dict[int, list[float]]:
    sql = """
        SELECT work_id, vector
        FROM embeddings
        WHERE embedding_version = %s
          AND work_id = ANY(%s)
    """
    with conn.cursor() as cur:
        _execute_select(cur, sql.strip(), (embedding_version, work_ids))
        fetched = cur.fetchall()

    result: dict[int, list[float]] = {}
    for row in fetched:
        if isinstance(row, Mapping):
            wid = int(row.get("work_id", 0))
            raw_vector = row.get("vector")
        else:
            wid = int(row[0])
            raw_vector = row[1]
        if isinstance(raw_vector, str):
            try:
                raw_vector = json.loads(raw_vector)
            except json.JSONDecodeError as exc:
                raise MLBridgeShadowPilotError(f"embedding vector is not valid JSON for work {wid}: {exc}") from exc
        if not isinstance(raw_vector, (list, tuple)):
            raise MLBridgeShadowPilotError(f"embedding vector for work {wid} is not an array")
        result[wid] = [float(v) for v in raw_vector]
    return result


# ---------------------------------------------------------------------------
# Rank percentile
# ---------------------------------------------------------------------------

def _rank_pct_from_pairs(pairs: list[tuple[str, float]]) -> dict[str, float]:
    """Rank percentile; higher score → higher pct. Keyed by work_id token."""
    n = len(pairs)
    if n == 0:
        return {}
    if n == 1:
        return {pairs[0][0]: 1.0}
    ordered = sorted(pairs, key=lambda t: (-t[1], t[0]))
    out: dict[str, float] = {}
    i = 0
    while i < n:
        j = i + 1
        val = ordered[i][1]
        while j < n and ordered[j][1] == val:
            j += 1
        pct = 1.0 - ((i + j) / 2.0) / n
        for k in range(i, j):
            out[ordered[k][0]] = float(pct)
        i = j
    return out


# ---------------------------------------------------------------------------
# Pilot scoring
# ---------------------------------------------------------------------------

def _score_candidates(
    candidates: list[dict[str, Any]],
    embeddings: dict[int, list[float]],
    *,
    frozen: dict[str, Any],
) -> list[dict[str, Any]]:
    scaler_mean = frozen["scaler_mean"]
    scaler_scale = frozen["scaler_scale"]
    coef = frozen["coef"]
    intercept = frozen["intercept"]

    missing_embeddings = [
        int(r["work_id_int"])
        for r in candidates
        if int(r["work_id_int"]) not in embeddings
    ]
    if missing_embeddings:
        raise MLBridgeShadowPilotError(
            f"{len(missing_embeddings)} bridge candidates have no embedding; "
            f"first few: {missing_embeddings[:5]}"
        )

    scored: list[dict[str, Any]] = []
    for row in candidates:
        wid = int(row["work_id_int"])
        token = normalize_w_token(str(row.get("openalex_id") or ""))
        ml_prob = _score_with_frozen_model(
            embeddings[wid],
            scaler_mean=scaler_mean,
            scaler_scale=scaler_scale,
            coef=coef,
            intercept=intercept,
        )
        scored.append({
            "work_id_int": wid,
            "openalex_id": str(row.get("openalex_id") or ""),
            "work_id_token": token or "",
            "title": str(row.get("title") or ""),
            "bridge_score": _as_float(row.get("bridge_score")),
            "final_score": _as_float(row.get("final_score")),
            "current_family_rank": int(row["current_family_rank"]),
            "ml_probability": ml_prob,
        })
    return scored


def _add_hybrid_ranks(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add rank percentiles and hybrid score + rank to scored candidates in place."""
    ml_pairs = [(r["work_id_token"], r["ml_probability"]) for r in scored]
    ml_pct = _rank_pct_from_pairs(ml_pairs)

    bridge_pairs = [
        (r["work_id_token"], r["bridge_score"])
        for r in scored
        if r["bridge_score"] is not None
    ]
    bridge_pct = _rank_pct_from_pairs(bridge_pairs)

    for row in scored:
        token = row["work_id_token"]
        mp = float(ml_pct.get(token, 0.0))
        bp = bridge_pct.get(token)
        row["ml_rank_pct"] = mp
        row["bridge_score_rank_pct"] = float(bp) if bp is not None else None
        if bp is not None:
            row["hybrid_score"] = 0.5 * float(bp) + 0.5 * mp
        else:
            row["hybrid_score"] = None

    # Hybrid rank: descending hybrid_score; papers with null hybrid_score ranked last
    sorted_by_hybrid = sorted(
        scored,
        key=lambda r: (
            -(r["hybrid_score"] if r["hybrid_score"] is not None else -999.0),
            r["work_id_token"],
        ),
    )
    for rank, row in enumerate(sorted_by_hybrid, start=1):
        row["hybrid_rank"] = rank

    return scored


# ---------------------------------------------------------------------------
# Comparison tables
# ---------------------------------------------------------------------------

def _top_k_tokens(candidates: Sequence[Mapping[str, Any]], *, rank_field: str, k: int) -> set[str]:
    top = [r for r in candidates if isinstance(r.get(rank_field), int) and r[rank_field] <= k]
    return {str(r.get("work_id_token") or "") for r in top}


def _comparison_tables(ranked: list[dict[str, Any]], *, k: int = TOP_K) -> dict[str, Any]:
    current_top_tokens = _top_k_tokens(ranked, rank_field="current_family_rank", k=k)
    hybrid_top_tokens = _top_k_tokens(ranked, rank_field="hybrid_rank", k=k)

    promoted_tokens = hybrid_top_tokens - current_top_tokens
    demoted_tokens = current_top_tokens - hybrid_top_tokens
    stable_tokens = current_top_tokens & hybrid_top_tokens

    def _pick_rows(tokens: set[str]) -> list[dict[str, Any]]:
        rows = [r for r in ranked if r.get("work_id_token") in tokens]
        return sorted(rows, key=lambda r: r.get("hybrid_rank", 9999))

    promoted_rows = _pick_rows(promoted_tokens)
    demoted_rows = _pick_rows(demoted_tokens)
    stable_rows = _pick_rows(stable_tokens)

    return {
        "top_k": k,
        "current_top_k_count": len(current_top_tokens),
        "hybrid_top_k_count": len(hybrid_top_tokens),
        "promoted_count": len(promoted_tokens),
        "demoted_count": len(demoted_tokens),
        "stable_count": len(stable_tokens),
        "promoted": promoted_rows,    # in hybrid top-k but NOT current top-k
        "demoted": demoted_rows,      # in current top-k but NOT hybrid top-k
        "stable": stable_rows,        # in both
    }


def _disagreement_buckets(
    ranked: list[dict[str, Any]], *, limit: int = DISAGREEMENT_BUCKET_LIMIT
) -> dict[str, Any]:
    covered = [r for r in ranked if r.get("bridge_score_rank_pct") is not None]

    high_ml_low_bridge = sorted(
        covered,
        key=lambda r: (
            -(float(r["ml_rank_pct"]) - float(r["bridge_score_rank_pct"])),  # type: ignore[arg-type]
            r.get("work_id_token", ""),
        ),
    )[:limit]

    high_bridge_low_ml = sorted(
        covered,
        key=lambda r: (
            -(float(r["bridge_score_rank_pct"]) - float(r["ml_rank_pct"])),  # type: ignore[arg-type]
            r.get("work_id_token", ""),
        ),
    )[:limit]

    return {
        "note": (
            "high_ml_low_bridge: ML model ranks these highly but bridge_score does not; "
            "high_bridge_low_ml: bridge_score ranks these highly but ML model does not."
        ),
        "high_ml_low_bridge": high_ml_low_bridge,
        "high_bridge_low_ml": high_bridge_low_ml,
    }


# ---------------------------------------------------------------------------
# Worksheet generation
# ---------------------------------------------------------------------------

WORKSHEET_COLUMNS = (
    "work_id",
    "title",
    "abstract_preview",
    "current_family_rank",
    "hybrid_rank",
    "disagreement_bucket",
    "bridge_like_label",
    "relevance_label",
    "notes",
)

REVIEW_COLUMNS = {"bridge_like_label", "relevance_label", "notes"}


def _build_worksheet_rows(
    comparison: dict[str, Any],
    buckets: dict[str, Any],
) -> list[dict[str, Any]]:
    """Collect worksheet rows from promoted, demoted, and disagreement bucket papers.

    Deduplicates by work_id_token. Each paper appears once, in the first bucket
    it was found in.
    """
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []

    def _add(papers: Sequence[Mapping[str, Any]], bucket_label: str) -> None:
        for p in papers:
            token = str(p.get("work_id_token") or "")
            if not token or token in seen:
                continue
            seen.add(token)
            rows.append({
                "work_id": p.get("openalex_id") or token,
                "title": p.get("title", ""),
                "abstract_preview": "",   # filled by caller or left blank
                "current_family_rank": p.get("current_family_rank"),
                "hybrid_rank": p.get("hybrid_rank"),
                "disagreement_bucket": bucket_label,
                "bridge_like_label": "",
                "relevance_label": "",
                "notes": "",
                # Hidden context (not in CSV header, stored in sidecar)
                "_work_id_int": p.get("work_id_int"),
                "_ml_probability": p.get("ml_probability"),
                "_bridge_score": p.get("bridge_score"),
                "_ml_rank_pct": p.get("ml_rank_pct"),
                "_bridge_score_rank_pct": p.get("bridge_score_rank_pct"),
                "_hybrid_score": p.get("hybrid_score"),
            })

    _add(comparison["promoted"], "promoted_by_hybrid")
    _add(comparison["demoted"], "demoted_by_hybrid")
    _add(buckets["high_ml_low_bridge"], "high_ml_low_bridge_score")
    _add(buckets["high_bridge_low_ml"], "high_bridge_score_low_ml")
    return rows


def _write_worksheet_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(WORKSHEET_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in WORKSHEET_COLUMNS})


def _write_worksheet_sidecar(
    rows: list[dict[str, Any]],
    *,
    ranking_run_id: str,
    path: Path,
    blank_csv_sha256: str,
    pilot_artifact_sha256: str,
) -> None:
    sidecar = {
        "artifact_type": WORKSHEET_ARTIFACT_TYPE,
        "worksheet_version": WORKSHEET_VERSION,
        "ranking_run_id": ranking_run_id,
        "embedding_version": EMBEDDING_VERSION,
        "generated_at": _now_iso_z(),
        "blank_csv_sha256": blank_csv_sha256,
        "pilot_artifact_sha256": pilot_artifact_sha256,
        "rows": [
            {
                "work_id": r.get("work_id"),
                "title": r.get("title"),
                "disagreement_bucket": r.get("disagreement_bucket"),
                "current_family_rank": r.get("current_family_rank"),
                "hybrid_rank": r.get("hybrid_rank"),
                "work_id_int": r.get("_work_id_int"),
                "ml_probability": r.get("_ml_probability"),
                "bridge_score": r.get("_bridge_score"),
                "ml_rank_pct": r.get("_ml_rank_pct"),
                "bridge_score_rank_pct": r.get("_bridge_score_rank_pct"),
                "hybrid_score": r.get("_hybrid_score"),
            }
            for r in rows
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sidecar, indent=2, sort_keys=False) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------

def markdown_from_shadow_pilot(payload: Mapping[str, Any]) -> str:
    run_id = payload.get("ranking_run_id", "?")
    n = payload.get("candidate_count", 0)
    cov = payload.get("bridge_score_coverage", {})
    comp = payload.get("top_20_comparison", {})
    ws = payload.get("worksheet_summary", {})

    def _row_summary(r: Mapping[str, Any]) -> str:
        title = str(r.get("title") or "")[:60]
        bs = r.get("bridge_score")
        bridge_str = f"{bs:.3f}" if isinstance(bs, float) else "N/A"
        return (
            f"  - rank {r.get('current_family_rank')} → {r.get('hybrid_rank')} | "
            f"ml={r.get('ml_probability', 0.0):.3f} bridge={bridge_str} | "
            f"{title}"
        )

    lines = [
        "# Bridge shadow pilot v1",
        "",
        "Offline re-ranking: frozen v2 ML model + bridge_score → hybrid_bridge_score_50_50 "
        f"across all {n} Bridge candidates. Not validation; no serving change.",
        "",
        f"- Ranking run (bridge_score source): `{run_id}`",
        f"- Embedding version: `{EMBEDDING_VERSION}`",
        f"- Candidates: {n}",
        f"- bridge_score coverage: {cov.get('non_null_count', 0)}/{n} "
        f"({cov.get('coverage_fraction', 0.0):.1%})",
        "",
        "## Top-20 comparison",
        "",
        f"- Promoted (hybrid top-20, not current top-20): **{comp.get('promoted_count', 0)}**",
        f"- Demoted (current top-20, not hybrid top-20): **{comp.get('demoted_count', 0)}**",
        f"- Stable (in both): **{comp.get('stable_count', 0)}**",
        "",
        "### Promoted papers",
        "",
    ]
    for r in comp.get("promoted", []):
        lines.append(_row_summary(r))
    lines.extend(["", "### Demoted papers", ""])
    for r in comp.get("demoted", []):
        lines.append(_row_summary(r))
    lines.extend(["", "### Stable papers", ""])
    for r in comp.get("stable", []):
        lines.append(_row_summary(r))
    lines.extend([
        "",
        "## Worksheet",
        "",
        f"- Total worksheet rows: {ws.get('row_count', 0)}",
        f"- Bucket breakdown: {ws.get('bucket_counts', {})}",
        "",
        "## Caveats",
        "",
        *[f"- {c}" for c in payload.get("caveats", [])],
        "",
    ])
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def _input_record(name: str, path: Path) -> dict[str, str]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise MLBridgeShadowPilotError(f"required input not found: {resolved}")
    return {"name": name, "path": portable_repo_path(resolved), "sha256": sha256_file(resolved)}


def build_ml_bridge_shadow_pilot_payload(
    *,
    v2_scorer_path: Path,
    ranking_run_id: str,
    database_url: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build the pilot payload.

    Returns (payload, worksheet_rows).
    worksheet_rows include hidden _context fields for the sidecar.
    """
    ranking_run_id = ranking_run_id.strip()
    if not ranking_run_id:
        raise MLBridgeShadowPilotError("ranking_run_id must be non-empty")

    scorer_path = v2_scorer_path.resolve()
    if not scorer_path.is_file():
        raise MLBridgeShadowPilotError(f"v2 scorer artifact not found: {scorer_path}")

    v2_artifact = json.loads(scorer_path.read_text(encoding="utf-8"))
    if not isinstance(v2_artifact, dict):
        raise MLBridgeShadowPilotError("v2 scorer artifact must be a JSON object")

    frozen = _load_frozen_scorer(v2_artifact)

    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION READ ONLY")
        candidates = _fetch_bridge_candidates(conn, ranking_run_id=ranking_run_id)
        work_ids = [int(r["work_id_int"]) for r in candidates]
        embeddings = _fetch_embeddings(conn, work_ids=work_ids, embedding_version=frozen["embedding_version"])

    # Bridge score coverage check
    non_null_bridge = sum(1 for r in candidates if r.get("bridge_score") is not None)
    coverage_fraction = non_null_bridge / len(candidates) if candidates else 0.0
    if coverage_fraction < MIN_BRIDGE_SCORE_COVERAGE:
        raise MLBridgeShadowPilotError(
            f"bridge_score coverage {coverage_fraction:.1%} below minimum {MIN_BRIDGE_SCORE_COVERAGE:.0%}. "
            "Run cluster-works + ranking-run --cluster-version first."
        )

    # Embedding coverage check
    missing = [wid for wid in work_ids if wid not in embeddings]
    if missing:
        raise MLBridgeShadowPilotError(
            f"{len(missing)} candidates are missing embeddings for {frozen['embedding_version']!r}: "
            f"first few: {missing[:5]}"
        )

    scored = _score_candidates(candidates, embeddings, frozen=frozen)
    scored = _add_hybrid_ranks(scored)
    comparison = _comparison_tables(scored)
    buckets = _disagreement_buckets(scored)
    worksheet_rows = _build_worksheet_rows(comparison, buckets)

    bucket_counts: Counter[str] = Counter(r.get("disagreement_bucket") for r in worksheet_rows)

    payload: dict[str, Any] = {
        "artifact_type": ARTIFACT_TYPE,
        "pilot_version": PILOT_VERSION,
        "generated_at": _now_iso_z(),
        "ranking_run_id": ranking_run_id,
        "embedding_version": frozen["embedding_version"],
        "embedding_dimensions": frozen["embedding_dimensions"],
        "candidate_count": len(scored),
        "bridge_score_coverage": {
            "non_null_count": non_null_bridge,
            "total_count": len(candidates),
            "coverage_fraction": round(coverage_fraction, 4),
        },
        "frozen_scorer_provenance": {
            "scorer_version": v2_artifact.get("scorer_version"),
            "artifact_type": v2_artifact.get("artifact_type"),
            "source_path": portable_repo_path(scorer_path),
            "source_sha256": sha256_file(scorer_path),
            "embedding_dimensions": frozen["embedding_dimensions"],
        },
        "top_20_comparison": comparison,
        "disagreement_buckets": buckets,
        "worksheet_summary": {
            "row_count": len(worksheet_rows),
            "bucket_counts": dict(bucket_counts),
        },
        "caveats": list(CAVEATS),
        "db_access": "SELECT-only on paper_scores, works, embeddings",
        "db_writes": False,
        "production_authorization": False,
        "all_candidates": scored,
    }
    return payload, worksheet_rows


# ---------------------------------------------------------------------------
# Writer + CLI runner
# ---------------------------------------------------------------------------

def write_ml_bridge_shadow_pilot(
    *,
    v2_scorer_path: Path,
    ranking_run_id: str,
    database_url: str,
    json_path: Path,
    markdown_path: Path | None,
    worksheet_csv_path: Path | None,
    worksheet_sidecar_path: Path | None,
) -> dict[str, Any]:
    payload, worksheet_rows = build_ml_bridge_shadow_pilot_payload(
        v2_scorer_path=v2_scorer_path,
        ranking_run_id=ranking_run_id,
        database_url=database_url,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_from_shadow_pilot(payload), encoding="utf-8")
    if worksheet_csv_path is not None:
        _write_worksheet_csv(worksheet_rows, worksheet_csv_path)
        if worksheet_sidecar_path is not None:
            blank_sha = sha256_file(worksheet_csv_path)
            pilot_sha = sha256_file(json_path)
            _write_worksheet_sidecar(
                worksheet_rows,
                ranking_run_id=ranking_run_id,
                path=worksheet_sidecar_path,
                blank_csv_sha256=blank_sha,
                pilot_artifact_sha256=pilot_sha,
            )
    return payload


def run_ml_bridge_shadow_pilot_cli(
    *,
    v2_scorer_path: Path,
    ranking_run_id: str,
    database_url: str | None,
    output_json: Path,
    markdown_output: Path | None,
    worksheet_csv: Path | None,
    worksheet_sidecar: Path | None,
) -> None:
    url = database_url or database_url_from_env()
    write_ml_bridge_shadow_pilot(
        v2_scorer_path=v2_scorer_path,
        ranking_run_id=ranking_run_id,
        database_url=url,
        json_path=output_json,
        markdown_path=markdown_output,
        worksheet_csv_path=worksheet_csv,
        worksheet_sidecar_path=worksheet_sidecar,
    )


__all__ = [
    "ARTIFACT_TYPE",
    "MLBridgeShadowPilotError",
    "PILOT_VERSION",
    "WORKSHEET_ARTIFACT_TYPE",
    "WORKSHEET_VERSION",
    "build_ml_bridge_shadow_pilot_payload",
    "markdown_from_shadow_pilot",
    "run_ml_bridge_shadow_pilot_cli",
    "write_ml_bridge_shadow_pilot",
]
