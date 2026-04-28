"""Read-only cluster inspection artifact for one explicit snapshot/embedding/cluster identity."""

from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from pipeline.bootstrap_loader import database_url_from_env

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
    "we",
    "this",
    "these",
    "those",
    "their",
    "our",
    "using",
    "use",
    "via",
    "study",
    "analysis",
}


class ClusterInspectionError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _tokenize(text: str) -> list[str]:
    out: list[str] = []
    cur: list[str] = []
    for ch in text.lower():
        if ch.isalnum():
            cur.append(ch)
            continue
        if cur:
            out.append("".join(cur))
            cur = []
    if cur:
        out.append("".join(cur))
    return [t for t in out if len(t) >= 3 and t not in STOPWORDS]


def _parse_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _median_int(values: list[int]) -> int | None:
    if not values:
        return None
    return int(statistics.median(values))


def _require_non_blank(value: str, flag: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise ClusterInspectionError(f"{flag} is required and must not be blank", code=2)
    return cleaned


def build_cluster_inspection_payload(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
) -> dict[str, Any]:
    snap = _require_non_blank(corpus_snapshot_version, "--corpus-snapshot-version")
    emb = _require_non_blank(embedding_version, "--embedding-version")
    clu = _require_non_blank(cluster_version, "--cluster-version")

    run_row = conn.execute(
        """
        SELECT status, algorithm, counts_json, config_json
        FROM clustering_runs
        WHERE cluster_version = %s
          AND corpus_snapshot_version = %s
          AND embedding_version = %s
        """,
        (clu, snap, emb),
    ).fetchone()
    if run_row is None:
        raise ClusterInspectionError(
            "No clustering_runs row matches cluster_version="
            f"{clu!r}, corpus_snapshot_version={snap!r}, embedding_version={emb!r}.",
            code=2,
        )
    status = str(run_row[0])
    if status != "succeeded":
        raise ClusterInspectionError(
            f"clustering run must be succeeded (cluster_version={clu!r}, status={status!r}).", code=2
        )
    counts_json = _parse_json(run_row[2])
    config_json = _parse_json(run_row[3])

    works = conn.execute(
        """
        SELECT
            w.id,
            w.title,
            w.abstract,
            w.year,
            w.citation_count,
            w.source_slug,
            c.cluster_id,
            e.work_id IS NOT NULL AS has_embedding,
            row_src.payload AS raw_payload
        FROM works w
        LEFT JOIN clusters c
          ON c.work_id = w.id
         AND c.cluster_version = %s
        LEFT JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        LEFT JOIN LATERAL (
            SELECT r.payload
            FROM raw_openalex_works r
            WHERE r.openalex_id = w.openalex_id
              AND r.source_snapshot_version = %s
            ORDER BY r.fetched_at DESC
            LIMIT 1
        ) AS row_src ON TRUE
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
        ORDER BY w.id ASC
        """,
        (clu, emb, snap, snap),
    ).fetchall()

    total_works = len(works)
    if total_works == 0:
        raise ClusterInspectionError(f"No included works found for corpus_snapshot_version={snap!r}.", code=2)

    missing_embedding = sum(1 for row in works if not bool(row[7]))
    missing_cluster_assignment = sum(1 for row in works if row[6] is None)
    if missing_embedding > 0:
        raise ClusterInspectionError(
            f"missing embeddings for {missing_embedding} included works in snapshot {snap!r} and embedding_version {emb!r}.",
            code=2,
        )
    if missing_cluster_assignment > 0:
        raise ClusterInspectionError(
            f"missing cluster assignments for {missing_cluster_assignment} included works in snapshot {snap!r} and cluster_version {clu!r}.",
            code=2,
        )

    cluster_rows: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in works:
        cluster_rows[str(row[6])].append(row)

    size_values = sorted(len(v) for v in cluster_rows.values())
    min_cluster_size = size_values[0] if size_values else 0
    max_cluster_size = size_values[-1] if size_values else 0
    median_cluster_size = int(statistics.median(size_values)) if size_values else 0
    imbalance_ratio = (max_cluster_size / min_cluster_size) if min_cluster_size > 0 else None
    tiny_cluster_count = sum(1 for size in size_values if size < 5)
    dominant_cluster_share = (max_cluster_size / total_works) if total_works else 0.0

    expected_k = counts_json.get("cluster_count")
    if not isinstance(expected_k, int):
        cfg_k = config_json.get("cluster_count")
        expected_k = int(cfg_k) if isinstance(cfg_k, int) else None

    warnings: list[str] = []
    if tiny_cluster_count > 0:
        warnings.append(f"Found {tiny_cluster_count} tiny cluster(s) with size < 5.")
    if dominant_cluster_share > 0.30:
        warnings.append(f"Dominant cluster share is high ({dominant_cluster_share:.3f} > 0.300).")
    if expected_k is not None and expected_k != len(cluster_rows):
        warnings.append(
            f"Observed cluster_count {len(cluster_rows)} does not match expected k {expected_k}."
        )

    cluster_summaries: list[dict[str, Any]] = []
    provenance_present_any = False
    for cluster_id in sorted(cluster_rows.keys()):
        rows = cluster_rows[cluster_id]
        size = len(rows)
        pct = size / total_works if total_works else 0.0
        sorted_titles = sorted(
            rows,
            key=lambda r: (
                -int(r[4] or 0),  # citation_count desc
                -int(r[3] or 0),  # year desc
                int(r[0]),  # id asc
            ),
        )
        representative_titles = [str(r[1] or "").strip() for r in sorted_titles if str(r[1] or "").strip()][:10]

        years = [int(r[3]) for r in rows if isinstance(r[3], int)]
        citations = [int(r[4]) for r in rows if r[4] is not None]
        source_mix: Counter[str] = Counter()
        bucket_mix: Counter[str] = Counter()
        terms: Counter[str] = Counter()
        for row in rows:
            source = str(row[5] or "").strip()
            if source:
                source_mix[source] += 1
            payload = _parse_json(row[8])
            bucket = payload.get("bucket_id")
            if isinstance(bucket, str) and bucket.strip():
                bucket_mix[bucket.strip()] += 1
            title_text = str(row[1] or "")
            abstract_text = str(row[2] or "")
            for token in _tokenize(f"{title_text} {abstract_text}"):
                terms[token] += 1
        if bucket_mix:
            provenance_present_any = True
        top_terms = [t for t, _count in terms.most_common(12)]
        cluster_summaries.append(
            {
                "cluster_id": cluster_id,
                "size": size,
                "percent_of_corpus": round(pct, 6),
                "representative_titles": representative_titles[:10],
                "source_mix": dict(sorted(source_mix.items())),
                "bucket_mix": dict(sorted(bucket_mix.items())),
                "year_range": {
                    "min": min(years) if years else None,
                    "max": max(years) if years else None,
                    "median": _median_int(years),
                },
                "citation_count": {
                    "min": min(citations) if citations else None,
                    "median": _median_int(citations),
                    "max": max(citations) if citations else None,
                },
                "common_terms": top_terms,
            }
        )

    if not provenance_present_any:
        warnings.append("Missing provenance/bucket metadata in available raw payload rows.")

    corpus_diagnostics = {
        "total_works": total_works,
        "cluster_count": len(cluster_rows),
        "min_cluster_size": min_cluster_size,
        "max_cluster_size": max_cluster_size,
        "median_cluster_size": median_cluster_size,
        "imbalance_ratio": round(float(imbalance_ratio), 6) if imbalance_ratio is not None else None,
        "tiny_cluster_count": tiny_cluster_count,
        "dominant_cluster_share": round(float(dominant_cluster_share), 6),
        "missing_embedding": missing_embedding,
        "missing_cluster_assignment": missing_cluster_assignment,
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": {
            "corpus_snapshot_version": snap,
            "embedding_version": emb,
            "cluster_version": clu,
            "algorithm": str(run_row[1]),
            "expected_cluster_count": expected_k,
        },
        "corpus_diagnostics": corpus_diagnostics,
        "cluster_summaries": cluster_summaries,
        "warnings": warnings,
        "next_step_recommendation": (
            "Proceed to zero-bridge ranking only if representative titles/terms look coherent and warning severity is acceptable."
        ),
        "caveat": (
            "Cluster inspection checks coherence/provenance signals only; it is not ranking validation and not bridge validation."
        ),
    }


def render_cluster_inspection_markdown(payload: dict[str, Any]) -> str:
    prov = payload.get("provenance", {})
    diag = payload.get("corpus_diagnostics", {})
    lines = [
        "# Corpus v2 cluster inspection",
        "",
        "Inspection-only artifact: this does **not** validate ranking quality and does **not** validate bridge scoring.",
        "",
        "## Provenance",
        "",
        f"- **corpus_snapshot_version:** `{prov.get('corpus_snapshot_version')}`",
        f"- **embedding_version:** `{prov.get('embedding_version')}`",
        f"- **cluster_version:** `{prov.get('cluster_version')}`",
        f"- **algorithm:** `{prov.get('algorithm')}`",
        f"- **expected_cluster_count (k):** `{prov.get('expected_cluster_count')}`",
        "",
        "## Corpus diagnostics",
        "",
        f"- **total_works:** `{diag.get('total_works')}`",
        f"- **cluster_count:** `{diag.get('cluster_count')}`",
        f"- **min_cluster_size:** `{diag.get('min_cluster_size')}`",
        f"- **max_cluster_size:** `{diag.get('max_cluster_size')}`",
        f"- **median_cluster_size:** `{diag.get('median_cluster_size')}`",
        f"- **imbalance_ratio:** `{diag.get('imbalance_ratio')}`",
        f"- **tiny_cluster_count (<5):** `{diag.get('tiny_cluster_count')}`",
        f"- **dominant_cluster_share:** `{diag.get('dominant_cluster_share')}`",
        f"- **missing_embedding:** `{diag.get('missing_embedding')}`",
        f"- **missing_cluster_assignment:** `{diag.get('missing_cluster_assignment')}`",
        "",
        "## Cluster size table",
        "",
        "| cluster_id | size | percent_of_corpus | year_min | year_median | year_max | cites_min | cites_median | cites_max |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for c in payload.get("cluster_summaries") or []:
        yr = c.get("year_range") or {}
        cc = c.get("citation_count") or {}
        lines.append(
            f"| {c.get('cluster_id')} | {c.get('size')} | {c.get('percent_of_corpus')} | "
            f"{yr.get('min')} | {yr.get('median')} | {yr.get('max')} | "
            f"{cc.get('min')} | {cc.get('median')} | {cc.get('max')} |"
        )
    lines.extend(["", "## Representative titles", ""])
    for c in payload.get("cluster_summaries") or []:
        lines.append(f"### Cluster `{c.get('cluster_id')}`")
        for t in c.get("representative_titles") or []:
            lines.append(f"- {t}")
        lines.append(f"- Common terms: `{', '.join(c.get('common_terms') or [])}`")
        lines.append(f"- Source mix: `{json.dumps(c.get('source_mix') or {}, sort_keys=True)}`")
        lines.append(f"- Bucket mix: `{json.dumps(c.get('bucket_mix') or {}, sort_keys=True)}`")
        lines.append("")
    lines.extend(["## Warnings", ""])
    warnings = payload.get("warnings") or []
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Next step recommendation",
            "",
            f"- {payload.get('next_step_recommendation')}",
            "",
            "> Caveat: cluster inspection is not ranking validation and not bridge validation.",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def run_cluster_inspection(
    *,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, connect_timeout=30) as conn:
        payload = build_cluster_inspection_payload(
            conn,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            cluster_version=cluster_version,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        render_cluster_inspection_markdown(payload), encoding="utf-8", newline="\n"
    )
    return payload


__all__ = [
    "ClusterInspectionError",
    "build_cluster_inspection_payload",
    "render_cluster_inspection_markdown",
    "run_cluster_inspection",
]
