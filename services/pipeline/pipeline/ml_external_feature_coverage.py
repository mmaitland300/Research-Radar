"""External near-miss feature coverage diagnostic.

Read-only coverage report for rows in `ml_external_near_miss_audit`. It does
not train a model, run a ranking, or write database rows.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_offline_baseline_eval import normalize_w_token
from pipeline.repo_paths import portable_repo_path

REVIEW_POOL_VARIANT = "ml_external_near_miss_audit"
DEFAULT_EMBEDDING_VERSION = "v2-title-abstract-1536-cleantext-r1"
DEFAULT_CORPUS_SNAPSHOT_VERSION = "source-snapshot-v2-candidate-plan-20260428"
REPRESENTATIVE_RANKING_RUN_ID = "rank-ee2ba6c816"

CAVEATS = (
    "Not validation.",
    "Feature coverage only.",
    "Preview text is not a full abstract.",
    "Embedding presence is not a production ranking signal.",
    "The sufficient_text_for_embedding_heuristic flag is operational coverage only, not a quality label.",
    "The external pool is audit-only and not family-selected.",
)


class MLExternalFeatureCoverageError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _norm_ws(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLExternalFeatureCoverageError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLExternalFeatureCoverageError(f"Expected JSON object in {path}")
    return payload


def _row_work_token(row: Mapping[str, Any]) -> str | None:
    for key in ("work_id", "openalex_work_id", "paper_id"):
        token = normalize_w_token(_norm_ws(row.get(key)))
        if token:
            return token
    return None


def select_external_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLExternalFeatureCoverageError("label dataset missing rows array")
    return [
        dict(row)
        for row in rows
        if isinstance(row, dict) and _norm_ws(row.get("review_pool_variant")) == REVIEW_POOL_VARIANT
    ]


def _openalex_variants(tokens: set[str]) -> list[str]:
    variants: set[str] = set()
    for token in tokens:
        upper = token.upper()
        variants.add(upper)
        variants.add(f"HTTPS://OPENALEX.ORG/{upper}")
        variants.add(f"HTTP://OPENALEX.ORG/{upper}")
    return sorted(variants)


def fetch_external_db_coverage(
    conn: psycopg.Connection,
    *,
    work_tokens: set[str],
    embedding_version: str,
    corpus_snapshot_version: str,
    representative_ranking_run_id: str = REPRESENTATIVE_RANKING_RUN_ID,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    if not work_tokens:
        return {}, []
    q = """
        SELECT
            w.id AS internal_work_id,
            w.openalex_id,
            w.title,
            w.abstract,
            w.inclusion_status,
            w.corpus_snapshot_version,
            EXISTS (
                SELECT 1
                FROM embeddings e
                WHERE e.work_id = w.id
                  AND e.embedding_version = %s
            ) AS embedding_row_present,
            (
                SELECT COUNT(*)::bigint
                FROM paper_scores ps
                WHERE ps.work_id = w.id
                  AND ps.ranking_run_id = %s
            ) AS representative_paper_scores_count,
            (
                w.inclusion_status = 'included'
                AND w.corpus_snapshot_version = %s
            ) AS corpus_v2_embed_eligible
        FROM works w
        WHERE UPPER(w.openalex_id) = ANY(%s)
        ORDER BY w.id ASC
    """
    rows = list(
        conn.execute(
            q,
            (
                embedding_version,
                representative_ranking_run_id,
                corpus_snapshot_version,
                _openalex_variants(work_tokens),
            ),
        ).fetchall()
    )
    by_token: dict[str, dict[str, Any]] = {}
    duplicate_tokens: list[str] = []
    for row in rows:
        d = dict(row)
        token = normalize_w_token(_norm_ws(d.get("openalex_id")))
        if not token:
            continue
        rec = {
            "internal_work_id": int(d["internal_work_id"]),
            "openalex_id": _norm_ws(d.get("openalex_id")),
            "title": _norm_ws(d.get("title")),
            "abstract": _norm_ws(d.get("abstract")),
            "inclusion_status": _norm_ws(d.get("inclusion_status")) or None,
            "corpus_snapshot_version": _norm_ws(d.get("corpus_snapshot_version")) or None,
            "title_present": bool(_norm_ws(d.get("title"))),
            "abstract_present": bool(_norm_ws(d.get("abstract"))),
            "title_length": len(_norm_ws(d.get("title"))),
            "abstract_length": len(_norm_ws(d.get("abstract"))),
            "embedding_row_present": bool(d.get("embedding_row_present")),
            "representative_paper_scores_count": int(d.get("representative_paper_scores_count") or 0),
            "representative_paper_scores_present": int(d.get("representative_paper_scores_count") or 0) > 0,
            "corpus_v2_embed_eligible": bool(d.get("corpus_v2_embed_eligible")),
        }
        if token in by_token:
            duplicate_tokens.append(token)
            continue
        by_token[token] = rec
    return by_token, sorted(set(duplicate_tokens))


def _length_bucket(n: int) -> str:
    if n <= 0:
        return "0"
    if n < 100:
        return "1_99"
    if n < 200:
        return "100_199"
    if n < 500:
        return "200_499"
    if n < 1000:
        return "500_999"
    return "ge_1000"


def _context(row: Mapping[str, Any]) -> dict[str, Any]:
    ctx = row.get("external_near_miss_context")
    return dict(ctx) if isinstance(ctx, dict) else {}


def _context_review_abstract_preview(ctx: Mapping[str, Any]) -> str:
    review = ctx.get("review_metadata")
    if isinstance(review, Mapping):
        return _norm_ws(review.get("abstract_preview"))
    return ""


def _heuristic_text(row: Mapping[str, Any], db: Mapping[str, Any] | None) -> dict[str, Any]:
    dataset_title = _norm_ws(row.get("title"))
    dataset_preview = _norm_ws(row.get("abstract_preview"))
    ctx_preview = _context_review_abstract_preview(_context(row))
    if db is not None and _norm_ws(db.get("abstract")):
        title = _norm_ws(db.get("title")) or dataset_title
        abstract = _norm_ws(db.get("abstract"))
        combined = len(title) + len(abstract)
        return {
            "text_source": "db_title_plus_db_abstract",
            "combined_length": combined,
            "sufficient_text_for_embedding_heuristic": combined >= 200,
        }
    title = dataset_title
    preview = dataset_preview
    source = "dataset_title_plus_dataset_abstract_preview"
    if not preview and ctx_preview:
        preview = ctx_preview
        source = "dataset_title_plus_context_review_abstract_preview"
    combined = len(title) + len(preview)
    return {
        "text_source": source,
        "combined_length": combined,
        "sufficient_text_for_embedding_heuristic": combined >= 200,
    }


def _sidecar_parity(
    *,
    context_sidecar_path: Path | None,
    dataset_row_ids: set[str],
) -> dict[str, Any] | None:
    if context_sidecar_path is None:
        return None
    path = context_sidecar_path.resolve()
    if not path.is_file():
        raise MLExternalFeatureCoverageError(f"context sidecar not found: {path}")
    payload = _load_json_object(path)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLExternalFeatureCoverageError(f"context sidecar missing rows array: {path}")
    sidecar_ids = {
        _norm_ws(row.get("row_id"))
        for row in rows
        if isinstance(row, dict) and _norm_ws(row.get("row_id"))
    }
    return {
        "context_sidecar_path": portable_repo_path(path),
        "context_sidecar_sha256": sha256_file(path),
        "sidecar_row_count": len(sidecar_ids),
        "dataset_external_row_count": len(dataset_row_ids),
        "row_id_sets_match": sidecar_ids == dataset_row_ids,
        "missing_in_sidecar": sorted(dataset_row_ids - sidecar_ids),
        "extra_in_sidecar": sorted(sidecar_ids - dataset_row_ids),
    }


def _count_present(rows: list[dict[str, Any]], field: str) -> int:
    return sum(1 for row in rows if row.get(field))


def _feature_inventory(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "available_without_db": {
            "sample_reason": _count_present(rows, "sample_reason"),
            "cluster_id": _count_present(rows, "cluster_id"),
            "topics": _count_present(rows, "topics"),
            "year": _count_present(rows, "year"),
            "citation_count": _count_present(rows, "citation_count"),
            "openalex_identifiers": _count_present(rows, "work_token"),
            "source_metadata": sum(1 for row in rows if row.get("source_metadata_present")),
            "hidden_diagnostics": sum(1 for row in rows if row.get("hidden_diagnostics_present")),
        },
        "requires_db": {
            "works_row": sum(1 for row in rows if row.get("works_row_present")),
            "db_full_abstract": sum(1 for row in rows if row.get("db_abstract_present")),
            "embedding_row": sum(1 for row in rows if row.get("embedding_row_present")),
            "representative_paper_scores": sum(1 for row in rows if row.get("representative_paper_scores_present")),
        },
        "unavailable_ranking_features_note": (
            "paper_scores/final_score/family scores/semantic scores/cluster ranks require ranking materialization; "
            "external v7 rows have ranking_run_id null and were not family-selected."
        ),
    }


def build_external_feature_coverage_payload(
    conn: psycopg.Connection,
    *,
    label_dataset_path: Path,
    context_sidecar_path: Path | None = None,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    representative_ranking_run_id: str = REPRESENTATIVE_RANKING_RUN_ID,
) -> dict[str, Any]:
    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLExternalFeatureCoverageError(f"label dataset not found: {path}")
    label_payload = _load_json_object(path)
    label_sha = sha256_file(path)
    dataset_version = _norm_ws(label_payload.get("dataset_version"))
    external_rows = select_external_rows(label_payload)
    dataset_row_ids = {_norm_ws(row.get("row_id")) for row in external_rows if _norm_ws(row.get("row_id"))}
    tokens = {_row_work_token(row) for row in external_rows}
    work_tokens = {t for t in tokens if t}

    sidecar = _sidecar_parity(context_sidecar_path=context_sidecar_path, dataset_row_ids=dataset_row_ids)
    corpus_snapshot_version = DEFAULT_CORPUS_SNAPSHOT_VERSION
    if sidecar is not None:
        sidecar_payload = _load_json_object(context_sidecar_path.resolve())  # type: ignore[union-attr]
        prov = sidecar_payload.get("provenance") if isinstance(sidecar_payload.get("provenance"), dict) else {}
        corpus_snapshot_version = _norm_ws(prov.get("corpus_snapshot_version")) or DEFAULT_CORPUS_SNAPSHOT_VERSION

    db_by_token, duplicate_db_tokens = fetch_external_db_coverage(
        conn,
        work_tokens=work_tokens,
        embedding_version=embedding_version,
        corpus_snapshot_version=corpus_snapshot_version,
        representative_ranking_run_id=representative_ranking_run_id,
    )

    row_reports: list[dict[str, Any]] = []
    for row in external_rows:
        ctx = _context(row)
        token = _row_work_token(row)
        db = db_by_token.get(token or "")
        dataset_title = _norm_ws(row.get("title"))
        dataset_preview = _norm_ws(row.get("abstract_preview"))
        ctx_preview = _context_review_abstract_preview(ctx)
        source_metadata = ctx.get("source_metadata")
        hidden_diagnostics = ctx.get("hidden_diagnostics")
        heuristic = _heuristic_text(row, db)
        row_reports.append(
            {
                "row_id": _norm_ws(row.get("row_id")),
                "paper_id": _norm_ws(row.get("paper_id")),
                "work_id": _norm_ws(row.get("work_id")),
                "openalex_work_id": _norm_ws(row.get("openalex_work_id")),
                "work_token": token,
                "title": dataset_title,
                "review_pool_variant": _norm_ws(row.get("review_pool_variant")),
                "sample_reason": row.get("sample_reason"),
                "cluster_id": row.get("cluster_id"),
                "topics": row.get("topics"),
                "year": row.get("year"),
                "citation_count": row.get("citation_count"),
                "nested_external_near_miss_context_present": bool(ctx),
                "source_metadata_present": isinstance(source_metadata, Mapping) and bool(source_metadata),
                "hidden_diagnostics_present": isinstance(hidden_diagnostics, Mapping) and bool(hidden_diagnostics),
                "text_coverage": {
                    "dataset_title_length": len(dataset_title),
                    "dataset_abstract_preview_length": len(dataset_preview),
                    "context_review_abstract_preview_length": len(ctx_preview),
                    "db_title_length": int(db.get("title_length", 0)) if db else 0,
                    "db_abstract_length": int(db.get("abstract_length", 0)) if db else 0,
                    "db_abstract_present": bool(db and db.get("abstract_present")),
                    **heuristic,
                },
                "db_coverage": {
                    "works_row_present": db is not None,
                    "internal_work_id": db.get("internal_work_id") if db else None,
                    "works_openalex_id": db.get("openalex_id") if db else None,
                    "inclusion_status": db.get("inclusion_status") if db else None,
                    "corpus_snapshot_version": db.get("corpus_snapshot_version") if db else None,
                    "title_present": bool(db and db.get("title_present")),
                    "abstract_present": bool(db and db.get("abstract_present")),
                    "embedding_row_present": bool(db and db.get("embedding_row_present")),
                    "corpus_v2_embed_eligible": bool(db and db.get("corpus_v2_embed_eligible")),
                    "representative_ranking_run_id": representative_ranking_run_id,
                    "representative_paper_scores_count": int(db.get("representative_paper_scores_count", 0)) if db else 0,
                    "representative_paper_scores_present": bool(db and db.get("representative_paper_scores_present")),
                },
                "embedding_row_present": bool(db and db.get("embedding_row_present")),
                "corpus_v2_embed_eligible": bool(db and db.get("corpus_v2_embed_eligible")),
                "sufficient_text_for_embedding_heuristic": bool(heuristic["sufficient_text_for_embedding_heuristic"]),
            }
        )

    aggregates = {
        "external_row_count": len(external_rows),
        "unique_work_token_count": len(work_tokens),
        "missing_work_token_count": len(external_rows) - sum(1 for row in external_rows if _row_work_token(row)),
        "duplicate_dataset_row_id_count": len(external_rows) - len(dataset_row_ids),
        "works_row_present_count": sum(1 for row in row_reports if row["db_coverage"]["works_row_present"]),
        "embedding_row_present_count": sum(1 for row in row_reports if row["embedding_row_present"]),
        "embedding_row_missing_count": sum(1 for row in row_reports if not row["embedding_row_present"]),
        "corpus_v2_embed_eligible_count": sum(1 for row in row_reports if row["corpus_v2_embed_eligible"]),
        "representative_paper_scores_present_count": sum(
            1 for row in row_reports if row["db_coverage"]["representative_paper_scores_present"]
        ),
        "sufficient_text_for_embedding_heuristic_count": sum(
            1 for row in row_reports if row["sufficient_text_for_embedding_heuristic"]
        ),
        "dataset_abstract_preview_length_buckets": dict(
            sorted(Counter(_length_bucket(row["text_coverage"]["dataset_abstract_preview_length"]) for row in row_reports).items())
        ),
        "db_abstract_length_buckets": dict(
            sorted(Counter(_length_bucket(row["text_coverage"]["db_abstract_length"]) for row in row_reports).items())
        ),
        "sample_reason_counts": dict(sorted(Counter(_norm_ws(row.get("sample_reason")) or "(null)" for row in row_reports).items())),
    }
    aggregates["feature_inventory"] = _feature_inventory(
        [
            {
                **row,
                "works_row_present": row["db_coverage"]["works_row_present"],
                "db_abstract_present": row["db_coverage"]["abstract_present"],
                "representative_paper_scores_present": row["db_coverage"]["representative_paper_scores_present"],
            }
            for row in row_reports
        ]
    )

    return {
        "artifact_type": "ml_external_feature_coverage",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provenance": {
            "label_dataset_path": portable_repo_path(path),
            "label_dataset_sha256": label_sha,
            "label_dataset_version": dataset_version,
            "context_sidecar": sidecar,
            "embedding_version": embedding_version,
            "corpus_snapshot_version_for_embed_eligibility": corpus_snapshot_version,
            "representative_ranking_run_id_for_paper_scores_check": representative_ranking_run_id,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "db_access": "read-only SELECTs against works, embeddings, and paper_scores",
        },
        "repo_accurate_nuance": {
            "corpus_v2_embed_candidate_logic": (
                "corpus_v2_embed / embedding_persistence candidate logic is snapshot-scoped: "
                "works.inclusion_status = 'included' and works.corpus_snapshot_version matches the requested snapshot."
            ),
            "embedding_row_present_definition": "An embeddings row exists for (works.id, embedding_version).",
            "corpus_v2_embed_eligible_definition": (
                "The work exists in works and would be selected by the current snapshot-scoped embed candidate query "
                "(included + snapshot match), independent of whether an embedding row already exists."
            ),
        },
        "aggregates": aggregates,
        "duplicate_db_work_tokens": duplicate_db_tokens,
        "rows": row_reports,
        "recommended_offline_feature_set": {
            "text_first_baseline": (
                "For future cross-pool offline baselines, use title+abstract embeddings: DB full text when present, "
                "otherwise offline embedding from exported preview/context text with caveats."
            ),
            "metadata_features": "Add categorical metadata such as sample_reason, topics, and source metadata.",
            "stratification": "Keep review_pool_variant for stratification and reporting, not as a label.",
        },
        "caveats": list(CAVEATS),
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    prov = payload["provenance"]
    agg = payload["aggregates"]
    feature_inventory = agg["feature_inventory"]
    sidecar = prov.get("context_sidecar")
    lines = [
        "# External Near-Miss Feature Coverage",
        "",
        "Read-only coverage diagnostic for `ml_external_near_miss_audit` rows. This is featureization readiness only: no model training, no ranking run, no production behavior change.",
        "",
        "## Provenance",
        "",
        f"- **label_dataset:** `{prov.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{prov.get('label_dataset_sha256')}`",
        f"- **label_dataset_version:** `{prov.get('label_dataset_version')}`",
        f"- **embedding_version:** `{prov.get('embedding_version')}`",
        f"- **corpus_snapshot_version_for_embed_eligibility:** `{prov.get('corpus_snapshot_version_for_embed_eligibility')}`",
        f"- **representative_paper_scores_run:** `{prov.get('representative_ranking_run_id_for_paper_scores_check')}`",
    ]
    if isinstance(sidecar, Mapping):
        lines.extend(
            [
                f"- **context_sidecar:** `{sidecar.get('context_sidecar_path')}`",
                f"- **context_sidecar_sha256:** `{sidecar.get('context_sidecar_sha256')}`",
                f"- **sidecar row_id parity:** `{str(sidecar.get('row_id_sets_match')).lower()}`",
            ]
        )
    else:
        lines.append("- **context_sidecar:** not provided")
    lines.extend(
        [
            "",
            "## Coverage Summary",
            "",
            f"- **external rows:** `{agg['external_row_count']}`",
            f"- **unique OpenAlex work tokens:** `{agg['unique_work_token_count']}`",
            f"- **works rows present:** `{agg['works_row_present_count']}`",
            f"- **embedding rows present:** `{agg['embedding_row_present_count']}`",
            f"- **embedding rows missing:** `{agg['embedding_row_missing_count']}`",
            f"- **corpus_v2_embed_eligible:** `{agg['corpus_v2_embed_eligible_count']}`",
            f"- **representative paper_scores present:** `{agg['representative_paper_scores_present_count']}`",
            f"- **sufficient text heuristic:** `{agg['sufficient_text_for_embedding_heuristic_count']}`",
            "",
            "## Repo-Accurate Nuance",
            "",
            "The current `corpus_v2_embed` / `embedding_persistence` candidate query is tied to `works.inclusion_status = 'included'` and a specific `corpus_snapshot_version`. External near-miss rows were sampled outside the committed snapshot manifest, so `embedding_row_present` and `corpus_v2_embed_eligible` are reported separately.",
            "",
            "## Feature Inventory",
            "",
            "Available without DB:",
            "",
            *[f"- `{k}`: `{v}` rows" for k, v in feature_inventory["available_without_db"].items()],
            "",
            "Requires DB/materialization:",
            "",
            *[f"- `{k}`: `{v}` rows" for k, v in feature_inventory["requires_db"].items()],
            "",
            "Ranking-shaped features such as `paper_scores`, `final_score`, family scores, semantic scores, and cluster ranks require ranking materialization. External v7 rows have `ranking_run_id=null`, so those channels are unavailable unless the works are explicitly ranked later.",
            "",
            "## Recommended Offline Feature Set",
            "",
            "Future cross-pool text-first baseline: title+abstract embedding using DB full text when present, otherwise offline embedding from exported preview/context text with caveats. Add categorical metadata (`sample_reason`, topics/source), and keep `review_pool_variant` for stratification rather than treating it as a label.",
            "",
            "## Caveats",
            "",
            *[f"- {c}" for c in payload["caveats"]],
            "",
        ]
    )
    return "\n".join(lines)


def run_ml_external_feature_coverage_cli(
    *,
    database_url: str | None,
    label_dataset_path: Path,
    context_sidecar_path: Path | None,
    embedding_version: str,
    output_json: Path,
    markdown_output: Path | None,
) -> dict[str, Any]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        payload = build_external_feature_coverage_payload(
            conn,
            label_dataset_path=label_dataset_path,
            context_sidecar_path=context_sidecar_path,
            embedding_version=embedding_version,
        )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_output is not None:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(render_markdown(payload), encoding="utf-8")
    return payload


__all__ = [
    "DEFAULT_EMBEDDING_VERSION",
    "MLExternalFeatureCoverageError",
    "REVIEW_POOL_VARIANT",
    "build_external_feature_coverage_payload",
    "fetch_external_db_coverage",
    "render_markdown",
    "run_ml_external_feature_coverage_cli",
    "select_external_rows",
]
