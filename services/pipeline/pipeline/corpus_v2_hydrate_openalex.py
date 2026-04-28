"""Hydrate corpus-v2 snapshot works with OpenAlex metadata + abstract text."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlencode

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.openalex import OPENALEX_WORKS_URL, build_work_select_clause
from pipeline.openalex_client import OPENALEX_API_KEY_ENV, fetch_openalex_json, openalex_api_key_from_env
from pipeline.openalex_text import abstract_plain_text, clean_openalex_text
from pipeline.policy import CorpusPolicy

DEFAULT_LANGUAGE = "en"
UNKNOWN_TYPE = "unknown"
FETCH_BATCH_SIZE = 25

HydrateFetch = Callable[[str], Mapping[str, Any] | None]


class CorpusV2HydrateError(RuntimeError):
    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _WorkRow:
    work_id: int
    openalex_id: str
    title: str
    abstract: str | None
    work_type: str
    language: str
    doi: str | None
    citation_count: int
    year: int
    publication_date: str | None
    source_slug: str | None


def run_corpus_v2_hydrate_openalex(
    *,
    snapshot_version: str,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
    mock_openalex: bool = False,
    fetch_work: HydrateFetch | None = None,
) -> dict[str, Any]:
    snapshot_version = (snapshot_version or "").strip()
    if not snapshot_version:
        raise CorpusV2HydrateError("--snapshot-version is required and must not be blank", code=2)
    if not mock_openalex and not openalex_api_key_from_env():
        raise CorpusV2HydrateError(
            f"live hydration requires {OPENALEX_API_KEY_ENV} (use --mock-openalex for offline tests)",
            code=2,
        )

    dsn = database_url or database_url_from_env()
    hydration_run_id = f"hydrate-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    accepted_types = {t.casefold() for t in CorpusPolicy().include_document_types}
    warnings: list[str] = []

    with psycopg.connect(dsn, autocommit=False) as conn:
        _assert_snapshot_exists(conn, snapshot_version)
        works = _load_snapshot_works(conn, snapshot_version=snapshot_version)
        before = _measure_before(works)
        _insert_hydration_run_started(
            conn,
            hydration_run_id=hydration_run_id,
            snapshot_version=snapshot_version,
            mock_openalex=mock_openalex,
            works_considered=len(works),
        )
        conn.commit()

        fetched_count = 0
        updated_count = 0
        failed_count = 0
        raw_payload_upserted_count = 0
        doi_added_count = 0

        fetcher = fetch_work or (lambda oid: _fetch_openalex_work_by_id(oid, mock_openalex=mock_openalex))
        try:
            with conn.transaction():
                for batch_start in range(0, len(works), FETCH_BATCH_SIZE):
                    batch = works[batch_start : batch_start + FETCH_BATCH_SIZE]
                    for row in batch:
                        payload = fetcher(row.openalex_id)
                        if payload is None:
                            failed_count += 1
                            continue
                        fetched_count += 1
                        raw_payload_upserted_count += _upsert_raw_payload(
                            conn,
                            hydration_run_id=hydration_run_id,
                            snapshot_version=snapshot_version,
                            openalex_id=row.openalex_id,
                            source_slug=row.source_slug,
                            payload=payload,
                        )
                        changed, doi_was_added = _hydrate_work_row(
                            conn,
                            row=row,
                            payload=payload,
                            snapshot_version=snapshot_version,
                            hydration_run_id=hydration_run_id,
                        )
                        if changed:
                            updated_count += 1
                        if doi_was_added:
                            doi_added_count += 1
                after_rows = _load_snapshot_works(conn, snapshot_version=snapshot_version)
                summary = _build_summary(
                    snapshot_version=snapshot_version,
                    hydration_run_id=hydration_run_id,
                    before_rows=works,
                    after_rows=after_rows,
                    fetched_count=fetched_count,
                    updated_count=updated_count,
                    failed_count=failed_count,
                    raw_payload_upserted_count=raw_payload_upserted_count,
                    doi_added_count=doi_added_count,
                    accepted_types=accepted_types,
                    mock_openalex=mock_openalex,
                )
                if failed_count > 0:
                    warnings.append(f"{failed_count} OpenAlex records failed to hydrate and remain blocked.")
                if summary["embedding_blocked_count"] > 0:
                    warnings.append("Snapshot still has metadata/text blockers; do not run embeddings until resolved.")
                summary["warnings"] = warnings
                _mark_hydration_run_final(conn, hydration_run_id=hydration_run_id, summary=summary, status="succeeded")
            conn.commit()
        except Exception as exc:
            _mark_hydration_run_failed(conn, hydration_run_id=hydration_run_id, message=str(exc))
            raise CorpusV2HydrateError(f"corpus-v2 hydrate failed: {exc}", code=1) from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(render_hydration_summary_markdown(summary), encoding="utf-8", newline="\n")
    return summary


def render_hydration_summary_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Corpus v2 OpenAlex hydration summary",
        "",
        f"- **snapshot_version:** `{summary.get('snapshot_version')}`",
        f"- **hydration_run_id:** `{summary.get('hydration_run_id')}`",
        f"- **works_considered_count:** `{summary.get('works_considered_count')}`",
        f"- **fetched_count:** `{summary.get('fetched_count')}`",
        f"- **updated_count:** `{summary.get('updated_count')}`",
        f"- **failed_count:** `{summary.get('failed_count')}`",
        "",
        "## Coverage before/after",
        "",
        f"- **abstract:** `{summary.get('abstract_before_count')}` -> `{summary.get('abstract_after_count')}` (+`{summary.get('abstract_added_count')}`)",
        f"- **type_unknown:** `{summary.get('type_unknown_before_count')}` -> `{summary.get('type_unknown_after_count')}` (resolved `{summary.get('type_resolved_count')}`)",
        f"- **language_defaulted:** `{summary.get('language_defaulted_before_count')}` -> `{summary.get('language_defaulted_after_count')}` (resolved `{summary.get('language_resolved_count')}`)",
        f"- **doi_added_count:** `{summary.get('doi_added_count')}`",
        "",
        "## Embedding readiness",
        "",
        f"- **embedding_ready_count:** `{summary.get('embedding_ready_count')}`",
        f"- **embedding_blocked_count:** `{summary.get('embedding_blocked_count')}`",
        f"- **snapshot_embedding_ready:** `{summary.get('snapshot_embedding_ready')}`",
        "",
        "## Remaining blockers",
        "",
    ]
    for warning in list(summary.get("warnings") or []):
        lines.append(f"- {warning}")
    if not list(summary.get("warnings") or []):
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Next step",
            "",
            "Generate embeddings only if metadata/text coverage is sufficient; hydration validates metadata/text readiness, not ranking quality or benchmark validity.",
            "",
            "> Caveat: this hydration step improves metadata/text completeness only. It is not ranking validation.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _assert_snapshot_exists(conn: psycopg.Connection, snapshot_version: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    if row is None:
        raise CorpusV2HydrateError(f"snapshot_version not found: {snapshot_version}", code=2)


def _load_snapshot_works(conn: psycopg.Connection, *, snapshot_version: str) -> list[_WorkRow]:
    rows = conn.execute(
        """
        SELECT id, openalex_id, title, abstract, type, language, doi, citation_count, year, publication_date, source_slug
        FROM works
        WHERE corpus_snapshot_version = %s
        ORDER BY id
        """,
        (snapshot_version,),
    ).fetchall()
    out: list[_WorkRow] = []
    for row in rows:
        out.append(
            _WorkRow(
                work_id=int(row[0]),
                openalex_id=str(row[1]),
                title=str(row[2] or ""),
                abstract=row[3] if isinstance(row[3], str) else None,
                work_type=str(row[4] or ""),
                language=str(row[5] or ""),
                doi=row[6] if isinstance(row[6], str) else None,
                citation_count=int(row[7] or 0),
                year=int(row[8] or 0),
                publication_date=str(row[9]) if row[9] else None,
                source_slug=str(row[10]) if row[10] else None,
            )
        )
    return out


def _measure_before(rows: list[_WorkRow]) -> dict[str, int]:
    return {
        "abstract_before_count": sum(1 for r in rows if _has_text(r.abstract)),
        "type_unknown_before_count": sum(1 for r in rows if _is_unknown_type(r.work_type)),
        "language_defaulted_before_count": sum(1 for r in rows if _is_defaulted_language(r.language)),
    }


def _insert_hydration_run_started(
    conn: psycopg.Connection,
    *,
    hydration_run_id: str,
    snapshot_version: str,
    mock_openalex: bool,
    works_considered: int,
) -> None:
    config = {
        "command": "corpus-v2-hydrate-openalex",
        "snapshot_version": snapshot_version,
        "api_key_provided": bool(openalex_api_key_from_env()) if not mock_openalex else False,
        "auth_mode": "mock" if mock_openalex else "api_key",
        "openalex_enrichment": "run",
        "works_considered_count": works_considered,
    }
    conn.execute(
        """
        INSERT INTO ingest_runs (
            ingest_run_id, source_snapshot_version, policy_hash, status,
            started_at, finished_at, config_json, counts_json, error_message
        )
        VALUES (%s, %s, %s, 'running', %s, NULL, %s::jsonb, NULL, NULL)
        """,
        (
            hydration_run_id,
            snapshot_version,
            "corpus-v2-openalex-hydration",
            datetime.now(UTC),
            json.dumps(config, sort_keys=True),
        ),
    )


def _mark_hydration_run_final(
    conn: psycopg.Connection,
    *,
    hydration_run_id: str,
    summary: Mapping[str, Any],
    status: str,
) -> None:
    conn.execute(
        """
        UPDATE ingest_runs
        SET status = %s,
            finished_at = %s,
            counts_json = %s::jsonb,
            error_message = NULL
        WHERE ingest_run_id = %s
        """,
        (status, datetime.now(UTC), json.dumps(summary, sort_keys=True), hydration_run_id),
    )


def _mark_hydration_run_failed(conn: psycopg.Connection, *, hydration_run_id: str, message: str) -> None:
    try:
        conn.execute(
            """
            UPDATE ingest_runs
            SET status = 'failed',
                finished_at = %s,
                error_message = %s
            WHERE ingest_run_id = %s
            """,
            (datetime.now(UTC), message, hydration_run_id),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _to_openalex_path_id(openalex_id: str) -> str:
    text = str(openalex_id or "").strip()
    if not text:
        return text
    if text.startswith("http://") or text.startswith("https://"):
        return text.rstrip("/").split("/")[-1]
    return text


def _fetch_openalex_work_by_id(openalex_id: str, *, mock_openalex: bool) -> Mapping[str, Any] | None:
    if mock_openalex:
        return None
    path_id = _to_openalex_path_id(openalex_id)
    if not path_id:
        return None
    select_clause = ",".join(
        build_work_select_clause(
            (
                "ids",
                "publication_year",
                "cited_by_count",
                "primary_location",
            )
        )
    )
    url = f"{OPENALEX_WORKS_URL}/{path_id}?{urlencode({'select': select_clause})}"
    payload = fetch_openalex_json(url, timeout_sec=90.0)
    return payload if isinstance(payload, Mapping) else None


def _payload_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _upsert_raw_payload(
    conn: psycopg.Connection,
    *,
    hydration_run_id: str,
    snapshot_version: str,
    openalex_id: str,
    source_slug: str | None,
    payload: Mapping[str, Any],
) -> int:
    content_hash = _payload_hash(payload)
    conn.execute(
        """
        INSERT INTO raw_openalex_works (
            openalex_id, ingest_run_id, source_snapshot_version, source_slug,
            page_cursor, updated_date, payload, content_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (openalex_id, ingest_run_id) DO UPDATE SET
            source_snapshot_version = EXCLUDED.source_snapshot_version,
            source_slug = EXCLUDED.source_slug,
            page_cursor = EXCLUDED.page_cursor,
            updated_date = EXCLUDED.updated_date,
            payload = EXCLUDED.payload,
            content_hash = EXCLUDED.content_hash,
            fetched_at = NOW()
        """,
        (
            openalex_id,
            hydration_run_id,
            snapshot_version,
            source_slug,
            "hydrate-openalex",
            payload.get("updated_date"),
            json.dumps(payload, sort_keys=True, ensure_ascii=False),
            content_hash,
        ),
    )
    return 1


def _hydrate_work_row(
    conn: psycopg.Connection,
    *,
    row: _WorkRow,
    payload: Mapping[str, Any],
    snapshot_version: str,
    hydration_run_id: str,
) -> tuple[bool, bool]:
    abstract = abstract_plain_text(payload)
    work_type = str(payload.get("type") or row.work_type or "").strip().casefold()
    language = str(payload.get("language") or row.language or "").strip().casefold()
    doi = _normalize_doi(payload.get("doi")) or _normalize_doi(row.doi)
    citation_count = int(payload.get("cited_by_count") or row.citation_count or 0)
    publication_date = payload.get("publication_date") or row.publication_date
    year = int(payload.get("publication_year") or row.year or 0)
    updated_date = payload.get("updated_date")
    title = clean_openalex_text(str(payload.get("title") or row.title or ""))

    did_change = any(
        [
            _normalize_none(row.abstract) != _normalize_none(abstract),
            _normalize_none(row.work_type) != _normalize_none(work_type),
            _normalize_none(row.language) != _normalize_none(language),
            _normalize_none(_normalize_doi(row.doi)) != _normalize_none(doi),
            int(row.citation_count or 0) != citation_count,
            int(row.year or 0) != year,
            _normalize_none(row.publication_date) != _normalize_none(publication_date),
            _normalize_none(row.title) != _normalize_none(title),
        ]
    )
    doi_was_added = not _has_text(row.doi) and _has_text(doi)
    if not did_change:
        return False, doi_was_added
    conn.execute(
        """
        UPDATE works
        SET title = %s,
            abstract = %s,
            type = %s,
            language = %s,
            doi = %s,
            citation_count = %s,
            publication_date = %s,
            year = %s,
            updated_date = %s,
            last_ingest_run_id = %s,
            updated_at = NOW()
        WHERE id = %s
          AND corpus_snapshot_version = %s
        """,
        (
            title or row.title,
            abstract or None,
            work_type or row.work_type,
            language or row.language,
            doi,
            citation_count,
            publication_date,
            year,
            updated_date,
            hydration_run_id,
            row.work_id,
            snapshot_version,
        ),
    )
    return True, doi_was_added


def _build_summary(
    *,
    snapshot_version: str,
    hydration_run_id: str,
    before_rows: list[_WorkRow],
    after_rows: list[_WorkRow],
    fetched_count: int,
    updated_count: int,
    failed_count: int,
    raw_payload_upserted_count: int,
    doi_added_count: int,
    accepted_types: set[str],
    mock_openalex: bool,
) -> dict[str, Any]:
    before_abstract = sum(1 for r in before_rows if _has_text(r.abstract))
    after_abstract = sum(1 for r in after_rows if _has_text(r.abstract))
    type_unknown_before = sum(1 for r in before_rows if _is_unknown_type(r.work_type))
    type_unknown_after = sum(1 for r in after_rows if _is_unknown_type(r.work_type))
    language_defaulted_before = sum(1 for r in before_rows if _is_defaulted_language(r.language))
    language_defaulted_after = sum(1 for r in after_rows if _is_defaulted_language(r.language))
    embedding_ready_count = sum(1 for r in after_rows if _is_embedding_ready(r, accepted_types=accepted_types))
    works_considered = len(after_rows)
    return {
        "snapshot_version": snapshot_version,
        "hydration_run_id": hydration_run_id,
        "works_considered_count": works_considered,
        "fetched_count": fetched_count,
        "updated_count": updated_count,
        "failed_count": failed_count,
        "abstract_before_count": before_abstract,
        "abstract_after_count": after_abstract,
        "abstract_added_count": max(after_abstract - before_abstract, 0),
        "type_unknown_before_count": type_unknown_before,
        "type_unknown_after_count": type_unknown_after,
        "type_resolved_count": max(type_unknown_before - type_unknown_after, 0),
        "language_defaulted_before_count": language_defaulted_before,
        "language_defaulted_after_count": language_defaulted_after,
        "language_resolved_count": max(language_defaulted_before - language_defaulted_after, 0),
        "doi_added_count": doi_added_count,
        "raw_payload_upserted_count": raw_payload_upserted_count,
        "embedding_ready_count": embedding_ready_count,
        "embedding_blocked_count": works_considered - embedding_ready_count,
        "snapshot_embedding_ready": embedding_ready_count == works_considered,
        "api_key_provided": bool(openalex_api_key_from_env()) if not mock_openalex else False,
        "auth_mode": "mock" if mock_openalex else "api_key",
        "openalex_enrichment": "run",
        "warnings": [],
    }


def _is_embedding_ready(row: _WorkRow, *, accepted_types: set[str]) -> bool:
    title_ok = _has_text(row.title)
    abstract_ok = _has_text(row.abstract)
    type_raw = (row.work_type or "").strip().casefold()
    type_ok = bool(type_raw) and type_raw != UNKNOWN_TYPE
    if type_raw in accepted_types:
        type_ok = True
    language_ok = _has_text(row.language)
    return title_ok and abstract_ok and type_ok and language_ok


def _normalize_doi(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    doi = value.strip().casefold()
    for prefix in ("https://doi.org/", "http://doi.org/"):
        if doi.startswith(prefix):
            doi = doi[len(prefix) :]
    return doi or None


def _normalize_none(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_unknown_type(value: str) -> bool:
    raw = (value or "").strip().casefold()
    return (not raw) or raw == UNKNOWN_TYPE


def _is_defaulted_language(value: str) -> bool:
    raw = (value or "").strip().casefold()
    return (not raw) or raw == DEFAULT_LANGUAGE


__all__ = [
    "CorpusV2HydrateError",
    "render_hydration_summary_markdown",
    "run_corpus_v2_hydrate_openalex",
]
