from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import psycopg

from pipeline.config import IngestRun, SnapshotCounts, SourceSnapshotVersion
from pipeline.jobs import (
    create_bootstrap_bundle,
    fail_ingest_run,
    finalize_snapshot_manifest,
    make_watermark,
    record_raw_work_batch,
    summarize_policy_decisions,
    write_bootstrap_plan,
    write_bootstrap_preflight_failure,
    write_ingest_artifacts,
    write_source_resolution_manifest,
    write_source_resolution_results,
)
from pipeline.normalize import hydrate_work_record
from pipeline.openalex import WorksPagePlan, build_bootstrap_work_plans, build_source_resolution_plans
from pipeline.openalex_client import fetch_openalex_json
from pipeline.policy import CorpusPolicy, PolicyDecision, corpus_policy_with_openalex_source_ids
from pipeline.source_resolution import resolve_all_sources, slug_to_openalex_id_map


def _canonical_json_blob(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _content_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical_json_blob(obj)).hexdigest()


def load_resolved_policy_from_database(dsn: str, policy: CorpusPolicy) -> CorpusPolicy:
    """Merge `openalex_source_id` values from `source_policies` (after a prior resolve + sync)."""
    with psycopg.connect(dsn) as conn:
        rows = conn.execute("SELECT source_slug, openalex_source_id FROM source_policies").fetchall()
    mapping = {str(slug): str(oid) for slug, oid in rows if oid}
    return corpus_policy_with_openalex_source_ids(policy, mapping)


def database_url_from_env() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "research_radar")
    password = os.environ.get("PGPASSWORD", "research_radar")
    db = os.environ.get("PGDATABASE", "research_radar")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def sync_source_policies(conn: psycopg.Connection, policy: CorpusPolicy) -> None:
    for src in policy.source_policies:
        conn.execute(
            """
            INSERT INTO source_policies (
                source_slug, display_name, openalex_source_id, venue_class, rationale, aliases
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (source_slug) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                openalex_source_id = EXCLUDED.openalex_source_id,
                venue_class = EXCLUDED.venue_class,
                rationale = EXCLUDED.rationale,
                aliases = EXCLUDED.aliases,
                updated_at = NOW()
            """,
            (
                src.slug,
                src.display_name,
                src.openalex_source_id,
                src.venue_class,
                src.rationale,
                json.dumps(list(src.aliases)),
            ),
        )


def insert_snapshot_version(conn: psycopg.Connection, snapshot: SourceSnapshotVersion) -> None:
    conn.execute(
        """
        INSERT INTO source_snapshot_versions (
            source_snapshot_version, policy_name, policy_hash, ingest_mode, note, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_snapshot_version) DO NOTHING
        """,
        (
            snapshot.source_snapshot_version,
            snapshot.policy_name,
            snapshot.policy_hash,
            snapshot.ingest_mode,
            snapshot.note,
            snapshot.created_at,
        ),
    )


def insert_ingest_run_started(conn: psycopg.Connection, ingest_run: IngestRun) -> None:
    conn.execute(
        """
        INSERT INTO ingest_runs (
            ingest_run_id, source_snapshot_version, policy_hash, status,
            started_at, finished_at, config_json, counts_json, error_message
        )
        VALUES (%s, %s, %s, %s, %s, NULL, %s::jsonb, NULL, NULL)
        """,
        (
            ingest_run.ingest_run_id,
            ingest_run.source_snapshot_version,
            ingest_run.policy_hash,
            ingest_run.status,
            ingest_run.started_at,
            json.dumps(ingest_run.config),
        ),
    )


def persist_ingest_watermark(
    conn: psycopg.Connection,
    *,
    source_snapshot_version: str,
    entity_type: str,
    source_slug: str | None,
    cursor: str | None,
    updated_date: str | None,
) -> None:
    """Upsert a durable checkpoint row (one logical stream per snapshot + entity + source)."""
    watermark_key = f"{entity_type}|{source_slug or 'global'}|{source_snapshot_version}"
    conn.execute(
        """
        INSERT INTO ingest_watermarks (
            watermark_key, entity_type, source_slug, cursor, updated_date,
            source_snapshot_version, recorded_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (watermark_key) DO UPDATE SET
            cursor = EXCLUDED.cursor,
            updated_date = EXCLUDED.updated_date,
            source_snapshot_version = EXCLUDED.source_snapshot_version,
            recorded_at = NOW()
        """,
        (watermark_key, entity_type, source_slug, cursor, updated_date, source_snapshot_version),
    )


def update_ingest_run_final(
    conn: psycopg.Connection,
    ingest_run_id: str,
    status: str,
    counts: SnapshotCounts | None,
    error_message: str | None,
) -> None:
    finished_at = datetime.now(UTC)
    counts_json = json.dumps(asdict(counts), default=str) if counts is not None else None
    conn.execute(
        """
        UPDATE ingest_runs
        SET status = %s, finished_at = %s, counts_json = %s::jsonb, error_message = %s
        WHERE ingest_run_id = %s
        """,
        (status, finished_at, counts_json, error_message, ingest_run_id),
    )


def insert_raw_openalex_work(
    conn: psycopg.Connection,
    *,
    openalex_id: str,
    ingest_run_id: str,
    source_snapshot_version: str,
    source_slug: str | None,
    page_cursor: str,
    updated_date: Any,
    payload: Mapping[str, Any],
    content_hash: str,
) -> None:
    conn.execute(
        """
        INSERT INTO raw_openalex_works (
            openalex_id, ingest_run_id, source_snapshot_version, source_slug,
            page_cursor, updated_date, payload, content_hash
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (openalex_id, ingest_run_id) DO UPDATE SET
            page_cursor = EXCLUDED.page_cursor,
            updated_date = EXCLUDED.updated_date,
            payload = EXCLUDED.payload,
            content_hash = EXCLUDED.content_hash,
            fetched_at = NOW()
        """,
        (
            openalex_id,
            ingest_run_id,
            source_snapshot_version,
            source_slug,
            page_cursor,
            updated_date,
            json.dumps(payload),
            content_hash,
        ),
    )


def _upsert_venue(
    conn: psycopg.Connection,
    *,
    openalex_id: str,
    display_name: str,
    source_slug: str | None,
    venue_class: str,
) -> int:
    row = conn.execute(
        """
        INSERT INTO venues (openalex_id, display_name, source_slug, venue_class)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (openalex_id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            source_slug = COALESCE(EXCLUDED.source_slug, venues.source_slug),
            venue_class = EXCLUDED.venue_class
        RETURNING id
        """,
        (openalex_id, display_name, source_slug, venue_class),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _upsert_author(conn: psycopg.Connection, openalex_id: str, display_name: str) -> int:
    row = conn.execute(
        """
        INSERT INTO authors (openalex_id, display_name)
        VALUES (%s, %s)
        ON CONFLICT (openalex_id) DO UPDATE SET display_name = EXCLUDED.display_name
        RETURNING id
        """,
        (openalex_id, display_name),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _upsert_topic(conn: psycopg.Connection, openalex_id: str, name: str, level: int) -> int:
    row = conn.execute(
        """
        INSERT INTO topics (openalex_id, name, level)
        VALUES (%s, %s, %s)
        ON CONFLICT (openalex_id) DO UPDATE SET
            name = EXCLUDED.name,
            level = EXCLUDED.level
        RETURNING id
        """,
        (openalex_id, name, level),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _upsert_work(
    conn: psycopg.Connection,
    *,
    nw: Any,
    venue_id: int | None,
    source_slug: str | None,
    raw_content_hash: str,
    corpus_snapshot_version: str,
    last_ingest_run_id: str,
) -> int:
    row = conn.execute(
        """
        INSERT INTO works (
            openalex_id, title, abstract, year, doi, type, language, publication_date, updated_date,
            venue_id, source_slug, citation_count, is_core_corpus, inclusion_status, exclusion_reason,
            raw_content_hash, corpus_snapshot_version, last_ingest_run_id
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (openalex_id) DO UPDATE SET
            title = EXCLUDED.title,
            abstract = EXCLUDED.abstract,
            year = EXCLUDED.year,
            doi = EXCLUDED.doi,
            type = EXCLUDED.type,
            language = EXCLUDED.language,
            publication_date = EXCLUDED.publication_date,
            updated_date = EXCLUDED.updated_date,
            venue_id = EXCLUDED.venue_id,
            source_slug = EXCLUDED.source_slug,
            citation_count = EXCLUDED.citation_count,
            is_core_corpus = EXCLUDED.is_core_corpus,
            inclusion_status = EXCLUDED.inclusion_status,
            exclusion_reason = EXCLUDED.exclusion_reason,
            raw_content_hash = EXCLUDED.raw_content_hash,
            corpus_snapshot_version = EXCLUDED.corpus_snapshot_version,
            last_ingest_run_id = EXCLUDED.last_ingest_run_id,
            updated_at = NOW()
        RETURNING id
        """,
        (
            nw.openalex_id,
            nw.title,
            nw.abstract,
            nw.year,
            nw.doi,
            nw.work_type,
            nw.language,
            nw.publication_date,
            nw.updated_date,
            venue_id,
            source_slug,
            nw.citation_count,
            nw.is_core_corpus,
            nw.inclusion_status,
            nw.exclusion_reason,
            raw_content_hash,
            corpus_snapshot_version,
            last_ingest_run_id,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _rewrite_work_edges(conn: psycopg.Connection, work_id: int, hydrated: Any) -> None:
    conn.execute("DELETE FROM work_authors WHERE work_id = %s", (work_id,))
    conn.execute("DELETE FROM work_topics WHERE work_id = %s", (work_id,))

    for link in hydrated.authors:
        aid = _upsert_author(conn, link.author_openalex_id, link.display_name)
        conn.execute(
            """
            INSERT INTO work_authors (work_id, author_id, author_position)
            VALUES (%s, %s, %s)
            ON CONFLICT (work_id, author_id) DO UPDATE SET author_position = EXCLUDED.author_position
            """,
            (work_id, aid, link.author_position),
        )

    for tlink in hydrated.topics:
        tid = _upsert_topic(conn, tlink.topic_openalex_id, tlink.display_name, tlink.level)
        conn.execute(
            """
            INSERT INTO work_topics (work_id, topic_id, score)
            VALUES (%s, %s, %s)
            ON CONFLICT (work_id, topic_id) DO UPDATE SET score = EXCLUDED.score
            """,
            (work_id, tid, tlink.score),
        )


def persist_work(
    conn: psycopg.Connection,
    *,
    policy: CorpusPolicy,
    work: Mapping[str, Any],
    snapshot: SourceSnapshotVersion,
    ingest_run: IngestRun,
    source_slug_hint: str | None,
    page_cursor: str,
    citation_edges: set[tuple[str, str]],
    decisions: list[PolicyDecision],
    venue_ids_seen: set[str],
    author_ids_seen: set[str],
    topic_ids_seen: set[str],
) -> None:
    hydrated = hydrate_work_record(work, policy)
    decisions.append(hydrated.policy_decision)

    oa_id = hydrated.work.openalex_id
    if not oa_id:
        return

    wh = _content_hash(work)
    insert_raw_openalex_work(
        conn,
        openalex_id=oa_id,
        ingest_run_id=ingest_run.ingest_run_id,
        source_snapshot_version=snapshot.source_snapshot_version,
        source_slug=source_slug_hint,
        page_cursor=page_cursor,
        updated_date=hydrated.work.updated_date,
        payload=work,
        content_hash=wh,
    )

    matched = policy.classify_source(
        _dig(work, "primary_location", "source", "id"),
        _dig(work, "primary_location", "source", "display_name"),
    )
    work_source_slug = matched.slug if matched else None

    venue_id: int | None = None
    src_oa = hydrated.work.source_openalex_id
    if src_oa:
        vname = hydrated.work.source_display_name or "Unknown source"
        venue_id = _upsert_venue(
            conn,
            openalex_id=src_oa,
            display_name=vname,
            source_slug=work_source_slug,
            venue_class=hydrated.policy_decision.venue_class,
        )
        venue_ids_seen.add(src_oa)

    for link in hydrated.authors:
        author_ids_seen.add(link.author_openalex_id)
    for tlink in hydrated.topics:
        topic_ids_seen.add(tlink.topic_openalex_id)

    _upsert_work(
        conn,
        nw=hydrated.work,
        venue_id=venue_id,
        source_slug=work_source_slug,
        raw_content_hash=wh,
        corpus_snapshot_version=snapshot.source_snapshot_version,
        last_ingest_run_id=ingest_run.ingest_run_id,
    )
    row = conn.execute("SELECT id FROM works WHERE openalex_id = %s", (oa_id,)).fetchone()
    assert row is not None
    work_id = int(row[0])
    _rewrite_work_edges(conn, work_id, hydrated)

    citing = oa_id
    for c in hydrated.citations:
        citation_edges.add((citing, c.cited_openalex_id))


def _dig(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def flush_citations(conn: psycopg.Connection, citation_edges: set[tuple[str, str]]) -> int:
    inserted = 0
    for citing_oa, cited_oa in citation_edges:
        res = conn.execute(
            """
            INSERT INTO citations (citing_work_id, cited_work_id)
            SELECT w1.id, w2.id
            FROM works w1
            JOIN works w2 ON w2.openalex_id = %s
            WHERE w1.openalex_id = %s
            ON CONFLICT (citing_work_id, cited_work_id) DO NOTHING
            """,
            (cited_oa, citing_oa),
        )
        inserted += res.rowcount or 0
    return inserted


def run_bootstrap_ingest(
    *,
    policy: CorpusPolicy,
    output_dir: Path,
    raw_root: Path,
    note: str,
    database_url: str | None = None,
    mailto: str | None = None,
    max_pages_per_source: int | None = None,
) -> IngestRun:
    """
    Resolve OpenAlex source IDs (authoritative), sync `source_policies`, plan a snapshot/run,
    fetch works by source id only, write raw files, load Postgres (including watermarks),
    and write snapshot-manifest.json. Requires a reachable Postgres with schema applied.
    """
    dsn = database_url or database_url_from_env()
    snapshot: SourceSnapshotVersion | None = None
    ingest_run: IngestRun | None = None
    stage = "source_resolution"

    try:
        outcomes = resolve_all_sources(policy, mailto=mailto)
        stage = "merge_source_ids"
        policy_resolved = corpus_policy_with_openalex_source_ids(policy, slug_to_openalex_id_map(outcomes))

        stage = "sync_source_policies"
        with psycopg.connect(dsn, autocommit=False) as conn:
            sync_source_policies(conn, policy_resolved)
            conn.commit()

        stage = "create_bootstrap_bundle"
        snapshot, ingest_run = create_bootstrap_bundle(policy=policy_resolved, note=note)
        assert snapshot is not None and ingest_run is not None
        stage = "write_plan_artifacts"
        write_ingest_artifacts(output_dir, snapshot, ingest_run)
        write_source_resolution_manifest(output_dir, snapshot, build_source_resolution_plans(policy))
        write_source_resolution_results(output_dir, snapshot, outcomes)
        write_bootstrap_plan(output_dir, snapshot, build_bootstrap_work_plans(policy_resolved))

        stage = "ingest_openalex_works"
        plans = build_bootstrap_work_plans(policy_resolved)
        raw_batches: list[Any] = []
        watermarks: list[Any] = []
        decisions: list[PolicyDecision] = []
        citation_edges: set[tuple[str, str]] = set()
        venue_ids_seen: set[str] = set()
        author_ids_seen: set[str] = set()
        topic_ids_seen: set[str] = set()

        with psycopg.connect(dsn, autocommit=False) as conn:
            stage = "register_snapshot_and_ingest_run"
            insert_snapshot_version(conn, snapshot)
            insert_ingest_run_started(conn, ingest_run)
            conn.commit()

            for plan in plans:
                current_plan: WorksPagePlan = plan
                page_index = 0
                pages_for_source = 0
                while True:
                    page_cursor = current_plan.params["cursor"]
                    url = current_plan.url()
                    page = fetch_openalex_json(url, mailto=mailto)
                    meta = page.get("meta") if isinstance(page, Mapping) else {}
                    results = page.get("results") if isinstance(page, Mapping) else None
                    if not isinstance(results, list):
                        results = []

                    manifest = record_raw_work_batch(
                        raw_root,
                        snapshot,
                        ingest_run,
                        plan.source_slug,
                        page_index,
                        page_cursor,
                        page,
                    )
                    raw_batches.append(manifest)

                    next_cursor = meta.get("next_cursor") if isinstance(meta, Mapping) else None

                    with conn.transaction():
                        for work in results:
                            if not isinstance(work, Mapping):
                                continue
                            persist_work(
                                conn,
                                policy=policy_resolved,
                                work=work,
                                snapshot=snapshot,
                                ingest_run=ingest_run,
                                source_slug_hint=plan.source_slug,
                                page_cursor=page_cursor,
                                citation_edges=citation_edges,
                                decisions=decisions,
                                venue_ids_seen=venue_ids_seen,
                                author_ids_seen=author_ids_seen,
                                topic_ids_seen=topic_ids_seen,
                            )
                        persist_ingest_watermark(
                            conn,
                            source_snapshot_version=snapshot.source_snapshot_version,
                            entity_type="openalex_works",
                            source_slug=plan.source_slug,
                            cursor=str(next_cursor) if next_cursor else None,
                            updated_date=None,
                        )
                    watermarks.append(
                        make_watermark(
                            snapshot.source_snapshot_version,
                            "openalex_works",
                            plan.source_slug,
                            str(next_cursor) if next_cursor else None,
                            None,
                        )
                    )

                    page_index += 1
                    pages_for_source += 1
                    if max_pages_per_source is not None and pages_for_source >= max_pages_per_source:
                        break
                    if not next_cursor or not results:
                        break
                    current_plan = plan.next_page(str(next_cursor))

            with conn.transaction():
                citation_count = flush_citations(conn, citation_edges)

            summary = summarize_policy_decisions(decisions)
            counts = SnapshotCounts(
                included_works=summary.included_works,
                excluded_works=summary.excluded_works,
                unique_authors=len(author_ids_seen),
                unique_sources=len(venue_ids_seen),
                unique_topics=len(topic_ids_seen),
                citation_edges=citation_count,
                excluded_by_reason=summary.excluded_by_reason,
            )

            update_ingest_run_final(conn, ingest_run.ingest_run_id, "succeeded", counts, None)
            conn.commit()

        finalized = ingest_run.complete(counts)
        finalize_snapshot_manifest(output_dir, snapshot, ingest_run, counts, raw_batches, watermarks)
        return finalized

    except Exception as exc:
        if snapshot is None:
            write_bootstrap_preflight_failure(output_dir, stage=stage, message=str(exc))
        elif ingest_run is not None:
            fail_ingest_run(output_dir, snapshot, ingest_run, str(exc))
            try:
                with psycopg.connect(dsn, autocommit=True) as conn:
                    update_ingest_run_final(conn, ingest_run.ingest_run_id, "failed", None, str(exc))
            except Exception:
                pass
        raise
