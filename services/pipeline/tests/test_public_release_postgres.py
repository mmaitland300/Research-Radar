from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from pipeline.public_release import promote_public_release
from pipeline.public_release_persistence import PUBLIC_RELEASE_ADVISORY_LOCK_ID


TEST_DATABASE_URL_ENV = "PUBLIC_RELEASE_TEST_DATABASE_URL"
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _disposable_loopback_database_url() -> str:
    database_url = os.getenv(TEST_DATABASE_URL_ENV)
    if not database_url:
        pytest.skip(f"{TEST_DATABASE_URL_ENV} is not set")
    hostname = urlparse(database_url).hostname
    if hostname not in LOOPBACK_HOSTS:
        pytest.fail(
            f"{TEST_DATABASE_URL_ENV} must target a disposable loopback database, "
            f"not {hostname!r}"
        )
    return database_url


def _seed_serveable_run(database_url: str) -> str:
    suffix = uuid4().hex
    snapshot = f"source-snapshot-concurrency-{suffix}"
    embedding = f"embedding-concurrency-{suffix}"
    ranking_run_id = f"rank-concurrency-{suffix}"
    config = {
        "families_written": ["emerging", "bridge", "undercited"],
        "selection_scope": {
            "type": "included_works",
            "corpus_snapshot_version": snapshot,
        },
        "clustering_artifact": None,
    }
    counts = {
        "total_candidate_works": 1,
        "total_rows_written": 3,
        "rows_by_family": {"emerging": 1, "bridge": 1, "undercited": 1},
        "rows_null_semantic": 0,
        "rows_null_bridge": 0,
    }

    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO source_snapshot_versions (
                source_snapshot_version, policy_name, policy_hash, ingest_mode, note
            )
            VALUES (%s, 'concurrency-test', %s, 'api-bootstrap', 'disposable CI fixture')
            """,
            (snapshot, f"policy-{suffix}"),
        )
        work_id = conn.execute(
            """
            INSERT INTO works (
                openalex_id, title, abstract, year, type, language,
                inclusion_status, corpus_snapshot_version
            )
            VALUES (%s, 'Concurrency fixture', 'Disposable fixture', 2026,
                    'article', 'en', 'included', %s)
            RETURNING id
            """,
            (f"W-CONCURRENCY-{suffix}", snapshot),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO work_source_snapshot_memberships (
                work_id, source_snapshot_version, inclusion_status
            )
            VALUES (%s, %s, 'included')
            """,
            (work_id, snapshot),
        )
        conn.execute(
            """
            INSERT INTO embeddings (work_id, embedding_version, vector)
            VALUES (%s, %s, array_fill(0.0::real, ARRAY[1536])::vector)
            """,
            (work_id, embedding),
        )
        conn.execute(
            """
            INSERT INTO ranking_runs (
                ranking_run_id, ranking_version, corpus_snapshot_version,
                embedding_version, status, started_at, finished_at,
                config_json, counts_json, error_message, notes
            )
            VALUES (%s, 'ranking-concurrency-v1', %s, %s, 'succeeded',
                    NOW(), NOW(), %s, %s, NULL, 'disposable CI fixture')
            """,
            (ranking_run_id, snapshot, embedding, Jsonb(config), Jsonb(counts)),
        )
        conn.executemany(
            """
            INSERT INTO paper_scores (
                ranking_run_id, work_id, recommendation_family,
                final_score, reason_short
            )
            VALUES (%s, %s, %s, 1.0, 'Concurrency fixture')
            """,
            [
                (ranking_run_id, work_id, family)
                for family in ("emerging", "bridge", "undercited")
            ],
        )
    return ranking_run_id


def _waiting_promoters(database_url: str) -> int:
    class_id = (PUBLIC_RELEASE_ADVISORY_LOCK_ID >> 32) & 0xFFFFFFFF
    object_id = PUBLIC_RELEASE_ADVISORY_LOCK_ID & 0xFFFFFFFF
    with psycopg.connect(database_url, autocommit=True) as conn:
        return int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND classid::bigint = %s
                  AND objid::bigint = %s
                  AND objsubid = 1
                  AND NOT granted
                """,
                (class_id, object_id),
            ).fetchone()[0]
        )


def test_concurrent_same_run_promotion_appends_once_after_waiting() -> None:
    database_url = _disposable_loopback_database_url()
    ranking_run_id = _seed_serveable_run(database_url)

    with psycopg.connect(database_url, autocommit=True) as blocker:
        blocker.execute(
            "SELECT pg_advisory_lock(%s)", (PUBLIC_RELEASE_ADVISORY_LOCK_ID,)
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    promote_public_release,
                    ranking_run_id=ranking_run_id,
                    database_url=database_url,
                )
                for _ in range(2)
            ]
            try:
                deadline = time.monotonic() + 10
                while _waiting_promoters(database_url) < 2:
                    if time.monotonic() >= deadline:
                        raise AssertionError("both promoters did not reach the advisory lock")
                    time.sleep(0.05)
            finally:
                blocker.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (PUBLIC_RELEASE_ADVISORY_LOCK_ID,),
                )
            results = [future.result(timeout=20) for future in futures]

    assert sorted(result.status for result in results) == ["already-active", "promoted"]
    assert sum(result.changed for result in results) == 1
    with psycopg.connect(database_url, autocommit=True) as conn:
        promotion_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM public_release_promotions
            WHERE ranking_run_id = %s
            """,
            (ranking_run_id,),
        ).fetchone()[0]
        active_run_id = conn.execute(
            """
            SELECT ranking_run_id
            FROM public_release_promotions
            ORDER BY promotion_id DESC
            LIMIT 1
            """
        ).fetchone()[0]
    assert promotion_count == 1
    assert active_run_id == ranking_run_id
