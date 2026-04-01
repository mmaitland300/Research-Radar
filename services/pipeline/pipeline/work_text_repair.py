from __future__ import annotations

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.embedding_persistence import latest_corpus_snapshot_version_with_works
from pipeline.openalex_text import clean_openalex_text


def repair_works_text_in_place(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Re-run clean_openalex_text on title and abstract for included works in the snapshot.
    Returns (rows_scanned, rows_changed).
    """
    rows = conn.execute(
        """
        SELECT id, title, abstract
        FROM works
        WHERE inclusion_status = 'included'
          AND corpus_snapshot_version = %s
        ORDER BY id ASC
        """,
        (corpus_snapshot_version,),
    ).fetchall()

    def _abstract_cell(raw: object) -> str | None:
        if raw is None:
            return None
        s = str(raw).strip()
        return s if s else None

    changed = 0
    for row in rows:
        work_id = int(row[0])
        title = str(row[1] or "")
        abstract_raw = row[2]
        new_title = clean_openalex_text(title)
        if abstract_raw is None:
            new_abstract: str | None = None
        else:
            cleaned = clean_openalex_text(str(abstract_raw))
            new_abstract = cleaned if cleaned else None

        old_abs = _abstract_cell(abstract_raw)
        if new_title == title and new_abstract == old_abs:
            continue
        changed += 1
        if not dry_run:
            conn.execute(
                """
                UPDATE works
                SET title = %s, abstract = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (new_title, new_abstract, work_id),
            )
    return len(rows), changed


def run_work_text_repair_cli(
    *,
    database_url: str | None = None,
    corpus_snapshot_version: str | None = None,
    dry_run: bool = False,
) -> tuple[str, int, int]:
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, autocommit=False) as conn:
        snap = corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
        if snap is None:
            raise RuntimeError("No corpus snapshot with included works found.")
        scanned, updated = repair_works_text_in_place(conn, corpus_snapshot_version=snap, dry_run=dry_run)
        if not dry_run and updated:
            conn.commit()
        elif not dry_run:
            conn.commit()
        else:
            conn.rollback()
    return snap, scanned, updated
