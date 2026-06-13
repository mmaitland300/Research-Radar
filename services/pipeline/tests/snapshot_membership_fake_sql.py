"""Shared fake-SQL helpers for snapshot membership in unit tests."""

from __future__ import annotations

from typing import Iterable


def build_memberships_from_works(
    works: dict[int, dict],
    *,
    inclusion_key: str = "inclusion_status",
    snapshot_key: str = "corpus_snapshot_version",
) -> set[tuple[int, str]]:
    memberships: set[tuple[int, str]] = set()
    for work_id, work in works.items():
        if work.get(inclusion_key, "included") != "included":
            continue
        snapshot = work.get(snapshot_key)
        if snapshot:
            memberships.add((work_id, str(snapshot)))
    return memberships


def is_membership_included_work_select(compact: str) -> bool:
    return (
        compact.startswith("SELECT w.")
        and "FROM works w JOIN work_source_snapshot_memberships wssm" in compact
        and "inclusion_status = 'included'" in compact
    )


def included_work_ids(
    works: dict[int, dict],
    memberships: set[tuple[int, str]],
    snapshot: str,
    *,
    inclusion_key: str = "inclusion_status",
) -> list[int]:
    ids: list[int] = []
    for work_id in works:
        if (work_id, snapshot) in memberships and works[work_id].get(inclusion_key, "included") == "included":
            ids.append(work_id)
    return sorted(ids)


def apply_membership_upsert(memberships: set[tuple[int, str]], params: tuple) -> None:
    work_id = int(params[0])
    snapshot = str(params[1])
    if str(params[2]) == "included":
        memberships.add((work_id, snapshot))
    else:
        memberships.discard((work_id, snapshot))


def count_embeddings_for_snapshot(
    embeddings: dict[tuple[int, str], object],
    memberships: set[tuple[int, str]],
    *,
    snapshot: str,
    version: str,
) -> int:
    return sum(
        1 for (work_id, ev) in embeddings if ev == version and (work_id, snapshot) in memberships
    )


def delete_embeddings_for_snapshot(
    embeddings: dict[tuple[int, str], object],
    memberships: set[tuple[int, str]],
    *,
    snapshot: str,
    version: str,
) -> None:
    for key in list(embeddings):
        work_id, ev = key
        if ev == version and (work_id, snapshot) in memberships:
            del embeddings[key]


def is_hydrate_work_update(compact: str) -> bool:
    return compact.startswith(
        "UPDATE works SET title = %s, abstract = %s, type = %s, language = %s, doi = %s, "
        "citation_count = %s, publication_date = %s, year = %s, updated_date = %s, "
        "last_ingest_run_id = %s, updated_at = NOW() WHERE id = %s"
    )


def apply_hydrate_work_update(works: dict[int, dict], params: tuple) -> None:
    work_id = int(params[10])
    work = works[work_id]
    work["title"] = params[0]
    work["abstract"] = params[1]
    work["type"] = params[2]
    work["language"] = params[3]
    work["doi"] = params[4]
    work["citation_count"] = params[5]
    work["publication_date"] = params[6]
    work["year"] = params[7]
    work["updated_date"] = params[8]
    work["last_ingest_run_id"] = params[9]


def embedding_coverage_counts(
    works: dict[int, dict],
    memberships: set[tuple[int, str]],
    embeddings: dict[tuple[int, str], object],
    *,
    snapshot: str,
    version: str,
    inclusion_key: str = "inclusion_status",
) -> tuple[int, int]:
    work_ids = included_work_ids(works, memberships, snapshot, inclusion_key=inclusion_key)
    embedded = sum(1 for work_id in work_ids if (work_id, version) in embeddings)
    return len(work_ids), embedded
