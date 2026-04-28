"""Import an approved corpus-v2 candidate plan as a versioned source snapshot."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.config import IngestRun, SourceSnapshotVersion
from pipeline.normalize import clean_openalex_text
from pipeline.policy import CorpusPolicy

NEXT_STEP = (
    "metadata/text hydration for this snapshot, or an explicit title-only embedding version; "
    "do not run ranking yet"
)
INGEST_MODE = "snapshot-import"
UNKNOWN_WORK_TYPE = "unknown"
DEFAULT_PLAN_LANGUAGE = "en"


class CorpusV2IngestError(RuntimeError):
    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class CandidatePlanDocument:
    path: Path
    sha256: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class CandidateImportResult:
    action: str
    bucket_id: str | None
    missing_abstract: bool
    missing_doi: bool
    defaulted_language: bool
    unknown_type: bool
    embedding_ready: bool


def candidate_plan_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_candidate_plan(path: Path) -> CandidatePlanDocument:
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise CorpusV2IngestError(f"candidate plan is not valid JSON: {exc}", code=2) from exc
    if not isinstance(payload, Mapping):
        raise CorpusV2IngestError("candidate plan JSON must be an object", code=2)
    validate_candidate_plan(payload)
    return CandidatePlanDocument(path=path, sha256=sha, payload=payload)


def validate_candidate_plan(plan: Mapping[str, Any]) -> None:
    selected_total = _required_int(plan, "selected_total")
    if selected_total <= 0:
        raise CorpusV2IngestError("selected_total must be > 0", code=2)

    selected_candidates = plan.get("selected_candidates")
    if not isinstance(selected_candidates, list):
        raise CorpusV2IngestError("selected_candidates must be present and must be a list", code=2)
    if len(selected_candidates) != selected_total:
        raise CorpusV2IngestError(
            f"selected_candidates length ({len(selected_candidates)}) does not match selected_total ({selected_total})",
            code=2,
        )

    target_min = _required_int(plan, "target_min")
    target_max = _required_int(plan, "target_max")
    if target_max < target_min:
        raise CorpusV2IngestError("target_max must be >= target_min", code=2)
    if selected_total < target_min or selected_total > target_max:
        raise CorpusV2IngestError(
            f"selected_total ({selected_total}) must be inside target range {target_min}-{target_max}",
            code=2,
        )

    if plan.get("auth_mode") != "api_key":
        raise CorpusV2IngestError("candidate plan must have auth_mode='api_key'", code=2)
    if plan.get("api_key_provided") is not True:
        raise CorpusV2IngestError("candidate plan must have api_key_provided=true", code=2)
    _reject_raw_secret_fields(plan)
    _validate_dry_run_artifact(plan)
    _validate_candidate_rows(selected_candidates)


def run_corpus_v2_ingest_from_plan(
    *,
    candidate_plan_path: Path,
    snapshot_version: str,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
) -> dict[str, Any]:
    snapshot_version = (snapshot_version or "").strip()
    if not snapshot_version:
        raise CorpusV2IngestError("--snapshot-version is required and must not be blank", code=2)

    plan_doc = load_candidate_plan(candidate_plan_path)
    policy_name, policy_hash = _derive_policy_identity(plan_doc.payload, plan_doc.sha256)
    snapshot = SourceSnapshotVersion(
        source_snapshot_version=snapshot_version,
        policy_name=policy_name,
        policy_hash=policy_hash,
        ingest_mode=INGEST_MODE,
        created_at=datetime.now(UTC),
        note=(
            "Corpus v2 candidate-plan ingest; "
            f"candidate_plan_sha256={plan_doc.sha256}"
        ),
    )
    selected_candidates = list(plan_doc.payload["selected_candidates"])
    warnings = _metadata_warnings(selected_candidates)
    ingest_run = IngestRun.start(
        snapshot=snapshot,
        config={
            "ingest_mode": INGEST_MODE,
            "candidate_plan_path": str(candidate_plan_path),
            "candidate_plan_sha256": plan_doc.sha256,
            "selected_total": int(plan_doc.payload["selected_total"]),
            "target_min": int(plan_doc.payload["target_min"]),
            "target_max": int(plan_doc.payload["target_max"]),
            "auth_mode": plan_doc.payload.get("auth_mode"),
            "api_key_provided": plan_doc.payload.get("api_key_provided"),
            "metadata_defaults": {
                "missing_type": UNKNOWN_WORK_TYPE,
                "missing_language": DEFAULT_PLAN_LANGUAGE,
            },
            "openalex_enrichment": "not_run",
            "warnings": warnings,
        },
    )
    dsn = database_url or database_url_from_env()

    try:
        with psycopg.connect(dsn, autocommit=False) as conn:
            _register_snapshot_and_ingest_run(conn, snapshot, ingest_run)
            conn.commit()
            try:
                with conn.transaction():
                    summary = _ingest_plan_candidates(
                        conn,
                        plan_doc=plan_doc,
                        snapshot=snapshot,
                        ingest_run=ingest_run,
                        warnings=warnings,
                    )
                    _update_ingest_run_final(
                        conn,
                        ingest_run.ingest_run_id,
                        status="succeeded",
                        counts=summary,
                        error_message=None,
                    )
                conn.commit()
            except Exception as exc:
                _mark_ingest_failed(conn, ingest_run.ingest_run_id, str(exc))
                raise
    except CorpusV2IngestError:
        raise
    except Exception as exc:
        raise CorpusV2IngestError(f"corpus-v2 ingest failed: {exc}", code=1) from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(render_ingest_summary_markdown(summary), encoding="utf-8", newline="\n")
    return summary


def render_ingest_summary_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Corpus v2 source snapshot ingest summary",
        "",
        f"- **snapshot_version:** `{summary.get('snapshot_version')}`",
        f"- **ingest_run_id:** `{summary.get('ingest_run_id')}`",
        f"- **candidate_plan_sha256:** `{summary.get('candidate_plan_sha256')}`",
        f"- **selected_total:** `{summary.get('selected_total')}`",
        f"- **inserted_count:** `{summary.get('inserted_count')}`",
        f"- **updated_count:** `{summary.get('updated_count')}`",
        f"- **skipped_existing_count:** `{summary.get('skipped_existing_count')}`",
        f"- **failed_count:** `{summary.get('failed_count')}`",
        f"- **missing_abstract_count:** `{summary.get('missing_abstract_count')}`",
        f"- **missing_doi_count:** `{summary.get('missing_doi_count')}`",
        f"- **defaulted_language_count:** `{summary.get('defaulted_language_count')}`",
        f"- **unknown_type_count:** `{summary.get('unknown_type_count')}`",
        f"- **embedding_ready_count:** `{summary.get('embedding_ready_count')}`",
        f"- **embedding_blocked_count:** `{summary.get('embedding_blocked_count')}`",
        f"- **snapshot_embedding_ready:** `{summary.get('snapshot_embedding_ready')}`",
        f"- **openalex_enrichment:** `{summary.get('openalex_enrichment')}`",
        "",
        "## Embedding Readiness",
        "",
        "This command creates a source snapshot import, not an embedding-ready corpus. Missing abstracts remain `NULL`; "
        "defaulted language values come from the candidate-plan policy; `unknown` work types are not validated document types.",
        "",
        "## Counts by bucket",
        "",
    ]
    for bucket_id, count in sorted((summary.get("counts_by_bucket") or {}).items()):
        lines.append(f"- **{bucket_id}:** `{count}`")
    lines.extend(["", "## Warnings", ""])
    warnings = list(summary.get("warnings") or [])
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Next step",
            "",
            str(summary.get("next_step") or NEXT_STEP),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _register_snapshot_and_ingest_run(
    conn: psycopg.Connection,
    snapshot: SourceSnapshotVersion,
    ingest_run: IngestRun,
) -> None:
    conn.execute(
        """
        INSERT INTO source_snapshot_versions (
            source_snapshot_version, policy_name, policy_hash, ingest_mode, note, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
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
            json.dumps(ingest_run.config, sort_keys=True),
        ),
    )


def _ingest_plan_candidates(
    conn: psycopg.Connection,
    *,
    plan_doc: CandidatePlanDocument,
    snapshot: SourceSnapshotVersion,
    ingest_run: IngestRun,
    warnings: list[str],
) -> dict[str, Any]:
    selected_candidates = list(plan_doc.payload["selected_candidates"])
    known_source_slugs = _load_known_source_slugs(conn)
    dedup_seen = _CandidateDedup()
    results: list[CandidateImportResult] = []
    skipped_existing_count = 0

    for index, candidate in enumerate(selected_candidates, start=1):
        assert isinstance(candidate, Mapping)
        duplicate_reason = dedup_seen.try_add(candidate)
        if duplicate_reason:
            skipped_existing_count += 1
            continue
        result = _upsert_candidate(
            conn,
            candidate=candidate,
            selected_rank=index,
            plan_doc=plan_doc,
            snapshot=snapshot,
            ingest_run=ingest_run,
            known_source_slugs=known_source_slugs,
        )
        results.append(result)

    inserted_count = sum(1 for r in results if r.action == "inserted")
    updated_count = sum(1 for r in results if r.action == "updated")
    embedding_ready_count = sum(1 for r in results if r.embedding_ready)
    embedding_blocked_count = len(results) - embedding_ready_count
    counts_by_bucket: dict[str, int] = {}
    for result in results:
        bucket = result.bucket_id or "unknown"
        counts_by_bucket[bucket] = counts_by_bucket.get(bucket, 0) + 1

    return {
        "snapshot_version": snapshot.source_snapshot_version,
        "ingest_run_id": ingest_run.ingest_run_id,
        "candidate_plan_path": str(plan_doc.path),
        "candidate_plan_sha256": plan_doc.sha256,
        "selected_total": int(plan_doc.payload["selected_total"]),
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "skipped_existing_count": skipped_existing_count,
        "failed_count": 0,
        "counts_by_bucket": counts_by_bucket,
        "missing_abstract_count": sum(1 for r in results if r.missing_abstract),
        "missing_doi_count": sum(1 for r in results if r.missing_doi),
        "defaulted_language_count": sum(1 for r in results if r.defaulted_language),
        "unknown_type_count": sum(1 for r in results if r.unknown_type),
        "embedding_ready_count": embedding_ready_count,
        "embedding_blocked_count": embedding_blocked_count,
        "snapshot_embedding_ready": embedding_blocked_count == 0,
        "openalex_enrichment": "not_run",
        "warnings": warnings,
        "next_step": NEXT_STEP,
    }


def _upsert_candidate(
    conn: psycopg.Connection,
    *,
    candidate: Mapping[str, Any],
    selected_rank: int,
    plan_doc: CandidatePlanDocument,
    snapshot: SourceSnapshotVersion,
    ingest_run: IngestRun,
    known_source_slugs: set[str],
) -> CandidateImportResult:
    work = _candidate_to_work(candidate)
    provenance_payload = _raw_payload_for_candidate(
        candidate,
        work=work,
        selected_rank=selected_rank,
        plan_doc=plan_doc,
    )
    content_hash = _content_hash(provenance_payload)
    source_slug = _candidate_source_slug(candidate, known_source_slugs)
    _insert_raw_candidate_payload(
        conn,
        openalex_id=work["openalex_id"],
        ingest_run_id=ingest_run.ingest_run_id,
        source_snapshot_version=snapshot.source_snapshot_version,
        source_slug=source_slug,
        updated_date=work["updated_date"],
        payload=provenance_payload,
        content_hash=content_hash,
    )

    existing = _find_existing_work(conn, openalex_id=work["openalex_id"], doi=work["doi"])
    if existing is None:
        _insert_work(
            conn,
            work=work,
            source_slug=source_slug,
            raw_content_hash=content_hash,
            snapshot_version=snapshot.source_snapshot_version,
            ingest_run_id=ingest_run.ingest_run_id,
        )
        action = "inserted"
    else:
        _update_work(
            conn,
            work_id=existing,
            work=work,
            source_slug=source_slug,
            raw_content_hash=content_hash,
            snapshot_version=snapshot.source_snapshot_version,
            ingest_run_id=ingest_run.ingest_run_id,
        )
        action = "updated"

    return CandidateImportResult(
        action=action,
        bucket_id=str(candidate.get("bucket_id") or "") or None,
        missing_abstract=not bool(work["abstract"]),
        missing_doi=not bool(work["doi"]),
        defaulted_language=bool(work["language_defaulted"]),
        unknown_type=bool(work["type_unknown"]),
        embedding_ready=bool(work["embedding_ready"]),
    )


def _candidate_to_work(candidate: Mapping[str, Any]) -> dict[str, Any]:
    abstract = _candidate_optional_text(candidate, "abstract")
    raw_type = candidate.get("type") or candidate.get("work_type")
    work_type = str(raw_type or UNKNOWN_WORK_TYPE)
    raw_language = candidate.get("language")
    language = str(raw_language or DEFAULT_PLAN_LANGUAGE)
    type_unknown = not bool(raw_type)
    language_defaulted = not bool(raw_language)
    return {
        "openalex_id": _candidate_openalex_id(candidate),
        "doi": _norm_doi_value(candidate.get("doi")),
        "title": clean_openalex_text(str(candidate.get("title") or "Untitled work")),
        "abstract": abstract,
        "year": int(candidate.get("year") or candidate.get("publication_year")),
        "type": work_type,
        "language": language,
        "publication_date": candidate.get("publication_date"),
        "updated_date": candidate.get("updated_date"),
        "citation_count": int(candidate.get("citation_count") or candidate.get("cited_by_count") or 0),
        "is_core_corpus": _candidate_is_core(candidate),
        "language_defaulted": language_defaulted,
        "type_unknown": type_unknown,
        "embedding_ready": bool(abstract) and not language_defaulted and not type_unknown,
    }


def _insert_work(
    conn: psycopg.Connection,
    *,
    work: Mapping[str, Any],
    source_slug: str | None,
    raw_content_hash: str,
    snapshot_version: str,
    ingest_run_id: str,
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
            NULL, %s, %s, %s, 'included', NULL,
            %s, %s, %s
        )
        RETURNING id
        """,
        (
            work["openalex_id"],
            work["title"],
            work["abstract"],
            work["year"],
            work["doi"],
            work["type"],
            work["language"],
            work["publication_date"],
            work["updated_date"],
            source_slug,
            work["citation_count"],
            work["is_core_corpus"],
            raw_content_hash,
            snapshot_version,
            ingest_run_id,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _update_work(
    conn: psycopg.Connection,
    *,
    work_id: int,
    work: Mapping[str, Any],
    source_slug: str | None,
    raw_content_hash: str,
    snapshot_version: str,
    ingest_run_id: str,
) -> None:
    conn.execute(
        """
        UPDATE works
        SET openalex_id = %s,
            title = %s,
            abstract = %s,
            year = %s,
            doi = %s,
            type = %s,
            language = %s,
            publication_date = %s,
            updated_date = %s,
            source_slug = %s,
            citation_count = %s,
            is_core_corpus = %s,
            inclusion_status = 'included',
            exclusion_reason = NULL,
            raw_content_hash = %s,
            corpus_snapshot_version = %s,
            last_ingest_run_id = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            work["openalex_id"],
            work["title"],
            work["abstract"],
            work["year"],
            work["doi"],
            work["type"],
            work["language"],
            work["publication_date"],
            work["updated_date"],
            source_slug,
            work["citation_count"],
            work["is_core_corpus"],
            raw_content_hash,
            snapshot_version,
            ingest_run_id,
            work_id,
        ),
    )


def _insert_raw_candidate_payload(
    conn: psycopg.Connection,
    *,
    openalex_id: str,
    ingest_run_id: str,
    source_snapshot_version: str,
    source_slug: str | None,
    updated_date: str | None,
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
            ingest_run_id,
            source_snapshot_version,
            source_slug,
            "candidate-plan",
            updated_date,
            json.dumps(payload, sort_keys=True, ensure_ascii=False),
            content_hash,
        ),
    )


def _find_existing_work(conn: psycopg.Connection, *, openalex_id: str, doi: str | None) -> int | None:
    row = conn.execute("SELECT id FROM works WHERE openalex_id = %s", (openalex_id,)).fetchone()
    if row is not None:
        return int(row[0])
    if not doi:
        return None
    candidates = [doi, f"https://doi.org/{doi}", f"http://doi.org/{doi}"]
    row = conn.execute(
        """
        SELECT id
        FROM works
        WHERE doi IS NOT NULL
          AND lower(doi) = ANY(%s)
        ORDER BY id
        LIMIT 1
        """,
        ([d.casefold() for d in candidates],),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def _update_ingest_run_final(
    conn: psycopg.Connection,
    ingest_run_id: str,
    *,
    status: str,
    counts: Mapping[str, Any] | None,
    error_message: str | None,
) -> None:
    conn.execute(
        """
        UPDATE ingest_runs
        SET status = %s,
            finished_at = %s,
            counts_json = %s::jsonb,
            error_message = %s
        WHERE ingest_run_id = %s
        """,
        (
            status,
            datetime.now(UTC),
            json.dumps(counts, sort_keys=True) if counts is not None else None,
            error_message,
            ingest_run_id,
        ),
    )


def _mark_ingest_failed(conn: psycopg.Connection, ingest_run_id: str, message: str) -> None:
    try:
        _update_ingest_run_final(conn, ingest_run_id, status="failed", counts=None, error_message=message)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _load_known_source_slugs(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute("SELECT source_slug FROM source_policies").fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def _candidate_source_slug(candidate: Mapping[str, Any], known_source_slugs: set[str]) -> str | None:
    raw = candidate.get("source_slug")
    if isinstance(raw, str) and raw.strip() in known_source_slugs:
        return raw.strip()
    source_name = candidate.get("source_display_name")
    if not source_name:
        return None
    matched = CorpusPolicy().classify_source(None, str(source_name))
    if matched and matched.slug in known_source_slugs:
        return matched.slug
    return None


def _candidate_is_core(candidate: Mapping[str, Any]) -> bool:
    if candidate.get("bucket_id") == "core_mir_existing_sources":
        return True
    source_name = candidate.get("source_display_name")
    if not source_name:
        return False
    matched = CorpusPolicy().classify_source(None, str(source_name))
    return bool(matched and matched.venue_class == "core")


def _raw_payload_for_candidate(
    candidate: Mapping[str, Any],
    *,
    work: Mapping[str, Any],
    selected_rank: int,
    plan_doc: CandidatePlanDocument,
) -> dict[str, Any]:
    return {
        "source": "corpus-v2-candidate-plan",
        "candidate_plan_path": str(plan_doc.path),
        "candidate_plan_sha256": plan_doc.sha256,
        "selected_rank": selected_rank,
        "bucket_id": candidate.get("bucket_id"),
        "inclusion_reason": candidate.get("inclusion_reason"),
        "matched_terms": list(candidate.get("matched_terms") or []),
        "selected_candidate": dict(candidate),
        "derived_work_fields": dict(work),
        "derived_field_provenance": _derived_field_provenance(work),
    }


def _metadata_warnings(selected_candidates: list[Any]) -> list[str]:
    warnings: list[str] = []
    if any(isinstance(c, Mapping) and not c.get("abstract") for c in selected_candidates):
        warnings.append("Candidate plan omits abstracts for one or more works; abstract remains NULL until text hydration.")
    if any(isinstance(c, Mapping) and not (c.get("type") or c.get("work_type")) for c in selected_candidates):
        warnings.append(
            "Candidate plan omits OpenAlex work type for one or more works; stored missing type as 'unknown' "
            "and did not validate it as an included document type."
        )
    if any(isinstance(c, Mapping) and not c.get("language") for c in selected_candidates):
        warnings.append(
            "Candidate plan omits language for one or more works; stored missing language as 'en' from the "
            "candidate-plan policy filter, not observed OpenAlex metadata."
        )
    warnings.append("No live OpenAlex enrichment, embeddings, clustering, ranking, or bridge-weight writes were run.")
    return warnings


def _derived_field_provenance(work: Mapping[str, Any]) -> dict[str, str]:
    return {
        "abstract": "observed_candidate_plan_field" if work.get("abstract") else "missing_in_candidate_plan",
        "language": (
            "candidate_plan_policy_default"
            if work.get("language_defaulted")
            else "observed_candidate_plan_field"
        ),
        "type": (
            "candidate_plan_unknown_default_not_validated"
            if work.get("type_unknown")
            else "observed_candidate_plan_field"
        ),
    }


def _derive_policy_identity(plan: Mapping[str, Any], plan_sha256: str) -> tuple[str, str]:
    ref = plan.get("policy_reference")
    if isinstance(ref, Mapping):
        name = ref.get("name")
        policy_hash = ref.get("policy_hash")
        if isinstance(name, str) and name.strip() and isinstance(policy_hash, str) and policy_hash.strip():
            return name.strip(), policy_hash.strip()
    return "candidate-plan-sha256", plan_sha256[:12]


def _required_int(plan: Mapping[str, Any], key: str) -> int:
    value = plan.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CorpusV2IngestError(f"{key} must be present and must be an integer", code=2)
    return value


def _validate_candidate_rows(selected_candidates: list[Any]) -> None:
    for idx, candidate in enumerate(selected_candidates, start=1):
        if not isinstance(candidate, Mapping):
            raise CorpusV2IngestError(f"selected_candidates[{idx}] must be an object", code=2)
        _candidate_openalex_id(candidate)
        if not str(candidate.get("title") or "").strip():
            raise CorpusV2IngestError(f"selected_candidates[{idx}] is missing title", code=2)
        if isinstance(candidate.get("year") or candidate.get("publication_year"), bool):
            raise CorpusV2IngestError(f"selected_candidates[{idx}] year must be an integer", code=2)
        if not isinstance(candidate.get("year") or candidate.get("publication_year"), int):
            raise CorpusV2IngestError(f"selected_candidates[{idx}] is missing integer year", code=2)


def _candidate_openalex_id(candidate: Mapping[str, Any]) -> str:
    value = candidate.get("openalex_id") or candidate.get("id")
    if not isinstance(value, str) or not value.strip():
        raise CorpusV2IngestError("selected candidate is missing openalex_id", code=2)
    return value.strip()


def _candidate_optional_text(candidate: Mapping[str, Any], key: str) -> str | None:
    value = candidate.get(key)
    if not isinstance(value, str):
        return None
    cleaned = clean_openalex_text(value)
    return cleaned if cleaned.strip() else None


def _validate_dry_run_artifact(plan: Mapping[str, Any]) -> None:
    caveats = plan.get("caveats")
    if not isinstance(caveats, list):
        raise CorpusV2IngestError("candidate plan must include dry-run caveats", code=2)
    caveat_text = " ".join(str(c).casefold() for c in caveats)
    if "dry-run" not in caveat_text or ("no postgres" not in caveat_text and "no database" not in caveat_text):
        raise CorpusV2IngestError("candidate plan must be dry-run/planning output with no DB writes", code=2)


_FORBIDDEN_SECRET_FIELD_NAMES = {
    "api_key",
    "openalex_api_key",
    "raw_api_key",
    "mailto",
    "raw_mailto",
    "contact",
    "contact_email",
    "email",
}
_ALLOWED_SECRET_METADATA_FIELD_NAMES = {"api_key_provided", "auth_mode", "contact_mode", "contact_provided"}
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


def _reject_raw_secret_fields(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            key_norm = key_text.casefold()
            child_path = f"{path}.{key_text}"
            if key_norm not in _ALLOWED_SECRET_METADATA_FIELD_NAMES and (
                key_norm in _FORBIDDEN_SECRET_FIELD_NAMES
                or key_norm.endswith("_api_key")
                or key_norm.endswith("_mailto")
            ):
                if child not in (None, "", False):
                    raise CorpusV2IngestError(f"candidate plan contains raw secret/contact field at {child_path}", code=2)
            _reject_raw_secret_fields(child, path=child_path)
        return
    if isinstance(value, list):
        for idx, child in enumerate(value):
            _reject_raw_secret_fields(child, path=f"{path}[{idx}]")
        return
    if isinstance(value, str) and _EMAIL_RE.search(value) and any(
        token in path.casefold() for token in ("mailto", "contact", "email")
    ):
        raise CorpusV2IngestError(f"candidate plan contains raw contact value at {path}", code=2)


def _norm_doi_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    doi = value.strip().casefold()
    for prefix in ("https://doi.org/", "http://doi.org/"):
        if doi.startswith(prefix):
            doi = doi[len(prefix) :]
    return doi or None


def _norm_title_key(candidate: Mapping[str, Any]) -> str:
    title = str(candidate.get("title") or "").strip().casefold()
    title = re.sub(r"\s+", " ", title)
    return title[:280] if title else ""


def _content_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass
class _CandidateDedup:
    openalex_ids: set[str]
    dois: set[str]
    titles: set[str]

    def __init__(self) -> None:
        self.openalex_ids = set()
        self.dois = set()
        self.titles = set()

    def try_add(self, candidate: Mapping[str, Any]) -> str | None:
        openalex_id = _candidate_openalex_id(candidate)
        doi = _norm_doi_value(candidate.get("doi"))
        title_key = _norm_title_key(candidate)
        if openalex_id in self.openalex_ids:
            return "openalex_id"
        if doi and doi in self.dois:
            return "doi"
        if title_key and title_key in self.titles:
            return "normalized_title"
        self.openalex_ids.add(openalex_id)
        if doi:
            self.dois.add(doi)
        if title_key:
            self.titles.add(title_key)
        return None


__all__ = [
    "CorpusV2IngestError",
    "candidate_plan_sha256",
    "load_candidate_plan",
    "render_ingest_summary_markdown",
    "run_corpus_v2_ingest_from_plan",
    "validate_candidate_plan",
]
