"""Ingest the fresh hybrid candidate plan as a source snapshot.

This is the first controlled DB-writing step on the fresh hybrid confirmation
path. It reuses the corpus-v2 candidate-plan ingest machinery and writes only
snapshot/ingest/raw-work/work rows from the committed plan file. It does not
call OpenAlex, create embeddings/clusters/rankings, write paper_scores, import
labels, or authorize shadow/prod.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import psycopg

from pipeline.config import IngestRun, SourceSnapshotVersion
from pipeline.corpus_v2_ingest_from_plan import (
    CandidatePlanDocument,
    CorpusV2IngestError,
    _derive_policy_identity,
    _ingest_plan_candidates,
    _mark_ingest_failed,
    _metadata_warnings,
    _register_snapshot_and_ingest_run,
    _update_ingest_run_final,
    validate_candidate_plan,
)
from pipeline.error_reporting import safe_exception_summary
from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_hybrid_candidate_plan_ingest"
INGEST_VERSION = "ml-fresh-hybrid-candidate-plan-ingest-v1"
PLAN_ARTIFACT_TYPE = "ml_fresh_hybrid_corpus_candidate_plan"
PLAN_VERSION = "ml-fresh-hybrid-corpus-candidate-plan-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
OLD_EVAL_WORK_SET_SHA256 = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
DEFAULT_INGEST_MODE = "snapshot-import"
FRESH_HYBRID_INGEST_INTENT = "fresh_hybrid_candidate_plan_snapshot_import"

ALLOWED_WRITE_TABLES = (
    "source_snapshot_versions",
    "ingest_runs",
    "raw_openalex_works",
    "works",
)

CAVEATS = (
    "Controlled local Postgres snapshot ingest from committed candidate plan only.",
    "No OpenAlex/network calls are made by this ingest command.",
    "No embeddings, clustering, ranking run, paper_scores, label import, hybrid scoring, shadow, or production changes.",
    "Snapshot is eval-only / fresh hybrid validation candidate source; it is not a production/default corpus switch.",
    "Old-217 overlap remains recorded in the plan and must be excluded later during materialization/gates.",
    "No shadow or production authorization.",
)


class MLFreshHybridCandidatePlanIngestError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _HybridPlanDocument:
    path: Path
    sha256: str
    payload: Mapping[str, Any]
    corpus_v2_payload: Mapping[str, Any]


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_snapshot_version() -> str:
    return f"source-snapshot-fresh-hybrid-v1-{datetime.now(UTC).strftime('%Y%m%d')}"


def _database_url_from_env() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "research_radar")
    password = os.environ.get("PGPASSWORD", "research_radar")
    db = os.environ.get("PGDATABASE", "research_radar")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _redacted_database_url(database_url: str) -> str:
    parsed = urlparse(str(database_url or ""))
    if not parsed.scheme:
        return "(unparseable local database target)"
    user = parsed.username or ""
    userinfo = f"{user}:***@" if user else ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{userinfo}{parsed.hostname or ''}{port}{parsed.path or ''}"


def assert_local_database_url(database_url: str) -> dict[str, Any]:
    text = str(database_url or "").strip()
    if not text:
        raise MLFreshHybridCandidatePlanIngestError("database URL is required")
    lower = text.lower()
    forbidden_hosts = ("railway", "rlwy", "render.com", "amazonaws", "neon.tech", "supabase", "herokuapp", "azure.com")
    if any(token in lower for token in forbidden_hosts):
        raise MLFreshHybridCandidatePlanIngestError(
            "database URL must target local Postgres, not hosted production infrastructure"
        )
    parsed = urlparse(text)
    host = parsed.hostname
    local_hosts = {None, "", "localhost", "127.0.0.1", "::1", "host.docker.internal"}
    if host not in local_hosts and not str(host).endswith(".local"):
        raise MLFreshHybridCandidatePlanIngestError(
            f"database URL must target local Postgres; host {host!r} is not allowed"
        )
    return {
        "database_target_redacted": _redacted_database_url(text),
        "database_url_host": host or "(local socket)",
        "database_url_port": parsed.port,
        "database_name": (parsed.path or "").lstrip("/") or None,
        "local_database_url_confirmed": True,
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshHybridCandidatePlanIngestError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshHybridCandidatePlanIngestError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshHybridCandidatePlanIngestError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLFreshHybridCandidatePlanIngestError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshHybridCandidatePlanIngestError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshHybridCandidatePlanIngestError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    if metadata.get("disallowed_eval_work_set_sha256") != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshHybridCandidatePlanIngestError("policy disallowed old 217 SHA is missing or unexpected")
    return metadata


def _validate_hybrid_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-hybrid-corpus-candidate-plan")
    if metadata.get("artifact_type") != PLAN_ARTIFACT_TYPE:
        raise MLFreshHybridCandidatePlanIngestError(
            f"expected plan metadata.artifact_type={PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PLAN_VERSION:
        raise MLFreshHybridCandidatePlanIngestError(
            f"expected plan metadata.plan_version={PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    if _get(payload, "candidate_selection.candidate_threshold_plausibly_met") is not True:
        raise MLFreshHybridCandidatePlanIngestError("candidate_threshold_plausibly_met must be true")
    if _get(payload, "readiness_estimate.expected_next_stage") != "ingest_fresh_hybrid_candidate_plan_as_snapshot_v1":
        raise MLFreshHybridCandidatePlanIngestError(
            "readiness_estimate.expected_next_stage must be ingest_fresh_hybrid_candidate_plan_as_snapshot_v1"
        )
    eligible = _get(payload, "candidate_selection.estimated_confirmatory_eligible_after_old_217_exclusion")
    if not isinstance(eligible, int) or isinstance(eligible, bool) or eligible < 100:
        raise MLFreshHybridCandidatePlanIngestError("estimated confirmatory eligible count must be >= 100")
    selected_total = _get(payload, "candidate_selection.selected_total")
    selected_candidates = _get(payload, "candidate_selection.selected_candidates")
    if not isinstance(selected_total, int) or isinstance(selected_total, bool) or selected_total <= 0:
        raise MLFreshHybridCandidatePlanIngestError("candidate plan selected_total must be > 0")
    if not isinstance(selected_candidates, list):
        raise MLFreshHybridCandidatePlanIngestError("candidate plan selected_candidates must be present")
    if len(selected_candidates) != selected_total:
        raise MLFreshHybridCandidatePlanIngestError("selected_total must match len(selected_candidates)")
    return metadata


def _candidate_to_corpus_v2_row(row: Mapping[str, Any]) -> dict[str, Any]:
    openalex_id = row.get("openalex_id")
    if not isinstance(openalex_id, str) or not openalex_id.strip():
        canonical = row.get("canonical_openalex_work_id")
        openalex_id = f"https://openalex.org/{canonical}" if canonical else None
    return {
        "openalex_id": openalex_id,
        "doi": row.get("doi"),
        "title": row.get("title"),
        "year": row.get("year"),
        "citation_count": row.get("citation_count"),
        "source_display_name": row.get("source_display_name"),
        "bucket_id": row.get("bucket_id"),
        "inclusion_reason": row.get("inclusion_reason"),
        "matched_terms": row.get("matched_terms") or [],
        "exclusion_reason": row.get("exclusion_reason"),
        "type": row.get("type"),
        "language": row.get("language"),
        "abstract": row.get("abstract"),
        "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
        "old_217_overlap": row.get("old_217_overlap"),
        "underpowered_source_overlap": row.get("underpowered_source_overlap"),
        "confirmatory_metric_candidate": row.get("confirmatory_metric_candidate"),
        "negative_or_borderline_candidate": row.get("negative_or_borderline_candidate"),
    }


def _corpus_v2_plan_from_hybrid_plan(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _metadata(payload, name="fresh-hybrid-corpus-candidate-plan")
    contact = metadata.get("openalex_contact_provenance")
    if not isinstance(contact, Mapping):
        contact = {}
    selected_candidates = [
        _candidate_to_corpus_v2_row(row)
        for row in (_get(payload, "candidate_selection.selected_candidates") or [])
        if isinstance(row, Mapping)
    ]
    selected_total = int(_get(payload, "candidate_selection.selected_total") or len(selected_candidates))
    plan = {
        "generated_at": metadata.get("generated_at"),
        "contact_provided": bool(contact.get("contact_provided")),
        "contact_mode": contact.get("contact_mode") or "none",
        "api_key_provided": bool(contact.get("api_key_provided")),
        "auth_mode": contact.get("auth_mode") or "no_key",
        "policy_reference": {"name": "fresh-hybrid-candidate-plan", "policy_hash": str(metadata.get("plan_version") or PLAN_VERSION)},
        "target_min": int(_get(payload, "planning_context.target_min") or 1),
        "target_max": int(_get(payload, "planning_context.target_max") or max(selected_total, 1)),
        "selected_total": selected_total,
        "bucket_summaries": _get(payload, "bucket_summary.by_bucket") or [],
        "selected_candidates": selected_candidates,
        "caveats": [
            "Dry-run only: no Postgres writes, no database writes, no snapshot, no embeddings, clustering, or ranking were performed by the candidate plan.",
            "Fresh hybrid candidate plan is planning output only; ingest command is the controlled write step.",
        ],
    }
    validate_candidate_plan(plan)
    return plan


def _load_hybrid_plan_document(path: Path) -> _HybridPlanDocument:
    raw = path.read_bytes()
    sha = sha256_file(path)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise MLFreshHybridCandidatePlanIngestError(f"candidate plan is not valid JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise MLFreshHybridCandidatePlanIngestError("candidate plan JSON must be an object")
    _validate_hybrid_plan(payload)
    corpus_v2_payload = _corpus_v2_plan_from_hybrid_plan(payload)
    return _HybridPlanDocument(path=path, sha256=sha, payload=payload, corpus_v2_payload=corpus_v2_payload)


def _snapshot_exists(conn: Any, snapshot_version: str) -> bool:
    row = conn.execute(
        "SELECT source_snapshot_version FROM source_snapshot_versions WHERE source_snapshot_version = %s",
        (snapshot_version,),
    ).fetchone()
    return row is not None


def _affected_row_counts_from_summary(summary: Mapping[str, Any]) -> dict[str, int]:
    written_work_rows = int(summary.get("inserted_count") or 0) + int(summary.get("updated_count") or 0)
    return {
        "source_snapshot_versions": 1,
        "ingest_runs": 2,
        "raw_openalex_works": written_work_rows,
        "works_inserted_or_updated": written_work_rows,
    }


def _run_ingest_with_conn(
    conn: Any,
    *,
    plan_doc: _HybridPlanDocument,
    snapshot_version: str,
) -> dict[str, Any]:
    if _snapshot_exists(conn, snapshot_version):
        raise MLFreshHybridCandidatePlanIngestError(
            f"snapshot_version already exists: {snapshot_version}; v1 has no idempotent re-run mode"
        )
    policy_name, policy_hash = _derive_policy_identity(plan_doc.corpus_v2_payload, plan_doc.sha256)
    snapshot = SourceSnapshotVersion(
        source_snapshot_version=snapshot_version,
        policy_name=policy_name,
        policy_hash=policy_hash,
        ingest_mode=DEFAULT_INGEST_MODE,
        created_at=datetime.now(UTC),
        note=(
            "Fresh hybrid candidate plan ingest; eval_only=true; "
            f"fresh_hybrid_candidate_plan_sha256={plan_doc.sha256}"
        ),
    )
    corpus_doc = CandidatePlanDocument(path=plan_doc.path, sha256=plan_doc.sha256, payload=plan_doc.corpus_v2_payload)
    warnings = _metadata_warnings(list(plan_doc.corpus_v2_payload["selected_candidates"]))
    ingest_run = IngestRun.start(
        snapshot=snapshot,
        config={
            "ingest_mode": DEFAULT_INGEST_MODE,
            "fresh_hybrid_ingest_intent": FRESH_HYBRID_INGEST_INTENT,
            "eval_only": True,
            "fresh_hybrid_validation_candidate_source": True,
            "candidate_plan_path": str(plan_doc.path),
            "candidate_plan_sha256": plan_doc.sha256,
            "selected_total": int(plan_doc.corpus_v2_payload["selected_total"]),
            "openalex_network_calls": "not_run",
            "warnings": warnings,
        },
    )
    _register_snapshot_and_ingest_run(conn, snapshot, ingest_run)
    conn.commit()
    try:
        with conn.transaction():
            summary = _ingest_plan_candidates(
                conn,
                plan_doc=corpus_doc,
                snapshot=snapshot,
                ingest_run=ingest_run,
                warnings=warnings,
            )
            summary["snapshot_version"] = snapshot.source_snapshot_version
            summary["ingest_run_id"] = ingest_run.ingest_run_id
            summary["next_step"] = "hydrate_fresh_hybrid_snapshot_metadata_v1"
            summary["openalex_enrichment"] = "not_run"
            _update_ingest_run_final(
                conn,
                ingest_run.ingest_run_id,
                status="succeeded",
                counts=summary,
                error_message=None,
            )
        conn.commit()
    except Exception as exc:
        _mark_ingest_failed(conn, ingest_run.ingest_run_id, safe_exception_summary(exc))
        raise
    return summary


def _artifact_from_summary(
    *,
    plan_doc: _HybridPlanDocument,
    policy_metadata: Mapping[str, Any],
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    snapshot_version: str,
    ingest_version: str,
    dry_run: bool,
    summary: Mapping[str, Any],
    affected_row_counts: Mapping[str, int],
) -> dict[str, Any]:
    selected_total = int(_get(plan_doc.payload, "candidate_selection.selected_total") or 0)
    inserted = int(summary.get("inserted_count") or 0)
    updated = int(summary.get("updated_count") or 0)
    skipped = int(summary.get("skipped_existing_count") or 0)
    failed = int(summary.get("failed_count") or 0)
    snapshot_work_count = inserted + updated
    status = "dry_run_validated" if dry_run else "succeeded"
    writes_enabled = not dry_run
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "ingest_version": ingest_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "dry_run": dry_run,
            "caveats": list(CAVEATS),
        },
        "snapshot": {
            "source_snapshot_version": snapshot_version,
            "ingest_mode": DEFAULT_INGEST_MODE,
            "fresh_hybrid_ingest_intent": FRESH_HYBRID_INGEST_INTENT,
            "eval_only": True,
            "fresh_hybrid_validation_candidate_source": True,
            "production_default_changed": False,
        },
        "candidate_plan_summary": {
            "selected_total": selected_total,
            "selected_candidate_work_set_sha256": _get(plan_doc.payload, "candidate_selection.selected_candidate_work_set_sha256"),
            "estimated_confirmatory_eligible_after_old_217_exclusion": _get(
                plan_doc.payload,
                "candidate_selection.estimated_confirmatory_eligible_after_old_217_exclusion",
            ),
            "estimated_overlap_with_old_217": _get(plan_doc.payload, "candidate_selection.estimated_overlap_with_old_217"),
            "negative_or_borderline_count": _get(plan_doc.payload, "bucket_summary.negative_or_borderline_candidate.selected_count"),
            "fresh_surface_policy_version": policy_metadata.get("policy_version"),
        },
        "ingest_result": {
            "status": status,
            "inserted_count": inserted,
            "updated_count": updated,
            "skipped_existing_count": skipped,
            "failed_count": failed,
            "snapshot_work_count": snapshot_work_count,
            "selected_total": selected_total,
            "planned_candidate_count": selected_total,
            "recommended_next_stage": "hydrate_fresh_hybrid_snapshot_metadata_v1",
            "warnings": list(summary.get("warnings") or []),
        },
        "sql_write_report": {
            "writes_enabled": writes_enabled,
            "allowed_tables": list(ALLOWED_WRITE_TABLES) if writes_enabled else [],
            "affected_row_counts": dict(affected_row_counts) if writes_enabled else {},
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "production_tables_modified": False,
        },
        "blocked_actions": [
            "ranking_run",
            "embeddings",
            "hybrid_validation",
            "shadow",
            "production_default",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }


def build_ml_fresh_hybrid_candidate_plan_ingest_payload(
    *,
    fresh_hybrid_corpus_candidate_plan_path: Path,
    fresh_surface_policy_path: Path,
    snapshot_version: str,
    database_url: str | None = None,
    ingest_version: str = INGEST_VERSION,
    dry_run: bool = False,
    repo_root: Path | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(fresh_hybrid_corpus_candidate_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    resolved_snapshot = (snapshot_version or "").strip() or _default_snapshot_version()
    if not resolved_snapshot:
        raise MLFreshHybridCandidatePlanIngestError("snapshot-version must not be blank")
    plan_doc = _load_hybrid_plan_document(plan_path)
    policy_payload = _load_json_object(policy_path)
    policy_metadata = _validate_policy(policy_payload)
    inputs = [
        _input_record("fresh_hybrid_corpus_candidate_plan", plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
    ]
    database_summary = assert_local_database_url(database_url) if database_url else {
        "database_target_redacted": None,
        "local_database_url_confirmed": None,
    }
    if dry_run:
        summary = {
            "inserted_count": 0,
            "updated_count": 0,
            "skipped_existing_count": 0,
            "failed_count": 0,
            "warnings": _metadata_warnings(list(plan_doc.corpus_v2_payload["selected_candidates"])),
        }
        return _artifact_from_summary(
            plan_doc=plan_doc,
            policy_metadata=policy_metadata,
            inputs=inputs,
            database_summary=database_summary,
            snapshot_version=resolved_snapshot,
            ingest_version=ingest_version,
            dry_run=True,
            summary=summary,
            affected_row_counts={},
        )
    if conn is None:
        raise MLFreshHybridCandidatePlanIngestError("internal: conn is required when dry_run is false")
    try:
        summary = _run_ingest_with_conn(conn, plan_doc=plan_doc, snapshot_version=resolved_snapshot)
    except MLFreshHybridCandidatePlanIngestError:
        raise
    except Exception as exc:
        raise MLFreshHybridCandidatePlanIngestError(
            f"fresh hybrid candidate plan ingest failed: {safe_exception_summary(exc)}",
            code=1,
        ) from exc
    return _artifact_from_summary(
        plan_doc=plan_doc,
        policy_metadata=policy_metadata,
        inputs=inputs,
        database_summary=database_summary,
        snapshot_version=resolved_snapshot,
        ingest_version=ingest_version,
        dry_run=False,
        summary=summary,
        affected_row_counts=_affected_row_counts_from_summary(summary),
    )


def _fmt(value: Any) -> str:
    return "n/a" if value is None else str(value)


def markdown_from_ml_fresh_hybrid_candidate_plan_ingest(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    snapshot = payload["snapshot"]
    plan = payload["candidate_plan_summary"]
    result = payload["ingest_result"]
    write_report = payload["sql_write_report"]
    lines = [
        f"# Fresh Hybrid Candidate Plan Ingest ({metadata['ingest_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact records the controlled ingest of the committed fresh hybrid candidate plan into a local eval-only source snapshot. It does not hydrate from OpenAlex, embed, rank, score hybrids, import labels, or authorize shadow/production.",
        "",
        f"- **Dry run:** {metadata['dry_run']}",
        f"- **Status:** `{result['status']}`",
        f"- **Snapshot version:** `{snapshot['source_snapshot_version']}`",
        f"- **Selected total:** {plan['selected_total']}",
        f"- **Snapshot work count:** {result['snapshot_work_count']}",
        f"- **Recommended next stage:** `{result['recommended_next_stage']}`",
        "",
        "## Candidate Plan Summary",
        "",
        f"- Candidate work-set SHA: `{plan['selected_candidate_work_set_sha256']}`",
        f"- Estimated eligible after old-217 exclusion: {plan['estimated_confirmatory_eligible_after_old_217_exclusion']}",
        f"- Estimated old-217 overlap: {plan['estimated_overlap_with_old_217']}",
        f"- Negative/borderline candidates: {plan['negative_or_borderline_count']}",
        "",
        "## DB Write Scope",
        "",
        f"- Writes enabled: {write_report['writes_enabled']}",
        f"- Allowed tables: {', '.join(write_report['allowed_tables']) if write_report['allowed_tables'] else 'none'}",
        f"- ranking_runs written: {write_report['ranking_runs_written']}",
        f"- paper_scores written: {write_report['paper_scores_written']}",
        f"- production tables modified: {write_report['production_tables_modified']}",
        "",
        "## Counts",
        "",
        f"- Inserted: {result['inserted_count']}",
        f"- Updated: {result['updated_count']}",
        f"- Skipped existing/duplicates: {result['skipped_existing_count']}",
        f"- Failed: {result['failed_count']}",
        "",
        "## Next Stage",
        "",
        result["recommended_next_stage"],
        "",
        "## Not Ranking / Not Embeddings / Not Shadow / Not Production",
        "",
    ]
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_hybrid_candidate_plan_ingest(
    *,
    fresh_hybrid_corpus_candidate_plan_path: Path,
    fresh_surface_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    snapshot_version: str | None = None,
    database_url: str | None = None,
    ingest_version: str = INGEST_VERSION,
    dry_run: bool = False,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    resolved_snapshot = (snapshot_version or "").strip() or _default_snapshot_version()
    dsn = database_url or _database_url_from_env()
    assert_local_database_url(dsn)
    if dry_run:
        payload = build_ml_fresh_hybrid_candidate_plan_ingest_payload(
            fresh_hybrid_corpus_candidate_plan_path=fresh_hybrid_corpus_candidate_plan_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            snapshot_version=resolved_snapshot,
            database_url=dsn,
            ingest_version=ingest_version,
            dry_run=True,
            repo_root=repo_root,
        )
    else:
        with psycopg.connect(dsn, autocommit=False) as conn:
            payload = build_ml_fresh_hybrid_candidate_plan_ingest_payload(
                fresh_hybrid_corpus_candidate_plan_path=fresh_hybrid_corpus_candidate_plan_path,
                fresh_surface_policy_path=fresh_surface_policy_path,
                snapshot_version=resolved_snapshot,
                database_url=dsn,
                ingest_version=ingest_version,
                dry_run=False,
                repo_root=repo_root,
                conn=conn,
            )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_hybrid_candidate_plan_ingest(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "INGEST_VERSION",
    "MLFreshHybridCandidatePlanIngestError",
    "assert_local_database_url",
    "build_ml_fresh_hybrid_candidate_plan_ingest_payload",
    "markdown_from_ml_fresh_hybrid_candidate_plan_ingest",
    "write_ml_fresh_hybrid_candidate_plan_ingest",
]
