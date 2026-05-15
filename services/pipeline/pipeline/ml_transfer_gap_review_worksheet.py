"""Transfer-gap reviewer worksheet.

Builds a reviewer-blank CSV and row_id-keyed sidecar for targeted transfer and
sparse-pool label collection. This command does not train models, run ranking,
ingest labels, create splits, or change product behavior.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.ml_blind_snapshot_review_worksheet import (
    ABSTRACT_PREVIEW_MAX_CHARS,
    BlindCandidate,
    assert_succeeded_clustering_run,
    assert_succeeded_ranking_run,
    fetch_candidate_pool,
    fetch_ranking_context,
    raw_pool_to_candidates,
    _truncate_abstract,
)
from pipeline.ml_external_near_miss_review_worksheet import (
    DEFAULT_CANDIDATE_PLAN_PATH,
    EXTERNAL_CLUSTER_SENTINEL,
    FetchOpenAlexJson,
    fetch_external_candidates,
    load_snapshot_exclusion_tokens,
)
from pipeline.ml_label_dataset import LABEL_FIELDS, row_has_explicit_label, sha256_file
from pipeline.openalex_client import (
    compute_contact_provenance,
    compute_openalex_auth_artifact_fields,
    fetch_openalex_json,
)
from pipeline.openalex_ids import normalize_w_token
from pipeline.repo_paths import default_repo_root, portable_repo_path

WORKSHEET_VERSION = "ml-transfer-gap-review-v1"
REVIEW_POOL_VARIANT = "ml_transfer_gap_audit"
DEFAULT_SAMPLE_SEED = 20260515

ALLOWED_SAMPLE_REASONS: tuple[str, ...] = (
    "transfer_gap_external_blind_balance",
    "transfer_gap_good_or_acceptable_balance",
    "transfer_gap_sparse_pool_negative",
    "transfer_gap_rank_shaped_boundary",
    "fallback_seeded_fill",
)

CSV_COLUMNS: tuple[str, ...] = (
    "row_id",
    "worksheet_version",
    "review_pool_variant",
    "paper_id",
    "openalex_work_id",
    "work_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "abstract_preview",
    "sample_reason",
    "cluster_id",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)

HIDDEN_REVIEWER_CSV_FIELDS: tuple[str, ...] = (
    "ranking_run_id",
    "ranking_run_id_context",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
    "internal_work_id",
    "final_score",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "diversity_penalty",
    "bridge_score",
    "ranking_context_family_scores_json",
    "ranking_context_family_ranks_json",
    "learned_logit",
    "model_prediction",
    "gap_source_pool",
)

CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation.",
    "Rows are for targeted transfer-gap manual labeling only.",
    "No model is trained, no ranking is run, and no production ranking change is supported.",
    "The reviewer CSV intentionally hides score, rank, model, ranking-run, internal database, and gap-source-pool fields.",
    "surprising_or_useful is deferred for production; good_or_acceptable is research-only.",
)

RANK_SHAPED_GAP_POOLS = {
    "bridge_eligible_only",
    "full_family_top_k",
    "ml_contrastive_offline_audit",
    "ml_emerging_target_gap_audit:good_or_acceptable",
}


class MLTransferGapReviewWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class TransferGapCandidate:
    paper_id: str
    work_token: str
    title: str
    year: int | None
    citation_count: int
    source_slug: str
    topics: tuple[str, ...]
    abstract: str
    sample_reason: str
    cluster_id: str
    gap_priority: str
    target_hint: str
    acquisition_channel: str
    gap_source_pool: str | None = None
    source_query: str | None = None
    ranking_context: dict[str, Any] | None = None
    exclusion_checks_passed: dict[str, Any] | None = None
    v7_label_snapshot_if_same_work: tuple[dict[str, Any], ...] = ()


def stable_row_id(*, worksheet_version: str, sample_seed: int, paper_id: str) -> str:
    raw = f"{worksheet_version}|{sample_seed}|{paper_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLTransferGapReviewWorksheetError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLTransferGapReviewWorksheetError(f"Expected JSON object in {path}")
    return payload


def _validate_plan(payload: Mapping[str, Any]) -> None:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLTransferGapReviewWorksheetError("production-readiness plan missing metadata object")
    if metadata.get("artifact_type") != "ml_production_readiness_plan":
        raise MLTransferGapReviewWorksheetError(
            f"expected plan metadata.artifact_type='ml_production_readiness_plan', got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != "ml-production-readiness-plan-v1":
        raise MLTransferGapReviewWorksheetError(
            f"expected plan metadata.plan_version='ml-production-readiness-plan-v1', got {metadata.get('plan_version')!r}"
        )
    if metadata.get("plan_schema_version") != 1:
        raise MLTransferGapReviewWorksheetError(
            f"expected plan metadata.plan_schema_version=1, got {metadata.get('plan_schema_version')!r}"
        )
    status = str(metadata.get("overall_status") or "")
    if status == "blocked":
        raise MLTransferGapReviewWorksheetError("production-readiness plan is blocked; transfer-gap worksheet is not allowed")
    if status not in {"research_only", "ready_for_offline_gate_experiment"}:
        raise MLTransferGapReviewWorksheetError(f"unsupported production-readiness overall_status: {status!r}")


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != "ml-label-dataset-v7":
        raise MLTransferGapReviewWorksheetError(
            f"expected label dataset_version='ml-label-dataset-v7', got {payload.get('dataset_version')!r}"
        )
    if not isinstance(payload.get("rows"), list):
        raise MLTransferGapReviewWorksheetError("label dataset missing rows array")


def _work_tokens_from_row(row: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("work_id", "openalex_work_id", "paper_id"):
        token = normalize_w_token(str(row.get(key) or ""))
        if token:
            out.add(token)
    return out


def explicit_label_exclusion_sets(label_payload: Mapping[str, Any]) -> tuple[set[str], set[str], dict[str, list[dict[str, Any]]]]:
    rows = label_payload.get("rows")
    if not isinstance(rows, list):
        return set(), set(), {}
    row_ids: set[str] = set()
    tokens: set[str] = set()
    snapshots: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        row_tokens = _work_tokens_from_row(row)
        for token in row_tokens:
            snapshots.setdefault(token, []).append(
                {
                    "row_id": row.get("row_id"),
                    "review_pool_variant": row.get("review_pool_variant"),
                    "source_worksheet_path": row.get("source_worksheet_path"),
                    "relevance_label": row.get("relevance_label"),
                    "novelty_label": row.get("novelty_label"),
                    "bridge_like_label": row.get("bridge_like_label"),
                }
            )
        if not row_has_explicit_label(dict(row)):
            continue
        rid = str(row.get("row_id") or "").strip()
        if rid:
            row_ids.add(rid)
        tokens.update(row_tokens)
    return row_ids, tokens, snapshots


def compute_slot_budget(rows: int) -> dict[str, int]:
    if rows < 1 or rows > 120:
        raise MLTransferGapReviewWorksheetError("--rows must be between 1 and 120")
    p1 = max(1, round(rows * 0.35))
    p2 = max(1, round(rows * 0.40))
    p3 = rows - p1 - p2
    if rows >= 3 and p3 < 1:
        while p3 < 1 and p2 > 1:
            p2 -= 1
            p3 = rows - p1 - p2
        p3 = max(0, rows - p1 - p2)
    return {"P1": p1, "P2": p2, "P3": p3}


def _candidate_plan_path(path: Path | None) -> Path:
    return path if path is not None else default_repo_root() / DEFAULT_CANDIDATE_PLAN_PATH


def _mock_openalex_fetch(url: str) -> dict[str, Any]:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    base = int(digest[:8], 16)
    results = []
    for i in range(8):
        token = f"W9{(base + i) % 10_000_000:07d}"
        results.append(
            {
                "id": f"https://openalex.org/{token}",
                "title": f"Mock transfer gap candidate {token}",
                "publication_year": 2024 + (i % 2),
                "publication_date": f"202{4 + (i % 2)}-01-01",
                "cited_by_count": i,
                "type": "article",
                "language": "en",
                "primary_location": {"source": {"display_name": "Mock OpenAlex Source", "id": "https://openalex.org/S1"}},
                "topics": [{"display_name": "Music recommendation"}, {"display_name": "Audio systems"}],
                "abstract": f"Mock abstract for {token} about music recommendation, transfer gaps, and audio relevance.",
            }
        )
    return {"meta": {"count": len(results)}, "results": results}


def _external_sort_key(cand: Any) -> tuple[str, str, str]:
    return (str(cand.work_token), str(cand.title).casefold(), str(cand.paper_id))


def _external_transfer_candidates(
    *,
    plan_payload: Mapping[str, Any],
    label_payload: Mapping[str, Any],
    label_tokens: set[str],
    corpus_snapshot_version: str,
    candidate_plan_path: Path | None,
    p1_slots: int,
    p2_slots: int,
    seed: int,
    fetch_json: FetchOpenAlexJson,
    retrieved_at: str,
) -> tuple[list[TransferGapCandidate], dict[str, Any]]:
    snapshot_tokens, snapshot_source = load_snapshot_exclusion_tokens(
        corpus_snapshot_version=corpus_snapshot_version,
        candidate_plan_path=_candidate_plan_path(candidate_plan_path),
    )
    raw_candidates, query_debug = fetch_external_candidates(fetch_json=fetch_json, retrieved_at=retrieved_at)
    seen: set[str] = set()
    filtered = []
    excluded = Counter()
    for cand in raw_candidates:
        token = cand.work_token.upper()
        if token in seen:
            excluded["duplicate_candidate"] += 1
            continue
        seen.add(token)
        if token in snapshot_tokens:
            excluded["source_snapshot_exclusion"] += 1
            continue
        if token in label_tokens:
            excluded["v7_labeled_exclusion"] += 1
            continue
        filtered.append(cand)
    filtered.sort(key=_external_sort_key)

    selected: list[TransferGapCandidate] = []
    used: set[str] = set()

    def take(priority: str, target_hint: str, sample_reason: str, slots: int) -> int:
        count = 0
        for cand in filtered:
            if count >= slots:
                break
            token = cand.work_token.upper()
            if token in used:
                continue
            used.add(token)
            count += 1
            selected.append(
                TransferGapCandidate(
                    paper_id=cand.paper_id,
                    work_token=cand.work_token,
                    title=cand.title,
                    year=cand.year,
                    citation_count=cand.citation_count,
                    source_slug=cand.source_slug,
                    topics=tuple(cand.topics),
                    abstract=cand.abstract,
                    sample_reason=sample_reason,
                    cluster_id=EXTERNAL_CLUSTER_SENTINEL,
                    gap_priority=priority,
                    target_hint=target_hint,
                    acquisition_channel="openalex_external",
                    source_query=cand.source_query,
                    exclusion_checks_passed={
                        "outside_source_snapshot_217": True,
                        "not_v7_explicitly_labeled": True,
                    },
                )
            )
        return count

    p1 = take("P1", "surprising_or_useful", "transfer_gap_external_blind_balance", p1_slots)
    p2 = take("P2", "good_or_acceptable", "transfer_gap_good_or_acceptable_balance", p2_slots)
    debug = {
        "snapshot_source": snapshot_source,
        "source_snapshot_exclusion_count": len(snapshot_tokens),
        "raw_candidate_count": len(raw_candidates),
        "filtered_external_candidate_count": len(filtered),
        "excluded_counts": dict(sorted(excluded.items())),
        "query_debug": query_debug,
        "achieved_slots": {"P1": p1, "P2": p2},
        "shortfall_slots": {"P1": max(0, p1_slots - p1), "P2": max(0, p2_slots - p2)},
        "plan_next_artifacts_has_transfer_gap": any(
            isinstance(item, Mapping) and item.get("name") == "ml-transfer-gap-review-worksheet"
            for item in plan_payload.get("next_artifacts", [])
            if isinstance(plan_payload.get("next_artifacts"), list)
        ),
    }
    return selected, debug


def _p3_gaps(plan_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    gaps = plan_payload.get("label_gaps")
    if not isinstance(gaps, list):
        return []
    out = [dict(gap) for gap in gaps if isinstance(gap, Mapping) and str(gap.get("priority")) == "P3"]
    return out


def _db_url_available(database_url: str | None) -> bool:
    if database_url:
        return True
    if os.environ.get("DATABASE_URL"):
        return True
    return any(os.environ.get(k) for k in ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE"))


def _mock_db_pool() -> list[BlindCandidate]:
    out = []
    for i in range(30):
        token = f"W8{i:07d}"
        out.append(
            BlindCandidate(
                internal_work_id=80_000 + i,
                paper_id=f"https://openalex.org/{token}",
                work_token=token,
                title=f"Mock in-snapshot boundary candidate {i}",
                year=2024,
                citation_count=i,
                source_slug="mock_snapshot",
                work_type="article",
                cluster_id=f"c{i % 4}",
                topics=("Music Information Retrieval", "Recommendation") if i % 2 else ("Audio", "Machine Listening"),
                abstract=f"Mock in-snapshot abstract {i} for sparse pool boundary labeling.",
                family_scores={"emerging": 0.1 + i / 1000},
                family_ranks={"emerging": i + 1},
            )
        )
    return out


def _fetch_db_candidates(
    *,
    database_url: str | None,
    mock_db: bool,
    corpus_snapshot_version: str,
    embedding_version: str,
    cluster_version: str,
    ranking_run_id: str,
) -> tuple[list[BlindCandidate], dict[str, Any]]:
    if mock_db:
        return _mock_db_pool(), {"db_access": "mock-db", "queries": []}
    if not _db_url_available(database_url):
        return [], {"db_access": "skipped; no database-url or DATABASE_URL/PG environment configured", "queries": []}
    dsn = database_url or database_url_from_env()
    queries: list[str] = []
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        assert_succeeded_clustering_run(
            conn,
            cluster_version=cluster_version,
            expected_corpus_snapshot_version=corpus_snapshot_version,
            expected_embedding_version=embedding_version,
        )
        assert_succeeded_ranking_run(
            conn,
            ranking_run_id=ranking_run_id,
            expected_corpus_snapshot_version=corpus_snapshot_version,
            expected_embedding_version=embedding_version,
        )
        raw_rows = fetch_candidate_pool(
            conn,
            corpus_snapshot_version=corpus_snapshot_version,
            cluster_version=cluster_version,
        )
        ranking_context = fetch_ranking_context(conn, ranking_run_id=ranking_run_id)
    return raw_pool_to_candidates(raw_rows, ranking_context=ranking_context), {"db_access": "read-only postgres", "queries": queries}


def _p3_sample_reason(pool: str) -> str:
    return "transfer_gap_rank_shaped_boundary" if pool in RANK_SHAPED_GAP_POOLS else "transfer_gap_sparse_pool_negative"


def _p3_target_hint(target: Any) -> str:
    text = str(target or "").strip()
    return text if text in {"good_or_acceptable", "surprising_or_useful"} else "either"


def _in_snapshot_transfer_candidates(
    *,
    plan_payload: Mapping[str, Any],
    label_tokens: set[str],
    label_snapshots: dict[str, list[dict[str, Any]]],
    p3_slots: int,
    seed: int,
    database_url: str | None,
    mock_db: bool,
    corpus_snapshot_version: str,
    embedding_version: str | None,
    cluster_version: str | None,
    ranking_run_id: str | None,
) -> tuple[list[TransferGapCandidate], dict[str, Any]]:
    if p3_slots <= 0:
        return [], {"db_access": "skipped; P3_slots is 0", "achieved_slots": {"P3": 0}, "shortfall_slots": {"P3": 0}}
    required_missing = [name for name, value in (("ranking_run_id", ranking_run_id), ("embedding_version", embedding_version), ("cluster_version", cluster_version)) if not value]
    if required_missing and not mock_db:
        return [], {
            "db_access": f"skipped; missing required DB flags: {', '.join(required_missing)}",
            "achieved_slots": {"P3": 0},
            "shortfall_slots": {"P3": p3_slots},
        }
    pool, db_debug = _fetch_db_candidates(
        database_url=database_url,
        mock_db=mock_db,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version or "",
        cluster_version=cluster_version or "",
        ranking_run_id=ranking_run_id or "",
    )
    gaps = _p3_gaps(plan_payload)
    if not gaps:
        gaps = [{"target": "either", "pool": "ml_hard_negative_audit"}]
    eligible = [cand for cand in pool if cand.work_token.upper() not in label_tokens]
    eligible.sort(key=lambda c: (c.work_token, c.title.casefold(), c.paper_id))
    selected: list[TransferGapCandidate] = []
    used: set[str] = set()
    gap_idx = 0
    for cand in eligible:
        if len(selected) >= p3_slots:
            break
        token = cand.work_token.upper()
        if token in used:
            continue
        gap = gaps[gap_idx % len(gaps)]
        gap_idx += 1
        source_pool = str(gap.get("pool") or "(null)")
        used.add(token)
        selected.append(
            TransferGapCandidate(
                paper_id=cand.paper_id,
                work_token=cand.work_token,
                title=cand.title,
                year=cand.year,
                citation_count=cand.citation_count,
                source_slug=cand.source_slug,
                topics=tuple(cand.topics),
                abstract=cand.abstract,
                sample_reason=_p3_sample_reason(source_pool),
                cluster_id=cand.cluster_id,
                gap_priority="P3",
                target_hint=_p3_target_hint(gap.get("target")),
                acquisition_channel="postgres_in_snapshot",
                gap_source_pool=source_pool,
                source_query=None,
                ranking_context={
                    "ranking_run_id": ranking_run_id,
                    "corpus_snapshot_version": corpus_snapshot_version,
                    "embedding_version": embedding_version,
                    "cluster_version": cluster_version,
                    "family_scores": cand.family_scores,
                    "family_ranks": cand.family_ranks,
                    "internal_work_id": cand.internal_work_id,
                },
                exclusion_checks_passed={"not_v7_explicitly_labeled": True, "in_snapshot_channel": True},
                v7_label_snapshot_if_same_work=tuple(label_snapshots.get(token, [])),
            )
        )
    debug = dict(db_debug)
    debug.update(
        {
            "raw_in_snapshot_candidate_count": len(pool),
            "eligible_in_snapshot_candidate_count": len(eligible),
            "gap_source_pools": [str(g.get("pool") or "(null)") for g in gaps],
            "achieved_slots": {"P3": len(selected)},
            "shortfall_slots": {"P3": max(0, p3_slots - len(selected))},
        }
    )
    return selected, debug


def _csv_row(cand: TransferGapCandidate, *, seed: int) -> dict[str, str]:
    return {
        "row_id": stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        "worksheet_version": WORKSHEET_VERSION,
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "work_id": cand.work_token,
        "title": cand.title,
        "year": str(cand.year) if cand.year is not None else "",
        "citation_count": str(cand.citation_count),
        "source_slug": cand.source_slug,
        "topics": ";".join(cand.topics),
        "abstract_preview": _truncate_abstract(cand.abstract, ABSTRACT_PREVIEW_MAX_CHARS) if cand.abstract else "",
        "sample_reason": cand.sample_reason,
        "cluster_id": cand.cluster_id,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def _sidecar_row(cand: TransferGapCandidate, *, seed: int) -> dict[str, Any]:
    return {
        "row_id": stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "work_id": cand.work_token,
        "gap_priority": cand.gap_priority,
        "target_hint": cand.target_hint,
        "acquisition_channel": cand.acquisition_channel,
        "sample_reason": cand.sample_reason,
        "gap_source_pool": cand.gap_source_pool,
        "source_query": cand.source_query,
        "ranking_context": cand.ranking_context,
        "exclusion_checks_passed": cand.exclusion_checks_passed or {},
        "v7_label_snapshot_if_same_work": list(cand.v7_label_snapshot_if_same_work),
    }


def _dedupe_and_sort(candidates: Sequence[TransferGapCandidate], *, seed: int) -> list[TransferGapCandidate]:
    priority_order = {"P1": 0, "P2": 1, "P3": 2}
    channel_order = {"openalex_external": 0, "postgres_in_snapshot": 1}
    best: dict[str, TransferGapCandidate] = {}
    for cand in candidates:
        token = cand.work_token.upper()
        current = best.get(token)
        if current is None:
            best[token] = cand
            continue
        cand_key = (
            priority_order.get(cand.gap_priority, 9),
            channel_order.get(cand.acquisition_channel, 9),
            stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        )
        current_key = (
            priority_order.get(current.gap_priority, 9),
            channel_order.get(current.acquisition_channel, 9),
            stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=current.paper_id),
        )
        if cand_key < current_key:
            best[token] = cand
    return sorted(
        best.values(),
        key=lambda cand: stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
    )


def render_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    return buf.getvalue()


def _input_record(name: str, path: Path) -> dict[str, str]:
    return {"name": name, "path": portable_repo_path(path), "sha256": sha256_file(path)}


def build_context_payload(
    *,
    rows: Sequence[dict[str, Any]],
    production_readiness_plan_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    seed: int,
    requested_rows: int,
    requested_slots: dict[str, int],
    achieved_slots: dict[str, int],
    exclusion_counts: dict[str, Any],
    shortfall_counts: dict[str, int],
    reallocation_events: list[dict[str, Any]],
    openalex_provenance: dict[str, Any],
    db_provenance: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_type": "ml_transfer_gap_review_v1_context",
        "generated_at": _now_iso_z(),
        "provenance": {
            "worksheet_version": WORKSHEET_VERSION,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "sample_seed": seed,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "inputs": [
                _input_record("production_readiness_plan", production_readiness_plan_path),
                _input_record("label_dataset", label_dataset_path),
                _input_record("conflict_policy", conflict_policy_path),
            ],
            "requested_rows": requested_rows,
            "achieved_rows": len(rows),
            "requested_slots": requested_slots,
            "achieved_slots": achieved_slots,
            "shortfall_counts": shortfall_counts,
            "exclusion_counts": exclusion_counts,
            "reallocation_events": reallocation_events,
            "openalex": openalex_provenance,
            "db": db_provenance,
        },
        "schema": {
            "key": "row_id",
            "hidden_from_reviewer_csv": list(HIDDEN_REVIEWER_CSV_FIELDS),
            "reviewer_csv_columns": list(CSV_COLUMNS),
        },
        "rows": list(rows),
    }


def render_markdown(
    *,
    context: Mapping[str, Any],
    plan_payload: Mapping[str, Any],
    output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> str:
    provenance = context["provenance"]
    sidecar_rows = context["rows"]
    by_reason = Counter(row["sample_reason"] for row in sidecar_rows)
    by_priority = Counter(row["gap_priority"] for row in sidecar_rows)
    shortfall = provenance["shortfall_counts"]
    has_next = bool(provenance["openalex"].get("plan_next_artifacts_has_transfer_gap", False))
    lines = [
        f"# Transfer gap review worksheet (`{WORKSHEET_VERSION}`)",
        "",
        "## Purpose",
        "",
        "Reviewer-blank worksheet for transfer and sparse-pool label gaps identified by `ml-production-readiness-plan-v1`. "
        "This is label collection infrastructure only: no training, no ranking, no split generation, and no production behavior change.",
        "",
        "## Quotas",
        "",
        f"- **requested rows:** `{provenance['requested_rows']}`",
        f"- **achieved rows:** `{provenance['achieved_rows']}`",
        f"- **requested slots:** `{json.dumps(provenance['requested_slots'], sort_keys=True)}`",
        f"- **achieved slots:** `{json.dumps(provenance['achieved_slots'], sort_keys=True)}`",
        f"- **shortfall counts:** `{json.dumps(shortfall, sort_keys=True)}`",
        f"- **csv_output:** `{portable_repo_path(output_path)}`",
        f"- **context_output:** `{portable_repo_path(context_output_path)}`",
        f"- **markdown_output:** `{portable_repo_path(markdown_output_path)}`",
    ]
    if not has_next:
        lines.append("- **plan warning:** `ml-transfer-gap-review-worksheet` was not listed in plan.next_artifacts.")
    lines.extend(
        [
            "",
            "## Breakdown By Priority",
            "",
            "| priority | rows |",
            "| --- | ---: |",
            *[f"| `{p}` | {by_priority[p]} |" for p in ("P1", "P2", "P3") if by_priority[p]],
            "",
            "## Breakdown By Sample Reason",
            "",
            "| sample_reason | rows |",
            "| --- | ---: |",
            *[f"| `{reason}` | {by_reason[reason]} |" for reason in ALLOWED_SAMPLE_REASONS if by_reason[reason]],
            "",
            "## Rubric Reminders",
            "",
            "- `surprising_or_useful` is deferred for production and needs rubric clarity plus balanced cross-source labels.",
            "- `good_or_acceptable` is research-only and may support future offline ranker research only after gates are addressed.",
            "- P3 rows record `gap_source_pool` only in the sidecar, not in the reviewer CSV.",
            "",
            "## Later Ingest Note",
            "",
            "When reviewed, save a dated labeled copy such as `ml_transfer_gap_review_v1_labeled_YYYY-MM-DD.csv`. "
            "A future v8 ingest should merge this sidecar by `row_id` and keep `review_pool_variant=ml_transfer_gap_audit` distinct.",
            "",
            "## Caveats",
            "",
            *[f"- {caveat}" for caveat in CAVEATS],
            "",
        ]
    )
    return "\n".join(lines)


def build_transfer_gap_review_worksheet(
    *,
    production_readiness_plan_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    rows: int,
    seed: int,
    source_snapshot_candidate_plan_path: Path | None,
    corpus_snapshot_version: str,
    mailto: str | None,
    mock_openalex: bool,
    ranking_run_id: str | None,
    embedding_version: str | None,
    cluster_version: str | None,
    database_url: str | None,
    mock_db: bool,
    fetch_json: FetchOpenAlexJson | None = None,
    db_candidates: Sequence[BlindCandidate] | None = None,
    retrieved_at: str | None = None,
) -> tuple[str, dict[str, Any], str, dict[str, Any]]:
    if not production_readiness_plan_path.is_file():
        raise MLTransferGapReviewWorksheetError(f"production-readiness plan not found: {production_readiness_plan_path}")
    if not label_dataset_path.is_file():
        raise MLTransferGapReviewWorksheetError(f"label dataset not found: {label_dataset_path}")
    if not conflict_policy_path.is_file():
        raise MLTransferGapReviewWorksheetError(f"conflict policy not found: {conflict_policy_path}")

    plan_payload = _load_json_object(production_readiness_plan_path)
    label_payload = _load_json_object(label_dataset_path)
    _validate_plan(plan_payload)
    _validate_label_dataset(label_payload)
    label_row_ids, label_tokens, label_snapshots = explicit_label_exclusion_sets(label_payload)
    requested_slots = compute_slot_budget(rows)
    retrieved = retrieved_at or _now_iso_z()
    if fetch_json is None:
        if mock_openalex:
            fetch_json = _mock_openalex_fetch
        else:
            fetch_json = lambda url: fetch_openalex_json(url, mailto=mailto, timeout_sec=60.0)

    external_candidates, external_debug = _external_transfer_candidates(
        plan_payload=plan_payload,
        label_payload=label_payload,
        label_tokens=label_tokens,
        corpus_snapshot_version=corpus_snapshot_version,
        candidate_plan_path=source_snapshot_candidate_plan_path,
        p1_slots=requested_slots["P1"],
        p2_slots=requested_slots["P2"],
        seed=seed,
        fetch_json=fetch_json,
        retrieved_at=retrieved,
    )

    if db_candidates is not None:
        pool, db_base_debug = list(db_candidates), {"db_access": "mock-db injected candidates", "queries": []}
        p3_candidates: list[TransferGapCandidate] = []
        if requested_slots["P3"] > 0:
            eligible = [cand for cand in pool if cand.work_token.upper() not in label_tokens]
            eligible.sort(key=lambda c: (c.work_token, c.title.casefold(), c.paper_id))
            gaps = _p3_gaps(plan_payload) or [{"target": "either", "pool": "ml_hard_negative_audit"}]
            for idx, cand in enumerate(eligible[: requested_slots["P3"]]):
                gap = gaps[idx % len(gaps)]
                source_pool = str(gap.get("pool") or "(null)")
                p3_candidates.append(
                    TransferGapCandidate(
                        paper_id=cand.paper_id,
                        work_token=cand.work_token,
                        title=cand.title,
                        year=cand.year,
                        citation_count=cand.citation_count,
                        source_slug=cand.source_slug,
                        topics=tuple(cand.topics),
                        abstract=cand.abstract,
                        sample_reason=_p3_sample_reason(source_pool),
                        cluster_id=cand.cluster_id,
                        gap_priority="P3",
                        target_hint=_p3_target_hint(gap.get("target")),
                        acquisition_channel="postgres_in_snapshot",
                        gap_source_pool=source_pool,
                        ranking_context={"ranking_run_id": ranking_run_id, "family_scores": cand.family_scores, "family_ranks": cand.family_ranks, "internal_work_id": cand.internal_work_id},
                        exclusion_checks_passed={"not_v7_explicitly_labeled": True, "in_snapshot_channel": True},
                        v7_label_snapshot_if_same_work=tuple(label_snapshots.get(cand.work_token.upper(), [])),
                    )
                )
        else:
            eligible = []
        db_debug = {
            **db_base_debug,
            "raw_in_snapshot_candidate_count": len(pool),
            "eligible_in_snapshot_candidate_count": len(eligible),
            "achieved_slots": {"P3": len(p3_candidates)},
            "shortfall_slots": {"P3": max(0, requested_slots["P3"] - len(p3_candidates))},
        }
    else:
        p3_candidates, db_debug = _in_snapshot_transfer_candidates(
            plan_payload=plan_payload,
            label_tokens=label_tokens,
            label_snapshots=label_snapshots,
            p3_slots=requested_slots["P3"],
            seed=seed,
            database_url=database_url,
            mock_db=mock_db,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            cluster_version=cluster_version,
            ranking_run_id=ranking_run_id,
        )

    selected = _dedupe_and_sort([*external_candidates, *p3_candidates], seed=seed)
    csv_rows = [_csv_row(cand, seed=seed) for cand in selected]
    sidecar_rows = [_sidecar_row(cand, seed=seed) for cand in selected]
    if {row["row_id"] for row in csv_rows} != {row["row_id"] for row in sidecar_rows}:
        raise MLTransferGapReviewWorksheetError("internal error: CSV and sidecar row_id sets differ")

    achieved_slots = {
        "P1": sum(1 for row in sidecar_rows if row["gap_priority"] == "P1"),
        "P2": sum(1 for row in sidecar_rows if row["gap_priority"] == "P2"),
        "P3": sum(1 for row in sidecar_rows if row["gap_priority"] == "P3"),
    }
    shortfall_counts = {key: max(0, requested_slots[key] - achieved_slots[key]) for key in requested_slots}
    contact_mode, contact_provided = compute_contact_provenance(mailto_cli=mailto or "", mock_openalex=mock_openalex)
    api_key_provided, auth_mode = compute_openalex_auth_artifact_fields(mock_openalex=mock_openalex)
    context = build_context_payload(
        rows=sidecar_rows,
        production_readiness_plan_path=production_readiness_plan_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        seed=seed,
        requested_rows=rows,
        requested_slots=requested_slots,
        achieved_slots=achieved_slots,
        exclusion_counts={
            "explicit_label_row_ids": len(label_row_ids),
            "explicit_label_work_tokens": len(label_tokens),
            **external_debug.get("excluded_counts", {}),
        },
        shortfall_counts=shortfall_counts,
        reallocation_events=[],
        openalex_provenance={
            "mock_openalex": mock_openalex,
            "contact_mode": contact_mode,
            "contact_provided": contact_provided,
            "api_key_provided": api_key_provided,
            "auth_mode": auth_mode,
            "source_snapshot_exclusion_source": external_debug.get("snapshot_source"),
            "source_snapshot_exclusion_count": external_debug.get("source_snapshot_exclusion_count"),
            "query_debug": external_debug.get("query_debug"),
            "plan_next_artifacts_has_transfer_gap": external_debug.get("plan_next_artifacts_has_transfer_gap"),
        },
        db_provenance={
            "ranking_run_id": ranking_run_id,
            "corpus_snapshot_version": corpus_snapshot_version,
            "embedding_version": embedding_version,
            "cluster_version": cluster_version,
            **db_debug,
        },
    )
    csv_text = render_csv(csv_rows)
    md_text = render_markdown(
        context=context,
        plan_payload=plan_payload,
        output_path=output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
    )
    debug = {
        "requested_slots": requested_slots,
        "achieved_slots": achieved_slots,
        "shortfall_counts": shortfall_counts,
        "external_debug": external_debug,
        "db_debug": db_debug,
        "row_count": len(selected),
    }
    return csv_text, context, md_text, debug


def run_ml_transfer_gap_review_worksheet_cli(
    *,
    production_readiness_plan_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    rows: int,
    seed: int,
    source_snapshot_candidate_plan_path: Path | None,
    corpus_snapshot_version: str,
    mailto: str | None,
    mock_openalex: bool,
    ranking_run_id: str | None,
    embedding_version: str | None,
    cluster_version: str | None,
    database_url: str | None,
    mock_db: bool,
) -> dict[str, Any]:
    csv_text, context, md_text, debug = build_transfer_gap_review_worksheet(
        production_readiness_plan_path=production_readiness_plan_path,
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        output_path=output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
        rows=rows,
        seed=seed,
        source_snapshot_candidate_plan_path=source_snapshot_candidate_plan_path,
        corpus_snapshot_version=corpus_snapshot_version,
        mailto=mailto,
        mock_openalex=mock_openalex,
        ranking_run_id=ranking_run_id,
        embedding_version=embedding_version,
        cluster_version=cluster_version,
        database_url=database_url,
        mock_db=mock_db,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    context_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(csv_text, encoding="utf-8", newline="")
    context_output_path.write_text(json.dumps(context, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    markdown_output_path.write_text(md_text, encoding="utf-8", newline="")
    return debug


__all__ = [
    "ALLOWED_SAMPLE_REASONS",
    "CSV_COLUMNS",
    "DEFAULT_SAMPLE_SEED",
    "HIDDEN_REVIEWER_CSV_FIELDS",
    "MLTransferGapReviewWorksheetError",
    "REVIEW_POOL_VARIANT",
    "WORKSHEET_VERSION",
    "build_transfer_gap_review_worksheet",
    "compute_slot_budget",
    "explicit_label_exclusion_sets",
    "render_csv",
    "run_ml_transfer_gap_review_worksheet_cli",
    "stable_row_id",
]
