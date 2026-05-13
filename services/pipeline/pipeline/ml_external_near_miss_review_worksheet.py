"""External near-miss reviewer-blind worksheet.

Collects plausible music/audio/recommender boundary examples from OpenAlex that
are outside the committed 217-work corpus snapshot and absent from existing v6
manual labels. The reviewer CSV deliberately omits score/rank/model fields; the
sidecar keeps acquisition and exclusion provenance keyed by row_id.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from pipeline.ml_blind_snapshot_review_worksheet import (
    ABSTRACT_PREVIEW_MAX_CHARS,
    MAX_ROWS,
    MIN_ROWS,
    _truncate_abstract,
)
from pipeline.ml_label_dataset import LABEL_FIELDS, paper_id_to_work_id, sha256_file
from pipeline.openalex import OPENALEX_WORKS_URL, build_work_select_clause
from pipeline.openalex_client import (
    compute_contact_provenance,
    compute_openalex_auth_artifact_fields,
    fetch_openalex_json,
)
from pipeline.openalex_text import abstract_plain_text, clean_openalex_text
from pipeline.repo_paths import default_repo_root, portable_repo_path

WORKSHEET_VERSION = "ml-external-near-miss-review-v1"
REVIEW_POOL_VARIANT = "ml_external_near_miss_audit"
DEFAULT_SAMPLE_SEED = 20260514
DEFAULT_CORPUS_SNAPSHOT_VERSION = "source-snapshot-v2-candidate-plan-20260428"
DEFAULT_CANDIDATE_PLAN_PATH = Path("docs/audit/corpus-v2-candidate-plan-20260428.json")
EXTERNAL_CLUSTER_SENTINEL = "ext"

ALLOWED_SAMPLE_REASONS: tuple[str, ...] = (
    "lexical_music_surface_match",
    "adjacent_audio_not_mir",
    "education_health_surface_match",
    "industrial_bioacoustic_surface_match",
    "recommender_not_music_specific",
    "topic_neighbor_near_miss",
    "fallback_deterministic_fill",
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
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
)

CAVEATS: tuple[str, ...] = (
    "This worksheet is not validation.",
    "Rows are for reviewer-blind external near-miss manual labeling only.",
    "No model is trained, no ranking is run, and no production ranking change is supported.",
    "The reviewer CSV intentionally hides score, rank, model, ranking-run, and internal database fields.",
)

OPENALEX_SELECT_FIELDS: tuple[str, ...] = build_work_select_clause()
OPENALEX_FILTER = ",".join(
    (
        "from_publication_date:2016-01-01",
        "language:en",
        "type:article|proceedings-article|preprint",
        "is_retracted:false",
        "has_abstract:true",
    )
)
OPENALEX_PER_PAGE = 50


@dataclass(frozen=True)
class QueryPlan:
    sample_reason: str
    query: str
    normalized_query: str
    per_page: int = OPENALEX_PER_PAGE

    def url(self) -> str:
        params = {
            "filter": OPENALEX_FILTER,
            "search": self.query,
            "per-page": str(self.per_page),
            "sort": "publication_date:desc",
            "select": ",".join(OPENALEX_SELECT_FIELDS),
        }
        return f"{OPENALEX_WORKS_URL}?{urlencode(params)}"


QUERY_PLANS: tuple[QueryPlan, ...] = (
    QueryPlan(
        "lexical_music_surface_match",
        "music recommendation user behavior playlist platform",
        "music recommendation user behavior playlist platform",
    ),
    QueryPlan(
        "lexical_music_surface_match",
        "music information retrieval metadata discovery",
        "music information retrieval metadata discovery",
    ),
    QueryPlan(
        "adjacent_audio_not_mir",
        "audio representation learning environmental sound classification",
        "audio representation learning environmental sound classification",
    ),
    QueryPlan(
        "adjacent_audio_not_mir",
        "speech audio foundation model acoustic scene classification",
        "speech audio foundation model acoustic scene classification",
    ),
    QueryPlan(
        "education_health_surface_match",
        "music therapy recommendation health patient audio",
        "music therapy recommendation health patient audio",
    ),
    QueryPlan(
        "education_health_surface_match",
        "music education learning system recommender audio",
        "music education learning system recommender audio",
    ),
    QueryPlan(
        "industrial_bioacoustic_surface_match",
        "industrial machine fault diagnosis audio sound",
        "industrial machine fault diagnosis audio sound",
    ),
    QueryPlan(
        "industrial_bioacoustic_surface_match",
        "bioacoustic environmental sound detection deep learning",
        "bioacoustic environmental sound detection deep learning",
    ),
    QueryPlan(
        "recommender_not_music_specific",
        "recommender system personalization platform user engagement",
        "recommender system personalization platform user engagement",
    ),
    QueryPlan(
        "recommender_not_music_specific",
        "recommendation algorithm fairness user modeling platform",
        "recommendation algorithm fairness user modeling platform",
    ),
    QueryPlan(
        "topic_neighbor_near_miss",
        "audio dataset bias benchmark machine listening",
        "audio dataset bias benchmark machine listening",
    ),
    QueryPlan(
        "topic_neighbor_near_miss",
        "multimodal sound source recognition metadata",
        "multimodal sound source recognition metadata",
    ),
    QueryPlan(
        "fallback_deterministic_fill",
        "sound recommendation learning classification audio",
        "sound recommendation learning classification audio",
    ),
)


class MLExternalNearMissReviewWorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ExternalCandidate:
    paper_id: str
    work_token: str
    title: str
    year: int | None
    citation_count: int
    source_slug: str
    source_display_name: str
    source_id: str
    work_type: str
    topics: tuple[str, ...]
    abstract: str
    sample_reason: str
    source_query: str
    normalized_query: str
    query_url: str
    retrieved_at: str
    raw_openalex_url: str
    doi: str
    publication_date: str
    hidden_diagnostics: dict[str, Any]


FetchOpenAlexJson = Callable[[str], Mapping[str, Any]]


def stable_row_id(*, worksheet_version: str, sample_seed: int, paper_id: str) -> str:
    raw = f"{worksheet_version}|{sample_seed}|{paper_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _norm_ws(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def canonical_work_token(value: Any) -> str | None:
    token = paper_id_to_work_id(_norm_ws(value))
    if token:
        return token.upper()
    raw = _norm_ws(value)
    if re.fullmatch(r"W\d+", raw, flags=re.IGNORECASE):
        return raw.upper()
    return None


def _work_tokens_from_row(row: Mapping[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for key in ("work_id", "openalex_work_id", "paper_id"):
        token = canonical_work_token(row.get(key))
        if token:
            tokens.add(token)
    return tokens


def _row_has_any_manual_label(row: Mapping[str, Any]) -> bool:
    return any(_norm_ws(row.get(field)) for field in LABEL_FIELDS)


def labeled_and_seen_unlabeled_work_tokens(payload: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return set(), set()
    labeled: set[str] = set()
    seen_unlabeled: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        tokens = _work_tokens_from_row(row)
        if not tokens:
            continue
        if _row_has_any_manual_label(row):
            labeled.update(tokens)
        else:
            seen_unlabeled.update(tokens)
    return labeled, seen_unlabeled - labeled


def load_label_dataset_payload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLExternalNearMissReviewWorksheetError(f"Failed to load label dataset {path}: {exc}") from exc


def _resolve_snapshot_plan_path(path: Path | None) -> Path:
    if path is not None:
        return path
    return default_repo_root() / DEFAULT_CANDIDATE_PLAN_PATH


def load_snapshot_exclusion_tokens(
    *,
    corpus_snapshot_version: str,
    candidate_plan_path: Path | None = None,
) -> tuple[set[str], dict[str, Any]]:
    path = _resolve_snapshot_plan_path(candidate_plan_path)
    attempted_sources = [
        {
            "type": "candidate_plan_manifest",
            "path": portable_repo_path(path),
            "expected_selected_candidates_field": "selected_candidates[].openalex_id",
        },
        {
            "type": "postgres_snapshot_table_fallback_not_used",
            "table": "works",
            "where": (
                "corpus_snapshot_version = "
                f"{corpus_snapshot_version!r} AND inclusion_status = 'included'"
            ),
        },
    ]
    if not path.is_file():
        raise MLExternalNearMissReviewWorksheetError(
            "Cannot construct outside-217 exclusion set: candidate plan manifest not found. "
            f"Attempted sources: {json.dumps(attempted_sources, sort_keys=True)}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MLExternalNearMissReviewWorksheetError(f"Failed to parse candidate plan manifest {path}: {exc}") from exc
    selected = payload.get("selected_candidates")
    if not isinstance(selected, list):
        raise MLExternalNearMissReviewWorksheetError(
            "Cannot construct outside-217 exclusion set: candidate plan manifest lacks selected_candidates list. "
            f"Attempted sources: {json.dumps(attempted_sources, sort_keys=True)}"
        )
    tokens: set[str] = set()
    for item in selected:
        if not isinstance(item, Mapping):
            continue
        token = canonical_work_token(item.get("openalex_id"))
        if token:
            tokens.add(token)
    if not tokens:
        raise MLExternalNearMissReviewWorksheetError(
            "Cannot construct outside-217 exclusion set: selected_candidates contained no OpenAlex W tokens. "
            f"Attempted sources: {json.dumps(attempted_sources, sort_keys=True)}"
        )
    provenance = {
        "type": "candidate_plan_manifest",
        "path": portable_repo_path(path),
        "sha256": sha256_file(path),
        "selected_candidates_field": "selected_candidates[].openalex_id",
        "snapshot_version": corpus_snapshot_version,
        "manifest_selected_total": payload.get("selected_total"),
        "attempted_sources": attempted_sources,
    }
    return tokens, provenance


def _slugify_source(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    return slug[:80]


def _source_metadata(work: Mapping[str, Any]) -> tuple[str, str, str]:
    loc = work.get("primary_location")
    source: Mapping[str, Any] | None = None
    if isinstance(loc, Mapping):
        raw_source = loc.get("source")
        if isinstance(raw_source, Mapping):
            source = raw_source
    display = _norm_ws(source.get("display_name") if source else "")
    source_id = _norm_ws(source.get("id") if source else "")
    slug = _slugify_source(display or source_id)
    return slug, display, source_id


def _topic_names_from_openalex(work: Mapping[str, Any]) -> tuple[str, ...]:
    raw_topics = work.get("topics")
    if not isinstance(raw_topics, list):
        return ()
    names: list[str] = []
    for item in raw_topics[:5]:
        if not isinstance(item, Mapping):
            continue
        name = _norm_ws(item.get("display_name") or item.get("name"))
        if name:
            names.append(name)
    return tuple(names)


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_year(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _candidate_from_openalex_work(
    work: Mapping[str, Any],
    *,
    plan: QueryPlan,
    query_url: str,
    retrieved_at: str,
) -> ExternalCandidate | None:
    work_url = _norm_ws(work.get("id"))
    token = canonical_work_token(work_url)
    if not token:
        return None
    title = clean_openalex_text(_norm_ws(work.get("title")))
    abstract = abstract_plain_text(work)
    source_slug, source_display_name, source_id = _source_metadata(work)
    topics = _topic_names_from_openalex(work)
    paper_id = f"https://openalex.org/{token}"
    topic_text = " ".join(topics)
    hidden = {
        "openalex_type": _norm_ws(work.get("type")),
        "language": _norm_ws(work.get("language")),
        "query_match_strategy": plan.sample_reason,
        "topic_names": list(topics),
        "title_has_music": bool(re.search(r"\b(music|song|playlist|recommender?)\b", title, flags=re.IGNORECASE)),
        "title_or_topic_has_audio": bool(
            re.search(r"\b(audio|sound|speech|acoustic|music)\b", f"{title} {topic_text}", flags=re.IGNORECASE)
        ),
    }
    return ExternalCandidate(
        paper_id=paper_id,
        work_token=token,
        title=title,
        year=_safe_year(work.get("publication_year")),
        citation_count=_safe_int(work.get("cited_by_count")),
        source_slug=source_slug,
        source_display_name=source_display_name,
        source_id=source_id,
        work_type=_norm_ws(work.get("type")),
        topics=topics,
        abstract=abstract,
        sample_reason=plan.sample_reason,
        source_query=plan.query,
        normalized_query=plan.normalized_query,
        query_url=query_url,
        retrieved_at=retrieved_at,
        raw_openalex_url=work_url,
        doi=_norm_ws(work.get("doi")),
        publication_date=_norm_ws(work.get("publication_date")),
        hidden_diagnostics=hidden,
    )


def fetch_external_candidates(
    *,
    fetch_json: FetchOpenAlexJson,
    retrieved_at: str,
) -> tuple[list[ExternalCandidate], dict[str, Any]]:
    candidates: list[ExternalCandidate] = []
    query_metadata: list[dict[str, Any]] = []
    for plan in QUERY_PLANS:
        url = plan.url()
        payload = fetch_json(url)
        results = payload.get("results")
        if not isinstance(results, list):
            results = []
        meta = payload.get("meta") if isinstance(payload.get("meta"), Mapping) else {}
        query_metadata.append(
            {
                "sample_reason": plan.sample_reason,
                "query": plan.query,
                "normalized_query": plan.normalized_query,
                "url": url,
                "results_returned": len(results),
                "openalex_count": meta.get("count") if isinstance(meta, Mapping) else None,
            }
        )
        for item in results:
            if not isinstance(item, Mapping):
                continue
            cand = _candidate_from_openalex_work(item, plan=plan, query_url=url, retrieved_at=retrieved_at)
            if cand is not None:
                candidates.append(cand)
    debug = {
        "query_metadata": query_metadata,
        "raw_candidate_count": len(candidates),
        "raw_candidate_count_by_strategy": dict(Counter(c.sample_reason for c in candidates)),
    }
    return candidates, debug


def _candidate_sort_key(cand: ExternalCandidate) -> tuple[str, str, str]:
    return (cand.work_token, cand.title.casefold(), cand.paper_id)


def _csv_order_key(cand: ExternalCandidate, *, seed: int) -> str:
    rid = stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id)
    return hashlib.sha256(f"{seed}|csv_order|{rid}".encode("utf-8")).hexdigest()


def select_external_near_miss_sample(
    candidates: Sequence[ExternalCandidate],
    *,
    snapshot_tokens: set[str],
    labeled_tokens: set[str],
    seen_unlabeled_tokens: set[str],
    rows: int,
    seed: int,
) -> tuple[list[ExternalCandidate], dict[str, Any]]:
    if rows < MIN_ROWS or rows > MAX_ROWS:
        raise MLExternalNearMissReviewWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")

    by_reason_raw = Counter(c.sample_reason for c in candidates)
    filter_counts = Counter()
    filtered_by_reason: dict[str, list[ExternalCandidate]] = defaultdict(list)
    seen_in_acquisition: set[str] = set()

    for cand in candidates:
        token = cand.work_token.upper()
        if token in seen_in_acquisition:
            filter_counts["duplicate_candidate"] += 1
            continue
        seen_in_acquisition.add(token)
        if token in snapshot_tokens:
            filter_counts["source_snapshot_exclusion"] += 1
            continue
        if token in labeled_tokens:
            filter_counts["v6_labeled_exclusion"] += 1
            continue
        if token in seen_unlabeled_tokens:
            filter_counts["v6_seen_unlabeled_exclusion"] += 1
            continue
        filtered_by_reason[cand.sample_reason].append(cand)

    for reason in ALLOWED_SAMPLE_REASONS:
        filtered_by_reason[reason].sort(key=_candidate_sort_key)

    selected: list[ExternalCandidate] = []
    used: set[str] = set()
    primary_reasons = tuple(r for r in ALLOWED_SAMPLE_REASONS if r != "fallback_deterministic_fill")
    for reasons in (primary_reasons, ("fallback_deterministic_fill",)):
        while len(selected) < rows:
            progressed = False
            for reason in reasons:
                if len(selected) >= rows:
                    break
                bucket = filtered_by_reason.get(reason, [])
                while bucket:
                    cand = bucket.pop(0)
                    token = cand.work_token.upper()
                    if token in used:
                        continue
                    selected.append(cand)
                    used.add(token)
                    progressed = True
                    break
            if not progressed:
                break
        if len(selected) >= rows:
            break

    selected.sort(key=lambda c: (_csv_order_key(c, seed=seed), c.work_token))
    by_reason_selected = Counter(c.sample_reason for c in selected)
    after_filter_counts = {
        reason: len([c for c in selected if c.sample_reason == reason])
        + len(filtered_by_reason.get(reason, []))
        for reason in ALLOWED_SAMPLE_REASONS
    }
    credible_count = sum(after_filter_counts.get(r, 0) for r in primary_reasons)
    achieved = len(selected)
    debug = {
        "requested_rows": rows,
        "achieved_rows": achieved,
        "shortfall_count": max(0, rows - achieved),
        "raw_candidate_count": len(candidates),
        "raw_candidate_count_by_strategy": dict(sorted(by_reason_raw.items())),
        "candidate_count_after_filter_by_strategy": {
            reason: after_filter_counts.get(reason, 0) for reason in ALLOWED_SAMPLE_REASONS
        },
        "selected_count_by_strategy": {reason: by_reason_selected.get(reason, 0) for reason in ALLOWED_SAMPLE_REASONS},
        "excluded_counts": dict(sorted(filter_counts.items())),
        "credible_candidate_pool_size": credible_count,
        "fallback_candidate_pool_size": after_filter_counts.get("fallback_deterministic_fill", 0),
        "pool_supported_requested_rows": achieved >= rows and by_reason_selected.get("fallback_deterministic_fill", 0) == 0,
        "selection_note": (
            "Requested row count was supported by non-fallback external near-miss strategies."
            if achieved >= rows and by_reason_selected.get("fallback_deterministic_fill", 0) == 0
            else "Shortfall or fallback occurred; inspect sample_reason counts before treating the sheet as a strong hard-negative pool."
        ),
        "csv_ordering": "seeded row_id hash order; not score/rank order",
    }
    return selected, debug


def _candidate_csv_row(*, cand: ExternalCandidate, seed: int) -> dict[str, str]:
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
        "topics": ";".join(cand.topics) if cand.topics else "",
        "abstract_preview": _truncate_abstract(cand.abstract, ABSTRACT_PREVIEW_MAX_CHARS) if cand.abstract else "",
        "sample_reason": cand.sample_reason,
        "cluster_id": EXTERNAL_CLUSTER_SENTINEL,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }


def _candidate_sidecar_row(*, cand: ExternalCandidate, seed: int) -> dict[str, Any]:
    return {
        "row_id": stable_row_id(worksheet_version=WORKSHEET_VERSION, sample_seed=seed, paper_id=cand.paper_id),
        "paper_id": cand.paper_id,
        "openalex_work_id": cand.work_token,
        "work_id_policy": "OpenAlex W token only; no Postgres works.id is used for this external worksheet.",
        "sample_seed": seed,
        "sample_reason": cand.sample_reason,
        "cluster_id": EXTERNAL_CLUSTER_SENTINEL,
        "external_cluster_policy": "external candidate; no pipeline k-means cluster assignment was fabricated",
        "source_query": cand.source_query,
        "normalized_query": cand.normalized_query,
        "query_url": cand.query_url,
        "raw_source_url": cand.raw_openalex_url,
        "openalex_api_url": f"{OPENALEX_WORKS_URL}/{cand.work_token}",
        "retrieved_at": cand.retrieved_at,
        "exclusion_checks_passed": {
            "outside_source_snapshot_217": True,
            "not_v6_labeled": True,
            "not_v6_seen_unlabeled": True,
        },
        "source_metadata": {
            "source_slug": cand.source_slug,
            "source_display_name": cand.source_display_name,
            "source_id": cand.source_id,
            "doi": cand.doi,
            "publication_date": cand.publication_date,
            "work_type": cand.work_type,
        },
        "review_metadata": {
            "title": cand.title,
            "year": cand.year,
            "citation_count": cand.citation_count,
            "topics": list(cand.topics),
            "abstract_preview": _truncate_abstract(cand.abstract, ABSTRACT_PREVIEW_MAX_CHARS) if cand.abstract else "",
        },
        "hidden_diagnostics": cand.hidden_diagnostics,
    }


def render_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    return buf.getvalue()


def build_context_payload(
    *,
    sidecar_rows: Sequence[dict[str, Any]],
    label_dataset_path: Path,
    label_dataset_sha256: str,
    conflict_policy_path: Path,
    conflict_policy_sha256: str,
    corpus_snapshot_version: str,
    snapshot_source: dict[str, Any],
    snapshot_exclusion_count: int,
    labeled_exclusion_count: int,
    seen_unlabeled_count: int,
    seed: int,
    requested_rows: int,
    debug: dict[str, Any],
    query_debug: dict[str, Any],
    mailto: str | None,
) -> dict[str, Any]:
    contact_mode, contact_provided = compute_contact_provenance(mailto_cli=mailto or "", mock_openalex=False)
    api_key_provided, auth_mode = compute_openalex_auth_artifact_fields(mock_openalex=False)
    query_metadata = list(query_debug.get("query_metadata", []))
    return {
        "artifact_type": "ml_external_near_miss_review_v1_context",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "provenance": {
            "worksheet_version": WORKSHEET_VERSION,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "sample_seed": seed,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": portable_repo_path(label_dataset_path),
            "label_dataset_sha256": label_dataset_sha256,
            "conflict_policy_path": portable_repo_path(conflict_policy_path),
            "conflict_policy_sha256": conflict_policy_sha256,
            "corpus_snapshot_version": corpus_snapshot_version,
            "source_snapshot_exclusion_source": snapshot_source,
            "source_snapshot_exclusion_count": snapshot_exclusion_count,
            "v6_labeled_exclusion_count": labeled_exclusion_count,
            "seen_unlabeled_count": seen_unlabeled_count,
            "candidate_acquisition_method": (
                "OpenAlex /works search query mix; filter by outside source snapshot, v6 labels, and v6 seen-unlabeled rows."
            ),
            "query_strings": [m.get("query") for m in query_metadata],
            "normalized_queries": [m.get("normalized_query") for m in query_metadata],
            "source_api_metadata": {
                "provider": "OpenAlex",
                "endpoint": OPENALEX_WORKS_URL,
                "contact_mode": contact_mode,
                "contact_provided": contact_provided,
                "api_key_provided": api_key_provided,
                "auth_mode": auth_mode,
                "per_page": OPENALEX_PER_PAGE,
                "select_fields": list(OPENALEX_SELECT_FIELDS),
                "filter": OPENALEX_FILTER,
            },
            "generated_candidate_counts": {
                "raw": int(query_debug.get("raw_candidate_count", debug.get("raw_candidate_count", 0))),
                "raw_by_strategy": dict(query_debug.get("raw_candidate_count_by_strategy", {})),
                "after_filter_by_strategy": dict(debug.get("candidate_count_after_filter_by_strategy", {})),
                "selected_by_strategy": dict(debug.get("selected_count_by_strategy", {})),
                "excluded_counts": dict(debug.get("excluded_counts", {})),
            },
            "requested_rows": requested_rows,
            "achieved_rows": int(debug.get("achieved_rows", len(sidecar_rows))),
            "shortfall_count": int(debug.get("shortfall_count", max(0, requested_rows - len(sidecar_rows)))),
        },
        "schema": {
            "key": "row_id",
            "hidden_from_reviewer_csv": list(HIDDEN_REVIEWER_CSV_FIELDS),
            "notes": (
                "Reviewer CSV stores only OpenAlex W tokens in work_id. OpenAlex query URLs, exclusion evidence, "
                "and diagnostic metadata appear only in this sidecar."
            ),
        },
        "sampling_policy": {
            "allowed_sample_reasons": list(ALLOWED_SAMPLE_REASONS),
            "outside_217_definition": (
                "Candidate OpenAlex work token is absent from the committed source-snapshot-v2 candidate-plan selected set."
            ),
            "v6_labeled_exclusion": (
                "Exclude a work when any v6 row for the same OpenAlex work has a non-empty relevance_label, "
                "novelty_label, or bridge_like_label."
            ),
            "seen_unlabeled_policy": "Seen-but-unlabeled v6 works are counted and excluded by default.",
            "csv_ordering": debug.get("csv_ordering", "seeded row_id hash order; not score/rank order"),
        },
        "query_metadata": query_metadata,
        "sampling_debug": dict(debug),
        "rows": list(sidecar_rows),
    }


def render_markdown(
    *,
    selected: Sequence[ExternalCandidate],
    debug: dict[str, Any],
    query_debug: dict[str, Any],
    label_dataset_path: Path,
    label_dataset_sha256: str,
    conflict_policy_path: Path,
    conflict_policy_sha256: str,
    corpus_snapshot_version: str,
    snapshot_source: dict[str, Any],
    snapshot_exclusion_count: int,
    labeled_exclusion_count: int,
    seen_unlabeled_count: int,
    seed: int,
    requested_rows: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
) -> str:
    by_reason = Counter(c.sample_reason for c in selected)
    achieved = int(debug.get("achieved_rows", len(selected)))
    shortfall = int(debug.get("shortfall_count", max(0, requested_rows - achieved)))
    fallback_count = by_reason.get("fallback_deterministic_fill", 0)
    supported = bool(debug.get("pool_supported_requested_rows", False))
    query_rows = []
    for item in query_debug.get("query_metadata", []):
        query_rows.append(
            f"| `{item.get('sample_reason')}` | {item.get('results_returned')} | {item.get('openalex_count', '')} | `{item.get('query')}` |"
        )
    lines = [
        f"# External near-miss review worksheet (`{WORKSHEET_VERSION}`)",
        "",
        "## Purpose",
        "",
        "Reviewer-blind worksheet for plausible but not-yet-curated external music/audio/recommender near misses. "
        "This expands negative-boundary label coverage outside the current 217-work snapshot; it is not model training, ranking, validation, or production readiness.",
        "",
        "## Provenance",
        "",
        f"- **worksheet_version:** `{WORKSHEET_VERSION}`",
        f"- **review_pool_variant:** `{REVIEW_POOL_VARIANT}`",
        f"- **sample_seed:** `{seed}`",
        f"- **row_id formula:** `sha256(worksheet_version|sample_seed|paper_id)`",
        f"- **label_dataset:** `{portable_repo_path(label_dataset_path)}`",
        f"- **label_dataset_sha256:** `{label_dataset_sha256}`",
        f"- **conflict_policy:** `{portable_repo_path(conflict_policy_path)}`",
        f"- **conflict_policy_sha256:** `{conflict_policy_sha256}`",
        f"- **corpus_snapshot_version:** `{corpus_snapshot_version}`",
        f"- **outside-217 exclusion source:** `{snapshot_source.get('type')}` `{snapshot_source.get('path')}`",
        f"- **outside-217 exclusion count:** `{snapshot_exclusion_count}`",
        f"- **v6 labeled exclusion count:** `{labeled_exclusion_count}`",
        f"- **seen-unlabeled v6 count:** `{seen_unlabeled_count}`",
        f"- **csv_output:** `{portable_repo_path(csv_output_path)}`",
        f"- **context_sidecar_output:** `{portable_repo_path(context_output_path)}`",
        f"- **markdown_output:** `{portable_repo_path(markdown_output_path)}`",
        "",
        "## Reviewer CSV Policy",
        "",
        "The reviewer CSV contains only reviewer-facing identity, bibliographic, topic, abstract-preview, sample_reason, and blank label columns. "
        "It excludes ranking identifiers, score/rank fields, family score/rank JSON, learned logits, model predictions, internal database IDs, and snapshot/embedding/cluster version fields.",
        "",
        f"`cluster_id` is the documented sentinel `{EXTERNAL_CLUSTER_SENTINEL}` for external candidates because no pipeline k-means cluster assignment exists outside the snapshot.",
        "",
        "## Acquisition Summary",
        "",
        f"- **requested rows:** `{requested_rows}`",
        f"- **achieved rows:** `{achieved}`",
        f"- **shortfall:** `{shortfall}`",
        f"- **fallback rows:** `{fallback_count}`",
        f"- **raw OpenAlex candidates:** `{query_debug.get('raw_candidate_count')}`",
        f"- **credible non-fallback candidate pool after filters:** `{debug.get('credible_candidate_pool_size')}`",
        f"- **pool supported requested near-miss intent:** `{str(supported).lower()}`",
        f"- **selection note:** {debug.get('selection_note')}",
    ]
    if shortfall > 0 or fallback_count > 0:
        lines.extend(
            [
                "",
                "## Shortfall Or Fallback",
                "",
                "The worksheet reports fallback and shortfall explicitly. It does not use ranking scores, random weak padding, or curated-snapshot rows to fill the sheet.",
            ]
        )
    lines.extend(
        [
            "",
            "## Row Counts By Sample Reason",
            "",
            "| sample_reason | rows |",
            "|---|---:|",
            *[f"| `{reason}` | {by_reason[reason]} |" for reason in ALLOWED_SAMPLE_REASONS if by_reason[reason] > 0],
            "",
            "## Candidate Counts By Strategy",
            "",
            "| strategy | raw | after_filter | selected |",
            "|---|---:|---:|---:|",
        ]
    )
    raw_counts = debug.get("raw_candidate_count_by_strategy", {})
    after_counts = debug.get("candidate_count_after_filter_by_strategy", {})
    selected_counts = debug.get("selected_count_by_strategy", {})
    for reason in ALLOWED_SAMPLE_REASONS:
        lines.append(
            f"| `{reason}` | {raw_counts.get(reason, 0)} | {after_counts.get(reason, 0)} | {selected_counts.get(reason, 0)} |"
        )
    lines.extend(
        [
            "",
            "## Query Metadata",
            "",
            "| strategy | returned | OpenAlex count | query |",
            "|---|---:|---:|---|",
            *query_rows,
            "",
            "## Future Ingest Note",
            "",
            "When labeled, a later dataset ingest should use a dated labeled copy such as `ml_external_near_miss_review_v1_labeled_YYYY-MM-DD.csv`, merge this sidecar by `row_id`, and keep `review_pool_variant=ml_external_near_miss_audit` distinct unless an experiment explicitly pools it.",
            "",
            "## Caveats",
            "",
            *[f"- {c}" for c in CAVEATS],
            "",
        ]
    )
    return "\n".join(lines)


def build_external_near_miss_review_worksheet(
    *,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    corpus_snapshot_version: str,
    candidate_plan_path: Path | None,
    rows: int,
    seed: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    mailto: str | None = None,
    fetch_json: FetchOpenAlexJson | None = None,
    retrieved_at: str | None = None,
) -> tuple[str, dict[str, Any], str, dict[str, Any]]:
    if rows < MIN_ROWS or rows > MAX_ROWS:
        raise MLExternalNearMissReviewWorksheetError(f"--rows must be between {MIN_ROWS} and {MAX_ROWS}")
    if not label_dataset_path.is_file():
        raise MLExternalNearMissReviewWorksheetError(f"label dataset not found: {label_dataset_path}")
    if not conflict_policy_path.is_file():
        raise MLExternalNearMissReviewWorksheetError(f"conflict policy not found: {conflict_policy_path}")

    label_payload = load_label_dataset_payload(label_dataset_path)
    label_sha = sha256_file(label_dataset_path)
    conflict_sha = sha256_file(conflict_policy_path)
    labeled_tokens, seen_unlabeled_tokens = labeled_and_seen_unlabeled_work_tokens(label_payload)
    snapshot_tokens, snapshot_source = load_snapshot_exclusion_tokens(
        corpus_snapshot_version=corpus_snapshot_version,
        candidate_plan_path=candidate_plan_path,
    )
    retrieved = retrieved_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if fetch_json is None:
        fetch_json = lambda url: fetch_openalex_json(url, mailto=mailto, timeout_sec=60.0)

    candidates, query_debug = fetch_external_candidates(fetch_json=fetch_json, retrieved_at=retrieved)
    selected, selection_debug = select_external_near_miss_sample(
        candidates,
        snapshot_tokens=snapshot_tokens,
        labeled_tokens=labeled_tokens,
        seen_unlabeled_tokens=seen_unlabeled_tokens,
        rows=rows,
        seed=seed,
    )
    debug = dict(selection_debug)
    debug.update(
        {
            "source_snapshot_exclusion_count": len(snapshot_tokens),
            "v6_labeled_exclusion_count": len(labeled_tokens),
            "seen_unlabeled_count": len(seen_unlabeled_tokens),
            "db_access": "none; source snapshot exclusion loaded from committed candidate-plan manifest",
            "query_count": len(query_debug.get("query_metadata", [])),
        }
    )
    csv_rows = [_candidate_csv_row(cand=cand, seed=seed) for cand in selected]
    sidecar_rows = [_candidate_sidecar_row(cand=cand, seed=seed) for cand in selected]
    csv_ids = {row["row_id"] for row in csv_rows}
    sidecar_ids = {row["row_id"] for row in sidecar_rows}
    if csv_ids != sidecar_ids:
        raise MLExternalNearMissReviewWorksheetError("internal error: CSV and sidecar row_id sets differ")

    csv_text = render_csv(csv_rows)
    context_payload = build_context_payload(
        sidecar_rows=sidecar_rows,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        conflict_policy_path=conflict_policy_path,
        conflict_policy_sha256=conflict_sha,
        corpus_snapshot_version=corpus_snapshot_version,
        snapshot_source=snapshot_source,
        snapshot_exclusion_count=len(snapshot_tokens),
        labeled_exclusion_count=len(labeled_tokens),
        seen_unlabeled_count=len(seen_unlabeled_tokens),
        seed=seed,
        requested_rows=rows,
        debug=debug,
        query_debug=query_debug,
        mailto=mailto,
    )
    md_text = render_markdown(
        selected=selected,
        debug=debug,
        query_debug=query_debug,
        label_dataset_path=label_dataset_path,
        label_dataset_sha256=label_sha,
        conflict_policy_path=conflict_policy_path,
        conflict_policy_sha256=conflict_sha,
        corpus_snapshot_version=corpus_snapshot_version,
        snapshot_source=snapshot_source,
        snapshot_exclusion_count=len(snapshot_tokens),
        labeled_exclusion_count=len(labeled_tokens),
        seen_unlabeled_count=len(seen_unlabeled_tokens),
        seed=seed,
        requested_rows=rows,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
    )
    return csv_text, context_payload, md_text, debug


def run_ml_external_near_miss_review_worksheet_cli(
    *,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    corpus_snapshot_version: str,
    candidate_plan_path: Path | None,
    rows: int,
    seed: int,
    csv_output_path: Path,
    context_output_path: Path,
    markdown_output_path: Path,
    mailto: str | None = None,
) -> dict[str, Any]:
    csv_text, context_payload, md_text, debug = build_external_near_miss_review_worksheet(
        label_dataset_path=label_dataset_path,
        conflict_policy_path=conflict_policy_path,
        corpus_snapshot_version=corpus_snapshot_version,
        candidate_plan_path=candidate_plan_path,
        rows=rows,
        seed=seed,
        csv_output_path=csv_output_path,
        context_output_path=context_output_path,
        markdown_output_path=markdown_output_path,
        mailto=mailto,
    )
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    context_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    csv_output_path.write_text(csv_text, encoding="utf-8", newline="")
    context_output_path.write_text(json.dumps(context_payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    markdown_output_path.write_text(md_text, encoding="utf-8", newline="")
    return debug


__all__ = [
    "ALLOWED_SAMPLE_REASONS",
    "CSV_COLUMNS",
    "DEFAULT_CORPUS_SNAPSHOT_VERSION",
    "DEFAULT_SAMPLE_SEED",
    "HIDDEN_REVIEWER_CSV_FIELDS",
    "MLExternalNearMissReviewWorksheetError",
    "REVIEW_POOL_VARIANT",
    "WORKSHEET_VERSION",
    "build_external_near_miss_review_worksheet",
    "canonical_work_token",
    "labeled_and_seen_unlabeled_work_tokens",
    "load_snapshot_exclusion_tokens",
    "render_csv",
    "render_markdown",
    "run_ml_external_near_miss_review_worksheet_cli",
    "select_external_near_miss_sample",
    "stable_row_id",
]
