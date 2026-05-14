"""External near-miss OpenAlex text hydration corpus.

Builds a read-only text-feature artifact for `ml_external_near_miss_audit`
rows. The command never reads or writes Postgres, never trains a model, and
never runs ranking materialization.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode

from pipeline.ml_label_dataset import sha256_file
from pipeline.openalex_ids import normalize_w_token
from pipeline.openalex import OPENALEX_WORKS_URL, build_work_select_clause
from pipeline.openalex_client import (
    compute_contact_provenance,
    compute_openalex_auth_artifact_fields,
    fetch_openalex_json,
)
from pipeline.openalex_text import abstract_plain_text, clean_openalex_text
from pipeline.repo_paths import portable_repo_path

REVIEW_POOL_VARIANT = "ml_external_near_miss_audit"
EXPECTED_EXTERNAL_ROWS = 60
CORPUS_VERSION = "ml-external-text-corpus-v7"
OPENALEX_SELECT_FIELDS: tuple[str, ...] = build_work_select_clause()

CAVEATS = (
    "Not validation.",
    "Text hydration only; hydrated OpenAlex text may differ from worksheet previews.",
    "No DB writes.",
    "No ranking or model training.",
    "OpenAlex metadata can drift vs frozen sidecar/context snapshots.",
)

FetchOpenAlexJson = Callable[[str], Mapping[str, Any]]


class MLExternalTextCorpusError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _norm_ws(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLExternalTextCorpusError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLExternalTextCorpusError(f"Expected JSON object in {path}")
    return payload


def _canonical_json_sha256(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def _row_work_token(row: Mapping[str, Any]) -> str | None:
    for key in ("work_id", "openalex_work_id", "paper_id"):
        token = normalize_w_token(_norm_ws(row.get(key)))
        if token:
            return token
    return None


def select_external_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLExternalTextCorpusError("label dataset missing rows array")
    return [
        dict(row)
        for row in rows
        if isinstance(row, dict) and _norm_ws(row.get("review_pool_variant")) == REVIEW_POOL_VARIANT
    ]


def _validate_external_rows(rows: list[Mapping[str, Any]], *, expected_count: int) -> None:
    if len(rows) != expected_count:
        raise MLExternalTextCorpusError(
            f"Expected exactly {expected_count} {REVIEW_POOL_VARIANT} rows, found {len(rows)}. "
            "This v7 corpus builder is strict by default so row coverage cannot silently drift."
        )
    row_ids = [_norm_ws(row.get("row_id")) for row in rows]
    missing_row_ids = [i + 1 for i, rid in enumerate(row_ids) if not rid]
    if missing_row_ids:
        raise MLExternalTextCorpusError(f"External rows missing row_id at selected positions: {missing_row_ids[:10]}")
    if len(set(row_ids)) != len(row_ids):
        raise MLExternalTextCorpusError("External near-miss rows contain duplicate row_id values")
    missing_tokens = [_norm_ws(row.get("row_id")) for row in rows if not _row_work_token(row)]
    if missing_tokens:
        raise MLExternalTextCorpusError(f"External rows missing OpenAlex W token: {missing_tokens[:10]}")


def _work_api_url(work_token: str) -> str:
    params = {"select": ",".join(OPENALEX_SELECT_FIELDS)}
    return f"{OPENALEX_WORKS_URL}/{work_token}?{urlencode(params)}"


def _context(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = row.get("external_near_miss_context")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _context_review_metadata(ctx: Mapping[str, Any]) -> dict[str, Any]:
    raw = ctx.get("review_metadata")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _best_preview(row: Mapping[str, Any]) -> tuple[str, str]:
    dataset_preview = _norm_ws(row.get("abstract_preview"))
    ctx_preview = _norm_ws(_context_review_metadata(_context(row)).get("abstract_preview"))
    if ctx_preview and len(ctx_preview) > len(dataset_preview):
        return ctx_preview, "context_review_metadata.abstract_preview"
    if dataset_preview:
        return dataset_preview, "dataset.abstract_preview"
    if ctx_preview:
        return ctx_preview, "context_review_metadata.abstract_preview"
    return "", "none"


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _source_fields(work: Mapping[str, Any]) -> dict[str, Any]:
    loc = work.get("primary_location")
    location = dict(loc) if isinstance(loc, Mapping) else {}
    source = location.get("source")
    source_map = dict(source) if isinstance(source, Mapping) else {}
    return {
        "primary_location": {
            "landing_page_url": _norm_ws(location.get("landing_page_url")) or None,
            "pdf_url": _norm_ws(location.get("pdf_url")) or None,
            "is_oa": location.get("is_oa"),
            "version": _norm_ws(location.get("version")) or None,
            "license": _norm_ws(location.get("license")) or None,
        },
        "source": {
            "id": _norm_ws(source_map.get("id")) or None,
            "display_name": _norm_ws(source_map.get("display_name")) or None,
            "issn_l": _norm_ws(source_map.get("issn_l")) or None,
            "issn": source_map.get("issn") if isinstance(source_map.get("issn"), list) else [],
            "type": _norm_ws(source_map.get("type")) or None,
            "host_organization": _norm_ws(source_map.get("host_organization")) or None,
        },
    }


def _openalex_topics(work: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_topics = work.get("topics")
    if not isinstance(raw_topics, list):
        return []
    topics: list[dict[str, Any]] = []
    for item in raw_topics:
        if not isinstance(item, Mapping):
            continue
        topic = {
            "id": _norm_ws(item.get("id")) or None,
            "display_name": _norm_ws(item.get("display_name") or item.get("name")) or None,
            "score": item.get("score"),
        }
        for key in ("subfield", "field", "domain"):
            raw_nested = item.get(key)
            if isinstance(raw_nested, Mapping):
                topic[key] = {
                    "id": _norm_ws(raw_nested.get("id")) or None,
                    "display_name": _norm_ws(raw_nested.get("display_name")) or None,
                }
        topics.append(topic)
    return topics


def _openalex_concepts(work: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_concepts = work.get("concepts")
    if not isinstance(raw_concepts, list):
        return []
    concepts: list[dict[str, Any]] = []
    for item in raw_concepts:
        if not isinstance(item, Mapping):
            continue
        concepts.append(
            {
                "id": _norm_ws(item.get("id")) or None,
                "display_name": _norm_ws(item.get("display_name")) or None,
                "score": item.get("score"),
                "level": item.get("level"),
            }
        )
    return concepts


def _labels_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "relevance_label",
        "novelty_label",
        "bridge_like_label",
        "reviewer_notes",
        "good_or_acceptable",
        "surprising_or_useful",
        "bridge_like_yes_or_partial",
    )
    return {key: row.get(key) for key in keys}


def _prefix_match(preview: str, full_abstract: str, *, n: int = 80) -> bool | None:
    p = _norm_ws(preview).casefold()
    f = _norm_ws(full_abstract).casefold()
    if not p or not f:
        return None
    return f.startswith(p[: min(n, len(p))])


def _comparison(row: Mapping[str, Any], ctx: Mapping[str, Any], hydrated: Mapping[str, Any]) -> dict[str, Any]:
    review = _context_review_metadata(ctx)
    hydrated_title = _norm_ws(hydrated.get("title"))
    hydrated_year = _as_int(hydrated.get("publication_year"))
    hydrated_citations = _as_int(hydrated.get("cited_by_count"))
    dataset_title = _norm_ws(row.get("title"))
    context_title = _norm_ws(review.get("title"))
    best_preview, preview_source = _best_preview(row)
    full_abstract = _norm_ws(hydrated.get("full_abstract"))
    return {
        "dataset": {
            "title_changed": bool(hydrated_title and dataset_title and hydrated_title != dataset_title),
            "year_changed": hydrated_year is not None
            and _as_int(row.get("year")) is not None
            and hydrated_year != _as_int(row.get("year")),
            "citation_count_changed": hydrated_citations is not None
            and _as_int(row.get("citation_count")) is not None
            and hydrated_citations != _as_int(row.get("citation_count")),
        },
        "nested_context_review_metadata": {
            "title_changed": bool(hydrated_title and context_title and hydrated_title != context_title),
            "year_changed": hydrated_year is not None
            and _as_int(review.get("year")) is not None
            and hydrated_year != _as_int(review.get("year")),
            "citation_count_changed": hydrated_citations is not None
            and _as_int(review.get("citation_count")) is not None
            and hydrated_citations != _as_int(review.get("citation_count")),
        },
        "abstract_preview_prefix_source": preview_source,
        "abstract_preview_prefix_matches_full_prefix": _prefix_match(best_preview, full_abstract),
    }


def _abstract_to_inverted_index(text: str) -> dict[str, list[int]]:
    index: dict[str, list[int]] = {}
    for i, word in enumerate(_norm_ws(text).split()):
        index.setdefault(word, []).append(i)
    return index


def _mock_work_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    token = _row_work_token(row) or ""
    preview, _source = _best_preview(row)
    source_slug = _norm_ws(row.get("source_slug")) or "mock-source"
    return {
        "id": f"https://openalex.org/{token}",
        "doi": None,
        "title": _norm_ws(row.get("title")),
        "type": "mock",
        "language": "en",
        "publication_year": _as_int(row.get("year")),
        "publication_date": None,
        "cited_by_count": _as_int(row.get("citation_count")) or 0,
        "primary_location": {
            "source": {
                "id": None,
                "display_name": source_slug,
                "type": "mock",
            }
        },
        "topics": [],
        "abstract_inverted_index": _abstract_to_inverted_index(preview),
    }


def _fetch_one_work(
    *,
    row: Mapping[str, Any],
    work_token: str,
    mailto: str | None,
    mock_openalex: bool,
    fetch_json: FetchOpenAlexJson | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    url = _work_api_url(work_token)
    if mock_openalex:
        work = _mock_work_from_row(row)
        return work, {
            "openalex_api_url_used": url,
            "retrieved_at": _now_iso_z(),
            "http_status": 200,
            "error_class": None,
            "error_message": None,
            "mock_openalex": True,
            "openalex_response_sha256": _canonical_json_sha256(work),
        }
    try:
        if fetch_json is None:
            work_raw = fetch_openalex_json(url, mailto=mailto, timeout_sec=60.0)
        else:
            work_raw = fetch_json(url)
        work = dict(work_raw)
        return work, {
            "openalex_api_url_used": url,
            "retrieved_at": _now_iso_z(),
            "http_status": 200,
            "error_class": None,
            "error_message": None,
            "mock_openalex": False,
            "openalex_response_sha256": _canonical_json_sha256(work),
        }
    except Exception as exc:  # OpenAlex errors are preserved per row so one failure does not drop labels.
        return None, {
            "openalex_api_url_used": url,
            "retrieved_at": _now_iso_z(),
            "http_status": getattr(exc, "code", None),
            "error_class": type(exc).__name__,
            "error_message": str(exc)[:500],
            "mock_openalex": False,
            "openalex_response_sha256": None,
        }


def _hydrated_fields(work: Mapping[str, Any] | None) -> dict[str, Any]:
    if work is None:
        return {
            "title": "",
            "full_abstract": "",
            "publication_year": None,
            "publication_date": None,
            "cited_by_count": None,
            "type": None,
            "language": None,
            "doi": None,
            "primary_location": {},
            "source": {},
            "topics": [],
            "concepts": [],
        }
    source = _source_fields(work)
    return {
        "title": clean_openalex_text(_norm_ws(work.get("title"))),
        "full_abstract": abstract_plain_text(work),
        "publication_year": _as_int(work.get("publication_year")),
        "publication_date": _norm_ws(work.get("publication_date")) or None,
        "cited_by_count": _as_int(work.get("cited_by_count")),
        "type": _norm_ws(work.get("type")) or None,
        "language": _norm_ws(work.get("language")) or None,
        "doi": _norm_ws(work.get("doi")) or None,
        "primary_location": source["primary_location"],
        "source": source["source"],
        "topics": _openalex_topics(work),
        "concepts": _openalex_concepts(work),
    }


def _text_payload(
    *,
    row: Mapping[str, Any],
    hydrated: Mapping[str, Any],
    fetch_failed: bool,
) -> dict[str, Any]:
    title = _norm_ws(hydrated.get("title")) or _norm_ws(row.get("title"))
    full_abstract = _norm_ws(hydrated.get("full_abstract"))
    preview, preview_source = _best_preview(row)
    if full_abstract:
        text = f"{title}\n\n{full_abstract}".strip()
        abstract_source = "openalex_inverted_index"
        text_fallback_source = None
    elif fetch_failed:
        text = f"{title}\n\n{preview}".strip() if preview else title
        abstract_source = "fetch_failed"
        text_fallback_source = preview_source if preview else None
    elif preview:
        text = f"{title}\n\n{preview}".strip()
        abstract_source = "preview_fallback"
        text_fallback_source = preview_source
    else:
        text = title
        abstract_source = "empty_after_fetch"
        text_fallback_source = None
    return {
        "abstract_source": abstract_source,
        "text_for_embedding": text,
        "text_length": len(text),
        "sufficient_text_for_embedding_heuristic": len(text) >= 200,
        "text_fallback_source": text_fallback_source,
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
        raise MLExternalTextCorpusError(f"context sidecar not found: {path}")
    payload = _load_json_object(path)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLExternalTextCorpusError(f"context sidecar missing rows array: {path}")
    sidecar_ids = {
        _norm_ws(row.get("row_id"))
        for row in rows
        if isinstance(row, Mapping) and _norm_ws(row.get("row_id"))
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


def _build_row_record(
    *,
    row: Mapping[str, Any],
    mailto: str | None,
    mock_openalex: bool,
    fetch_json: FetchOpenAlexJson | None,
) -> dict[str, Any]:
    token = _row_work_token(row)
    if not token:
        raise MLExternalTextCorpusError(f"External row missing OpenAlex W token: {_norm_ws(row.get('row_id'))}")
    work, provenance = _fetch_one_work(
        row=row,
        work_token=token,
        mailto=mailto,
        mock_openalex=mock_openalex,
        fetch_json=fetch_json,
    )
    hydrated = _hydrated_fields(work)
    text = _text_payload(row=row, hydrated=hydrated, fetch_failed=provenance.get("error_class") is not None)
    ctx = _context(row)
    return {
        "row_id": _norm_ws(row.get("row_id")),
        "paper_id": _norm_ws(row.get("paper_id")) or f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": _norm_ws(row.get("work_id")),
        "review_pool_variant": _norm_ws(row.get("review_pool_variant")),
        "labels": _labels_from_row(row),
        "hydrated": hydrated,
        "comparison": _comparison(row, ctx, hydrated),
        **text,
        "provenance": provenance,
    }


def build_external_text_corpus_payload(
    *,
    label_dataset_path: Path,
    context_sidecar_path: Path | None = None,
    mailto: str | None = None,
    mock_openalex: bool = False,
    fetch_json: FetchOpenAlexJson | None = None,
    expected_external_rows: int = EXPECTED_EXTERNAL_ROWS,
) -> dict[str, Any]:
    path = label_dataset_path.resolve()
    if not path.is_file():
        raise MLExternalTextCorpusError(f"label dataset not found: {path}")
    label_payload = _load_json_object(path)
    label_sha = sha256_file(path)
    dataset_version = _norm_ws(label_payload.get("dataset_version"))
    external_rows = select_external_rows(label_payload)
    _validate_external_rows(external_rows, expected_count=expected_external_rows)
    dataset_row_ids = {_norm_ws(row.get("row_id")) for row in external_rows}

    sidecar = _sidecar_parity(context_sidecar_path=context_sidecar_path, dataset_row_ids=dataset_row_ids)
    api_key_provided, auth_mode = compute_openalex_auth_artifact_fields(mock_openalex=mock_openalex)
    contact_mode, contact_provided = compute_contact_provenance(mailto_cli=mailto or "", mock_openalex=mock_openalex)

    row_records = [
        _build_row_record(
            row=row,
            mailto=mailto,
            mock_openalex=mock_openalex,
            fetch_json=fetch_json,
        )
        for row in external_rows
    ]

    fetch_ok = [row for row in row_records if row["provenance"].get("error_class") is None]
    fetch_failed = [row for row in row_records if row["provenance"].get("error_class") is not None]
    empty_abstract = [row for row in fetch_ok if not _norm_ws(row["hydrated"].get("full_abstract"))]
    sufficient = [row for row in row_records if row.get("sufficient_text_for_embedding_heuristic")]
    summary = {
        "n_rows": len(row_records),
        "n_fetch_ok": len(fetch_ok),
        "n_fetch_failed": len(fetch_failed),
        "n_empty_abstract": len(empty_abstract),
        "n_preview_fallback": sum(1 for row in row_records if row.get("abstract_source") == "preview_fallback"),
        "n_fetch_failed_with_preview_fallback_text": sum(
            1
            for row in row_records
            if row.get("abstract_source") == "fetch_failed" and row.get("text_fallback_source")
        ),
        "n_sufficient_text_heuristic_true": len(sufficient),
    }

    return {
        "metadata": {
            "artifact_type": "ml_external_text_corpus",
            "corpus_version": CORPUS_VERSION,
            "dataset_version_reference": dataset_version,
            "generated_at": _now_iso_z(),
            "label_dataset_path": portable_repo_path(path),
            "label_dataset_sha256": label_sha,
            "context_sidecar": sidecar,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "strict_expected_external_row_count": expected_external_rows,
            "openalex_endpoint": OPENALEX_WORKS_URL,
            "openalex_select_fields": list(OPENALEX_SELECT_FIELDS),
            "openalex_auth_artifact_fields": {
                "api_key_provided": api_key_provided,
                "auth_mode": auth_mode,
                "contact_mode": contact_mode,
                "contact_provided": contact_provided,
                "mock_openalex": mock_openalex,
            },
            "summary": summary,
            "caveats": list(CAVEATS),
        },
        "rows": row_records,
    }


def render_markdown(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    summary = metadata["summary"]
    sidecar = metadata.get("context_sidecar")
    lines = [
        "# External Near-Miss Text Corpus",
        "",
        "Read-only OpenAlex text hydration artifact for `ml_external_near_miss_audit` rows. This prepares text for offline featureization only: no Postgres writes, no ranking run, and no model training.",
        "",
        "## Provenance",
        "",
        f"- **corpus_version:** `{metadata.get('corpus_version')}`",
        f"- **label_dataset:** `{metadata.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{metadata.get('label_dataset_sha256')}`",
        f"- **dataset_version_reference:** `{metadata.get('dataset_version_reference')}`",
        f"- **review_pool_variant:** `{metadata.get('review_pool_variant')}`",
        f"- **OpenAlex endpoint:** `{metadata.get('openalex_endpoint')}`",
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
            "## Summary",
            "",
            f"- **rows:** `{summary.get('n_rows')}`",
            f"- **fetch OK:** `{summary.get('n_fetch_ok')}`",
            f"- **fetch failed:** `{summary.get('n_fetch_failed')}`",
            f"- **empty OpenAlex abstracts:** `{summary.get('n_empty_abstract')}`",
            f"- **preview fallback after successful fetch:** `{summary.get('n_preview_fallback')}`",
            f"- **sufficient text heuristic true:** `{summary.get('n_sufficient_text_heuristic_true')}`",
            "",
            "## Offline Embedding Note",
            "",
            "For a future offline embedding pass, read `rows[].text_for_embedding` and keep `rows[].review_pool_variant` for stratified reporting rather than treating the pool as a label. Rows with `abstract_source=preview_fallback` or `fetch_failed` should be interpreted as preview-derived text, not confirmed full abstracts.",
            "",
            "## Caveats",
            "",
            *[f"- {c}" for c in metadata.get("caveats", [])],
            "",
        ]
    )
    return "\n".join(lines)


def run_ml_external_text_corpus_cli(
    *,
    label_dataset_path: Path,
    context_sidecar_path: Path | None,
    output_json: Path,
    markdown_output: Path | None,
    mailto: str | None,
    mock_openalex: bool,
) -> dict[str, Any]:
    payload = build_external_text_corpus_payload(
        label_dataset_path=label_dataset_path,
        context_sidecar_path=context_sidecar_path,
        mailto=mailto,
        mock_openalex=mock_openalex,
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    if markdown_output is not None:
        markdown_output.parent.mkdir(parents=True, exist_ok=True)
        markdown_output.write_text(render_markdown(payload), encoding="utf-8")
    return payload


__all__ = [
    "CORPUS_VERSION",
    "EXPECTED_EXTERNAL_ROWS",
    "MLExternalTextCorpusError",
    "REVIEW_POOL_VARIANT",
    "build_external_text_corpus_payload",
    "render_markdown",
    "run_ml_external_text_corpus_cli",
    "select_external_rows",
]
