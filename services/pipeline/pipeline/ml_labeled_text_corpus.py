"""Observation-level text corpus for all explicitly labeled audit rows.

This artifact is a data-prep layer only: it hydrates or reuses text for each
human-labeled audit observation without embeddings, ranking, Postgres access,
splits, label inference, or conflict resolution.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.openalex_ids import normalize_w_token
from pipeline.openalex import OPENALEX_WORKS_URL, build_work_select_clause
from pipeline.openalex_client import (
    compute_contact_provenance,
    compute_openalex_auth_artifact_fields,
    fetch_openalex_json,
)
from pipeline.openalex_text import abstract_plain_text, clean_openalex_text
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_labeled_text_corpus"
CORPUS_VERSION = "ml-labeled-text-corpus-v1"
EXTERNAL_TEXT_CORPUS_ARTIFACT_TYPE = "ml_external_text_corpus"
EXTERNAL_TEXT_CORPUS_VERSION = "ml-external-text-corpus-v7"
OPENALEX_SELECT_FIELDS: tuple[str, ...] = build_work_select_clause()

TEXT_SOURCE_EXTERNAL_REUSE = "external_text_corpus_reuse"
TEXT_SOURCE_OPENALEX_FETCH = "openalex_fetch"
TEXT_SOURCE_LABEL_PREVIEW_FALLBACK = "label_preview_fallback"
TEXT_SOURCE_FETCH_FAILED = "fetch_failed"
TEXT_SOURCE_MISSING_WORK_ID = "missing_work_id"

FORMAT_EXTERNAL_REUSE = "external_text_corpus_v7_verbatim"
FORMAT_OPENALEX_TITLE_ABSTRACT = "labeled_text_corpus_v1_openalex_title_abstract"
FORMAT_LABEL_PREVIEW_FALLBACK = "labeled_text_corpus_v1_label_preview_fallback"
FORMAT_FETCH_FAILED_FALLBACK = "labeled_text_corpus_v1_fetch_failed_fallback"
FORMAT_MISSING_WORK_ID_FALLBACK = "labeled_text_corpus_v1_missing_work_id_fallback"

CAVEATS = (
    "Not validation.",
    "Text corpus only; no embeddings or training.",
    "Observation-level rows preserved; duplicate paper_id and raw label conflicts are intentional evidence, not resolved here.",
    "OpenAlex metadata can drift for newly fetched rows; external reused rows remain frozen relative to ml-external-text-corpus-v7.",
    "Mixed text formats may exist across sources; future embedding artifacts must document format_version per row or globally.",
)

LAYERING_NOTE = (
    "Layering: ml-label-dataset-* supplies observation-level labels; ml-external-text-corpus-* supplies frozen "
    "external text reuse; ml-labeled-text-corpus-v1 freezes a labeled text substrate for future "
    "ml-labeled-text-embeddings-v1 and cross-pool offline diagnostics."
)

FetchOpenAlexJson = Callable[[str], Mapping[str, Any]]


class MLLabeledTextCorpusError(Exception):
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
        raise MLLabeledTextCorpusError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLLabeledTextCorpusError(f"Expected JSON object in {path}")
    return payload


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _row_work_token(row: Mapping[str, Any]) -> str | None:
    for key in ("work_id", "openalex_work_id", "paper_id"):
        token = normalize_w_token(_norm_ws(row.get(key)))
        if token:
            return token
    return None


def _work_api_url(work_token: str) -> str:
    params = {"select": ",".join(OPENALEX_SELECT_FIELDS)}
    return f"{OPENALEX_WORKS_URL}/{work_token}?{urlencode(params)}"


def _context_review_preview(row: Mapping[str, Any]) -> str:
    for context_key in ("external_near_miss_context", "blind_snapshot_context", "hard_negative_context"):
        ctx = row.get(context_key)
        if not isinstance(ctx, Mapping):
            continue
        review = ctx.get("review_metadata")
        if isinstance(review, Mapping):
            preview = _norm_ws(review.get("abstract_preview"))
            if preview:
                return preview
    return ""


def _best_preview(row: Mapping[str, Any]) -> tuple[str, str]:
    label_preview = _norm_ws(row.get("abstract_preview"))
    context_preview = _context_review_preview(row)
    if label_preview:
        return label_preview, "label_row.abstract_preview"
    if context_preview:
        return context_preview, "nested_context.review_metadata.abstract_preview"
    return "", "none"


def _fallback_text(row: Mapping[str, Any]) -> tuple[str, str]:
    title = _norm_ws(row.get("title"))
    preview, preview_source = _best_preview(row)
    if title and preview:
        return f"{title}\n\n{preview}", preview_source
    if title:
        return title, "label_row.title"
    if preview:
        return preview, preview_source
    return "", "none"


def _abstract_to_inverted_index(text: str) -> dict[str, list[int]]:
    index: dict[str, list[int]] = {}
    for i, word in enumerate(_norm_ws(text).split()):
        index.setdefault(word, []).append(i)
    return index


def _mock_work_from_row(row: Mapping[str, Any], work_token: str) -> dict[str, Any]:
    text, _source = _fallback_text(row)
    title = _norm_ws(row.get("title")) or f"Mock title {work_token}"
    abstract = text.split("\n\n", 1)[1] if "\n\n" in text else f"Mock abstract for {work_token}."
    return {
        "id": f"https://openalex.org/{work_token}",
        "title": title,
        "publication_year": row.get("year"),
        "abstract_inverted_index": _abstract_to_inverted_index(abstract),
    }


def select_labeled_audit_rows(payload: Mapping[str, Any], *, max_rows: int | None = None) -> list[dict[str, Any]]:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLLabeledTextCorpusError("label dataset missing rows array")
    selected = [
        dict(row)
        for row in rows
        if isinstance(row, dict)
        and str(row.get("split") or "").strip() == "audit_only"
        and row_has_explicit_label(row)
    ]
    if max_rows is not None:
        if max_rows < 1:
            raise MLLabeledTextCorpusError("--max-rows must be >= 1 when provided")
        selected = selected[:max_rows]
    return selected


def _external_rows_by_id(path: Path | None) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None, str | None]:
    if path is None:
        return {}, None, None
    resolved = path.resolve()
    payload = _load_json_object(resolved)
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, dict) or metadata.get("artifact_type") != EXTERNAL_TEXT_CORPUS_ARTIFACT_TYPE:
        raise MLLabeledTextCorpusError(
            f"external text corpus must have metadata.artifact_type={EXTERNAL_TEXT_CORPUS_ARTIFACT_TYPE!r}"
        )
    if not isinstance(rows, list):
        raise MLLabeledTextCorpusError("external text corpus missing rows array")
    by_id: dict[str, dict[str, Any]] = {}
    dupes: list[str] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row_id = _norm_ws(row.get("row_id"))
        if not row_id:
            continue
        if row_id in by_id:
            dupes.append(row_id)
        by_id[row_id] = row
    if dupes:
        raise MLLabeledTextCorpusError(f"external text corpus contains duplicate row_id values: {sorted(set(dupes))[:10]}")
    return by_id, metadata, sha256_file(resolved)


def _build_reuse_row(label_row: Mapping[str, Any], reuse_row: Mapping[str, Any]) -> dict[str, Any]:
    text = reuse_row.get("text_for_embedding")
    if not isinstance(text, str):
        raise MLLabeledTextCorpusError(f"external reuse row missing text_for_embedding: {_norm_ws(label_row.get('row_id'))}")
    existing_sha = reuse_row.get("text_sha256")
    text_sha = _sha256_text(text)
    if existing_sha and existing_sha != text_sha:
        raise MLLabeledTextCorpusError(
            f"external reuse text_sha256 mismatch for row_id {_norm_ws(label_row.get('row_id'))}: "
            f"{existing_sha!r} != {text_sha!r}"
        )
    hydrated = reuse_row.get("hydrated") if isinstance(reuse_row.get("hydrated"), Mapping) else {}
    return {
        "text_source": TEXT_SOURCE_EXTERNAL_REUSE,
        "embedding_text_format_version": FORMAT_EXTERNAL_REUSE,
        "hydrated_title": _norm_ws(hydrated.get("title")) or None,
        "hydrated_abstract": _norm_ws(hydrated.get("full_abstract")) or None,
        "text_for_embedding": text,
        "openalex_api_url_used": None,
        "retrieved_at": None,
        "fetch_error_class": None,
        "fetch_error_message": None,
    }


def _build_fetch_row(
    label_row: Mapping[str, Any],
    *,
    work_token: str,
    mailto: str | None,
    mock_openalex: bool,
    fetch_json: FetchOpenAlexJson | None,
    fetch_cache: dict[str, tuple[dict[str, Any] | None, dict[str, Any]]],
) -> dict[str, Any]:
    if work_token in fetch_cache:
        work, provenance = fetch_cache[work_token]
    else:
        url = _work_api_url(work_token)
        retrieved_at = _now_iso_z()
        if mock_openalex:
            work = _mock_work_from_row(label_row, work_token)
            provenance = {
                "openalex_api_url_used": url,
                "retrieved_at": retrieved_at,
                "fetch_error_class": None,
                "fetch_error_message": None,
            }
        else:
            try:
                work_raw = fetch_json(url) if fetch_json is not None else fetch_openalex_json(url, mailto=mailto, timeout_sec=60.0)
                work = dict(work_raw)
                provenance = {
                    "openalex_api_url_used": url,
                    "retrieved_at": retrieved_at,
                    "fetch_error_class": None,
                    "fetch_error_message": None,
                }
            except Exception as exc:
                work = None
                provenance = {
                    "openalex_api_url_used": url,
                    "retrieved_at": retrieved_at,
                    "fetch_error_class": type(exc).__name__,
                    "fetch_error_message": str(exc)[:500],
                }
        fetch_cache[work_token] = (work, provenance)

    if work is None:
        text, _source = _fallback_text(label_row)
        return {
            "text_source": TEXT_SOURCE_FETCH_FAILED,
            "embedding_text_format_version": FORMAT_FETCH_FAILED_FALLBACK,
            "hydrated_title": None,
            "hydrated_abstract": None,
            "text_for_embedding": text,
            **provenance,
        }

    title = clean_openalex_text(_norm_ws(work.get("title"))) or _norm_ws(label_row.get("title"))
    abstract = abstract_plain_text(work)
    if abstract:
        return {
            "text_source": TEXT_SOURCE_OPENALEX_FETCH,
            "embedding_text_format_version": FORMAT_OPENALEX_TITLE_ABSTRACT,
            "hydrated_title": title or None,
            "hydrated_abstract": abstract,
            "text_for_embedding": f"{title}\n\n{abstract}".strip(),
            **provenance,
        }
    text, _source = _fallback_text(label_row)
    return {
        "text_source": TEXT_SOURCE_LABEL_PREVIEW_FALLBACK,
        "embedding_text_format_version": FORMAT_LABEL_PREVIEW_FALLBACK,
        "hydrated_title": title or None,
        "hydrated_abstract": None,
        "text_for_embedding": text,
        **provenance,
    }


def _build_missing_work_id_row(label_row: Mapping[str, Any]) -> dict[str, Any]:
    text, _source = _fallback_text(label_row)
    return {
        "text_source": TEXT_SOURCE_MISSING_WORK_ID,
        "embedding_text_format_version": FORMAT_MISSING_WORK_ID_FALLBACK,
        "hydrated_title": None,
        "hydrated_abstract": None,
        "text_for_embedding": text,
        "openalex_api_url_used": None,
        "retrieved_at": None,
        "fetch_error_class": None,
        "fetch_error_message": None,
    }


def _output_row(label_row: Mapping[str, Any], text_payload: Mapping[str, Any]) -> dict[str, Any]:
    text = str(text_payload.get("text_for_embedding") or "")
    return {
        "row_id": _norm_ws(label_row.get("row_id")),
        "dataset_version": label_row.get("dataset_version"),
        "review_pool_variant": label_row.get("review_pool_variant"),
        "source_worksheet_path": label_row.get("source_worksheet_path"),
        "paper_id": label_row.get("paper_id"),
        "openalex_work_id": label_row.get("openalex_work_id"),
        "work_id": label_row.get("work_id"),
        "family": label_row.get("family"),
        "ranking_run_id": label_row.get("ranking_run_id"),
        "relevance_label": label_row.get("relevance_label"),
        "novelty_label": label_row.get("novelty_label"),
        "bridge_like_label": label_row.get("bridge_like_label"),
        "reviewer_notes": label_row.get("reviewer_notes"),
        "good_or_acceptable": label_row.get("good_or_acceptable"),
        "surprising_or_useful": label_row.get("surprising_or_useful"),
        "bridge_like_yes_or_partial": label_row.get("bridge_like_yes_or_partial"),
        "text_source": text_payload.get("text_source"),
        "embedding_text_format_version": text_payload.get("embedding_text_format_version"),
        "hydrated_title": text_payload.get("hydrated_title"),
        "hydrated_abstract": text_payload.get("hydrated_abstract"),
        "text_for_embedding": text,
        "text_sha256": _sha256_text(text),
        "text_length": len(text),
        "sufficient_text_for_embedding_heuristic": len(text.strip()) >= 200,
        "openalex_api_url_used": text_payload.get("openalex_api_url_used"),
        "retrieved_at": text_payload.get("retrieved_at"),
        "fetch_error_class": text_payload.get("fetch_error_class"),
        "fetch_error_message": text_payload.get("fetch_error_message"),
    }


def build_ml_labeled_text_corpus_payload(
    *,
    label_dataset_path: Path,
    external_text_corpus_path: Path | None = None,
    corpus_version: str = CORPUS_VERSION,
    mailto: str | None = None,
    mock_openalex: bool = False,
    max_rows: int | None = None,
    fetch_json: FetchOpenAlexJson | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    label_path = Path(label_dataset_path).resolve()
    label_payload = _load_json_object(label_path)
    label_sha = sha256_file(label_path)
    label_dataset_version = _norm_ws(label_payload.get("dataset_version"))
    selected_rows = select_labeled_audit_rows(label_payload, max_rows=max_rows)

    external_by_id, external_meta, external_sha = _external_rows_by_id(external_text_corpus_path)
    fetch_cache: dict[str, tuple[dict[str, Any] | None, dict[str, Any]]] = {}
    output_rows: list[dict[str, Any]] = []
    for label_row in selected_rows:
        row_id = _norm_ws(label_row.get("row_id"))
        if row_id in external_by_id:
            text_payload = _build_reuse_row(label_row, external_by_id[row_id])
        else:
            work_token = _row_work_token(label_row)
            if work_token:
                text_payload = _build_fetch_row(
                    label_row,
                    work_token=work_token,
                    mailto=mailto,
                    mock_openalex=mock_openalex,
                    fetch_json=fetch_json,
                    fetch_cache=fetch_cache,
                )
            else:
                text_payload = _build_missing_work_id_row(label_row)
        output_rows.append(_output_row(label_row, text_payload))

    api_key_provided, auth_mode = compute_openalex_auth_artifact_fields(mock_openalex=mock_openalex)
    contact_mode, contact_provided = compute_contact_provenance(mailto_cli=mailto or "", mock_openalex=mock_openalex)
    by_variant = Counter(_norm_ws(row.get("review_pool_variant")) or "(null)" for row in output_rows)
    by_family = Counter(_norm_ws(row.get("family")) or "(null)" for row in output_rows)
    by_source = Counter(_norm_ws(row.get("text_source")) or "(null)" for row in output_rows)
    layering_note = (
        "Layering: ml-label-dataset-* supplies observation-level labels; ml-external-text-corpus-* supplies frozen "
        f"external text reuse; {corpus_version} freezes a labeled text substrate for future "
        "ml-labeled-text-embeddings-* and cross-pool offline diagnostics."
    )
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "corpus_version": corpus_version,
        "generated_at": generated_at or _now_iso_z(),
        "label_dataset_path": portable_repo_path(label_path),
        "label_dataset_sha256": label_sha,
        "label_dataset_version": label_dataset_version,
        "external_text_corpus_path": portable_repo_path(Path(external_text_corpus_path).resolve())
        if external_text_corpus_path is not None
        else None,
        "external_text_corpus_sha256": external_sha,
        "external_text_corpus_version": external_meta.get("corpus_version") if isinstance(external_meta, Mapping) else None,
        "row_count": len(output_rows),
        "counts_by_review_pool_variant": dict(sorted(by_variant.items())),
        "counts_by_family": dict(sorted(by_family.items())),
        "counts_by_text_source": dict(sorted(by_source.items())),
        "n_sufficient_text_for_embedding_heuristic": sum(
            1 for row in output_rows if row["sufficient_text_for_embedding_heuristic"]
        ),
        "max_rows": max_rows,
        "openalex_auth_artifact_fields": {
            "api_key_provided": api_key_provided,
            "auth_mode": auth_mode,
            "contact_mode": contact_mode,
            "contact_provided": contact_provided,
            "mock_openalex": mock_openalex,
        },
        "caveats": list(CAVEATS),
        "layering_note": layering_note,
    }
    return {"metadata": metadata, "rows": output_rows}


def render_markdown(payload: Mapping[str, Any]) -> str:
    meta = payload["metadata"]
    lines = [
        f"# Labeled Text Corpus ({meta.get('corpus_version')})",
        "",
        "Observation-level text corpus for explicitly labeled audit rows. This is data preparation only: no embeddings, no model training, no ranking, and no Postgres.",
        "",
        "## Inputs",
        "",
        f"- **label_dataset:** `{meta.get('label_dataset_path')}`",
        f"- **label_dataset_sha256:** `{meta.get('label_dataset_sha256')}`",
        f"- **label_dataset_version:** `{meta.get('label_dataset_version')}`",
        f"- **corpus_version:** `{meta.get('corpus_version')}`",
        f"- **external_text_corpus:** `{meta.get('external_text_corpus_path') or 'not provided'}`",
        f"- **external_text_corpus_sha256:** `{meta.get('external_text_corpus_sha256') or 'n/a'}`",
        f"- **row_count:** `{meta.get('row_count')}`",
        f"- **sufficient_text_heuristic:** `{meta.get('n_sufficient_text_for_embedding_heuristic')}`",
        "",
        "## Text Source Distribution",
        "",
        *[f"- `{k}`: `{v}`" for k, v in meta.get("counts_by_text_source", {}).items()],
        "",
        "## Review Pool Distribution",
        "",
        *[f"- `{k}`: `{v}`" for k, v in meta.get("counts_by_review_pool_variant", {}).items()],
        "",
        "## Intended Next Step",
        "",
        "Generate a matching `ml-labeled-text-embeddings-*` artifact from this frozen corpus, then run source-transfer or cross-pool offline diagnostics stratified by `review_pool_variant` and `family`.",
        "",
        "## Layering",
        "",
        str(meta.get("layering_note")),
        "",
        "## Caveats",
        "",
        *[f"- {caveat}" for caveat in meta.get("caveats", [])],
        "",
        "Full abstracts are intentionally omitted from this Markdown summary; see the JSON artifact for row-level text.",
        "",
    ]
    return "\n".join(lines)


def write_ml_labeled_text_corpus(
    *,
    label_dataset_path: Path,
    external_text_corpus_path: Path | None,
    output_path: Path,
    markdown_output_path: Path | None,
    corpus_version: str = CORPUS_VERSION,
    mailto: str | None = None,
    mock_openalex: bool = False,
    max_rows: int | None = None,
) -> dict[str, Any]:
    payload = build_ml_labeled_text_corpus_payload(
        label_dataset_path=label_dataset_path,
        external_text_corpus_path=external_text_corpus_path,
        corpus_version=corpus_version,
        mailto=mailto,
        mock_openalex=mock_openalex,
        max_rows=max_rows,
    )
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if markdown_output_path is not None:
        md = Path(markdown_output_path)
        md.parent.mkdir(parents=True, exist_ok=True)
        md.write_text(render_markdown(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "CORPUS_VERSION",
    "CAVEATS",
    "LAYERING_NOTE",
    "MLLabeledTextCorpusError",
    "build_ml_labeled_text_corpus_payload",
    "render_markdown",
    "select_labeled_audit_rows",
    "write_ml_labeled_text_corpus",
]
