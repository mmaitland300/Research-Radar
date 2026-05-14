"""Canonical text-format normalization for labeled text corpus artifacts.

This is a file-in/file-out data-prep layer only. It does not call OpenAlex,
read/write Postgres, generate embeddings, run ranking, train models, or create
split artifacts.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_labeled_text_corpus"
SOURCE_CORPUS_VERSION = "ml-labeled-text-corpus-v1"
CORPUS_VERSION = "ml-labeled-text-corpus-v2"
FORMAT_V2_CANONICAL = "labeled_text_corpus_v2_canonical_title_abstract"

STATUS_CANONICAL_TITLE_ABSTRACT = "canonical_title_abstract"
STATUS_ORIGINAL_TEXT_FALLBACK = "original_text_fallback"
STATUS_MISSING_TEXT = "missing_text"

CAVEATS = (
    "Not validation.",
    "Normalization only; labels unchanged; duplicates/conflicts preserved.",
    "Rows in original_text_fallback still carry v1 format limits; missing_text rows cannot be de-confounded by this pass.",
    "Not product ranking quality evidence.",
)

LAYERING_NOTE = (
    "Layering: ml-labeled-text-corpus-v1 freezes observation-level hydrated text; "
    "ml-labeled-text-corpus-v2 normalizes text_for_embedding into a canonical title+abstract string where available; "
    "future ml-labeled-text-embeddings-v2 and cross-pool diagnostics can use this as a text-format sensitivity control."
)


class MLLabeledTextCorpusNormalizeError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLLabeledTextCorpusNormalizeError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLLabeledTextCorpusNormalizeError(f"Expected JSON object in {path}")
    return payload


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _strip(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _bucket(value: Any) -> str:
    text = _strip(value)
    return text if text else "(null)"


def validate_source_corpus_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, dict):
        raise MLLabeledTextCorpusNormalizeError("source corpus missing metadata object")
    if not isinstance(rows, list):
        raise MLLabeledTextCorpusNormalizeError("source corpus missing rows array")
    if metadata.get("artifact_type") != ARTIFACT_TYPE:
        raise MLLabeledTextCorpusNormalizeError(
            f"expected metadata.artifact_type={ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("corpus_version") != SOURCE_CORPUS_VERSION:
        raise MLLabeledTextCorpusNormalizeError(
            f"expected metadata.corpus_version={SOURCE_CORPUS_VERSION!r}, got {metadata.get('corpus_version')!r}"
        )
    expected = metadata.get("row_count")
    if not isinstance(expected, int):
        raise MLLabeledTextCorpusNormalizeError("source corpus metadata.row_count must be an integer")
    if len(rows) != expected:
        raise MLLabeledTextCorpusNormalizeError(f"row count {len(rows)} does not match metadata.row_count {expected}")

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    dupes: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise MLLabeledTextCorpusNormalizeError(f"source corpus row {idx} is not an object")
        row = dict(raw)
        row_id = _strip(row.get("row_id"))
        if not row_id:
            raise MLLabeledTextCorpusNormalizeError(f"source corpus row {idx} missing row_id")
        if row_id in seen:
            dupes.append(row_id)
        seen.add(row_id)
        normalized.append(row)
    if dupes:
        raise MLLabeledTextCorpusNormalizeError(
            f"source corpus contains duplicate row_id values: {sorted(set(dupes))[:10]}"
        )
    return metadata, sorted(normalized, key=lambda row: str(row["row_id"]))


def _canonical_title(row: Mapping[str, Any]) -> str:
    for key in ("hydrated_title", "title", "work_id", "openalex_work_id", "paper_id"):
        text = _strip(row.get(key))
        if text:
            return text
    return ""


def _canonical_abstract(row: Mapping[str, Any]) -> str:
    return _strip(row.get("hydrated_abstract"))


def normalize_labeled_text_corpus_row(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    previous_text = row.get("text_for_embedding")
    title = _canonical_title(row)
    abstract = _canonical_abstract(row)

    if title and abstract:
        text = f"{title}\n\n{abstract}"
        status = STATUS_CANONICAL_TITLE_ABSTRACT
    elif isinstance(previous_text, str) and previous_text != "":
        text = previous_text
        status = STATUS_ORIGINAL_TEXT_FALLBACK
    else:
        text = ""
        status = STATUS_MISSING_TEXT

    out["previous_text_source"] = row.get("text_source")
    out["previous_text_sha256"] = row.get("text_sha256")
    out["previous_text_length"] = row.get("text_length")
    out["previous_embedding_text_format_version"] = row.get("embedding_text_format_version")
    out["canonicalization_status"] = status
    out["embedding_text_format_version"] = FORMAT_V2_CANONICAL
    out["text_for_embedding"] = text
    out["text_sha256"] = _sha256_text(text)
    out["text_length"] = len(text)
    out["sufficient_text_for_embedding_heuristic"] = len(text.strip()) >= 200
    return out


def build_ml_labeled_text_corpus_normalize_payload(
    *,
    source_corpus_path: Path,
    generated_at: str | None = None,
) -> dict[str, Any]:
    source_path = Path(source_corpus_path).resolve()
    source_payload = _load_json_object(source_path)
    try:
        source_sha = sha256_file(source_path)
    except OSError as exc:
        raise MLLabeledTextCorpusNormalizeError(f"Failed to hash source corpus {source_path}: {exc}") from exc
    source_metadata, source_rows = validate_source_corpus_payload(source_payload)

    output_rows = [normalize_labeled_text_corpus_row(row) for row in source_rows]

    by_variant = Counter(_bucket(row.get("review_pool_variant")) for row in output_rows)
    by_family = Counter(_bucket(row.get("family")) for row in output_rows)
    by_previous_source = Counter(_bucket(row.get("previous_text_source")) for row in output_rows)
    by_previous_format = Counter(_bucket(row.get("previous_embedding_text_format_version")) for row in output_rows)
    by_status = Counter(_bucket(row.get("canonicalization_status")) for row in output_rows)
    by_text_source = Counter(_bucket(row.get("text_source")) for row in output_rows)
    n_changed = sum(
        1 for row in output_rows if row.get("text_sha256") != row.get("previous_text_sha256")
    )

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "corpus_version": CORPUS_VERSION,
        "generated_at": generated_at or _now_iso_z(),
        "source_corpus_path": portable_repo_path(source_path),
        "source_corpus_sha256": source_sha,
        "source_corpus_version": source_metadata.get("corpus_version"),
        "source_label_dataset_sha256": source_metadata.get("label_dataset_sha256"),
        "source_label_dataset_version": source_metadata.get("label_dataset_version"),
        "row_count": len(output_rows),
        "counts_by_review_pool_variant": dict(sorted(by_variant.items())),
        "counts_by_family": dict(sorted(by_family.items())),
        "counts_by_previous_text_source": dict(sorted(by_previous_source.items())),
        "counts_by_previous_embedding_text_format_version": dict(sorted(by_previous_format.items())),
        "counts_by_canonicalization_status": dict(sorted(by_status.items())),
        "counts_by_text_source": dict(sorted(by_text_source.items())),
        "n_sufficient_text_for_embedding_heuristic": sum(
            1 for row in output_rows if row["sufficient_text_for_embedding_heuristic"]
        ),
        "n_text_changed_from_v1": n_changed,
        "caveats": list(CAVEATS),
        "layering_note": LAYERING_NOTE,
    }
    return {"metadata": metadata, "rows": output_rows}


def render_markdown(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    lines = [
        "# Labeled Text Corpus v2",
        "",
        "Canonical text-format normalization for `ml-labeled-text-corpus-v1`. This is data preparation only: no OpenAlex calls, no Postgres, no embeddings, no ranking, and no label edits.",
        "",
        "## Summary",
        "",
        f"- **source_corpus_path:** `{metadata.get('source_corpus_path')}`",
        f"- **source_corpus_sha256:** `{metadata.get('source_corpus_sha256')}`",
        f"- **corpus_version:** `{metadata.get('corpus_version')}`",
        f"- **row_count:** `{metadata.get('row_count')}`",
        f"- **n_text_changed_from_v1:** `{metadata.get('n_text_changed_from_v1')}`",
        f"- **n_sufficient_text_for_embedding_heuristic:** `{metadata.get('n_sufficient_text_for_embedding_heuristic')}`",
        "",
        "## Canonicalization Status",
        "",
        *[f"- `{key}`: `{value}`" for key, value in metadata.get("counts_by_canonicalization_status", {}).items()],
        "",
        "## Previous Text Format Counts",
        "",
        *[
            f"- `{key}`: `{value}`"
            for key, value in metadata.get("counts_by_previous_embedding_text_format_version", {}).items()
        ],
        "",
        "## Previous Text Source Counts",
        "",
        *[f"- `{key}`: `{value}`" for key, value in metadata.get("counts_by_previous_text_source", {}).items()],
        "",
        "## Layering",
        "",
        str(metadata.get("layering_note")),
        "",
        "## Caveats",
        "",
        *[f"- {caveat}" for caveat in metadata.get("caveats", [])],
        "",
        "Full abstracts are intentionally omitted from this Markdown summary; see the JSON artifact for row-level text.",
        "",
    ]
    return "\n".join(lines)


def write_ml_labeled_text_corpus_normalize(
    *,
    source_corpus_path: Path,
    output_path: Path,
    markdown_output_path: Path | None,
) -> dict[str, Any]:
    payload = build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=source_corpus_path)
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
    "CAVEATS",
    "CORPUS_VERSION",
    "FORMAT_V2_CANONICAL",
    "LAYERING_NOTE",
    "MLLabeledTextCorpusNormalizeError",
    "SOURCE_CORPUS_VERSION",
    "build_ml_labeled_text_corpus_normalize_payload",
    "normalize_labeled_text_corpus_row",
    "render_markdown",
    "validate_source_corpus_payload",
    "write_ml_labeled_text_corpus_normalize",
]
