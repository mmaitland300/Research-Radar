"""Frozen embedding artifact for the labeled text corpus.

This vectorizes `ml_labeled_text_corpus` rows only. It does not read/write
Postgres, run ranking, create splits, or train a model.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.embedding_provider import (
    DEFAULT_OPENAI_BASE_URL,
    DEFAULT_OPENAI_EMBEDDING_MODEL,
    EXPECTED_EMBEDDING_DIMENSIONS,
    EmbeddingProvider,
    openai_embedding_provider_from_env,
)
from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_labeled_text_embeddings"
EMBEDDING_ARTIFACT_VERSION = "ml-labeled-text-embeddings-v1"
SOURCE_ARTIFACT_TYPE = "ml_labeled_text_corpus"
SOURCE_CORPUS_VERSION = "ml-labeled-text-corpus-v1"
DEFAULT_BATCH_SIZE = 16

CAVEATS = (
    "Not validation.",
    "Frozen vectorization of frozen labeled text corpus only.",
    "Vectors are not production ranking signals.",
    "No Postgres reads or writes.",
    "No ranking, no train/dev/test split generation, no production behavior change.",
    "Mixed text formats may exist across rows; downstream diagnostics must account for embedding_text_format_version.",
    "Future cross-pool baselines must document whether they pool or stratify by review_pool_variant, family, and text format.",
)

LAYERING_NOTE = (
    "Layering: ml-labeled-text-corpus-v1 freezes observation-level text; "
    "ml-labeled-text-embeddings-v1 freezes vectors for that exact text; later source-transfer "
    "or cross-pool baselines consume this artifact plus labels offline only."
)


class MLLabeledTextEmbeddingsError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLLabeledTextEmbeddingsError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLLabeledTextEmbeddingsError(f"Expected JSON object in {path}")
    return payload


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _mock_vector(*, row_id: str, text_sha256: str, dimensions: int) -> list[float]:
    seed = f"{row_id}|{text_sha256}".encode("utf-8")
    out: list[float] = []
    counter = 0
    while len(out) < dimensions:
        digest = hashlib.sha256(seed + b"|" + str(counter).encode("ascii")).digest()
        counter += 1
        for i in range(0, len(digest), 4):
            if len(out) >= dimensions:
                break
            chunk = digest[i : i + 4]
            if len(chunk) < 4:
                continue
            intval = int.from_bytes(chunk, "big")
            out.append(round((intval / 0xFFFFFFFF) * 2.0 - 1.0, 8))
    return out


def validate_text_corpus_payload(
    payload: Mapping[str, Any],
    *,
    expected_source_corpus_version: str = SOURCE_CORPUS_VERSION,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, dict):
        raise MLLabeledTextEmbeddingsError("labeled text corpus missing metadata object")
    if not isinstance(rows, list):
        raise MLLabeledTextEmbeddingsError("labeled text corpus missing rows array")
    if metadata.get("artifact_type") != SOURCE_ARTIFACT_TYPE:
        raise MLLabeledTextEmbeddingsError(
            f"expected metadata.artifact_type={SOURCE_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("corpus_version") != expected_source_corpus_version:
        raise MLLabeledTextEmbeddingsError(
            "expected metadata.corpus_version="
            f"{expected_source_corpus_version!r}, got {metadata.get('corpus_version')!r}"
        )
    expected = metadata.get("row_count")
    if isinstance(expected, int) and len(rows) != expected:
        raise MLLabeledTextEmbeddingsError(f"row count {len(rows)} does not match metadata.row_count {expected}")
    if not rows:
        raise MLLabeledTextEmbeddingsError("labeled text corpus rows array is empty")

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    dupes: list[str] = []
    empty_text: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise MLLabeledTextEmbeddingsError(f"labeled text corpus row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLLabeledTextEmbeddingsError(f"labeled text corpus row {idx} missing row_id")
        if row_id in seen:
            dupes.append(row_id)
        seen.add(row_id)
        text = row.get("text_for_embedding")
        if not isinstance(text, str) or not text.strip():
            empty_text.append(row_id)
        normalized.append(row)
    if dupes:
        raise MLLabeledTextEmbeddingsError(f"labeled text corpus contains duplicate row_id values: {sorted(set(dupes))[:10]}")
    if empty_text:
        raise MLLabeledTextEmbeddingsError(f"text_for_embedding missing/empty for row_id values: {empty_text[:20]}")
    return metadata, sorted(normalized, key=lambda row: str(row["row_id"]))


def _batch(items: Sequence[dict[str, Any]], size: int) -> list[Sequence[dict[str, Any]]]:
    if size < 1:
        raise MLLabeledTextEmbeddingsError("--batch-size must be >= 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


def _openai_auth_artifact_fields(*, mock_embeddings: bool) -> dict[str, Any]:
    if mock_embeddings:
        return {"api_key_provided": False, "auth_mode": "mock"}
    api_key_provided = bool(os.environ.get("OPENAI_API_KEY"))
    return {"api_key_provided": api_key_provided, "auth_mode": "api_key" if api_key_provided else "no_key"}


def build_ml_labeled_text_embeddings_payload(
    *,
    text_corpus_path: Path,
    source_corpus_version: str = SOURCE_CORPUS_VERSION,
    embedding_artifact_version: str = EMBEDDING_ARTIFACT_VERSION,
    embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    mock_embeddings: bool = False,
    provider: EmbeddingProvider | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    corpus_path = Path(text_corpus_path)
    corpus_payload = _load_json_object(corpus_path)
    try:
        source_sha = sha256_file(corpus_path)
    except OSError as exc:
        raise MLLabeledTextEmbeddingsError(f"Failed to hash text corpus {corpus_path}: {exc}") from exc
    source_metadata, corpus_rows = validate_text_corpus_payload(
        corpus_payload,
        expected_source_corpus_version=source_corpus_version,
    )

    row_inputs: list[dict[str, Any]] = []
    for row in corpus_rows:
        text = str(row["text_for_embedding"])
        text_sha = _sha256_text(text)
        row_inputs.append({"row": row, "text": text, "text_sha256": text_sha})

    aggregate_lines = "".join(f"{item['row']['row_id']}\0{item['text_sha256']}\n" for item in row_inputs)
    aggregate_input_text_sha256 = hashlib.sha256(aggregate_lines.encode("utf-8")).hexdigest()

    if mock_embeddings:
        vectors = [
            _mock_vector(
                row_id=str(item["row"]["row_id"]),
                text_sha256=str(item["text_sha256"]),
                dimensions=expected_dimensions,
            )
            for item in row_inputs
        ]
        statuses = ["mock"] * len(vectors)
    else:
        try:
            active_provider = provider or openai_embedding_provider_from_env(
                model=embedding_model,
                expected_dimensions=expected_dimensions,
            )
        except Exception as exc:
            raise MLLabeledTextEmbeddingsError(f"embedding provider initialization failed: {exc}") from exc
        vectors = []
        statuses = []
        for chunk in _batch(row_inputs, batch_size):
            try:
                chunk_vectors = active_provider.embed_texts([str(item["text"]) for item in chunk])
            except Exception as exc:
                raise MLLabeledTextEmbeddingsError(f"embedding provider failed: {exc}") from exc
            if len(chunk_vectors) != len(chunk):
                raise MLLabeledTextEmbeddingsError(
                    f"embedding provider returned {len(chunk_vectors)} vectors for batch of {len(chunk)}"
                )
            for item, vector in zip(chunk, chunk_vectors, strict=True):
                if len(vector) != expected_dimensions:
                    raise MLLabeledTextEmbeddingsError(
                        f"embedding dimension mismatch for row_id {item['row']['row_id']}: "
                        f"expected {expected_dimensions}, got {len(vector)}"
                    )
                vectors.append([float(v) for v in vector])
                statuses.append("ok")

    output_rows: list[dict[str, Any]] = []
    for item, vector, status in zip(row_inputs, vectors, statuses, strict=True):
        row = item["row"]
        text = str(item["text"])
        output_rows.append(
            {
                "row_id": row.get("row_id"),
                "paper_id": row.get("paper_id"),
                "openalex_work_id": row.get("openalex_work_id"),
                "work_id": row.get("work_id"),
                "review_pool_variant": row.get("review_pool_variant"),
                "family": row.get("family"),
                "ranking_run_id": row.get("ranking_run_id"),
                "embedding_text_format_version": row.get("embedding_text_format_version"),
                "text_source": row.get("text_source"),
                "text_sha256": item["text_sha256"],
                "text_length": len(text),
                "embedding": vector,
                "embedding_status": status,
            }
        )

    counts_by_variant = Counter(str(row.get("review_pool_variant") or "(null)") for row in output_rows)
    counts_by_family = Counter(str(row.get("family") or "(null)") for row in output_rows)
    counts_by_format = Counter(str(row.get("embedding_text_format_version") or "(null)") for row in output_rows)
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "embedding_artifact_version": embedding_artifact_version,
        "generated_at": generated_at or _now_iso_z(),
        "source_text_corpus_path": portable_repo_path(corpus_path),
        "source_text_corpus_sha256": source_sha,
        "source_text_corpus_version": source_metadata.get("corpus_version"),
        "source_label_dataset_sha256": source_metadata.get("label_dataset_sha256")
        or source_metadata.get("source_label_dataset_sha256"),
        "source_label_dataset_version": source_metadata.get("label_dataset_version")
        or source_metadata.get("source_label_dataset_version"),
        "embedding_model": embedding_model,
        "embedding_dimensions": expected_dimensions,
        "embedding_provider": "openai",
        "openai_base_url": os.environ.get("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL),
        "openai_auth_artifact_fields": _openai_auth_artifact_fields(mock_embeddings=mock_embeddings),
        "aggregate_input_text_sha256": aggregate_input_text_sha256,
        "row_count": len(output_rows),
        "n_embedded_ok": sum(1 for row in output_rows if row["embedding_status"] == "ok"),
        "n_mock": sum(1 for row in output_rows if row["embedding_status"] == "mock"),
        "counts_by_review_pool_variant": dict(sorted(counts_by_variant.items())),
        "counts_by_family": dict(sorted(counts_by_family.items())),
        "counts_by_embedding_text_format_version": dict(sorted(counts_by_format.items())),
        "caveats": list(CAVEATS),
        "layering_note": (
            f"Layering: {source_corpus_version} freezes observation-level text; "
            f"{embedding_artifact_version} freezes vectors for that exact text; later source-transfer "
            "or cross-pool baselines consume this artifact plus labels offline only."
        ),
    }
    return {"metadata": metadata, "rows": output_rows}


def render_markdown(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    lines = [
        f"# Labeled Text Embeddings ({metadata.get('embedding_artifact_version')})",
        "",
        f"Frozen vectorization artifact for `{metadata.get('source_text_corpus_version')}`.",
        "",
        "## Summary",
        "",
        f"- **source_text_corpus_path:** `{metadata.get('source_text_corpus_path')}`",
        f"- **source_text_corpus_sha256:** `{metadata.get('source_text_corpus_sha256')}`",
        f"- **embedding_artifact_version:** `{metadata.get('embedding_artifact_version')}`",
        f"- **embedding_model:** `{metadata.get('embedding_model')}`",
        f"- **embedding_dimensions:** `{metadata.get('embedding_dimensions')}`",
        f"- **row_count:** `{metadata.get('row_count')}`",
        f"- **n_embedded_ok:** `{metadata.get('n_embedded_ok')}`",
        f"- **n_mock:** `{metadata.get('n_mock')}`",
        f"- **aggregate_input_text_sha256:** `{metadata.get('aggregate_input_text_sha256')}`",
        "",
        "## Review Pool Counts",
        "",
        *[f"- `{k}`: `{v}`" for k, v in metadata.get("counts_by_review_pool_variant", {}).items()],
        "",
        "## Text Format Counts",
        "",
        *[f"- `{k}`: `{v}`" for k, v in metadata.get("counts_by_embedding_text_format_version", {}).items()],
        "",
        "## Layering",
        "",
        str(metadata.get("layering_note")),
        "",
        "## Caveats",
        "",
        *[f"- {caveat}" for caveat in metadata.get("caveats", [])],
        "",
        "Embedding vectors are intentionally omitted from this Markdown file; see the JSON artifact for vectors.",
        "",
    ]
    return "\n".join(lines)


def write_ml_labeled_text_embeddings(
    *,
    text_corpus_path: Path,
    output_path: Path,
    markdown_output_path: Path | None,
    source_corpus_version: str = SOURCE_CORPUS_VERSION,
    embedding_artifact_version: str = EMBEDDING_ARTIFACT_VERSION,
    embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    mock_embeddings: bool = False,
    provider: EmbeddingProvider | None = None,
) -> dict[str, Any]:
    payload = build_ml_labeled_text_embeddings_payload(
        text_corpus_path=text_corpus_path,
        source_corpus_version=source_corpus_version,
        embedding_artifact_version=embedding_artifact_version,
        embedding_model=embedding_model,
        expected_dimensions=expected_dimensions,
        batch_size=batch_size,
        mock_embeddings=mock_embeddings,
        provider=provider,
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
    "CAVEATS",
    "EMBEDDING_ARTIFACT_VERSION",
    "LAYERING_NOTE",
    "MLLabeledTextEmbeddingsError",
    "SOURCE_CORPUS_VERSION",
    "build_ml_labeled_text_embeddings_payload",
    "render_markdown",
    "validate_text_corpus_payload",
    "write_ml_labeled_text_embeddings",
]
