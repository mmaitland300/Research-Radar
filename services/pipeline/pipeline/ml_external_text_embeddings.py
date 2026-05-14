"""Offline embedding artifact for external near-miss text corpus rows.

This module vectorizes the frozen `ml_external_text_corpus` artifact only. It
does not read or write Postgres, does not run ranking, and does not create ML
splits.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.embedding_provider import (
    DEFAULT_OPENAI_BASE_URL,
    DEFAULT_OPENAI_EMBEDDING_MODEL,
    EXPECTED_EMBEDDING_DIMENSIONS,
    EmbeddingProvider,
    openai_embedding_provider_from_env,
)
from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import portable_repo_path

ARTIFACT_TYPE = "ml_external_text_embeddings"
SOURCE_ARTIFACT_TYPE = "ml_external_text_corpus"
EMBEDDING_ARTIFACT_VERSION = "ml-external-text-embeddings-v7"
SOURCE_CORPUS_VERSION = "ml-external-text-corpus-v7"
REVIEW_POOL_VARIANT = "ml_external_near_miss_audit"
DEFAULT_BATCH_SIZE = 16

CAVEATS = (
    "Not validation.",
    "Frozen vectorization of frozen text corpus only; vectors are not production ranking signals.",
    "No Postgres reads or writes; not joinable to paper_scores unless a future explicit step adds ranked rows.",
    (
        "Verbatim text_for_embedding was embedded; cross-pool comparison to DB title+abstract format may require "
        "a documented transform in a later experiment, not silent relabeling here."
    ),
)

LAYERING_NOTE = (
    "Layering: ml-external-text-corpus-* = frozen text hydration; "
    "ml-external-text-embeddings-* = frozen vectorization of that corpus only; "
    "future ml-text-only-baseline consumes embedding artifacts plus labels for offline diagnostics only."
)


class MLExternalTextEmbeddingsError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLExternalTextEmbeddingsError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLExternalTextEmbeddingsError(f"Expected JSON object in {path}")
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


def validate_text_corpus_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metadata = payload.get("metadata")
    rows = payload.get("rows")
    if not isinstance(metadata, dict):
        raise MLExternalTextEmbeddingsError("text corpus missing metadata object")
    if not isinstance(rows, list):
        raise MLExternalTextEmbeddingsError("text corpus missing rows array")
    if metadata.get("artifact_type") != SOURCE_ARTIFACT_TYPE:
        raise MLExternalTextEmbeddingsError(
            f"expected metadata.artifact_type={SOURCE_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("corpus_version") != SOURCE_CORPUS_VERSION:
        raise MLExternalTextEmbeddingsError(
            f"expected metadata.corpus_version={SOURCE_CORPUS_VERSION!r}, got {metadata.get('corpus_version')!r}"
        )
    if metadata.get("review_pool_variant") != REVIEW_POOL_VARIANT:
        raise MLExternalTextEmbeddingsError(
            f"expected metadata.review_pool_variant={REVIEW_POOL_VARIANT!r}, got {metadata.get('review_pool_variant')!r}"
        )

    expected = metadata.get("strict_expected_external_row_count")
    if not isinstance(expected, int):
        summary = metadata.get("summary")
        if isinstance(summary, dict) and isinstance(summary.get("n_rows"), int):
            expected = int(summary["n_rows"])
    if not isinstance(expected, int):
        raise MLExternalTextEmbeddingsError("text corpus missing strict_expected_external_row_count or summary.n_rows")
    if len(rows) != expected:
        raise MLExternalTextEmbeddingsError(f"text corpus row count {len(rows)} does not match expected {expected}")
    if expected != 60:
        raise MLExternalTextEmbeddingsError(f"expected v7 corpus to contain exactly 60 rows, got {expected}")

    normalized_rows: list[dict[str, Any]] = []
    missing: list[str] = []
    seen: set[str] = set()
    dupes: list[str] = []
    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise MLExternalTextEmbeddingsError(f"text corpus row {idx} is not an object")
        row = dict(raw)
        row_id = str(row.get("row_id") or "").strip()
        if not row_id:
            raise MLExternalTextEmbeddingsError(f"text corpus row {idx} missing row_id")
        if row_id in seen:
            dupes.append(row_id)
        seen.add(row_id)
        text = row.get("text_for_embedding")
        if not isinstance(text, str) or not text.strip():
            missing.append(row_id)
        normalized_rows.append(row)
    if dupes:
        raise MLExternalTextEmbeddingsError(f"text corpus contains duplicate row_id values: {dupes[:10]}")
    if missing:
        raise MLExternalTextEmbeddingsError(f"text_for_embedding missing/empty for row_id values: {missing[:20]}")
    return metadata, sorted(normalized_rows, key=lambda r: str(r["row_id"]))


def _batch(items: Sequence[dict[str, Any]], size: int) -> list[Sequence[dict[str, Any]]]:
    if size < 1:
        raise MLExternalTextEmbeddingsError("--batch-size must be >= 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


def _openai_auth_artifact_fields(*, mock_embeddings: bool) -> dict[str, Any]:
    if mock_embeddings:
        return {"api_key_provided": False, "auth_mode": "mock"}
    api_key_provided = bool(os.environ.get("OPENAI_API_KEY"))
    return {"api_key_provided": api_key_provided, "auth_mode": "api_key" if api_key_provided else "no_key"}


def build_external_text_embeddings_payload(
    *,
    text_corpus_path: Path,
    embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    mock_embeddings: bool = False,
    provider: EmbeddingProvider | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    corpus_path = Path(text_corpus_path)
    source_sha = sha256_file(corpus_path)
    corpus_payload = _load_json_object(corpus_path)
    source_metadata, corpus_rows = validate_text_corpus_payload(corpus_payload)

    row_inputs: list[dict[str, Any]] = []
    for row in corpus_rows:
        text = str(row["text_for_embedding"])
        text_sha = _sha256_text(text)
        row_inputs.append(
            {
                "row": row,
                "text": text,
                "text_sha256": text_sha,
            }
        )

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
            raise MLExternalTextEmbeddingsError(f"embedding provider initialization failed: {exc}") from exc
        vectors = []
        statuses = []
        for chunk in _batch(row_inputs, batch_size):
            try:
                chunk_vectors = active_provider.embed_texts([str(item["text"]) for item in chunk])
            except Exception as exc:
                raise MLExternalTextEmbeddingsError(f"embedding provider failed: {exc}") from exc
            if len(chunk_vectors) != len(chunk):
                raise MLExternalTextEmbeddingsError(
                    f"embedding provider returned {len(chunk_vectors)} vectors for batch of {len(chunk)}"
                )
            for item, vector in zip(chunk, chunk_vectors, strict=True):
                if len(vector) != expected_dimensions:
                    raise MLExternalTextEmbeddingsError(
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
                "text_sha256": item["text_sha256"],
                "text_length": len(text),
                "embedding": vector,
                "embedding_status": status,
            }
        )

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "embedding_artifact_version": EMBEDDING_ARTIFACT_VERSION,
        "generated_at": generated_at or _now_iso_z(),
        "source_text_corpus_path": portable_repo_path(corpus_path),
        "source_text_corpus_sha256": source_sha,
        "source_text_corpus_version": source_metadata.get("corpus_version"),
        "source_label_dataset_sha256": source_metadata.get("label_dataset_sha256"),
        "embedding_model": embedding_model,
        "embedding_dimensions": expected_dimensions,
        "embedding_provider": "openai",
        "openai_base_url": os.environ.get("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL),
        "openai_auth_artifact_fields": _openai_auth_artifact_fields(mock_embeddings=mock_embeddings),
        "aggregate_input_text_sha256": aggregate_input_text_sha256,
        "row_count": len(output_rows),
        "n_embedded_ok": sum(1 for row in output_rows if row["embedding_status"] == "ok"),
        "n_mock": sum(1 for row in output_rows if row["embedding_status"] == "mock"),
        "caveats": list(CAVEATS),
        "layering_note": LAYERING_NOTE,
    }
    return {"metadata": metadata, "rows": output_rows}


def render_markdown(payload: dict[str, Any]) -> str:
    metadata = payload["metadata"]
    lines = [
        "# External Text Embeddings",
        "",
        "Frozen vectorization artifact for the external near-miss text corpus.",
        "",
        "## Disciplined Layering",
        "",
        LAYERING_NOTE,
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
        "## Caveats",
        "",
    ]
    lines.extend(f"- {caveat}" for caveat in metadata.get("caveats", []))
    lines.extend(
        [
            "",
            "Embedding vectors are intentionally omitted from this Markdown file; see the JSON artifact for vectors.",
            "",
        ]
    )
    return "\n".join(lines)


def write_external_text_embeddings(
    *,
    text_corpus_path: Path,
    output_path: Path,
    markdown_output_path: Path | None = None,
    embedding_model: str = DEFAULT_OPENAI_EMBEDDING_MODEL,
    expected_dimensions: int = EXPECTED_EMBEDDING_DIMENSIONS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    mock_embeddings: bool = False,
    provider: EmbeddingProvider | None = None,
) -> dict[str, Any]:
    payload = build_external_text_embeddings_payload(
        text_corpus_path=text_corpus_path,
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
    "MLExternalTextEmbeddingsError",
    "SOURCE_CORPUS_VERSION",
    "build_external_text_embeddings_payload",
    "render_markdown",
    "validate_text_corpus_payload",
    "write_external_text_embeddings",
]
