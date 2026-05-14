"""Tests for frozen external near-miss text embedding artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Sequence

import pytest

from pipeline.ml_external_text_embeddings import (
    CAVEATS,
    EMBEDDING_ARTIFACT_VERSION,
    LAYERING_NOTE,
    MLExternalTextEmbeddingsError,
    REVIEW_POOL_VARIANT,
    SOURCE_CORPUS_VERSION,
    build_external_text_embeddings_payload,
    render_markdown,
)


class _FakeProvider:
    def __init__(self, *, dimensions: int = 3, wrong_dimensions: bool = False) -> None:
        self.dimensions = dimensions
        self.wrong_dimensions = wrong_dimensions
        self.calls: list[list[str]] = []

    def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        dim = self.dimensions + 1 if self.wrong_dimensions else self.dimensions
        return [[float(len(text)), float(index), 0.5][:dim] + [0.0] * max(0, dim - 3) for index, text in enumerate(texts)]


def _row(index: int, *, row_id: str | None = None, text: str | None = None) -> dict:
    token = f"W800{index:03d}"
    return {
        "row_id": row_id or f"row-{index:03d}",
        "paper_id": f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": token,
        "text_for_embedding": text if text is not None else f"Title {index}\n\nAbstract body {index}.",
    }


def _corpus_payload(*, rows: list[dict] | None = None, version: str = SOURCE_CORPUS_VERSION) -> dict:
    actual_rows = rows if rows is not None else [_row(i) for i in range(1, 61)]
    return {
        "metadata": {
            "artifact_type": "ml_external_text_corpus",
            "corpus_version": version,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "strict_expected_external_row_count": 60,
            "summary": {"n_rows": len(actual_rows)},
            "label_dataset_sha256": "label-sha-fixture",
        },
        "rows": actual_rows,
    }


def _write_corpus(tmp_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "ml-external-text-corpus-v7.json"
    path.write_text(json.dumps(payload or _corpus_payload(), indent=2) + "\n", encoding="utf-8")
    return path


def test_build_payload_sorts_rows_and_hashes_exact_text(tmp_path: Path) -> None:
    rows = [_row(i) for i in range(60, 0, -1)]
    rows[-1]["text_for_embedding"] = "Title 1\n\nText with  exact spacing."
    path = _write_corpus(tmp_path, _corpus_payload(rows=rows))
    provider = _FakeProvider(dimensions=3)

    payload = build_external_text_embeddings_payload(
        text_corpus_path=path,
        expected_dimensions=3,
        batch_size=17,
        provider=provider,
        generated_at="2026-05-14T00:00:00Z",
    )

    out_rows = payload["rows"]
    assert [row["row_id"] for row in out_rows] == sorted(row["row_id"] for row in rows)
    first_text = "Title 1\n\nText with  exact spacing."
    assert out_rows[0]["text_sha256"] == hashlib.sha256(first_text.encode("utf-8")).hexdigest()
    assert out_rows[0]["text_length"] == len(first_text)
    assert out_rows[0]["work_id"] == "W800001"
    assert out_rows[0]["embedding_status"] == "ok"
    assert provider.calls[0][0] == first_text

    aggregate_lines = "".join(f"{row['row_id']}\0{row['text_sha256']}\n" for row in out_rows)
    assert payload["metadata"]["aggregate_input_text_sha256"] == hashlib.sha256(
        aggregate_lines.encode("utf-8")
    ).hexdigest()
    assert payload["metadata"]["row_count"] == 60
    assert payload["metadata"]["n_embedded_ok"] == 60
    assert payload["metadata"]["n_mock"] == 0
    assert payload["metadata"]["source_label_dataset_sha256"] == "label-sha-fixture"


def test_mock_vectors_are_deterministic(tmp_path: Path) -> None:
    path = _write_corpus(tmp_path)
    left = build_external_text_embeddings_payload(
        text_corpus_path=path,
        expected_dimensions=5,
        mock_embeddings=True,
        generated_at="2026-05-14T00:00:00Z",
    )
    right = build_external_text_embeddings_payload(
        text_corpus_path=path,
        expected_dimensions=5,
        mock_embeddings=True,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert left["rows"][0]["embedding"] == right["rows"][0]["embedding"]
    assert left["rows"][0]["embedding_status"] == "mock"
    assert len(left["rows"][0]["embedding"]) == 5
    assert left["metadata"]["openai_auth_artifact_fields"]["auth_mode"] == "mock"
    assert left["metadata"]["n_mock"] == 60


def test_rejects_wrong_shape_version_pool_or_count(tmp_path: Path) -> None:
    with pytest.raises(MLExternalTextEmbeddingsError, match="Failed to load JSON"):
        build_external_text_embeddings_payload(text_corpus_path=tmp_path / "missing.json", mock_embeddings=True)

    with pytest.raises(MLExternalTextEmbeddingsError, match="metadata object"):
        build_external_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, {"rows": []}), mock_embeddings=True)

    bad_type = _corpus_payload()
    bad_type["metadata"]["artifact_type"] = "wrong_artifact"
    with pytest.raises(MLExternalTextEmbeddingsError, match="artifact_type"):
        build_external_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, bad_type), mock_embeddings=True)

    bad_version = _corpus_payload(version="ml-external-text-corpus-v6")
    with pytest.raises(MLExternalTextEmbeddingsError, match="corpus_version"):
        build_external_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, bad_version), mock_embeddings=True)

    bad_pool = _corpus_payload()
    bad_pool["metadata"]["review_pool_variant"] = "other"
    with pytest.raises(MLExternalTextEmbeddingsError, match="review_pool_variant"):
        build_external_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, bad_pool), mock_embeddings=True)

    short = _corpus_payload(rows=[_row(i) for i in range(1, 60)])
    short["metadata"]["strict_expected_external_row_count"] = 59
    with pytest.raises(MLExternalTextEmbeddingsError, match="exactly 60"):
        build_external_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, short), mock_embeddings=True)

    dupes = _corpus_payload(rows=[_row(i) for i in range(1, 61)])
    dupes["rows"][1]["row_id"] = dupes["rows"][0]["row_id"]
    with pytest.raises(MLExternalTextEmbeddingsError, match="duplicate row_id"):
        build_external_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, dupes), mock_embeddings=True)


def test_empty_text_and_dimension_mismatch_fail(tmp_path: Path) -> None:
    rows = [_row(i) for i in range(1, 61)]
    rows[10]["text_for_embedding"] = "  "
    with pytest.raises(MLExternalTextEmbeddingsError, match="row-011"):
        build_external_text_embeddings_payload(
            text_corpus_path=_write_corpus(tmp_path, _corpus_payload(rows=rows)),
            mock_embeddings=True,
        )

    with pytest.raises(MLExternalTextEmbeddingsError, match="dimension mismatch"):
        build_external_text_embeddings_payload(
            text_corpus_path=_write_corpus(tmp_path, _corpus_payload()),
            expected_dimensions=3,
            provider=_FakeProvider(dimensions=3, wrong_dimensions=True),
        )


def test_markdown_has_caveats_and_no_vectors(tmp_path: Path) -> None:
    payload = build_external_text_embeddings_payload(
        text_corpus_path=_write_corpus(tmp_path),
        expected_dimensions=3,
        mock_embeddings=True,
        generated_at="2026-05-14T00:00:00Z",
    )
    md = render_markdown(payload)
    assert LAYERING_NOTE in md
    for caveat in CAVEATS:
        assert caveat in md
    assert "Embedding vectors are intentionally omitted" in md
    assert str(payload["rows"][0]["embedding"][0]) not in md
    assert EMBEDDING_ARTIFACT_VERSION in md


def test_no_database_flag_or_psycopg_dependency() -> None:
    module_source = Path("pipeline/ml_external_text_embeddings.py").read_text(encoding="utf-8")
    assert "psycopg" not in module_source.lower()

    cli_source = Path("pipeline/cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-external-text-embeddings"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
