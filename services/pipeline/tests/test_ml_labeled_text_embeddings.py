"""Tests for labeled text embedding artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Sequence

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_labeled_text_embeddings import (
    CAVEATS,
    EMBEDDING_ARTIFACT_VERSION,
    LAYERING_NOTE,
    MLLabeledTextEmbeddingsError,
    SOURCE_CORPUS_VERSION,
    build_ml_labeled_text_embeddings_payload,
    render_markdown,
    write_ml_labeled_text_embeddings,
)


class _FakeProvider:
    def __init__(self, *, dimensions: int = 3, wrong_dimensions: bool = False, fail: bool = False) -> None:
        self.dimensions = dimensions
        self.wrong_dimensions = wrong_dimensions
        self.fail = fail
        self.calls: list[list[str]] = []

    def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        if self.fail:
            raise RuntimeError("provider fixture failure")
        dim = self.dimensions + 1 if self.wrong_dimensions else self.dimensions
        return [[float(len(text)), float(index), 0.25][:dim] + [0.0] * max(0, dim - 3) for index, text in enumerate(texts)]


def _row(index: int, *, row_id: str | None = None, text: str | None = None) -> dict:
    token = f"W1000{index:03d}"
    return {
        "row_id": row_id or f"row-{index:03d}",
        "paper_id": f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": token,
        "review_pool_variant": "ml_blind_snapshot_audit" if index % 2 else "ml_external_near_miss_audit",
        "family": None if index % 2 else "emerging",
        "ranking_run_id": None if index % 2 else "rank-x",
        "embedding_text_format_version": "format-a" if index % 2 else "format-b",
        "text_source": "openalex_fetch" if index % 2 else "external_text_corpus_reuse",
        "text_for_embedding": text if text is not None else f"Title {index}\n\nAbstract body {index}.",
    }


def _corpus_payload(*, rows: list[dict] | None = None, version: str = SOURCE_CORPUS_VERSION) -> dict:
    actual_rows = rows if rows is not None else [_row(i) for i in range(1, 5)]
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_corpus",
            "corpus_version": version,
            "row_count": len(actual_rows),
            "label_dataset_sha256": "label-sha",
            "label_dataset_version": "ml-label-dataset-v7",
        },
        "rows": actual_rows,
    }


def _write_corpus(tmp_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "ml-labeled-text-corpus-v1.json"
    path.write_text(json.dumps(payload or _corpus_payload(), indent=2) + "\n", encoding="utf-8")
    return path


def test_build_payload_sorts_hashes_and_passthroughs(tmp_path: Path) -> None:
    rows = [_row(3), _row(1, text="Title 1\n\nText with  exact spacing."), _row(2), _row(4)]
    path = _write_corpus(tmp_path, _corpus_payload(rows=rows))
    provider = _FakeProvider(dimensions=3)

    payload = build_ml_labeled_text_embeddings_payload(
        text_corpus_path=path,
        expected_dimensions=3,
        batch_size=2,
        provider=provider,
        generated_at="2026-05-14T00:00:00Z",
    )

    out_rows = payload["rows"]
    assert [row["row_id"] for row in out_rows] == sorted(row["row_id"] for row in rows)
    first_text = "Title 1\n\nText with  exact spacing."
    assert out_rows[0]["text_sha256"] == hashlib.sha256(first_text.encode("utf-8")).hexdigest()
    assert out_rows[0]["text_length"] == len(first_text)
    assert out_rows[0]["work_id"] == "W1000001"
    assert out_rows[0]["review_pool_variant"] == "ml_blind_snapshot_audit"
    assert out_rows[0]["family"] is None
    assert out_rows[0]["ranking_run_id"] is None
    assert out_rows[0]["embedding_text_format_version"] == "format-a"
    assert out_rows[0]["text_source"] == "openalex_fetch"
    assert provider.calls[0][0] == first_text

    aggregate_lines = "".join(f"{row['row_id']}\0{row['text_sha256']}\n" for row in out_rows)
    assert payload["metadata"]["aggregate_input_text_sha256"] == hashlib.sha256(
        aggregate_lines.encode("utf-8")
    ).hexdigest()
    assert payload["metadata"]["source_label_dataset_sha256"] == "label-sha"
    assert payload["metadata"]["source_label_dataset_version"] == "ml-label-dataset-v7"
    assert payload["metadata"]["counts_by_review_pool_variant"] == {
        "ml_blind_snapshot_audit": 2,
        "ml_external_near_miss_audit": 2,
    }
    assert payload["metadata"]["counts_by_embedding_text_format_version"] == {"format-a": 2, "format-b": 2}


def test_mock_vectors_are_deterministic(tmp_path: Path) -> None:
    path = _write_corpus(tmp_path)
    left = build_ml_labeled_text_embeddings_payload(
        text_corpus_path=path,
        expected_dimensions=5,
        mock_embeddings=True,
        generated_at="2026-05-14T00:00:00Z",
    )
    right = build_ml_labeled_text_embeddings_payload(
        text_corpus_path=path,
        expected_dimensions=5,
        mock_embeddings=True,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert left["rows"][0]["embedding"] == right["rows"][0]["embedding"]
    assert left["rows"][0]["embedding_status"] == "mock"
    assert len(left["rows"][0]["embedding"]) == 5
    assert left["metadata"]["openai_auth_artifact_fields"]["auth_mode"] == "mock"
    assert left["metadata"]["n_mock"] == 4


def test_accepts_caller_supplied_source_and_embedding_versions(tmp_path: Path) -> None:
    corpus = _corpus_payload(version="ml-labeled-text-corpus-v3-normalized")
    corpus["metadata"].pop("label_dataset_sha256")
    corpus["metadata"].pop("label_dataset_version")
    corpus["metadata"]["source_label_dataset_sha256"] = "source-label-sha"
    corpus["metadata"]["source_label_dataset_version"] = "ml-label-dataset-v8"
    path = _write_corpus(tmp_path, corpus)
    payload = build_ml_labeled_text_embeddings_payload(
        text_corpus_path=path,
        source_corpus_version="ml-labeled-text-corpus-v3-normalized",
        embedding_artifact_version="ml-labeled-text-embeddings-v3",
        expected_dimensions=3,
        mock_embeddings=True,
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["source_text_corpus_version"] == "ml-labeled-text-corpus-v3-normalized"
    assert payload["metadata"]["source_label_dataset_sha256"] == "source-label-sha"
    assert payload["metadata"]["source_label_dataset_version"] == "ml-label-dataset-v8"
    assert payload["metadata"]["embedding_artifact_version"] == "ml-labeled-text-embeddings-v3"
    assert "ml-labeled-text-embeddings-v3" in payload["metadata"]["layering_note"]
    assert "ml-labeled-text-embeddings-v3" in render_markdown(payload)


def test_validation_failures(tmp_path: Path) -> None:
    with pytest.raises(MLLabeledTextEmbeddingsError, match="metadata object"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, {"rows": []}), mock_embeddings=True)

    bad_type = _corpus_payload()
    bad_type["metadata"]["artifact_type"] = "wrong"
    with pytest.raises(MLLabeledTextEmbeddingsError, match="artifact_type"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, bad_type), mock_embeddings=True)

    bad_version = _corpus_payload(version="ml-labeled-text-corpus-v0")
    with pytest.raises(MLLabeledTextEmbeddingsError, match="corpus_version"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, bad_version), mock_embeddings=True)

    bad_count = _corpus_payload()
    bad_count["metadata"]["row_count"] = 99
    with pytest.raises(MLLabeledTextEmbeddingsError, match="row count"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, bad_count), mock_embeddings=True)

    empty = _corpus_payload(rows=[])
    with pytest.raises(MLLabeledTextEmbeddingsError, match="empty"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, empty), mock_embeddings=True)

    dupes = _corpus_payload(rows=[_row(1), _row(2, row_id="row-001")])
    with pytest.raises(MLLabeledTextEmbeddingsError, match="duplicate row_id"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, dupes), mock_embeddings=True)

    empty_text = _corpus_payload(rows=[_row(1), _row(2, text="  ")])
    with pytest.raises(MLLabeledTextEmbeddingsError, match="row-002"):
        build_ml_labeled_text_embeddings_payload(text_corpus_path=_write_corpus(tmp_path, empty_text), mock_embeddings=True)


def test_dimension_and_provider_errors_fail_without_partial_write(tmp_path: Path) -> None:
    path = _write_corpus(tmp_path)
    with pytest.raises(MLLabeledTextEmbeddingsError, match="dimension mismatch"):
        build_ml_labeled_text_embeddings_payload(
            text_corpus_path=path,
            expected_dimensions=3,
            provider=_FakeProvider(dimensions=3, wrong_dimensions=True),
        )

    output = tmp_path / "out.json"
    with pytest.raises(MLLabeledTextEmbeddingsError, match="provider failed"):
        write_ml_labeled_text_embeddings(
            text_corpus_path=path,
            output_path=output,
            markdown_output_path=None,
            expected_dimensions=3,
            provider=_FakeProvider(dimensions=3, fail=True),
        )
    assert not output.exists()


def test_markdown_has_caveats_layering_and_no_vectors(tmp_path: Path) -> None:
    payload = build_ml_labeled_text_embeddings_payload(
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


def test_no_database_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_labeled_text_embeddings.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source

    corpus_source = (package_root / "pipeline" / "ml_labeled_text_corpus.py").read_text(encoding="utf-8")
    assert "ml_offline_baseline_eval" not in corpus_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-labeled-text-embeddings"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
