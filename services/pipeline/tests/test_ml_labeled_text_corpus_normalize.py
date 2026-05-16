"""Tests for canonical labeled text corpus normalization."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pipeline.ml_labeled_text_corpus_normalize import (
    CAVEATS,
    CORPUS_VERSION,
    FORMAT_V2_CANONICAL,
    LAYERING_NOTE,
    MLLabeledTextCorpusNormalizeError,
    SOURCE_CORPUS_VERSION,
    build_ml_labeled_text_corpus_normalize_payload,
    render_markdown,
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _row(
    row_id: str,
    *,
    hydrated_title: str | None = "Hydrated Title",
    hydrated_abstract: str | None = "Hydrated abstract text " * 12,
    text_for_embedding: str | None = "Old text",
    text_source: str = "openalex_fetch",
    format_version: str = "labeled_text_corpus_v1_openalex_title_abstract",
) -> dict:
    text = text_for_embedding
    return {
        "row_id": row_id,
        "dataset_version": "ml-label-dataset-v7",
        "review_pool_variant": "ml_external_near_miss_audit" if row_id.endswith("2") else "ml_blind_snapshot_audit",
        "source_worksheet_path": "docs/audit/manual-review/example.csv",
        "paper_id": f"https://openalex.org/W{row_id[-1]}",
        "openalex_work_id": f"W{row_id[-1]}",
        "work_id": f"W{row_id[-1]}",
        "family": None if row_id.endswith("2") else "emerging",
        "ranking_run_id": None,
        "title": "Label Title",
        "relevance_label": "yes",
        "novelty_label": "partial",
        "bridge_like_label": "no",
        "reviewer_notes": f"note {row_id}",
        "good_or_acceptable": True,
        "surprising_or_useful": True,
        "bridge_like_yes_or_partial": False,
        "text_source": text_source,
        "embedding_text_format_version": format_version,
        "hydrated_title": hydrated_title,
        "hydrated_abstract": hydrated_abstract,
        "text_for_embedding": text,
        "text_sha256": _sha(text) if isinstance(text, str) else None,
        "text_length": len(text) if isinstance(text, str) else None,
        "sufficient_text_for_embedding_heuristic": False,
        "custom_passthrough": {"kept": row_id},
    }


def _payload(*, rows: list[dict] | None = None, version: str = SOURCE_CORPUS_VERSION) -> dict:
    actual_rows = rows if rows is not None else [_row("row-002"), _row("row-001")]
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


def _write_source(tmp_path: Path, payload: dict | None = None) -> Path:
    path = tmp_path / "ml-labeled-text-corpus-v1.json"
    path.write_text(json.dumps(payload or _payload(), indent=2) + "\n", encoding="utf-8")
    return path


def test_normalizes_rows_with_stable_sort_and_metadata(tmp_path: Path) -> None:
    canonical = _row(
        "row-002",
        hydrated_title="  Canonical Title  ",
        hydrated_abstract="  Canonical abstract body.  ",
        text_for_embedding="Different old text",
        text_source="external_text_corpus_reuse",
        format_version="external_text_corpus_v7_verbatim",
    )
    fallback = _row("row-001", hydrated_abstract="", text_for_embedding="Exact old fallback text")
    missing = _row("row-003", hydrated_title="", hydrated_abstract="", text_for_embedding="")
    path = _write_source(tmp_path, _payload(rows=[canonical, missing, fallback]))

    payload = build_ml_labeled_text_corpus_normalize_payload(
        source_corpus_path=path,
        generated_at="2026-05-14T00:00:00Z",
    )

    rows = payload["rows"]
    assert [row["row_id"] for row in rows] == ["row-001", "row-002", "row-003"]

    assert rows[0]["canonicalization_status"] == "original_text_fallback"
    assert rows[0]["text_for_embedding"] == "Exact old fallback text"
    assert rows[0]["text_sha256"] == _sha("Exact old fallback text")

    assert rows[1]["canonicalization_status"] == "canonical_title_abstract"
    assert rows[1]["text_for_embedding"] == "Canonical Title\n\nCanonical abstract body."
    assert rows[1]["embedding_text_format_version"] == FORMAT_V2_CANONICAL
    assert rows[1]["previous_text_source"] == "external_text_corpus_reuse"
    assert rows[1]["previous_text_sha256"] == _sha("Different old text")
    assert rows[1]["previous_text_length"] == len("Different old text")
    assert rows[1]["previous_embedding_text_format_version"] == "external_text_corpus_v7_verbatim"
    assert rows[1]["custom_passthrough"] == {"kept": "row-002"}
    assert rows[1]["reviewer_notes"] == "note row-002"
    assert rows[1]["good_or_acceptable"] is True

    assert rows[2]["canonicalization_status"] == "missing_text"
    assert rows[2]["text_for_embedding"] == ""
    assert rows[2]["text_sha256"] == _sha("")

    meta = payload["metadata"]
    assert meta["corpus_version"] == CORPUS_VERSION
    assert meta["source_corpus_version"] == SOURCE_CORPUS_VERSION
    assert meta["source_label_dataset_sha256"] == "label-sha"
    assert meta["row_count"] == 3
    assert meta["counts_by_canonicalization_status"] == {
        "canonical_title_abstract": 1,
        "missing_text": 1,
        "original_text_fallback": 1,
    }
    assert meta["counts_by_previous_embedding_text_format_version"] == {
        "external_text_corpus_v7_verbatim": 1,
        "labeled_text_corpus_v1_openalex_title_abstract": 2,
    }
    assert meta["counts_by_previous_text_source"] == {"external_text_corpus_reuse": 1, "openalex_fetch": 2}
    assert meta["n_text_changed_from_v1"] == 1


def test_canonical_title_falls_back_to_label_title_then_work_id(tmp_path: Path) -> None:
    title_row = _row("row-010", hydrated_title="", hydrated_abstract="Abstract", text_for_embedding="old")
    work_id_row = _row("row-011", hydrated_title="", hydrated_abstract="Abstract", text_for_embedding="old")
    work_id_row.pop("title")
    work_id_row["work_id"] = "Wfallback"

    payload = build_ml_labeled_text_corpus_normalize_payload(
        source_corpus_path=_write_source(tmp_path, _payload(rows=[work_id_row, title_row])),
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["rows"][0]["text_for_embedding"] == "Label Title\n\nAbstract"
    assert payload["rows"][1]["text_for_embedding"] == "Wfallback\n\nAbstract"


def test_accepts_caller_supplied_source_and_output_versions(tmp_path: Path) -> None:
    source = _payload(version="ml-labeled-text-corpus-v3")
    payload = build_ml_labeled_text_corpus_normalize_payload(
        source_corpus_path=_write_source(tmp_path, source),
        source_corpus_version="ml-labeled-text-corpus-v3",
        corpus_version="ml-labeled-text-corpus-v3-normalized",
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["source_corpus_version"] == "ml-labeled-text-corpus-v3"
    assert payload["metadata"]["corpus_version"] == "ml-labeled-text-corpus-v3-normalized"
    assert "ml-labeled-text-corpus-v3-normalized" in payload["metadata"]["layering_note"]
    assert "ml-labeled-text-corpus-v3-normalized" in render_markdown(payload)


def test_validation_failures(tmp_path: Path) -> None:
    with pytest.raises(MLLabeledTextCorpusNormalizeError, match="metadata object"):
        build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=_write_source(tmp_path, {"rows": []}))

    bad_type = _payload()
    bad_type["metadata"]["artifact_type"] = "wrong"
    with pytest.raises(MLLabeledTextCorpusNormalizeError, match="artifact_type"):
        build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=_write_source(tmp_path, bad_type))

    bad_version = _payload(version="ml-labeled-text-corpus-v0")
    with pytest.raises(MLLabeledTextCorpusNormalizeError, match="corpus_version"):
        build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=_write_source(tmp_path, bad_version))

    bad_count = _payload()
    bad_count["metadata"]["row_count"] = 99
    with pytest.raises(MLLabeledTextCorpusNormalizeError, match="row count"):
        build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=_write_source(tmp_path, bad_count))

    bad_count_type = _payload()
    bad_count_type["metadata"]["row_count"] = "2"
    with pytest.raises(MLLabeledTextCorpusNormalizeError, match="row_count"):
        build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=_write_source(tmp_path, bad_count_type))

    dupes = _payload(rows=[_row("row-001"), _row("row-001")])
    with pytest.raises(MLLabeledTextCorpusNormalizeError, match="duplicate row_id"):
        build_ml_labeled_text_corpus_normalize_payload(source_corpus_path=_write_source(tmp_path, dupes))


def test_markdown_caveats_and_no_full_abstracts(tmp_path: Path) -> None:
    payload = build_ml_labeled_text_corpus_normalize_payload(
        source_corpus_path=_write_source(tmp_path),
        generated_at="2026-05-14T00:00:00Z",
    )
    md = render_markdown(payload)
    assert LAYERING_NOTE in md
    for caveat in CAVEATS:
        assert caveat in md
    assert "Canonicalization Status" in md
    assert "Hydrated abstract text" not in md


def test_no_http_or_database_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_labeled_text_corpus_normalize.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source
    assert "embedding_provider" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-labeled-text-corpus-normalize"')
    end = cli_source.index("ml_external_text_embeddings_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
