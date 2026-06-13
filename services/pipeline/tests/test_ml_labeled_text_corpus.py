"""Tests for observation-level labeled text corpus artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse

from cli_parser_source import read_cli_parser_source
from pipeline.ml_labeled_text_corpus import (
    CAVEATS,
    MLLabeledTextCorpusError,
    build_ml_labeled_text_corpus_payload,
    render_markdown,
    select_labeled_audit_rows,
)


def _label_row(
    row_id: str,
    *,
    paper_id: str = "https://openalex.org/W100",
    title: str = "Fixture title",
    relevance_label: str = "good",
    novelty_label: str = "",
    bridge_like_label: str = "",
    notes: str = "notes",
    variant: str = "full_family_top_k",
) -> dict:
    token = paper_id.rsplit("/", 1)[-1] if "W" in paper_id else ""
    return {
        "dataset_version": "ml-label-dataset-v7",
        "row_id": row_id,
        "split": "audit_only",
        "review_pool_variant": variant,
        "source_worksheet_path": "worksheet.csv",
        "paper_id": paper_id,
        "openalex_work_id": token if token.startswith("W") else "",
        "work_id": token if token.startswith("W") else "",
        "family": "emerging",
        "ranking_run_id": "rank-x",
        "title": title,
        "abstract_preview": "Preview text " + ("p" * 220),
        "relevance_label": relevance_label,
        "novelty_label": novelty_label,
        "bridge_like_label": bridge_like_label,
        "reviewer_notes": notes,
        "good_or_acceptable": True,
        "surprising_or_useful": None,
        "bridge_like_yes_or_partial": None,
    }


def _label_payload(rows: list[dict]) -> dict:
    return {"dataset_version": "ml-label-dataset-v7", "rows": rows}


def _external_text_corpus(row_id: str = "external-1", *, text: str = "External title\n\nExternal frozen text.") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_external_text_corpus",
            "corpus_version": "ml-external-text-corpus-v7",
        },
        "rows": [
            {
                "row_id": row_id,
                "paper_id": "https://openalex.org/W999",
                "openalex_work_id": "W999",
                "work_id": "W999",
                "text_for_embedding": text,
                "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "hydrated": {
                    "title": "External title",
                    "full_abstract": "External frozen text.",
                },
            }
        ],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _work(token: str, *, abstract: str | None = "Fetched abstract text with enough detail.") -> dict:
    index: dict[str, list[int]] = {}
    if abstract is not None:
        for i, word in enumerate(abstract.split()):
            index.setdefault(word, []).append(i)
    return {
        "id": f"https://openalex.org/{token}",
        "title": f"Fetched title {token}",
        "abstract_inverted_index": index if abstract is not None else {},
    }


def test_selection_requires_audit_and_explicit_label_not_notes_only() -> None:
    rows = [
        _label_row("yes", relevance_label="good", notes=""),
        _label_row("notes-only", relevance_label="", novelty_label="", bridge_like_label="", notes="has notes"),
        {**_label_row("wrong-split"), "split": "not_audit"},
    ]
    selected = select_labeled_audit_rows(_label_payload(rows))
    assert [row["row_id"] for row in selected] == ["yes"]


def test_observation_preservation_duplicate_paper_reuse_fetch_and_fallbacks(tmp_path: Path) -> None:
    rows = [
        _label_row(
            "external-1",
            paper_id="https://openalex.org/W999",
            variant="ml_external_near_miss_audit",
        ),
        _label_row("dup-a", paper_id="https://openalex.org/W101"),
        _label_row("dup-b", paper_id="https://openalex.org/W101", novelty_label="useful"),
        _label_row("empty-abstract", paper_id="https://openalex.org/W102"),
        _label_row("missing-id", paper_id="", title="No ID title"),
    ]
    label_path = _write_json(tmp_path, "labels.json", _label_payload(rows))
    external_path = _write_json(tmp_path, "external.json", _external_text_corpus())
    calls: list[str] = []

    def fetch(url: str) -> dict:
        calls.append(url)
        token = urlparse(url).path.rsplit("/", 1)[-1]
        if token == "W102":
            return _work(token, abstract=None)
        return _work(token, abstract=f"Fetched abstract for {token} with music and audio recommender context.")

    payload = build_ml_labeled_text_corpus_payload(
        label_dataset_path=label_path,
        external_text_corpus_path=external_path,
        fetch_json=fetch,
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["row_count"] == 5
    assert payload["metadata"]["counts_by_text_source"]["external_text_corpus_reuse"] == 1
    assert payload["metadata"]["counts_by_text_source"]["openalex_fetch"] == 2
    assert payload["metadata"]["counts_by_text_source"]["label_preview_fallback"] == 1
    assert payload["metadata"]["counts_by_text_source"]["missing_work_id"] == 1
    assert len(calls) == 2
    assert sorted(row["row_id"] for row in payload["rows"] if row["paper_id"] == "https://openalex.org/W101") == [
        "dup-a",
        "dup-b",
    ]

    external = next(row for row in payload["rows"] if row["row_id"] == "external-1")
    assert external["text_for_embedding"] == "External title\n\nExternal frozen text."
    assert external["text_sha256"] == hashlib.sha256(external["text_for_embedding"].encode("utf-8")).hexdigest()
    assert external["embedding_text_format_version"] == "external_text_corpus_v7_verbatim"

    fetched = next(row for row in payload["rows"] if row["row_id"] == "dup-a")
    assert fetched["text_source"] == "openalex_fetch"
    assert fetched["hydrated_title"] == "Fetched title W101"
    assert "Fetched abstract for W101" in fetched["text_for_embedding"]

    fallback = next(row for row in payload["rows"] if row["row_id"] == "empty-abstract")
    assert fallback["text_source"] == "label_preview_fallback"
    assert fallback["hydrated_title"] == "Fetched title W102"
    assert fallback["hydrated_abstract"] is None
    assert fallback["text_for_embedding"].startswith("Fixture title")

    missing = next(row for row in payload["rows"] if row["row_id"] == "missing-id")
    assert missing["text_source"] == "missing_work_id"
    assert missing["openalex_api_url_used"] is None


def test_fetch_failure_keeps_observation(tmp_path: Path) -> None:
    row = _label_row("fetch-fail", paper_id="https://openalex.org/W500")
    label_path = _write_json(tmp_path, "labels.json", _label_payload([row]))

    def fetch(_url: str) -> dict:
        raise TimeoutError("timeout fixture")

    payload = build_ml_labeled_text_corpus_payload(
        label_dataset_path=label_path,
        fetch_json=fetch,
        generated_at="2026-05-14T00:00:00Z",
    )
    out = payload["rows"][0]
    assert out["text_source"] == "fetch_failed"
    assert out["fetch_error_class"] == "TimeoutError"
    assert out["text_for_embedding"].startswith("Fixture title")


def test_mock_openalex_uses_stub_without_network(tmp_path: Path) -> None:
    row = _label_row("mock-row", paper_id="https://openalex.org/W777")
    label_path = _write_json(tmp_path, "labels.json", _label_payload([row]))

    def fetch(_url: str) -> dict:
        raise AssertionError("fetch should not be called")

    payload = build_ml_labeled_text_corpus_payload(
        label_dataset_path=label_path,
        mock_openalex=True,
        fetch_json=fetch,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert payload["metadata"]["openalex_auth_artifact_fields"]["auth_mode"] == "mock"
    assert payload["rows"][0]["text_source"] == "openalex_fetch"


def test_can_emit_caller_supplied_corpus_version(tmp_path: Path) -> None:
    row = _label_row("versioned-row", paper_id="https://openalex.org/W778")
    label_path = _write_json(tmp_path, "labels.json", _label_payload([row]))
    payload = build_ml_labeled_text_corpus_payload(
        label_dataset_path=label_path,
        corpus_version="ml-labeled-text-corpus-v3",
        mock_openalex=True,
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["corpus_version"] == "ml-labeled-text-corpus-v3"
    assert payload["metadata"]["label_dataset_version"] == "ml-label-dataset-v7"
    assert "ml-labeled-text-corpus-v3" in payload["metadata"]["layering_note"]
    assert "ml-labeled-text-corpus-v3" in render_markdown(payload)


def test_external_reuse_hash_mismatch_fails(tmp_path: Path) -> None:
    label_path = _write_json(
        tmp_path,
        "labels.json",
        _label_payload([_label_row("external-1", paper_id="https://openalex.org/W999")]),
    )
    ext = _external_text_corpus()
    ext["rows"][0]["text_sha256"] = "wrong"
    ext_path = _write_json(tmp_path, "external.json", ext)
    try:
        build_ml_labeled_text_corpus_payload(label_dataset_path=label_path, external_text_corpus_path=ext_path)
    except MLLabeledTextCorpusError as exc:
        assert "text_sha256 mismatch" in str(exc)
    else:
        raise AssertionError("expected MLLabeledTextCorpusError")


def test_markdown_caveats_and_no_database_parser_flag(tmp_path: Path) -> None:
    label_path = _write_json(
        tmp_path,
        "labels.json",
        _label_payload([_label_row("mock-row", paper_id="https://openalex.org/W777")]),
    )
    payload = build_ml_labeled_text_corpus_payload(
        label_dataset_path=label_path,
        mock_openalex=True,
        generated_at="2026-05-14T00:00:00Z",
    )
    md = render_markdown(payload)
    for caveat in CAVEATS:
        assert caveat in md
    assert "Full abstracts are intentionally omitted" in md
    assert payload["rows"][0]["hydrated_abstract"] not in md

    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_labeled_text_corpus.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-labeled-text-corpus"')
    end = cli_source.index("ml_external_text_embeddings_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
