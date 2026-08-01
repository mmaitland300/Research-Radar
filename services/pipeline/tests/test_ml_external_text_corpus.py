"""Tests for external near-miss OpenAlex text corpus hydration."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.ml_external_text_corpus import (
    EXPECTED_EXTERNAL_ROWS,
    MLExternalTextCorpusError,
    REVIEW_POOL_VARIANT,
    build_external_text_corpus_payload,
    render_markdown,
    select_external_rows,
)


def _inverted_index(text: str) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for i, word in enumerate(text.split()):
        out.setdefault(word, []).append(i)
    return out


def _external_row(i: int, *, preview: str | None = None) -> dict:
    token = f"W700{i:03d}"
    return {
        "dataset_version": "ml-label-dataset-v7",
        "row_id": f"row-{i:03d}",
        "paper_id": f"https://openalex.org/{token.lower()}" if i == 1 else f"https://openalex.org/{token}",
        "work_id": "" if i == 1 else token,
        "openalex_work_id": f"https://openalex.org/{token.lower()}" if i == 1 else token,
        "title": f"Dataset title {i}",
        "year": "2025",
        "citation_count": str(i),
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "split": "audit_only",
        "family": None,
        "relevance_label": "match" if i % 2 else "miss",
        "novelty_label": "known",
        "bridge_like_label": "no",
        "reviewer_notes": f"review notes {i}",
        "good_or_acceptable": bool(i % 2),
        "surprising_or_useful": False,
        "bridge_like_yes_or_partial": False,
        "sample_reason": "adjacent_audio_not_mir",
        "cluster_id": "ext",
        "topics": "Audio;Recommender",
        "abstract_preview": preview if preview is not None else f"Dataset preview for {token}.",
        "external_near_miss_context": {
            "row_id": f"row-{i:03d}",
            "paper_id": f"https://openalex.org/{token}",
            "openalex_work_id": token,
            "sample_reason": "adjacent_audio_not_mir",
            "review_metadata": {
                "title": f"Dataset title {i}",
                "year": "2025",
                "citation_count": str(i),
                "abstract_preview": "Context preview " + ("x" * 230),
            },
            "source_metadata": {"source_display_name": "Fixture Journal"},
        },
    }


def _write_label_dataset(tmp_path: Path, *, n: int = EXPECTED_EXTERNAL_ROWS) -> Path:
    rows = [_external_row(i) for i in range(1, n + 1)]
    if n >= 2:
        rows[1]["abstract_preview"] = ""
    payload = {
        "dataset_version": "ml-label-dataset-v7",
        "rows": rows + [{"row_id": "other", "review_pool_variant": "ml_blind_snapshot_audit"}],
    }
    path = tmp_path / "ml-label-dataset-v7.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _write_sidecar(tmp_path: Path, *, n: int = EXPECTED_EXTERNAL_ROWS) -> Path:
    path = tmp_path / "ml_external_near_miss_review_v1_context.json"
    path.write_text(
        json.dumps({"artifact_type": "ctx", "rows": [{"row_id": f"row-{i:03d}"} for i in range(1, n + 1)]})
        + "\n",
        encoding="utf-8",
    )
    return path


def _fake_work(token: str, *, abstract: str | None = None) -> dict:
    return {
        "id": f"https://openalex.org/{token}",
        "doi": f"https://doi.org/10.1234/{token.lower()}",
        "title": f"Hydrated title {token}",
        "type": "article",
        "language": "en",
        "publication_year": 2026,
        "publication_date": "2026-05-01",
        "cited_by_count": 44,
        "primary_location": {
            "landing_page_url": f"https://example.test/{token}",
            "source": {
                "id": "https://openalex.org/S123",
                "display_name": "Fixture Journal",
                "issn_l": "1234-5678",
                "issn": ["1234-5678"],
                "type": "journal",
            },
        },
        "topics": [
            {
                "id": "https://openalex.org/T1",
                "display_name": "Music information retrieval",
                "score": 0.91,
                "field": {"id": "https://openalex.org/fields/17", "display_name": "Computer science"},
            }
        ],
        "abstract_inverted_index": _inverted_index(
            abstract
            if abstract is not None
            else f"Hydrated abstract for {token} with music recommendation context and enough words."
        ),
    }


def test_select_external_rows_only(tmp_path: Path) -> None:
    payload = {
        "rows": [
            _external_row(1),
            {"row_id": "other", "review_pool_variant": "ml_blind_snapshot_audit"},
        ]
    }
    rows = select_external_rows(payload)
    assert len(rows) == 1
    assert rows[0]["row_id"] == "row-001"


def test_hydrates_openalex_text_and_normalizes_tokens(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    sidecar_path = _write_sidecar(tmp_path)
    calls: list[str] = []

    def fetch(url: str) -> dict:
        calls.append(url)
        token = urlparse(url).path.rsplit("/", 1)[-1]
        if token == "W700002":
            return _fake_work(token, abstract="")
        if token == "W700003":
            raise RuntimeError("network fixture failure")
        return _fake_work(token)

    payload = build_external_text_corpus_payload(
        label_dataset_path=label_path,
        context_sidecar_path=sidecar_path,
        mailto="reviewer@example.test",
        fetch_json=fetch,
    )

    assert payload["metadata"]["summary"]["n_rows"] == 60
    assert payload["metadata"]["summary"]["n_fetch_ok"] == 59
    assert payload["metadata"]["summary"]["n_fetch_failed"] == 1
    assert payload["metadata"]["context_sidecar"]["row_id_sets_match"] is True
    assert len(calls) == 60
    assert all("api.openalex.org/works" in url for url in calls)
    assert "abstract_inverted_index" in parse_qs(urlparse(calls[0]).query)["select"][0]

    row1 = payload["rows"][0]
    assert row1["openalex_work_id"] == "W700001"
    assert urlparse(row1["provenance"]["openalex_api_url_used"]).path.endswith("/W700001")
    assert row1["hydrated"]["full_abstract"].startswith("Hydrated abstract for W700001")
    assert row1["abstract_source"] == "openalex_inverted_index"
    assert row1["hydrated"]["source"]["display_name"] == "Fixture Journal"
    assert row1["labels"]["relevance_label"] == "match"
    assert row1["labels"]["good_or_acceptable"] is True


def test_text_for_embedding_fallback_and_heuristic(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)

    def fetch(url: str) -> dict:
        token = urlparse(url).path.rsplit("/", 1)[-1]
        if token == "W700002":
            return _fake_work(token, abstract="")
        return _fake_work(token, abstract="short abstract")

    payload = build_external_text_corpus_payload(label_dataset_path=label_path, fetch_json=fetch)
    row2 = next(row for row in payload["rows"] if row["row_id"] == "row-002")
    assert row2["abstract_source"] == "preview_fallback"
    assert row2["text_fallback_source"] == "context_review_metadata.abstract_preview"
    assert row2["sufficient_text_for_embedding_heuristic"] is True
    assert "Context preview" in row2["text_for_embedding"]


def test_failure_row_is_preserved_with_preview_text(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    secret = "openalex-secret-never-persist"

    def fetch(url: str) -> dict:
        token = urlparse(url).path.rsplit("/", 1)[-1]
        if token == "W700003":
            raise TimeoutError(f"timeout fixture api_key={secret}")
        return _fake_work(token)

    payload = build_external_text_corpus_payload(label_dataset_path=label_path, fetch_json=fetch)
    row3 = next(row for row in payload["rows"] if row["row_id"] == "row-003")
    assert row3["abstract_source"] == "fetch_failed"
    assert row3["provenance"]["error_class"] == "TimeoutError"
    assert row3["provenance"]["error_message"] == "TimeoutError: details redacted"
    assert row3["hydrated"]["full_abstract"] == ""
    assert row3["text_for_embedding"].startswith("Dataset title 3")
    assert secret not in json.dumps(payload)


def test_strict_external_row_count_fails_fast(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path, n=59)
    with pytest.raises(MLExternalTextCorpusError, match="Expected exactly 60"):
        build_external_text_corpus_payload(label_dataset_path=label_path, mock_openalex=True)


def test_mock_openalex_skips_fetch_callable(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)

    def fetch(_url: str) -> dict:
        raise AssertionError("fetch should not be called in mock mode")

    payload = build_external_text_corpus_payload(
        label_dataset_path=label_path,
        mock_openalex=True,
        fetch_json=fetch,
    )
    assert payload["metadata"]["openalex_auth_artifact_fields"]["mock_openalex"] is True
    assert payload["metadata"]["summary"]["n_fetch_failed"] == 0


def test_markdown_contains_required_caveats(tmp_path: Path) -> None:
    label_path = _write_label_dataset(tmp_path)
    payload = build_external_text_corpus_payload(label_dataset_path=label_path, mock_openalex=True)
    md = render_markdown(payload)
    assert "Not validation" in md
    assert "Text hydration only" in md
    assert "No DB writes" in md
    assert "No ranking or model training" in md
    assert "OpenAlex metadata can drift" in md
    assert "offline embedding" in md
