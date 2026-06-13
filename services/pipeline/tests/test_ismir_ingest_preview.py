"""ISMIR ingest preview: attribution, buckets, overlap, ingest-plan compatibility."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
from pipeline.corpus_v2_ingest_from_plan import CorpusV2IngestError, validate_candidate_plan
from pipeline.ismir_ingest_preview import (
    attribute_ismir,
    ismir_name_match,
    render_ismir_ingest_preview_markdown,
    run_ismir_ingest_preview,
)
from pipeline.policy import CorpusPolicy

ISMIR_SOURCE_ID = "https://openalex.org/S4306420076"


def _w(
    *,
    wid: str,
    title: str,
    abstract: str,
    year: int = 2020,
    source_id: str | None = None,
    source_name: str | None = None,
    raw_source_name: str | None = None,
) -> dict:
    source = None
    if source_id or source_name:
        source = {"id": source_id, "display_name": source_name}
    return {
        "id": wid,
        "title": title,
        "publication_year": year,
        "doi": None,
        "language": "en",
        "type": "article",
        "is_retracted": False,
        "has_abstract": True,
        "cited_by_count": 3,
        "abstract_inverted_index": {w: [i] for i, w in enumerate(abstract.split())},
        "primary_location": {"source": source, "raw_source_name": raw_source_name},
        "locations": [],
    }


def _run(source_works: list[dict], search_works: list[dict], **kwargs) -> dict:
    pages = {
        "primary_location.source.id": {"meta": {"count": len(source_works), "next_cursor": None}, "results": source_works},
        "search=": {"meta": {"count": len(search_works), "next_cursor": None}, "results": search_works},
    }

    def fetch(url: str) -> dict:
        if "primary_location.source.id" in url:
            return pages["primary_location.source.id"]
        assert "search=" in url
        return pages["search="]

    return run_ismir_ingest_preview(
        policy=CorpusPolicy(),
        mailto="x@y.z",
        contact_mode="cli",
        contact_provided=True,
        fetch=fetch,
        **kwargs,
    )


def test_ismir_name_match_variants() -> None:
    assert ismir_name_match("ISMIR")
    assert ismir_name_match("Proceedings of the 24th International Society for Music Information Retrieval Conference")
    assert ismir_name_match("International Symposium/Conference on Music Information Retrieval")
    assert not ismir_name_match("Transactions of the International Society for Music Information Retrieval")
    assert not ismir_name_match("Zenodo (CERN European Organization for Nuclear Research)")
    assert not ismir_name_match("")


def test_attribute_by_source_id_then_location_name() -> None:
    by_id = _w(
        wid="https://openalex.org/W1",
        title="Beat tracking",
        abstract="music information retrieval beat tracking",
        source_id=ISMIR_SOURCE_ID,
        source_name="International Symposium/Conference on Music Information Retrieval",
    )
    assert attribute_ismir(by_id, ismir_source_id=ISMIR_SOURCE_ID) == "source_id"

    by_raw_name = _w(
        wid="https://openalex.org/W2",
        title="Music tagging",
        abstract="music tagging mir study",
        source_name="Zenodo (CERN European Organization for Nuclear Research)",
        raw_source_name="Proceedings of the 22nd International Society for Music Information Retrieval Conference",
    )
    assert attribute_ismir(by_raw_name, ismir_source_id=ISMIR_SOURCE_ID) == "location_name"

    unattributed = _w(
        wid="https://openalex.org/W3",
        title="Music genre study",
        abstract="music information retrieval genre",
        source_name="Some Unrelated Journal",
    )
    assert attribute_ismir(unattributed, ismir_source_id=ISMIR_SOURCE_ID) is None


def test_preview_buckets_overlap_and_approved_set() -> None:
    shared = _w(
        wid="https://openalex.org/W10",
        title="Joint beat and downbeat tracking",
        abstract="music information retrieval beat tracking recurrent networks",
        source_id=ISMIR_SOURCE_ID,
        source_name="International Symposium/Conference on Music Information Retrieval",
    )
    search_only_attributed = _w(
        wid="https://openalex.org/W11",
        title="Music tagging at scale",
        abstract="ismir music tagging audio embeddings",
        source_name="Zenodo (CERN European Organization for Nuclear Research)",
        raw_source_name="Proceedings of the 23rd International Society for Music Information Retrieval Conference",
    )
    search_only_unattributed = _w(
        wid="https://openalex.org/W12",
        title="Music similarity in streaming",
        abstract="ismir-style music similarity audio study",
        source_name="Some Repository",
    )
    noise = _w(
        wid="https://openalex.org/W13",
        title="Manatee vocalization patterns",
        abstract="marine mammal manatee sounds underwater",
        source_name="Marine Biology Letters",
    )

    plan = _run([shared], [shared, search_only_attributed, search_only_unattributed, noise])

    assert plan["bucket_overlap"] == {"in_both_buckets": 1, "source_id_only": 0, "search_only": 3}
    assert plan["selected_total"] == 2
    selected_ids = {row["openalex_id"] for row in plan["selected_candidates"]}
    assert selected_ids == {"https://openalex.org/W10", "https://openalex.org/W11"}
    assert plan["attribution_statistics"]["by_source_id"] == 1
    assert plan["attribution_statistics"]["by_location_name"] == 1
    assert plan["attribution_statistics"]["unattributed_passing_filters"] == 1
    unattributed_ids = {ex["openalex_id"] for ex in plan["unattributed_passing_examples"]}
    assert unattributed_ids == {"https://openalex.org/W12"}
    search_summary = next(b for b in plan["bucket_summaries"] if b["bucket_id"] == "ismir_title_abstract_search")
    rejected = plan["rejected_noise_examples_by_bucket"]["ismir_title_abstract_search"]
    assert any(ex["openalex_id"] == "https://openalex.org/W13" for ex in rejected)
    assert search_summary["passed_filter_count"] == 3


def test_preview_artifact_is_ingest_plan_compatible(monkeypatch: pytest.MonkeyPatch) -> None:
    """The approved set must validate against corpus-v2-ingest-from-plan unchanged."""
    monkeypatch.setenv("OPENALEX_API_KEY", "test-key-not-serialized")
    work = _w(
        wid="https://openalex.org/W20",
        title="Music structure analysis",
        abstract="music information retrieval structure segmentation",
        source_id=ISMIR_SOURCE_ID,
        source_name="International Symposium/Conference on Music Information Retrieval",
    )
    plan = _run([work], [])
    validate_candidate_plan(plan)  # must not raise
    assert "test-key-not-serialized" not in json.dumps(plan)


def test_preview_empty_approved_set_is_rejected_by_ingest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENALEX_API_KEY", "k")
    plan = _run([], [])
    assert plan["selected_total"] == 0
    with pytest.raises(CorpusV2IngestError):
        validate_candidate_plan(plan)


def test_markdown_render_mentions_attribution_and_dry_run() -> None:
    plan = _run([], [])
    md = render_ismir_ingest_preview_markdown(plan)
    low = md.lower()
    assert "no database writes" in low
    assert "attribution" in low
    assert "not" in low and "attributable" in low


def test_cli_ismir_preview_mock(tmp_path: Path) -> None:
    out = tmp_path / "p.json"
    md = tmp_path / "p.md"
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "ismir-ingest-preview",
            "--output",
            str(out),
            "--markdown-output",
            str(md),
            "--mailto",
            "cli@example.com",
            "--max-works-per-bucket",
            "5",
            "--mock-openalex",
        ],
    ):
        cli_main.main()
    assert out.is_file() and md.is_file()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["selected_total"] == 0
    assert payload["contact_mode"] == "mock"
    assert payload["policy_reference"]["ismir_openalex_source_id"] == ISMIR_SOURCE_ID
