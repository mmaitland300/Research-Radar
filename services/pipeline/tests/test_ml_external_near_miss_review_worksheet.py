"""Tests for external near-miss reviewer-blind worksheet."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.ml_external_near_miss_review_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    CSV_COLUMNS,
    HIDDEN_REVIEWER_CSV_FIELDS,
    REVIEW_POOL_VARIANT,
    WORKSHEET_VERSION,
    MLExternalNearMissReviewWorksheetError,
    build_external_near_miss_review_worksheet,
    canonical_work_token,
    stable_row_id,
)


def _work(
    token: str,
    *,
    title: str | None = None,
    topics: list[str] | None = None,
    abstract: str | None = None,
) -> dict:
    return {
        "id": f"https://openalex.org/{token}",
        "doi": f"https://doi.org/10.1234/{token.lower()}",
        "title": title or f"Fixture paper {token}",
        "type": "article",
        "language": "en",
        "publication_year": 2024,
        "publication_date": "2024-01-01",
        "cited_by_count": int(token.replace("W", "")) % 10,
        "is_retracted": False,
        "primary_location": {
            "source": {
                "id": "https://openalex.org/S123",
                "display_name": "Fixture Journal",
            }
        },
        "topics": [{"display_name": name} for name in (topics or ["Music Information Retrieval"])],
        "abstract": abstract or f"Abstract text for {token} with audio and recommendation context.",
    }


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    label_path = tmp_path / "ml-label-dataset-v6.json"
    label_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "paper_id": "https://openalex.org/w200",
                        "work_id": "w200",
                        "relevance_label": "miss",
                        "novelty_label": "",
                        "bridge_like_label": "",
                    },
                    {
                        "paper_id": "https://openalex.org/W201",
                        "work_id": "W201",
                        "relevance_label": "",
                        "novelty_label": "",
                        "bridge_like_label": "",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    conflict_path = tmp_path / "ml-label-conflict-policy.md"
    conflict_path.write_text("# conflict policy\n", encoding="utf-8")
    plan_path = tmp_path / "corpus-v2-candidate-plan-20260428.json"
    plan_path.write_text(
        json.dumps(
            {
                "selected_total": 2,
                "selected_candidates": [
                    {"openalex_id": "https://openalex.org/W100"},
                    {"openalex_id": "W101"},
                ],
            }
        ),
        encoding="utf-8",
    )
    return label_path, conflict_path, plan_path


def _fake_fetch_factory(*, small: bool = False):
    calls: list[str] = []

    def fetch(url: str) -> dict:
        calls.append(url)
        query = parse_qs(urlparse(url).query).get("search", [""])[0]
        if "music recommendation user behavior" in query:
            results = [
                _work("W100", title="Snapshot work should be excluded"),
                _work("W200", title="Already labeled should be excluded"),
                _work("W300", title="Music recommender near miss"),
            ]
        elif "audio representation learning" in query:
            results = [_work("W301", title="Environmental audio classifier", topics=["Environmental Sound"])]
        elif "music therapy recommendation" in query:
            results = [
                _work("W201", title="Seen unlabeled should be excluded"),
                _work("W302", title="Music therapy borderline"),
            ]
        elif "industrial machine fault" in query:
            results = [_work("W303", title="Machine fault audio diagnosis", topics=["Machine Diagnostics"])]
        elif "recommender system personalization" in query:
            results = [_work("W304", title="Generic platform recommender")]
        elif "audio dataset bias" in query:
            results = [_work("W305", title="Machine listening benchmark bias")]
        elif "sound recommendation learning" in query:
            results = [_work("W306", title="Fallback sound learning paper")]
        else:
            results = []
        if small and "music recommendation user behavior" not in query:
            results = []
        return {"meta": {"count": len(results)}, "results": results}

    fetch.calls = calls  # type: ignore[attr-defined]
    return fetch


def _build(tmp_path: Path, *, rows: int = 5, small: bool = False) -> tuple[str, dict, str, dict, list[str]]:
    label_path, conflict_path, plan_path = _write_inputs(tmp_path)
    fetch = _fake_fetch_factory(small=small)
    csv_text, context, markdown, debug = build_external_near_miss_review_worksheet(
        label_dataset_path=label_path,
        conflict_policy_path=conflict_path,
        corpus_snapshot_version="source-snapshot-v2-candidate-plan-20260428",
        candidate_plan_path=plan_path,
        rows=rows,
        seed=123,
        csv_output_path=tmp_path / "worksheet.csv",
        context_output_path=tmp_path / "context.json",
        markdown_output_path=tmp_path / "worksheet.md",
        mailto="reviewer@example.com",
        fetch_json=fetch,
        retrieved_at="2026-05-13T00:00:00Z",
    )
    return csv_text, context, markdown, debug, fetch.calls  # type: ignore[attr-defined]


def _csv_rows(csv_text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(csv_text)))


def test_outside_217_and_v6_exclusions_with_id_normalization(tmp_path: Path) -> None:
    csv_text, context, _markdown, debug, _calls = _build(tmp_path, rows=5)
    tokens = {row["work_id"] for row in _csv_rows(csv_text)}
    assert "W100" not in tokens
    assert "W200" not in tokens
    assert "W201" not in tokens
    assert "W300" in tokens
    assert context["provenance"]["source_snapshot_exclusion_count"] == 2
    assert context["provenance"]["v6_labeled_exclusion_count"] == 1
    assert context["provenance"]["seen_unlabeled_count"] == 1
    assert debug["excluded_counts"]["source_snapshot_exclusion"] == 1
    assert debug["excluded_counts"]["v6_labeled_exclusion"] == 1
    assert debug["excluded_counts"]["v6_seen_unlabeled_exclusion"] == 1
    assert canonical_work_token("https://openalex.org/w123") == "W123"


def test_seen_unlabeled_reporting(tmp_path: Path) -> None:
    _csv_text, context, _markdown, debug, _calls = _build(tmp_path, rows=3)
    assert context["provenance"]["seen_unlabeled_count"] == 1
    assert debug["seen_unlabeled_count"] == 1


def test_deterministic_row_id_and_csv_sidecar_match(tmp_path: Path) -> None:
    csv_text, context, _markdown, _debug, _calls = _build(tmp_path, rows=5)
    csv_rows = _csv_rows(csv_text)
    csv_ids = {row["row_id"] for row in csv_rows}
    sidecar_ids = {row["row_id"] for row in context["rows"]}
    assert csv_ids == sidecar_ids
    for row in csv_rows:
        assert row["row_id"] == stable_row_id(
            worksheet_version=WORKSHEET_VERSION,
            sample_seed=123,
            paper_id=row["paper_id"],
        )
        assert row["worksheet_version"] == WORKSHEET_VERSION
        assert row["review_pool_variant"] == REVIEW_POOL_VARIANT


def test_reviewer_csv_forbidden_fields_absent_and_labels_blank(tmp_path: Path) -> None:
    csv_text, _context, _markdown, _debug, _calls = _build(tmp_path, rows=5)
    reader = csv.DictReader(io.StringIO(csv_text))
    assert tuple(reader.fieldnames or ()) == CSV_COLUMNS
    for hidden in HIDDEN_REVIEWER_CSV_FIELDS:
        assert hidden not in (reader.fieldnames or [])
    assert "source-snapshot-v2-candidate-plan-20260428" not in csv_text
    assert "ranking_run_id" not in csv_text
    assert "final_score" not in csv_text
    for row in _csv_rows(csv_text):
        assert row["relevance_label"] == ""
        assert row["novelty_label"] == ""
        assert row["bridge_like_label"] == ""
        assert row["reviewer_notes"] == ""
        assert row["cluster_id"] == "ext"


def test_sidecar_provenance_includes_query_and_source_metadata(tmp_path: Path) -> None:
    _csv_text, context, markdown, _debug, calls = _build(tmp_path, rows=5)
    provenance = context["provenance"]
    assert provenance["source_snapshot_exclusion_source"]["type"] == "candidate_plan_manifest"
    assert provenance["query_strings"]
    assert provenance["normalized_queries"]
    assert provenance["source_api_metadata"]["provider"] == "OpenAlex"
    assert provenance["source_api_metadata"]["contact_mode"] == "cli"
    assert context["query_metadata"]
    assert "Query Metadata" in markdown
    assert calls
    assert all("api.openalex.org/works" in url for url in calls)
    assert context["rows"][0]["exclusion_checks_passed"]["outside_source_snapshot_217"] is True


def test_closed_sample_reason_vocabulary(tmp_path: Path) -> None:
    csv_text, context, _markdown, _debug, _calls = _build(tmp_path, rows=5)
    csv_reasons = {row["sample_reason"] for row in _csv_rows(csv_text)}
    sidecar_reasons = {row["sample_reason"] for row in context["rows"]}
    assert csv_reasons
    assert csv_reasons <= set(ALLOWED_SAMPLE_REASONS)
    assert sidecar_reasons <= set(ALLOWED_SAMPLE_REASONS)


def test_shortfall_behavior_when_candidate_pool_too_small(tmp_path: Path) -> None:
    csv_text, context, markdown, debug, _calls = _build(tmp_path, rows=4, small=True)
    assert len(_csv_rows(csv_text)) == 1
    assert len(context["rows"]) == 1
    assert debug["shortfall_count"] == 3
    assert context["provenance"]["shortfall_count"] == 3
    assert "Shortfall Or Fallback" in markdown


def test_mocked_openalex_network_calls_used(tmp_path: Path) -> None:
    _csv_text, _context, _markdown, debug, calls = _build(tmp_path, rows=5)
    assert calls
    assert debug["query_count"] == len(calls)


def test_no_db_writes(tmp_path: Path) -> None:
    _csv_text, context, _markdown, debug, _calls = _build(tmp_path, rows=5)
    assert debug["db_access"].startswith("none")
    assert "INSERT " not in json.dumps(context).upper()
    assert "UPDATE " not in json.dumps(context).upper()
    assert "DELETE " not in json.dumps(context).upper()


def test_snapshot_load_failure_raises_clear_error(tmp_path: Path) -> None:
    label_path, conflict_path, _plan_path = _write_inputs(tmp_path)
    with pytest.raises(MLExternalNearMissReviewWorksheetError) as exc:
        build_external_near_miss_review_worksheet(
            label_dataset_path=label_path,
            conflict_policy_path=conflict_path,
            corpus_snapshot_version="source-snapshot-v2-candidate-plan-20260428",
            candidate_plan_path=tmp_path / "missing-plan.json",
            rows=1,
            seed=123,
            csv_output_path=tmp_path / "worksheet.csv",
            context_output_path=tmp_path / "context.json",
            markdown_output_path=tmp_path / "worksheet.md",
            fetch_json=_fake_fetch_factory(),
            retrieved_at="2026-05-13T00:00:00Z",
        )
    message = str(exc.value)
    assert "Cannot construct outside-217 exclusion set" in message
    assert "attempted" in message.casefold()
    assert "works" in message
