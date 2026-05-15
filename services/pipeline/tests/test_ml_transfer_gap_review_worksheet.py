"""Tests for transfer-gap review worksheet generation."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.ml_blind_snapshot_review_worksheet import BlindCandidate
from pipeline.ml_transfer_gap_review_worksheet import (
    ALLOWED_SAMPLE_REASONS,
    CSV_COLUMNS,
    DEFAULT_SAMPLE_SEED,
    HIDDEN_REVIEWER_CSV_FIELDS,
    REVIEW_POOL_VARIANT,
    WORKSHEET_VERSION,
    MLTransferGapReviewWorksheetError,
    build_transfer_gap_review_worksheet,
    compute_slot_budget,
    stable_row_id,
)


def _work(token: str, *, title: str | None = None) -> dict:
    return {
        "id": f"https://openalex.org/{token}",
        "title": title or f"Transfer candidate {token}",
        "publication_year": 2025,
        "publication_date": "2025-01-01",
        "cited_by_count": int(token.replace("W", "")) % 25,
        "type": "article",
        "language": "en",
        "primary_location": {"source": {"display_name": "Fixture Journal", "id": "https://openalex.org/S1"}},
        "topics": [{"display_name": "Music Recommendation"}, {"display_name": "Audio"}],
        "abstract": f"Abstract for {token} about music recommendation transfer gaps and audio relevance.",
    }


def _fake_fetch(url: str) -> dict:
    query = parse_qs(urlparse(url).query).get("search", [""])[0]
    if "music recommendation user behavior" in query:
        results = [_work("W100"), _work("W200"), _work("W300"), _work("W301")]
    elif "music information retrieval" in query:
        results = [_work("W302"), _work("W303")]
    elif "audio representation learning" in query:
        results = [_work("W304"), _work("W305")]
    elif "music therapy" in query:
        results = [_work("W306")]
    else:
        results = []
    return {"meta": {"count": len(results)}, "results": results}


def _small_fetch(url: str) -> dict:
    query = parse_qs(urlparse(url).query).get("search", [""])[0]
    if "music recommendation user behavior" in query:
        return {"meta": {"count": 1}, "results": [_work("W300")]}
    return {"meta": {"count": 0}, "results": []}


def _plan_payload(*, status: str = "research_only", include_next: bool = True) -> dict:
    next_artifacts = [{"name": "ml-transfer-gap-review-worksheet"}] if include_next else []
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
            "plan_schema_version": 1,
            "overall_status": status,
        },
        "label_gaps": [
            {
                "priority": "P1",
                "target": "surprising_or_useful",
                "pool": "ml_external_near_miss_audit + ml_blind_snapshot_audit",
            },
            {
                "priority": "P2",
                "target": "good_or_acceptable",
                "pool": "ml_external_near_miss_audit + ml_blind_snapshot_audit",
            },
            {"priority": "P3", "target": "good_or_acceptable", "pool": "full_family_top_k"},
            {"priority": "P3", "target": "surprising_or_useful", "pool": "ml_hard_negative_audit"},
        ],
        "next_artifacts": next_artifacts,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v7",
        "rows": [
            {
                "row_id": "labeled-1",
                "paper_id": "https://openalex.org/W200",
                "work_id": "W200",
                "openalex_work_id": "W200",
                "relevance_label": "good",
                "novelty_label": "",
                "bridge_like_label": "",
            },
            {
                "row_id": "unlabeled-1",
                "paper_id": "https://openalex.org/W8000002",
                "work_id": "W8000002",
                "openalex_work_id": "W8000002",
                "relevance_label": "",
                "novelty_label": "",
                "bridge_like_label": "",
            },
        ],
    }


def _candidate_plan() -> dict:
    return {
        "selected_total": 1,
        "selected_candidates": [{"openalex_id": "https://openalex.org/W100"}],
    }


def _db_candidate(index: int, token: str | None = None) -> BlindCandidate:
    actual = token or f"W800000{index}"
    return BlindCandidate(
        internal_work_id=8000 + index,
        paper_id=f"https://openalex.org/{actual}",
        work_token=actual,
        title=f"DB sparse candidate {actual}",
        year=2024,
        citation_count=index,
        source_slug="db_fixture",
        work_type="article",
        cluster_id=f"c{index % 3}",
        topics=("Audio", "Recommendation"),
        abstract=f"DB abstract {index} for sparse transfer pool.",
        family_scores={"emerging": 0.1 + index / 100},
        family_ranks={"emerging": index + 1},
    )


def _write_inputs(tmp_path: Path, *, blocked: bool = False) -> tuple[Path, Path, Path, Path]:
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps(_plan_payload(status="blocked" if blocked else "research_only"), indent=2), encoding="utf-8")
    labels = tmp_path / "labels.json"
    labels.write_text(json.dumps(_label_payload(), indent=2), encoding="utf-8")
    policy = tmp_path / "policy.md"
    policy.write_text("# Conflict policy\nNo silent merge.\n", encoding="utf-8")
    candidate_plan = tmp_path / "candidate-plan.json"
    candidate_plan.write_text(json.dumps(_candidate_plan(), indent=2), encoding="utf-8")
    return plan, labels, policy, candidate_plan


def _build(
    tmp_path: Path,
    *,
    rows: int = 10,
    fetch_json=_fake_fetch,
    db_candidates: list[BlindCandidate] | None = None,
    blocked: bool = False,
) -> tuple[str, dict, str, dict]:
    plan, labels, policy, candidate_plan = _write_inputs(tmp_path, blocked=blocked)
    return build_transfer_gap_review_worksheet(
        production_readiness_plan_path=plan,
        label_dataset_path=labels,
        conflict_policy_path=policy,
        output_path=tmp_path / "worksheet.csv",
        context_output_path=tmp_path / "context.json",
        markdown_output_path=tmp_path / "worksheet.md",
        rows=rows,
        seed=123,
        source_snapshot_candidate_plan_path=candidate_plan,
        corpus_snapshot_version="source-snapshot-v2-candidate-plan-20260428",
        mailto="reviewer@example.com",
        mock_openalex=False,
        ranking_run_id="rank-x",
        embedding_version="emb-v",
        cluster_version="cluster-v",
        database_url=None,
        mock_db=True,
        fetch_json=fetch_json,
        db_candidates=db_candidates if db_candidates is not None else [_db_candidate(i) for i in range(1, 8)],
        retrieved_at="2026-05-15T00:00:00Z",
    )


def _csv_rows(csv_text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(csv_text)))


def test_constants_and_quota_math() -> None:
    assert WORKSHEET_VERSION == "ml-transfer-gap-review-v1"
    assert REVIEW_POOL_VARIANT == "ml_transfer_gap_audit"
    assert DEFAULT_SAMPLE_SEED == 20260515
    assert set(ALLOWED_SAMPLE_REASONS) == {
        "transfer_gap_external_blind_balance",
        "transfer_gap_good_or_acceptable_balance",
        "transfer_gap_sparse_pool_negative",
        "transfer_gap_rank_shaped_boundary",
        "fallback_seeded_fill",
    }
    assert compute_slot_budget(60) == {"P1": 21, "P2": 24, "P3": 15}
    assert compute_slot_budget(3) == {"P1": 1, "P2": 1, "P3": 1}
    with pytest.raises(MLTransferGapReviewWorksheetError):
        compute_slot_budget(0)


def test_blocked_plan_fails(tmp_path: Path) -> None:
    with pytest.raises(MLTransferGapReviewWorksheetError, match="blocked"):
        _build(tmp_path, blocked=True)


def test_csv_sidecar_row_id_parity_and_exclusions(tmp_path: Path) -> None:
    csv_text, context, _md, debug = _build(tmp_path, rows=10)
    rows = _csv_rows(csv_text)
    csv_ids = {row["row_id"] for row in rows}
    sidecar_ids = {row["row_id"] for row in context["rows"]}
    assert csv_ids == sidecar_ids
    assert {row["work_id"] for row in rows}.isdisjoint({"W100", "W200"})
    for row in rows:
        assert row["row_id"] == stable_row_id(
            worksheet_version=WORKSHEET_VERSION,
            sample_seed=123,
            paper_id=row["paper_id"],
        )
        assert row["review_pool_variant"] == REVIEW_POOL_VARIANT
    assert debug["requested_slots"] == {"P1": 4, "P2": 4, "P3": 2}
    assert context["provenance"]["requested_slots"] == {"P1": 4, "P2": 4, "P3": 2}
    assert context["provenance"]["exclusion_counts"]["source_snapshot_exclusion"] == 1
    assert context["provenance"]["exclusion_counts"]["v7_labeled_exclusion"] == 1


def test_reviewer_csv_forbidden_fields_absent_and_labels_blank(tmp_path: Path) -> None:
    csv_text, _context, _md, _debug = _build(tmp_path, rows=10)
    reader = csv.DictReader(io.StringIO(csv_text))
    assert tuple(reader.fieldnames or ()) == CSV_COLUMNS
    for hidden in HIDDEN_REVIEWER_CSV_FIELDS:
        assert hidden not in (reader.fieldnames or [])
    assert "ranking_run_id" not in csv_text
    assert "gap_source_pool" not in csv_text
    assert "internal_work_id" not in csv_text
    for row in _csv_rows(csv_text):
        assert row["relevance_label"] == ""
        assert row["novelty_label"] == ""
        assert row["bridge_like_label"] == ""
        assert row["reviewer_notes"] == ""


def test_gap_source_pool_only_on_p3_and_dedupe_prefers_external(tmp_path: Path) -> None:
    csv_text, context, _md, _debug = _build(tmp_path, rows=10, db_candidates=[_db_candidate(1, token="W300"), _db_candidate(2)])
    sidecar_by_token = {row["work_id"]: row for row in context["rows"]}
    assert sidecar_by_token["W300"]["gap_priority"] == "P1"
    for row in context["rows"]:
        if row["gap_priority"] == "P3":
            assert row["gap_source_pool"] in {"full_family_top_k", "ml_hard_negative_audit"}
            assert row["acquisition_channel"] == "postgres_in_snapshot"
        else:
            assert row["gap_source_pool"] is None
            assert row["acquisition_channel"] == "openalex_external"
    assert len(_csv_rows(csv_text)) == len(context["rows"])


def test_shortfall_when_candidates_are_insufficient(tmp_path: Path) -> None:
    csv_text, context, md, debug = _build(tmp_path, rows=8, fetch_json=_small_fetch, db_candidates=[])
    assert len(_csv_rows(csv_text)) == 1
    assert context["provenance"]["shortfall_counts"]["P1"] > 0
    assert context["provenance"]["shortfall_counts"]["P2"] > 0
    assert context["provenance"]["shortfall_counts"]["P3"] > 0
    assert debug["shortfall_counts"]["P3"] > 0
    assert "shortfall" in md.lower()


def test_no_mutating_sql_and_markdown_caveats(tmp_path: Path) -> None:
    _csv_text, context, md, _debug = _build(tmp_path, rows=10)
    serialized = json.dumps(context).upper()
    assert "INSERT " not in serialized
    assert "UPDATE " not in serialized
    assert "DELETE " not in serialized
    assert "surprising_or_useful is deferred" in md
    assert "good_or_acceptable` is research-only" in md
    assert "future v8 ingest" in md
