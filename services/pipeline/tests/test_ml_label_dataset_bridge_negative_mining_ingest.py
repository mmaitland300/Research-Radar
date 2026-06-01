"""Tests for ml-label-dataset-v12 bridge negative-mining label ingest."""

from __future__ import annotations

import copy
import csv
import hashlib
import json
from pathlib import Path

import pytest

from pipeline.ml_label_dataset import (
    MLLabelDatasetError,
    build_ml_label_dataset_v12_bridge_negative_mining_ingest,
    markdown_from_ml_label_dataset,
)


CSV_COLUMNS = [
    "row_id",
    "worksheet_version",
    "review_pool_variant",
    "paper_id",
    "openalex_work_id",
    "work_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "abstract_preview",
    "sample_reason",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
]

WORKSHEET_VERSION = "ml-bridge-negative-mining-v1"
REVIEW_POOL_VARIANT = "ml_bridge_negative_mining_audit"
RANKING_RUN_ID = "rank-83787b91ef"
FAMILY = "bridge"
SEED = 20260531


def _row_id(paper_id: str) -> str:
    return hashlib.sha256(f"{WORKSHEET_VERSION}|{SEED}|{paper_id}".encode("utf-8")).hexdigest()


def _base_v11_payload() -> dict:
    rows = [
        {
            "dataset_version": "ml-label-dataset-v11",
            "row_id": "legacy-overlap",
            "paper_id": "https://openalex.org/W9900001",
            "work_id": "W9900001",
            "title": "Legacy overlap",
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "review_pool_variant": "legacy_bridge",
            "source_worksheet_path": "docs/audit/manual-review/legacy.csv",
            "source_row_number": 2,
            "relevance_label": "good",
            "novelty_label": "useful",
            "bridge_like_label": "yes",
            "reviewer_notes": "legacy overlap",
            "split": "audit_only",
            "good_or_acceptable": True,
            "surprising_or_useful": True,
            "bridge_like_yes_or_partial": True,
        },
        {
            "dataset_version": "ml-label-dataset-v11",
            "row_id": "legacy-nullish",
            "paper_id": "https://openalex.org/W123",
            "work_id": "W123",
            "title": "Legacy nullish",
            "ranking_run_id": RANKING_RUN_ID,
            "family": "emerging",
            "review_pool_variant": "legacy_emerging",
            "source_worksheet_path": "docs/audit/manual-review/legacy.csv",
            "source_row_number": 3,
            "relevance_label": "miss",
            "novelty_label": "not_useful",
            "bridge_like_label": "not_applicable",
            "reviewer_notes": "legacy miss",
            "split": "audit_only",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "bridge_like_yes_or_partial": None,
        },
    ]
    return {
        "dataset_version": "ml-label-dataset-v11",
        "generated_at": "2026-05-31T00:00:00Z",
        "source_worksheets": ["docs/audit/manual-review/legacy.csv"],
        "source_worksheet_sha256": {"docs/audit/manual-review/legacy.csv": "legacy-sha"},
        "rows": rows,
        "metadata": {
            "dataset_version": "ml-label-dataset-v11",
            "manual_review_dir": "docs/audit/manual-review",
            "row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 2},
            "included_labeled_row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 2},
            "skipped_blank_row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 0},
            "skipped_blank_worksheets": [],
            "skipped_malformed_rows": [],
            "duplicate_paper_id_report": {"duplicate_paper_id_count": 0, "duplicate_paper_ids": []},
            "conflicting_label_report": {"conflicting_label_count": 0, "conflicts": []},
            "shadow_generalization_second_surface_v1_ingest": {
                "row_count_appended": 168,
                "output_row_count": 170,
                "ranking_run_id": RANKING_RUN_ID,
                "family": "emerging",
                "candidate_pool_work_set_sha256": "candidate-sha",
                "source_row_number_convention": "physical CSV line including header; first data row = 2",
                "label_distribution": {
                    "relevance_label": {"good": 94, "miss": 74},
                    "novelty_label": {"useful": 94, "not_useful": 74},
                    "bridge_like_label": {"not_applicable": 168},
                },
                "positive_count": 94,
                "negative_count": 74,
                "label_thresholds_passed": True,
            },
        },
    }


def _labels_for_index(index: int) -> tuple[str, str, str]:
    if index <= 33:
        relevance = "good"
    elif index <= 60:
        relevance = "acceptable"
    elif index <= 66:
        relevance = "miss"
    else:
        relevance = "irrelevant"
    bridge = "yes" if index <= 13 else ("partial" if index <= 38 else "no")
    novelty = "useful" if relevance in {"good", "acceptable"} else "not_useful"
    return relevance, novelty, bridge


def _worksheet_row(index: int, *, labels: bool) -> dict[str, str]:
    work_id = f"W990{index:04d}"
    paper_id = f"https://openalex.org/{work_id}"
    relevance, novelty, bridge = _labels_for_index(index)
    sample_reason = (
        "bridge_deep_cut"
        if index <= 24
        else ("bridge_suppressed_final" if index <= 47 else "corpus_blind_seeded_fill")
    )
    row = {
        "row_id": _row_id(paper_id),
        "worksheet_version": WORKSHEET_VERSION,
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "paper_id": paper_id,
        "openalex_work_id": work_id,
        "work_id": work_id,
        "title": f"Bridge negative-mining work {index}",
        "year": "2026",
        "citation_count": str(index),
        "source_slug": "fixture",
        "topics": "Music Information Retrieval;Signal Processing",
        "abstract_preview": f"Abstract for bridge negative-mining work {index}.",
        "sample_reason": sample_reason,
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }
    if labels:
        row["relevance_label"] = relevance
        row["novelty_label"] = novelty
        row["bridge_like_label"] = bridge
        row["reviewer_notes"] = f"bridge negative-mining note {index}"
    return row


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _sidecar_payload(rows: list[dict[str, str]], *, base_sha: str) -> dict:
    return {
        "artifact_type": "ml_bridge_negative_mining_v1_context",
        "provenance": {
            "worksheet_version": WORKSHEET_VERSION,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "sample_seed": SEED,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": "docs/audit/ml-label-dataset-v11.json",
            "label_dataset_sha256": base_sha,
            "ranking_run_id": RANKING_RUN_ID,
            "requested_rows": 70,
            "achieved_rows": 70,
        },
        "sampling_debug": {
            "sample_reason_counts": {
                "bridge_deep_cut": 24,
                "bridge_suppressed_final": 23,
                "corpus_blind_seeded_fill": 23,
            },
        },
        "rows": [
            {
                "row_id": row["row_id"],
                "paper_id": row["paper_id"],
                "openalex_work_id": row["openalex_work_id"],
                "internal_work_id": index,
                "sample_seed": SEED,
                "sample_reason": row["sample_reason"],
                "family": FAMILY,
                "family_rank": 20 + index,
                "ranking_run_id": RANKING_RUN_ID,
                "ranking_version": "shadow-generalization-product-candidate-ranking-v1",
                "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
                "embedding_version": "shadow-generalization-text-embedding-v1",
                "cluster_version": "",
                "final_score": round(0.7 - index / 1000, 6),
                "semantic_score": None,
                "citation_velocity_score": 0.1,
                "topic_growth_score": 0.2,
                "bridge_score": None,
                "diversity_penalty": 0.0,
                "bridge_eligible": None,
                "reason_short": f"ranking context {index}",
            }
            for index, row in enumerate(rows, start=1)
        ],
    }


def _fixture(root: Path) -> dict[str, object]:
    base_payload = _base_v11_payload()
    blank_rows = [_worksheet_row(index, labels=False) for index in range(1, 71)]
    labeled_rows = [_worksheet_row(index, labels=True) for index in range(1, 71)]
    base_path = root / "docs/audit/ml-label-dataset-v11.json"
    blank_path = root / "docs/audit/manual-review/bridge_negative_mining_rank-83787b91ef_v1.csv"
    labeled_path = root / "docs/audit/manual-review/bridge_negative_mining_rank-83787b91ef_v1_labeled.csv"
    sidecar_path = root / "docs/audit/manual-review/bridge_negative_mining_rank-83787b91ef_v1_context.json"
    conflict_path = root / "docs/audit/ml-label-conflict-policy.md"
    _write_json(base_path, base_payload)
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    base_sha = hashlib.sha256(base_path.read_bytes()).hexdigest()
    _write_json(sidecar_path, _sidecar_payload(labeled_rows, base_sha=base_sha))
    conflict_path.write_text("# Conflict policy\n\nDo not silently dedupe.\n", encoding="utf-8")
    return {
        "base_payload": base_payload,
        "blank_rows": blank_rows,
        "labeled_rows": labeled_rows,
        "base_path": base_path,
        "blank_path": blank_path,
        "labeled_path": labeled_path,
        "sidecar_path": sidecar_path,
        "conflict_path": conflict_path,
    }


def _build(root: Path, fixture: dict[str, object] | None = None) -> dict:
    paths = fixture or _fixture(root)
    return build_ml_label_dataset_v12_bridge_negative_mining_ingest(
        repo_root=root,
        base_dataset_path=paths["base_path"],  # type: ignore[arg-type]
        blank_worksheet_path=paths["blank_path"],  # type: ignore[arg-type]
        labeled_worksheet_path=paths["labeled_path"],  # type: ignore[arg-type]
        context_sidecar_path=paths["sidecar_path"],  # type: ignore[arg-type]
        conflict_policy_path=paths["conflict_path"],  # type: ignore[arg-type]
    )


def test_happy_path_appends_70_rows_and_preserves_v11_rows(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = _build(tmp_path, paths)
    base_rows = copy.deepcopy(paths["base_payload"]["rows"])  # type: ignore[index]
    for row in base_rows:
        row["bridge_recommendable"] = row["row_id"] == "legacy-overlap" or False

    appended = payload["rows"][len(base_rows) :]
    ingest = payload["metadata"]["bridge_negative_mining_v1_ingest"]

    assert payload["dataset_version"] == "ml-label-dataset-v12"
    assert payload["rows"][: len(base_rows)] == base_rows
    assert len(appended) == 70
    assert ingest["row_count_appended"] == 70
    assert ingest["output_row_count"] == len(base_rows) + 70
    assert appended[0]["family"] == "bridge"
    assert appended[0]["family_rank"] == 21
    assert appended[0]["bridge_negative_mining_context"]["row_id"] == appended[0]["row_id"]
    assert appended[0]["reason_short"] == "ranking context 1"


def test_bridge_recommendable_backfilled_on_all_v12_rows(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert all("bridge_recommendable" in row for row in payload["rows"])
    assert payload["rows"][0]["bridge_recommendable"] is True
    assert payload["rows"][1]["bridge_recommendable"] is False


def test_expected_counts_and_global_overlap_reported_not_rejected(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    ingest = payload["metadata"]["bridge_negative_mining_v1_ingest"]

    assert ingest["label_distribution"]["bridge_like_label"] == {"no": 32, "partial": 25, "yes": 13}
    assert ingest["label_distribution"]["relevance_label"] == {
        "acceptable": 27,
        "good": 33,
        "irrelevant": 4,
        "miss": 6,
    }
    assert ingest["bridge_recommendable_positive_count"] == 38
    assert ingest["bridge_recommendable_negative_count"] == 32
    assert ingest["hard_negative_count"] == 22
    assert payload["metadata"]["duplicate_paper_id_report"]["duplicate_paper_id_count"] >= 1
    assert "https://openalex.org/W9900001" in payload["metadata"]["duplicate_paper_id_report"]["duplicate_paper_ids"]


def test_rejects_row_id_mismatch_between_csv_and_sidecar(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    sidecar = json.loads(Path(paths["sidecar_path"]).read_text(encoding="utf-8"))  # type: ignore[arg-type]
    sidecar["rows"][0]["row_id"] = "different"
    _write_json(paths["sidecar_path"], sidecar)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="sidecar row_id set differs"):
        _build(tmp_path, paths)


def test_rejects_blank_labeled_non_review_column_drift(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["title"] = "Changed title"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="changed non-review"):
        _build(tmp_path, paths)


def test_rejects_invalid_label_token(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["relevance_label"] = "maybe"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="unsupported relevance_label"):
        _build(tmp_path, paths)


def test_markdown_includes_bridge_negative_mining_v12_section(tmp_path: Path) -> None:
    markdown = markdown_from_ml_label_dataset(_build(tmp_path))

    assert "Bridge negative-mining v1 ingest" in markdown
    assert "offline bridge scorer trainable" in markdown
    assert "review_pool_variant == ml_bridge_negative_mining_audit" in markdown
    assert "not validation, production readiness, or a serving change" in markdown
