"""Tests for ml-label-dataset-v14 bridge shadow-pilot label ingest."""

from __future__ import annotations

import copy
import csv
import hashlib
import json
from pathlib import Path

import pytest

from pipeline.ml_label_dataset import (
    MLLabelDatasetError,
    build_ml_label_dataset_v14_bridge_shadow_pilot_ingest,
    markdown_from_ml_label_dataset,
)


CSV_COLUMNS = [
    "work_id",
    "title",
    "abstract_preview",
    "current_family_rank",
    "hybrid_rank",
    "disagreement_bucket",
    "bridge_like_label",
    "relevance_label",
    "notes",
]

WORKSHEET_VERSION = "ml-bridge-shadow-pilot-disagreements-v1"
CONTEXT_ARTIFACT_TYPE = "ml_bridge_shadow_pilot_disagreements_context"
REVIEW_POOL_VARIANT = "ml_bridge_shadow_pilot_audit"
RANKING_RUN_ID = "rank-5a7efa5ca3"


def _row_id(work_id_url: str) -> str:
    return hashlib.sha256(f"{WORKSHEET_VERSION}|{RANKING_RUN_ID}|{work_id_url}".encode("utf-8")).hexdigest()


def _base_v13_payload() -> dict:
    rows = []
    for index in range(1, 838):
        work_token = f"WBASE{index:04d}"
        paper_id = "https://openalex.org/W9000001" if index == 1 else f"https://openalex.org/{work_token}"
        rows.append(
            {
                "dataset_version": "ml-label-dataset-v13",
                "row_id": f"legacy-{index}",
                "paper_id": paper_id,
                "work_id": paper_id.rsplit("/", 1)[-1],
                "title": f"Legacy row {index}",
                "ranking_run_id": "rank-83787b91ef",
                "family": "bridge",
                "review_pool_variant": "ml_bridge_top_ranked_validation_audit",
                "source_worksheet_path": "docs/audit/manual-review/legacy.csv",
                "source_row_number": index + 1,
                "relevance_label": "good" if index == 1 else "miss",
                "novelty_label": "useful",
                "bridge_like_label": "no" if index == 1 else "not_applicable",
                "reviewer_notes": f"legacy note {index}",
                "split": "audit_only",
                "good_or_acceptable": index == 1,
                "surprising_or_useful": True,
                "bridge_like_yes_or_partial": False if index == 1 else None,
                "bridge_recommendable": False if index == 1 else None,
            }
        )
    return {
        "dataset_version": "ml-label-dataset-v13",
        "generated_at": "2026-06-01T00:00:00Z",
        "source_worksheets": ["docs/audit/manual-review/legacy.csv"],
        "source_worksheet_sha256": {"docs/audit/manual-review/legacy.csv": "legacy-sha"},
        "rows": rows,
        "metadata": {
            "dataset_version": "ml-label-dataset-v13",
            "manual_review_dir": "docs/audit/manual-review",
            "row_counts_by_source": {"docs/audit/manual-review/legacy.csv": len(rows)},
            "included_labeled_row_counts_by_source": {"docs/audit/manual-review/legacy.csv": len(rows)},
            "skipped_blank_row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 0},
            "skipped_blank_worksheets": [],
            "skipped_malformed_rows": [],
            "duplicate_paper_id_report": {"duplicate_paper_id_count": 0, "duplicate_paper_ids": []},
            "conflicting_label_report": {"conflicting_label_count": 0, "conflicts": []},
            "derived_target_conflict_report": {"derived_target_conflict_count": 0, "conflicts": []},
            "bridge_top_ranked_v1_ingest": {
                "row_count_appended": 30,
                "output_row_count": 837,
                "ranking_run_id": "rank-83787b91ef",
                "family": "bridge",
                "review_pool_variant": "ml_bridge_top_ranked_validation_audit",
                "label_distribution": {
                    "relevance_label": {"good": 28, "acceptable": 2},
                    "bridge_like_label": {"yes": 5, "partial": 10, "no": 15},
                },
            },
        },
    }


def _bucket_for_index(index: int) -> tuple[str, bool]:
    if index <= 20:
        return "promoted_by_hybrid", index <= 14
    if index <= 40:
        return "demoted_by_hybrid", index <= 28
    if index <= 50:
        return "high_ml_low_bridge_score", True
    return "high_bridge_score_low_ml", index <= 52


def _shadow_rows(*, labels: bool = True) -> list[dict[str, str]]:
    positives_seen = 0
    negative_relevance = ["good"] * 14 + ["acceptable"] * 5 + ["miss"] * 3 + ["irrelevant"] * 4
    negatives_seen = 0
    rows: list[dict[str, str]] = []
    for index in range(1, 61):
        bucket, positive = _bucket_for_index(index)
        work_token = f"W900{index:04d}"
        if index == 1:
            work_token = "W9000001"
        if index == 49:
            work_token = "W4413990340"
        if index == 50:
            work_token = "W7131735100"
        if index == 51:
            work_token = "W7112868420"
        if index == 52:
            work_token = "W7125951231"
        work_id_url = f"https://openalex.org/{work_token}"
        relevance = ""
        bridge = ""
        notes = ""
        if labels:
            if positive:
                positives_seen += 1
                relevance = "good"
                bridge = "yes" if positives_seen <= 19 else "partial"
            else:
                relevance = negative_relevance[negatives_seen]
                negatives_seen += 1
                bridge = "no"
            notes = f"shadow-pilot note {index}"
        rows.append(
            {
                "work_id": work_id_url,
                "title": f"Shadow pilot row {index}",
                "abstract_preview": "" if not labels else f"Cleaned abstract {index}",
                "current_family_rank": str(100 + index),
                "hybrid_rank": str(index),
                "disagreement_bucket": bucket,
                "bridge_like_label": bridge,
                "relevance_label": relevance,
                "notes": notes,
            }
        )
    return rows


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _sidecar_payload(rows: list[dict[str, str]]) -> dict:
    return {
        "artifact_type": CONTEXT_ARTIFACT_TYPE,
        "worksheet_version": WORKSHEET_VERSION,
        "ranking_run_id": RANKING_RUN_ID,
        "embedding_version": "shadow-generalization-text-embedding-v1",
        "generated_at": "2026-06-02T00:00:00Z",
        "rows": [
            {
                "work_id": row["work_id"],
                "title": row["title"],
                "disagreement_bucket": row["disagreement_bucket"],
                "current_family_rank": int(row["current_family_rank"]),
                "hybrid_rank": int(row["hybrid_rank"]),
                "work_id_int": index,
                "ml_probability": 0.99 - index / 1000,
                "bridge_score": 0.80 + index / 1000,
                "hybrid_score": 0.90 - index / 1000,
                "ml_rank_pct": 1.0 - index / 100,
                "bridge_score_rank_pct": 0.5 + index / 200,
            }
            for index, row in enumerate(rows, start=1)
        ],
    }


def _fixture(root: Path) -> dict[str, object]:
    base_payload = _base_v13_payload()
    blank_rows = _shadow_rows(labels=False)
    labeled_rows = _shadow_rows(labels=True)
    base_path = root / "docs/audit/ml-label-dataset-v13.json"
    blank_path = root / "docs/audit/manual-review/bridge_shadow_pilot_rank-5a7efa5ca3_v1_blank.csv"
    labeled_path = root / "docs/audit/manual-review/bridge_shadow_pilot_rank-5a7efa5ca3_v1_labeled.csv"
    sidecar_path = root / "docs/audit/manual-review/bridge_shadow_pilot_rank-5a7efa5ca3_v1_context.json"
    _write_json(base_path, base_payload)
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    _write_json(sidecar_path, _sidecar_payload(labeled_rows))
    return {
        "base_payload": base_payload,
        "blank_rows": blank_rows,
        "labeled_rows": labeled_rows,
        "base_path": base_path,
        "blank_path": blank_path,
        "labeled_path": labeled_path,
        "sidecar_path": sidecar_path,
    }


def _build(root: Path, fixture: dict[str, object] | None = None) -> dict:
    paths = fixture or _fixture(root)
    return build_ml_label_dataset_v14_bridge_shadow_pilot_ingest(
        repo_root=root,
        base_dataset_path=paths["base_path"],  # type: ignore[arg-type]
        blank_worksheet_path=paths["blank_path"],  # type: ignore[arg-type]
        labeled_worksheet_path=paths["labeled_path"],  # type: ignore[arg-type]
        context_sidecar_path=paths["sidecar_path"],  # type: ignore[arg-type]
    )


def test_happy_path_v14_ingest_with_60_rows(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = _build(tmp_path, paths)
    appended = payload["rows"][837:]
    ingest = payload["metadata"]["bridge_shadow_pilot_v1_ingest"]

    assert payload["dataset_version"] == "ml-label-dataset-v14"
    assert len(payload["rows"]) == 897
    assert len(appended) == 60
    assert ingest["row_count_appended"] == 60
    assert ingest["output_row_count"] == 897
    assert appended[0]["row_id"] == _row_id(appended[0]["paper_id"])
    assert appended[0]["work_id"] == "W9000001"
    assert appended[0]["internal_work_id"] == 1
    assert appended[0]["novelty_label"] is None
    assert appended[0]["reviewer_notes"] == "shadow-pilot note 1"
    assert appended[0]["bridge_shadow_pilot_context"]["work_id"] == appended[0]["paper_id"]


def test_rejects_malformed_csv_wrong_row_count(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    _write_csv(paths["labeled_path"], copy.deepcopy(paths["labeled_rows"])[:-1])  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="exactly 60"):
        _build(tmp_path, paths)


def test_rejects_missing_context_row(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    sidecar = json.loads(Path(paths["sidecar_path"]).read_text(encoding="utf-8"))  # type: ignore[arg-type]
    sidecar["rows"].pop()
    _write_json(paths["sidecar_path"], sidecar)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="exactly 60 context rows"):
        _build(tmp_path, paths)


def test_rejects_missing_labels_and_notes(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["bridge_like_label"] = ""
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="blank bridge_like_label"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["notes"] = ""
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="blank notes"):
        _build(tmp_path, paths)


def test_rejects_invalid_bridge_like_and_relevance_label(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["bridge_like_label"] = "maybe"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="unsupported bridge_like_label"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["relevance_label"] = "maybe"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="unsupported relevance_label"):
        _build(tmp_path, paths)


def test_rejects_positive_bridge_label_with_miss_or_irrelevant_relevance(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["bridge_like_label"] = "yes"
    labeled_rows[0]["relevance_label"] = "miss"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="positive bridge label"):
        _build(tmp_path, paths)


def test_validates_blank_labeled_stable_fields_but_allows_abstract_preview_drift(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    assert payload["metadata"]["bridge_shadow_pilot_v1_ingest"]["validation_summary"]["abstract_preview_drift_allowed"] is True

    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["title"] = "Changed title"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="stable template field"):
        _build(tmp_path, paths)


def test_records_near_duplicate_notes_without_deduping(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    notes = payload["metadata"]["near_duplicate_companion_paper_notes"]
    work_ids = {row["work_id"] for row in payload["rows"][-60:]}

    assert notes
    assert {"W4413990340", "W7131735100", "W7112868420", "W7125951231"}.issubset(work_ids)
    assert len(payload["rows"][-60:]) == 60


def test_appends_overlapping_v13_work_ids_and_records_conflicts(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    ingest = payload["metadata"]["bridge_shadow_pilot_v1_ingest"]

    assert ingest["overlap_count_with_v13"] >= 1
    assert "https://openalex.org/W9000001" in payload["metadata"]["duplicate_paper_id_report"]["duplicate_paper_ids"]
    assert payload["metadata"]["conflicting_label_report"]["conflicting_label_count"] >= 1
    assert payload["metadata"]["derived_target_conflict_report"]["derived_target_conflict_count"] >= 1


def test_strict_slice_count_checks_pass_and_markdown_mentions_v14(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    ingest = payload["metadata"]["bridge_shadow_pilot_v1_ingest"]

    assert ingest["label_distribution"]["bridge_like_label"] == {"no": 26, "partial": 15, "yes": 19}
    assert ingest["label_distribution"]["relevance_label"] == {
        "acceptable": 5,
        "good": 48,
        "irrelevant": 4,
        "miss": 3,
    }
    assert ingest["bridge_recommendable_positive_count"] == 34
    assert ingest["bridge_recommendable_negative_count"] == 26
    assert ingest["disagreement_bucket_counts"] == {
        "demoted_by_hybrid": 20,
        "high_bridge_score_low_ml": 10,
        "high_ml_low_bridge_score": 10,
        "promoted_by_hybrid": 20,
    }

    markdown = markdown_from_ml_label_dataset(payload)
    assert "Bridge shadow-pilot v1 ingest" in markdown
    assert "ml_bridge_shadow_pilot_audit" in markdown
    assert "do not authorize Bridge serving" in markdown
