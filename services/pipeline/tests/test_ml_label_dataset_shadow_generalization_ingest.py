"""Tests for ml-label-dataset-v11 shadow generalization label ingest."""

from __future__ import annotations

import csv
import copy
import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_label_dataset import (
    MLLabelDatasetError,
    build_ml_label_dataset_v11_shadow_generalization_ingest,
    markdown_from_ml_label_dataset,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]

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
    "ranking_run_id",
    "family",
    "final_score",
    "rank_in_family",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
]

WORKSHEET_VERSION = "ml-shadow-scorer-second-surface-labeling-worksheet-v1"
REVIEW_POOL_VARIANT = "ml_shadow_scorer_second_surface_generalization_v1"
RANKING_RUN_ID = "rank-83787b91ef"
FAMILY = "emerging"
CANDIDATE_SHA = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
SEED = 20260522


def _row_id(work_id: str) -> str:
    return hashlib.sha256(f"{WORKSHEET_VERSION}|{SEED}|{work_id}".encode("utf-8")).hexdigest()


def _base_v10_payload(*, version: str = "ml-label-dataset-v10") -> dict:
    rows = [
        {
            "dataset_version": "ml-label-dataset-v9",
            "row_id": "legacy-conflict-a",
            "paper_id": "https://openalex.org/W1",
            "work_id": "W1",
            "title": "Legacy good",
            "family": "emerging",
            "review_pool_variant": "legacy",
            "source_worksheet_path": "docs/audit/manual-review/legacy.csv",
            "source_row_number": 2,
            "relevance_label": "good",
            "novelty_label": "useful",
            "bridge_like_label": "not_applicable",
            "reviewer_notes": "legacy good",
            "split": "audit_only",
            "good_or_acceptable": True,
            "surprising_or_useful": True,
            "bridge_like_yes_or_partial": None,
        },
        {
            "dataset_version": "ml-label-dataset-v10",
            "row_id": "legacy-conflict-b",
            "paper_id": "https://openalex.org/W1",
            "work_id": "W1",
            "title": "Legacy miss",
            "family": "emerging",
            "review_pool_variant": "legacy",
            "source_worksheet_path": "docs/audit/manual-review/legacy-topup.csv",
            "source_row_number": 2,
            "relevance_label": "miss",
            "novelty_label": "obvious",
            "bridge_like_label": "not_applicable",
            "reviewer_notes": "legacy miss",
            "split": "audit_only",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "bridge_like_yes_or_partial": None,
        },
    ]
    return {
        "dataset_version": version,
        "generated_at": "2026-05-23T00:00:00Z",
        "source_worksheets": ["docs/audit/manual-review/legacy.csv"],
        "source_worksheet_sha256": {"docs/audit/manual-review/legacy.csv": "legacy-sha"},
        "rows": rows,
        "metadata": {
            "dataset_version": version,
            "manual_review_dir": "docs/audit/manual-review",
            "row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 2},
            "included_labeled_row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 2},
            "skipped_blank_row_counts_by_source": {"docs/audit/manual-review/legacy.csv": 0},
            "skipped_blank_worksheets": [],
            "skipped_malformed_rows": [],
            "fresh_hybrid_positive_topup_v1_ingest": {"row_count_appended": 22, "base_row_count": 547},
            "conflicting_label_report": {"conflicting_label_count": 1, "conflicts": []},
        },
    }


def _worksheet_row(index: int, *, labels: bool) -> dict[str, str]:
    work_id = f"W92{index:04d}"
    if index <= 45:
        relevance = "good"
    elif index <= 94:
        relevance = "acceptable"
    elif index <= 157:
        relevance = "miss"
    else:
        relevance = "irrelevant"
    row = {
        "row_id": _row_id(work_id),
        "worksheet_version": WORKSHEET_VERSION,
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "paper_id": f"https://openalex.org/{work_id}",
        "openalex_work_id": work_id,
        "work_id": work_id,
        "title": f"Shadow generalization work {index}",
        "year": "2025",
        "citation_count": str(index),
        "source_slug": "fixture",
        "topics": "Music Information Retrieval;Evaluation",
        "abstract_preview": f"Abstract for shadow generalization work {index}.",
        "sample_reason": "second_surface_high_score_candidate",
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "final_score": f"{1.0 - index / 1000:.6f}",
        "rank_in_family": str(index),
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }
    if labels:
        row["relevance_label"] = relevance
        row["novelty_label"] = "useful" if relevance in {"good", "acceptable"} else "not_useful"
        row["bridge_like_label"] = "not_applicable"
        row["reviewer_notes"] = f"manual boundary note {index}"
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


def _sidecar_payload(rows: list[dict[str, str]]) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_surface_labeling_worksheet",
            "worksheet_version": WORKSHEET_VERSION,
            "review_pool_variant": REVIEW_POOL_VARIANT,
            "seed": SEED,
        },
        "discovery_provenance": {
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "candidate_pool_work_count": 528,
            "candidate_pool_work_set_sha256": CANDIDATE_SHA,
            "confirmatory_metric_eligible_work_count": 168,
        },
        "selection_summary": {
            "selected_row_count": 168,
            "excluded_old_217_count": 217,
            "excluded_first_surface_count": 358,
        },
        "rows": [
            {
                "row_id": row["row_id"],
                "canonical_openalex_work_id": row["work_id"],
                "paper_id": row["paper_id"],
                "openalex_work_id": row["openalex_work_id"],
                "work_id": row["work_id"],
                "ranking_run_id": row["ranking_run_id"],
                "family": row["family"],
                "rank_in_family": row["rank_in_family"],
                "final_score": float(row["final_score"]),
                "sample_reason": row["sample_reason"],
                "split_intent": "future_confirmatory_eval_only",
            }
            for row in rows
        ],
    }


def _discovery_payload(**overrides: object) -> dict:
    payload = {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
        },
        "discovery_summary": {
            "status": "selected_needs_labels",
            "recommended_next_stage": "create_second_surface_labeling_plan_for_shadow_generalization_v1",
        },
        "selected_second_surface": {
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_count": 528,
            "candidate_pool_work_set_sha256": CANDIDATE_SHA,
            "confirmatory_metric_eligible_work_count": 168,
        },
        "threshold_check": {
            "minimum_confirmatory_labeled_work_count": {"observed": 0, "threshold": 100, "passed": False},
            "minimum_confirmatory_positive_work_count": {"observed": 0, "threshold": 50, "passed": False},
            "minimum_confirmatory_negative_work_count": {"observed": 0, "threshold": 20, "passed": False},
            "minimum_distinct_negative_work_count": {"observed": 0, "threshold": 20, "passed": False},
            "minimum_confirmatory_label_coverage_rate": {"observed": 0.0, "threshold": 0.6, "passed": False},
            "unresolved_label_conflicts": {"observed": 0, "threshold": 0, "passed": True},
        },
    }
    for key, value in overrides.items():
        if key == "status":
            payload["discovery_summary"]["status"] = value
        elif key == "ranking_run_id":
            payload["selected_second_surface"]["ranking_run_id"] = value
        elif key == "candidate_sha":
            payload["selected_second_surface"]["candidate_pool_work_set_sha256"] = value
        elif key == "eligible":
            payload["selected_second_surface"]["confirmatory_metric_eligible_work_count"] = value
        else:
            payload[key] = value
    return payload


def _fixture(root: Path) -> dict[str, object]:
    base_payload = _base_v10_payload()
    blank_rows = [_worksheet_row(index, labels=False) for index in range(1, 169)]
    labeled_rows = [_worksheet_row(index, labels=True) for index in range(1, 169)]
    base_path = root / "docs/audit/ml-label-dataset-v10.json"
    blank_path = root / "docs/audit/manual-review/shadow_generalization_second_surface_v1.csv"
    labeled_path = root / "docs/audit/manual-review/shadow_generalization_second_surface_v1_labeled_2026-05-23.csv"
    sidecar_path = root / "docs/audit/manual-review/shadow_generalization_second_surface_v1_context.json"
    discovery_path = root / "docs/audit/ml-shadow-scorer-v1-generalization-second-surface-v1.json"
    conflict_path = root / "docs/audit/ml-label-conflict-policy.md"
    _write_json(base_path, base_payload)
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    _write_json(sidecar_path, _sidecar_payload(labeled_rows))
    _write_json(discovery_path, _discovery_payload())
    conflict_path.write_text("# Conflict policy\n\nDo not silently dedupe.\n", encoding="utf-8")
    return {
        "base_payload": base_payload,
        "blank_rows": blank_rows,
        "labeled_rows": labeled_rows,
        "base_path": base_path,
        "blank_path": blank_path,
        "labeled_path": labeled_path,
        "sidecar_path": sidecar_path,
        "discovery_path": discovery_path,
        "conflict_path": conflict_path,
    }


def _build(root: Path, fixture: dict[str, object] | None = None) -> dict:
    paths = fixture or _fixture(root)
    return build_ml_label_dataset_v11_shadow_generalization_ingest(
        repo_root=root,
        base_dataset_path=paths["base_path"],  # type: ignore[arg-type]
        blank_worksheet_path=paths["blank_path"],  # type: ignore[arg-type]
        labeled_worksheet_path=paths["labeled_path"],  # type: ignore[arg-type]
        context_sidecar_path=paths["sidecar_path"],  # type: ignore[arg-type]
        generalization_second_surface_path=paths["discovery_path"],  # type: ignore[arg-type]
        conflict_policy_path=paths["conflict_path"],  # type: ignore[arg-type]
    )


def test_happy_path_appends_168_rows_and_preserves_sidecar_context(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = _build(tmp_path, paths)

    base_rows = paths["base_payload"]["rows"]  # type: ignore[index]
    appended = payload["rows"][len(base_rows) :]
    ingest = payload["metadata"]["shadow_generalization_second_surface_v1_ingest"]

    assert payload["dataset_version"] == "ml-label-dataset-v11"
    assert payload["rows"][: len(base_rows)] == base_rows
    assert len(appended) == 168
    assert ingest["row_count_appended"] == 168
    assert ingest["output_row_count"] == len(base_rows) + 168
    assert appended[0]["shadow_generalization_second_surface_context"]["row_id"] == appended[0]["row_id"]
    assert appended[0]["dataset_version"] == "ml-label-dataset-v11"
    assert appended[0]["review_pool_variant"] == REVIEW_POOL_VARIANT


def test_rejects_changed_non_review_worksheet_columns(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["title"] = "Changed title"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="changed non-review"):
        _build(tmp_path, paths)


def test_rejects_blank_labels_blank_notes_and_unsupported_enum(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["reviewer_notes"] = ""
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="blank reviewer_notes"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "blanklabel")
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["relevance_label"] = ""
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="blank relevance_label"):
        _build(tmp_path / "blanklabel", paths)

    paths = _fixture(tmp_path / "badlabel")
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["novelty_label"] = "maybe"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="unsupported novelty_label"):
        _build(tmp_path / "badlabel", paths)

    paths = _fixture(tmp_path / "bridge")
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["bridge_like_label"] = "partial"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="bridge_like_label must be 'not_applicable'"):
        _build(tmp_path / "bridge", paths)


def test_rejects_row_id_mismatch_between_blank_labeled_and_context(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[0]["row_id"] = "different"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="row_id set differs"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "context")
    sidecar = _sidecar_payload(paths["labeled_rows"])  # type: ignore[arg-type]
    sidecar["rows"][0]["row_id"] = "different"
    _write_json(paths["sidecar_path"], sidecar)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="sidecar row_id set differs"):
        _build(tmp_path / "context", paths)


def test_rejects_duplicate_row_id_or_duplicate_work_id(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    labeled_rows[1]["row_id"] = labeled_rows[0]["row_id"]
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="duplicate row_id"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "dupwork")
    labeled_rows = copy.deepcopy(paths["labeled_rows"])  # type: ignore[arg-type]
    duplicate = labeled_rows[0]["work_id"]
    labeled_rows[1]["paper_id"] = f"https://openalex.org/{duplicate}"
    labeled_rows[1]["openalex_work_id"] = duplicate
    labeled_rows[1]["work_id"] = duplicate
    labeled_rows[1]["row_id"] = _row_id(duplicate + "X")
    sidecar = _sidecar_payload(labeled_rows)
    sidecar["rows"][1]["canonical_openalex_work_id"] = duplicate
    _write_csv(paths["blank_path"], [{**row, "relevance_label": "", "novelty_label": "", "bridge_like_label": "", "reviewer_notes": ""} for row in labeled_rows])  # type: ignore[arg-type]
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    _write_json(paths["sidecar_path"], sidecar)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="duplicate shadow-generalization work_id"):
        _build(tmp_path / "dupwork", paths)


def test_rejects_wrong_base_dataset_version(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    _write_json(paths["base_path"], _base_v10_payload(version="ml-label-dataset-v9"))  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="expected 'ml-label-dataset-v10'"):
        _build(tmp_path, paths)


def test_rejects_wrong_discovery_status_run_sha_or_eligible_count(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    _write_json(paths["discovery_path"], _discovery_payload(status="selected_ready_for_generalization_audit"))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="status"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "run")
    _write_json(paths["discovery_path"], _discovery_payload(ranking_run_id="rank-other"))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="ranking_run_id"):
        _build(tmp_path / "run", paths)

    paths = _fixture(tmp_path / "sha")
    _write_json(paths["discovery_path"], _discovery_payload(candidate_sha="different"))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="candidate_pool_work_set_sha256"):
        _build(tmp_path / "sha", paths)

    paths = _fixture(tmp_path / "eligible")
    _write_json(paths["discovery_path"], _discovery_payload(eligible=167))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="confirmatory_metric_eligible_work_count"):
        _build(tmp_path / "eligible", paths)


def test_derived_counts_and_thresholds_are_recorded(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    ingest = payload["metadata"]["shadow_generalization_second_surface_v1_ingest"]

    assert ingest["good_count"] == 45
    assert ingest["acceptable_count"] == 49
    assert ingest["miss_count"] == 63
    assert ingest["irrelevant_count"] == 11
    assert ingest["positive_count"] == 94
    assert ingest["negative_count"] == 74
    assert ingest["label_thresholds_passed"] is True
    assert ingest["label_threshold_summary"]["checks"]["minimum_confirmatory_positive_work_count"]["passed"] is True


def test_conflict_report_is_preserved_and_reported_not_deduped(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    metadata = payload["metadata"]
    ingest = metadata["shadow_generalization_second_surface_v1_ingest"]

    assert metadata["conflicting_label_report"]["conflicting_label_count"] >= 1
    assert ingest["conflict_report_policy"]["silent_deduplication_used"] is False
    assert ingest["conflict_report_policy"]["post_ingest_conflict_report_location"] == "metadata.conflicting_label_report"


def test_markdown_includes_shadow_generalization_section(tmp_path: Path) -> None:
    markdown = markdown_from_ml_label_dataset(_build(tmp_path))

    assert "Shadow generalization second surface v1 ingest" in markdown
    assert "learned probability coverage remains separate" in markdown
    assert "ml-label-dataset-v11-shadow-generalization-ingest" in markdown


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    out_json = tmp_path / "docs/audit/ml-label-dataset-v11.json"
    out_md = tmp_path / "docs/audit/ml-label-dataset-v11.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-label-dataset-v11-shadow-generalization-ingest",
        "--base-dataset",
        str(paths["base_path"]),
        "--blank-worksheet",
        str(paths["blank_path"]),
        "--labeled-worksheet",
        str(paths["labeled_path"]),
        "--context-sidecar",
        str(paths["sidecar_path"]),
        "--generalization-second-surface",
        str(paths["discovery_path"]),
        "--conflict-policy",
        str(paths["conflict_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["metadata"]["shadow_generalization_second_surface_v1_ingest"]["row_count_appended"] == 168
    assert "Shadow generalization second surface v1 ingest" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_label_dataset.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "openalex_client" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = read_cli_parser_source(PACKAGE_ROOT)
    start = cli_source.index('"ml-label-dataset-v11-shadow-generalization-ingest"')
    end = cli_source.index("ml_offline_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
