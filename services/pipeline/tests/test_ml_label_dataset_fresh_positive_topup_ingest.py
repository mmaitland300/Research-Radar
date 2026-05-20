"""Tests for ml-label-dataset-v10 fresh hybrid positive top-up ingest."""

from __future__ import annotations

import csv
import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_fresh_eval_positive_topup_worksheet_hybrid import stable_row_id as topup_row_id
from pipeline.ml_label_dataset import (
    MLLabelDatasetError,
    build_ml_label_dataset_v10_fresh_positive_topup_ingest,
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
    "ranking_run_id",
    "family",
    "final_score",
    "rank_in_family",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
]


def _base_v9_payload() -> dict:
    rows = [
        {
            "dataset_version": "ml-label-dataset-v4",
            "row_id": "legacy-row",
            "paper_id": "https://openalex.org/W1",
            "work_id": "W1",
            "title": "Legacy row",
            "family": "emerging",
            "review_pool_variant": "legacy",
            "source_worksheet_path": "docs/audit/manual-review/legacy.csv",
            "source_row_number": 2,
            "relevance_label": "good",
            "novelty_label": "useful",
            "bridge_like_label": "yes",
            "reviewer_notes": "legacy",
            "split": "audit_only",
            "good_or_acceptable": True,
            "surprising_or_useful": True,
            "bridge_like_yes_or_partial": True,
        },
        {
            "dataset_version": "ml-label-dataset-v9",
            "row_id": "fresh-v9-row",
            "paper_id": "https://openalex.org/W2",
            "work_id": "W2",
            "title": "Fresh v9 row",
            "family": "emerging",
            "review_pool_variant": "ml_fresh_hybrid_eval_v1",
            "source_worksheet_path": "docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv",
            "source_row_number": 2,
            "relevance_label": "irrelevant",
            "novelty_label": "neither",
            "bridge_like_label": "not_applicable",
            "reviewer_notes": "base",
            "split": "audit_only",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "bridge_like_yes_or_partial": None,
        },
    ]
    return {
        "dataset_version": "ml-label-dataset-v9",
        "generated_at": "2026-05-20T00:00:00Z",
        "source_worksheets": ["docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv"],
        "source_worksheet_sha256": {
            "docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv": "base-sha"
        },
        "rows": rows,
        "metadata": {
            "manual_review_dir": "docs/audit/manual-review",
            "row_counts_by_source": {
                "docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv": 1
            },
            "included_labeled_row_counts_by_source": {
                "docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv": 1
            },
            "skipped_blank_row_counts_by_source": {
                "docs/audit/manual-review/fresh_hybrid_eval_v1_labeled_2026-05-19.csv": 0
            },
            "skipped_blank_worksheets": [],
            "skipped_malformed_rows": [],
            "fresh_hybrid_v1_ingest": {"row_count_appended": 120, "base_row_count": 427},
        },
    }


def _worksheet_row(index: int, *, labels: bool) -> dict[str, str]:
    work_id = f"W900{index:03d}"
    row_id = topup_row_id(
        worksheet_version="ml-fresh-eval-positive-topup-worksheet-hybrid-v1",
        seed=20260519,
        canonical_openalex_work_id=work_id,
    )
    row = {
        "row_id": row_id,
        "worksheet_version": "ml-fresh-eval-positive-topup-worksheet-hybrid-v1",
        "review_pool_variant": "ml_fresh_hybrid_positive_topup_v1",
        "paper_id": f"https://openalex.org/{work_id}",
        "openalex_work_id": work_id,
        "work_id": work_id,
        "title": f"Positive top-up fixture {index}",
        "year": "2026",
        "citation_count": str(index),
        "source_slug": "fixture_source",
        "topics": "fixture topic",
        "abstract_preview": f"abstract {index}",
        "sample_reason": "fresh_hybrid_positive_topup",
        "ranking_run_id": "rank-9f4b2a2084",
        "family": "emerging",
        "final_score": f"{0.2 + index / 1000:.6f}",
        "rank_in_family": str(80 + index),
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }
    if labels:
        positive = index <= 15
        row["relevance_label"] = "good" if index <= 10 else ("acceptable" if positive else "miss")
        row["novelty_label"] = "useful" if positive else "obvious"
        row["bridge_like_label"] = "partial" if positive else "not_applicable"
        row["reviewer_notes"] = f"manual note {index}"
    return row


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _sidecar_payload(rows: list[dict[str, str]], *, artifact_type: str = "ml_fresh_eval_positive_topup_worksheet_hybrid") -> dict:
    return {
        "metadata": {
            "artifact_type": artifact_type,
            "worksheet_version": "ml-fresh-eval-positive-topup-worksheet-hybrid-v1",
            "review_pool_variant": "ml_fresh_hybrid_positive_topup_v1",
            "seed": 20260519,
            "source_surface_summary": {
                "candidate_work_set_sha256": "surface-sha",
                "ranking_run_id": "rank-9f4b2a2084",
                "family": "emerging",
                "snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
                "positive_work_count": 39,
            },
            "positive_threshold_before_labeling": {
                "observed": 39,
                "threshold": 50,
                "deficit": 11,
                "passed": False,
            },
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
                "final_score": row["final_score"],
                "sample_reason": row["sample_reason"],
                "split_intent": "future_confirmatory_eval_only",
            }
            for row in rows
        ],
    }


def _surface_payload(*, candidate_sha: str = "surface-sha", label_version: str = "ml-label-dataset-v9") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "label_dataset_version": label_version,
            "expected_label_dataset_version": label_version,
        },
        "candidate_pool": {"candidate_work_set_sha256": candidate_sha},
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _fixture(root: Path) -> dict[str, Path | list[dict[str, str]] | dict]:
    base_payload = _base_v9_payload()
    blank_rows = [_worksheet_row(i, labels=False) for i in range(1, 23)]
    labeled_rows = [_worksheet_row(i, labels=True) for i in range(1, 23)]
    base_path = root / "docs/audit/ml-label-dataset-v9.json"
    blank_path = root / "docs/audit/manual-review/fresh_hybrid_positive_topup_v1.csv"
    labeled_path = root / "docs/audit/manual-review/fresh_hybrid_positive_topup_v1_labeled_2026-05-20.csv"
    sidecar_path = root / "docs/audit/manual-review/fresh_hybrid_positive_topup_v1_context.json"
    conflict_path = root / "docs/audit/ml-label-conflict-policy.md"
    surface_path = root / "docs/audit/ml-fresh-eval-surface-hybrid-v1.json"
    _write_json(base_path, base_payload)
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    _write_json(sidecar_path, _sidecar_payload(labeled_rows))
    conflict_path.write_text("# Conflict Policy\n\nNo merge.\n", encoding="utf-8")
    _write_json(surface_path, _surface_payload())
    return {
        "base_path": base_path,
        "blank_path": blank_path,
        "labeled_path": labeled_path,
        "sidecar_path": sidecar_path,
        "conflict_path": conflict_path,
        "surface_path": surface_path,
        "base_payload": base_payload,
        "blank_rows": blank_rows,
        "labeled_rows": labeled_rows,
    }


def _build(root: Path, fixture: dict | None = None) -> dict:
    paths = fixture or _fixture(root)
    return build_ml_label_dataset_v10_fresh_positive_topup_ingest(
        repo_root=root,
        base_dataset_path=paths["base_path"],  # type: ignore[arg-type]
        blank_worksheet_path=paths["blank_path"],  # type: ignore[arg-type]
        labeled_worksheet_path=paths["labeled_path"],  # type: ignore[arg-type]
        context_sidecar_path=paths["sidecar_path"],  # type: ignore[arg-type]
        conflict_policy_path=paths["conflict_path"],  # type: ignore[arg-type]
        fresh_eval_surface_path=paths["surface_path"],  # type: ignore[arg-type]
    )


def test_happy_path_appends_22_and_keeps_v9_rows_deep_equal(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = _build(tmp_path, paths)

    base_rows = paths["base_payload"]["rows"]  # type: ignore[index]
    assert payload["dataset_version"] == "ml-label-dataset-v10"
    assert len(payload["rows"]) == len(base_rows) + 22
    assert payload["rows"][: len(base_rows)] == base_rows
    assert payload["metadata"]["fresh_hybrid_positive_topup_v1_ingest"]["row_count_appended"] == 22


def test_blank_labeled_non_review_mismatch_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])
    labeled_rows[0]["title"] = "changed title"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]

    with pytest.raises(MLLabelDatasetError, match="changed non-review"):
        _build(tmp_path, paths)


def test_missing_label_or_notes_and_invalid_closed_set_fail(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    labeled_rows = copy.deepcopy(paths["labeled_rows"])
    labeled_rows[0]["reviewer_notes"] = ""
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="blank reviewer_notes"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "invalid")
    labeled_rows = copy.deepcopy(paths["labeled_rows"])
    labeled_rows[0]["bridge_like_label"] = "maybe"
    _write_csv(paths["labeled_path"], labeled_rows)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="unsupported bridge_like_label"):
        _build(tmp_path / "invalid", paths)


def test_sidecar_row_id_mismatch_and_wrong_artifact_type_fail(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    sidecar = _sidecar_payload(paths["labeled_rows"])  # type: ignore[arg-type]
    sidecar["rows"][0]["row_id"] = "different"
    _write_json(paths["sidecar_path"], sidecar)  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="row_id set differs"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "badtype")
    _write_json(paths["sidecar_path"], _sidecar_payload(paths["labeled_rows"], artifact_type="wrong"))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="metadata.artifact_type"):
        _build(tmp_path / "badtype", paths)


def test_source_row_number_versions_context_and_derived_booleans(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    payload = _build(tmp_path, paths)
    base_count = len(paths["base_payload"]["rows"])  # type: ignore[index]
    row = payload["rows"][base_count]
    sidecar_first = json.loads(Path(paths["sidecar_path"]).read_text(encoding="utf-8"))["rows"][0]  # type: ignore[arg-type]

    assert row["source_row_number"] == 2
    assert row["dataset_version"] == "ml-label-dataset-v10"
    assert payload["rows"][0]["dataset_version"] == "ml-label-dataset-v4"
    assert row["fresh_hybrid_positive_topup_context"] == sidecar_first
    assert row["good_or_acceptable"] is True
    assert row["surprising_or_useful"] is True
    assert row["bridge_like_yes_or_partial"] is True
    assert row["rank"] == row["rank_in_family"]
    assert row["final_score"] == "0.201000"


def test_optional_fresh_eval_surface_validation_failures(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    _write_json(paths["surface_path"], _surface_payload(candidate_sha="different"))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="candidate_work_set_sha256"):
        _build(tmp_path, paths)

    paths = _fixture(tmp_path / "v10surface")
    _write_json(paths["surface_path"], _surface_payload(label_version="ml-label-dataset-v10"))  # type: ignore[arg-type]
    with pytest.raises(MLLabelDatasetError, match="label_dataset_version"):
        _build(tmp_path / "v10surface", paths)


def test_existing_ingest_metadata_preserved_and_projection_recorded(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    metadata = payload["metadata"]
    ingest = metadata["fresh_hybrid_positive_topup_v1_ingest"]

    assert metadata["fresh_hybrid_v1_ingest"] == {"row_count_appended": 120, "base_row_count": 427}
    assert ingest["label_distribution"]["relevance_label"] == {"acceptable": 5, "good": 10, "miss": 7}
    assert ingest["good_or_acceptable_positive_count"] == 15
    assert ingest["good_or_acceptable_negative_count"] == 7
    assert ingest["projected_positive_work_count_if_unique"] == 54
    assert ingest["projection_formula"] == "39 + count(appended good_or_acceptable)"
    assert ingest["context_preserved_field_name"] == "fresh_hybrid_positive_topup_context"
    assert ingest["positive_threshold_before_labeling"] == {
        "observed": 39,
        "threshold": 50,
        "deficit": 11,
        "passed": False,
    }
    input_names = {item["name"] for item in metadata["inputs"]}
    assert {
        "base_dataset",
        "blank_worksheet",
        "labeled_worksheet",
        "context_sidecar",
        "conflict_policy",
        "fresh_eval_surface",
    } <= input_names


def test_markdown_includes_positive_topup_section(tmp_path: Path) -> None:
    markdown = markdown_from_ml_label_dataset(_build(tmp_path))

    assert "Fresh hybrid positive top-up v1 ingest" in markdown
    assert "Projected positive count" in markdown
    assert "54 = 39 + 15" in markdown
    assert "materializer" in markdown.lower()


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    out_json = tmp_path / "docs/audit/ml-label-dataset-v10.json"
    out_md = tmp_path / "docs/audit/ml-label-dataset-v10.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-label-dataset-v10-fresh-positive-topup-ingest",
        "--base-dataset",
        str(paths["base_path"]),
        "--blank-worksheet",
        str(paths["blank_path"]),
        "--labeled-worksheet",
        str(paths["labeled_path"]),
        "--context-sidecar",
        str(paths["sidecar_path"]),
        "--conflict-policy",
        str(paths["conflict_path"]),
        "--fresh-eval-surface",
        str(paths["surface_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["metadata"]["fresh_hybrid_positive_topup_v1_ingest"]["row_count_appended"] == 22
    markdown = out_md.read_text(encoding="utf-8")
    assert "Fresh hybrid positive top-up v1 ingest" in markdown
    assert "copied from v9 unchanged" in markdown


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_label_dataset.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "postgres" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-label-dataset-v10-fresh-positive-topup-ingest"')
    end = cli_source.index("ml_offline_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
    assert "--split-policy" not in parser_block
