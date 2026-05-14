"""Tests for external near-miss text-only baseline diagnostic."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pipeline.ml_text_only_baseline import (
    CAVEATS,
    MLTextOnlyBaselineError,
    REVIEW_POOL_VARIANT,
    build_ml_text_only_baseline_payload,
    render_markdown,
)


def _embedding_row(i: int, *, row_id: str | None = None, status: str = "ok") -> dict:
    rid = row_id or f"row-{i:03d}"
    token = f"W900{i:03d}"
    return {
        "row_id": rid,
        "paper_id": f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": token,
        "text_sha256": f"sha-{i}",
        "text_length": 100 + i,
        "embedding": [float(i % 7), float((i * 3) % 11), float(i % 2)],
        "embedding_status": status,
    }


def _label_row(i: int, *, row_id: str | None = None, goa: bool | None = None, sou: bool | None = None) -> dict:
    rid = row_id or f"row-{i:03d}"
    token = f"W900{i:03d}"
    return {
        "dataset_version": "ml-label-dataset-v7",
        "row_id": rid,
        "paper_id": f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": token,
        "title": f"Fixture title {i}",
        "review_pool_variant": REVIEW_POOL_VARIANT,
        "sample_reason": f"reason-{i % 3}",
        "good_or_acceptable": (i % 4 == 0) if goa is None else goa,
        "surprising_or_useful": (i % 3 == 0) if sou is None else sou,
    }


def _embedding_payload(*, rows: list[dict] | None = None) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_external_text_embeddings",
            "embedding_artifact_version": "ml-external-text-embeddings-v7",
            "source_text_corpus_version": "ml-external-text-corpus-v7",
            "source_text_corpus_sha256": "corpus-sha",
            "embedding_dimensions": 3,
            "embedding_model": "fixture-model",
            "row_count": 60,
        },
        "rows": rows if rows is not None else [_embedding_row(i) for i in range(1, 61)],
    }


def _label_payload(*, rows: list[dict] | None = None) -> dict:
    return {
        "dataset_version": "ml-label-dataset-v7",
        "rows": rows if rows is not None else [_label_row(i) for i in range(1, 61)],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _write_inputs(
    tmp_path: Path,
    *,
    embeddings: dict | None = None,
    labels: dict | None = None,
) -> tuple[Path, Path]:
    emb_path = _write_json(tmp_path, "embeddings.json", embeddings or _embedding_payload())
    label_path = _write_json(tmp_path, "labels.json", labels or _label_payload())
    return emb_path, label_path


def test_text_only_baseline_joins_rows_and_reports_metrics(tmp_path: Path) -> None:
    emb_path, label_path = _write_inputs(tmp_path)
    payload = build_ml_text_only_baseline_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=13,
        cv_folds=5,
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["artifact_type"] == "ml_text_only_baseline_external"
    assert payload["join_summary"]["joined_rows"] == 60
    assert payload["metadata"]["sklearn_version"]
    target = payload["per_target"]["good_or_acceptable"]
    assert target["class_counts"] == {"positive": 15, "negative": 45, "null": 0, "total": 60}
    assert target["effective_cv_folds"] == 5
    assert set(target["models"]) == {
        "embedding_logistic",
        "metadata_sample_reason_logistic",
        "majority_class",
        "stratified_random_prevalence",
    }
    metrics = target["models"]["embedding_logistic"]["aggregate_metrics"]
    assert 0.0 <= metrics["accuracy"] <= 1.0
    assert metrics["roc_auc"] is not None
    majority = target["models"]["majority_class"]["aggregate_metrics"]
    assert majority["roc_auc"] is None
    assert "row-specific probabilistic" in majority["roc_auc_reason"]


def test_duplicate_row_id_failure(tmp_path: Path) -> None:
    emb = _embedding_payload()
    emb["rows"][1]["row_id"] = emb["rows"][0]["row_id"]
    emb_path, label_path = _write_inputs(tmp_path, embeddings=emb)
    with pytest.raises(MLTextOnlyBaselineError, match="duplicate row_id"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)

    labels = _label_payload()
    labels["rows"][1]["row_id"] = labels["rows"][0]["row_id"]
    emb_path, label_path = _write_inputs(tmp_path, labels=labels)
    with pytest.raises(MLTextOnlyBaselineError, match="duplicate row_id"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_mismatched_embedding_label_keys_fail(tmp_path: Path) -> None:
    emb = _embedding_payload()
    emb["rows"][0]["row_id"] = "row-extra"
    emb_path, label_path = _write_inputs(tmp_path, embeddings=emb)
    with pytest.raises(MLTextOnlyBaselineError, match="missing_embeddings=1, extra_embeddings=1"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_mock_or_non_ok_embedding_rows_fail(tmp_path: Path) -> None:
    emb = _embedding_payload()
    emb["rows"][3]["embedding_status"] = "mock"
    emb_path, label_path = _write_inputs(tmp_path, embeddings=emb)
    with pytest.raises(MLTextOnlyBaselineError, match="not ok"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_boolean_target_validation(tmp_path: Path) -> None:
    labels = _label_payload()
    labels["rows"][4]["good_or_acceptable"] = None
    emb_path, label_path = _write_inputs(tmp_path, labels=labels)
    with pytest.raises(MLTextOnlyBaselineError, match="non-boolean"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_deterministic_output_with_fixed_seed(tmp_path: Path) -> None:
    emb_path, label_path = _write_inputs(tmp_path)
    left = build_ml_text_only_baseline_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=99,
        cv_folds=5,
        generated_at="2026-05-14T00:00:00Z",
    )
    right = build_ml_text_only_baseline_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=99,
        cv_folds=5,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert left == right


def test_cv_fold_reduction_when_class_count_is_small(tmp_path: Path) -> None:
    labels = _label_payload(
        rows=[
            _label_row(i, goa=(i <= 2), sou=(i <= 2))
            for i in range(1, 61)
        ]
    )
    emb_path, label_path = _write_inputs(tmp_path, labels=labels)
    payload = build_ml_text_only_baseline_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        cv_folds=5,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert payload["per_target"]["good_or_acceptable"]["effective_cv_folds"] == 2
    assert payload["per_target"]["surprising_or_useful"]["effective_cv_folds"] == 2


def test_wrong_embedding_artifact_and_dimension_failures(tmp_path: Path) -> None:
    emb = _embedding_payload()
    emb["metadata"]["artifact_type"] = "wrong"
    emb_path, label_path = _write_inputs(tmp_path, embeddings=emb)
    with pytest.raises(MLTextOnlyBaselineError, match="artifact_type"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)

    emb = _embedding_payload()
    emb["rows"][0]["embedding"] = [1.0, 2.0]
    emb_path, label_path = _write_inputs(tmp_path, embeddings=emb)
    with pytest.raises(MLTextOnlyBaselineError, match="invalid vector"):
        build_ml_text_only_baseline_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_markdown_includes_caveats_and_production_warning(tmp_path: Path) -> None:
    emb_path, label_path = _write_inputs(tmp_path)
    payload = build_ml_text_only_baseline_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        generated_at="2026-05-14T00:00:00Z",
    )
    md = render_markdown(payload)
    for caveat in CAVEATS:
        assert caveat in md
    assert "Not A Production Recommender Test" in md
    assert "Production-grade evaluation" in md
    assert "embedding" not in md.lower() or "[" not in md


def test_no_database_or_network_usage_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_text_only_baseline.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "urllib" not in module_source
    assert "requests" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-text-only-baseline"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
