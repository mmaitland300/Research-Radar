"""Tests for cross-pool text baseline diagnostic."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_text_baseline_cross_pool import (
    CAVEATS,
    MLTextBaselineCrossPoolError,
    build_ml_text_baseline_cross_pool_payload,
    render_markdown,
)


SLICE_VARIANTS = {
    "external": "ml_external_near_miss_audit",
    "blind": "ml_blind_snapshot_audit",
    "rank": "full_family_top_k",
    "hard": "ml_hard_negative_audit",
    "legacy": None,
}


def _embedding_row(row_id: str, value: int, *, status: str = "ok", fmt: str = "format-a") -> dict:
    token = f"W{10000 + value}"
    return {
        "row_id": row_id,
        "paper_id": f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": token,
        "review_pool_variant": "unused",
        "family": None,
        "ranking_run_id": None,
        "embedding_text_format_version": fmt,
        "text_source": "openalex_fetch",
        "text_sha256": f"sha-{row_id}",
        "text_length": 100,
        "embedding": [float(value), float(value % 2), float(value % 3)],
        "embedding_status": status,
    }


def _label_row(
    row_id: str,
    *,
    variant: str | None,
    family: str | None,
    goa: bool | None,
    sou: bool | None,
    sample_reason: str = "reason",
) -> dict:
    token = "W" + "".join(ch for ch in row_id if ch.isdigit()).rjust(5, "0")
    return {
        "dataset_version": "ml-label-dataset-v7",
        "row_id": row_id,
        "paper_id": f"https://openalex.org/{token}",
        "openalex_work_id": token,
        "work_id": token,
        "review_pool_variant": variant,
        "family": family,
        "sample_reason": sample_reason,
        "good_or_acceptable": goa,
        "surprising_or_useful": sou,
    }


def _rows() -> tuple[list[dict], list[dict]]:
    embeddings: list[dict] = []
    labels: list[dict] = []
    idx = 0
    specs = [
        ("external", 8, "external_text_corpus_v7_verbatim", None),
        ("blind", 8, "labeled_text_corpus_v1_openalex_title_abstract", None),
        ("rank", 8, "labeled_text_corpus_v1_openalex_title_abstract", "emerging"),
        ("hard", 4, "labeled_text_corpus_v1_openalex_title_abstract", None),
        ("legacy", 4, "labeled_text_corpus_v1_openalex_title_abstract", "bridge"),
    ]
    for slice_key, count, fmt, family in specs:
        for local in range(count):
            idx += 1
            row_id = f"{slice_key}-{local:02d}"
            if slice_key == "hard":
                goa = True
                sou = local % 2 == 0
            else:
                goa = local % 2 == 0
                sou = local % 3 != 0
            value = 10 if goa else -10
            embeddings.append(_embedding_row(row_id, value=value + local, fmt=fmt))
            labels.append(
                _label_row(
                    row_id,
                    variant=SLICE_VARIANTS[slice_key],
                    family=family,
                    goa=goa,
                    sou=sou,
                    sample_reason=f"{slice_key}-reason-{local % 2}",
                )
            )
    return embeddings, labels


def _embedding_payload(rows: list[dict] | None = None) -> dict:
    actual = rows if rows is not None else _rows()[0]
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v1",
            "embedding_dimensions": 3,
        },
        "rows": actual,
    }


def _label_payload(rows: list[dict] | None = None, *, version: str = "ml-label-dataset-v7") -> dict:
    actual = rows if rows is not None else _rows()[1]
    return {"dataset_version": version, "rows": actual}


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
    return (
        _write_json(tmp_path, "embeddings.json", embeddings or _embedding_payload()),
        _write_json(tmp_path, "labels.json", labels or _label_payload()),
    )


def test_cross_pool_payload_reports_slices_metrics_and_histograms(tmp_path: Path) -> None:
    emb_path, label_path = _write_inputs(tmp_path)
    payload = build_ml_text_baseline_cross_pool_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=7,
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["artifact_type"] == "ml_text_baseline_cross_pool"
    assert payload["join_summary"]["joined_rows"] == 32
    target = payload["per_target"]["good_or_acceptable"]
    assert target["excluded_count"] == 0
    assert target["slice_counts"]["external_near_miss"] == {"positive": 4, "negative": 4, "n": 8}
    assert target["in_pool_cv"]["external_near_miss"]["effective_cv_folds"] == 4
    assert target["in_pool_cv"]["hard_negative"]["skipped"] is True
    transfer = target["source_transfer"]["external_near_miss_to_blind_snapshot"]
    assert transfer["skipped"] is False
    assert set(transfer["models"]) == {
        "embedding_logistic",
        "majority_train_baseline",
        "train_prevalence_score_baseline",
    }
    assert transfer["models"]["embedding_logistic"]["roc_auc"] is not None
    assert transfer["models"]["train_prevalence_score_baseline"]["roc_auc"] is None
    assert transfer["train_histograms"]["embedding_text_format_version"] == {"external_text_corpus_v7_verbatim": 8}
    assert transfer["test_histograms"]["embedding_text_format_version"] == {
        "labeled_text_corpus_v1_openalex_title_abstract": 8
    }
    assert "metadata_sample_reason_logistic" in target["in_pool_cv"]["external_near_miss"]["models"]
    assert "metadata_sample_reason_logistic" not in transfer["models"]


def test_join_uniqueness_and_version_failures(tmp_path: Path) -> None:
    embeddings, labels = _rows()
    embeddings[1]["row_id"] = embeddings[0]["row_id"]
    emb_path, label_path = _write_inputs(tmp_path, embeddings=_embedding_payload(embeddings))
    with pytest.raises(MLTextBaselineCrossPoolError, match="duplicate row_id"):
        build_ml_text_baseline_cross_pool_payload(embeddings_path=emb_path, label_dataset_path=label_path)

    embeddings, labels = _rows()
    labels[1]["row_id"] = labels[0]["row_id"]
    emb_path, label_path = _write_inputs(tmp_path, labels=_label_payload(labels))
    with pytest.raises(MLTextBaselineCrossPoolError, match="duplicate row_id"):
        build_ml_text_baseline_cross_pool_payload(embeddings_path=emb_path, label_dataset_path=label_path)

    embeddings, labels = _rows()
    embeddings[0]["row_id"] = "extra"
    emb_path, label_path = _write_inputs(tmp_path, embeddings=_embedding_payload(embeddings))
    with pytest.raises(MLTextBaselineCrossPoolError, match="missing_labels=1, extra_labels=1"):
        build_ml_text_baseline_cross_pool_payload(embeddings_path=emb_path, label_dataset_path=label_path)

    emb_path, label_path = _write_inputs(tmp_path, labels=_label_payload(version="wrong"))
    with pytest.raises(MLTextBaselineCrossPoolError, match="dataset_version"):
        build_ml_text_baseline_cross_pool_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_non_ok_embedding_and_boolean_filtering(tmp_path: Path) -> None:
    embeddings, labels = _rows()
    embeddings[0]["embedding_status"] = "mock"
    emb_path, label_path = _write_inputs(tmp_path, embeddings=_embedding_payload(embeddings))
    with pytest.raises(MLTextBaselineCrossPoolError, match="not ok"):
        build_ml_text_baseline_cross_pool_payload(embeddings_path=emb_path, label_dataset_path=label_path)

    embeddings, labels = _rows()
    labels[0]["good_or_acceptable"] = None
    emb_path, label_path = _write_inputs(tmp_path, embeddings=_embedding_payload(embeddings), labels=_label_payload(labels))
    payload = build_ml_text_baseline_cross_pool_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=7,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert payload["per_target"]["good_or_acceptable"]["excluded_count"] == 1
    assert payload["per_target"]["good_or_acceptable"]["excluded_row_ids"] == [labels[0]["row_id"]]
    assert payload["per_target"]["surprising_or_useful"]["excluded_count"] == 0


def test_unknown_slice_fails_and_legacy_null_pool_is_assigned(tmp_path: Path) -> None:
    emb_path, label_path = _write_inputs(tmp_path)
    payload = build_ml_text_baseline_cross_pool_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=7,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert payload["per_target"]["good_or_acceptable"]["slice_counts"]["legacy_or_uncategorized"]["n"] == 4

    embeddings, labels = _rows()
    labels[0]["review_pool_variant"] = "new_pool"
    emb_path, label_path = _write_inputs(tmp_path, labels=_label_payload(labels))
    with pytest.raises(MLTextBaselineCrossPoolError, match="did not match any declared source slice"):
        build_ml_text_baseline_cross_pool_payload(embeddings_path=emb_path, label_dataset_path=label_path)


def test_deterministic_output_and_markdown(tmp_path: Path) -> None:
    emb_path, label_path = _write_inputs(tmp_path)
    left = build_ml_text_baseline_cross_pool_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=11,
        generated_at="2026-05-14T00:00:00Z",
    )
    right = build_ml_text_baseline_cross_pool_payload(
        embeddings_path=emb_path,
        label_dataset_path=label_path,
        random_seed=11,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert left == right
    md = render_markdown(left)
    for caveat in CAVEATS:
        assert caveat in md
    assert "ml_emerging_target_gap_audit:good_or_acceptable" in md
    assert "Not A Production Recommender Test" in md
    assert "What This Means" in md


def test_no_database_or_network_usage_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_text_baseline_cross_pool.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "urllib" not in module_source
    assert "requests" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-text-baseline-cross-pool"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
