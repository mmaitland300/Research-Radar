"""Tests for text transfer readiness synthesis."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_text_transfer_readiness import (
    CAVEATS,
    NO_REEMBED_NEEDED_CONCLUSION,
    PRODUCTION_RECOMMENDER_MISSING_GATES,
    MLTextTransferReadinessError,
    build_ml_text_transfer_readiness_payload,
    render_markdown,
)


def _comparison(
    *,
    target: str,
    skipped: bool = False,
    reason: str | None = None,
    ba: float = 0.75,
    auc: float = 0.85,
) -> dict:
    return {
        "comparison_name": "fixture",
        "comparison_type": "fixture",
        "target": target,
        "skipped": skipped,
        "skip_reason": reason,
        "train_histograms": {"review_pool_variant": {"a": 1}},
        "test_histograms": {"review_pool_variant": {"b": 1}},
        "models": {}
        if skipped
        else {
            "embedding_logistic": {
                "train_n": 10,
                "train_pos": 5,
                "train_neg": 5,
                "test_n": 8,
                "test_pos": 4,
                "test_neg": 4,
                "accuracy": 0.7,
                "balanced_accuracy": ba,
                "macro_f1": 0.68,
                "roc_auc": auc,
                "roc_auc_skip_reason": None,
                "confusion": {"tn": 3, "fp": 1, "fn": 2, "tp": 2},
            },
            "majority_train_baseline": {
                "balanced_accuracy": 0.5,
                "macro_f1": 0.33,
                "roc_auc": None,
                "confusion": {"tn": 4, "fp": 0, "fn": 4, "tp": 0},
            },
        },
    }


def _cross_pool_payload() -> dict:
    per_target = {}
    for target in ("good_or_acceptable", "surprising_or_useful"):
        per_target[target] = {
            "target": target,
            "eligible_row_count": 6,
            "excluded_count": 0,
            "excluded_row_ids": [],
            "slice_counts": {},
            "in_pool_cv": {
                "external_near_miss": _comparison(target=target, ba=0.76, auc=0.86),
                "blind_snapshot": _comparison(target=target, ba=0.54, auc=0.58),
                "hard_negative": _comparison(
                    target=target,
                    skipped=True,
                    reason="slice lacks enough rows in both classes for stratified CV",
                ),
            },
            "source_transfer": {
                "external_near_miss_to_blind_snapshot": _comparison(target=target, ba=0.50, auc=0.55),
                "blind_snapshot_to_external_near_miss": _comparison(target=target, ba=0.62, auc=0.75),
                "rank_shaped_family_to_external_near_miss": _comparison(target=target, ba=0.61, auc=0.72),
            },
        }
    return {
        "metadata": {
            "artifact_type": "ml_text_baseline_cross_pool",
            "baseline_version": "ml-text-baseline-cross-pool-v1",
        },
        "per_target": per_target,
    }


def _label_row(index: int, *, variant: str, paper_id: str, split: str = "audit_only", explicit: bool = True) -> dict:
    return {
        "row_id": f"row-{index}",
        "split": split,
        "review_pool_variant": variant,
        "paper_id": paper_id,
        "relevance_label": "good" if explicit else "",
        "novelty_label": "yes" if explicit else "",
        "bridge_like_label": "no" if explicit else "",
        "reviewer_notes": "notes only" if not explicit else "labeled",
        "good_or_acceptable": index % 2 == 0,
        "surprising_or_useful": index % 3 == 0,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v7",
        "rows": [
            _label_row(1, variant="ml_external_near_miss_audit", paper_id="W1"),
            _label_row(2, variant="ml_external_near_miss_audit", paper_id="W2"),
            _label_row(3, variant="ml_blind_snapshot_audit", paper_id="W2"),
            _label_row(4, variant="full_family_top_k", paper_id="W4"),
            _label_row(5, variant="ml_hard_negative_audit", paper_id="W5"),
            _label_row(6, variant="", paper_id="W6"),
            _label_row(7, variant="ml_external_near_miss_audit", paper_id="W7", explicit=False),
            _label_row(8, variant="ml_external_near_miss_audit", paper_id="W8", split="candidate", explicit=True),
        ],
    }


def _text_corpus_payload(*, changed: int = 0) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_corpus",
            "corpus_version": "ml-labeled-text-corpus-v2",
            "counts_by_previous_embedding_text_format_version": {"external_text_corpus_v7_verbatim": 2},
            "counts_by_canonicalization_status": {"canonical_title_abstract": 6},
            "n_text_changed_from_v1": changed,
        },
        "rows": [{"row_id": "unused"}],
    }


def _embeddings_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v1",
        },
        "rows": [{"row_id": "not inspected", "embedding_status": "mock", "embedding": ["not", "validated"]}],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_readiness_payload_counts_flags_and_text_format_conclusion(tmp_path: Path) -> None:
    payload = build_ml_text_transfer_readiness_payload(
        cross_pool_path=_write_json(tmp_path, "cross.json", _cross_pool_payload()),
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        text_corpus_v2_path=_write_json(tmp_path, "corpus-v2.json", _text_corpus_payload(changed=0)),
        embeddings_v1_path=_write_json(tmp_path, "embeddings.json", _embeddings_payload()),
        generated_at="2026-05-14T00:00:00Z",
    )

    assert payload["metadata"]["readiness_version"] == "ml-text-transfer-readiness-v1"
    assert [item["name"] for item in payload["metadata"]["inputs"]] == [
        "cross_pool",
        "label_dataset",
        "text_corpus_v2",
        "embeddings_v1",
    ]
    assert payload["class_balance_by_review_pool_variant"]["total_explicit_labeled_row_count"] == 6
    good_external = payload["class_balance_by_review_pool_variant"]["by_target"]["good_or_acceptable"][
        "ml_external_near_miss_audit"
    ]
    assert good_external == {"true": 1, "false": 1, "null": 0, "total": 2}
    assert "(null)" in payload["class_balance_by_review_pool_variant"]["by_target"]["good_or_acceptable"]
    assert payload["duplicate_paper_summary"]["duplicate_paper_id_count"] == 1
    assert payload["duplicate_paper_summary"]["duplicate_observation_pressure_count"] == 1

    synthesis = payload["cross_pool_synthesis"]["good_or_acceptable"]
    assert synthesis["in_pool_cv"]["external_near_miss"]["models"]["embedding_logistic"]["balanced_accuracy"] == 0.76
    assert synthesis["source_transfer"]["external_near_miss_to_blind_snapshot"]["models"]["embedding_logistic"][
        "roc_auc"
    ] == 0.55
    assert synthesis["skipped_reason_counts"] == {"slice lacks enough rows in both classes for stratified CV": 1}

    flags = payload["heuristic_readiness_flags"]["good_or_acceptable"]
    assert flags["in_pool_signal_strong"]["value"] is True
    assert flags["external_blind_transfer_weak"]["value"] is True
    assert flags["transfer_inconsistent"]["value"] is True
    assert flags["needs_more_labels"]["value"] is True
    assert flags["production_ready"]["value"] is False
    assert flags["external_blind_transfer_weak"]["evidence"][0]["threshold_name"].startswith("weak_transfer")

    assert payload["text_format_evidence"]["conclusion"] == NO_REEMBED_NEEDED_CONCLUSION
    assert payload["text_format_evidence"]["n_text_changed_from_v1"] == 0
    assert PRODUCTION_RECOMMENDER_MISSING_GATES[0] in payload["production_recommender_missing_gates"]


def test_text_changed_conclusion_recommends_v2_embeddings(tmp_path: Path) -> None:
    payload = build_ml_text_transfer_readiness_payload(
        cross_pool_path=_write_json(tmp_path, "cross.json", _cross_pool_payload()),
        label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
        text_corpus_v2_path=_write_json(tmp_path, "corpus-v2.json", _text_corpus_payload(changed=3)),
        embeddings_v1_path=None,
        generated_at="2026-05-14T00:00:00Z",
    )
    assert "generate v2 embeddings" in payload["text_format_evidence"]["conclusion"]


def test_input_version_validation(tmp_path: Path) -> None:
    bad_cross = _cross_pool_payload()
    bad_cross["metadata"]["baseline_version"] = "wrong"
    with pytest.raises(MLTextTransferReadinessError, match="baseline_version"):
        build_ml_text_transfer_readiness_payload(
            cross_pool_path=_write_json(tmp_path, "bad-cross.json", bad_cross),
            label_dataset_path=_write_json(tmp_path, "labels.json", _label_payload()),
            generated_at="2026-05-14T00:00:00Z",
        )

    bad_labels = _label_payload()
    bad_labels["dataset_version"] = "wrong"
    with pytest.raises(MLTextTransferReadinessError, match="dataset_version"):
        build_ml_text_transfer_readiness_payload(
            cross_pool_path=_write_json(tmp_path, "cross.json", _cross_pool_payload()),
            label_dataset_path=_write_json(tmp_path, "bad-labels.json", bad_labels),
            generated_at="2026-05-14T00:00:00Z",
        )

    bad_corpus = _text_corpus_payload()
    bad_corpus["metadata"]["corpus_version"] = "wrong"
    with pytest.raises(MLTextTransferReadinessError, match="corpus_version"):
        build_ml_text_transfer_readiness_payload(
            cross_pool_path=_write_json(tmp_path, "cross2.json", _cross_pool_payload()),
            label_dataset_path=_write_json(tmp_path, "labels2.json", _label_payload()),
            text_corpus_v2_path=_write_json(tmp_path, "bad-corpus.json", bad_corpus),
            generated_at="2026-05-14T00:00:00Z",
        )

    bad_embed = _embeddings_payload()
    bad_embed["metadata"]["embedding_artifact_version"] = "wrong"
    with pytest.raises(MLTextTransferReadinessError, match="embedding_artifact_version"):
        build_ml_text_transfer_readiness_payload(
            cross_pool_path=_write_json(tmp_path, "cross3.json", _cross_pool_payload()),
            label_dataset_path=_write_json(tmp_path, "labels3.json", _label_payload()),
            embeddings_v1_path=_write_json(tmp_path, "bad-embed.json", bad_embed),
            generated_at="2026-05-14T00:00:00Z",
        )


def test_deterministic_output_and_markdown(tmp_path: Path) -> None:
    kwargs = {
        "cross_pool_path": _write_json(tmp_path, "cross.json", _cross_pool_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", _label_payload()),
        "text_corpus_v2_path": _write_json(tmp_path, "corpus-v2.json", _text_corpus_payload(changed=0)),
        "embeddings_v1_path": _write_json(tmp_path, "embeddings.json", _embeddings_payload()),
        "generated_at": "2026-05-14T00:00:00Z",
    }
    left = build_ml_text_transfer_readiness_payload(**kwargs)
    right = build_ml_text_transfer_readiness_payload(**kwargs)
    assert left == right

    md = render_markdown(left)
    assert "## Not Doing Yet" in md
    for caveat in CAVEATS:
        assert caveat in md
    assert "Production readiness remains explicitly false" in md


def test_no_database_network_or_embedding_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_text_transfer_readiness.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source
    assert "embedding_provider" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-text-transfer-readiness"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
