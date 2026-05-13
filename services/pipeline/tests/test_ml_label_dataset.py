"""Tests for manual-review → ml-label-dataset export (no DB)."""

from __future__ import annotations

import json
import csv
import hashlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_label_dataset import (
    BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION,
    EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT,
    EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION,
    HARD_NEGATIVE_REVIEW_POOL_VARIANT,
    HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION,
    MLLabelDatasetError,
    VERBATIM_CAVEATS,
    bridge_like_yes_or_partial,
    build_ml_label_dataset,
    build_ml_label_dataset_v5_reviewer_blind_ingest,
    build_ml_label_dataset_v6_hard_negative_ingest,
    build_ml_label_dataset_v7_external_near_miss_ingest,
    discover_manual_review_csvs,
    good_or_acceptable,
    markdown_from_ml_label_dataset,
    parse_manual_review_worksheet,
    row_has_explicit_label,
    sha256_file,
    stable_blind_snapshot_v2_row_id,
    stable_external_near_miss_v1_row_id,
    stable_hard_negative_v1_row_id,
    stable_row_id,
    surprising_or_useful,
    write_ml_label_dataset,
    worksheet_has_label_schema,
    worksheet_infer_bridge_family_from_context,
)


def _write(p: Path, content: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


HEADER_STANDARD = (
    "ranking_run_id,ranking_version,corpus_snapshot_version,embedding_version,cluster_version,"
    "review_pool_variant,family,rank,paper_id,title,year,citation_count,source_slug,topics,"
    "final_score,reason_short,semantic_score,citation_velocity_score,topic_growth_score,"
    "bridge_score,diversity_penalty,bridge_eligible,relevance_label,novelty_label,bridge_like_label,reviewer_notes\n"
)


def _std_data_row(
    *,
    family: str = "bridge",
    rank: str = "1",
    paper_id: str = "https://openalex.org/W1",
    title: str = "T",
    relevance: str = "",
    novelty: str = "",
    bridge_like: str = "",
    notes: str = "",
) -> str:
    """One data row matching HEADER_STANDARD column count (26 fields)."""
    return (
        f"r1,v,c,e,cl,pv,{family},{rank},{paper_id},{title},2025,0,x,t,0.1,rs,,0,0,0,0,true,"
        f"{relevance},{novelty},{bridge_like},{notes}\n"
    )

HEADER_DELTA = (
    "baseline_ranking_run_id,experiment_ranking_run_id,experiment_rank,paper_id,title,year,citation_count,"
    "source_slug,topics,final_score,bridge_score,reason_short,relevance_label,novelty_label,bridge_like_label,reviewer_notes\n"
)


def test_worksheet_schema_and_row_explicit_label() -> None:
    assert worksheet_has_label_schema(
        [
            "paper_id",
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
        ]
    )
    assert not worksheet_has_label_schema(["paper_id", "x"])
    row = {k: "" for k in ["relevance_label", "novelty_label", "bridge_like_label", "reviewer_notes"]}
    assert not row_has_explicit_label(row)
    row["relevance_label"] = "good"
    assert row_has_explicit_label(row)
    row = {k: "" for k in ["relevance_label", "novelty_label", "bridge_like_label"]}
    row["reviewer_notes"] = "only notes"
    assert not row_has_explicit_label(row)


def test_sha256_file(tmp_path: Path) -> None:
    f = tmp_path / "x.txt"
    f.write_bytes(b"abc")
    assert sha256_file(f) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_skips_blank_scaffold_rows_and_counts(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    csv_path = mr / "sheet.csv"
    body = _std_data_row(relevance="", novelty="", bridge_like="") + _std_data_row(
        rank="2",
        paper_id="https://openalex.org/W2",
        title="T",
        relevance="good",
        novelty="useful",
        bridge_like="yes",
        notes="n",
    )
    _write(csv_path, HEADER_STANDARD + body)
    pw = parse_manual_review_worksheet(csv_path, repo_root=root)
    assert pw is not None
    assert pw.skipped_blank_rows == 1
    assert len(pw.included_rows) == 1
    assert pw.included_rows[0]["paper_id"] == "https://openalex.org/W2"


def test_fully_blank_worksheet_reported(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    csv_path = mr / "blank.csv"
    _write(csv_path, HEADER_STANDARD + _std_data_row())
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["metadata"]["skipped_blank_worksheets"] == ["docs/mr/blank.csv"]
    assert payload["metadata"]["total_explicit_labeled_rows"] == 0


def test_derived_targets_relevance_novelty_bridge(tmp_path: Path) -> None:
    assert good_or_acceptable("good") is True
    assert good_or_acceptable("acceptable") is True
    assert good_or_acceptable("miss") is False
    assert good_or_acceptable("irrelevant") is False
    assert good_or_acceptable("") is None
    assert good_or_acceptable("   ") is None
    assert good_or_acceptable(None) is None

    assert surprising_or_useful("surprising") is True
    assert surprising_or_useful("useful") is True
    assert surprising_or_useful("obvious") is False
    assert surprising_or_useful("not_useful") is False
    assert surprising_or_useful("neither") is False
    assert surprising_or_useful("") is None

    assert bridge_like_yes_or_partial("yes") is True
    assert bridge_like_yes_or_partial("partial") is True
    assert bridge_like_yes_or_partial("no") is False
    assert bridge_like_yes_or_partial("not_applicable") is None
    assert bridge_like_yes_or_partial("") is None
    assert bridge_like_yes_or_partial(None) is None


def test_duplicate_paper_id_preserved_and_conflict(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    a = HEADER_STANDARD + _std_data_row(
        paper_id="https://openalex.org/W9",
        relevance="good",
        novelty="useful",
        bridge_like="yes",
        notes="n",
    )
    b = HEADER_STANDARD + _std_data_row(
        family="emerging",
        rank="2",
        paper_id="https://openalex.org/W9",
        title="T2",
        relevance="miss",
        novelty="obvious",
        bridge_like="no",
        notes="n2",
    )
    _write(mr / "a.csv", a)
    _write(mr / "b.csv", b)
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["metadata"]["total_explicit_labeled_rows"] == 2
    assert payload["metadata"]["duplicate_paper_id_report"]["duplicate_paper_id_count"] == 1
    pids = payload["metadata"]["duplicate_paper_id_report"]["duplicate_paper_ids"]
    assert pids == ["https://openalex.org/W9"]
    conf = payload["metadata"]["conflicting_label_report"]
    assert conf["conflicting_label_count"] >= 1
    fields = {c["field"] for c in conf["conflicts"]}
    assert "relevance_label" in fields
    assert "novelty_label" in fields


def test_duplicate_same_labels_no_conflict(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    row = _std_data_row(
        paper_id="https://openalex.org/W9",
        relevance="good",
        novelty="useful",
        bridge_like="yes",
        notes="n",
    )
    _write(mr / "a.csv", HEADER_STANDARD + row)
    _write(
        mr / "b.csv",
        HEADER_STANDARD
        + _std_data_row(rank="2", paper_id="https://openalex.org/W9", relevance="good", novelty="useful", bridge_like="yes", notes="n"),
    )
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["metadata"]["conflicting_label_report"]["conflicting_label_count"] == 0


def test_split_defaults_audit_only(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(
        mr / "one.csv",
        HEADER_STANDARD + _std_data_row(relevance="good", novelty="", bridge_like="", notes="n"),
    )
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["rows"][0]["split"] == "audit_only"


def test_markdown_contains_verbatim_caveats(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(
        mr / "one.csv",
        HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes", notes="n"),
    )
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    md = markdown_from_ml_label_dataset(payload)
    for c in VERBATIM_CAVEATS:
        assert c in md


def test_discover_manual_review_csvs_order(tmp_path: Path) -> None:
    mr = tmp_path / "mr"
    _write(mr / "b.csv", "x\n")
    _write(mr / "a.csv", "y\n")
    paths = discover_manual_review_csvs(mr)
    assert [p.name for p in paths] == ["a.csv", "b.csv"]


def test_cli_ml_label_dataset(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "manual"
    _write(
        mr / "w.csv",
        HEADER_DELTA
        + "rb,re,1,https://openalex.org/W1,T,2025,0,x,,0.5,0.9,r,acceptable,surprising,yes,n\n",
    )
    out_json = tmp_path / "out.json"
    out_md = tmp_path / "out.md"
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-label-dataset",
        "--repo-root",
        str(root),
        "--manual-review-dir",
        str(mr),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()
    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["metadata"]["total_explicit_labeled_rows"] == 1
    assert "ml-label-dataset" in out_md.read_text(encoding="utf-8")


def test_malformed_labeled_row_reported(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(
        mr / "bad.csv",
        HEADER_STANDARD
        + "r1,v,c,e,cl,pv,bridge,1,,T,2025,0,x,t,0.1,rs,,0,0,0,0,true,good,useful,yes,n\n",
    )
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert len(payload["metadata"]["skipped_malformed_rows"]) == 1


def test_write_ml_label_dataset_writes_files(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(
        mr / "w.csv",
        HEADER_STANDARD
        + _std_data_row(
            paper_id="https://openalex.org/W4412072230",
            relevance="good",
            novelty="useful",
            bridge_like="not_applicable",
            notes="n",
        ),
    )
    j = tmp_path / "d.json"
    m = tmp_path / "d.md"
    write_ml_label_dataset(repo_root=root, json_path=j, markdown_path=m, manual_review_dir=mr)
    row = json.loads(j.read_text(encoding="utf-8"))["rows"][0]
    assert row["work_id"] == "W4412072230"
    assert row["bridge_like_yes_or_partial"] is None
    assert row["good_or_acceptable"] is True
    assert row["surprising_or_useful"] is True


def test_worksheet_infer_bridge_family_from_context() -> None:
    delta_fields = [c.strip() for c in HEADER_DELTA.strip().split(",")]
    assert worksheet_infer_bridge_family_from_context(
        "docs/audit/manual-review/bridge_weight_experiment_rank-bc1123e00c_delta_review.csv",
        delta_fields,
    )
    assert worksheet_infer_bridge_family_from_context(
        "docs/audit/manual-review/bridge_objective_delta_rank-60910a47b4_one_row_review.csv",
        delta_fields,
    )
    assert worksheet_infer_bridge_family_from_context(
        "docs/audit/manual-review/bridge_objective_elig_delta_rank-x_review.csv",
        delta_fields,
    )
    std_fields = [c.strip() for c in HEADER_STANDARD.strip().split(",")]
    assert not worksheet_infer_bridge_family_from_context("docs/audit/manual-review/x.csv", std_fields)
    assert not worksheet_infer_bridge_family_from_context(
        "docs/audit/manual-review/bridge_weight_experiment_rank-bc1123e00c_delta_review.csv",
        std_fields,
    )


def test_bridge_delta_worksheet_infers_family_bridge(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "audit" / "manual-review"
    name = "bridge_weight_experiment_rank-bc1123e00c_delta_review.csv"
    csv_path = mr / name
    row = (
        "rank-ee2ba6c816,rank-bc1123e00c,1,https://openalex.org/W1,T,2025,0,x,,0.5,0.9,r,good,useful,yes,n\n"
    )
    _write(csv_path, HEADER_DELTA + row)
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    r0 = payload["rows"][0]
    assert r0["family"] == "bridge"
    assert r0.get("family_inferred") is True
    assert "family inference" in markdown_from_ml_label_dataset(payload).lower()


def test_inferred_family_metadata(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    row = "rb,re,1,https://openalex.org/W1,T,2025,0,x,,0.5,0.9,r,good,useful,yes,n\n"
    _write(mr / "bridge_objective_delta_rank-x_one_row_review.csv", HEADER_DELTA + row)
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    meta = payload["metadata"]
    assert meta["inferred_family_count"] == 1
    assert meta["inferred_family_by_source"]["docs/mr/bridge_objective_delta_rank-x_one_row_review.csv"] == 1


def test_derived_target_conflict_true_false(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    a = HEADER_STANDARD + _std_data_row(
        paper_id="https://openalex.org/Wdup",
        relevance="good",
        novelty="useful",
        bridge_like="yes",
        notes="",
    )
    b = HEADER_STANDARD + _std_data_row(
        rank="2",
        paper_id="https://openalex.org/Wdup",
        title="T2",
        relevance="miss",
        novelty="obvious",
        bridge_like="no",
        notes="",
    )
    _write(mr / "a.csv", a)
    _write(mr / "b.csv", b)
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    drep = payload["metadata"]["derived_target_conflict_report"]
    assert drep["derived_target_conflict_count"] >= 1
    fields = {c["field"] for c in drep["conflicts"]}
    assert "good_or_acceptable" in fields
    assert "surprising_or_useful" in fields
    assert "bridge_like_yes_or_partial" in fields


def test_derived_target_no_conflict_surprising_vs_useful(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    a = HEADER_STANDARD + _std_data_row(
        paper_id="https://openalex.org/Wsame",
        relevance="good",
        novelty="surprising",
        bridge_like="yes",
        notes="",
    )
    b = HEADER_STANDARD + _std_data_row(
        rank="2",
        paper_id="https://openalex.org/Wsame",
        title="T2",
        relevance="good",
        novelty="useful",
        bridge_like="yes",
        notes="",
    )
    _write(mr / "a.csv", a)
    _write(mr / "b.csv", b)
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    drep = payload["metadata"]["derived_target_conflict_report"]
    for c in drep["conflicts"]:
        assert c["field"] != "surprising_or_useful"


def test_no_train_dev_test_split_in_dataset(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(mr / "one.csv", HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes"))
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert all(r["split"] == "audit_only" for r in payload["rows"])
    meta = payload["metadata"]
    assert "train_split" not in meta and "dev_split" not in meta and "test_split" not in meta


HEADER_FAMILY_RANK = (
    "ranking_run_id,ranking_version,corpus_snapshot_version,embedding_version,cluster_version,"
    "review_pool_variant,family,family_rank,paper_id,title,year,citation_count,source_slug,topics,"
    "final_score,reason_short,semantic_score,citation_velocity_score,topic_growth_score,"
    "bridge_score,diversity_penalty,bridge_eligible,relevance_label,novelty_label,bridge_like_label,reviewer_notes\n"
)


def _row_family_rank(fr: str = "42") -> str:
    return (
        f"r1,v,c,e,cl,pv,emerging,{fr},https://openalex.org/W2,T2,2025,0,x,t,0.1,rs,,0,0,0,0,true,"
        "good,useful,not_applicable,notes\n"
    )


def test_dataset_version_parameter_on_payload_and_rows(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(mr / "one.csv", HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes"))
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr, dataset_version="ml-label-dataset-v2")
    assert payload["dataset_version"] == "ml-label-dataset-v2"
    assert all(r["dataset_version"] == "ml-label-dataset-v2" for r in payload["rows"])


def test_family_rank_used_when_rank_column_missing(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(mr / "fr.csv", HEADER_FAMILY_RANK + _row_family_rank("99"))
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["rows"][0]["rank"] == "99"


HEADER_BLIND = (
    "worksheet_version,sample_seed,sample_reason,corpus_snapshot_version,embedding_version,cluster_version,"
    "ranking_run_id_context,review_pool_variant,paper_id,openalex_work_id,internal_work_id,title,year,"
    "citation_count,source_slug,type,cluster_id,topics,abstract_preview,"
    "ranking_context_family_scores_json,ranking_context_family_ranks_json,"
    "relevance_label,novelty_label,bridge_like_label,reviewer_notes\n"
)


def _blind_data_row(
    *,
    paper_id: str = "https://openalex.org/W7153448625",
    cluster_id: str = "c000",
    topics: str = "Music and Audio Processing",
    abstract: str = "An abstract preview.",
    scores_json: str = '"{""bridge"": -0.2, ""emerging"": 0.16}"',
    ranks_json: str = '"{""bridge"": 96, ""emerging"": 174}"',
    relevance: str = "good",
    novelty: str = "useful",
    bridge_like: str = "yes",
    notes: str = "blind notes",
) -> str:
    return (
        f"ml-blind-snapshot-review-v1,20260430,cluster_stratified_seeded,corpus-v2,emb-v2,clust-v2,"
        f"rank-ee2ba6c816,ml_blind_snapshot_audit,{paper_id},W7153448625,2296,Title,2026,0,,article,"
        f"{cluster_id},{topics},{abstract},{scores_json},{ranks_json},{relevance},{novelty},{bridge_like},{notes}\n"
    )


def test_blind_worksheet_rows_preserve_context_and_family_null(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "audit" / "manual-review"
    _write(mr / "ml_blind_snapshot_review_v1.csv", HEADER_BLIND + _blind_data_row())
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["metadata"]["total_explicit_labeled_rows"] == 1
    row = payload["rows"][0]
    assert row["family"] is None
    assert row["review_pool_variant"] == "ml_blind_snapshot_audit"
    assert row["worksheet_version"] == "ml-blind-snapshot-review-v1"
    assert row["sample_seed"] == "20260430"
    assert row["sample_reason"] == "cluster_stratified_seeded"
    assert row["cluster_id"] == "c000"
    assert row["topics"] == "Music and Audio Processing"
    assert row["abstract_preview"] == "An abstract preview."
    assert row["ranking_context_family_scores_json"] == '{"bridge": -0.2, "emerging": 0.16}'
    assert row["ranking_context_family_ranks_json"] == '{"bridge": 96, "emerging": 174}'
    assert row["openalex_work_id"] == "W7153448625"
    assert row["internal_work_id"] == "2296"


def test_blind_worksheet_does_not_infer_labels_from_context(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "audit" / "manual-review"
    _write(mr / "ml_blind_snapshot_review_v1.csv", HEADER_BLIND + _blind_data_row())
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    row = payload["rows"][0]
    assert row["relevance_label"] == "good"
    assert row["novelty_label"] == "useful"
    assert row["bridge_like_label"] == "yes"
    assert row["good_or_acceptable"] is True
    assert row["surprising_or_useful"] is True
    assert row["bridge_like_yes_or_partial"] is True
    assert row["family"] is None
    assert row.get("rank") is None
    assert row.get("experiment_rank") is None


def test_non_blind_rows_do_not_get_blind_context_fields(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    _write(
        mr / "one.csv",
        HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes"),
    )
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    row = payload["rows"][0]
    for f in (
        "worksheet_version",
        "sample_seed",
        "sample_reason",
        "cluster_id",
        "ranking_context_family_scores_json",
        "ranking_context_family_ranks_json",
        "openalex_work_id",
        "internal_work_id",
        "abstract_preview",
    ):
        assert f not in row


def test_ranking_run_id_context_used_when_ranking_run_id_missing(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    mr = root / "docs" / "mr"
    header = (
        "ranking_run_id_context,review_pool_variant,paper_id,relevance_label,novelty_label,bridge_like_label,reviewer_notes\n"
    )
    row = "rank-ee2ba6c816,ml_blind_snapshot_audit,https://openalex.org/W1,good,useful,yes,n\n"
    _write(mr / "blind_like.csv", header + row)
    payload = build_ml_label_dataset(repo_root=root, manual_review_dir=mr)
    assert payload["rows"][0]["ranking_run_id"] == "rank-ee2ba6c816"


V2_FIELDS = [
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
    "cluster_id",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
]


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=V2_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _base_v4_payload() -> dict[str, object]:
    base_row = {
        "dataset_version": "ml-label-dataset-v4",
        "row_id": "base-row-1",
        "paper_id": "https://openalex.org/W100",
        "work_id": "W100",
        "title": "Base row",
        "ranking_run_id": "rank-ee2ba6c816",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snapshot",
        "family": "emerging",
        "review_pool_variant": "full_family_top_k",
        "rank": "1",
        "experiment_rank": None,
        "source_worksheet_path": "docs/audit/manual-review/base.csv",
        "source_worksheet_sha256": "base-sha",
        "source_row_number": 2,
        "relevance_label": "good",
        "novelty_label": "useful",
        "bridge_like_label": "yes",
        "reviewer_notes": "base note",
        "label_provenance": "manual_review_worksheet_csv",
        "split": "audit_only",
        "good_or_acceptable": True,
        "surprising_or_useful": True,
        "bridge_like_yes_or_partial": True,
    }
    return {
        "dataset_version": "ml-label-dataset-v4",
        "generated_at": "2026-05-12T00:00:00Z",
        "caveats": list(VERBATIM_CAVEATS),
        "source_worksheets": ["docs/audit/manual-review/base.csv"],
        "source_worksheet_sha256": {"docs/audit/manual-review/base.csv": "base-sha"},
        "rows": [base_row],
        "metadata": {
            "manual_review_dir": "docs/audit/manual-review",
            "row_counts_by_source": {"docs/audit/manual-review/base.csv": 1},
            "included_labeled_row_counts_by_source": {"docs/audit/manual-review/base.csv": 1},
            "skipped_blank_row_counts_by_source": {"docs/audit/manual-review/base.csv": 0},
            "skipped_blank_worksheets": [],
            "skipped_malformed_rows": [],
        },
    }


def _write_base_dataset(root: Path) -> Path:
    base_path = root / "docs" / "audit" / "ml-label-dataset-v4.json"
    base_path.parent.mkdir(parents=True, exist_ok=True)
    base_path.write_text(json.dumps(_base_v4_payload(), indent=2) + "\n", encoding="utf-8")
    return base_path


def _v2_row(index: int, *, labels: bool) -> dict[str, str]:
    work_id = f"W{7000000000 + index}"
    paper_id = f"https://openalex.org/{work_id}"
    row_id = stable_blind_snapshot_v2_row_id(
        worksheet_version=BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION,
        sample_seed=20260512,
        paper_id=paper_id,
    )
    row = {
        "row_id": row_id,
        "worksheet_version": BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION,
        "review_pool_variant": "ml_blind_snapshot_audit",
        "paper_id": paper_id,
        "openalex_work_id": work_id,
        "work_id": work_id,
        "title": f"Blind v2 title {index}",
        "year": "2026",
        "citation_count": str(index),
        "source_slug": "openalex",
        "topics": f"topic {index % 3}",
        "abstract_preview": f"abstract {index}",
        "sample_reason": "cluster_stratified_seeded",
        "cluster_id": f"c{index % 4:03d}",
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }
    if labels:
        row.update(
            {
                "relevance_label": "good" if index % 2 else "miss",
                "novelty_label": "surprising" if index % 3 else "neither",
                "bridge_like_label": "partial" if index % 5 else "no",
                "reviewer_notes": f"review note {index}",
            }
        )
    return row


def _sidecar_for_rows(rows: list[dict[str, str]], *, base_sha: str) -> dict[str, object]:
    sidecar_rows = []
    for index, row in enumerate(rows, start=1):
        sidecar_rows.append(
            {
                "row_id": row["row_id"],
                "paper_id": row["paper_id"],
                "openalex_work_id": row["openalex_work_id"],
                "internal_work_id": 9000 + index,
                "sample_seed": 20260512,
                "sample_reason": row["sample_reason"],
                "cluster_id": row["cluster_id"],
                "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
                "embedding_version": "v2-title-abstract-1536-cleantext-r1",
                "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
                "ranking_run_id": "rank-ee2ba6c816",
                "ranking_context_family_scores_json": '{"bridge": -0.2, "emerging": 0.1}',
                "ranking_context_family_ranks_json": '{"bridge": 10, "emerging": 20}',
                "emerging_paper_scores": {
                    "family": "emerging",
                    "final_score": 0.1,
                    "semantic_score": 0.2,
                    "citation_velocity_score": 0.0,
                    "topic_growth_score": 0.0,
                    "diversity_penalty": 0.0,
                    "bridge_score": 0.0,
                },
                "paper_scores_by_family": {"emerging": {"final_score": 0.1}},
            }
        )
    return {
        "artifact_type": "ml_blind_snapshot_review_v2_context",
        "provenance": {
            "worksheet_version": BLIND_SNAPSHOT_REVIEW_V2_WORKSHEET_VERSION,
            "review_pool_variant": "ml_blind_snapshot_audit",
            "sample_seed": 20260512,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": "docs/audit/ml-label-dataset-v4.json",
            "label_dataset_sha256": base_sha,
            "ranking_run_id": "rank-ee2ba6c816",
            "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
            "embedding_version": "v2-title-abstract-1536-cleantext-r1",
            "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
        },
        "rows": sidecar_rows,
    }


def _write_v2_ingest_fixture(root: Path) -> tuple[Path, Path, Path, Path, list[dict[str, str]]]:
    base_path = _write_base_dataset(root)
    blank_rows = [_v2_row(i, labels=False) for i in range(1, 61)]
    labeled_rows = [_v2_row(i, labels=True) for i in range(1, 61)]
    manual = root / "docs" / "audit" / "manual-review"
    blank_path = manual / "ml_blind_snapshot_review_v2.csv"
    labeled_path = manual / "ml_blind_snapshot_review_v2_labeled_2026-05-13.csv"
    sidecar_path = manual / "ml_blind_snapshot_review_v2_context.json"
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    sidecar_path.write_text(
        json.dumps(_sidecar_for_rows(labeled_rows, base_sha=sha256_file(base_path)), indent=2) + "\n",
        encoding="utf-8",
    )
    return base_path, blank_path, labeled_path, sidecar_path, labeled_rows


def test_v5_ingest_uses_v2_csv_row_id_as_canonical_and_preserves_sidecar(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, labeled_rows = _write_v2_ingest_fixture(root)
    payload = build_ml_label_dataset_v5_reviewer_blind_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
    )

    assert len(payload["rows"]) == 61
    assert payload["rows"][0] == _base_v4_payload()["rows"][0]
    row = payload["rows"][1]
    expected = labeled_rows[0]["row_id"]
    assert row["row_id"] == expected
    old_style_id = stable_row_id(
        source_rel="docs/audit/manual-review/ml_blind_snapshot_review_v2_labeled_2026-05-13.csv",
        source_row_number=2,
        paper_id=labeled_rows[0]["paper_id"],
        ranking_run_id="rank-ee2ba6c816",
        rank_key=None,
        experiment_rank=None,
    )
    assert row["row_id"] != old_style_id
    assert row["family"] is None
    assert row["work_id"] == labeled_rows[0]["work_id"]
    assert row["openalex_work_id"] == labeled_rows[0]["openalex_work_id"]
    assert row["internal_work_id"] == 9001
    assert row["internal_work_id"] != row["work_id"]
    assert row["blind_snapshot_context"]["emerging_paper_scores"]["final_score"] == 0.1
    assert row["blind_snapshot_context"]["internal_work_id"] == 9001
    assert row["source_worksheet_path"] == "docs/audit/manual-review/ml_blind_snapshot_review_v2_labeled_2026-05-13.csv"
    assert row["source_row_number"] == 2
    assert payload["metadata"]["reviewer_blind_v2_ingest"]["v2_rows_appended"] == 60


def test_v5_ingest_missing_or_extra_sidecar_rows_fail(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_v2_ingest_fixture(root)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    payload["rows"] = payload["rows"][1:]
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MLLabelDatasetError, match="sidecar row_id set differs"):
        build_ml_label_dataset_v5_reviewer_blind_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
        )

    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_v2_ingest_fixture(root)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    extra = dict(payload["rows"][0])
    extra["row_id"] = hashlib.sha256(b"extra").hexdigest()
    payload["rows"].append(extra)
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MLLabelDatasetError, match="sidecar row_id set differs"):
        build_ml_label_dataset_v5_reviewer_blind_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
        )


def test_v5_labeled_csv_template_comparison_ignores_only_review_columns(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_v2_ingest_fixture(root)
    rows = list(csv.DictReader(labeled_path.read_text(encoding="utf-8").splitlines()))
    rows[0]["title"] = "Changed title"
    _write_csv(labeled_path, rows)
    with pytest.raises(MLLabelDatasetError, match="changed non-review template field"):
        build_ml_label_dataset_v5_reviewer_blind_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
        )


def test_v5_derived_targets_from_explicit_labels_only(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_v2_ingest_fixture(root)
    rows = list(csv.DictReader(labeled_path.read_text(encoding="utf-8").splitlines()))
    rows[0]["relevance_label"] = "miss"
    rows[0]["novelty_label"] = "neither"
    rows[0]["bridge_like_label"] = "no"
    rows[0]["reviewer_notes"] = "negative explicit labels despite high context"
    _write_csv(labeled_path, rows)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    payload["rows"][0]["emerging_paper_scores"]["final_score"] = 999.0
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")

    out = build_ml_label_dataset_v5_reviewer_blind_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
    )
    row = out["rows"][1]
    assert row["good_or_acceptable"] is False
    assert row["surprising_or_useful"] is False
    assert row["bridge_like_yes_or_partial"] is False


def test_v5_assembly_is_base_plus_exact_v2_slice_no_glob_drift(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_v2_ingest_fixture(root)
    _write(
        root / "docs" / "audit" / "manual-review" / "unrelated_labeled.csv",
        HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes", notes="do not glob"),
    )
    payload = build_ml_label_dataset_v5_reviewer_blind_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
    )
    source_paths = set(payload["source_worksheets"])
    assert "docs/audit/manual-review/unrelated_labeled.csv" not in source_paths
    assert payload["metadata"]["total_explicit_labeled_rows"] == 61
    assert payload["metadata"]["reviewer_blind_v2_ingest"]["base_row_count"] == 1
    assert payload["metadata"]["reviewer_blind_v2_ingest"]["v2_rows_appended"] == 60


def _base_v5_payload() -> dict[str, object]:
    payload = _base_v4_payload()
    payload["dataset_version"] = "ml-label-dataset-v5"
    payload["metadata"]["reviewer_blind_v2_ingest"] = {"v2_rows_appended": 60}
    return payload


def _write_base_v5_dataset(root: Path) -> Path:
    base_path = root / "docs" / "audit" / "ml-label-dataset-v5.json"
    base_path.parent.mkdir(parents=True, exist_ok=True)
    base_path.write_text(json.dumps(_base_v5_payload(), indent=2) + "\n", encoding="utf-8")
    return base_path


def _hard_negative_row(index: int, *, labels: bool) -> dict[str, str]:
    work_id = f"W{8000000000 + index}"
    paper_id = f"https://openalex.org/{work_id}"
    row_id = stable_hard_negative_v1_row_id(
        worksheet_version=HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION,
        sample_seed=20260513,
        paper_id=paper_id,
    )
    row = {
        "row_id": row_id,
        "worksheet_version": HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION,
        "review_pool_variant": HARD_NEGATIVE_REVIEW_POOL_VARIANT,
        "paper_id": paper_id,
        "openalex_work_id": work_id,
        "work_id": work_id,
        "title": f"Hard negative title {index}",
        "year": "2026",
        "citation_count": str(index),
        "source_slug": "openalex",
        "topics": f"topic {index % 2}",
        "abstract_preview": f"hard negative abstract {index}",
        "sample_reason": "low_family_score_near_miss",
        "cluster_id": f"c{index % 3:03d}",
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }
    if labels:
        row.update(
            {
                "relevance_label": "miss" if index % 2 else "acceptable",
                "novelty_label": "not_useful" if index % 3 else "useful",
                "bridge_like_label": "no" if index % 4 else "partial",
                "reviewer_notes": f"hard-negative review note {index}",
            }
        )
    return row


def _hard_negative_sidecar(rows: list[dict[str, str]], *, base_sha: str) -> dict[str, object]:
    sidecar_rows = []
    for index, row in enumerate(rows, start=1):
        sidecar_rows.append(
            {
                "row_id": row["row_id"],
                "paper_id": row["paper_id"],
                "openalex_work_id": row["openalex_work_id"],
                "internal_work_id": 7000 + index,
                "sample_seed": 20260513,
                "sample_reason": row["sample_reason"],
                "hard_negative_signals": ["low_emerging_final_score"],
                "selection_auxiliary_scores": {"emerging_final_score": 0.01 * index},
                "cluster_id": row["cluster_id"],
                "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
                "embedding_version": "v2-title-abstract-1536-cleantext-r1",
                "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
                "ranking_run_id": "rank-ee2ba6c816",
                "ranking_context_family_scores_json": '{"bridge": -0.2, "emerging": 0.1}',
                "ranking_context_family_ranks_json": '{"bridge": 10, "emerging": 20}',
                "emerging_paper_scores": {"family": "emerging", "final_score": 0.01 * index},
                "paper_scores_by_family": {"emerging": {"final_score": 0.01 * index}},
            }
        )
    return {
        "artifact_type": "ml_hard_negative_review_v1_context",
        "provenance": {
            "worksheet_version": HARD_NEGATIVE_REVIEW_V1_WORKSHEET_VERSION,
            "review_pool_variant": HARD_NEGATIVE_REVIEW_POOL_VARIANT,
            "sample_seed": 20260513,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": "docs/audit/ml-label-dataset-v5.json",
            "label_dataset_sha256": base_sha,
            "ranking_run_id": "rank-ee2ba6c816",
            "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
            "embedding_version": "v2-title-abstract-1536-cleantext-r1",
            "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
        },
        "rows": sidecar_rows,
    }


def _write_hard_negative_ingest_fixture(root: Path) -> tuple[Path, Path, Path, Path, list[dict[str, str]]]:
    base_path = _write_base_v5_dataset(root)
    blank_rows = [_hard_negative_row(i, labels=False) for i in range(1, 8)]
    labeled_rows = [_hard_negative_row(i, labels=True) for i in range(1, 8)]
    manual = root / "docs" / "audit" / "manual-review"
    blank_path = manual / "ml_hard_negative_review_v1.csv"
    labeled_path = manual / "ml_hard_negative_review_v1_labeled_2026-05-13.csv"
    sidecar_path = manual / "ml_hard_negative_review_v1_context.json"
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    sidecar_path.write_text(
        json.dumps(_hard_negative_sidecar(labeled_rows, base_sha=sha256_file(base_path)), indent=2) + "\n",
        encoding="utf-8",
    )
    return base_path, blank_path, labeled_path, sidecar_path, labeled_rows


def test_v6_hard_negative_row_id_is_csv_canonical_and_context_preserved(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, labeled_rows = _write_hard_negative_ingest_fixture(root)
    payload = build_ml_label_dataset_v6_hard_negative_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
    )
    assert len(payload["rows"]) == 8
    assert payload["rows"][0] == _base_v5_payload()["rows"][0]
    row = payload["rows"][1]
    assert row["row_id"] == labeled_rows[0]["row_id"]
    old_style_id = stable_row_id(
        source_rel="docs/audit/manual-review/ml_hard_negative_review_v1_labeled_2026-05-13.csv",
        source_row_number=2,
        paper_id=labeled_rows[0]["paper_id"],
        ranking_run_id="rank-ee2ba6c816",
        rank_key=None,
        experiment_rank=None,
    )
    assert row["row_id"] != old_style_id
    assert row["dataset_version"] == "ml-label-dataset-v6"
    assert row["family"] is None
    assert row["review_pool_variant"] == HARD_NEGATIVE_REVIEW_POOL_VARIANT
    assert row["work_id"] == labeled_rows[0]["work_id"]
    assert row["openalex_work_id"] == labeled_rows[0]["openalex_work_id"]
    assert row["internal_work_id"] == 7001
    assert row["internal_work_id"] != row["work_id"]
    assert row["hard_negative_context"]["selection_auxiliary_scores"]["emerging_final_score"] == 0.01
    assert row["source_worksheet_path"] == "docs/audit/manual-review/ml_hard_negative_review_v1_labeled_2026-05-13.csv"
    assert payload["metadata"]["hard_negative_v1_ingest"]["hard_negative_rows_appended"] == 7


def test_v6_hard_negative_sidecar_mismatch_fails(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_hard_negative_ingest_fixture(root)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    payload["rows"] = payload["rows"][1:]
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MLLabelDatasetError, match="sidecar row_id set differs"):
        build_ml_label_dataset_v6_hard_negative_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
        )


def test_v6_hard_negative_template_comparison_ignores_only_review_columns(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_hard_negative_ingest_fixture(root)
    rows = list(csv.DictReader(labeled_path.read_text(encoding="utf-8").splitlines()))
    rows[0]["sample_reason"] = "changed"
    _write_csv(labeled_path, rows)
    with pytest.raises(MLLabelDatasetError, match="changed non-review template field"):
        build_ml_label_dataset_v6_hard_negative_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
        )


def test_v6_hard_negative_assembly_and_derived_targets_from_explicit_labels_only(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, _rows = _write_hard_negative_ingest_fixture(root)
    rows = list(csv.DictReader(labeled_path.read_text(encoding="utf-8").splitlines()))
    rows[0]["relevance_label"] = "miss"
    rows[0]["novelty_label"] = "neither"
    rows[0]["bridge_like_label"] = "no"
    rows[0]["reviewer_notes"] = "explicit negative labels"
    _write_csv(labeled_path, rows)
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["rows"][0]["emerging_paper_scores"]["final_score"] = 999.0
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    _write(
        root / "docs" / "audit" / "manual-review" / "unrelated_labeled.csv",
        HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes", notes="do not glob"),
    )

    payload = build_ml_label_dataset_v6_hard_negative_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
    )
    assert len(payload["rows"]) == 8
    assert payload["rows"][:1] == _base_v5_payload()["rows"]
    assert "docs/audit/manual-review/unrelated_labeled.csv" not in set(payload["source_worksheets"])
    appended = payload["rows"][1:]
    assert all(r["dataset_version"] == "ml-label-dataset-v6" for r in appended)
    assert all(r["review_pool_variant"] == HARD_NEGATIVE_REVIEW_POOL_VARIANT for r in appended)
    assert appended[0]["good_or_acceptable"] is False
    assert appended[0]["surprising_or_useful"] is False
    assert appended[0]["bridge_like_yes_or_partial"] is False
    assert payload["metadata"]["total_explicit_labeled_rows"] == 8
    assert payload["metadata"]["hard_negative_v1_ingest"]["base_row_count"] == 1
    assert payload["metadata"]["hard_negative_v1_ingest"]["hard_negative_rows_appended"] == 7


def _base_v6_payload() -> dict[str, object]:
    payload = _base_v5_payload()
    payload["dataset_version"] = "ml-label-dataset-v6"
    payload["metadata"]["previous_reviewer_blind_v2_ingest"] = {"v2_rows_appended": 60}
    payload["metadata"]["hard_negative_v1_ingest"] = {"hard_negative_rows_appended": 7}
    return payload


def _write_base_v6_dataset(root: Path) -> Path:
    base_path = root / "docs" / "audit" / "ml-label-dataset-v6.json"
    base_path.parent.mkdir(parents=True, exist_ok=True)
    base_path.write_text(json.dumps(_base_v6_payload(), indent=2) + "\n", encoding="utf-8")
    return base_path


def _external_near_miss_row(index: int, *, labels: bool) -> dict[str, str]:
    work_id = f"W{9000000000 + index}"
    paper_id = f"https://openalex.org/{work_id}"
    row_id = stable_external_near_miss_v1_row_id(
        worksheet_version=EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION,
        sample_seed=20260514,
        paper_id=paper_id,
    )
    row = {
        "row_id": row_id,
        "worksheet_version": EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION,
        "review_pool_variant": EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT,
        "paper_id": paper_id,
        "openalex_work_id": work_id,
        "work_id": work_id,
        "title": f"External near-miss title {index}",
        "year": "2025",
        "citation_count": str(index),
        "source_slug": "external_fixture",
        "topics": f"external topic {index % 4}",
        "abstract_preview": f"external abstract {index}",
        "sample_reason": "adjacent_audio_not_mir" if index % 2 else "recommender_not_music_specific",
        "cluster_id": "ext",
        "relevance_label": "",
        "novelty_label": "",
        "bridge_like_label": "",
        "reviewer_notes": "",
    }
    if labels:
        row.update(
            {
                "relevance_label": "irrelevant" if index % 3 == 0 else "miss",
                "novelty_label": "not_useful" if index % 4 else "neither",
                "bridge_like_label": "not_applicable" if index % 5 else "no",
                "reviewer_notes": f"external review note {index}",
            }
        )
    return row


def _external_near_miss_sidecar(rows: list[dict[str, str]], *, base_sha: str) -> dict[str, object]:
    sidecar_rows = []
    for index, row in enumerate(rows, start=1):
        sidecar_rows.append(
            {
                "row_id": row["row_id"],
                "paper_id": row["paper_id"],
                "openalex_work_id": row["openalex_work_id"],
                "internal_work_id": 5000 + index,
                "sample_seed": 20260514,
                "sample_reason": row["sample_reason"],
                "cluster_id": row["cluster_id"],
                "source_query": "fixture audio query",
                "normalized_query": "fixture audio query",
                "exclusion_checks_passed": {
                    "outside_source_snapshot_217": True,
                    "not_v6_labeled": True,
                    "not_v6_seen_unlabeled": True,
                },
                "hidden_diagnostics": {"fixture_score_like_field": 999.0},
            }
        )
    return {
        "artifact_type": "ml_external_near_miss_review_v1_context",
        "provenance": {
            "worksheet_version": EXTERNAL_NEAR_MISS_REVIEW_V1_WORKSHEET_VERSION,
            "review_pool_variant": EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT,
            "sample_seed": 20260514,
            "row_id_formula": "sha256(worksheet_version|sample_seed|paper_id)",
            "label_dataset_path": "docs/audit/ml-label-dataset-v6.json",
            "label_dataset_sha256": base_sha,
            "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
        },
        "rows": sidecar_rows,
    }


def _write_external_near_miss_ingest_fixture(root: Path) -> tuple[Path, Path, Path, Path, Path, list[dict[str, str]]]:
    base_path = _write_base_v6_dataset(root)
    blank_rows = [_external_near_miss_row(i, labels=False) for i in range(1, 61)]
    labeled_rows = [_external_near_miss_row(i, labels=True) for i in range(1, 61)]
    manual = root / "docs" / "audit" / "manual-review"
    blank_path = manual / "ml_external_near_miss_review_v1.csv"
    labeled_path = manual / "ml_external_near_miss_review_v1_labeled_2026-05-13.csv"
    sidecar_path = manual / "ml_external_near_miss_review_v1_context.json"
    conflict_path = root / "docs" / "audit" / "ml-label-conflict-policy.md"
    _write_csv(blank_path, blank_rows)
    _write_csv(labeled_path, labeled_rows)
    sidecar_path.write_text(
        json.dumps(_external_near_miss_sidecar(labeled_rows, base_sha=sha256_file(base_path)), indent=2) + "\n",
        encoding="utf-8",
    )
    conflict_path.parent.mkdir(parents=True, exist_ok=True)
    conflict_path.write_text("# conflict policy\n", encoding="utf-8")
    return base_path, blank_path, labeled_path, sidecar_path, conflict_path, labeled_rows


def test_v7_external_near_miss_row_id_is_csv_canonical_and_context_preserved(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, conflict_path, labeled_rows = _write_external_near_miss_ingest_fixture(root)
    payload = build_ml_label_dataset_v7_external_near_miss_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
        conflict_policy_path=conflict_path,
    )

    assert len(payload["rows"]) == 61
    assert payload["rows"][0] == _base_v6_payload()["rows"][0]
    row = payload["rows"][1]
    assert row["row_id"] == labeled_rows[0]["row_id"]
    old_style_id = stable_row_id(
        source_rel="docs/audit/manual-review/ml_external_near_miss_review_v1_labeled_2026-05-13.csv",
        source_row_number=2,
        paper_id=labeled_rows[0]["paper_id"],
        ranking_run_id=None,
        rank_key=None,
        experiment_rank=None,
    )
    assert row["row_id"] != old_style_id
    assert row["dataset_version"] == "ml-label-dataset-v7"
    assert row["family"] is None
    assert row["review_pool_variant"] == EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT
    assert row["work_id"] == labeled_rows[0]["work_id"]
    assert row["openalex_work_id"] == labeled_rows[0]["openalex_work_id"]
    assert row["internal_work_id"] == 5001
    assert row["internal_work_id"] != row["work_id"]
    assert row["external_near_miss_context"]["hidden_diagnostics"]["fixture_score_like_field"] == 999.0
    assert row["source_worksheet_path"] == "docs/audit/manual-review/ml_external_near_miss_review_v1_labeled_2026-05-13.csv"
    assert payload["metadata"]["external_near_miss_v1_ingest"]["external_near_miss_rows_appended"] == 60


def test_v7_external_near_miss_sidecar_mismatch_fails(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, conflict_path, _rows = _write_external_near_miss_ingest_fixture(root)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    payload["rows"] = payload["rows"][1:]
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MLLabelDatasetError, match="sidecar row_id set differs"):
        build_ml_label_dataset_v7_external_near_miss_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
            conflict_policy_path=conflict_path,
        )


def test_v7_external_near_miss_sidecar_base_dataset_sha_mismatch_fails(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, conflict_path, _rows = _write_external_near_miss_ingest_fixture(root)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    payload["provenance"]["label_dataset_sha256"] = "not-the-base-sha"
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MLLabelDatasetError, match="label_dataset_sha256 does not match"):
        build_ml_label_dataset_v7_external_near_miss_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
            conflict_policy_path=conflict_path,
        )


def test_v7_external_near_miss_template_comparison_ignores_only_review_columns(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, conflict_path, _rows = _write_external_near_miss_ingest_fixture(root)
    rows = list(csv.DictReader(labeled_path.read_text(encoding="utf-8").splitlines()))
    rows[0]["title"] = "Changed external title"
    _write_csv(labeled_path, rows)
    with pytest.raises(MLLabelDatasetError, match="changed non-review template field"):
        build_ml_label_dataset_v7_external_near_miss_ingest(
            repo_root=root,
            base_dataset_path=base_path,
            blank_worksheet_path=blank_path,
            labeled_worksheet_path=labeled_path,
            context_sidecar_path=sidecar_path,
            conflict_policy_path=conflict_path,
        )


def test_v7_external_near_miss_assembly_and_derived_targets_from_explicit_labels_only(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, conflict_path, _rows = _write_external_near_miss_ingest_fixture(root)
    rows = list(csv.DictReader(labeled_path.read_text(encoding="utf-8").splitlines()))
    rows[0]["relevance_label"] = "good"
    rows[0]["novelty_label"] = "surprising"
    rows[0]["bridge_like_label"] = "yes"
    rows[0]["reviewer_notes"] = "explicit positive labels despite near-miss context"
    _write_csv(labeled_path, rows)
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["rows"][0]["hidden_diagnostics"]["fixture_score_like_field"] = -999.0
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")
    _write(
        root / "docs" / "audit" / "manual-review" / "unrelated_labeled.csv",
        HEADER_STANDARD + _std_data_row(relevance="good", novelty="useful", bridge_like="yes", notes="do not glob"),
    )

    payload = build_ml_label_dataset_v7_external_near_miss_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
        conflict_policy_path=conflict_path,
    )
    assert len(payload["rows"]) == 61
    assert payload["rows"][:1] == _base_v6_payload()["rows"]
    assert "docs/audit/manual-review/unrelated_labeled.csv" not in set(payload["source_worksheets"])
    appended = payload["rows"][1:]
    assert all(r["dataset_version"] == "ml-label-dataset-v7" for r in appended)
    assert all(r["review_pool_variant"] == EXTERNAL_NEAR_MISS_REVIEW_POOL_VARIANT for r in appended)
    assert appended[0]["family"] is None
    assert appended[0]["good_or_acceptable"] is True
    assert appended[0]["surprising_or_useful"] is True
    assert appended[0]["bridge_like_yes_or_partial"] is True
    assert payload["metadata"]["total_explicit_labeled_rows"] == 61
    assert payload["metadata"]["external_near_miss_v1_ingest"]["base_row_count"] == 1
    assert payload["metadata"]["external_near_miss_v1_ingest"]["validation_summary"]["sidecar_base_dataset_sha256_matched"] is True
    assert payload["metadata"]["external_near_miss_v1_ingest"]["external_near_miss_rows_appended"] == 60


def test_v7_markdown_regeneration_hint_names_external_near_miss_command(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    base_path, blank_path, labeled_path, sidecar_path, conflict_path, _rows = _write_external_near_miss_ingest_fixture(root)
    payload = build_ml_label_dataset_v7_external_near_miss_ingest(
        repo_root=root,
        base_dataset_path=base_path,
        blank_worksheet_path=blank_path,
        labeled_worksheet_path=labeled_path,
        context_sidecar_path=sidecar_path,
        conflict_policy_path=conflict_path,
    )
    md = markdown_from_ml_label_dataset(payload)
    assert "ml-label-dataset-v7-external-near-miss-ingest" in md
    assert "ml_external_near_miss_audit" in md
    assert "external_near_miss_context" in md
