"""Tests for manual-review → ml-label-dataset export (no DB)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_label_dataset import (
    VERBATIM_CAVEATS,
    bridge_like_yes_or_partial,
    build_ml_label_dataset,
    discover_manual_review_csvs,
    good_or_acceptable,
    markdown_from_ml_label_dataset,
    parse_manual_review_worksheet,
    row_has_explicit_label,
    sha256_file,
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
