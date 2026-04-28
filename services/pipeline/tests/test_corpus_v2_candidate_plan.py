"""Corpus v2 candidate plan dry-run: filters, dedup, caps, no DB, no contact leak."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
import pipeline.corpus_v2_candidate_plan as cv2
from pipeline.corpus_v2_candidate_plan import (
    compute_contact_provenance,
    evaluate_v2_candidate,
    render_corpus_v2_plan_markdown,
    run_corpus_v2_candidate_plan,
)
from pipeline.openalex_client import OPENALEX_API_KEY_ENV
from pipeline.policy import CorpusPolicy


def _w(
    *,
    wid: str,
    title: str,
    year: int = 2020,
    doi: str | None = None,
    lang: str = "en",
    wtype: str = "article",
    abstract: str = "abstract body",
) -> dict:
    return {
        "id": wid,
        "title": title,
        "publication_year": year,
        "doi": doi,
        "language": lang,
        "type": wtype,
        "is_retracted": False,
        "has_abstract": True,
        "cited_by_count": 5,
        "abstract_inverted_index": {"abstract": [0], "body": [1]} if abstract else None,
        "primary_location": {"source": {"display_name": "Test Venue"}},
    }


def test_filter_accepts_obvious_mir_work() -> None:
    policy = CorpusPolicy()
    w = _w(
        wid="https://openalex.org/W1",
        title="Deep learning for music information retrieval benchmarks",
        abstract="We study music information retrieval and audio embeddings for tagging.",
    )
    r = evaluate_v2_candidate(w, policy=policy, bucket_id="ismir_proceedings_or_mir_conference")
    assert r["included"] is True
    assert "music information retrieval" in r["matched_terms"] or "ismir_or_mir_context" in r["matched_terms"]


def test_filter_rejects_manatee_style_noise() -> None:
    policy = CorpusPolicy()
    w = _w(
        wid="https://openalex.org/W2",
        title="Manatee vocalization and marine mammal communication",
        abstract="We record dugong sounds underwater without musical structure.",
    )
    r = evaluate_v2_candidate(w, policy=policy, bucket_id="ismir_proceedings_or_mir_conference")
    assert r["included"] is False
    assert r["exclusion_reason"] == "noise_animal_or_non_music_biology"


def test_filter_rejects_generic_database_without_music() -> None:
    policy = CorpusPolicy()
    w = _w(
        wid="https://openalex.org/W3",
        title="Nearest neighbor search in relational database systems",
        abstract="We optimize SQL server indexes for k-nearest neighbor queries in large RDBMS deployments.",
    )
    r = evaluate_v2_candidate(w, policy=policy, bucket_id="audio_ml_signal_processing")
    assert r["included"] is False
    assert r["exclusion_reason"] in (
        "noise_generic_database_without_music_hook",
        "no_strong_topic_or_bucket_allow_signal",
    )


def test_filter_rejects_speech_only_without_music() -> None:
    policy = CorpusPolicy()
    w = _w(
        wid="https://openalex.org/W4",
        title="Neural text-to-speech for call centers",
        abstract="Speech synthesis and text-to-speech conversion for telephony prompts; no musical signals.",
    )
    r = evaluate_v2_candidate(w, policy=policy, bucket_id="audio_ml_signal_processing")
    assert r["included"] is False
    assert r["exclusion_reason"] == "noise_speech_focus_without_music_hook"


def test_filter_rejects_biomedical_audio_without_music() -> None:
    policy = CorpusPolicy()
    w = _w(
        wid="https://openalex.org/W5",
        title="Clinical audio monitoring",
        abstract="Biomedical audio signals for patient monitoring in hospitals.",
    )
    r = evaluate_v2_candidate(w, policy=policy, bucket_id="audio_ml_signal_processing")
    assert r["included"] is False
    assert r["exclusion_reason"] == "noise_biomedical_audio_without_music_hook"


def test_dedup_by_openalex_id() -> None:
    policy = CorpusPolicy()
    w1 = _w(
        wid="https://openalex.org/W10",
        title="Music source separation with deep nets",
        abstract="Music source separation benchmark on musdb.",
    )
    w2 = dict(w1)
    assert evaluate_v2_candidate(w1, policy=policy, bucket_id="source_separation_benchmarks")["included"] is True

    def fetch(_url: str) -> dict:
        return {"meta": {"next_cursor": None}, "results": [w1, w2]}

    with patch.object(cv2, "V2_BUCKET_ORDER", ("source_separation_benchmarks",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"source_separation_benchmarks": 10}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=policy,
                mailto="x@y.z",
                contact_mode="cli",
                contact_provided=True,
                per_bucket_limit=5,
                target_min=1,
                target_max=50,
                fetch=fetch,
                mock_openalex=False,
            )
    oa_ids = [c["openalex_id"] for c in plan["selected_candidates"]]
    assert oa_ids.count("https://openalex.org/W10") == 1


def test_dedup_by_doi() -> None:
    policy = CorpusPolicy()
    w1 = _w(
        wid="https://openalex.org/W20",
        title="Music tagging A",
        doi="https://doi.org/10.1234/example.one",
        abstract="music tagging evaluation mir",
    )
    w2 = _w(
        wid="https://openalex.org/W21",
        title="Music tagging B",
        doi="10.1234/example.one",
        abstract="music tagging evaluation mir",
    )

    pages = [
        {"meta": {"next_cursor": "n1"}, "results": [w1]},
        {"meta": {"next_cursor": None}, "results": [w2]},
    ]

    def fetch(_url: str) -> dict:
        if not pages:
            return {"meta": {"next_cursor": None}, "results": []}
        return pages.pop(0)

    with patch.object(cv2, "V2_BUCKET_ORDER", ("ismir_proceedings_or_mir_conference",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"ismir_proceedings_or_mir_conference": 10}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=policy,
                mailto="x@y.z",
                contact_mode="cli",
                contact_provided=True,
                per_bucket_limit=10,
                target_min=1,
                target_max=50,
                fetch=fetch,
                mock_openalex=False,
            )
    assert plan["dedup_statistics"]["drops_by_doi"] >= 1
    assert len(plan["selected_candidates"]) == 1


def test_dedup_by_normalized_title() -> None:
    policy = CorpusPolicy()
    title = "Music   Harmony  Analysis"
    w1 = _w(wid="https://openalex.org/W30", title=title, abstract="harmony and music score analysis")
    w2 = _w(wid="https://openalex.org/W31", title="Music Harmony Analysis", abstract="harmony and music score analysis")

    def fetch(_url: str) -> dict:
        return {"meta": {"next_cursor": None}, "results": [w1, w2]}

    with patch.object(cv2, "V2_BUCKET_ORDER", ("symbolic_music_and_harmony",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"symbolic_music_and_harmony": 10}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=policy,
                mailto="x@y.z",
                contact_mode="cli",
                contact_provided=True,
                per_bucket_limit=10,
                target_min=1,
                target_max=50,
                fetch=fetch,
                mock_openalex=False,
            )
    assert plan["dedup_statistics"]["drops_by_normalized_title"] >= 1


def test_per_bucket_cap_enforced() -> None:
    policy = CorpusPolicy()
    works = [
        _w(
            wid=f"https://openalex.org/W{i}",
            title=f"Music information retrieval study {i}",
            abstract="music information retrieval dataset benchmark",
        )
        for i in range(50)
    ]

    def fetch(_url: str) -> dict:
        return {"meta": {"next_cursor": None}, "results": works}

    cap = 3
    with patch.object(cv2, "V2_BUCKET_ORDER", ("ismir_proceedings_or_mir_conference",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"ismir_proceedings_or_mir_conference": cap}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=policy,
                mailto="x@y.z",
                contact_mode="cli",
                contact_provided=True,
                per_bucket_limit=50,
                target_min=1,
                target_max=500,
                fetch=fetch,
                mock_openalex=False,
            )
    ismir_summary = plan["bucket_summaries"][0]
    assert ismir_summary["selected_count_after_dedup_and_cap"] == cap


def test_module_has_no_psycopg_import() -> None:
    src = Path(__file__).resolve().parent.parent / "pipeline" / "corpus_v2_candidate_plan.py"
    text = src.read_text(encoding="utf-8")
    assert "psycopg" not in text


def test_artifacts_do_not_contain_raw_mailto(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    secret = "secret-operator-mailbox-7f3c@example.invalid"
    w = _w(
        wid="https://openalex.org/W99",
        title="ISMIR music retrieval",
        abstract="music information retrieval ismir paper",
    )

    def fetch(_url: str) -> dict:
        return {"meta": {"next_cursor": None}, "results": [w]}

    with patch.object(cv2, "V2_BUCKET_ORDER", ("ismir_proceedings_or_mir_conference",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"ismir_proceedings_or_mir_conference": 5}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=CorpusPolicy(),
                mailto=secret,
                contact_mode="cli",
                contact_provided=True,
                per_bucket_limit=5,
                target_min=1,
                target_max=50,
                fetch=fetch,
                mock_openalex=False,
            )
    json_text = json.dumps(plan, ensure_ascii=False)
    md_text = render_corpus_v2_plan_markdown(plan)
    assert secret not in json_text
    assert secret not in md_text
    assert plan.get("contact_mode") == "cli"
    assert plan.get("contact_provided") is True
    assert plan.get("auth_mode") == "no_key"
    assert plan.get("api_key_provided") is False


def test_artifacts_do_not_contain_openalex_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "oa-secret-never-serialize-999")
    w = _w(
        wid="https://openalex.org/W98",
        title="ISMIR retrieval",
        abstract="music information retrieval ismir",
    )

    def fetch(_url: str) -> dict:
        return {"meta": {"next_cursor": None}, "results": [w]}

    with patch.object(cv2, "V2_BUCKET_ORDER", ("ismir_proceedings_or_mir_conference",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"ismir_proceedings_or_mir_conference": 5}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=CorpusPolicy(),
                mailto="x@y.z",
                contact_mode="cli",
                contact_provided=True,
                per_bucket_limit=5,
                target_min=1,
                target_max=50,
                fetch=fetch,
                mock_openalex=False,
            )
    blob = json.dumps(plan, ensure_ascii=False) + render_corpus_v2_plan_markdown(plan)
    assert "oa-secret-never-serialize-999" not in blob
    assert plan.get("auth_mode") == "api_key"
    assert plan.get("api_key_provided") is True


def test_markdown_caveats_not_validation() -> None:
    plan = run_corpus_v2_candidate_plan(
        policy=CorpusPolicy(),
        mailto="a@b.c",
        contact_mode="mock",
        contact_provided=True,
        per_bucket_limit=1,
        target_min=500,
        target_max=500,
        mock_openalex=True,
    )
    assert plan.get("auth_mode") == "mock"
    assert plan.get("api_key_provided") is False
    md = render_corpus_v2_plan_markdown(plan)
    low = md.lower()
    assert "benchmark" in low or "planning" in low
    assert "validated" not in low or "not" in low


def test_cli_corpus_v2_mock(tmp_path: Path) -> None:
    out = tmp_path / "o.json"
    md = tmp_path / "o.md"
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "corpus-v2-candidate-plan",
            "--output",
            str(out),
            "--markdown-output",
            str(md),
            "--mailto",
            "cli@example.com",
            "--per-bucket-limit",
            "5",
            "--target-min",
            "1",
            "--target-max",
            "20",
            "--mock-openalex",
        ],
    ):
        cli_main.main()
    assert out.is_file()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["selected_total"] == 0
    assert payload["contact_mode"] == "mock"
    assert payload["contact_provided"] is True
    assert payload["auth_mode"] == "mock"
    assert payload["api_key_provided"] is False


def test_compute_contact_provenance_cli_env_mock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    assert compute_contact_provenance(mailto_cli="  u@v.w  ", mock_openalex=False) == ("cli", True)
    with patch.dict("os.environ", {"OPENALEX_MAILTO": "env@x.z", OPENALEX_API_KEY_ENV: ""}, clear=False):
        assert compute_contact_provenance(mailto_cli="", mock_openalex=False) == ("env", True)
    with patch.dict("os.environ", {"OPENALEX_MAILTO": "env@x.z", OPENALEX_API_KEY_ENV: ""}, clear=False):
        assert compute_contact_provenance(mailto_cli="cli@x.z", mock_openalex=False) == ("cli", True)
    assert compute_contact_provenance(mailto_cli="", mock_openalex=True) == ("mock", False)
    with patch.dict("os.environ", {"OPENALEX_MAILTO": "env@x.z", OPENALEX_API_KEY_ENV: ""}, clear=False):
        assert compute_contact_provenance(mailto_cli="", mock_openalex=True) == ("mock", True)


def test_plan_contact_mode_env_in_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulated live path: resolved mailto from env only; artifact reports env, no raw address."""
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    policy = CorpusPolicy()
    w = _w(
        wid="https://openalex.org/W200",
        title="ISMIR retrieval",
        abstract="music information retrieval ismir",
    )

    def fetch(_url: str) -> dict:
        return {"meta": {"next_cursor": None}, "results": [w]}

    secret = "only-in-env@secret.invalid"
    with patch.object(cv2, "V2_BUCKET_ORDER", ("ismir_proceedings_or_mir_conference",)):
        with patch.object(cv2, "V2_BUCKET_CAPS", {"ismir_proceedings_or_mir_conference": 5}):
            plan = cv2.run_corpus_v2_candidate_plan(
                policy=policy,
                mailto=secret,
                contact_mode="env",
                contact_provided=True,
                per_bucket_limit=5,
                target_min=1,
                target_max=50,
                fetch=fetch,
                mock_openalex=False,
            )
    blob = json.dumps(plan, ensure_ascii=False) + render_corpus_v2_plan_markdown(plan)
    assert plan["contact_mode"] == "env"
    assert plan["auth_mode"] == "no_key"
    assert secret not in blob
