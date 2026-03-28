from pipeline.policy import CorpusPolicy


def test_evaluate_work_includes_core_source_with_topic_signal() -> None:
    policy = CorpusPolicy()
    work = {
        "publication_year": 2023,
        "language": "en",
        "type": "article",
        "is_retracted": False,
        "title": "Music information retrieval with robust audio embeddings",
        "abstract": "We study self-supervised audio embeddings in MIR tasks.",
        "primary_location": {
            "source": {
                "id": None,
                "display_name": "ISMIR",
            }
        },
    }

    decision = policy.evaluate_work(work)

    assert decision.included is True
    assert decision.reason == "core_source_topic_match"
    assert decision.venue_class == "core"
    assert decision.is_core_corpus is True


def test_evaluate_work_blocks_explicit_exclusion_terms() -> None:
    policy = CorpusPolicy()
    work = {
        "publication_year": 2022,
        "language": "en",
        "type": "article",
        "is_retracted": False,
        "title": "Music information retrieval for speech recognition pipelines",
        "abstract": "This paper uses music information retrieval methods for speech recognition.",
        "primary_location": {
            "source": {
                "id": None,
                "display_name": "International Society for Music Information Retrieval Conference",
            }
        },
    }

    decision = policy.evaluate_work(work)

    assert decision.included is False
    assert decision.reason == "explicit_exclusion_term"
    assert "speech recognition" in decision.matched_exclusions
