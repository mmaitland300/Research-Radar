from pipeline.normalize import extract_topics


def test_extract_topics_accepts_openalex_work_shape_without_level() -> None:
    work = {
        "topics": [
            {
                "id": "https://openalex.org/T11309",
                "display_name": "Music and Audio Processing",
                "score": 1.0,
                "subfield": {"id": "https://openalex.org/subfields/1711", "display_name": "Signal Processing"},
            }
        ]
    }
    topics = extract_topics(work)
    assert len(topics) == 1
    assert topics[0].topic_openalex_id == "https://openalex.org/T11309"
    assert topics[0].display_name == "Music and Audio Processing"
    assert topics[0].score == 1.0
    assert topics[0].level == 0


def test_extract_topics_uses_level_when_present() -> None:
    work = {
        "topics": [
            {
                "id": "https://openalex.org/T1",
                "display_name": "Example",
                "score": 0.5,
                "level": 2,
            }
        ]
    }
    topics = extract_topics(work)
    assert len(topics) == 1
    assert topics[0].level == 2
