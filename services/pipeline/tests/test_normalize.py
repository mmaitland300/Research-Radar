from pipeline.normalize import extract_topics, hydrate_work_record
from pipeline.policy import CorpusPolicy


MOJIBAKE_TITLE = (
    "Towards an "
    + chr(0x00E2)
    + chr(0x20AC)
    + chr(0x02DC)
    + "Everything Corpus"
    + chr(0x00E2)
    + chr(0x20AC)
    + chr(0x2122)
    + ": Audio&amp;ndash;Video Methods"
)
MOJIBAKE_AUTHOR = "Vlora Arifi-M" + chr(0x00C3) + chr(0x00BC) + "ller"
EXPECTED_TITLE = "Towards an 'Everything Corpus': Audio-Video Methods"
EXPECTED_DASH_TEXT = "Music Question-Answering"
EXPECTED_SOURCE = "Transactions of Music-IR"
EXPECTED_AUTHOR = "Vlora Arifi-M" + chr(0x00FC) + "ller"


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


def test_hydrate_work_record_cleans_html_entities_and_mojibake() -> None:
    policy = CorpusPolicy()
    work = {
        "id": "https://openalex.org/W1",
        "title": MOJIBAKE_TITLE,
        "publication_year": 2025,
        "type": "article",
        "language": "en",
        "cited_by_count": 3,
        "abstract": "Music Question&amp;amp;ndash;Answering and corpus design.",
        "primary_location": {
            "source": {
                "id": "https://openalex.org/S1",
                "display_name": "Transactions of Music&amp;amp;ndash;IR",
            }
        },
        "authorships": [
            {"author": {"id": "https://openalex.org/A1", "display_name": MOJIBAKE_AUTHOR}}
        ],
        "topics": [
            {
                "id": "https://openalex.org/T1",
                "display_name": "Music Question&amp;amp;ndash;Answering",
                "score": 0.7,
            }
        ],
        "referenced_works": [],
    }

    record = hydrate_work_record(work, policy)

    assert record.work.title == EXPECTED_TITLE
    assert record.work.abstract == EXPECTED_DASH_TEXT + " and corpus design."
    assert record.work.source_display_name == EXPECTED_SOURCE
    assert record.authors[0].display_name == EXPECTED_AUTHOR
    assert record.topics[0].display_name == EXPECTED_DASH_TEXT
