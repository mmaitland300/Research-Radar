from pipeline.openalex_text import clean_openalex_text


def test_clean_openalex_text_api_visible_mojibake_dash() -> None:
    assert clean_openalex_text("The Sound Demixing Challenge 2023 â Music Demixing Track") == (
        "The Sound Demixing Challenge 2023 - Music Demixing Track"
    )


def test_clean_openalex_text_cross_modal_mojibake() -> None:
    assert clean_openalex_text("CrossâModal Approaches to Beat Tracking") == (
        "Cross-Modal Approaches to Beat Tracking"
    )


def test_clean_openalex_text_beethoven_possessive() -> None:
    assert clean_openalex_text("Beethovenâs Piano Sonatas") == "Beethoven's Piano Sonatas"


def test_clean_openalex_text_smart_quotes_and_colon() -> None:
    raw = "Towards an âEverything Corpusâ: A Framework"
    out = clean_openalex_text(raw)
    assert out == 'Towards an "Everything Corpus": A Framework'
    assert "â" not in out


def test_clean_openalex_text_curly_apostrophe_triplet() -> None:
    assert clean_openalex_text("donâ€™t break") == "don't break"
