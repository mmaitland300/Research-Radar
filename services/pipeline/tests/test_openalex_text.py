from pipeline.openalex_text import clean_openalex_text


def test_clean_openalex_text_api_visible_mojibake_dash() -> None:
    assert clean_openalex_text("The Sound Demixing Challenge 2023 \u00e2 Music Demixing Track") == (
        "The Sound Demixing Challenge 2023 - Music Demixing Track"
    )


def test_clean_openalex_text_cross_modal_mojibake() -> None:
    assert clean_openalex_text("Cross\u00e2Modal Approaches to Beat Tracking") == (
        "Cross-Modal Approaches to Beat Tracking"
    )


def test_clean_openalex_text_utf8_en_dash_misdecoded_latin1() -> None:
    raw = "Foo\u00e2\u0080\u0093Bar"
    assert clean_openalex_text(raw) == "Foo-Bar"


def test_clean_openalex_text_utf8_en_dash_misdecoded_cp1252() -> None:
    raw = "Foo\u00e2\u20ac\u201cBar"
    assert clean_openalex_text(raw) == "Foo-Bar"


def test_clean_openalex_text_beethoven_possessive() -> None:
    assert clean_openalex_text("Beethoven\u00e2s Piano Sonatas") == "Beethoven's Piano Sonatas"


def test_clean_openalex_text_smart_quotes_and_colon() -> None:
    raw = "Towards an \u00e2Everything Corpus\u00e2: A Framework"
    out = clean_openalex_text(raw)
    assert out == 'Towards an "Everything Corpus": A Framework'
    assert "\u00e2" not in out


def test_clean_openalex_text_curly_apostrophe_triplet() -> None:
    assert clean_openalex_text("don\u00e2\u20ac\u2122t break") == "don't break"
