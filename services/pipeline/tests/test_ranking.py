from math import isclose

from pipeline.ranking import PaperSignals, ScoreWeights, final_score, final_score_partial


def test_final_score_uses_default_weights() -> None:
    signals = PaperSignals(
        semantic=0.8,
        citation_velocity=0.4,
        topic_growth=0.6,
        bridge=0.5,
        diversity_penalty=0.2,
    )

    score = final_score(signals)

    expected = (0.30 * 0.8) + (0.20 * 0.4) + (0.20 * 0.6) + (0.20 * 0.5) - (0.10 * 0.2)
    assert isclose(score, expected, rel_tol=1e-9)


def test_final_score_supports_custom_weights() -> None:
    signals = PaperSignals(
        semantic=0.2,
        citation_velocity=0.1,
        topic_growth=0.9,
        bridge=0.8,
        diversity_penalty=0.3,
    )
    weights = ScoreWeights(
        semantic=0.1,
        citation_velocity=0.1,
        topic_growth=0.5,
        bridge=0.2,
        diversity_penalty=0.1,
    )

    score = final_score(signals, weights=weights)

    expected = (0.1 * 0.2) + (0.1 * 0.1) + (0.5 * 0.9) + (0.2 * 0.8) - (0.1 * 0.3)
    assert isclose(score, expected, rel_tol=1e-9)


def test_final_score_partial_all_positive_null_returns_zero_positive() -> None:
    assert isclose(
        final_score_partial(
            semantic=None,
            citation_velocity=None,
            topic_growth=None,
            bridge=None,
            diversity_penalty=None,
        ),
        0.0,
        rel_tol=1e-9,
    )


def test_final_score_partial_renormalizes_over_available_signals() -> None:
    w = ScoreWeights(
        semantic=0.5,
        citation_velocity=0.5,
        topic_growth=0.0,
        bridge=0.0,
        diversity_penalty=0.0,
    )
    s = final_score_partial(
        semantic=None,
        citation_velocity=1.0,
        topic_growth=None,
        bridge=None,
        diversity_penalty=0.0,
        weights=w,
    )
    assert isclose(s, 1.0, rel_tol=1e-9)


def test_final_score_partial_null_semantic_and_bridge() -> None:
    w = ScoreWeights()
    s = final_score_partial(
        semantic=None,
        citation_velocity=0.5,
        topic_growth=0.5,
        bridge=None,
        diversity_penalty=0.1,
        weights=w,
    )
    expected_pos = 0.5 * 0.5 + 0.5 * 0.5
    expected = expected_pos - 0.10 * 0.1
    assert isclose(s, expected, rel_tol=1e-9)
