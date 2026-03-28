from math import isclose

from pipeline.ranking import PaperSignals, ScoreWeights, final_score


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
