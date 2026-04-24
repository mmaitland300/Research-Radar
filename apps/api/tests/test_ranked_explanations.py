from app.ranked_explanations import (
    build_list_ranking_explanation,
    build_signal_explanations,
    family_weights_from_config,
)


def test_family_weights_from_config_reads_family_block() -> None:
    cfg = {
        "family_weights": {
            "emerging": {
                "semantic": 0.0,
                "citation_velocity": 0.5,
                "topic_growth": 0.5,
                "bridge": 0.0,
                "diversity_penalty": 0.05,
            }
        }
    }
    w = family_weights_from_config(cfg, "emerging")
    assert w["citation_velocity"] == 0.5
    assert w["topic_growth"] == 0.5


def test_emerging_signal_roles_used_vs_measured() -> None:
    w = family_weights_from_config(None, "emerging")
    expl = build_signal_explanations(
        family="emerging",
        semantic=None,
        citation_velocity=0.9,
        topic_growth=0.4,
        bridge=0.5,
        diversity_penalty=0.0,
        weights=w,
    )
    by_key = {e["key"]: e for e in expl}
    assert by_key["citation_velocity"]["role"] == "used"
    assert by_key["topic_growth"]["role"] == "used"
    assert by_key["bridge"]["role"] == "measured"
    assert by_key["semantic"]["role"] == "not_computed"
    assert by_key["diversity_penalty"]["role"] == "penalty"


def test_emerging_list_explanation_headline() -> None:
    w = family_weights_from_config(None, "emerging")
    le = build_list_ranking_explanation(family="emerging", weights=w)
    assert "Emerging" in le["headline"]
    assert "Recent attention" in le["used_in_ordering"]
    assert "Embedding slice fit (corpus centroid)" in le["measured_only"]
