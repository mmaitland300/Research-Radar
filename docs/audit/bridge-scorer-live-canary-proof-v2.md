# Bridge Scorer Live Canary Proof v2

Generated: 2026-06-08T13:08:56Z

## Decision

`live_canary_proof_passed=true`

The live Railway API returned a successful bounded Bridge scorer cohort canary. Public/default Bridge traffic stayed fail-closed on `materialized_heuristic`, while the allowlisted canary header returned `ranking_mode=bounded_bridge_ml_scorer` with `emitted_to_public_users=false`.

Recommended next stage:

`human_review_bridge_live_canary_top20_v1`

## Scope

This artifact is evidence collection only. It does not change API, web, gate, or scorer logic. It does not enable public Bridge rollout and does not label papers.

Constants used:

- Pinned Bridge run: `rank-5a7efa5ca3`
- Canary subject: `bridge-deploy-readiness-v1`
- Product Emerging version: `shadow-generalization-product-candidate-ranking-v1`
- API base used: `https://capable-light-production.up.railway.app`

## Public Bridge Request

- HTTP 200
- `ranking_mode=materialized_heuristic`
- `ranking_run_id=rank-5a7efa5ca3`
- `bridge_recommendations_ml_served=null`
- `emitted_to_public_users=null`
- `item_count=20`

Public/default Bridge remained fail-closed to materialized heuristic order.

Top 5:

| Rank | Paper ID | Title |
| --- | --- | --- |
| 1 | https://openalex.org/W4411141874 | Physical Modeling of a Spring Reverb Tank Incorporating Helix Angle, Damping, and Magnetic Bead Coupling |
| 2 | https://openalex.org/W4408772031 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research |
| 3 | https://openalex.org/W4409215217 | Modeling Time-Variant Responses of Optical Compressors With Selective State Space Models |
| 4 | https://openalex.org/W4409215261 | Toward an Improved Auditory Model for Predicting Binaural Coloration |
| 5 | https://openalex.org/W4410091503 | Towards an &#x27;Everything Corpus&#x27;: A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data |

## Cohort Canary Request

Header: `X-Research-Radar-Canary-Subject: bridge-deploy-readiness-v1`

- HTTP 200
- `ranking_mode=bounded_bridge_ml_scorer`
- `scorer_surface=bridge`
- `bridge_recommendations_ml_served=true`
- `bridge_rank_pct_hybrid_alpha=0.5`
- `bridge_rank_pct_scope=full_bridge_candidate_pool`
- `emitted_to_public_users=false`
- `item_count=20`

## Canary Response Top 20

| Rank | Paper ID | Final score | Title |
| --- | --- | ---: | --- |
| 1 | https://openalex.org/W4417471638 | -0.2 | Neural Audio Synthesis for Sound Effects: A Scope Review |
| 2 | https://openalex.org/W7126213550 | -0.2 | Potential and Pitfalls of Audio as Data for Political Research: Alignment, Features, and Classification Models |
| 3 | https://openalex.org/W7128741623 | -0.2 | Melodies of the forest: Nature as an improvisational space for shared creative embodiment |
| 4 | https://openalex.org/W4415337516 | -0.2 | Sparse Autoencoders Make Audio Foundation Models More Explainable |
| 5 | https://openalex.org/W4401445948 | -0.1895 | The Games We Play: Exploring the Impact of ISMIR on Musicology |
| 6 | https://openalex.org/W4413133017 | -0.2 | Music, humans, and machines: initial reflections for the development of research with collaboration between composers and artificial intelligence in the creative process of Brazilian music |
| 7 | https://openalex.org/W7155391647 | -0.2 | A Focused Survey of Generative AI-Based Music Therapy Systems: Recent Progress and Open Challenges |
| 8 | https://openalex.org/W4411141659 | 0.487547 | Distilling DDSP: Exploring Real-Time Audio Generation on Embedded Systems |
| 9 | https://openalex.org/W7150806375 | -0.2 | Praat Audiotools: An offline analysis-resynthesis toolkit for experimental composition |
| 10 | https://openalex.org/W4404570638 | -0.2 | The Sound of Water: Inferring Physical Properties from Pouring Liquids |
| 11 | https://openalex.org/W4414818544 | -0.2 | TalkPlay-Tools: Conversational Music Recommendation with LLM Tool Calling |
| 12 | https://openalex.org/W7131744631 | -0.2 | Audio-visual archiving project in Dagbon of Northern Ghana during the period 1999-2010 |
| 13 | https://openalex.org/W4414011891 | 0.541666 | Estimation and Restoration of Unknown Nonlinear Distortion Using Diffusion |
| 14 | https://openalex.org/W4414112292 | -0.2 | Science of music-based citizen science: How seeing influences hearing |
| 15 | https://openalex.org/W7116661261 | -0.2 | A Journey through the &lt;em&gt;Unity Atlantic Rhythm Map&lt;/em&gt; |
| 16 | https://openalex.org/W4409474100 | 0.363726 | Analysis of Human Participants in the Journal of the Audio Engineering Society |
| 17 | https://openalex.org/W4410025358 | 0.537667 | Designing Neural Synthesizers for Low-Latency Interaction |
| 18 | https://openalex.org/W4401727628 | 0.458894 | Charting the Universe of Metal Music Lyrics and Analyzing Their Relation to Perceived Audio Hardness |
| 19 | https://openalex.org/W4412072230 | 0.447624 | Audio Signal Processing in the Artificial Intelligence Era: Challenges and Directions |
| 20 | https://openalex.org/W4414581097 | -0.2 | Spotify Audio Features and Exercise Tolerance in Cycling: An Exploratory Analysis |

## Emerging Regression Check

- HTTP 200
- `family=emerging`
- `ranking_run_id=rank-83787b91ef`
- `ranking_version=shadow-generalization-product-candidate-ranking-v1`
- `ranking_mode=bounded_ml_scorer`
- `item_count=20`

Bridge env did not cause the explicit product-pinned Emerging request to resolve to `rank-5a7efa5ca3`.

## Sanity Cross-Check

The live canary top 5 matched the expected scorer-served top 5 from the serving helper.

Expected/actual top 5:

1. `https://openalex.org/W4417471638`
2. `https://openalex.org/W7126213550`
3. `https://openalex.org/W7128741623`
4. `https://openalex.org/W4415337516`
5. `https://openalex.org/W4401445948`

## Human Review Handoff

Review these items before widening exposure:

- Are the canary top 20 plausible Bridge recommendations?
- Any obvious junk, out-of-domain, or misleading papers?
- Which of the previously unlabeled proposed top-20 papers should be labeled now?
- Does default Recommended Bridge UI still describe the public heuristic path?
- Does the API canary response clearly disclose `bounded_bridge_ml_scorer`?

Previously unlabeled proposed top-20 items appearing in this live canary top 20:

| Controlled rank | Paper ID | Title |
| ---: | --- | --- |
| 7 | https://openalex.org/W7155391647 | A Focused Survey of Generative AI-Based Music Therapy Systems: Recent Progress and Open Challenges |
| 9 | https://openalex.org/W7150806375 | Praat Audiotools: An offline analysis-resynthesis toolkit for experimental composition |
| 12 | https://openalex.org/W7131744631 | Audio-visual archiving project in Dagbon of Northern Ghana during the period 1999-2010 |
| 14 | https://openalex.org/W4414112292 | Science of music-based citizen science: How seeing influences hearing |
| 15 | https://openalex.org/W7116661261 | A Journey through the &lt;em&gt;Unity Atlantic Rhythm Map&lt;/em&gt; |
| 19 | https://openalex.org/W4412072230 | Audio Signal Processing in the Artificial Intelligence Era: Challenges and Directions |
| 20 | https://openalex.org/W4414581097 | Spotify Audio Features and Exercise Tolerance in Cycling: An Exploratory Analysis |

Web Recommended does not send canary headers. This is API-operator evidence until a private preview path exists.

## Limitations

- Railway CLI/token was unavailable, so deployed build metadata was inferred from successful runtime behavior rather than queried directly.
- This is an API-operator canary proof; the public Recommended web page does not send canary headers.
- The canary request may consume one in-process exposure-cap slot. Exposure cap is process-local, not global across multiple replicas.
- This does not enable public Bridge rollout and does not relabel any papers.
- Human review of the canary top 20 is still required before widening exposure.
