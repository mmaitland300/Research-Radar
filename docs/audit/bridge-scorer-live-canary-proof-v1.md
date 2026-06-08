# Bridge Scorer Live Canary Proof v1

Generated: 2026-06-08T12:43:56.8107561Z

## Decision

`live_canary_proof_passed=false`

The live Railway API did not return a successful bounded Bridge scorer cohort
canary. The canary-header request returned HTTP 200, but the response stayed on
`ranking_mode=materialized_heuristic` instead of
`ranking_mode=bounded_bridge_ml_scorer`.

Recommended next stage:

`fix_bridge_live_canary_proof`

## Scope

This artifact is evidence collection only. It does not change API, web, gate,
or scorer logic. It does not enable public Bridge rollout and does not label
papers.

Constants used:

- Pinned Bridge run: `rank-5a7efa5ca3`
- Canary subject: `bridge-deploy-readiness-v1`
- Product Emerging version: `shadow-generalization-product-candidate-ranking-v1`
- API base used: `https://capable-light-production.up.railway.app`

`RESEARCH_RADAR_API_BASE` was not set in this shell. The API base was taken from
the prior deployed-readiness artifact and recorded as a limitation.

## Deploy Baseline

- Local HEAD: `75d146442f04c643705474d22e006370cbdb8d69`
- Origin/main HEAD: `75d146442f04c643705474d22e006370cbdb8d69`
- Required commits on origin/main: `2bc7a98`, `9775bf5`, and `75d1464`
- Deployed commit/build id: unavailable from this operator session
- Railway CLI/token: unavailable
- `deployed_commit_includes_hash_stability_fix=false` because Railway build
  metadata could not be directly confirmed

## Public Bridge Request

Request:

`GET /api/v1/recommendations/ranked?family=bridge&limit=20&ranking_run_id=rank-5a7efa5ca3`

Result:

- HTTP 200
- `ranking_run_id=rank-5a7efa5ca3`
- `ranking_mode=materialized_heuristic`
- `bridge_recommendations_ml_served=null`
- `emitted_to_public_users=null`
- `item_count=20`

Public/default Bridge remained fail-closed to materialized heuristic order.

Top 5:

| Rank | Paper ID | Title |
| --- | --- | --- |
| 1 | `https://openalex.org/W4411141874` | Physical Modeling of a Spring Reverb Tank Incorporating Helix Angle, Damping, and Magnetic Bead Coupling |
| 2 | `https://openalex.org/W4408772031` | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research |
| 3 | `https://openalex.org/W4409215217` | Modeling Time-Variant Responses of Optical Compressors With Selective State Space Models |
| 4 | `https://openalex.org/W4409215261` | Toward an Improved Auditory Model for Predicting Binaural Coloration |
| 5 | `https://openalex.org/W4410091503` | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data |

`public_request_verified=true`

## Cohort Canary Request

Request:

`GET /api/v1/recommendations/ranked?family=bridge&limit=20&ranking_run_id=rank-5a7efa5ca3`

Header:

`X-Research-Radar-Canary-Subject: bridge-deploy-readiness-v1`

Result:

- HTTP 200
- `ranking_run_id=rank-5a7efa5ca3`
- `ranking_mode=materialized_heuristic`
- `scorer_surface=null`
- `bridge_recommendations_ml_served=null`
- `bridge_rank_pct_hybrid_alpha=null`
- `bridge_rank_pct_scope=null`
- `emitted_to_public_users=null`
- `item_count=20`
- Item schema preserved: `paper_id`, `title`, `signals`, `final_score`,
  `bridge_eligible`

This is not a successful Bridge scorer canary proof.

`canary_request_verified=false`

Gate log evidence was not available because Railway CLI/token was unavailable
and the deployed response did not expose a closure reason. The exact blocker is
therefore unconfirmed from this session.

## Canary Response Top 20

These are the rows returned by the canary-header request. Because the response
was materialized heuristic, this is not the scorer-served hybrid top 20.

| Rank | Paper ID | Final score | Title |
| --- | --- | ---: | --- |
| 1 | `https://openalex.org/W4411141874` | 0.671000 | Physical Modeling of a Spring Reverb Tank Incorporating Helix Angle, Damping, and Magnetic Bead Coupling |
| 2 | `https://openalex.org/W4408772031` | 0.663265 | CCMusic: An Open and Diverse Database for Chinese Music Information Retrieval Research |
| 3 | `https://openalex.org/W4409215217` | 0.646333 | Modeling Time-Variant Responses of Optical Compressors With Selective State Space Models |
| 4 | `https://openalex.org/W4409215261` | 0.642667 | Toward an Improved Auditory Model for Predicting Binaural Coloration |
| 5 | `https://openalex.org/W4410091503` | 0.630245 | Towards an 'Everything Corpus': A Framework and Guidelines for the Curation of More Comprehensive Multimodal Music Data |
| 6 | `https://openalex.org/W4409967775` | 0.624339 | ChoraleBricks: A Modular Multitrack Dataset for Wind Music Research |
| 7 | `https://openalex.org/W7131681001` | 0.612319 | Inferring Communities of Medieval Music Manuscripts Using Stochastic Block Models |
| 8 | `https://openalex.org/W4415947443` | 0.609245 | Beyond a Western Center of Music Information Retrieval: A Bibliometric Analysis of the First 25 Years of ISMIR Authorship |
| 9 | `https://openalex.org/W4409474070` | 0.604333 | Analysis of Various 3D Acquisition Techniques and Mesh Differences for Head-related Transfer Functions Calculation |
| 10 | `https://openalex.org/W4411649085` | 0.603339 | The AI Music Arms Race: On the Detection of AI-Generated Music |
| 11 | `https://openalex.org/W4402645135` | 0.600386 | BPSD: A Coherent Multi-Version Dataset for Analyzing the First Movements of Beethoven's Piano Sonatas |
| 12 | `https://openalex.org/W4412900768` | 0.600265 | STAR Drums: A Dataset for Automatic Drum Transcription |
| 13 | `https://openalex.org/W4411141958` | 0.599020 | Reverse Engineering of Music Mixing Graphs With Differentiable Processors and Iterative Pruning |
| 14 | `https://openalex.org/W4409215215` | 0.594725 | Issues and Challenges of Audio Technologies for the Musical Metaverse |
| 15 | `https://openalex.org/W7128595024` | 0.585000 | High-Resolution Directivity Measurements of an Artificial Head and Mouth Shaped to Three Vowels |
| 16 | `https://openalex.org/W4414799687` | 0.583333 | Improved Real-Time Six-Degrees-of-Freedom Dynamic Auralization Through Nonuniformly Partitioned Convolution |
| 17 | `https://openalex.org/W4407236737` | 0.572386 | The GigaMIDI Dataset with Features for Expressive Music Performance Detection |
| 18 | `https://openalex.org/W7116976483` | 0.572386 | Multimodal Datasets for Studying Expert Performances of Musical Scores |
| 19 | `https://openalex.org/W4414199528` | 0.571547 | PESTO: Real-Time Pitch Estimation with Self-Supervised Transposition-Equivariant Objective |
| 20 | `https://openalex.org/W4416267785` | 0.568068 | ImproVision Equilibrium: Toward Multimodal Musical Human-Machine Interaction |

## Emerging Regression Check

Request:

`GET /api/v1/recommendations/ranked?family=emerging&limit=20&ranking_version=shadow-generalization-product-candidate-ranking-v1`

Result:

- HTTP 200
- `family=emerging`
- `ranking_run_id=rank-83787b91ef`
- `ranking_version=shadow-generalization-product-candidate-ranking-v1`
- `ranking_mode=bounded_ml_scorer`
- `item_count=20`

Bridge env did not cause the explicit product-pinned Emerging request to resolve
to `rank-5a7efa5ca3`.

`emerging_regression_verified=true`

## Sanity Cross-Check

The live canary top 5 did not match the expected scorer-served top 5 from the
serving helper or the local API gate-open proof.

Expected scorer top 5 from
`docs/audit/bridge-scorer-railway-data-alignment-v1.json`:

1. `https://openalex.org/W4417471638`
2. `https://openalex.org/W7126213550`
3. `https://openalex.org/W7128741623`
4. `https://openalex.org/W4415337516`
5. `https://openalex.org/W4401445948`

Actual live canary-header top 5:

1. `https://openalex.org/W4411141874`
2. `https://openalex.org/W4408772031`
3. `https://openalex.org/W4409215217`
4. `https://openalex.org/W4409215261`
5. `https://openalex.org/W4410091503`

Explanation: the live canary request returned `materialized_heuristic`, so this
mismatch is expected and confirms the scorer path did not open.

## Human Review Handoff

This artifact is not ready for human review of a scorer-served canary top 20,
because the canary request did not serve `bounded_bridge_ml_scorer`.

When the live canary opens, review:

- Are the canary top 20 plausible Bridge recommendations?
- Any obvious junk, out-of-domain, or misleading papers?
- Which of the 7 previously unlabeled proposed top-20 papers from
  `docs/audit/ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1.json`
  appear in the live canary top 20 and warrant labeling?
- Does default Recommended Bridge UI still describe the public heuristic path?
- Does the API canary response clearly disclose `bounded_bridge_ml_scorer`?

The seven previously unlabeled proposed hybrid top-20 papers did not appear in
this failed materialized canary response. They remain the reference rows to
check after a successful scorer-served canary:

- `https://openalex.org/W7155391647`
- `https://openalex.org/W7150806375`
- `https://openalex.org/W7131744631`
- `https://openalex.org/W4414112292`
- `https://openalex.org/W7116661261`
- `https://openalex.org/W4412072230`
- `https://openalex.org/W4414581097`

Web Recommended does not send canary headers. This is API-operator evidence
until a private preview path exists.

## Limitations

- `RESEARCH_RADAR_API_BASE` was not set in this shell; the Railway API base was
  taken from prior deployed readiness documentation.
- The first attempted live HTTP script made requests but failed while printing
  Unicode to the Windows console; the captured UTF-8 rerun still returned
  `materialized_heuristic` for the cohort canary.
- Railway CLI/token was unavailable, so deployed build metadata, env values, and
  gate logs could not be inspected directly.
- The deployed response did not expose a gate-closure reason.
- Public rollout stayed unserved in HTTP behavior, but public rollout disabled
  was not directly confirmed from Railway env.

