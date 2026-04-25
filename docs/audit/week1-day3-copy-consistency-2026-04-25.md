# Week 1 Day 3 - Copy consistency pass

Date: `2026-04-25`
Owner: `@mmaitland300`
Status: `complete`

## Scope

- trends page
- recommended page
- evaluation page
- recommendation-family API descriptions
- product/meta endpoint language
- candidate pool definition docs

## Preflight rules

- If docs and code disagree, record the mismatch and treat current shipped code/API behavior as the Day 3 audit target.
- Do not silently rewrite docs to match aspirational behavior from planning notes.
- `docs/candidate-pool-low-cite.md` is read-only for Day 3 unless implementation mismatch is found.
- If candidate-pool semantics need to change, defer to a separate revision with a pool revision bump.

## Canonical references

- `docs/candidate-pool-low-cite.md`
- `docs/build-brief.md`
- `docs/roadmap.md`

## Canonical statements used in this pass

- Undercited pool is the frozen low-cite candidate pool definition in `docs/candidate-pool-low-cite.md` (not zero-citation-only, not whole corpus).
- Materialized undercited rows come from that pool; run config may still affect ordering/signal blend inside the eligible set.
- Emerging and bridge operate over included corpus works unless explicitly filtered.
- Semantic relevance should not be described as active ordering logic unless the selected run says that signal is used.
- Bridge-specific eligibility/signals are bridge-family only and run-dependent.
- Evaluation v0 is proxy comparison, not human relevance judgment.

## Surface matrix

- `apps/web/app/recommended/page.tsx` - edited
- `apps/web/app/page.tsx` - edited
- `apps/web/app/evaluation/page.tsx` - checked, no change needed
- `apps/web/app/trends/page.tsx` - checked, no change needed
- `apps/api/app/main.py` (`GET /api/v1/recommendations/families`) - edited
- `apps/api/app/main.py` (product/evaluation summary language) - checked, no change needed
- `docs/candidate-pool-low-cite.md` - checked, no change needed

## Known risky phrases checklist

- "semantically relevant" - found and replaced in API family description.
- "semantic fit" - found and replaced in recommended and home-page copy.
- "semantic score" - reviewed; retained only where explicitly qualified.
- "bridge papers connect clusters" - reviewed and qualified as run-dependent where needed.
- "beats popularity-only" - found and replaced in API family description.
- "undiscovered gems" - not present in audited surfaces.
- "all papers" - not present in audited surfaces.
- "not yet modeled" - no contradictory usage found in audited surfaces.
- "latest" / "pinned" - Recommended and Evaluation wording remain consistent (explicit pin vs latest fallback).
- "candidate pool" - verified against low-cite doc framing and evaluation disclaimers.

## Findings

### 1) Recommended page ML1 wording conflicted with roadmap non-goal

- Surface: `apps/web/app/recommended/page.tsx`
- Previous text: "ML milestone 1 fills semantic_score and retrieval; bridge-style scores follow once clusters are available."
- Conflict: `docs/roadmap.md` states ML1 retrieval is in scope, but writing `semantic_score` into `paper_scores` is deferred until a defined relevance target exists.
- Resolution: updated recommended page copy to reflect ML1 retrieval scope and deferred `semantic_score`.

### 2) Recommendation-family API descriptions overclaimed semantics and baseline wins

- Surface: `apps/api/app/main.py` (`GET /api/v1/recommendations/families`)
- Previous text included "semantically relevant" and "beats popularity-only ranking baselines."
- Conflict: those phrases can overstate active ordering semantics and evaluation confidence across runs.
- Resolution: replaced descriptions with behavior-accurate wording tied to materialized ranking runs and low-cite candidate-pool scope.

### 3) Home-page card copy implied active semantic ordering

- Surface: `apps/web/app/page.tsx`
- Previous text: "Rank papers by semantic fit, citation velocity, and local topic growth..."
- Conflict: implies semantic ordering is always active rather than run-dependent.
- Resolution: revised the card to state citation/topic-growth ordering and that semantic fields are shown only when a selected run computes and uses them.

## Edits made

- `apps/web/app/recommended/page.tsx`: revised Roadmap card copy so ML1 is retrieval-first, with `semantic_score` explicitly deferred pending relevance target definition.
- `apps/web/app/recommended/page.tsx`: revised emerging-family guidance to avoid implying semantic ordering unless explicitly marked as used by the run.
- `apps/web/app/page.tsx`: revised home-page "Emerging Papers" card to remove unconditional semantic-ordering claim.
- `apps/api/app/main.py`: revised `GET /api/v1/recommendations/families` descriptions for emerging, bridge, and undercited to avoid semantic/baseline overclaims.

## Result

Day 3 copy consistency pass closed for the audited surfaces. Highest-risk overclaims were removed from recommended-page family guidance and recommendation-family API descriptions; trends/evaluation framing already matched curated-scope and proxy-only semantics and did not require edits.

## Done criteria checklist

- [x] Identified and fixed at least one contradictory family/ML semantics statement.
- [x] Audit note exists and lists canonical statements.
- [x] Every audited surface is marked checked or edited.
- [x] Undercited copy matches `docs/candidate-pool-low-cite.md` v0.
- [x] Emerging and bridge copy does not imply semantic/bridge signals are used unless run explanations indicate usage.
- [x] Evaluation copy clearly states proxy comparison, not human relevance judgment.
- [x] `NEXT_PUBLIC_RANKING_VERSION` / latest-run wording is consistent across Recommended and Evaluation.
- [x] Day 3 status line added to `docs/eval-foundation-two-week-plan.md` with this evidence path.
