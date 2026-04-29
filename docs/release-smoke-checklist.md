# Release smoke checklist (web)

Manual checks before tagging a release or demoing a new deploy. Assumes API and web are running against the same Postgres snapshot you intend to show.

**Environment**

- `DATABASE_URL` / API can reach the ranking data you expect.
- Optional: `NEXT_PUBLIC_RANKING_VERSION` and/or `NEXT_PUBLIC_EMBEDDING_VERSION` pinned for a stable demo.
- For **experimental** bridge steps below: set `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW=true`, restart the web app, then revert after review.

**Scope reminder**

- Bridge surfaces are **diagnostic evidence** and an **experimental bridge review arm**, not validation and not default-ready (`ready_for_default=false` in audit artifacts).

---

## Recommended — emerging

- [ ] Open `/recommended`.
- [ ] Open `/recommended?family=emerging` (e.g. `http://localhost:3000/recommended?family=emerging`).
- [ ] Page loads without API errors; run metadata (version / run id / snapshot) looks intended.
- [ ] List renders with scores and explanations where the API provides them.

## Recommended — bridge (full preview)

- [ ] Open `/recommended?family=bridge`.
- [ ] Copy states bridge as **preview / diagnostics** (measured signal; ordering depends on pinned run).
- [ ] Optional: append `&ranking_run_id=<id>` to pin a known succeeded run for reviewer evidence.

## Recommended — undercited

- [ ] Open `/recommended?family=undercited`.
- [ ] Page explains low-cite pool scope; list loads.

## Recommended — eligible-only bridge, experimental **disabled**

- [ ] With `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW` unset or `false`, open  
  `/recommended?family=bridge&bridge_eligible_only=true`.
- [ ] UI shows the **disabled** notice and the **full** bridge feed (eligible-only filter not applied to the request).
- [ ] No crash; API is not called with `bridge_eligible_only=true` unless the flag is on (see `recommended/page.tsx`).

## Recommended — eligible-only bridge, experimental **enabled** (objective run)

- [ ] Set `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW=true` and restart web.
- [ ] Open  
  `/recommended?family=bridge&ranking_run_id=rank-60910a47b4&bridge_eligible_only=true`.
- [ ] Experimental guardrail copy is visible (not validated, not default; single-reviewer, top-20, offline audit).
- [ ] List loads; rows show bridge eligibility where returned by the API.
- [ ] Toggle back to full bridge tab (`bridge_eligible_only` off) and confirm navigation works.

## Evaluation — bridge (if route is deployed)

- [ ] Open `/evaluation`.
- [ ] Open `/evaluation?family=bridge` (bridge is a supported family in `evaluation/page.tsx`).
- [ ] Disclaimer and ranked vs baseline arms render; no hard error from the compare API.

## Stale route guard

For the current public reviewer routes, confirm the old smoke identifiers do not appear:

- [ ] `/recommended`
- [ ] `/recommended?family=bridge`
- [ ] `/recommended?family=undercited`
- [ ] `/trends`
- [ ] `/evaluation`
- [ ] `/evaluation?family=bridge`

Expected current public metadata:

- [ ] `rank-60910a47b4` appears on ranking-backed routes.
- [ ] `source-snapshot-v2-candidate-plan-20260428` appears where run or snapshot metadata is visible.
- [ ] `rank-3904fec89d` does not appear.
- [ ] `source-snapshot-20260425-044015` does not appear.

---

After checks, turn **off** `NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW` for normal/public demos unless you deliberately ship that exposure.
