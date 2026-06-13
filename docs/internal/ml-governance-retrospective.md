# ML governance experiment retrospective

> Internal context. The historical artifacts referenced here live on the
> `archive/ml-governance-audit` branch and in git history, not on `main`.

Research Radar had a phase where I tried to answer a reasonable question:
could the recommendation system use a learned scorer without quietly making the
product worse?

The shadow/governance experiment explored that question by freezing candidate
pools, recording scorer inputs, separating labels from serving paths, and
checking that any learned ranking stayed disabled until it cleared basic
quality and safety gates. That was useful evaluation discipline for a prototype
that was starting to mix data engineering, ranking, manual review, and product
surface area.

The extra scaffolding grew because each concern had a real failure mode. Labels
could leak into scoring. A scorer could look good on the same rows it was tuned
on. A rollout artifact could be confused with production authorization. A
ranking change could be hard to explain later. The audit files made those
boundaries explicit while the experiment was active.

What I learned:

- frozen inputs and hashes are worth keeping when runtime code depends on
  them;
- generated worksheets, eval dumps, and review bundles are useful during an
  experiment but noisy as permanent project material;
- the best product signal came from smaller, clearer checks tied to actual
  serving behavior;
- the process layer was becoming larger than the thing it was supposed to
  protect.

So I discontinued the governance-notebook approach and archived the full record.
The historical branch is `archive/ml-governance-audit`. It preserves the
experiment trail for anyone who wants to inspect it, while `main` stays focused
on the portfolio project: product code, concise public docs, tests, and only the
runtime-pinned artifacts the deployed scorers need.

That boundary is intentional. The project should read like a person built a
research recommender, learned from a heavy evaluation experiment, and kept the
parts that still earn their place.
