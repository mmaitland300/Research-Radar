import Link from "next/link";

import { FamilyBrief } from "./family-brief";
import { FAMILY_LABEL, FAMILY_SUMMARY, compactSnapshotLabel } from "./ranking-copy";
import { buildRecommendedFamilyHref, paperAnchorId } from "./url-state";
import { FAMILIES } from "./types";
import type { Family, RankedItem, RankedResponse } from "./types";

type RecommendedHeroProps = {
  bridgeEligibleOnly: boolean;
  bridgeEligibleOnlyDisabledNotice: boolean;
  bridgeRankingRunId: string;
  bridgeRunOverrideIgnored: boolean;
  data: RankedResponse | null;
  enableExperimentalBridgeView: boolean;
  family: Family;
  focusItem: RankedItem | null;
  focusPaperId: string | undefined;
  limit: number;
  nonBridgeRankingRunId: string | undefined;
  productRankingVersion: string;
  requestedRankingRunId: string | undefined;
  runContextPinned: boolean;
  surfacedWithTopics: number;
  topScore: number | null;
  usingUnpinnedLatestRun: boolean;
};

export function RecommendedHero({
  bridgeEligibleOnly,
  bridgeEligibleOnlyDisabledNotice,
  bridgeRankingRunId,
  bridgeRunOverrideIgnored,
  data,
  enableExperimentalBridgeView,
  family,
  focusItem,
  focusPaperId,
  limit,
  nonBridgeRankingRunId,
  productRankingVersion,
  requestedRankingRunId,
  runContextPinned,
  surfacedWithTopics,
  topScore,
  usingUnpinnedLatestRun
}: RecommendedHeroProps) {
  return (
    <section className={`panel page-hero family-hero family-hero-${family}`}>
      <div className="family-hero-grid">
        <div>
          <div className="panel-header">
            <div>
              <p className={`eyebrow family-${family}`}>Recommended</p>
              <h1>Ranked recommendations</h1>
            </div>
            <div className="stamp-row">
              <span className={`stamp stamp-family stamp-family-${family}`}>
                {family === "bridge" && bridgeEligibleOnly
                  ? "Eligible-only bridge view"
                  : family === "bridge"
                    ? "Bridge preview"
                    : `${FAMILY_LABEL[family]} feed`}
              </span>
              <span className="stamp">Materialized ranking run</span>
            </div>
          </div>
          <p className="hero-lead">{FAMILY_SUMMARY[family]}</p>
          {usingUnpinnedLatestRun ? (
            <div className="ranking-how-panel" role="status">
              <h3>Run pin warning</h3>
              <p className="muted-inline">
                Using latest succeeded run. For evidence review, use an explicit run id or run label
                so the page stays tied to the intended snapshot.
              </p>
            </div>
          ) : null}
          {family === "bridge" ? (
            <p className="muted-inline">
              Bridge evidence is experimental and run-specific. This page pins Bridge to{" "}
              <code>{bridgeRankingRunId}</code>; canary ML serving requires <code>limit=20</code>.
            </p>
          ) : null}
          {bridgeRunOverrideIgnored ? (
            <div className="ranking-how-panel" role="status">
              <h3>Bridge run pin applied</h3>
              <p className="muted-inline">
                Ignoring URL run override <code>{requestedRankingRunId}</code>. Bridge
                recommendations use the pinned Bridge scorer diagnostic run{" "}
                <code>{bridgeRankingRunId}</code> on this page.
              </p>
            </div>
          ) : null}
          {bridgeEligibleOnlyDisabledNotice ? (
            <div className="ranking-how-panel" role="status">
              <p className="muted-inline">
                Experimental eligible-only bridge review view is disabled. Showing the full bridge
                preview instead.
              </p>
            </div>
          ) : null}
          {family === "bridge" && enableExperimentalBridgeView ? (
            <div className="ranking-how-panel">
              <h3>Experimental bridge review guardrail</h3>
              <p>
                <strong>Experimental bridge review view; still under evaluation and not default.</strong>
              </p>
              <p className="muted-inline">Single-reviewer, top-20, offline audit evidence only.</p>
              <p className="muted-inline">
                Current Bridge scorer diagnostic run: <code>{bridgeRankingRunId}</code>. This is an
                experimental arm for this corpus snapshot, not proof or default readiness.
              </p>
              <p>
                <Link
                  className="action-link"
                  href={buildRecommendedFamilyHref("bridge", {
                    focusPaperId,
                    limit: 20,
                    bridgeEligibleOnly: true
                  })}
                  scroll={false}
                >
                  Open current experimental bridge review view
                </Link>
              </p>
            </div>
          ) : null}
          {family === "bridge" && bridgeEligibleOnly ? (
            <div className="ranking-how-panel">
              <h3>Eligible-only bridge view</h3>
              <p>
                <strong>Experimental bridge review view; still under evaluation and not default.</strong>
              </p>
              <p className="muted-inline">Single-reviewer, top-20, offline audit evidence only.</p>
              <p className="muted-inline">
                Eligible-only filtering is exposed only as an experimental review aid for the resolved
                run. It is not proof, not a superiority claim, and not default readiness.
              </p>
            </div>
          ) : null}
          <p>
            Papers come from a <strong>materialized ranking run</strong>. The explanations reflect the
            recorded signal weights for the resolved run. The <strong>undercited</strong> family is scoped
            to a frozen low-citation candidate pool for the same corpus snapshot.
          </p>
          <nav className="tabs" aria-label="Recommendation family">
            {FAMILIES.map((f) => (
              <Link
                key={f}
                href={buildRecommendedFamilyHref(f, {
                  focusPaperId,
                  rankingRunId: nonBridgeRankingRunId,
                  limit,
                  bridgeEligibleOnly: f === "bridge" ? bridgeEligibleOnly : false
                })}
                aria-current={f === family ? "page" : undefined}
                scroll={false}
              >
                {FAMILY_LABEL[f]}
              </Link>
            ))}
          </nav>
          {family === "bridge" && enableExperimentalBridgeView ? (
            <nav className="tabs" aria-label="Bridge feed view">
              <Link
                href={buildRecommendedFamilyHref("bridge", {
                  focusPaperId,
                  limit,
                  bridgeEligibleOnly: false
                })}
                aria-current={!bridgeEligibleOnly ? "page" : undefined}
                scroll={false}
              >
                Full bridge feed
              </Link>
              <Link
                href={buildRecommendedFamilyHref("bridge", {
                  focusPaperId,
                  limit,
                  bridgeEligibleOnly: true
                })}
                aria-current={bridgeEligibleOnly ? "page" : undefined}
                scroll={false}
              >
                Eligible-only bridge feed
              </Link>
            </nav>
          ) : null}
          {data ? (
            <div className="hero-metrics" aria-label="Ranking run summary">
              <article className="metric-card">
                <p className="metric-label">Run context</p>
                <p className="metric-value">{runContextPinned ? "Pinned" : "Latest"}</p>
              </article>
              <article className="metric-card">
                <p className="metric-label">Rows surfaced</p>
                <p className="metric-value">{data.total}</p>
              </article>
              <article className="metric-card">
                <p className="metric-label">Top score</p>
                <p className="metric-value">{topScore != null ? topScore.toFixed(3) : "n/a"}</p>
              </article>
              <article className="metric-card">
                <p className="metric-label">Snapshot</p>
                <p className="metric-value">{compactSnapshotLabel(data.corpus_snapshot_version)}</p>
              </article>
            </div>
          ) : null}
          {data ? (
            <p className="muted-inline">
              Showing {data.total} {data.total === 1 ? "paper" : "papers"} from{" "}
              {data.ranking_mode === "bounded_ml_scorer"
                ? "bounded ML scorer order over materialized metadata"
                : data.ranking_mode === "bounded_bridge_ml_scorer"
                  ? "experimental Bridge ranking over materialized metadata"
                  : `a materialized ${FAMILY_LABEL[family].toLowerCase()} ranking run`}
              {"; "}
              {surfacedWithTopics} include topic labels.
            </p>
          ) : null}
          <p className="muted-inline">
            Topic labels are imported metadata and can be noisy; use them as coarse navigation hints,
            not authoritative classifications.
          </p>
          {focusPaperId ? (
            <p className="muted-inline">
              Focus paper: <code>{focusPaperId}</code>
              {focusItem ? (
                <>
                  {` is visible in this ${FAMILY_LABEL[family].toLowerCase()} list. `}
                  <Link href={`#${paperAnchorId(focusPaperId)}`}>Jump to focused row</Link>.
                </>
              ) : (
                ` is not in the current top ${limit} rows for this view, but the run context is still pinned while you switch families.`
              )}
            </p>
          ) : null}
          <details className="ranking-why-details">
            <summary>Technical run metadata</summary>
            <p className="result-breakdown">
              Resolved run label: <code>{data?.ranking_version ?? "unavailable"}</code>.
            </p>
            <p className="result-breakdown">
              Ranking mode: <code>{data?.ranking_mode ?? "materialized_heuristic"}</code>
              {data?.ranking_mode_detail ? <>. {data.ranking_mode_detail}</> : "."}
            </p>
            {family === "emerging" ? (
              <p className="result-breakdown">
                Bounded scorer serving is eligible only for Emerging requests with{" "}
                <code>limit=20</code>; other limits use materialized heuristic order.
              </p>
            ) : null}
            {family === "bridge" ? (
              <p className="result-breakdown">
                Bounded Bridge scorer serving is eligible only for Bridge requests with{" "}
                <code>limit=20</code> and the pinned Bridge run; otherwise the API uses materialized
                heuristic order.
              </p>
            ) : null}
            <p className="result-breakdown">
              {family === "bridge" ? (
                <>
                  Bridge run pin: <code>{bridgeRankingRunId}</code>. The global product run label is
                  not sent for Bridge requests.
                </>
              ) : productRankingVersion ? (
                <>
                  Run label filter: <code>{productRankingVersion}</code>.
                </>
              ) : (
                <>No run label filter is configured; the API resolves the latest succeeded run.</>
              )}
              {data ? (
                <>
                  {" "}
                  Resolved run: <code>{data.ranking_run_id}</code>; snapshot:{" "}
                  <code>{data.corpus_snapshot_version}</code>.
                </>
              ) : null}
            </p>
            <p className="result-breakdown">
              Storage provenance: materialized family rows are read from <code>paper_scores</code>.
              {data?.ranking_mode === "bounded_ml_scorer" ? (
                <>
                  {" "}
                  Result order was selected by the bounded ML scorer; displayed <code>final_score</code>{" "}
                  and signal metadata still come from the materialized row.
                </>
              ) : data?.ranking_mode === "bounded_bridge_ml_scorer" ? (
                <>
                  {" "}
                  Bridge order was selected by bounded ML scorer rollout. It blends{" "}
                  <code>bridge_score</code> with a frozen Bridge ML scorer; displayed{" "}
                  <code>final_score</code> and signal metadata still come from the materialized row.
                </>
              ) : (
                <>
                  {" "}
                  Result ordering is <code>final_score desc, work_id asc</code>.
                </>
              )}{" "}
              The undercited pool definition is documented in{" "}
              <code>docs/candidate-pool-low-cite.md</code>.
            </p>
          </details>
        </div>
        <FamilyBrief family={family} />
      </div>
    </section>
  );
}
