import Link from "next/link";

import {
  BRIDGE_SCORER_RESPONSE_COPY,
  EMERGING_SCORER_RESPONSE_COPY,
  FAMILY_LABEL
} from "./ranking-copy";
import {
  BridgeSignalOrderingParagraph,
  RankingHowPanel,
  SignalExplanationDetails,
  bridgeEligibilityLabel,
  bridgeRationaleLine,
  formatSignals
} from "./ranking-panels";
import { paperAnchorId } from "./url-state";
import type { Family, RankedResponse } from "./types";

type RecommendedResultsProps = {
  bridgeEligibleOnly: boolean;
  data: RankedResponse;
  family: Family;
  focusPaperId: string | undefined;
  limit: number;
};

export function RecommendedErrorPanel({
  error,
  status
}: {
  error: string;
  status: number | null;
}) {
  return (
    <section className="panel instrument-panel">
      <p>{error}</p>
      {status === 404 ? (
        <p className="muted-inline">
          Example run label: <code>shadow-generalization-product-candidate-ranking-v1</code>
        </p>
      ) : null}
    </section>
  );
}

export function RecommendedResults({
  bridgeEligibleOnly,
  data,
  family,
  focusPaperId,
  limit
}: RecommendedResultsProps) {
  return (
    <section className="panel section-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow eyebrow-muted">Live ranking surface</p>
          <h2>{family === "bridge" ? "Bridge preview results" : `${FAMILY_LABEL[family]} results`}</h2>
        </div>
        <div className="stamp-row">
          <span className={`stamp stamp-family stamp-family-${family}`}>Family: {data.family}</span>
          {family === "bridge" && bridgeEligibleOnly ? <span className="stamp">Eligible only</span> : null}
          {data.ranking_mode === "bounded_bridge_ml_scorer" ? (
            <span className="stamp">Experimental Bridge ranking</span>
          ) : data.ranking_mode === "bounded_ml_scorer" ? (
            <span className="stamp">Bounded ML scorer order</span>
          ) : (
            <span className="stamp">Order: score desc, stable tie-break</span>
          )}
          <span className="stamp">Limit: {limit}</span>
        </div>
      </div>
      {data.ranking_mode === "bounded_bridge_ml_scorer" ? (
        <p className="muted-inline">{BRIDGE_SCORER_RESPONSE_COPY}</p>
      ) : data.ranking_mode === "bounded_ml_scorer" ? (
        <p className="muted-inline">{EMERGING_SCORER_RESPONSE_COPY}</p>
      ) : null}
      {family === "bridge" ? (
        <p className="muted-inline">
          Bridge preview shows measured cross-cluster signal for the resolved run. Some rows may not
          pass the optional bridge gate; use the eligible-only view when you want to hide those rows.
        </p>
      ) : null}
      {family === "emerging" || family === "bridge" ? (
        <RankingHowPanel
          expl={data.list_explanation}
          family={family}
          rankingMode={data.ranking_mode}
        />
      ) : null}
      {data.items.length === 0 ? (
        <p>No rows for this family in the selected run.</p>
      ) : (
        <ul className="result-list">
          {data.items.map((item) => (
            <li
              key={item.paper_id}
              id={focusPaperId === item.paper_id ? paperAnchorId(item.paper_id) : undefined}
              className={`result-item result-item-${family}${
                focusPaperId === item.paper_id ? " result-item-focus" : ""
              }`}
            >
              <div className="result-heading">
                <p className="result-title">
                  <Link href={`/papers/${encodeURIComponent(item.paper_id)}`}>{item.title}</Link>
                </p>
                <span className={`result-score result-score-${family}`}>
                  {item.final_score.toFixed(3)}
                </span>
              </div>
              <p className="result-meta">
                {item.year} | cites: {item.citation_count} | {item.source_slug ?? "unknown venue"}
              </p>
              <div className="stamp-row stamp-row-inline">
                <span className={`stamp stamp-family stamp-family-${family}`}>
                  {FAMILY_LABEL[family]}
                </span>
                {family === "bridge" ? (
                  <span className="stamp">{bridgeEligibilityLabel(item.bridge_eligible)}</span>
                ) : null}
                {focusPaperId === item.paper_id ? <span className="stamp">Focus paper</span> : null}
                <span className="stamp">
                  {item.topics.length} {item.topics.length === 1 ? "topic label" : "topic labels"}
                </span>
              </div>
              {item.topics.length > 0 ? (
                <div className="chip-row" aria-label="Top topics">
                  {item.topics.map((t) => (
                    <span key={t} className="chip">
                      {t}
                    </span>
                  ))}
                </div>
              ) : null}
              <p className="result-reason">{item.reason_short}</p>
              {family === "bridge" ? (
                <p className="result-breakdown">{bridgeRationaleLine(item)}</p>
              ) : null}
              {family === "bridge" && bridgeEligibleOnly ? (
                <p className="result-breakdown">
                  This row passed the bridge eligibility gate for the resolved run.
                </p>
              ) : null}
              {family === "bridge" ? (
                <BridgeSignalOrderingParagraph explanations={item.signal_explanations} />
              ) : null}
              {family === "emerging" && item.signal_explanations?.length ? (
                <SignalExplanationDetails explanations={item.signal_explanations} />
              ) : null}
              {family === "bridge" && item.signal_explanations?.length ? (
                <SignalExplanationDetails explanations={item.signal_explanations} />
              ) : null}
              {family === "bridge" ? (
                <p className="result-breakdown">Signals: {formatSignals(item.signals)}</p>
              ) : family === "emerging" && item.signal_explanations?.length ? null : (
                <p className="result-breakdown">Signals: {formatSignals(item.signals)}</p>
              )}
              <div className="action-row" aria-label="Related views">
                <Link className="action-link" href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                  Open dossier
                </Link>
                <Link className="action-link" href={`/evaluation?family=${family}`}>
                  Compare in evaluation
                </Link>
                <Link className="action-link" href="/trends">
                  Check topic momentum
                </Link>
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export function RecommendationContextCards() {
  return (
    <section className="grid">
      <article className="card">
        <h2>Current ML boundary</h2>
        <p>
          Emerging can be reordered by the bounded ML scorer only when the backend rollout gate is
          enabled. Bridge and Under-cited are not served by that scorer; their order comes from
          materialized ranking rows.
        </p>
      </article>
      <article className="card">
        <h2>Heuristic baseline</h2>
        <p>
          The rule-only undercited list (<code>/api/v1/recommendations/undercited</code>) uses the same
          pool definition but is not tied to a corpus snapshot. For snapshot-scoped A/B against the
          ranked undercited family, use <Link href="/evaluation?family=undercited">Evaluation</Link>.
        </p>
      </article>
    </section>
  );
}
