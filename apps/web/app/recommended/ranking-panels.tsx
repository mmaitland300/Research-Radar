import type {
  Family,
  RankedItem,
  RankedListExplanation,
  RankedSignalExplanation,
  RankedSignals,
  RankingMode
} from "./types";
import { FAMILY_LABEL } from "./ranking-copy";

export function formatSignals(signals: RankedSignals): string {
  const entries: [string, number][] = [];
  if (signals.semantic != null) entries.push(["semantic", signals.semantic]);
  if (signals.citation_velocity != null) {
    entries.push(["citation_velocity", signals.citation_velocity]);
  }
  if (signals.topic_growth != null) entries.push(["topic_growth", signals.topic_growth]);
  if (signals.bridge != null) entries.push(["bridge", signals.bridge]);
  if (signals.diversity_penalty != null) {
    entries.push(["diversity_penalty", signals.diversity_penalty]);
  }
  if (entries.length === 0) return "n/a";
  return entries.map(([k, v]) => `${k}=${Number(v).toFixed(4)}`).join(", ");
}

function barWidthPercent(value: number | null, role: RankedSignalExplanation["role"]): number {
  if (value == null || role === "not_computed") return 0;
  return Math.min(100, Math.round(Math.max(0, value) * 100));
}

function barFillClass(role: RankedSignalExplanation["role"]): string {
  if (role === "used") return "ranking-bar-fill ranking-bar-used";
  if (role === "measured" || role === "experimental") {
    return "ranking-bar-fill ranking-bar-measured";
  }
  if (role === "penalty") return "ranking-bar-fill ranking-bar-penalty";
  return "ranking-bar-fill ranking-bar-none";
}

function explanationSummary(explanations: RankedSignalExplanation[]): string {
  const count = (role: RankedSignalExplanation["role"]) =>
    explanations.filter((e) => e.role === role).length;
  const parts: string[] = [];
  const used = count("used");
  const measured = count("measured");
  const experimental = count("experimental");
  const penalty = count("penalty");
  const notComputed = count("not_computed");
  if (used) parts.push(`${used} used`);
  if (measured) parts.push(`${measured} measured`);
  if (experimental) parts.push(`${experimental} experimental`);
  if (penalty) parts.push(`${penalty} penalty`);
  if (notComputed) parts.push(`${notComputed} not computed`);
  return parts.length > 0 ? parts.join(" | ") : "No signal breakdown";
}

export function RankingHowPanel({
  expl,
  family,
  rankingMode
}: {
  expl: RankedListExplanation;
  family: Family;
  rankingMode: RankingMode;
}) {
  const emergingScorerOrdered = rankingMode === "bounded_ml_scorer";
  const bridgeScorerOrdered = rankingMode === "bounded_bridge_ml_scorer";
  const scorerOrdered = emergingScorerOrdered || bridgeScorerOrdered;
  return (
    <div className="ranking-how-panel">
      <h3>{expl.headline}</h3>
      {emergingScorerOrdered ? (
        <p className="muted-inline">
          Order selected by bounded ML scorer rollout. The bullets below describe materialized row
          metadata from the ranking run, not the final visible order.
        </p>
      ) : null}
      {bridgeScorerOrdered ? (
        <p className="muted-inline">
          Experimental Bridge ranking. The Bridge order blends bridge_score with a frozen Bridge ML
          scorer; the bullets below describe materialized row metadata from the ranking run.
        </p>
      ) : null}
      <ul>
        {expl.bullets.map((b) => (
          <li key={b}>{b}</li>
        ))}
      </ul>
      <p className="ranking-how-meta">
        <strong>{scorerOrdered ? "Materialized score signals:" : "Used in ordering:"}</strong>{" "}
        {expl.used_in_ordering.join(", ") || "none"}
        <br />
        <strong>Measured only (transparency):</strong> {expl.measured_only.join(", ") || "none"}
        {expl.experimental.length > 0 ? (
          <>
            <br />
            <strong>Experimental:</strong> {expl.experimental.join(", ")}
          </>
        ) : null}
        <br />
        <span className="muted-inline">
          Full {family === "bridge" ? "Bridge" : FAMILY_LABEL[family]} run details are available in
          Technical run metadata.
        </span>
      </p>
    </div>
  );
}

export function bridgeEligibilityLabel(bridgeEligible: boolean | null): string {
  if (bridgeEligible === true) return "Bridge gate passed";
  if (bridgeEligible === false) return "Bridge gate not passed";
  return "Bridge gate not recorded";
}

function bridgeSignalOrderingLine(explanations: RankedSignalExplanation[]): string | null {
  const bridge = explanations.find((e) => e.key === "bridge");
  if (!bridge) return null;
  if (bridge.role === "used") {
    return "Cross-cluster (bridge) signal is used in final ordering for this run.";
  }
  if (bridge.role === "measured" || bridge.role === "experimental") {
    return "Cross-cluster (bridge) signal is measured for this run but is not used in final ordering.";
  }
  return "Cross-cluster (bridge) signal is not computed for this row.";
}

function bridgeOrderingState(
  explanations: RankedSignalExplanation[]
): "used" | "measured only" | "not computed" {
  const bridge = explanations.find((e) => e.key === "bridge");
  if (!bridge) return "not computed";
  if (bridge.role === "used") return "used";
  if (bridge.role === "measured" || bridge.role === "experimental") return "measured only";
  return "not computed";
}

function bridgeEligibilityState(
  bridgeEligible: boolean | null
): "passed" | "not passed" | "not recorded" {
  if (bridgeEligible === true) return "passed";
  if (bridgeEligible === false) return "not passed";
  return "not recorded";
}

export function bridgeRationaleLine(item: RankedItem): string {
  const ordering = bridgeOrderingState(item.signal_explanations);
  const eligibility = bridgeEligibilityState(item.bridge_eligible);
  const bridgeScore =
    item.signals.bridge != null && Number.isFinite(item.signals.bridge)
      ? item.signals.bridge.toFixed(3)
      : "n/a";
  return `Bridge signal: ${ordering}. Bridge gate: ${eligibility}. Bridge score: ${bridgeScore}.`;
}

export function BridgeSignalOrderingParagraph({
  explanations
}: {
  explanations: RankedSignalExplanation[];
}) {
  const line = bridgeSignalOrderingLine(explanations);
  return line ? <p className="result-breakdown">{line}</p> : null;
}

export function SignalExplanationDetails({
  explanations
}: {
  explanations: RankedSignalExplanation[];
}) {
  return (
    <details className="ranking-why-details">
      <summary>Why this surfaced | {explanationSummary(explanations)}</summary>
      {explanations.map((e) => (
        <div key={e.key} className="ranking-signal-row">
          <div className="ranking-signal-label">
            <span>{e.label}</span>
            <span className="ranking-signal-role">{e.role.replace("_", " ")}</span>
          </div>
          <div className="ranking-bar-track" aria-hidden>
            <div
              className={barFillClass(e.role)}
              style={{ width: `${barWidthPercent(e.value, e.role)}%` }}
            />
          </div>
          <p className="result-breakdown" style={{ marginTop: 4 }}>
            {e.summary}
            {e.contribution != null && e.role !== "not_computed" ? (
              <>
                {" "}
                (contribution to score: {e.contribution.toFixed(4)})
              </>
            ) : null}
          </p>
        </div>
      ))}
    </details>
  );
}
