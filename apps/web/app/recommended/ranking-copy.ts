import type { Family } from "./types";

export const FAMILY_LABEL: Record<Family, string> = {
  emerging: "Emerging",
  bridge: "Bridge",
  undercited: "Under-cited"
};

export const FAMILY_SUMMARY: Record<Family, string> = {
  emerging: "Momentum-weighted papers gaining relevance inside the current corpus.",
  bridge:
    "Candidate bridge papers with measured cross-cluster signal, presented as a separate experimental view.",
  undercited: "Low-cite candidates that appear stronger than their current attention level."
};

export const FAMILY_NOTES: Record<Family, string[]> = {
  emerging: [
    "Topic-growth and citation-velocity signals should dominate the list.",
    "General semantic relevance is not treated as a default quality score. Some pinned runs use embedding fit as one bounded ranking feature, and the UI labels when that feature is used.",
    "A bounded ML scorer rollout may reorder Emerging only when the backend gate is explicitly enabled.",
    "The goal is early importance, not raw popularity."
  ],
  bridge: [
    "Bridge signal is visible for inspection and may be measured-only or experimental depending on the pinned run.",
    "Use this page to inspect cross-cluster candidates; it is still under evaluation.",
    "Pinned runs matter because bridge evidence is run-specific."
  ],
  undercited: [
    "These rows are judged against a low-cite candidate pool, not the whole corpus.",
    "The family should surface overlooked strength before attention catches up.",
    "Snapshot scope matters because the pool is frozen to a corpus version."
  ]
};

export const EMERGING_SCORER_RESPONSE_COPY =
  "This response was ordered by the bounded ML scorer rollout. Displayed scores and signal explanations still come from the materialized ranking row.";
export const BRIDGE_SCORER_RESPONSE_COPY =
  "Bridge order selected by bounded ML scorer rollout. Experimental Bridge ranking blends bridge_score with a frozen Bridge ML scorer and is still under evaluation.";

export function compactSnapshotLabel(snapshotVersion: string): string {
  const match = snapshotVersion.match(/(\d{8})/);
  if (!match) return "Current corpus snapshot";
  const raw = match[1];
  return `Snapshot ${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
}
