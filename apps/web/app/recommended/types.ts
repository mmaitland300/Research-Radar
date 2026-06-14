export const FAMILIES = ["emerging", "bridge", "undercited"] as const;

export type Family = (typeof FAMILIES)[number];

export type RankedSignals = {
  semantic: number | null;
  citation_velocity: number | null;
  topic_growth: number | null;
  bridge: number | null;
  diversity_penalty: number | null;
};

export type RankedSignalExplanation = {
  key: string;
  label: string;
  role: "used" | "measured" | "experimental" | "penalty" | "not_computed";
  value: number | null;
  contribution: number | null;
  summary: string;
};

export type RankedListExplanation = {
  family: string;
  headline: string;
  bullets: string[];
  used_in_ordering: string[];
  measured_only: string[];
  experimental: string[];
};

export type RankingMode =
  | "materialized_heuristic"
  | "bounded_ml_scorer"
  | "bounded_bridge_ml_scorer";

export type RankedItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  topics: string[];
  signals: RankedSignals;
  final_score: number;
  reason_short: string;
  signal_explanations: RankedSignalExplanation[];
  bridge_eligible: boolean | null;
};

export type RankedResponse = {
  ranking_run_id: string;
  ranking_version: string;
  corpus_snapshot_version: string;
  family: string;
  ranking_mode: RankingMode;
  ranking_mode_detail: string | null;
  scorer_surface: string | null;
  bridge_recommendations_ml_served: boolean | null;
  bridge_rank_pct_hybrid_alpha: number | null;
  bridge_rank_pct_scope: string | null;
  emitted_to_public_users: boolean | null;
  total: number;
  list_explanation: RankedListExplanation;
  items: RankedItem[];
};
