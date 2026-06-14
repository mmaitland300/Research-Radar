import {
  BRIDGE_RANKING_RUN_ID,
  ENABLE_EXPERIMENTAL_BRIDGE_VIEW,
  PRODUCT_RANKING_VERSION,
  fetchRanked
} from "./data";
import { RecommendedHero } from "./recommended-hero";
import {
  RecommendationContextCards,
  RecommendedErrorPanel,
  RecommendedResults
} from "./recommended-results";
import { parseRecommendedUrlState } from "./url-state";
import type { SearchParams } from "./url-state";

type PageProps = {
  searchParams: Promise<SearchParams>;
};

export default async function RecommendedPage({ searchParams }: PageProps) {
  const resolvedSearchParams = await searchParams;
  const {
    family,
    focusPaperId,
    requestedRankingRunId,
    limit,
    bridgeEligibleOnlyRequested
  } = parseRecommendedUrlState(resolvedSearchParams);

  const rankingRunId = family === "bridge" ? BRIDGE_RANKING_RUN_ID : requestedRankingRunId;
  const bridgeEligibleOnly = ENABLE_EXPERIMENTAL_BRIDGE_VIEW && bridgeEligibleOnlyRequested;
  const bridgeEligibleOnlyDisabledNotice =
    family === "bridge" && bridgeEligibleOnlyRequested && !ENABLE_EXPERIMENTAL_BRIDGE_VIEW;
  const bridgeRunOverrideIgnored =
    family === "bridge" &&
    Boolean(requestedRankingRunId) &&
    requestedRankingRunId !== BRIDGE_RANKING_RUN_ID;
  const nonBridgeRankingRunId = family === "bridge" ? undefined : requestedRankingRunId;
  const runContextPinned =
    family === "bridge" || Boolean(rankingRunId || PRODUCT_RANKING_VERSION);
  const usingUnpinnedLatestRun =
    family !== "bridge" && !rankingRunId && !PRODUCT_RANKING_VERSION;

  const { data, error, status } = await fetchRanked(family, {
    limit,
    rankingRunId,
    bridgeEligibleOnly
  });

  const topScore = data?.items[0]?.final_score ?? null;
  const surfacedWithTopics = data?.items.filter((item) => item.topics.length > 0).length ?? 0;
  const focusItem = focusPaperId
    ? data?.items.find((item) => item.paper_id === focusPaperId) ?? null
    : null;

  return (
    <main className={`page page-family page-family-${family}`}>
      <RecommendedHero
        bridgeEligibleOnly={bridgeEligibleOnly}
        bridgeEligibleOnlyDisabledNotice={bridgeEligibleOnlyDisabledNotice}
        bridgeRankingRunId={BRIDGE_RANKING_RUN_ID}
        bridgeRunOverrideIgnored={bridgeRunOverrideIgnored}
        data={data}
        enableExperimentalBridgeView={ENABLE_EXPERIMENTAL_BRIDGE_VIEW}
        family={family}
        focusItem={focusItem}
        focusPaperId={focusPaperId}
        limit={limit}
        nonBridgeRankingRunId={nonBridgeRankingRunId}
        productRankingVersion={PRODUCT_RANKING_VERSION}
        requestedRankingRunId={requestedRankingRunId}
        runContextPinned={runContextPinned}
        surfacedWithTopics={surfacedWithTopics}
        topScore={topScore}
        usingUnpinnedLatestRun={usingUnpinnedLatestRun}
      />

      {error ? <RecommendedErrorPanel error={error} status={status} /> : null}

      {data && !error ? (
        <RecommendedResults
          bridgeEligibleOnly={bridgeEligibleOnly}
          data={data}
          family={family}
          focusPaperId={focusPaperId}
          limit={limit}
        />
      ) : null}

      <RecommendationContextCards />
    </main>
  );
}
