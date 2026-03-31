CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS source_policies (
    source_slug TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    openalex_source_id TEXT UNIQUE,
    venue_class TEXT NOT NULL CHECK (venue_class IN ('core', 'edge', 'excluded')),
    rationale TEXT NOT NULL,
    aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_snapshot_versions (
    source_snapshot_version TEXT PRIMARY KEY,
    policy_name TEXT NOT NULL,
    policy_hash TEXT NOT NULL,
    ingest_mode TEXT NOT NULL CHECK (ingest_mode IN ('api-bootstrap', 'api-incremental', 'snapshot-import')),
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    ingest_run_id TEXT PRIMARY KEY,
    source_snapshot_version TEXT NOT NULL REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE CASCADE,
    policy_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    config_json JSONB NOT NULL,
    counts_json JSONB,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ingest_watermarks (
    watermark_key TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    source_slug TEXT REFERENCES source_policies(source_slug),
    cursor TEXT,
    updated_date DATE,
    source_snapshot_version TEXT NOT NULL REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE CASCADE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS venues (
    id BIGSERIAL PRIMARY KEY,
    openalex_id TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    source_slug TEXT REFERENCES source_policies(source_slug),
    venue_class TEXT NOT NULL CHECK (venue_class IN ('core', 'edge', 'excluded')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS works (
    id BIGSERIAL PRIMARY KEY,
    openalex_id TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    abstract TEXT,
    year INTEGER NOT NULL,
    doi TEXT,
    type TEXT NOT NULL,
    language TEXT NOT NULL,
    publication_date DATE,
    updated_date DATE,
    venue_id BIGINT REFERENCES venues(id),
    source_slug TEXT REFERENCES source_policies(source_slug),
    citation_count INTEGER NOT NULL DEFAULT 0,
    is_core_corpus BOOLEAN NOT NULL DEFAULT FALSE,
    inclusion_status TEXT NOT NULL CHECK (inclusion_status IN ('included', 'excluded')),
    exclusion_reason TEXT,
    raw_content_hash TEXT,
    corpus_snapshot_version TEXT NOT NULL REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE RESTRICT,
    last_ingest_run_id TEXT REFERENCES ingest_runs(ingest_run_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_openalex_works (
    openalex_id TEXT NOT NULL,
    ingest_run_id TEXT NOT NULL REFERENCES ingest_runs(ingest_run_id) ON DELETE CASCADE,
    source_snapshot_version TEXT NOT NULL REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE CASCADE,
    source_slug TEXT REFERENCES source_policies(source_slug),
    page_cursor TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_date DATE,
    payload JSONB NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY (openalex_id, ingest_run_id)
);

CREATE TABLE IF NOT EXISTS authors (
    id BIGSERIAL PRIMARY KEY,
    openalex_id TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work_authors (
    work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    author_id BIGINT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    author_position INTEGER NOT NULL,
    PRIMARY KEY (work_id, author_id)
);

CREATE TABLE IF NOT EXISTS topics (
    id BIGSERIAL PRIMARY KEY,
    openalex_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    level INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work_topics (
    work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    topic_id BIGINT NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    score DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (work_id, topic_id)
);

CREATE TABLE IF NOT EXISTS citations (
    citing_work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    cited_work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    PRIMARY KEY (citing_work_id, cited_work_id)
);

CREATE TABLE IF NOT EXISTS embeddings (
    work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    embedding_version TEXT NOT NULL,
    vector VECTOR(1536) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (work_id, embedding_version)
);

CREATE TABLE IF NOT EXISTS clustering_runs (
    cluster_version TEXT PRIMARY KEY,
    embedding_version TEXT NOT NULL,
    corpus_snapshot_version TEXT NOT NULL REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    algorithm TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    config_json JSONB NOT NULL,
    counts_json JSONB,
    error_message TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clusters (
    work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    cluster_version TEXT NOT NULL REFERENCES clustering_runs(cluster_version) ON DELETE CASCADE,
    PRIMARY KEY (work_id, cluster_version)
);

CREATE TABLE IF NOT EXISTS ranking_runs (
    ranking_run_id TEXT PRIMARY KEY,
    ranking_version TEXT NOT NULL,
    corpus_snapshot_version TEXT NOT NULL REFERENCES source_snapshot_versions(source_snapshot_version) ON DELETE RESTRICT,
    embedding_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    config_json JSONB NOT NULL,
    counts_json JSONB,
    error_message TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_scores (
    ranking_run_id TEXT NOT NULL REFERENCES ranking_runs(ranking_run_id) ON DELETE CASCADE,
    work_id BIGINT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
    recommendation_family TEXT NOT NULL CHECK (recommendation_family IN ('emerging', 'bridge', 'undercited')),
    semantic_score DOUBLE PRECISION,
    citation_velocity_score DOUBLE PRECISION,
    topic_growth_score DOUBLE PRECISION,
    bridge_score DOUBLE PRECISION,
    diversity_penalty DOUBLE PRECISION,
    final_score DOUBLE PRECISION NOT NULL,
    reason_short TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ranking_run_id, work_id, recommendation_family)
);

CREATE INDEX IF NOT EXISTS idx_source_snapshot_policy ON source_snapshot_versions(policy_hash, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingest_runs_snapshot ON ingest_runs(source_snapshot_version, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_watermarks_entity ON ingest_watermarks(entity_type, source_slug);
CREATE INDEX IF NOT EXISTS idx_works_snapshot ON works(corpus_snapshot_version, year);
CREATE INDEX IF NOT EXISTS idx_works_status ON works(inclusion_status, exclusion_reason);
CREATE INDEX IF NOT EXISTS idx_works_core ON works(is_core_corpus, year);
CREATE INDEX IF NOT EXISTS idx_raw_works_snapshot ON raw_openalex_works(source_snapshot_version, source_slug, updated_date);
CREATE INDEX IF NOT EXISTS idx_ranking_runs_version_started ON ranking_runs(ranking_version, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ranking_runs_snapshot_started ON ranking_runs(corpus_snapshot_version, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ranking_runs_status_started ON ranking_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_ranking_runs_snapshot_status_finished ON ranking_runs(corpus_snapshot_version, status, finished_at DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_paper_scores_run_family_score ON paper_scores(ranking_run_id, recommendation_family, final_score DESC);
CREATE INDEX IF NOT EXISTS idx_paper_scores_work_run ON paper_scores(work_id, ranking_run_id);
CREATE INDEX IF NOT EXISTS idx_work_topics_topic ON work_topics(topic_id, score DESC);
CREATE INDEX IF NOT EXISTS idx_embeddings_vector ON embeddings USING hnsw (vector vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_clustering_runs_snapshot_started ON clustering_runs(corpus_snapshot_version, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_clustering_runs_embedding_started ON clustering_runs(embedding_version, started_at DESC);
