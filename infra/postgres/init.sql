CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS papers (
  id UUID PRIMARY KEY,
  title TEXT NOT NULL,
  abstract TEXT,
  venue TEXT,
  year INT,
  n_citation INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS authors (
  author_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS paper_authors (
  paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
  author_id BIGINT NOT NULL REFERENCES authors(author_id) ON DELETE CASCADE,
  PRIMARY KEY (paper_id, author_id)
);

-- Citation edges. We keep cited_paper_id without FK to allow references to papers
-- not present in the ingested subset.
CREATE TABLE IF NOT EXISTS citations (
  citing_paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
  cited_paper_id UUID NOT NULL,
  PRIMARY KEY (citing_paper_id, cited_paper_id)
);

-- Indexes for common filters/joins/analytics
CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);
CREATE INDEX IF NOT EXISTS idx_papers_venue ON papers(venue);
CREATE INDEX IF NOT EXISTS idx_papers_ncitation ON papers(n_citation DESC);
CREATE INDEX IF NOT EXISTS idx_authors_name_trgm ON authors USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_paper_authors_author ON paper_authors(author_id);
CREATE INDEX IF NOT EXISTS idx_citations_cited ON citations(cited_paper_id);

