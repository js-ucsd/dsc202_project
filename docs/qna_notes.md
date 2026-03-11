## Q&A notes (quick answers)

### Why do you need three stores?
- **Qdrant**: required for semantic similarity (“most similar to a research question”) via vector kNN.
- **Neo4j**: required for multi-hop relationship reasoning (indirect citations) and network analytics (clusters/bridges).
- **Postgres**: required for structured, reliable analytics (citation counts, time/venue filtering, rollups) and as a stable source-of-truth for entities.

### Why is “venue” used as a field?
The raw CSV does not provide a canonical field taxonomy. Venue is the best available proxy for an MVP; the design leaves room to add field labels later.

### How do you ensure IDs match across stores?
The paper UUID `id` from the CSV is used as the canonical identifier in Postgres (`papers.id`), Neo4j (`Paper.id`), and Qdrant (point id + `paper_id` payload).

### How do you scale ingestion?
- Develop with `--limit` (e.g. 50k).\n
- Increase limit progressively; optimize: Postgres COPY, Neo4j UNWIND batch writes, and embedding batch sizing.\n
- Optionally parallelize embedding generation and upserts.

