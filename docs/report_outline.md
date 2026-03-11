## Report outline (fill in)

### 1. Problem & motivation
- What problem the tool solves: exploring paper relationships + semantic discovery.

### 2. Data source
- CSV schema (columns, list parsing for `authors`/`references`).
- Data quality notes (missing abstracts, ambiguous author names).

### 3. Architecture overview
- Ingestion pipeline: CSV -> Postgres + Neo4j + Qdrant.
- Query router: which store is used for which competency question.

### 4. Store justifications (tied to competency questions)
- **Postgres**: structured filters + analytics; joins; citation counts/time/venue aggregates.
- **Neo4j**: multi-hop citation paths; co-author networks; communities/bridges via graph analytics.
- **Qdrant**: semantic similarity search and cross-field relevance via embeddings.

### 5. Schemas + design decisions
- Postgres ERD
- Neo4j node/edge model + constraints
- Qdrant collection + embedding model choices

### 6. Competency questions demo results
- For each question: API endpoint, example input, example output, and why the store is necessary.

### 7. Limitations and future work
- Better field taxonomy (beyond `venue`), author disambiguation, improved topic modeling, scaling strategy.

### 8. Reproducibility
- Docker compose, environment variables, ingestion commands, and demo instructions.

