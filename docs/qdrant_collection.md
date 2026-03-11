## Qdrant collection design

- **Collection name**: `papers_vectors` (configurable via `QDRANT_COLLECTION`)
- **Text embedded**: `title + "\n" + abstract` (fallback to title-only if abstract missing)
- **Embedding provider**: `fastembed` (default; no API keys)
- **Model**: `BAAI/bge-small-en-v1.5`
- **Distance**: cosine
- **Payload fields**:
  - `paper_id` (UUID string; same as Postgres/Neo4j `Paper.id`)
  - `title` (string)
  - `year` (int)
  - `venue` (string)

