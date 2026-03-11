## Scientific Paper Knowledge Graph & Semantic Search (Report Draft)

### Abstract
This project demonstrates how combining a relational database (Postgres), a graph database (Neo4j), and a vector database (Qdrant) enables a research-paper exploration tool that answers competency questions involving structured analytics, multi-hop relationship reasoning, and semantic similarity search.

### 1. Data
- Source: `data/raw/dblp-v10.csv`
- Key fields: `id`, `title`, `abstract`, `venue`, `year`, `n_citation`, `authors` (list), `references` (list)

### 2. System architecture
- Ingestion: `python -m pipeline.cli ingest ...`
- Serving: FastAPI (`apps/api/main.py`) + Streamlit UI (`apps/web/app.py`)

### 3. Database designs and justifications
#### Postgres (relational)
- Schema: see `docs/postgres_schema.md`
- Needed for structured filters/aggregations: year, venue, citation totals, author-level rollups.

#### Neo4j (graph)
- Schema: see `docs/neo4j_graph.md`
- Needed for indirect citations, collaboration patterns, and graph analytics (clusters/bridges).

#### Qdrant (vector)
- Schema: see `docs/qdrant_collection.md`
- Needed for semantic similarity and cross-field relevance by content.

### 4. Competency questions mapping (store justification)
Each API endpoint includes a `store_justification` string explaining why that store is required.

- Semantic similarity: `GET /semantic_search` (Qdrant)\n
- Collaboration frequency: `GET /top_collaborators` (Neo4j)\n
- Indirect citations: `GET /indirect_citers` (Neo4j)\n
- Author clusters by field proxy (venue): `GET /author_clusters_by_venue` (Neo4j GDS)\n
- Emerging trends: `GET /emerging_trends` (Qdrant)\n
- Bridge authors: `GET /bridge_authors` (Neo4j GDS)\n
- Citations vs similarity: `GET /citations_vs_similarity` (Qdrant + Postgres)\n
- Cross-field relevance: `GET /cross_field_relevance` (Qdrant + metadata filters)\n
- Central but under-cited: `GET /central_but_undercited` (Neo4j + Postgres)\n
- Topics connected via co-authorship: `GET /topics_connected_via_coauthorship` (Qdrant + Neo4j)\n

### 5. Reproducibility
1) Start stores: `docker compose -f infra/docker-compose.yml up -d`\n
2) Install: `pip install -r requirements.txt && pip install -e packages/pipeline`\n
3) Ingest: `python -m pipeline.cli ingest --csv data/raw/dblp-v10.csv --limit 50000`\n
4) Run API: `uvicorn apps.api.main:app --reload --port 8000`\n
5) Run UI: `streamlit run apps/web/app.py`\n

### 6. Limitations and future work
- Replace venue-as-field with a richer taxonomy or topic labeling pipeline.
- Add author disambiguation beyond name strings.
- Improve ingestion performance via bulk COPY + batched Neo4j writes.

