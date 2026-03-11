## 15-minute demo script (suggested)

### Setup (30–60s)
- Show `infra/docker-compose.yml` and `python -m pipeline.cli ingest ...`.
- Mention subset ingestion for dev and scaling with `--limit`.

### Flow A: Semantic search (Qdrant) (3–4 min)
- Question: “Which papers are most semantically similar to a given research question?”
- Use UI search box, show top-k results and explain vectors + payload filters.
- Justification: only vector DB answers semantic similarity efficiently.

### Flow B: Collaboration network (Neo4j) (3–4 min)
- Question: “Which authors collaborate most frequently?”
- Show top collaborator pairs endpoint and explain co-authorship graph pattern.
- Justification: relationship queries are natural in a graph store.

### Flow C: Indirect citations (Neo4j) (3–4 min)
- Question: “Suggest papers that cite a given paper indirectly.”
- Input a paper UUID from earlier results; show multi-hop paths.
- Justification: path queries are graph-native.

### Flow D: Cross-store analysis (Qdrant + Postgres) (3–4 min)
- Question: “What is the relationship between citations and topic similarity?”
- Show similar papers (Qdrant) enriched with citation counts (Postgres).
- Justification: similarity is vector search; citation totals are structured aggregates.

### Wrap (1–2 min)
- Recap store roles and show schema diagrams (`docs/schemas.md`).
- Mention future improvements (field taxonomy, author disambiguation, scaling).

