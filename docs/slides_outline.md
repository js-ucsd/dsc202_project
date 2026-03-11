## Slides outline (15 minutes)

### 1. Title + team (30s)
- Scientific Paper Knowledge Graph & Semantic Search

### 2. Problem + competency questions (1 min)
- List the competency questions from `resources/Topic_Details.md`.

### 3. Data source (1 min)
- DBLP CSV fields; parsing of `authors`/`references`.

### 4. Why three stores? (2 min)
- Postgres / Neo4j / Qdrant: one slide each with **why necessary** + which questions it answers.

### 5. Architecture (2 min)
- Ingestion pipeline diagram (CSV -> 3 stores -> API -> UI).

### 6. Schemas (2 min)
- Postgres ERD
- Neo4j graph diagram
- Qdrant collection/payload diagram

### 7. Demo (5–6 min)
- Semantic search (Qdrant)
- Collaborations + indirect citations (Neo4j)
- Citations vs similarity (Qdrant + Postgres)

### 8. Limitations + next steps (1 min)
- Better field taxonomy, author disambiguation, scaling, topic labels.

### 9. Q&A backup slides
- Store justifications per question
- Performance/scaling notes

