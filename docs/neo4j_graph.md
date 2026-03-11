## Neo4j graph model (graph element)

### Node labels
- `:Paper` with properties `{id, title, year, venue}`
- `:Author` with property `{name}`

### Relationship types
- `(:Author)-[:AUTHORED]->(:Paper)`
- `(:Paper)-[:CITES]->(:Paper)`

### Constraints / indexes
- Unique constraints on `Paper.id` and `Author.name`
- Indexes on `Paper.year` and `Paper.venue`

### Why Neo4j is required (mapped to competency questions)
- Indirect citation recommendations require multi-hop traversal over `:CITES` paths.
- Collaboration frequency and author networks are naturally expressed via shared `:AUTHORED` neighborhoods.
- “Clusters” and “bridges” are graph analytics problems (community detection / betweenness), handled via Neo4j GDS.

