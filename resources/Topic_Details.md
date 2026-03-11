# Topic: Scientific Paper Knowledge Graph & Semantic Search

Use Case: Explore research paper relationships, authorship networks, and semantic similarity of abstracts.

## Data Sources:

We read the data from the CSV dataset file.

- Postgres: Structured papers, authors, categories
- Neo4j: Author-paper graph for co-authorship and citation networks
- Qdrant: Paper abstracts embedded for semantic search

## Sample Competency Questions:

Need of individual data stores must be satisfied in the competency questions:

- Which papers are most semantically similar to a given research question?
- Which authors collaborate most frequently?
- Can we suggest relevant papers that cite a given paper indirectly?
- Which author clusters dominate a research field?
- Which papers are emerging trends based on semantic similarity?
- Which authors act as bridges between research domains?
- What is the relationship between paper citations and topic similarity?
- Which papers in one field could be relevant to another based on content similarity?
- Are there authors whose work is central in the network but under-cited?
- Which topics are most connected via co-authorship networks?
