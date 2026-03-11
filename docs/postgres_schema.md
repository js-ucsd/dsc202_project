## Postgres schema (relational element)

### Tables
- `papers`: one row per paper (`id`, `title`, `abstract`, `venue`, `year`, `n_citation`)
- `authors`: unique authors by name (`author_id`, `name`)
- `paper_authors`: many-to-many join for authorship
- `citations`: directed citation edges (`citing_paper_id` -> `cited_paper_id`)

### Indexes (why they matter)
- `papers(year)`, `papers(venue)`: fast “field/time” filtering for analytics and demos
- `papers(n_citation desc)`: quick “most cited” reporting
- trigram index on `authors(name)`: author lookup / autocomplete friendly
- `citations(cited_paper_id)`: inbound citation fan-in queries

