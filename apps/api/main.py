from __future__ import annotations

from typing import Any, List, Optional

import psycopg
from fastapi import FastAPI, Query
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from pipeline.settings import Settings


settings = Settings()
app = FastAPI(title="Scientific Paper KG API")


def pg_conn():
    return psycopg.connect(settings.postgres_dsn())


def neo4j_driver():
    return GraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
    )


def qdrant_client():
    return QdrantClient(url=settings.qdrant_url)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
def stats(
    paper_ids: Optional[List[str]] = Query(None, description="When set, Postgres stats are restricted to these papers (topic scope)"),
) -> dict[str, Any]:
    """Dashboard statistics from all three stores. Optionally restrict metrics to a scoped set of paper_ids (topic scope)."""
    with pg_conn() as conn, conn.cursor() as cur:
        if paper_ids:
            cur.execute(
                "SELECT count(*) FROM papers WHERE id = ANY(%s::uuid[])",
                (paper_ids,),
            )
            total_papers = cur.fetchone()[0]
            cur.execute(
                """
                SELECT count(DISTINCT a.author_id) FROM authors a
                JOIN paper_authors pa ON pa.author_id = a.author_id
                WHERE pa.paper_id = ANY(%s::uuid[])
                """,
                (paper_ids,),
            )
            total_authors = cur.fetchone()[0]
            cur.execute(
                "SELECT count(DISTINCT venue) FROM papers WHERE venue IS NOT NULL AND venue != '' AND id = ANY(%s::uuid[])",
                (paper_ids,),
            )
            total_venues = cur.fetchone()[0]
            cur.execute(
                "SELECT COALESCE(SUM(n_citation),0) FROM papers WHERE id = ANY(%s::uuid[])",
                (paper_ids,),
            )
            total_citations = cur.fetchone()[0]
            cur.execute(
                "SELECT venue, count(*) AS cnt FROM papers WHERE venue IS NOT NULL AND venue != '' AND id = ANY(%s::uuid[]) "
                "GROUP BY venue ORDER BY cnt DESC LIMIT 10",
                (paper_ids,),
            )
            top_venues = [{"venue": r[0], "count": r[1]} for r in cur.fetchall()]
            cur.execute(
                "SELECT year, count(*) AS cnt FROM papers WHERE year IS NOT NULL AND id = ANY(%s::uuid[]) "
                "GROUP BY year ORDER BY year",
                (paper_ids,),
            )
            papers_by_year = [{"year": r[0], "count": r[1]} for r in cur.fetchall()]
        else:
            cur.execute("SELECT count(*) FROM papers")
            total_papers = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM authors")
            total_authors = cur.fetchone()[0]
            cur.execute(
                "SELECT count(DISTINCT venue) FROM papers WHERE venue IS NOT NULL AND venue != ''"
            )
            total_venues = cur.fetchone()[0]
            cur.execute("SELECT COALESCE(SUM(n_citation),0) FROM papers")
            total_citations = cur.fetchone()[0]
            cur.execute(
                "SELECT venue, count(*) AS cnt FROM papers WHERE venue IS NOT NULL AND venue != '' "
                "GROUP BY venue ORDER BY cnt DESC LIMIT 10"
            )
            top_venues = [{"venue": r[0], "count": r[1]} for r in cur.fetchall()]
            cur.execute(
                "SELECT year, count(*) AS cnt FROM papers WHERE year IS NOT NULL "
                "GROUP BY year ORDER BY year"
            )
            papers_by_year = [{"year": r[0], "count": r[1]} for r in cur.fetchall()]

    # Neo4j counts
    driver = neo4j_driver()
    with driver.session() as s:
        if paper_ids:
            scoped_ids = paper_ids[:500]
            # Nodes: scoped papers + their authors
            nodes_row = s.run(
                """
                MATCH (p:Paper)
                WHERE p.paperId IN $paper_ids
                OPTIONAL MATCH (a:Author)-[:WROTE]->(p)
                RETURN count(DISTINCT p) AS paper_nodes,
                       count(DISTINCT a) AS author_nodes
                """,
                paper_ids=scoped_ids,
            ).single()
            paper_nodes = nodes_row["paper_nodes"] if nodes_row else 0
            author_nodes = nodes_row["author_nodes"] if nodes_row else 0

            # Relationships: WROTE edges to scoped papers + CITES edges between scoped papers
            rels_row = s.run(
                """
                MATCH (a:Author)-[w:WROTE]->(p:Paper)
                WHERE p.paperId IN $paper_ids
                WITH collect(DISTINCT w) AS wrote_rels
                MATCH (p1:Paper)-[c:CITES]->(p2:Paper)
                WHERE p1.paperId IN $paper_ids AND p2.paperId IN $paper_ids
                RETURN size(wrote_rels) AS wrote_count, count(DISTINCT c) AS cites_count
                """,
                paper_ids=scoped_ids,
            ).single()
            wrote_count = rels_row["wrote_count"] if rels_row else 0
            cites_count = rels_row["cites_count"] if rels_row else 0

            neo4j_nodes = paper_nodes + author_nodes
            neo4j_rels = wrote_count + cites_count
        else:
            neo4j_nodes = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            neo4j_rels = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    driver.close()

    # Qdrant count
    qc = qdrant_client()
    if paper_ids:
        scoped_ids = paper_ids[:500]
        # Build a Qdrant filter that matches any of the scoped paper IDs
        conditions = [
            qm.FieldCondition(
                key="paper_id",
                match=qm.MatchValue(value=pid),
            )
            for pid in scoped_ids
        ]
        count_res = qc.count(
            collection_name=settings.qdrant_collection,
            count_filter=qm.Filter(should=conditions),
            exact=True,
        )
        qdrant_vectors = count_res.count
    else:
        col = qc.get_collection(settings.qdrant_collection)
        qdrant_vectors = col.points_count

    return {
        "postgres": {
            "papers": total_papers,
            "authors": total_authors,
            "venues": total_venues,
            "total_citations": total_citations,
            "top_venues": top_venues,
            "papers_by_year": papers_by_year,
        },
        "neo4j": {"nodes": neo4j_nodes, "relationships": neo4j_rels},
        "qdrant": {"vectors": qdrant_vectors},
    }


# --- Filter & explore: Postgres-only preset queries (competency I–VII) ---

_J = "store_justification"


@app.get("/filter/papers_query")
def filter_papers_query(
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    venue: Optional[str] = None,
    author: Optional[str] = None,
    min_citations: Optional[int] = None,
    max_citations: Optional[int] = None,
    sort_by: str = "n_citation_desc",
    limit: int = 100,
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (e.g. from topic scope)"),
) -> dict[str, Any]:
    """
    Combined paper query: optional year range, venue, author, citation range; sort and limit.
    When paper_ids is provided, results are restricted to that set (topic scope).
    Returns results and the SQL used (for Show SQL in UI).
    """
    allowed_sorts = {
        "n_citation_desc": "p.n_citation DESC NULLS LAST",
        "year_desc": "p.year DESC NULLS LAST",
        "year_asc": "p.year ASC NULLS LAST",
        "title_asc": "p.title ASC",
    }
    order_clause = allowed_sorts.get(sort_by, "p.n_citation DESC NULLS LAST")

    conditions = []
    params: list[Any] = []

    if paper_ids:
        conditions.append("p.id = ANY(%s::uuid[])")
        params.append(paper_ids)
    if year_min is not None:
        conditions.append("p.year >= %s")
        params.append(year_min)
    if year_max is not None:
        conditions.append("p.year <= %s")
        params.append(year_max)
    if venue:
        conditions.append("p.venue ILIKE %s")
        params.append(f"%{venue}%")
    if min_citations is not None:
        conditions.append("p.n_citation >= %s")
        params.append(min_citations)
    if max_citations is not None:
        conditions.append("p.n_citation <= %s")
        params.append(max_citations)

    join_author = False
    if author:
        join_author = True
        conditions.append("a.name ILIKE %s")
        params.append(f"%{author}%")

    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    from_clause = (
        "FROM papers p "
        "JOIN paper_authors pa ON pa.paper_id = p.id "
        "JOIN authors a ON a.author_id = pa.author_id "
        if join_author
        else "FROM papers p "
    )
    sql = (
        "SELECT p.id::text, p.title, p.year, p.venue, p.n_citation "
        + from_clause
        + " WHERE "
        + where_clause
        + " ORDER BY "
        + order_clause
        + " LIMIT %s"
    )
    params.append(limit)

    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {
        "results": results,
        _J: "Combined WHERE + ORDER BY in Postgres; no graph or vector.",
        "sql": sql,
    }


@app.get("/filter/papers_year_range")
def filter_papers_year_range(
    year_min: int, year_max: int, limit: int = 100
) -> dict[str, Any]:
    """Papers published between year_min and year_max (I)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers WHERE year BETWEEN %s AND %s
            ORDER BY n_citation DESC NULLS LAST
            LIMIT %s
            """,
            (year_min, year_max, limit),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {
        "year_min": year_min,
        "year_max": year_max,
        "results": results,
        _J: "Filtering by year range is a standard WHERE clause; Postgres indexes support this.",
    }


@app.get("/filter/papers_by_venue")
def filter_papers_by_venue(venue: str, limit: int = 100) -> dict[str, Any]:
    """Papers published in a venue (I). Partial match on venue name."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers WHERE venue ILIKE %s
            ORDER BY n_citation DESC NULLS LAST
            LIMIT %s
            """,
            (f"%{venue}%", limit),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"venue": venue, "results": results, _J: "Filter by venue uses indexed column; ILIKE for partial match."}


@app.get("/filter/papers_by_author")
def filter_papers_by_author(author_name: str, limit: int = 100) -> dict[str, Any]:
    """All papers by a specific author (I). Uses paper_authors + authors join."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id::text, p.title, p.year, p.venue, p.n_citation
            FROM papers p
            JOIN paper_authors pa ON pa.paper_id = p.id
            JOIN authors a ON a.author_id = pa.author_id
            WHERE a.name ILIKE %s
            ORDER BY p.n_citation DESC NULLS LAST
            LIMIT %s
            """,
            (f"%{author_name}%", limit),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"author": author_name, "results": results, _J: "JOIN papers ↔ paper_authors ↔ authors; relational join."}


@app.get("/filter/papers_min_citations")
def filter_papers_min_citations(min_citations: int, limit: int = 100) -> dict[str, Any]:
    """Papers with more than N citations (I)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers WHERE n_citation > %s
            ORDER BY n_citation DESC
            LIMIT %s
            """,
            (min_citations, limit),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"min_citations": min_citations, "results": results, _J: "Simple WHERE filter; index on n_citation."}


@app.get("/filter/papers_zero_citations")
def filter_papers_zero_citations(limit: int = 100) -> dict[str, Any]:
    """Papers with zero (or null) citations (I)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers WHERE COALESCE(n_citation, 0) = 0
            ORDER BY year DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"results": results, _J: "WHERE COALESCE(n_citation,0)=0; relational filter."}


@app.get("/filter/papers_single_year")
def filter_papers_single_year(year: int, limit: int = 100) -> dict[str, Any]:
    """Papers published in a single year (I)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers WHERE year = %s
            ORDER BY n_citation DESC NULLS LAST
            LIMIT %s
            """,
            (year, limit),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"year": year, "results": results, _J: "WHERE year = %s; single-table filter."}


# --- II. Aggregation & counting ---


@app.get("/filter/papers_per_year")
def filter_papers_per_year(
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (topic scope)"),
) -> dict[str, Any]:
    """How many papers per year (II). Optionally restricted to paper_ids."""
    if paper_ids:
        sql = "SELECT year, count(*) AS cnt FROM papers WHERE year IS NOT NULL AND id = ANY(%s::uuid[]) GROUP BY year ORDER BY year"
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (paper_ids,))
            rows = cur.fetchall()
    else:
        sql = "SELECT year, count(*) AS cnt FROM papers WHERE year IS NOT NULL GROUP BY year ORDER BY year"
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    results = [{"year": r[0], "count": r[1]} for r in rows]
    return {"results": results, _J: "GROUP BY year, COUNT(*); SQL aggregation.", "sql": sql}


@app.get("/filter/avg_citations_per_year")
def filter_avg_citations_per_year(
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (topic scope)"),
) -> dict[str, Any]:
    """Average citation count per year (II). Optionally restricted to paper_ids."""
    if paper_ids:
        sql = (
            "SELECT year, round(AVG(n_citation)::numeric, 2) AS avg_citations "
            "FROM papers WHERE year IS NOT NULL AND id = ANY(%s::uuid[]) GROUP BY year ORDER BY year"
        )
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (paper_ids,))
            rows = cur.fetchall()
    else:
        sql = (
            "SELECT year, round(AVG(n_citation)::numeric, 2) AS avg_citations "
            "FROM papers WHERE year IS NOT NULL GROUP BY year ORDER BY year"
        )
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    results = [{"year": r[0], "avg_citations": float(r[1])} for r in rows]
    return {"results": results, _J: "GROUP BY year, AVG(n_citation).", "sql": sql}


@app.get("/filter/distinct_venues")
def filter_distinct_venues(
    paper_ids: Optional[List[str]] = Query(
        None, description="Restrict to venues appearing in these paper IDs (topic scope)"
    ),
) -> dict[str, Any]:
    """
    List distinct non-empty venues.

    When paper_ids is provided, restricts venues to those that appear in the scoped paper set.
    Intended for populating venue dropdowns in the UI.
    """
    if paper_ids:
        sql = """
        SELECT DISTINCT venue
        FROM papers
        WHERE venue IS NOT NULL AND trim(venue) != '' AND id = ANY(%s::uuid[])
        ORDER BY venue
        """
        params: tuple[Any, ...] = (paper_ids,)
    else:
        sql = """
        SELECT DISTINCT venue
        FROM papers
        WHERE venue IS NOT NULL AND trim(venue) != ''
        ORDER BY venue
        """
        params = tuple()

    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql.strip(), params)
        rows = cur.fetchall()
    results = [{"venue": r[0]} for r in rows]
    return {"results": results, _J: "DISTINCT venues from papers; optionally restricted to topic scope."}


@app.get("/filter/venues_by_paper_count")
def filter_venues_by_paper_count(
    limit: int = 30,
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (topic scope)"),
) -> dict[str, Any]:
    """Venues that publish the most papers (II). Optionally restricted to paper_ids."""
    if paper_ids:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT venue, count(*) AS paper_count
                FROM papers WHERE venue IS NOT NULL AND venue != '' AND id = ANY(%s::uuid[])
                GROUP BY venue ORDER BY paper_count DESC
                LIMIT %s
                """,
                (paper_ids, limit),
            )
            rows = cur.fetchall()
    else:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT venue, count(*) AS paper_count
                FROM papers WHERE venue IS NOT NULL AND venue != ''
                GROUP BY venue ORDER BY paper_count DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    results = [{"venue": r[0], "paper_count": r[1]} for r in rows]
    return {"results": results, _J: "GROUP BY venue, COUNT(*)."}


@app.get("/filter/avg_citations_per_venue")
def filter_avg_citations_per_venue(
    limit: int = 30,
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (topic scope)"),
) -> dict[str, Any]:
    """Average citation count per venue (II). Optionally restricted to paper_ids."""
    if paper_ids:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT venue, round(AVG(n_citation)::numeric, 2) AS avg_citations
                FROM papers WHERE venue IS NOT NULL AND venue != '' AND id = ANY(%s::uuid[])
                GROUP BY venue ORDER BY avg_citations DESC NULLS LAST
                LIMIT %s
                """,
                (paper_ids, limit),
            )
            rows = cur.fetchall()
    else:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT venue, round(AVG(n_citation)::numeric, 2) AS avg_citations
                FROM papers WHERE venue IS NOT NULL AND venue != ''
                GROUP BY venue ORDER BY avg_citations DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    results = [{"venue": r[0], "avg_citations": float(r[1])} for r in rows]
    return {"results": results, _J: "GROUP BY venue, AVG(n_citation)."}


@app.get("/filter/authors_by_paper_count")
def filter_authors_by_paper_count(
    limit: int = 30,
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (topic scope)"),
) -> dict[str, Any]:
    """Authors with the most papers (II). Optionally restricted to paper_ids."""
    if paper_ids:
        sql = """
        SELECT a.name, count(pa.paper_id) AS paper_count
        FROM authors a JOIN paper_authors pa ON pa.author_id = a.author_id
        WHERE pa.paper_id = ANY(%s::uuid[])
        GROUP BY a.name ORDER BY paper_count DESC
        LIMIT %s
        """
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql.strip(), (paper_ids, limit))
            rows = cur.fetchall()
        sql_display = sql.strip().replace("%s::uuid[]", "<paper_ids>").replace("LIMIT %s", f"LIMIT {limit}")
    else:
        sql = """
        SELECT a.name, count(pa.paper_id) AS paper_count
        FROM authors a JOIN paper_authors pa ON pa.author_id = a.author_id
        GROUP BY a.name ORDER BY paper_count DESC
        LIMIT %s
        """
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql.strip(), (limit,))
            rows = cur.fetchall()
        sql_display = sql.strip().replace("%s", str(limit))
    results = [{"author": r[0], "paper_count": r[1]} for r in rows]
    return {"results": results, "sql": sql_display, _J: "JOIN + GROUP BY author, COUNT; relational."}


@app.get("/filter/median_citations")
def filter_median_citations() -> dict[str, Any]:
    """Median citation count across all papers (II)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n_citation) FROM papers"
        )
        row = cur.fetchone()
    median = float(row[0]) if row and row[0] is not None else None
    return {"median_citations": median, _J: "percentile_cont(0.5); SQL aggregate."}


@app.get("/filter/distinct_authors_count")
def filter_distinct_authors_count() -> dict[str, Any]:
    """How many distinct authors (II)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM authors")
        row = cur.fetchone()
    return {"distinct_authors": row[0], _J: "COUNT(*) on authors table."}


@app.get("/filter/pct_papers_with_citations")
def filter_pct_papers_with_citations() -> dict[str, Any]:
    """Percentage of papers with at least one citation (II)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE n_citation > 0) * 100.0 / nullif(count(*), 0)
            FROM papers
            """
        )
        row = cur.fetchone()
    pct = round(float(row[0]), 2) if row and row[0] is not None else 0
    return {"pct_papers_with_citations": pct, _J: "FILTER + COUNT; conditional aggregate."}


# --- III. Multi-table joins ---


@app.get("/filter/authors_in_more_than_n_venues")
def filter_authors_in_more_than_n_venues(n: int = 3, limit: int = 30) -> dict[str, Any]:
    """Authors who published in more than N different venues (III)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.name, count(DISTINCT p.venue) AS venue_count
            FROM authors a
            JOIN paper_authors pa ON pa.author_id = a.author_id
            JOIN papers p ON p.id = pa.paper_id
            WHERE p.venue IS NOT NULL AND p.venue != ''
            GROUP BY a.name
            HAVING count(DISTINCT p.venue) > %s
            ORDER BY venue_count DESC
            LIMIT %s
            """,
            (n, limit),
        )
        rows = cur.fetchall()
    results = [{"author": r[0], "venue_count": r[1]} for r in rows]
    return {"min_venues": n, "results": results, _J: "JOIN + GROUP BY + HAVING COUNT(DISTINCT venue)."}


@app.get("/filter/venues_by_avg_citations")
def filter_venues_by_avg_citations(limit: int = 30) -> dict[str, Any]:
    """Venues with highest average citations per paper (III)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT venue, round(AVG(n_citation)::numeric, 2) AS avg_citations
            FROM papers WHERE venue IS NOT NULL AND venue != ''
            GROUP BY venue ORDER BY avg_citations DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [{"venue": r[0], "avg_citations": float(r[1])} for r in rows]
    return {"results": results, _J: "GROUP BY venue, AVG(n_citation)."}


@app.get("/filter/authors_in_both_venues")
def filter_authors_in_both_venues(venue_a: str, venue_b: str) -> dict[str, Any]:
    """Authors who published in both Venue A and Venue B (III)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.name FROM authors a
            JOIN paper_authors pa ON pa.author_id = a.author_id
            JOIN papers p ON p.id = pa.paper_id
            WHERE p.venue ILIKE %s
            INTERSECT
            SELECT a.name FROM authors a
            JOIN paper_authors pa ON pa.author_id = a.author_id
            JOIN papers p ON p.id = pa.paper_id
            WHERE p.venue ILIKE %s
            """,
            (f"%{venue_a}%", f"%{venue_b}%"),
        )
        rows = cur.fetchall()
    results = [{"author": r[0]} for r in rows]
    return {"venue_a": venue_a, "venue_b": venue_b, "results": results, _J: "INTERSECT of two author sets; JOIN."}


@app.get("/filter/total_citations_per_author")
def filter_total_citations_per_author(
    limit: int = 30,
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to these paper IDs (topic scope)"),
) -> dict[str, Any]:
    """Total citation count for each author across their papers (III). Optionally restricted to paper_ids."""
    if paper_ids:
        sql = """
        SELECT a.name, coalesce(sum(p.n_citation), 0)::bigint AS total_citations
        FROM authors a
        JOIN paper_authors pa ON pa.author_id = a.author_id
        JOIN papers p ON p.id = pa.paper_id
        WHERE p.id = ANY(%s::uuid[])
        GROUP BY a.name ORDER BY total_citations DESC
        LIMIT %s
        """
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql.strip(), (paper_ids, limit))
            rows = cur.fetchall()
        sql_display = sql.strip().replace("%s::uuid[]", "<paper_ids>").replace("LIMIT %s", f"LIMIT {limit}")
    else:
        sql = """
        SELECT a.name, coalesce(sum(p.n_citation), 0)::bigint AS total_citations
        FROM authors a
        JOIN paper_authors pa ON pa.author_id = a.author_id
        JOIN papers p ON p.id = pa.paper_id
        GROUP BY a.name ORDER BY total_citations DESC
        LIMIT %s
        """
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(sql.strip(), (limit,))
            rows = cur.fetchall()
        sql_display = sql.strip().replace("%s", str(limit))
    results = [{"author": r[0], "total_citations": r[1]} for r in rows]
    return {"results": results, "sql": sql_display, _J: "JOIN papers-authors; GROUP BY author, SUM(n_citation)."}


@app.get("/filter/authors_not_published_since")
def filter_authors_not_published_since(since_year: int, limit: int = 50) -> dict[str, Any]:
    """Authors with no paper published in or after since_year (III)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.name, max(p.year) AS last_year
            FROM authors a
            JOIN paper_authors pa ON pa.author_id = a.author_id
            JOIN papers p ON p.id = pa.paper_id
            GROUP BY a.name
            HAVING max(p.year) < %s
            ORDER BY last_year DESC NULLS LAST
            LIMIT %s
            """,
            (since_year, limit),
        )
        rows = cur.fetchall()
    results = [{"author": r[0], "last_paper_year": r[1]} for r in rows]
    return {"since_year": since_year, "results": results, _J: "JOIN + GROUP BY + HAVING max(year) < ?."}


# --- IV. Data integrity ---


@app.get("/filter/duplicate_paper_ids")
def filter_duplicate_paper_ids() -> dict[str, Any]:
    """Check for duplicate paper IDs; expect empty (IV)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id::text, count(*) FROM papers GROUP BY id HAVING count(*) > 1")
        rows = cur.fetchall()
    results = [{"paper_id": r[0], "count": r[1]} for r in rows]
    return {"results": results, "note": "Empty if PK is respected.", _J: "GROUP BY id HAVING COUNT>1; data integrity."}


@app.get("/filter/papers_missing_venue")
def filter_papers_missing_venue(limit: int = 100) -> dict[str, Any]:
    """Papers with null or empty venue (IV)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers
            WHERE venue IS NULL OR trim(venue) = ''
            ORDER BY year DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"results": results, _J: "WHERE venue IS NULL OR trim(venue)=''."}


@app.get("/filter/paper_authors_orphaned")
def filter_paper_authors_orphaned() -> dict[str, Any]:
    """paper_authors rows whose paper_id does not exist (IV); expect empty (FK)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pa.paper_id::text, pa.author_id
            FROM paper_authors pa
            LEFT JOIN papers p ON p.id = pa.paper_id
            WHERE p.id IS NULL
            LIMIT 100
            """
        )
        rows = cur.fetchall()
    results = [{"paper_id": r[0], "author_id": r[1]} for r in rows]
    return {"results": results, "note": "Empty if FK is respected.", _J: "LEFT JOIN; find orphaned FKs."}


@app.get("/filter/papers_future_year")
def filter_papers_future_year(limit: int = 50) -> dict[str, Any]:
    """Papers with publication year in the future (IV)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers WHERE year > extract(year from current_date)
            ORDER BY year
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"results": results, _J: "WHERE year > current year; validation."}


# --- V. Time-based ---


@app.get("/filter/avg_citations_by_decade")
def filter_avg_citations_by_decade() -> dict[str, Any]:
    """Average citation count by decade (V)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT (floor(year/10)*10)::int AS decade,
                   round(avg(n_citation)::numeric, 2) AS avg_citations
            FROM papers WHERE year IS NOT NULL
            GROUP BY floor(year/10)*10 ORDER BY decade
            """
        )
        rows = cur.fetchall()
    results = [{"decade": r[0], "avg_citations": float(r[1])} for r in rows]
    return {"results": results, _J: "GROUP BY decade (floor(year/10)); time aggregation."}


@app.get("/filter/venue_growth")
def filter_venue_growth(years_recent: int = 5, limit: int = 20) -> dict[str, Any]:
    """Venues with more papers in the last N years than in the previous N years (V)."""
    import datetime
    cy = datetime.datetime.now().year
    recent_min = cy - years_recent
    older_min = cy - 2 * years_recent
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH recent AS (
                SELECT venue, count(*) AS cnt FROM papers
                WHERE venue IS NOT NULL AND venue != '' AND year >= %s AND year < %s
                GROUP BY venue
            ),
            older AS (
                SELECT venue, count(*) AS cnt FROM papers
                WHERE venue IS NOT NULL AND venue != '' AND year >= %s AND year < %s
                GROUP BY venue
            )
            SELECT r.venue, r.cnt AS recent_count, o.cnt AS older_count
            FROM recent r JOIN older o ON r.venue = o.venue
            WHERE r.cnt > o.cnt
            ORDER BY (r.cnt - o.cnt) DESC
            LIMIT %s
            """,
            (recent_min, cy, older_min, recent_min, limit),
        )
        rows = cur.fetchall()
    results = [{"venue": r[0], "recent_count": r[1], "older_count": r[2]} for r in rows]
    return {"years_recent": years_recent, "results": results, _J: "Compare two time windows; GROUP BY venue."}


@app.get("/filter/citation_distribution_by_years")
def filter_citation_distribution_by_years(year1: int, year2: int) -> dict[str, Any]:
    """Citation distribution (avg, count) for two years (V)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT year, count(*), round(avg(n_citation)::numeric, 2)
            FROM papers WHERE year IN (%s, %s)
            GROUP BY year ORDER BY year
            """,
            (year1, year2),
        )
        rows = cur.fetchall()
    results = [{"year": r[0], "paper_count": r[1], "avg_citations": float(r[2])} for r in rows]
    return {"year1": year1, "year2": year2, "results": results, _J: "GROUP BY year for two years."}


@app.get("/filter/top_cited_papers")
def filter_top_cited_papers(limit: int = 10) -> dict[str, Any]:
    """Top N most cited papers (VI)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers ORDER BY n_citation DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"results": results, _J: "ORDER BY n_citation DESC LIMIT; SQL ranking."}


@app.get("/filter/top_cited_per_venue")
def filter_top_cited_per_venue(limit_per_venue: int = 5, limit_venues: int = 20) -> dict[str, Any]:
    """Top K most cited papers per venue (VI)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT id::text, title, year, venue, n_citation,
                       row_number() OVER (PARTITION BY venue ORDER BY n_citation DESC NULLS LAST) AS rn
                FROM papers WHERE venue IS NOT NULL AND venue != ''
            ),
            top_venues AS (
                SELECT venue FROM papers
                WHERE venue IS NOT NULL AND venue != ''
                GROUP BY venue ORDER BY count(*) DESC LIMIT %s
            )
            SELECT r.id, r.title, r.year, r.venue, r.n_citation
            FROM ranked r JOIN top_venues t ON r.venue = t.venue
            WHERE r.rn <= %s ORDER BY r.venue, r.n_citation DESC NULLS LAST
            """,
            (limit_venues, limit_per_venue),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"limit_per_venue": limit_per_venue, "results": results, _J: "ROW_NUMBER() OVER (PARTITION BY venue)."}


@app.get("/filter/top_pct_authors_by_papers")
def filter_top_pct_authors_by_papers(pct: float = 10, limit: int = 50) -> dict[str, Any]:
    """Authors in the top P% by paper count (VI)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH author_counts AS (
                SELECT a.name, count(pa.paper_id) AS paper_count
                FROM authors a JOIN paper_authors pa ON pa.author_id = a.author_id
                GROUP BY a.name
            ),
            ranked AS (
                SELECT name, paper_count, percent_rank() OVER (ORDER BY paper_count DESC) AS pct_rank
                FROM author_counts
            )
            SELECT name, paper_count, round((1 - pct_rank) * 100, 1)
            FROM ranked WHERE (1 - pct_rank) * 100 < %s
            ORDER BY paper_count DESC LIMIT %s
            """,
            (pct, limit),
        )
        rows = cur.fetchall()
    results = [{"author": r[0], "paper_count": r[1], "percentile": float(r[2])} for r in rows]
    return {"top_pct": pct, "results": results, _J: "PERCENT_RANK(); top fraction by count."}


@app.get("/filter/paper_percentile_rank")
def filter_paper_percentile_rank(paper_id: str) -> dict[str, Any]:
    """Citation percentile rank of a given paper (VI)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH r AS (
                SELECT id, n_citation,
                       percent_rank() OVER (ORDER BY n_citation ASC NULLS LAST) * 100 AS pct
                FROM papers
            )
            SELECT id::text, n_citation, round(pct::numeric, 2) FROM r WHERE id = %s
            """,
            (paper_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"paper_id": paper_id, "error": "not found", "results": []}
    return {"paper_id": paper_id, "n_citation": row[1], "percentile_rank": float(row[2]), _J: "PERCENT_RANK()."}


@app.get("/filter/venues_by_citation_variance")
def filter_venues_by_citation_variance(limit: int = 20) -> dict[str, Any]:
    """Venues with highest citation variance (VI)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT venue, round((var_samp(n_citation))::numeric, 2) AS variance
            FROM papers WHERE venue IS NOT NULL AND venue != '' AND n_citation IS NOT NULL
            GROUP BY venue HAVING count(*) > 1
            ORDER BY variance DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [{"venue": r[0], "citation_variance": float(r[1]) if r[1] is not None else None} for r in rows]
    return {"results": results, _J: "GROUP BY venue, VAR_SAMP(n_citation)."}


@app.get("/filter/papers_sorted_by_citations")
def filter_papers_sorted_by_citations(limit: int = 100) -> dict[str, Any]:
    """Papers sorted by citation count (VII)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers ORDER BY n_citation DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {"results": results, _J: "ORDER BY n_citation; index-supported sort."}


@app.get("/filter/papers_year_range_min_citations")
def filter_papers_year_range_min_citations(
    year_min: int, year_max: int, min_citations: int, limit: int = 100
) -> dict[str, Any]:
    """Papers in year range with citation count above threshold (VII)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, title, year, venue, n_citation
            FROM papers
            WHERE year BETWEEN %s AND %s AND n_citation >= %s
            ORDER BY n_citation DESC
            LIMIT %s
            """,
            (year_min, year_max, min_citations, limit),
        )
        rows = cur.fetchall()
    results = [
        {"paper_id": r[0], "title": r[1], "year": r[2], "venue": r[3], "n_citation": r[4]}
        for r in rows
    ]
    return {
        "year_min": year_min,
        "year_max": year_max,
        "min_citations": min_citations,
        "results": results,
        _J: "WHERE year range AND n_citation >= ?; combined filter.",
    }


@app.get("/filter/avg_citations_by_venue_year")
def filter_avg_citations_by_venue_year(limit: int = 200) -> dict[str, Any]:
    """Average citations grouped by venue and year (VII)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT venue, year, round(avg(n_citation)::numeric, 2) AS avg_citations
            FROM papers
            WHERE venue IS NOT NULL AND venue != '' AND year IS NOT NULL
            GROUP BY venue, year
            ORDER BY venue, year
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    results = [{"venue": r[0], "year": r[1], "avg_citations": float(r[2])} for r in rows]
    return {"results": results, _J: "GROUP BY venue, year; two-dimensional aggregate."}


@app.get("/semantic_search")
def semantic_search(q: str, k: int = 10) -> dict[str, Any]:
    """
    Which papers are most semantically similar to a given research question?
    Uses Qdrant vector similarity search (cosine) over embedded title+abstract.
    """
    from fastembed import TextEmbedding

    embedder = TextEmbedding(model_name=settings.fastembed_model)
    vec = next(embedder.embed([q]))

    qc = qdrant_client()
    res = qc.query_points(
        collection_name=settings.qdrant_collection,
        query=vec.tolist(),
        limit=k,
        with_payload=True,
    )
    hits = res.points
    return {
        "query": q,
        "results": [
            {
                "paper_id": h.payload.get("paper_id") if h.payload else None,
                "score": h.score,
                "title": (h.payload or {}).get("title"),
                "year": (h.payload or {}).get("year"),
                "venue": (h.payload or {}).get("venue"),
            }
            for h in hits
        ],
        "store_justification": "Vector similarity search requires Qdrant; relational/graph stores do not natively support semantic nearest-neighbor retrieval.",
    }


@app.get("/top_collaborators")
def top_collaborators(
    limit: int = 20,
    paper_ids: Optional[List[str]] = Query(None, description="Restrict to co-authorship on these papers (topic scope)"),
) -> dict[str, Any]:
    """
    Which authors collaborate most frequently?
    Uses Neo4j graph edges via shared papers. When paper_ids is set, only papers in that set are counted (topic scope).
    """
    driver = neo4j_driver()
    if paper_ids:
        paper_ids = paper_ids[:500]
        query = """
        MATCH (a1:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(a2:Author)
        WHERE a1.authorName < a2.authorName AND p.paperId IN $paper_ids
        WITH a1, a2, count(DISTINCT p) AS joint_papers
        ORDER BY joint_papers DESC
        RETURN a1.authorName AS author1, a2.authorName AS author2, joint_papers
        LIMIT $limit
        """
        with driver.session() as s:
            rows = [r.data() for r in s.run(query, paper_ids=paper_ids, limit=limit)]
    else:
        query = """
        MATCH (a1:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(a2:Author)
        WHERE a1.authorName < a2.authorName
        WITH a1, a2, count(DISTINCT p) AS joint_papers
        ORDER BY joint_papers DESC
        RETURN a1.authorName AS author1, a2.authorName AS author2, joint_papers
        LIMIT $limit
        """
        with driver.session() as s:
            rows = [r.data() for r in s.run(query, limit=limit)]
    driver.close()
    return {
        "results": rows,
        "store_justification": "Co-authorship is inherently a graph pattern (shared neighbors). Neo4j expresses and executes this relationship query naturally."
        + (" Scoped to topic papers." if paper_ids else ""),
    }


@app.get("/indirect_citers")
def indirect_citers(
    paper_id: str, max_hops: int = 3, limit: int = 20
) -> dict[str, Any]:
    """
    Suggest relevant papers that cite a given paper indirectly.
    Uses Neo4j path queries over the citation graph.
    """
    driver = neo4j_driver()
    query = f"""
    MATCH (target:Paper {{paperId: $paper_id}})
    MATCH path = (src:Paper)-[:CITES*1..{max_hops}]->(target)
    WITH src, min(length(path)) AS hops
    RETURN src.paperId AS paper_id, src.title AS title, hops
    ORDER BY hops ASC
    LIMIT $limit
    """
    with driver.session() as s:
        rows = [r.data() for r in s.run(query, paper_id=paper_id, limit=limit)]
    driver.close()
    return {
        "paper_id": paper_id,
        "results": rows,
        "store_justification": "Indirect citation recommendations require multi-hop traversal over citation edges, which is a core strength of graph databases.",
    }


@app.get("/author_clusters_by_venue")
def author_clusters_by_venue(venue: str, top_k: int = 5) -> dict[str, Any]:
    """
    Which author clusters dominate a research field?
    Approximate 'field' by venue string; compute communities on a co-authorship graph
    restricted to papers in that venue using Neo4j Graph Data Science (Louvain).

    Returns clusters enriched with:
    - author_count: number of authors in the cluster
    - papers_in_venue: distinct papers in the venue with at least one author from the cluster
    - share_of_venue: percentage of venue papers contributed by the cluster
    - top_authors: sample of author names for preview
    - all_authors: full list of author names in the cluster
    - neo4j_query: Cypher snippet to visualize the cluster's co-author subgraph
    """
    driver = neo4j_driver()
    with driver.session() as s:
        exists = s.run(
            "CALL gds.graph.exists('venueGraph') YIELD exists RETURN exists"
        ).single()
        if exists and exists.get("exists"):
            s.run(
                "CALL gds.graph.drop('venueGraph') YIELD graphName RETURN graphName"
            ).consume()

        # Compute total papers in this venue for share-of-venue calculation
        total_papers_row = s.run(
            """
            MATCH (p:Paper)-[:PUBLISHED_IN]->(v:Venue {venueName: $venue})
            RETURN count(DISTINCT p) AS total_papers
            """,
            venue=venue,
        ).single()
        total_papers = total_papers_row["total_papers"] if total_papers_row else 0

        # Use Cypher aggregation projection (modern GDS syntax)
        s.run(
            """
            MATCH (a1:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(a2:Author)
            WHERE (p)-[:PUBLISHED_IN]->(:Venue {venueName: $venue})
              AND id(a1) < id(a2)
            WITH a1, a2, count(p) AS weight
            WITH gds.graph.project('venueGraph', a1, a2, {relationshipProperties: {weight: weight}}) AS g
            RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
            """,
            venue=venue,
        ).consume()

        # Louvain community detection and enrichment
        # 1) Run Louvain and collect authors per community
        community_rows = list(
            s.run(
                """
                CALL gds.louvain.stream('venueGraph', {relationshipWeightProperty: 'weight'})
                YIELD nodeId, communityId
                WITH communityId, gds.util.asNode(nodeId) AS a
                WITH communityId, collect(DISTINCT a) AS authors, count(*) AS author_count
                ORDER BY author_count DESC
                LIMIT $k
                RETURN communityId, authors, author_count
                """,
                k=top_k,
            )
        )

        clusters: list[dict[str, Any]] = []
        for rank, row in enumerate(community_rows, start=1):
            data = row.data()
            community_id = data["communityId"]
            authors = data["authors"]
            author_count = data["author_count"]
            author_names = [a["authorName"] for a in authors]

            # Papers in venue that involve at least one author from this cluster
            papers_row = s.run(
                """
                MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->(v:Venue {venueName: $venue})
                WHERE a.authorName IN $author_names
                RETURN count(DISTINCT p) AS papers_in_venue
                """,
                venue=venue,
                author_names=author_names,
            ).single()
            papers_in_venue = papers_row["papers_in_venue"] if papers_row else 0

            share_of_venue = (
                float(papers_in_venue) * 100.0 / total_papers if total_papers else 0.0
            )

            top_authors = author_names[:5]
            cluster_label = (
                f"Cluster {rank} ({top_authors[0]} et al.)"
                if top_authors
                else f"Cluster {rank}"
            )

            # Pre-baked Cypher query for Neo4j Browser visualization of this cluster
            # Inline author names for copy-paste convenience
            escaped_names = [name.replace('"', '\\"') for name in author_names]
            author_list_literal = "[" + ", ".join(f'"{n}"' for n in escaped_names) + "]"
            neo4j_query = (
                "MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PUBLISHED_IN]->"
                f"(v:Venue {{venueName: \"{venue}\"}})\n"
                f"WHERE a.authorName IN {author_list_literal}\n"
                "WITH collect(DISTINCT a) AS authors\n"
                "UNWIND authors AS a1\n"
                "UNWIND authors AS a2\n"
                "WITH DISTINCT a1, a2\n"
                "MATCH path = (a1)-[:WROTE]->(:Paper)<-[:WROTE]-(a2)\n"
                "RETURN path\n"
                "LIMIT 500;"
            )

            clusters.append(
                {
                    "rank": rank,
                    "community_id": community_id,
                    "cluster_label": cluster_label,
                    "author_count": author_count,
                    "papers_in_venue": papers_in_venue,
                    "share_of_venue": round(share_of_venue, 2),
                    "top_authors": top_authors,
                    "all_authors": author_names,
                    "neo4j_query": neo4j_query,
                }
            )

        s.run(
            "CALL gds.graph.drop('venueGraph') YIELD graphName RETURN graphName"
        ).consume()
    driver.close()
    return {
        "venue": venue,
        "total_papers_in_venue": total_papers,
        "clusters": clusters,
        "store_justification": "Communities/clusters are graph structure. Neo4j GDS provides community detection directly on the co-authorship graph.",
        "note": "Field is approximated by venue for the MVP; you can replace this with a richer field taxonomy later.",
    }


@app.get("/emerging_trends")
def emerging_trends(q: str, since_year: int = 2020, k: int = 20) -> dict[str, Any]:
    """
    Which papers are emerging trends based on semantic similarity?
    Uses Qdrant semantic search + year filter (payload) to focus on recent work.
    """
    from fastembed import TextEmbedding

    embedder = TextEmbedding(model_name=settings.fastembed_model)
    vec = next(embedder.embed([q]))
    qc = qdrant_client()

    res = qc.query_points(
        collection_name=settings.qdrant_collection,
        query=vec.tolist(),
        limit=k,
        with_payload=True,
        query_filter={
            "must": [
                {"key": "year", "range": {"gte": since_year}},
            ]
        },
    )
    hits = res.points
    return {
        "query": q,
        "since_year": since_year,
        "results": [
            {
                "paper_id": h.payload.get("paper_id") if h.payload else None,
                "score": h.score,
                "title": (h.payload or {}).get("title"),
                "year": (h.payload or {}).get("year"),
                "venue": (h.payload or {}).get("venue"),
            }
            for h in hits
        ],
        "store_justification": "Emerging-trend discovery needs semantic similarity (vector search) plus structured time filtering; Qdrant supports payload filtering with kNN.",
    }


@app.get("/bridge_authors")
def bridge_authors(
    limit: int = 20,
    paper_ids: Optional[List[str]] = Query(
        None, description="When set, restrict the co-authorship graph to these papers (topic scope)"
    ),
) -> dict[str, Any]:
    """
    Which authors act as bridges between research domains?
    Uses Neo4j GDS betweenness centrality on the co-authorship graph.

    When paper_ids is provided, the projected co-authorship graph is restricted
    to authors connected via the scoped paper set. This makes 'bridge authors'
    topic-aware instead of global.
    """
    driver = neo4j_driver()
    with driver.session() as s:
        exists = s.run(
            "CALL gds.graph.exists('coauthorGraph') YIELD exists RETURN exists"
        ).single()
        if exists and exists.get("exists"):
            s.run(
                "CALL gds.graph.drop('coauthorGraph') YIELD graphName RETURN graphName"
            ).consume()

        # Use Cypher aggregation projection (modern GDS syntax)
        if paper_ids:
            scoped_ids = paper_ids[:500]
            s.run(
                """
                MATCH (a1:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(a2:Author)
                WHERE id(a1) < id(a2) AND p.paperId IN $paper_ids
                WITH a1, a2, count(p) AS weight
                WITH gds.graph.project('coauthorGraph', a1, a2, {relationshipProperties: {weight: weight}}) AS g
                RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
                """,
                paper_ids=scoped_ids,
            ).consume()
        else:
            s.run(
                """
                MATCH (a1:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(a2:Author)
                WHERE id(a1) < id(a2)
                WITH a1, a2, count(p) AS weight
                WITH gds.graph.project('coauthorGraph', a1, a2, {relationshipProperties: {weight: weight}}) AS g
                RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
                """
            ).consume()

        rows = [
            r.data()
            for r in s.run(
                """
                CALL gds.betweenness.stream('coauthorGraph', {relationshipWeightProperty: 'weight'})
                YIELD nodeId, score
                WITH gds.util.asNode(nodeId) AS a, score
                RETURN a.authorName AS author, score
                ORDER BY score DESC
                LIMIT $limit
                """,
                limit=limit,
            )
        ]

        # Enrich with coauthor degree from the co-authorship graph
        authors = [r["author"] for r in rows]
        if authors:
            if paper_ids:
                degree_rows = s.run(
                    """
                    MATCH (a:Author)-[:WROTE]->(p:Paper)<-[:WROTE]-(co:Author)
                    WHERE a.authorName IN $authors AND p.paperId IN $paper_ids AND a <> co
                    RETURN a.authorName AS author, count(DISTINCT co) AS coauthor_count
                    """,
                    authors=authors,
                    paper_ids=paper_ids[:500],
                )
            else:
                degree_rows = s.run(
                    """
                    MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(co:Author)
                    WHERE a.authorName IN $authors AND a <> co
                    RETURN a.authorName AS author, count(DISTINCT co) AS coauthor_count
                    """,
                    authors=authors,
                )
            coauthor_degree: dict[str, int] = {}
            for dr in degree_rows:
                d = dr.data()
                coauthor_degree[d["author"]] = d["coauthor_count"]
        s.run(
            "CALL gds.graph.drop('coauthorGraph') YIELD graphName RETURN graphName"
        ).consume()
    driver.close()

    # Enrich with scoped paper counts and top 2 venues from Postgres
    scoped_papers_by_author: dict[str, int] = {}
    venues_by_author: dict[str, dict[str, int]] = {}
    if authors:
        with pg_conn() as conn, conn.cursor() as cur:
            # Scoped papers per author
            if paper_ids:
                cur.execute(
                    """
                    SELECT a.name,
                           count(DISTINCT p.id) AS scoped_papers
                    FROM authors a
                    JOIN paper_authors pa ON pa.author_id = a.author_id
                    JOIN papers p ON p.id = pa.paper_id
                    WHERE a.name = ANY(%s) AND p.id = ANY(%s::uuid[])
                    GROUP BY a.name
                    """,
                    (authors, paper_ids[:500]),
                )
            else:
                cur.execute(
                    """
                    SELECT a.name,
                           count(DISTINCT p.id) AS scoped_papers
                    FROM authors a
                    JOIN paper_authors pa ON pa.author_id = a.author_id
                    JOIN papers p ON p.id = pa.paper_id
                    WHERE a.name = ANY(%s)
                    GROUP BY a.name
                    """,
                    (authors,),
                )
            for name, scoped_papers in cur.fetchall():
                scoped_papers_by_author[name] = int(scoped_papers)

            # Venue counts per author
            if paper_ids:
                cur.execute(
                    """
                    SELECT a.name, p.venue, count(*) AS cnt
                    FROM authors a
                    JOIN paper_authors pa ON pa.author_id = a.author_id
                    JOIN papers p ON p.id = pa.paper_id
                    WHERE a.name = ANY(%s)
                      AND p.id = ANY(%s::uuid[])
                      AND p.venue IS NOT NULL AND trim(p.venue) != ''
                    GROUP BY a.name, p.venue
                    """,
                    (authors, paper_ids[:500]),
                )
            else:
                cur.execute(
                    """
                    SELECT a.name, p.venue, count(*) AS cnt
                    FROM authors a
                    JOIN paper_authors pa ON pa.author_id = a.author_id
                    JOIN papers p ON p.id = pa.paper_id
                    WHERE a.name = ANY(%s)
                      AND p.venue IS NOT NULL AND trim(p.venue) != ''
                    GROUP BY a.name, p.venue
                    """,
                    (authors,),
                )
            for name, venue, cnt in cur.fetchall():
                if name not in venues_by_author:
                    venues_by_author[name] = {}
                venues_by_author[name][venue] = venues_by_author[name].get(venue, 0) + int(cnt)

    enriched = []
    for r in rows:
        author = r["author"]
        score = r["score"]
        co_deg = coauthor_degree.get(author, 0)
        scoped_papers = scoped_papers_by_author.get(author, 0)
        venue_counts = venues_by_author.get(author, {})
        # Top 2 venues by count
        sorted_venues = sorted(
            venue_counts.items(), key=lambda kv: kv[1], reverse=True
        )[:2]
        top_venues = [v for v, _ in sorted_venues]
        enriched.append(
            {
                "author": author,
                "score": score,
                "coauthors": co_deg,
                "scoped_papers": scoped_papers,
                "top_venues": top_venues,
            }
        )

    return {
        "results": enriched,
        "store_justification": "Bridge detection is a network-structure problem (betweenness). Graph analytics belong in Neo4j/GDS, not SQL or vector search."
        + (" Scoped to topic papers." if paper_ids else ""),
    }


@app.get("/citations_vs_similarity")
def citations_vs_similarity(
    q: str,
    k: int = 20,
    paper_ids: Optional[List[str]] = Query(
        None, description="When set, intersect topic-scoped papers with semantic search results"
    ),
) -> dict[str, Any]:
    """
    Relationship between paper citations and topic similarity.
    Uses Qdrant to retrieve similar papers, then Postgres for citation counts.

    When paper_ids (topic scope) is provided, we still use semantic similarity from Qdrant
    but intersect the hits with the scoped paper set so that similarity is interpretable
    relative to the query while remaining within scope.
    """
    from fastembed import TextEmbedding

    embedder = TextEmbedding(model_name=settings.fastembed_model)
    vec = next(embedder.embed([q]))
    qc = qdrant_client()
    # If a scoped paper_ids set is provided, request a larger pool of candidates from Qdrant
    # and intersect with the scope. Otherwise, just take top-k.
    effective_limit = max(k, 200) if paper_ids else k
    res = qc.query_points(
        collection_name=settings.qdrant_collection,
        query=vec.tolist(),
        limit=effective_limit,
        with_payload=True,
    )
    hits = res.points
    hit_ids = [
        h.payload.get("paper_id")
        for h in hits
        if h.payload and h.payload.get("paper_id")
    ]

    # If scoped paper_ids are provided, intersect with the IDs returned by Qdrant
    # so that we keep only scoped papers but preserve similarity scores.
    if paper_ids:
        scoped_set = set(paper_ids[:500])
        filtered_hits = [h for h in hits if (h.payload or {}).get("paper_id") in scoped_set]
        hits = filtered_hits[:k]
        paper_ids = [
            (h.payload or {}).get("paper_id")
            for h in hits
            if (h.payload or {}).get("paper_id")
        ]
    else:
        paper_ids = hit_ids

    rows = []
    if paper_ids:
        with pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text, n_citation, year, venue, title
                FROM papers
                WHERE id = ANY(%s::uuid[])
                """,
                (paper_ids,),
            )
            by_id = {
                r[0]: {"n_citation": r[1], "year": r[2], "venue": r[3], "title": r[4]}
                for r in cur.fetchall()
            }

        for h in hits:
            pid = (h.payload or {}).get("paper_id")
            if not pid:
                continue
            meta = by_id.get(pid, {})
            rows.append(
                {
                    "paper_id": pid,
                    "similarity_score": h.score,
                    "n_citation": meta.get("n_citation"),
                    "year": meta.get("year"),
                    "venue": meta.get("venue"),
                    "title": meta.get("title") or (h.payload or {}).get("title"),
                }
            )

    return {
        "query": q,
        "results": rows,
        "store_justification": "Similarity comes from vector search (Qdrant); citation counts and structured stats come from the relational store (Postgres).",
    }


@app.get("/cross_field_relevance")
def cross_field_relevance(
    source_venue: str, target_venue: str, q: str, k: int = 20
) -> dict[str, Any]:
    """
    Which papers in one field could be relevant to another based on content similarity?
    Uses Postgres to constrain candidate papers by venue, Qdrant for semantic search,
    then filters to target venue via payload.
    """
    from fastembed import TextEmbedding

    embedder = TextEmbedding(model_name=settings.fastembed_model)
    vec = next(embedder.embed([q]))

    qc = qdrant_client()
    res = qc.query_points(
        collection_name=settings.qdrant_collection,
        query=vec.tolist(),
        limit=k,
        with_payload=True,
        query_filter={
            "must": [
                {"key": "venue", "match": {"value": target_venue}},
            ]
        },
    )
    hits = res.points
    return {
        "source_venue": source_venue,
        "target_venue": target_venue,
        "query": q,
        "results": [
            {
                "paper_id": (h.payload or {}).get("paper_id"),
                "score": h.score,
                "title": (h.payload or {}).get("title"),
                "year": (h.payload or {}).get("year"),
                "venue": (h.payload or {}).get("venue"),
            }
            for h in hits
        ],
        "store_justification": "Cross-field relevance is content-based (vector search in Qdrant) while the concept of 'field' is represented as structured metadata (venue/category), typically stored in Postgres or payload filters.",
    }


@app.get("/central_but_undercited")
def central_but_undercited(limit: int = 20) -> dict[str, Any]:
    """
    Are there authors whose work is central in the network but under-cited?
    Uses Neo4j degree centrality + Postgres for aggregate citations per author.
    """
    driver = neo4j_driver()
    with driver.session() as s:
        rows = [
            r.data()
            for r in s.run(
                """
                MATCH (a:Author)-[:WROTE]->(:Paper)<-[:WROTE]-(b:Author)
                WHERE a <> b
                WITH a, count(DISTINCT b) AS coauthor_degree
                RETURN a.authorName AS author, coauthor_degree
                ORDER BY coauthor_degree DESC
                LIMIT $limit
                """,
                limit=limit * 5,
            )
        ]
    driver.close()

    # Enrich with citations from Postgres
    authors = [r["author"] for r in rows]
    citations_by_author: dict[str, int] = {}
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.name, COALESCE(SUM(p.n_citation), 0) AS total_citations
            FROM authors a
            JOIN paper_authors pa ON pa.author_id = a.author_id
            JOIN papers p ON p.id = pa.paper_id
            WHERE a.name = ANY(%s)
            GROUP BY a.name
            """,
            (authors,),
        )
        for name, total in cur.fetchall():
            citations_by_author[name] = int(total)

    combined = [
        {
            "author": r["author"],
            "coauthor_degree": r["coauthor_degree"],
            "total_citations": citations_by_author.get(r["author"], 0),
        }
        for r in rows
    ]
    combined.sort(key=lambda x: (-x["coauthor_degree"], x["total_citations"]))
    return {
        "results": combined[:limit],
        "store_justification": "Network centrality comes from graph structure (Neo4j), while citation totals are structured aggregates (Postgres). Combining them reveals central-but-undercited authors.",
    }


@app.get("/topics_connected_via_coauthorship")
def topics_connected_via_coauthorship(
    q: str,
    k: int = 30,
    paper_ids: Optional[List[str]] = Query(None, description="Use these paper IDs instead of semantic search (topic scope)"),
) -> dict[str, Any]:
    """
    Which topics are most connected via co-authorship networks?
    MVP: treat the user's query as a 'topic' representation; retrieve semantically similar papers (Qdrant),
    then use Neo4j to compute how interconnected their authors are. Or use provided paper_ids (topic scope).
    """
    if not paper_ids:
        from fastembed import TextEmbedding

        embedder = TextEmbedding(model_name=settings.fastembed_model)
        vec = next(embedder.embed([q]))
        qc = qdrant_client()
        res = qc.query_points(
            collection_name=settings.qdrant_collection,
            query=vec.tolist(),
            limit=k,
            with_payload=True,
        )
        hits = res.points
        paper_ids = [
            h.payload.get("paper_id")
            for h in hits
            if h.payload and h.payload.get("paper_id")
        ]
    else:
        paper_ids = paper_ids[:500]

    driver = neo4j_driver()
    with driver.session() as s:
        res = s.run(
            """
            MATCH (p:Paper)<-[:WROTE]-(a:Author)
            WHERE p.paperId IN $paper_ids
            WITH collect(DISTINCT a) AS authors
            UNWIND authors AS a1
            UNWIND authors AS a2
            WITH a1, a2 WHERE a1 <> a2
            MATCH (a1)-[:WROTE]->(:Paper)<-[:WROTE]-(a2)
            RETURN count(DISTINCT a1) AS author_count, count(*) AS coauth_links
            """,
            paper_ids=paper_ids,
        ).single()
    driver.close()

    return {
        "query_topic": q,
        "paper_sample_size": len(paper_ids),
        "author_count": res["author_count"] if res else 0,
        "coauth_links": res["coauth_links"] if res else 0,
        "store_justification": "Topic similarity is derived from embeddings (Qdrant), while connectivity is a co-authorship network property (Neo4j).",
        "note": "For a richer notion of 'topic', add topic labels via clustering or taxonomy and compute connectivity per topic label.",
    }
