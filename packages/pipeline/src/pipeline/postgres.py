from __future__ import annotations

from collections.abc import Iterable

import psycopg

from .dblp import DblpPaper


def ensure_postgres_schema(conn: psycopg.Connection) -> None:
    # Schema is created via docker init.sql, but this makes local runs robust.
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
              id UUID PRIMARY KEY,
              title TEXT NOT NULL,
              abstract TEXT,
              venue TEXT,
              year INT,
              n_citation INT DEFAULT 0
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS authors (
              author_id BIGSERIAL PRIMARY KEY,
              name TEXT NOT NULL UNIQUE
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_authors (
              paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
              author_id BIGINT NOT NULL REFERENCES authors(author_id) ON DELETE CASCADE,
              PRIMARY KEY (paper_id, author_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS citations (
              citing_paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
              cited_paper_id UUID NOT NULL,
              PRIMARY KEY (citing_paper_id, cited_paper_id)
            );
            """
        )
        conn.commit()


def truncate_all(conn: psycopg.Connection) -> None:
    """
    Truncate all project tables so we can reload a fresh snapshot.
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE citations, paper_authors, authors, papers RESTART IDENTITY CASCADE;")
    conn.commit()


def upsert_papers(conn: psycopg.Connection, papers: Iterable[DblpPaper]) -> None:
    rows = [
        (p.id, p.title, p.abstract, p.venue, p.year, p.n_citation)
        for p in papers
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO papers (id, title, abstract, venue, year, n_citation)
            VALUES (%s::uuid, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
              title = EXCLUDED.title,
              abstract = EXCLUDED.abstract,
              venue = EXCLUDED.venue,
              year = EXCLUDED.year,
              n_citation = EXCLUDED.n_citation;
            """,
            rows,
        )
    conn.commit()


def upsert_authors_and_links(conn: psycopg.Connection, papers: Iterable[DblpPaper]) -> None:
    with conn.cursor() as cur:
        for p in papers:
            for a in p.authors:
                cur.execute(
                    "INSERT INTO authors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING;",
                    (a,),
                )
        for p in papers:
            for a in p.authors:
                cur.execute("SELECT author_id FROM authors WHERE name = %s;", (a,))
                author_id = cur.fetchone()[0]
                cur.execute(
                    """
                    INSERT INTO paper_authors (paper_id, author_id)
                    VALUES (%s::uuid, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (p.id, author_id),
                )
    conn.commit()


def upsert_citations(conn: psycopg.Connection, papers: Iterable[DblpPaper]) -> None:
    with conn.cursor() as cur:
        for p in papers:
            for ref in p.references:
                cur.execute(
                    """
                    INSERT INTO citations (citing_paper_id, cited_paper_id)
                    VALUES (%s::uuid, %s::uuid)
                    ON CONFLICT DO NOTHING;
                    """,
                    (p.id, ref),
                )
    conn.commit()


