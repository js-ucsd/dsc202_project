from __future__ import annotations

from collections.abc import Iterable

from neo4j import GraphDatabase

from .dblp import DblpPaper


CONSTRAINTS_CYPHER = """
CREATE CONSTRAINT paper_id IF NOT EXISTS
FOR (p:Paper) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT author_name IF NOT EXISTS
FOR (a:Author) REQUIRE a.name IS UNIQUE;

CREATE INDEX paper_year IF NOT EXISTS
FOR (p:Paper) ON (p.year);

CREATE INDEX paper_venue IF NOT EXISTS
FOR (p:Paper) ON (p.venue);
"""


def ensure_constraints(driver) -> None:
    with driver.session() as session:
        for stmt in [s.strip() for s in CONSTRAINTS_CYPHER.split(";") if s.strip()]:
            session.run(stmt)


def clear_graph(driver) -> None:
    """
    Remove all Paper/Author nodes and their relationships.
    Safe for this project-specific Neo4j instance.
    """
    with driver.session() as session:
        session.run("MATCH (n:Author)-[r]-() DELETE r")
        session.run("MATCH (n:Paper)-[r]-() DELETE r")
        session.run("MATCH (n:Author) DELETE n")
        session.run("MATCH (n:Paper) DELETE n")


def upsert_graph(driver, papers: Iterable[DblpPaper]) -> None:
    with driver.session() as session:
        for p in papers:
            session.run(
                """
                MERGE (paper:Paper {id: $id})
                SET paper.title = $title,
                    paper.year = $year,
                    paper.venue = $venue
                """,
                id=p.id,
                title=p.title,
                year=p.year,
                venue=p.venue,
            )

            for author in p.authors:
                session.run(
                    """
                    MERGE (a:Author {name: $name})
                    WITH a
                    MATCH (p:Paper {id: $paper_id})
                    MERGE (a)-[:AUTHORED]->(p)
                    """,
                    name=author,
                    paper_id=p.id,
                )

            for ref in p.references:
                session.run(
                    """
                    MERGE (src:Paper {id: $src})
                    MERGE (dst:Paper {id: $dst})
                    MERGE (src)-[:CITES]->(dst)
                    """,
                    src=p.id,
                    dst=ref,
                )


def neo4j_driver(uri: str, user: str, password: str):
    return GraphDatabase.driver(uri, auth=(user, password))

