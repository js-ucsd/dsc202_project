from __future__ import annotations

import time
from typing import Optional

import psycopg
import typer
from qdrant_client import QdrantClient

from .dblp import iter_batches, iter_dblp_papers
from .neo4j_loader import clear_graph, ensure_constraints, neo4j_driver, upsert_graph
from .postgres import (
    ensure_postgres_schema,
    truncate_all,
    upsert_authors_and_links,
    upsert_citations,
    upsert_papers,
)
from .qdrant_loader import drop_collection_if_exists, embedder_fastembed, ensure_collection, upsert_vectors
from .settings import settings


app = typer.Typer(no_args_is_help=True)


@app.command()
def ingest(
    csv: str = typer.Option(settings.dblp_parquet_path, help="Path to DBLP Parquet or CSV"),
    limit: Optional[int] = typer.Option(50_000, help="Max rows to ingest (dev-friendly default)"),
    batch_size: int = typer.Option(500, help="Batch size for inserts/upserts"),
    truncate: bool = typer.Option(
        False,
        "--truncate",
        help="Truncate all project data (Postgres/Neo4j/Qdrant) before loading a fresh snapshot.",
    ),
):
    """
    Ingest DBLP CSV into Postgres, Neo4j, and Qdrant.
    """
    t0 = time.time()

    # Postgres
    pg = psycopg.connect(settings.postgres_dsn())
    ensure_postgres_schema(pg)

    # Neo4j
    driver = neo4j_driver(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
    ensure_constraints(driver)

    # Qdrant + embeddings
    qdrant = QdrantClient(url=settings.qdrant_url)
    embedder = embedder_fastembed(settings.fastembed_model)
    # Infer vector dimension from a sample embedding (fastembed TextEmbedding has no .dim attribute)
    sample_vec = next(embedder.embed(["dimension probe"]))
    vector_size = len(sample_vec)

    if truncate:
        typer.echo("Truncating Postgres tables...")
        truncate_all(pg)
        typer.echo("Clearing Neo4j graph...")
        clear_graph(driver)
        typer.echo(f"Dropping Qdrant collection '{settings.qdrant_collection}' if it exists...")
        drop_collection_if_exists(qdrant, settings.qdrant_collection)

    ensure_collection(qdrant, settings.qdrant_collection, vector_size)

    papers_iter = iter_dblp_papers(csv, limit=limit)
    for batch in iter_batches(papers_iter, batch_size=batch_size):
        upsert_papers(pg, batch)
        upsert_authors_and_links(pg, batch)
        upsert_citations(pg, batch)

        upsert_graph(driver, batch)
        upsert_vectors(qdrant, settings.qdrant_collection, embedder, batch)

    driver.close()
    pg.close()

    typer.echo(f"Done in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    app()

