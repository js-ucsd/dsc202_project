from __future__ import annotations

from collections.abc import Iterable

import numpy as np
from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from .dblp import DblpPaper


def ensure_collection(client: QdrantClient, collection: str, vector_size: int) -> None:
    existing = {c.name for c in client.get_collections().collections}
    if collection in existing:
        return
    client.create_collection(
        collection_name=collection,
        vectors_config=qm.VectorParams(size=vector_size, distance=qm.Distance.COSINE),
    )


def drop_collection_if_exists(client: QdrantClient, collection: str) -> None:
    existing = {c.name for c in client.get_collections().collections}
    if collection in existing:
        client.delete_collection(collection_name=collection)


def embedder_fastembed(model_name: str) -> TextEmbedding:
    return TextEmbedding(model_name=model_name)


def paper_text(p: DblpPaper) -> str:
    if p.abstract:
        return f"{p.title}\n{p.abstract}"
    return p.title


def upsert_vectors(
    client: QdrantClient,
    collection: str,
    embedder: TextEmbedding,
    papers: Iterable[DblpPaper],
) -> None:
    papers_list = list(papers)
    texts = [paper_text(p) for p in papers_list]
    vectors = list(embedder.embed(texts))
    vectors_np = [np.asarray(v, dtype=np.float32).tolist() for v in vectors]

    points: list[qm.PointStruct] = []
    for p, vec in zip(papers_list, vectors_np, strict=True):
        payload = {
            "paper_id": p.id,
            "title": p.title,
            "year": p.year,
            "venue": p.venue,
        }
        points.append(qm.PointStruct(id=p.id, vector=vec, payload=payload))

    client.upsert(collection_name=collection, points=points)

