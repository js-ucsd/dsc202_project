from __future__ import annotations

import ast
import csv
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

import pandas as pd


@dataclass(frozen=True)
class DblpPaper:
    id: str
    title: str
    abstract: Optional[str]
    venue: Optional[str]
    year: Optional[int]
    n_citation: int
    authors: tuple[str, ...]
    references: tuple[str, ...]


def _parse_list_field(value: str) -> list[str]:
    """
    The CSV encodes lists like "['A', 'B']". Some rows may already be unquoted.
    """
    if value is None:
        return []
    s = value.strip()
    if s == "" or s == "[]":
        return []
    if s[0] != "[":
        # Unexpected shape; treat as single token.
        return [s]
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
        return []
    except Exception:
        return []


def _iter_from_csv(path: str, limit: Optional[int]) -> Iterator[DblpPaper]:
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            if limit is not None and count >= limit:
                break
            pid = (row.get("id") or "").strip()
            if not pid:
                continue

            title = (row.get("title") or "").strip()
            abstract = (row.get("abstract") or "").strip() or None
            venue = (row.get("venue") or "").strip() or None

            year_raw = (row.get("year") or "").strip()
            year = int(year_raw) if year_raw.isdigit() else None

            nc_raw = (row.get("n_citation") or "").strip()
            try:
                n_citation = int(nc_raw)
            except Exception:
                n_citation = 0

            authors = tuple(_parse_list_field(row.get("authors") or ""))
            references = tuple(_parse_list_field(row.get("references") or ""))

            yield DblpPaper(
                id=pid,
                title=title or "(missing title)",
                abstract=abstract,
                venue=venue,
                year=year,
                n_citation=n_citation,
                authors=authors,
                references=references,
            )
            count += 1


def _iter_from_parquet(path: str, limit: Optional[int]) -> Iterator[DblpPaper]:
    df = pd.read_parquet(path)
    if limit is not None:
        df = df.head(limit)

    for row in df.itertuples(index=False):
        pid = getattr(row, "id", "").strip()
        if not pid:
            continue

        title = (getattr(row, "title", "") or "").strip()
        abstract_raw = getattr(row, "abstract", None)
        abstract = (
            abstract_raw.strip()
            if isinstance(abstract_raw, str) and abstract_raw.strip()
            else None
        )
        venue_raw = getattr(row, "venue", None)
        venue = venue_raw.strip() if isinstance(venue_raw, str) and venue_raw.strip() else None

        year_raw = getattr(row, "year", None)
        year = int(year_raw) if year_raw is not None and str(year_raw).isdigit() else None

        nc_raw = getattr(row, "n_citation", 0)
        try:
            n_citation = int(nc_raw)
        except Exception:
            n_citation = 0

        authors = tuple(_parse_list_field(getattr(row, "authors", "") or ""))
        references = tuple(_parse_list_field(getattr(row, "references", "") or ""))

        yield DblpPaper(
            id=pid,
            title=title or "(missing title)",
            abstract=abstract,
            venue=venue,
            year=year,
            n_citation=n_citation,
            authors=authors,
            references=references,
        )


def iter_dblp_papers(path: str, limit: Optional[int] = None) -> Iterator[DblpPaper]:
    if path.endswith(".parquet"):
        return _iter_from_parquet(path, limit)
    return _iter_from_csv(path, limit)


def iter_batches(items: Iterable, batch_size: int) -> Iterator[list]:
    batch: list = []
    for x in items:
        batch.append(x)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

