from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st


API_BASE = os.getenv("API_BASE", "http://localhost:8000")

st.set_page_config(page_title="Paper KG + Semantic Search", layout="wide")
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.2rem;
        max-width: 1200px;
    }
    .hero {
        background: linear-gradient(120deg, #0f172a 0%, #1f2937 45%, #0b3b66 100%);
        border-radius: 16px;
        padding: 1.2rem 1.4rem;
        color: #e5f3ff;
        margin-bottom: 1rem;
        border: 1px solid rgba(148, 163, 184, 0.25);
    }
    .hero h2 {
        margin: 0;
        letter-spacing: 0.2px;
        font-size: 1.4rem;
    }
    .hero p {
        margin: 0.4rem 0 0 0;
        color: #c7dff5;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
      <h2>Scientific Paper Knowledge Graph + Semantic Search</h2>
      <p>Qdrant for semantic retrieval, Neo4j for graph reasoning, Postgres for structured analytics.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


def _table(rows: list[dict[str, Any]], height: int = 360) -> None:
    if not rows:
        st.warning("No results returned.")
        return
    st.dataframe(rows, use_container_width=True, height=height)


def _call_api(path: str, params: dict | None = None) -> dict[str, Any] | None:
    try:
        with httpx.Client(timeout=60.0) as client:
            r = client.get(f"{API_BASE}{path}", params=params)
            r.raise_for_status()
            return r.json()
    except httpx.HTTPError as e:
        st.error(f"API request failed for {path}: {e}")
        return None


with st.expander("Current app logic and what each tab does", expanded=False):
    st.markdown(
        """
This app is a query router UI over three stores:

- `Qdrant`: semantic nearest-neighbor search on embedded title+abstract.
- `Neo4j`: relationship traversals and graph algorithms (GDS).
- `Postgres`: citation and metadata aggregation/filtering.

Tab guide:

- `Dashboard`: overview stats from all three databases.
- `Semantic search`: text query to retrieve semantically similar papers.
- `Graph exploration`: co-authorship and indirect citation traversals.
- `Cross-store analytics`: combined vector + SQL + graph insights.
- `Graph analytics (GDS)`: Louvain communities and bridge-author centrality.
        """
    )


tab0, tab1, tab2, tab3, tab4 = st.tabs(
    [
        "Dashboard",
        "Semantic search",
        "Graph exploration",
        "Cross-store analytics",
        "Graph analytics (GDS)",
    ]
)

with tab0:
    st.caption("At-a-glance statistics from Postgres, Neo4j, and Qdrant.")
    data = _call_api("/stats")
    if data:
        pg = data.get("postgres", {})
        n4 = data.get("neo4j", {})
        qd = data.get("qdrant", {})

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Papers", f"{pg.get('papers', 0):,}")
        c2.metric("Authors", f"{pg.get('authors', 0):,}")
        c3.metric("Venues", f"{pg.get('venues', 0):,}")
        c4.metric("Total Citations", f"{pg.get('total_citations', 0):,}")

        c5, c6, c7 = st.columns(3)
        c5.metric("Neo4j Nodes", f"{n4.get('nodes', 0):,}")
        c6.metric("Neo4j Relationships", f"{n4.get('relationships', 0):,}")
        c7.metric("Qdrant Vectors", f"{qd.get('vectors', 0):,}")

        import pandas as pd

        col_left, col_right = st.columns(2)
        with col_left:
            st.subheader("Top 10 Venues")
            venues = pg.get("top_venues", [])
            if venues:
                df_v = pd.DataFrame(venues)
                st.bar_chart(df_v.set_index("venue")["count"])
        with col_right:
            st.subheader("Papers by Year")
            pby = pg.get("papers_by_year", [])
            if pby:
                df_y = pd.DataFrame(pby)
                st.bar_chart(df_y.set_index("year")["count"])

with tab1:
    st.caption("Find papers by meaning, not keyword overlap.")
    q = st.text_input(
        "Research question / query",
        value="graph neural networks for citation prediction",
    )
    k = st.slider("Top K", 5, 30, 10)
    if st.button("Search"):
        data = _call_api("/semantic_search", {"q": q, "k": k})
        if data:
            rows = data.get("results", [])
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Results", len(rows))
            with c2:
                best = rows[0]["score"] if rows else None
                st.metric(
                    "Best similarity score",
                    f"{best:.4f}" if isinstance(best, float) else "n/a",
                )
            _table(rows)
            st.info(data["store_justification"])

with tab2:
    st.caption("Explore graph paths and collaborations in Neo4j.")
    st.subheader("Top collaborators (co-authorship)")
    if st.button("Compute top collaborator pairs"):
        data = _call_api("/top_collaborators", {"limit": 20})
        if data:
            _table(data.get("results", []))
            st.info(data["store_justification"])

    st.divider()
    st.subheader("Indirect citers of a paper (citation paths)")
    pid = st.text_input("Paper UUID", value="56cd3fdb-73ff-431e-8945-d673f9469f33")
    hops = st.slider("Max hops", 1, 5, 3)
    if st.button("Find indirect citers"):
        if not pid.strip():
            st.warning("Enter a paper UUID.")
        else:
            data = _call_api(
                "/indirect_citers",
                {"paper_id": pid.strip(), "max_hops": hops, "limit": 20},
            )
            if data:
                _table(data.get("results", []))
                st.info(data["store_justification"])

with tab3:
    st.caption("Blend vector similarity with relational and graph analytics.")
    st.subheader("Citations vs similarity")
    q2 = st.text_input("Topic query", value="deep learning")
    if st.button("Analyze citations vs similarity"):
        data = _call_api("/citations_vs_similarity", {"q": q2, "k": 20})
        if data:
            _table(data.get("results", []))
            st.info(data["store_justification"])

    st.divider()
    st.subheader("Emerging trends (recent papers similar to query)")
    since = st.number_input("Since year", value=2015, min_value=1950, max_value=2100)
    if st.button("Find emerging papers"):
        data = _call_api(
            "/emerging_trends", {"q": q2, "since_year": int(since), "k": 20}
        )
        if data:
            _table(data.get("results", []))
            st.info(data["store_justification"])

    st.divider()
    st.subheader("Cross-field relevance (by venue)")
    source_venue = st.text_input(
        "Source venue (label only, MVP)", value="Neurocomputing"
    )
    target_venue = st.text_input(
        "Target venue", value="international conference on computer vision"
    )
    if st.button("Find cross-field relevant papers"):
        data = _call_api(
            "/cross_field_relevance",
            {
                "source_venue": source_venue,
                "target_venue": target_venue,
                "q": q2,
                "k": 20,
            },
        )
        if data:
            _table(data.get("results", []))
            st.info(data["store_justification"])

    st.divider()
    st.subheader("Central but under-cited authors")
    if st.button("Find central-but-undercited"):
        data = _call_api("/central_but_undercited", {"limit": 20})
        if data:
            _table(data.get("results", []))
            st.info(data["store_justification"])

    st.divider()
    st.subheader("Topics connected via co-authorship")
    if st.button("Compute topic connectivity"):
        data = _call_api("/topics_connected_via_coauthorship", {"q": q2, "k": 30})
        if data:
            c1, c2, c3 = st.columns(3)
            c1.metric("Paper sample", data.get("paper_sample_size", 0))
            c2.metric("Authors in sample", data.get("author_count", 0))
            c3.metric("Co-authorship links", data.get("coauth_links", 0))
            st.info(data["store_justification"])
            st.caption(data.get("note", ""))

with tab4:
    st.caption("Run graph data science routines from Neo4j GDS.")
    st.subheader("Author clusters dominating a venue (Louvain)")
    venue = st.text_input("Venue (field proxy)", value="Neurocomputing")
    if st.button("Compute clusters"):
        data = _call_api("/author_clusters_by_venue", {"venue": venue, "top_k": 5})
        if data:
            _table(data.get("top_communities", []), height=260)
            st.info(data["store_justification"])
            st.caption(data.get("note", ""))

    st.divider()
    st.subheader("Bridge authors (betweenness centrality)")
    if st.button("Compute bridge authors"):
        data = _call_api("/bridge_authors", {"limit": 20})
        if data:
            _table(data.get("results", []))
        st.info(data["store_justification"])
