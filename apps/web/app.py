from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st


API_BASE = os.getenv("API_BASE", "http://localhost:8000")

st.set_page_config(page_title="Research Discovery and Influence Analysis Tool", layout="wide")
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
    [data-testid="stMetricValue"] {
        font-size: 1.25rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
      <h2>Research Discovery and Influence Analysis Tool</h2>
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


# Topic scope: optional set of papers from semantic search; when set, filter/explore and some other tabs restrict to these papers.
if "topic_papers" not in st.session_state:
    st.session_state.topic_papers = None
if "topic_paper_ids" not in st.session_state:
    st.session_state.topic_paper_ids = None


def _scope_params() -> dict:
    """Query params to restrict results to the current topic scope (when set)."""
    ids = st.session_state.get("topic_paper_ids")
    if not ids:
        return {}
    # Cap at 500 to avoid huge URLs
    return {"paper_ids": ids[:500]}


with st.expander("Topic scope (optional)", expanded=True):
    st.caption("Set a research topic to restrict Filter & explore and related tabs to semantically similar papers.")
    topic_q = st.text_input(
        "Research topic / problem",
        value="",
        placeholder="e.g. graph neural networks for citation prediction",
        key="topic_query",
    )
    topic_k = st.slider("Number of papers to include in scope", 20, 200, 50, key="topic_k")
    col_t1, col_t2 = st.columns(2)
    with col_t1:
        if st.button("Set scope", key="set_scope"):
            if not topic_q.strip():
                st.warning("Enter a research topic.")
            else:
                data = _call_api("/semantic_search", {"q": topic_q.strip(), "k": topic_k})
                if data:
                    rows = data.get("results", [])
                    ids = [r["paper_id"] for r in rows if r.get("paper_id")]
                    st.session_state.topic_papers = rows
                    st.session_state.topic_paper_ids = ids
                    st.success(f"Scope set to {len(ids)} papers. Other tabs will filter to this set when you run queries.")
    with col_t2:
        if st.button("Clear scope", key="clear_scope"):
            st.session_state.topic_papers = None
            st.session_state.topic_paper_ids = None
            st.rerun()
    if st.session_state.topic_papers:
        st.markdown(f"**Scope: {len(st.session_state.topic_paper_ids)} papers** (from semantic search)")
        _table(st.session_state.topic_papers, height=220)


with st.expander("Current app logic and what each tab does", expanded=False):
    st.markdown(
        """
This app is a query router UI over three stores:

- `Qdrant`: semantic nearest-neighbor search on embedded title+abstract.
- `Neo4j`: relationship traversals and graph algorithms (GDS).
- `Postgres`: citation and metadata aggregation/filtering.

Tab guide:

- `Dashboard`: overview stats from all three databases.
- `Paper search & filters`: Postgres-only structured queries (filtering, aggregation, joins, integrity, time, ranking).
- `Collaboration & citation networks`: co-authorship and indirect citation traversals.
- `Topic insights & trends`: combined vector + SQL + graph insights.
- `Advanced network analysis`: Louvain communities and bridge-author centrality.
        """
    )


tab_dashboard, tab_filter, tab2, tab3, tab4 = st.tabs(
    [
        "Dashboard",
        "Paper search & filters",
        "Collaboration & citation networks",
        "Topic insights & trends",
        "Advanced network analysis",
    ]
)


def _show_justification(data: dict | None) -> None:
    if data and data.get("store_justification"):
        st.caption(data["store_justification"])


def _show_sql(data: dict | None) -> None:
    """Show SQL in a collapsed expander when the API returns it."""
    if data and data.get("sql"):
        with st.expander("Show SQL", expanded=False):
            st.code(data["sql"], language="sql")


with tab_filter:
    st.markdown("Find and summarize papers using filters or ready-made views.")
    tab_basic, tab_adv = st.tabs(["Basic filters", "Advanced analytics"])

    with tab_basic:
        st.markdown("Narrow down papers by year, venue, author, and citations, then sort the results.")
        col1, col2 = st.columns(2)
        with col1:
            year_min = st.number_input("Year min", value=2005, min_value=1900, max_value=2100, key="qb_ymin")
            year_max = st.number_input("Year max", value=2015, min_value=1900, max_value=2100, key="qb_ymax")
            min_cit = st.number_input("Min citations", value=0, min_value=0, key="qb_minc")
            max_cit = st.number_input("Max citations (0 = no limit)", value=0, min_value=0, key="qb_maxc")
        with col2:
            venue = st.text_input("Venue (partial match)", value="", placeholder="e.g. Neurocomputing", key="qb_venue")
            author = st.text_input("Author (optional)", value="", placeholder="e.g. Smith", key="qb_author")
            sort_by = st.selectbox(
                "Sort by",
                ["n_citation_desc", "year_desc", "year_asc", "title_asc"],
                format_func=lambda x: {"n_citation_desc": "Most cited first", "year_desc": "Newest first", "year_asc": "Oldest first", "title_asc": "Title A→Z"}[x],
                key="qb_sort",
            )
            limit = st.slider("Max results", 10, 200, 50, key="qb_limit")
        if st.button("Run query", key="qb_run"):
            params = {"year_min": year_min, "year_max": year_max, "min_citations": min_cit, "sort_by": sort_by, "limit": limit}
            if venue.strip():
                params["venue"] = venue.strip()
            if author.strip():
                params["author"] = author.strip()
            if max_cit > 0:
                params["max_citations"] = max_cit
            params = {**params, **_scope_params()}
            data = _call_api("/filter/papers_query", params)
            if data:
                _table(data.get("results", []))
                _show_justification(data)
                _show_sql(data)

    with tab_adv:
        st.markdown("See ready-made summaries (trends, venues, authors) and quick data quality checks.")
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Publication trends & aggregates")
            if st.button("Run trends", key="adv_trends"):
                d1 = _call_api("/filter/papers_per_year", _scope_params())
                d2 = _call_api("/filter/avg_citations_per_year", _scope_params())
                if d1:
                    _table(d1.get("results", []))
                    _show_sql(d1)
                if d2:
                    _table(d2.get("results", []))
                    _show_sql(d2)

            st.subheader("Venue analytics")
            top_n_v = st.slider("Top N venues", 5, 30, 10, key="adv_topv")
            if st.button("Run venue analytics", key="adv_venues"):
                dv = _call_api("/filter/venues_by_paper_count", {"limit": top_n_v, **_scope_params()})
                if dv:
                    _table(dv.get("results", []))
                    _show_justification(dv)
                davg = _call_api("/filter/avg_citations_per_venue", {"limit": top_n_v, **_scope_params()})
                if davg:
                    _table(davg.get("results", []))
                    _show_justification(davg)

        with c2:
            st.subheader("Author analytics")
            if st.button("Run author analytics", key="adv_authors"):
                da = _call_api("/filter/authors_by_paper_count", {"limit": 30, **_scope_params()})
                if da:
                    _table(da.get("results", []))
                    _show_sql(da)
                dtc = _call_api("/filter/total_citations_per_author", {"limit": 30, **_scope_params()})
                if dtc:
                    _table(dtc.get("results", []))
                    _show_sql(dtc)

        st.subheader("Data quality & integrity")
        if st.button("Run data quality checks", key="adv_dq"):
            dup = _call_api("/filter/duplicate_paper_ids")
            miss = _call_api("/filter/papers_missing_venue", {"limit": 50})
            if dup:
                st.caption("Duplicate paper IDs (expect empty)")
                _table(dup.get("results", []))
            if miss:
                st.caption("Papers missing venue")
                _table(miss.get("results", []))
            orphan = _call_api("/filter/paper_authors_orphaned")
            fut = _call_api("/filter/papers_future_year", {"limit": 20})
            if orphan:
                st.caption("Orphaned paper_authors (expect empty)")
                _table(orphan.get("results", []))
            if fut:
                st.caption("Papers with future year")
                _table(fut.get("results", []))


with tab_dashboard:
    st.markdown("### At-a-glance statistics")
    st.caption("Overview of papers, authors, venues, and graph/semantic coverage." + (" **Scoped to topic.**" if st.session_state.get("topic_paper_ids") else ""))
    data = _call_api("/stats", _scope_params())
    if not data:
        st.info("Could not reach the API. Start it with: `uvicorn apps.api.main:app --reload --port 8000` (or set API_BASE if using another port).")
    else:
        try:
            pg = data.get("postgres", {})
            n4 = data.get("neo4j", {})
            qd = data.get("qdrant", {})

            # Single row of 7 equal metric cards, aligned
            m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
            m1.metric("Papers", f"{pg.get('papers', 0):,}")
            m2.metric("Authors", f"{pg.get('authors', 0):,}")
            m3.metric("Venues", f"{pg.get('venues', 0):,}")
            m4.metric("Total Citations", f"{pg.get('total_citations', 0):,}")
            m5.metric("Graph nodes", f"{n4.get('nodes', 0):,}")
            m6.metric("Graph relationships", f"{n4.get('relationships', 0):,}")
            m7.metric("Semantic vectors", f"{qd.get('vectors', 0):,}")

            import pandas as pd

            try:
                import altair as alt
            except ImportError:
                alt = None

            col_left, col_right = st.columns(2)
            with col_left:
                st.subheader("Top 10 Venues")
                venues = pg.get("top_venues", [])
                if venues:
                    df_v = pd.DataFrame(venues)
                    if alt is not None:
                        chart_v = (
                            alt.Chart(df_v)
                            .mark_bar(cornerRadius=4)
                            .encode(
                                x=alt.X("venue:N", sort="-y", title="Venue", axis=alt.Axis(labelLimit=120, labelFontSize=12, titleFontSize=13)),
                                y=alt.Y("count:Q", title="Papers", axis=alt.Axis(labelFontSize=12, titleFontSize=13)),
                                color=alt.Color("venue:N", scale=alt.Scale(scheme="tableau20"), legend=None),
                            )
                            .properties(height=420, width=500)
                            .configure_axis(labelFontSize=12, titleFontSize=13)
                        )
                        st.altair_chart(chart_v, use_container_width=True)
                    else:
                        st.bar_chart(df_v.set_index("venue")["count"])
                else:
                    st.caption("No venue data.")
            with col_right:
                st.subheader("Papers by Year")
                pby = pg.get("papers_by_year", [])
                if pby:
                    df_y = pd.DataFrame(pby)
                    if alt is not None:
                        chart_y = (
                            alt.Chart(df_y)
                            .mark_bar(cornerRadius=4)
                            .encode(
                                x=alt.X("year:O", title="Year", axis=alt.Axis(labelFontSize=12, titleFontSize=13)),
                                y=alt.Y("count:Q", title="Papers", axis=alt.Axis(labelFontSize=12, titleFontSize=13)),
                                color=alt.Color("year:O", scale=alt.Scale(scheme="plasma"), legend=None),
                            )
                            .properties(height=420, width=500)
                            .configure_axis(labelFontSize=12, titleFontSize=13)
                        )
                        st.altair_chart(chart_y, use_container_width=True)
                    else:
                        st.bar_chart(df_y.set_index("year")["count"])
                else:
                    st.caption("No year data.")
        except Exception as e:
            st.error(f"Dashboard error: {e}")

with tab2:
    st.caption("See who works with whom and how papers cite each other.")
    st.subheader("Top collaborators (co-authorship)")
    top_collab_limit = st.slider("Number of collaborator pairs to show", 10, 100, 10, step=5)
    if st.button("Compute top collaborator pairs"):
        data = _call_api("/top_collaborators", {"limit": top_collab_limit, **_scope_params()})
        if data:
            _table(data.get("results", []))
            st.info(data["store_justification"])

    st.divider()
    st.subheader("Indirect citers of a paper (citation paths)")
    topic_papers = st.session_state.get("topic_papers") or []
    if topic_papers:
        pick_idx = st.selectbox(
            "Pick a paper from topic scope",
            range(len(topic_papers)),
            format_func=lambda i: f"{(topic_papers[i].get('title') or topic_papers[i].get('paper_id', ''))[:55]}...",
            key="graph_pick_scope",
        )
        pid = topic_papers[pick_idx]["paper_id"]
        st.caption(f"Paper ID: `{pid}`")
    else:
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
                results = data.get("results", [])
                if results:
                    # Aggregate by hop length for quick interpretation
                    by_hops: dict[int, int] = {}
                    for r in results:
                        h = int(r.get("hops") or 0)
                        by_hops[h] = by_hops.get(h, 0) + 1
                    c1, c2 = st.columns(2)
                    direct = by_hops.get(1, 0)
                    indirect = sum(cnt for h, cnt in by_hops.items() if h > 1)
                    c1.metric("Direct citers (1 hop)", direct)
                    c2.metric("Indirect citers (>1 hop)", indirect)
                    st.caption("Table below shows each citing paper and how many citation hops away it is from the target.")
                    _table(results)
                else:
                    _table(results)
                st.info(data["store_justification"])

with tab3:
    st.caption("Understand how your topic connects to highly cited and emerging work.")
    st.subheader("Citations vs similarity")
    q2 = st.text_input("Topic query", value="deep learning")
    if st.button("Analyze citations vs similarity"):
        data = _call_api("/citations_vs_similarity", {"q": q2, "k": 20, **_scope_params()})
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
    venue_scope = _scope_params()
    venues_data = _call_api("/filter/distinct_venues", venue_scope if venue_scope else None)
    venue_options = [v["venue"] for v in (venues_data or {}).get("results", [])] if venues_data else []
    if venue_options:
        source_venue = st.selectbox("Source venue", venue_options, index=0)
        target_venue = st.selectbox("Target venue", venue_options, index=min(1, len(venue_options) - 1))
    else:
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
        data = _call_api("/topics_connected_via_coauthorship", {"q": q2, "k": 30, **_scope_params()})
        if data:
            paper_sample = data.get("paper_sample_size", 0)
            author_count = data.get("author_count", 0)
            coauth_links = data.get("coauth_links", 0)
            c1, c2, c3 = st.columns(3)
            c1.metric("Paper sample", paper_sample)
            c2.metric("Authors in sample", author_count)
            c3.metric("Co-authorship links", coauth_links)

            # Derived connectivity measures for interpretability
            if author_count > 0:
                avg_links_per_author = coauth_links * 2 / author_count
            else:
                avg_links_per_author = 0.0

            density_note = ""
            if avg_links_per_author < 1:
                density_note = "This topic's author community is quite fragmented: most authors coauthor with very few others."
            elif avg_links_per_author < 3:
                density_note = "This topic has a moderately connected community: some collaboration across authors, but still room for new bridges."
            else:
                density_note = "This topic's author community is densely connected: many authors coauthor with several others."

            st.markdown(
                f"**Average co-authorship links per author:** {avg_links_per_author:.2f}  \n"
                f"{density_note}"
            )
            st.info(data["store_justification"])
            st.caption(data.get("note", ""))

with tab4:
    st.caption("Discover author communities and key connectors in the collaboration network.")
    st.subheader("Author clusters dominating a venue (Louvain)")
    gds_scope = _scope_params()
    gds_venues = _call_api("/filter/distinct_venues", gds_scope if gds_scope else None)
    gds_options = [v["venue"] for v in (gds_venues or {}).get("results", [])] if gds_venues else []
    if gds_options:
        venue = st.selectbox("Venue (field proxy)", gds_options, index=0)
    else:
        venue = st.text_input("Venue (field proxy)", value="Neurocomputing")
    if st.button("Compute clusters"):
        data = _call_api("/author_clusters_by_venue", {"venue": venue, "top_k": 5})
        if data:
            clusters = data.get("clusters", [])
            total_papers = data.get("total_papers_in_venue", 0)
            if clusters:
                # Summary table for quick comparison
                table_rows: list[dict[str, Any]] = []
                for c in clusters:
                    table_rows.append(
                        {
                            "Rank": c.get("rank"),
                            "Cluster": c.get("cluster_label"),
                            "Authors": c.get("author_count"),
                            "Papers in venue": c.get("papers_in_venue"),
                            "Share of venue (%)": c.get("share_of_venue"),
                        }
                    )
                st.markdown("**Top clusters in this venue**")
                _table(table_rows, height=260)

                st.caption(
                    f"Total papers in venue: {total_papers:,}. Click a cluster below to see author names and a Neo4j query for visualization."
                )

                # Per-cluster details: authors and copyable Neo4j query
                for c in clusters:
                    rank = c.get("rank")
                    label = c.get("cluster_label")
                    with st.expander(f"{rank}. {label}", expanded=False):
                        st.markdown(
                            f"**Authors in cluster ({c.get('author_count', 0)}):**"
                        )
                        all_authors = c.get("all_authors", [])
                        if all_authors:
                            st.write(", ".join(all_authors))
                        else:
                            st.caption("No authors found for this cluster.")

                        st.markdown(
                            "**Neo4j query to visualize this cluster**  "
                            "[Open Neo4j Browser](http://localhost:7474)"
                        )
                        st.code(c.get("neo4j_query", ""), language="cypher")

            else:
                st.warning("No clusters found for this venue.")
            st.info(data["store_justification"])
            st.caption(data.get("note", ""))

    st.divider()
    st.subheader("Bridge authors (betweenness centrality)")
    if st.button("Compute bridge authors"):
        data = _call_api("/bridge_authors", {"limit": 20, **_scope_params()})
        if data:
            _table(data.get("results", []))
        st.info(data["store_justification"])
