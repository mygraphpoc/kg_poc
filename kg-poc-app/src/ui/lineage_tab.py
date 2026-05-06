"""src/ui/lineage_tab.py — Tab 2: Data Lineage Knowledge Graph."""

import streamlit as st
from src import config, graphdb, lineage


def render() -> None:
    st.markdown("## Data Lineage Knowledge Graph")
    st.caption(
        "Interactive graph — click any table to see columns, KPIs and lineage. "
        "Use the top bar to filter by layer or platform."
    )

    if not config.is_configured():
        missing = config.missing_keys()
        st.warning(
            f"⚠️ **{len(missing)} secret(s) not configured.** "
            "Add them in **App Settings → Secrets** on Streamlit Cloud.\n\n"
            f"Missing: `{'`, `'.join(missing)}`"
        )
        return

    token, err = graphdb.get_token()
    if not token:
        st.error(f"GraphDB connection required: {err}")
        return

    with st.spinner("Loading metadata from GraphDB…"):
        data = lineage.load_full(token)

    if not data.get("nodes"):
        st.warning(
            "No table metadata found in GraphDB. "
            "Run notebooks 10–11 to generate and upload the RDF graph."
        )
        return

    n_nodes = len(data["nodes"])
    n_edges = len(data["edges"])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tables",  n_nodes)
    c2.metric("Edges",   n_edges)
    c3.metric("Layers",  len(data["layers"]))
    c4.metric("Domains", len(data["domains"]))

    # Full-width graph — 800px tall self-contained HTML
    html = lineage.build_rich_html(data)
    try:
        st.html(html)
    except AttributeError:
        import streamlit.components.v1 as components
        components.html(html, height=820, scrolling=False)
