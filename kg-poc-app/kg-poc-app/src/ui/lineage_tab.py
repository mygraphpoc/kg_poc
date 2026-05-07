"""src/ui/lineage_tab.py — Tab 2: Data Lineage."""

import streamlit as st
from src import config, graphdb, lineage


def render() -> None:
    st.markdown("## Data Lineage")
    st.caption("Click any node to highlight its neighbours and see metadata. Use the toolbar to filter by layer or platform.")

    missing = config.missing_keys()
    if missing:
        st.warning(
            f"⚠️ **{len(missing)} secret(s) not configured.** "
            "Add them in **App Settings → Secrets**.\n\n"
            f"Missing: `{'`, `'.join(missing)}`"
        )
        return

    token, err = graphdb.get_token()
    if not token:
        st.error(f"GraphDB connection required: {err}")
        return

    with st.spinner("Loading lineage from GraphDB…"):
        data = lineage.load_full(token)

    if not data.get("nodes"):
        st.warning("No table metadata found in GraphDB. Run notebooks 10–11 to upload the RDF graph.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tables",    len(data["nodes"]))
    c2.metric("Edges",     len(data["edges"]))
    c3.metric("Platforms", len(data["platforms"]))
    c4.metric("Domains",   len(data["domains"]))

    html = lineage.build_rich_html(data)

    # st.html renders inline without an iframe — works reliably in Streamlit Cloud
    try:
        st.html(html)
    except AttributeError:
        # Fallback for Streamlit < 1.31
        import streamlit.components.v1 as components
        components.html(html, height=820, scrolling=False)
