"""src/ui/lineage_tab.py — Tab 2: Data Lineage Knowledge Graph."""

import streamlit as st
import streamlit.components.v1 as components
from src import config, graphdb, lineage


def render() -> None:
    st.markdown("## Data Lineage Knowledge Graph")
    st.markdown(
        "Interactive view of all tables and their lineage relationships "
        "across Databricks and Snowflake. Nodes are sized by number of connections; "
        "hover for metadata."
    )

    if not config.is_configured():
        st.info("⚙️ Fill in your credentials in the sidebar to get started.")
        return

    token, err = graphdb.get_token()
    if not token:
        st.error(f"GraphDB connection required for lineage view: {err}")
        return

    with st.spinner("Loading lineage from GraphDB…"):
        nodes, edges = lineage.load(token)

    if not nodes:
        st.warning("No table metadata found in GraphDB. "
                   "Run notebooks 10–11 to generate and upload the RDF graph.")
        return

    # ── Filters ───────────────────────────────────────────────────────────
    layers    = sorted({(n.get("layer")    or "").lower() for n in nodes if n.get("layer")})
    platforms = sorted({(n.get("platform") or "").lower() for n in nodes if n.get("platform")})
    domains   = sorted({(n.get("domain")   or "").capitalize() for n in nodes if n.get("domain")})

    f1, f2, f3, f4 = st.columns([2, 2, 2, 3])
    with f1:
        sel_layers = st.multiselect("Layer", layers, default=layers)
    with f2:
        sel_platforms = st.multiselect("Platform", platforms, default=platforms)
    with f3:
        sel_domains = st.multiselect("Domain", domains, default=[])
    with f4:
        search = st.text_input("🔍 Search tables", placeholder="e.g. agg_revenue")

    # Legend
    st.markdown("""
<div style="display:flex;gap:16px;flex-wrap:wrap;margin:6px 0 2px;
            font-size:.78rem;color:#8b949e">
  <span>🔻 Bronze (raw)</span>
  <span>⬤ Silver (dim / fact)</span>
  <span>★ Gold (KPI agg)</span>
  <span>■ Staging</span>
  <span style="margin-left:12px">🟠 Databricks border</span>
  <span>🔷 Snowflake border</span>
  <span>Node size ∝ connections</span>
</div>""", unsafe_allow_html=True)

    # Summary counts
    visible_count = sum(
        1 for n in nodes
        if (not sel_layers    or (n.get("layer")    or "").lower() in sel_layers)
        and (not sel_platforms or (n.get("platform") or "").lower() in sel_platforms)
        and (not sel_domains   or (n.get("domain")   or "").capitalize() in sel_domains)
        and (not search        or search.lower() in (n.get("tname","")).lower())
    )
    st.caption(f"Showing **{visible_count}** of {len(nodes)} tables · {len(edges)} lineage edges")

    # ── Graph ─────────────────────────────────────────────────────────────
    html = lineage.build_graph(
        nodes, edges,
        sel_layers, sel_platforms,
        [d.lower() for d in sel_domains],
        search,
    )
    components.html(html, height=660, scrolling=False)

    # ── Table Inspector ───────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📋 Table Inspector")
    st.caption("Select a table to explore its columns, KPIs, and lineage details.")

    all_tables = sorted({n.get("tname","") for n in nodes if n.get("tname")})
    selected   = st.selectbox(
        "Table",
        options=[""] + all_tables,
        format_func=lambda x: "— choose a table —" if x == "" else x,
        label_visibility="collapsed",
    )

    if not selected:
        return

    with st.spinner(f"Loading metadata for {selected}…"):
        detail = lineage.fetch_detail(token, selected)
        meta   = next((n for n in nodes if n.get("tname","") == selected), {})

    plat   = (meta.get("platform") or "").lower()
    layer  = (meta.get("layer")    or "").lower()
    domain = (meta.get("domain")   or "—").capitalize()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Table",    selected)
    m2.metric("Platform", "❄️ Snowflake" if plat == "snowflake" else "🟠 Databricks" if plat else plat)
    m3.metric("Layer",    layer.capitalize())
    m4.metric("Domain",   domain)

    col_left, col_right = st.columns(2)

    # Columns
    with col_left:
        st.markdown("**Columns**")
        cols = detail.get("columns", [])
        if cols:
            rows = []
            for c in cols:
                flags = []
                if c.get("isPII"):  flags.append("🔒 PII")
                if c.get("isKPI"):  flags.append("📊 KPI")
                rows.append({"Column": c.get("colName",""), "Flags": " ".join(flags) or "—"})
            st.dataframe(rows, use_container_width=True, hide_index=True, height=300)
        else:
            st.info("No column metadata registered in the graph for this table.")

    # KPIs
    with col_right:
        st.markdown("**KPIs**")
        kpis = detail.get("kpis", [])
        if kpis:
            for k in kpis:
                with st.expander(k.get("kpiName","KPI"), expanded=False):
                    if k.get("direction"):  st.write(f"Direction: {k['direction']}")
                    if k.get("benchmark"):  st.write(f"Benchmark: {k['benchmark']}")
        else:
            st.info("No KPIs registered for this table.")

    # Lineage arrows
    st.markdown("**Lineage**")
    la, lb = st.columns(2)
    with la:
        ups = detail.get("upstream", [])
        st.markdown("*Upstream — feeds into this table*")
        if ups:
            for u in ups:
                st.markdown(f"← `{u.get('srcName','')}` "
                            f"({u.get('srcPlatform','')}) "
                            f"via *{u.get('transformType','?')}*")
        else:
            st.caption("No upstream tables registered.")
    with lb:
        downs = detail.get("downstream", [])
        st.markdown("*Downstream — this table feeds into*")
        if downs:
            for d in downs:
                st.markdown(f"→ `{d.get('tgtName','')}` ({d.get('tgtPlatform','')})")
        else:
            st.caption("No downstream tables registered.")
