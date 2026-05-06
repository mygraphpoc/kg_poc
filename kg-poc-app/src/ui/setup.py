"""src/ui/setup.py — Sidebar credential setup wizard."""

import streamlit as st
from src import config, graphdb


def render_sidebar() -> None:
    """Draw the sidebar. If credentials are incomplete, show a setup form."""
    with st.sidebar:
        st.markdown("### 🔷 KG-POC Assistant")
        st.markdown("---")

        if not config.is_configured():
            _render_setup_form()
        else:
            _render_status_panel()

        st.markdown("---")
        st.markdown("**Quick questions**")
        _render_sample_questions()


# ─── Setup form ───────────────────────────────────────────────────────────────

def _render_setup_form() -> None:
    st.warning("⚙️ Configure your connections to get started.")

    with st.expander("🔷 GraphDB", expanded=True):
        url  = st.text_input("Base URL", value=config.get("GRAPHDB_BASE_URL"),
                             placeholder="https://…sandbox.graphwise.ai", key="cfg_gdb_url")
        user = st.text_input("Username", value=config.get("GRAPHDB_USER"),
                             placeholder="user@email.com", key="cfg_gdb_user")
        pw   = st.text_input("Password", type="password", key="cfg_gdb_pw")
        repo = st.text_input("Repository", value=config.get("GRAPHDB_REPO"),
                             key="cfg_gdb_repo")

    with st.expander("🟠 Databricks", expanded=False):
        host = st.text_input("Workspace host",
                             value=config.get("DATABRICKS_HOST"),
                             placeholder="workspace.azuredatabricks.net", key="cfg_dbx_host")
        tok  = st.text_input("Personal access token", type="password", key="cfg_dbx_tok")
        wh   = st.text_input("SQL Warehouse HTTP path",
                             value=config.get("SQL_WAREHOUSE_HTTP"),
                             placeholder="/sql/1.0/warehouses/…", key="cfg_dbx_wh")
        vs   = st.text_input("VS Endpoint name",
                             value=config.get("VS_ENDPOINT_NAME"), key="cfg_vs_ep")
        idx  = st.text_input("Vector Index name",
                             value=config.get("INDEX_NAME"), key="cfg_vs_idx")

    with st.expander("❄️ Snowflake", expanded=False):
        sf_acc = st.text_input("Account ID",  value=config.get("SF_ACCOUNT"),  key="cfg_sf_acc")
        sf_usr = st.text_input("Username",    value=config.get("SF_USER"),     key="cfg_sf_usr")
        sf_pw  = st.text_input("Password", type="password",                    key="cfg_sf_pw")
        sf_db  = st.text_input("Database",    value=config.get("SF_DATABASE"), key="cfg_sf_db")
        sf_wh  = st.text_input("Warehouse",   value=config.get("SF_WAREHOUSE"),key="cfg_sf_wh")

    with st.expander("🤖 Anthropic", expanded=False):
        ant = st.text_input("API Key", type="password", key="cfg_ant_key")

    if st.button("✅ Save & Connect", use_container_width=True, type="primary"):
        values = {
            "GRAPHDB_BASE_URL":   url,  "GRAPHDB_USER": user,
            "GRAPHDB_PASSWORD":   pw,   "GRAPHDB_REPO": repo,
            "DATABRICKS_HOST":    host, "DATABRICKS_TOKEN": tok,
            "SQL_WAREHOUSE_HTTP": wh,   "VS_ENDPOINT_NAME": vs,
            "INDEX_NAME":         idx,
            "SF_ACCOUNT": sf_acc, "SF_USER": sf_usr,
            "SF_PASSWORD": sf_pw, "SF_DATABASE": sf_db, "SF_WAREHOUSE": sf_wh,
            "ANTHROPIC_API_KEY":  ant,
        }
        config.save_to_session(values)
        # Bust cached connections so they re-init with new creds
        st.cache_resource.clear()
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
<div style="font-size:0.72rem;color:#8b949e;margin-top:10px;line-height:1.5">
ℹ️ Credentials live in your browser session only.<br/>
For permanent storage, add them as <a href="https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management"
style="color:#58a6ff">Streamlit Cloud Secrets</a>.
</div>""", unsafe_allow_html=True)


# ─── Status panel (after configuration) ──────────────────────────────────────

def _render_status_panel() -> None:
    # Test GraphDB
    token, err = graphdb.get_token()
    dot = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{};margin-right:6px"></span>'
    ok_dot  = dot.format("#3fb950")
    err_dot = dot.format("#f85149")

    st.markdown(
        f'{ok_dot if token else err_dot}GraphDB {"✓" if token else ("⚠ " + str(err)[:40])}',
        unsafe_allow_html=True)
    st.markdown(
        f'{ok_dot}Databricks (config present)',
        unsafe_allow_html=True)
    st.markdown(
        f'{ok_dot}Snowflake (config present)',
        unsafe_allow_html=True)

    if st.button("⚙️ Edit credentials", use_container_width=True):
        # Clear session config so the form reappears
        if "_cfg" in st.session_state:
            del st.session_state["_cfg"]
        st.cache_resource.clear()
        st.cache_data.clear()
        st.rerun()


# ─── Sample questions ─────────────────────────────────────────────────────────

SAMPLES = [
    "What was total revenue by channel last quarter?",
    "Which product categories have the highest gross margin?",
    "Which customers are at risk of churning?",
    "Show top 5 employees by commission earned this year",
    "What is the MoM revenue growth for credit card payments?",
    "Which suppliers have a return rate above 10 percent?",
    "What is the geographic revenue breakdown by state?",
    "Show executive summary for the last quarter",
    "Which customer segments have the highest average order value?",
    "What are all the Gold tables?",
    "What are all the KPIs available?",
    "Show cross-platform lineage between Databricks and Snowflake",
]


def _render_sample_questions() -> None:
    for q in SAMPLES:
        if st.button(q, key=f"sq_{hash(q)}"):
            st.session_state["pending_q"] = q
