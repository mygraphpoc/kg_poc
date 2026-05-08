"""src/ui/setup.py — Sidebar: connection status. Sample questions injected by chat.py per mode."""

import streamlit as st
from src import config, graphdb

_DOT = ('<span style="display:inline-block;width:8px;height:8px;'
        'border-radius:50%;background:{c};margin-right:6px;vertical-align:middle"></span>')

def _dot(ok: bool) -> str:
    return _DOT.format(c="#3fb950" if ok else "#f85149")


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### 🔷 KG-POC Assistant")
        st.markdown("---")

        missing = config.missing_keys()
        if missing:
            st.error(
                f"⚠️ {len(missing)} secret(s) missing.\n\n"
                "Add in **App Settings → Secrets**.\n\n"
                f"Missing: `{'`, `'.join(missing)}`"
            )
        else:
            token, gdb_err = graphdb.get_token()
            st.markdown(
                f"{_dot(bool(token))} GraphDB {'connected' if token else gdb_err[:40]}",
                unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} Databricks (token set)",       unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} Snowflake (credentials set)",  unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} LLM: Llama 3.3 70B",          unsafe_allow_html=True)

        # Only show generic samples when no mode is active
        # (mode-specific samples are injected by chat.py)
        if not st.session_state.get("explore_mode"):
            st.markdown("---")
            st.markdown("**Quick start**")
            for q in [
                "What are all the Gold tables?",
                "What KPIs are available?",
                "What was total revenue by channel?",
                "Which customers are at risk of churning?",
                "What data does the dim_customer table hold?",
            ]:
                if st.button(q, key=f"sq_{hash(q)}"):
                    st.session_state["pending"] = q
