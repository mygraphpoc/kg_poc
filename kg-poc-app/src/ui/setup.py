"""src/ui/setup.py — Sidebar status panel."""
import streamlit as st
from src import config, graphdb

_DOT = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{c};margin-right:6px;vertical-align:middle"></span>'

def _dot(ok): return _DOT.format(c="#3fb950" if ok else "#f85149")

def render_sidebar():
    with st.sidebar:
        st.markdown("### 🔷 KG-POC Assistant")
        st.markdown("---")
        missing = config.missing_keys()
        if missing:
            st.error(f"⚠️ {len(missing)} secret(s) missing.\n\nAdd in **App Settings → Secrets**.\n\nMissing: `{'`, `'.join(missing)}`")
        else:
            token, gdb_err = graphdb.get_token()
            st.markdown(f"{_dot(bool(token))} GraphDB {'connected' if token else gdb_err[:40]}", unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} Databricks (token set)", unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} Snowflake (credentials set)", unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} LLM: Llama 3.3 70B", unsafe_allow_html=True)
