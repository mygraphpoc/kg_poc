"""src/ui/setup.py — Sidebar status panel."""
import streamlit as st
from src import config, graphdb
from src.retrieval import vs_retriever as vr
from src.pipeline.llm import get_endpoint_name

_DOT = '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{c};margin-right:6px;vertical-align:middle"></span>'

def _dot(ok):
    return _DOT.format(c="#3fb950" if ok else "#f85149")


def render_sidebar():
    with st.sidebar:
        st.markdown("### 🔷 KG-POC v2")
        st.markdown("---")

        missing = config.missing_keys()
        if missing:
            st.error(f"⚠️ **{len(missing)} secret(s) missing**\n\n"
                     f"Add in **App Settings → Secrets**:\n\n"
                     + "\n".join(f"- `{k}`" for k in missing))
            return

        # GraphDB
        token, gdb_err = graphdb.get_token()
        st.markdown(
            f"{_dot(bool(token))} GraphDB "
            f"{'connected ✓' if token else gdb_err[:40]}",
            unsafe_allow_html=True)

        # VS embedding
        if token:
            vec = vr.embed("test")
            st.markdown(
                f"{_dot(vec is not None)} VS Embeddings "
                f"{'OK ✓' if vec is not None else 'failed'}",
                unsafe_allow_html=True)

        # LLM
        ep      = get_endpoint_name()
        is_free = "llama" in ep.lower() or "mixtral" in ep.lower()
        tag     = " (free)" if is_free else " (paid)"
        st.markdown(f"{_dot(True)} LLM: `{ep}`{tag}", unsafe_allow_html=True)

        # Databricks
        host = config.get("DATABRICKS_HOST")
        st.markdown(f"{_dot(bool(host))} Databricks: `{host[:30] if host else 'not set'}`",
                    unsafe_allow_html=True)

        # Snowflake
        sf = config.get("SF_ACCOUNT")
        st.markdown(f"{_dot(bool(sf))} Snowflake: `{sf if sf else 'not set'}`",
                    unsafe_allow_html=True)

        st.markdown("---")
        st.caption(f"Retrieval: SPARQL × 0.6 + VS × 0.4")
