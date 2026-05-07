"""src/ui/setup.py — Sidebar: connection status + sample questions.

Credentials are configured in Streamlit Cloud via Settings → Secrets
(or via environment variables for local runs). No credential UI here.
"""

import streamlit as st
from src import config, graphdb

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

_DOT = ('<span style="display:inline-block;width:8px;height:8px;'
        'border-radius:50%;background:{c};margin-right:6px;'
        'vertical-align:middle"></span>')


def _dot(ok: bool) -> str:
    return _DOT.format(c="#3fb950" if ok else "#f85149")


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### 🔷 KG-POC Assistant")
        st.markdown("---")

        missing = config.missing_keys()
        if missing:
            st.error(
                f"\u26a0\ufe0f {len(missing)} secret(s) missing.\n\n"
                "Add them in **App Settings \u2192 Secrets** on Streamlit Cloud, "
                "or in `.streamlit/secrets.toml` for local runs.\n\n"
                f"Missing: `{'`, `'.join(missing)}`"
            )
        else:
            token, gdb_err = graphdb.get_token()
            st.markdown(
                f"{_dot(bool(token))} GraphDB "
                f"{'connected' if token else gdb_err[:50]}",
                unsafe_allow_html=True,
            )
            st.markdown(f"{_dot(True)} Databricks (token set)", unsafe_allow_html=True)
            st.markdown(f"{_dot(True)} Snowflake (credentials set)", unsafe_allow_html=True)
            st.markdown(
                f"{_dot(True)} LLM: Databricks Meta-Llama 3.3 70B",
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown("**Sample questions**")
        for q in SAMPLES:
            if st.button(q, key=f"sq_{hash(q)}"):
                st.session_state["pending"] = q
