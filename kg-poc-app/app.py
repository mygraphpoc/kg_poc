"""KG-POC Sales DWH Assistant — Streamlit entry point."""

import os
import streamlit as st

st.set_page_config(
    page_title="KG-POC | Sales DWH Assistant",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Read secrets / env vars ───────────────────────────────────────────────────
def _cfg(key: str, default: str = "") -> str:
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)

# ── Set Databricks env vars at MODULE LEVEL before any @st.cache_resource ────
_dbx_host = _cfg("DATABRICKS_HOST")
if _dbx_host and not _dbx_host.startswith("http"):
    _dbx_host = f"https://{_dbx_host}"
os.environ["DATABRICKS_HOST"]  = _dbx_host
os.environ["DATABRICKS_TOKEN"] = _cfg("DATABRICKS_TOKEN")

# ── Session state ─────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "pending" not in st.session_state:
    st.session_state["pending"] = None

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
html, body, [class*="css"] { font-family:'Segoe UI',system-ui,sans-serif; }
section[data-testid="stSidebar"] { background:#0d1117; border-right:1px solid #21262d; }
section[data-testid="stSidebar"] * { color:#c9d1d9 !important; }
section[data-testid="stSidebar"] .stButton button {
    background:#161b22; border:1px solid #30363d; color:#c9d1d9 !important;
    border-radius:6px; font-size:.78rem; text-align:left; width:100%;
    margin-bottom:3px; padding:6px 10px; white-space:normal; line-height:1.3;
}
section[data-testid="stSidebar"] .stButton button:hover {
    border-color:#58a6ff; color:#58a6ff !important; background:#1c2128;
}
.main .block-container { padding:1.5rem 2rem; max-width:1200px; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
from src.ui.setup import render_sidebar
render_sidebar()

with st.sidebar:
    st.markdown("---")
    if st.button("🗑 Clear conversation"):
        st.session_state["messages"] = []
        st.rerun()

# ── Main ──────────────────────────────────────────────────────────────────────
from src.ui.chat import render as render_chat
render_chat()
