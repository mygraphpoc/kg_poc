"""KG-POC v2 — Hybrid Graph-RAG (SPARQL + Vector Search) Sales DWH Assistant."""

import os
import sys
import pathlib

# ── PATH FIX — must be first, before any src import ──────────────────────────
_HERE = pathlib.Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
os.chdir(str(_HERE))

import streamlit as st

st.set_page_config(
    page_title="KG-POC v2 | Sales DWH Assistant",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config helper ─────────────────────────────────────────────────────────────
def _cfg(key, default=""):
    try:    return st.secrets[key]
    except: return os.getenv(key, default)

# ── Databricks env vars at MODULE LEVEL (required by Databricks SDK) ──────────
_h = _cfg("DATABRICKS_HOST")
os.environ["DATABRICKS_HOST"]  = f"https://{_h}" if _h and not _h.startswith("http") else (_h or "")
os.environ["DATABRICKS_TOKEN"] = _cfg("DATABRICKS_TOKEN")

# ── Session state ─────────────────────────────────────────────────────────────
for _k, _v in [("messages",[]),("pending",None),("test_log",[]),("test_results",[]),("ttyg_messages",[]),("ttyg_chat_id",None),("ttyg_agent_id",None)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
html,body,[class*="css"]{font-family:'Segoe UI',system-ui,sans-serif;}
section[data-testid="stSidebar"]{background:#0d1117;border-right:1px solid #21262d;}
section[data-testid="stSidebar"] *{color:#c9d1d9 !important;}
section[data-testid="stSidebar"] .stButton button{
  background:#161b22;border:1px solid #30363d;color:#c9d1d9 !important;
  border-radius:6px;font-size:.78rem;text-align:left;width:100%;
  margin-bottom:3px;padding:6px 10px;white-space:normal;line-height:1.3;}
section[data-testid="stSidebar"] .stButton button:hover{
  border-color:#58a6ff;color:#58a6ff !important;background:#1c2128;}
.main .block-container{padding:1.5rem 2rem;max-width:1300px;}
</style>""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
from src.ui.setup import render_sidebar
render_sidebar()

with st.sidebar:
    st.markdown("---")
    if st.button("🗑 Clear conversation"):
        st.session_state["messages"] = []
        st.rerun()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_chat, tab_ttyg, tab_test = st.tabs(["💬  Ask the DWH", "🧠  Talk to Graph", "🧪  Test Suite"])

with tab_chat:
    from src.ui.chat import render as render_chat
    render_chat()

with tab_ttyg:
    from src.ui.ttyg_tab import render as render_ttyg
    render_ttyg()

with tab_ttyg:
    from src.ui.ttyg_tab import render as render_ttyg
    render_ttyg()

with tab_test:
    from src.ui.test_tab import render as render_test
    render_test()
