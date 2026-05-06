"""
KG-POC Sales DWH Assistant
Streamlit entry point — configure via sidebar, then use the two tabs.

Tab 1  💬 Ask the DWH    — Graph-RAG Q&A: Source / SQL / Answer
Tab 2  🔷 Data Lineage   — Interactive knowledge graph from GraphDB
"""

import streamlit as st

st.set_page_config(
    page_title="KG-POC | Sales DWH Assistant",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state bootstrap ────────────────────────────────────────────────────
if "history" not in st.session_state:
    st.session_state["history"] = []
if "pending_q" not in st.session_state:
    st.session_state["pending_q"] = ""

# ── Shared CSS (dark theme, card styles, badges) ──────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family:'IBM Plex Sans',sans-serif; }

section[data-testid="stSidebar"]          { background:#0d1117;border-right:1px solid #21262d; }
section[data-testid="stSidebar"] *        { color:#c9d1d9 !important; }
section[data-testid="stSidebar"] h3       { color:#e6edf3 !important;font-size:1rem !important; }
section[data-testid="stSidebar"] .stButton button {
    background:#161b22;border:1px solid #30363d;color:#c9d1d9 !important;
    border-radius:6px;font-size:.78rem;text-align:left;width:100%;
    margin-bottom:3px;padding:6px 10px;white-space:normal;line-height:1.3;
}
section[data-testid="stSidebar"] .stButton button:hover {
    border-color:#58a6ff;color:#58a6ff !important;background:#1c2128;
}
.main .block-container { padding:1.5rem 2rem;max-width:1300px; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
from src.ui.setup import render_sidebar
render_sidebar()

with st.sidebar:
    st.markdown("---")
    if st.button("🗑 Clear conversation history"):
        st.session_state["history"] = []
        st.rerun()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_chat, tab_lineage = st.tabs(["💬  Ask the DWH", "🔷  Data Lineage"])

with tab_chat:
    from src.ui.chat import render as render_chat
    render_chat()

with tab_lineage:
    from src.ui.lineage_tab import render as render_lineage
    render_lineage()
