"""src/ui/chat.py — Tab 1: Ask the DWH."""

import time
import streamlit as st
from src import agent, config

CSS = """
<style>
.answer-card {
    background:#161b22;border:1px solid #30363d;border-radius:10px;
    padding:18px 22px;margin-bottom:18px;
}
.q-label { font-size:.72rem;font-weight:600;letter-spacing:.08em;color:#8b949e;
           text-transform:uppercase;margin-bottom:6px; }
.q-text  { font-size:1.05rem;font-weight:500;color:#e6edf3;margin-bottom:14px;
           border-bottom:1px solid #21262d;padding-bottom:10px; }
.meta-row{ display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap; }
.badge   { display:inline-flex;align-items:center;gap:6px;padding:3px 10px;
           border-radius:20px;font-size:.75rem;font-weight:500;font-family:monospace; }
.bdg-dbx { background:#1f2d1f;color:#56d364;border:1px solid #388a3a; }
.bdg-sf  { background:#162032;color:#58a6ff;border:1px solid #1f6feb; }
.bdg-gdb { background:#2d1b4e;color:#bc8cff;border:1px solid #8957e5; }
.bdg-tbl { background:#1f2328;color:#f0883e;border:1px solid #d1571a; }
.sec-lbl { font-size:.68rem;font-weight:600;letter-spacing:.1em;text-transform:uppercase;
           color:#8b949e;margin:12px 0 4px; }
.ans-text{ color:#c9d1d9;font-size:.93rem;line-height:1.65;background:#0d1117;
           border-radius:6px;padding:12px 14px;border-left:3px solid #58a6ff; }
.stat    { background:#161b22;border:1px solid #30363d;border-radius:6px;
           padding:10px 18px;text-align:center; }
.stat .n { font-size:1.4rem;font-weight:600;color:#58a6ff; }
.stat .l { font-size:.7rem;color:#8b949e;text-transform:uppercase;letter-spacing:.05em; }
</style>
"""


def render() -> None:
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown("## Sales DWH Assistant")
    st.markdown(
        "Ask any question about the warehouse. "
        "The agent queries GraphDB, vector search, Databricks, and Snowflake to answer."
    )

    if not config.is_configured():
        missing = config.missing_keys()
        st.warning(
            f"⚠️ **{len(missing)} secret(s) not configured.** "
            "Add them in **App Settings → Secrets** on Streamlit Cloud, "
            "or in `.streamlit/secrets.toml` for local runs.\n\n"
            f"Missing: `{'`, `'.join(missing)}`"
        )
        return

    # Stats strip
    h  = st.session_state.get("history", [])
    c1, c2, c3, c4 = st.columns(4)
    for col, num, lbl in [
        (c1, len(h),                                          "Questions"),
        (c2, sum(1 for x in h if x.get("platform") == "databricks"), "Databricks"),
        (c3, sum(1 for x in h if x.get("platform") == "snowflake"),  "Snowflake"),
        (c4, sum(1 for x in h if x.get("is_structural")),            "Structural"),
    ]:
        col.markdown(
            f'<div class="stat"><div class="n">{num}</div>'
            f'<div class="l">{lbl}</div></div>',
            unsafe_allow_html=True)

    st.markdown("---")

    # Question input
    with st.form("ask_form", clear_on_submit=True):
        pending  = st.session_state.pop("pending_q", "") or ""
        question = st.text_input(
            "Question",
            value=pending,
            placeholder="e.g. What was total revenue by channel last quarter?",
            label_visibility="collapsed")
        submitted = st.form_submit_button("Ask →", type="primary")

    if submitted and question.strip():
        with st.spinner("Querying knowledge graph, vector store, and data warehouse…"):
            t0     = time.time()
            result = agent.run(question.strip())
            result["question"] = question.strip()
            result["elapsed"]  = round(time.time() - t0, 1)
        if "history" not in st.session_state:
            st.session_state["history"] = []
        st.session_state["history"].insert(0, result)
        st.rerun()

    # History cards
    for item in st.session_state.get("history", []):
        plat = item.get("platform", "")
        if plat == "snowflake":
            badge_cls, ico = "bdg-sf",  "❄️"
        elif plat == "databricks":
            badge_cls, ico = "bdg-dbx", "🟠"
        else:
            badge_cls, ico = "bdg-gdb", "🔷"

        src   = item.get("source", "")
        sql   = item.get("sql", "").strip()
        ans   = item.get("answer", "")
        secs  = item.get("elapsed", "")
        table = src.split("→")[-1].strip() if "→" in src else ""
        err   = item.get("error", "")

        st.markdown(f"""
<div class="answer-card">
  <div class="q-label">Question · {secs}s</div>
  <div class="q-text">{item['question']}</div>
  <div class="meta-row">
    <span class="badge {badge_cls}">{ico} {plat.capitalize() or 'GraphDB'}</span>
    {"<span class='badge bdg-tbl'>📋 " + table + "</span>" if table else ""}
  </div>
""", unsafe_allow_html=True)

        if err:
            st.error(f"Error: {err}")
        elif sql:
            with st.expander("SQL", expanded=False):
                st.code(sql, language="sql")

        st.markdown(
            f'<div class="sec-lbl">Answer</div>'
            f'<div class="ans-text">{ans}</div></div>',
            unsafe_allow_html=True)
