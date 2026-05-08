"""src/ui/chat.py — Chat tab with exploration mode selector."""

import time
import streamlit as st
from src import agent, config

_ICON_COLOR = {
    "🔍": "#58a6ff", "🗺️": "#58a6ff", "🎯": "#56d364",
    "📋": "#8b949e", "✍️": "#f0883e", "⚡": "#f9a825",
    "📊": "#56d364", "💬": "#bc8cff", "✅": "#56d364",
    "❌": "#f85149", "📖": "#58a6ff",
}

# ── Exploration modes ─────────────────────────────────────────────────────────
MODES = {
    "structural": {
        "icon":    "🔷",
        "title":   "Knowledge Graph & Lineage",
        "tagline": "Explore the warehouse structure",
        "desc":    "Browse tables, columns, KPIs, lineage, PII flags, OWL classes. "
                   "Answered directly from GraphDB — no SQL needed.",
        "color":   "#1f3a5f",
        "border":  "#58a6ff",
        "badge":   "#58a6ff",
        "examples": [
            "What are all the Gold tables?",
            "What KPIs are available?",
            "Which tables contain PII columns?",
            "Show cross-platform lineage",
            "What are all the Silver tables?",
            "Which tables are on Snowflake?",
        ],
        "hint": "dim / fact / gold / kpi / lineage / pii",
    },
    "kpi": {
        "icon":    "⭐",
        "title":   "KPI & Aggregated Data",
        "tagline": "Query Gold layer analytics",
        "desc":    "Revenue, margins, churn, promotions, supplier performance — "
                   "pre-aggregated KPI tables on Databricks and Snowflake.",
        "color":   "#2a2000",
        "border":  "#f9a825",
        "badge":   "#f9a825",
        "examples": [
            "What was total revenue by channel last quarter?",
            "Which customers are at risk of churning?",
            "What is the MoM revenue growth for credit card payments?",
            "Which suppliers have a return rate above 10%?",
            "Show executive summary for the last quarter",
            "What is the revenue by product over all months?",
        ],
        "hint": "Gold tables: agg_revenue, agg_customer_360, agg_supplier…",
    },
    "silver": {
        "icon":    "●",
        "title":   "Dimension & Fact Tables",
        "tagline": "Query Silver layer raw data",
        "desc":    "Explore cleaned dimension and fact tables — "
                   "customers, products, stores, sales transactions, returns.",
        "color":   "#141f2a",
        "border":  "#79afd1",
        "badge":   "#79afd1",
        "examples": [
            "What kind of data does the dim_customer table hold?",
            "Show me the columns in the fct_sales table",
            "What data does the product dimension have?",
            "Describe the sales fact table",
            "What fields does dim_store contain?",
            "Show me the structure of dim_employee",
        ],
        "hint": "Silver tables: dim_customer, fct_sales, dim_product…",
    },
}

# Mode → agent hint injected into the question context
_MODE_HINT = {
    "structural": "",                          # structural SPARQL path handles it
    "kpi":        "[Focus: Gold KPI aggregates] ",
    "silver":     "[Focus: Silver dimension/fact tables] ",
}


# ── Welcome screen ────────────────────────────────────────────────────────────

def _render_welcome() -> None:
    st.markdown("""
<div style="text-align:center;padding:20px 0 10px">
  <div style="font-size:2rem;font-weight:700;color:#e6edf3">Sales DWH Assistant</div>
  <div style="font-size:1rem;color:#8b949e;margin-top:6px">
    What would you like to explore today?
  </div>
</div>""", unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)

    for col, (mode_key, mode) in zip([c1, c2, c3], MODES.items()):
        with col:
            # Card HTML
            st.markdown(f"""
<div style="background:{mode['color']};border:1px solid {mode['border']};
     border-radius:12px;padding:20px 16px;min-height:180px">
  <div style="font-size:1.6rem;margin-bottom:6px">{mode['icon']}</div>
  <div style="font-size:1rem;font-weight:700;color:#e6edf3;margin-bottom:4px">{mode['title']}</div>
  <div style="font-size:0.8rem;color:{mode['badge']};margin-bottom:10px;font-weight:600">{mode['tagline']}</div>
  <div style="font-size:0.78rem;color:#8b949e;line-height:1.5">{mode['desc']}</div>
</div>""", unsafe_allow_html=True)
            st.markdown("<br/>", unsafe_allow_html=True)
            if st.button(f"Explore →", key=f"mode_{mode_key}", use_container_width=True):
                st.session_state["explore_mode"] = mode_key
                st.rerun()

    # Hint: or just type a question directly
    st.markdown("""
<div style="text-align:center;margin-top:8px;color:#8b949e;font-size:0.8rem">
  Or just type your question below — the agent will figure out the right layer automatically.
</div>""", unsafe_allow_html=True)


# ── Mode banner (shown once a mode is active) ─────────────────────────────────

def _render_mode_banner(mode_key: str) -> None:
    mode = MODES[mode_key]
    c1, c2 = st.columns([9, 1])
    with c1:
        st.markdown(f"""
<div style="background:{mode['color']};border:1px solid {mode['border']};
     border-radius:8px;padding:8px 14px;display:flex;align-items:center;gap:10px">
  <span style="font-size:1.1rem">{mode['icon']}</span>
  <span style="font-weight:600;color:#e6edf3">{mode['title']}</span>
  <span style="color:#8b949e;font-size:0.78rem">— {mode['hint']}</span>
</div>""", unsafe_allow_html=True)
    with c2:
        if st.button("✕", key="clear_mode", help="Change exploration mode"):
            st.session_state["explore_mode"] = None
            st.session_state["messages"] = []
            st.rerun()


# ── Sample questions for active mode ─────────────────────────────────────────

def _render_mode_samples(mode_key: str) -> None:
    mode = MODES[mode_key]
    with st.sidebar:
        st.markdown(f"**{mode['icon']} {mode['title']} examples**")
        for q in mode["examples"]:
            if st.button(q, key=f"ms_{hash(q)}"):
                st.session_state["pending"] = q


# ── Main render ───────────────────────────────────────────────────────────────

def render() -> None:
    missing = config.missing_keys()
    if missing:
        st.markdown("## Sales DWH Assistant")
        st.warning(
            f"⚠️ **{len(missing)} secret(s) not configured.** "
            "Add them in **App Settings → Secrets**.\n\n"
            f"Missing: `{'`, `'.join(missing)}`"
        )
        return

    mode_key  = st.session_state.get("explore_mode")
    all_msgs  = st.session_state.get("messages", [])
    user_msgs = [m for m in all_msgs if m["role"] == "user"]

    # ── Show welcome screen if no mode chosen and no messages yet ────────────
    if not mode_key and not all_msgs:
        _render_welcome()
        # Still allow free-text even from welcome screen
        question = st.chat_input("Or type a question directly…")
        if question:
            st.session_state["messages"] = []
            _process(question, mode_key=None)
        return

    # ── Active session ────────────────────────────────────────────────────────
    st.markdown("## Sales DWH Assistant")

    # Mode banner + inject mode-specific sidebar samples
    if mode_key:
        _render_mode_banner(mode_key)
        _render_mode_samples(mode_key)
    else:
        st.caption("Ask any business question about your retail data warehouse.")

    st.markdown("<br/>", unsafe_allow_html=True)

    # Stats
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Questions",  len(user_msgs))
    c2.metric("Databricks", sum(1 for m in all_msgs if m.get("platform") == "databricks"))
    c3.metric("Snowflake",  sum(1 for m in all_msgs if m.get("platform") == "snowflake"))
    c4.metric("Structural", sum(1 for m in all_msgs if m.get("is_structural")))

    # Conversation history
    for msg in all_msgs:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                st.markdown(msg["content"])
                continue
            steps = msg.get("steps", [])
            if steps:
                with st.expander("🔄 Pipeline steps", expanded=False):
                    for s in steps:
                        color = _ICON_COLOR.get(s["icon"], "#8b949e")
                        st.markdown(
                            f'<span style="color:{color}">{s["icon"]}</span> '
                            f'<span style="font-size:0.85rem;color:#c9d1d9">{s["msg"]}</span>',
                            unsafe_allow_html=True)
            st.markdown(msg["content"])
            rows = msg.get("result_rows", [])
            cols = msg.get("result_cols", [])
            if rows and cols:
                import pandas as pd
                st.dataframe(pd.DataFrame(rows, columns=cols),
                             use_container_width=True, hide_index=True)
            if msg.get("sql"):
                with st.expander("🔍 SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            if msg.get("source") and not msg.get("is_structural"):
                co1, co2, co3 = st.columns(3)
                co1.caption(f"📁 `{msg['source'].split('→')[-1].strip()}`")
                co2.caption(f"⚡ {msg.get('elapsed','—')}s")
                plat = msg.get("platform","")
                co3.caption("❄️ Snowflake" if plat == "snowflake" else "🟠 Databricks")

    # Input
    question = None
    if st.session_state.get("pending"):
        question = st.session_state.pop("pending")
    user_input = st.chat_input("Ask a question…")
    if user_input:
        question = user_input

    if not question:
        if not all_msgs and mode_key:
            mode = MODES[mode_key]
            st.info(f"{mode['icon']} Ask about **{mode['title'].lower()}** or pick an example from the sidebar.")
        return

    _process(question, mode_key)


# ── Process a question ────────────────────────────────────────────────────────

def _process(question: str, mode_key: str | None) -> None:
    # Prefix question with mode hint so agent biases its table search
    hint   = _MODE_HINT.get(mode_key or "", "")
    q_with_hint = hint + question

    st.session_state["messages"].append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        step_lines: list = []
        step_container = st.empty()

        def on_step(icon: str, msg: str):
            step_lines.append({"icon": icon, "msg": msg})
            lines_html = "<br/>".join(
                f'<span style="color:{_ICON_COLOR.get(s["icon"],"#8b949e")}">{s["icon"]}</span> '
                f'<span style="font-size:0.85rem;color:#c9d1d9">{s["msg"]}</span>'
                for s in step_lines
            )
            step_container.markdown(
                f'<div style="background:#161b22;border:1px solid #30363d;'
                f'border-radius:8px;padding:10px 14px;margin-bottom:8px;line-height:2">'
                f'{lines_html}</div>',
                unsafe_allow_html=True)

        t0 = time.time()
        try:
            result = agent.run(q_with_hint, on_step=on_step)
        except Exception as exc:
            result = agent._err(str(exc))
        elapsed = round(time.time() - t0, 1)

        step_container.empty()
        steps = result.get("steps", step_lines)
        if steps:
            with st.expander("🔄 Pipeline steps", expanded=False):
                for s in steps:
                    color = _ICON_COLOR.get(s["icon"], "#8b949e")
                    st.markdown(
                        f'<span style="color:{color}">{s["icon"]}</span> '
                        f'<span style="font-size:0.85rem;color:#c9d1d9">{s["msg"]}</span>',
                        unsafe_allow_html=True)

        err    = result.get("error", "")
        answer = result.get("answer", "") or (f"⚠️ {err}" if err else "—")
        rows   = result.get("result_rows", [])
        cols   = result.get("result_cols", [])

        st.markdown(answer)
        if rows and cols and not err:
            import pandas as pd
            st.dataframe(pd.DataFrame(rows, columns=cols),
                         use_container_width=True, hide_index=True)

        sql = result.get("sql", "")
        if sql:
            with st.expander("🔍 SQL generated", expanded=False):
                st.code(sql, language="sql")

        source = result.get("source", "")
        if source and not result.get("is_structural"):
            co1, co2, co3 = st.columns(3)
            co1.caption(f"📁 `{source.split('→')[-1].strip()}`")
            co2.caption(f"⚡ {elapsed}s")
            plat = result.get("platform", "")
            co3.caption("❄️ Snowflake" if plat == "snowflake" else "🟠 Databricks")

    st.session_state["messages"].append({
        "role": "assistant", "content": answer,
        "sql": sql, "result_rows": rows, "result_cols": cols,
        "source": source, "platform": result.get("platform",""),
        "is_structural": result.get("is_structural", False),
        "elapsed": elapsed, "steps": steps,
    })
