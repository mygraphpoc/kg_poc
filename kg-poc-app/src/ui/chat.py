"""src/ui/chat.py — Chat tab, no welcome screen."""

import time
import streamlit as st
from src import agent, config

_ICON_COLOR = {
    "🔍": "#58a6ff", "🗺️": "#58a6ff", "🎯": "#56d364",
    "📋": "#8b949e", "✍️": "#f0883e", "⚡": "#f9a825",
    "📊": "#56d364", "💬": "#bc8cff", "✅": "#56d364",
    "❌": "#f85149", "📖": "#58a6ff",
}

SAMPLES = [
    "What was total revenue by channel last quarter?",
    "What is the revenue by product over all months?",
    "Which customers are at risk of churning?",
    "Which suppliers have a return rate above 10%?",
    "What are all the Gold tables?",
    "What KPIs are available?",
    "What kind of data does the dim_customer table hold?",
    "Show executive summary for last quarter",
    "What is the geographic revenue breakdown by state?",
    "Which stores are underperforming vs targets?",
]


def render() -> None:
    st.markdown("## Sales DWH Assistant")
    st.caption("Ask any business question about your retail data warehouse.")

    missing = config.missing_keys()
    if missing:
        st.warning(
            f"⚠️ **{len(missing)} secret(s) not configured.** "
            "Add them in **App Settings → Secrets**.\n\n"
            f"Missing: `{'`, `'.join(missing)}`"
        )
        return

    all_msgs  = st.session_state.get("messages", [])
    user_msgs = [m for m in all_msgs if m["role"] == "user"]

    # Sidebar samples
    with st.sidebar:
        st.markdown("---")
        st.markdown("**Sample questions**")
        for q in SAMPLES:
            if st.button(q, key=f"sq_{hash(q)}"):
                st.session_state["pending"] = q

    # Stats
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Questions",  len(user_msgs))
    c2.metric("Databricks", sum(1 for m in all_msgs if m.get("platform") == "databricks"))
    c3.metric("Snowflake",  sum(1 for m in all_msgs if m.get("platform") == "snowflake"))
    c4.metric("Structural", sum(1 for m in all_msgs if m.get("is_structural")))

    # History
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
                             width='stretch', hide_index=True)
            if msg.get("sql"):
                with st.expander("🔍 SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            if msg.get("source") and not msg.get("is_structural"):
                co1, co2, co3 = st.columns(3)
                co1.caption(f"📁 `{msg['source'].split('→')[-1].strip()}`")
                co2.caption(f"⚡ {msg.get('elapsed','—')}s")
                plat = msg.get("platform", "")
                co3.caption("❄️ Snowflake" if plat == "snowflake" else "🟠 Databricks")

    # Input
    question = None
    if st.session_state.get("pending"):
        question = st.session_state.pop("pending")
    user_input = st.chat_input("Ask a question about your sales data…")
    if user_input:
        question = user_input

    if not question:
        if not all_msgs:
            st.info("👋 Ask a question above or pick an example from the sidebar.")
        return

    st.session_state["messages"].append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        step_lines: list = []
        step_container = st.empty()

        def on_step(icon: str, msg: str):
            step_lines.append({"icon": icon, "msg": msg})
            lines_html = "<br/>".join(
                f'<span style="color:{_ICON_COLOR.get(s["icon"],"#8b949e")}">{s["icon"]}</span>'
                f' <span style="font-size:0.85rem;color:#c9d1d9">{s["msg"]}</span>'
                for s in step_lines)
            step_container.markdown(
                f'<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;'
                f'padding:10px 14px;margin-bottom:8px;line-height:2">{lines_html}</div>',
                unsafe_allow_html=True)

        t0 = time.time()
        try:
            result = agent.run(question, on_step=on_step)
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
                        f'<span style="color:{color}">{s["icon"]}</span>'
                        f' <span style="font-size:0.85rem;color:#c9d1d9">{s["msg"]}</span>',
                        unsafe_allow_html=True)

        err    = result.get("error", "")
        answer = result.get("answer", "") or (f"⚠️ {err}" if err else "—")
        rows   = result.get("result_rows", [])
        cols   = result.get("result_cols", [])

        st.markdown(answer)
        if rows and cols and not err:
            import pandas as pd
            st.dataframe(pd.DataFrame(rows, columns=cols),
                         width='stretch', hide_index=True)
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
