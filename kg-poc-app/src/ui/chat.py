"""
src/ui/chat.py — Tab 1: Ask the DWH.

Uses st.chat_input() + st.chat_message() — exact pattern from working reference app.
No st.form, no st.rerun() needed. Streamlit handles the chat flow natively.
"""

import time
import streamlit as st
from src import agent, config


def render() -> None:
    st.markdown("## Sales DWH Assistant")
    st.caption(
        "Ask any business question. The agent queries GraphDB, "
        "Databricks, and Snowflake to answer."
    )

    missing = config.missing_keys()
    if missing:
        st.warning(
            f"⚠️ **{len(missing)} secret(s) not configured.** "
            "Add them in **App Settings → Secrets**.\n\n"
            f"Missing: `{'`, `'.join(missing)}`"
        )
        return

    # Stats strip
    msgs = [m for m in st.session_state.get("messages", []) if m["role"] == "user"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Questions",  len(msgs))
    c2.metric("Databricks", sum(1 for m in st.session_state.get("messages",[])
                                if m.get("platform") == "databricks"))
    c3.metric("Snowflake",  sum(1 for m in st.session_state.get("messages",[])
                                if m.get("platform") == "snowflake"))
    c4.metric("Structural", sum(1 for m in st.session_state.get("messages",[])
                                if m.get("is_structural")))

    # Render message history (same pattern as reference app lines 650-670)
    for msg in st.session_state.get("messages", []):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sql"):
                with st.expander("🔍 SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            if msg.get("source") and not msg.get("is_structural"):
                cols = st.columns(3)
                cols[0].caption(f"📁 `{msg['source'].split('→')[-1].strip()}`")
                cols[1].caption(f"⚡ {msg.get('elapsed','—')}s")
                cols[2].caption(f"🧠 KG + {'❄️' if msg.get('platform')=='snowflake' else '🟠'}")

    # Handle sidebar sample question (sets st.session_state["pending"])
    question = None
    if st.session_state.get("pending"):
        question = st.session_state["pending"]
        st.session_state["pending"] = None

    # Chat input (reference app pattern — lines 678-680)
    user_input = st.chat_input("Ask a question about your sales data…")
    if user_input:
        question = user_input

    if not question:
        if not st.session_state.get("messages"):
            st.info("👋 Ask a question or pick an example from the sidebar.")
        return

    # Show user message immediately (reference app pattern — lines 683-685)
    st.session_state["messages"].append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Run agent and stream result (reference app pattern — lines 687-710)
    with st.chat_message("assistant"):
        with st.spinner("Querying knowledge graph and data warehouse…"):
            t0 = time.time()
            try:
                result = agent.run(question)
            except Exception as exc:
                result = {"source":"","sql":"","answer":"","platform":"",
                          "is_structural":False,"error":str(exc)}
            elapsed = round(time.time() - t0, 1)

        err = result.get("error","")
        answer = result.get("answer","") or (f"⚠️ {err}" if err else "—")
        st.markdown(answer)

        sql = result.get("sql","")
        if sql:
            with st.expander("🔍 SQL generated", expanded=False):
                st.code(sql, language="sql")

        source = result.get("source","")
        if source and not result.get("is_structural"):
            co1, co2, co3 = st.columns(3)
            co1.caption(f"📁 `{source.split('→')[-1].strip()}`")
            co2.caption(f"⚡ {elapsed}s")
            co3.caption(f"🧠 KG + {'❄️' if result.get('platform')=='snowflake' else '🟠'}")

    # Persist to history
    st.session_state["messages"].append({
        "role":         "assistant",
        "content":      answer,
        "sql":          sql,
        "source":       source,
        "platform":     result.get("platform",""),
        "is_structural":result.get("is_structural",False),
        "elapsed":      elapsed,
    })
