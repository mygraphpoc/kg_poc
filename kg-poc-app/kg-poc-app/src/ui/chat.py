"""src/ui/chat.py — Tab 1: Ask the DWH."""

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
    all_msgs = st.session_state.get("messages", [])
    user_msgs = [m for m in all_msgs if m["role"] == "user"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Questions",  len(user_msgs))
    c2.metric("Databricks", sum(1 for m in all_msgs if m.get("platform") == "databricks"))
    c3.metric("Snowflake",  sum(1 for m in all_msgs if m.get("platform") == "snowflake"))
    c4.metric("Structural", sum(1 for m in all_msgs if m.get("is_structural")))

    # Render history
    for msg in all_msgs:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            # Show dataframe if rows were returned
            rows = msg.get("result_rows", [])
            cols = msg.get("result_cols", [])
            if rows and cols:
                import pandas as pd
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True, hide_index=True)
            # SQL expander
            if msg.get("sql"):
                with st.expander("🔍 SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            # Source metadata
            if msg.get("source") and not msg.get("is_structural"):
                co1, co2, co3 = st.columns(3)
                co1.caption(f"📁 `{msg['source'].split('→')[-1].strip()}`")
                co2.caption(f"⚡ {msg.get('elapsed','—')}s")
                plat = msg.get("platform","")
                co3.caption(f"{'❄️ Snowflake' if plat=='snowflake' else '🟠 Databricks'}")

    # Handle sidebar sample question
    question = None
    if st.session_state.get("pending"):
        question = st.session_state["pending"]
        st.session_state["pending"] = None

    user_input = st.chat_input("Ask a question about your sales data…")
    if user_input:
        question = user_input

    if not question:
        if not all_msgs:
            st.info("👋 Ask a question or pick an example from the sidebar.")
        return

    # Show user message
    st.session_state["messages"].append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Run agent
    with st.chat_message("assistant"):
        with st.spinner("Querying knowledge graph and data warehouse…"):
            t0 = time.time()
            try:
                result = agent.run(question)
            except Exception as exc:
                result = {"source":"","sql":"","answer":"","platform":"",
                          "result_rows":[],"result_cols":[],
                          "is_structural":False,"error":str(exc)}
            elapsed = round(time.time() - t0, 1)

        err    = result.get("error","")
        answer = result.get("answer","") or (f"⚠️ {err}" if err else "—")
        rows   = result.get("result_rows", [])
        cols   = result.get("result_cols", [])

        st.markdown(answer)

        # Show results table when there are multiple rows
        if rows and cols and not err:
            import pandas as pd
            df = pd.DataFrame(rows, columns=cols)
            st.dataframe(df, use_container_width=True, hide_index=True)

        sql = result.get("sql","")
        if sql:
            with st.expander("🔍 SQL generated", expanded=False):
                st.code(sql, language="sql")

        source = result.get("source","")
        if source and not result.get("is_structural"):
            co1, co2, co3 = st.columns(3)
            co1.caption(f"📁 `{source.split('→')[-1].strip()}`")
            co2.caption(f"⚡ {elapsed}s")
            plat = result.get("platform","")
            co3.caption(f"{'❄️ Snowflake' if plat=='snowflake' else '🟠 Databricks'}")

    # Persist to history
    st.session_state["messages"].append({
        "role":          "assistant",
        "content":       answer,
        "sql":           sql,
        "result_rows":   rows,
        "result_cols":   cols,
        "source":        source,
        "platform":      result.get("platform",""),
        "is_structural": result.get("is_structural",False),
        "elapsed":       elapsed,
    })
