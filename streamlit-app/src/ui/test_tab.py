"""
src/ui/test_tab.py — Test Suite tab.

Runs 100 mixed questions against the live agent, captures full verbosity,
writes a JSONL log downloadable from the UI.
"""

import time
import json
import io
import datetime
import streamlit as st
from src.pipeline import agent
from src import config

# ── 100-question test suite ───────────────────────────────────────────────────
# Fields: q=question, cat=expected category, exp=expected platform/source hint
# cat values: structural | databricks | snowflake | silver

TEST_SUITE = [
    # ── Structural / GraphDB (no SQL) ─────────────────────────────────────────
    {"id":  1, "cat": "structural", "exp": "graphdb",
     "q": "What are all the Gold tables?"},
    {"id":  2, "cat": "structural", "exp": "graphdb",
     "q": "What KPIs are available in the warehouse?"},
    {"id":  3, "cat": "structural", "exp": "graphdb",
     "q": "Which tables contain PII columns?"},
    {"id":  4, "cat": "structural", "exp": "graphdb",
     "q": "Show cross-platform lineage between Databricks and Snowflake"},
    {"id":  5, "cat": "structural", "exp": "graphdb",
     "q": "What are all the Silver tables?"},
    {"id":  6, "cat": "structural", "exp": "graphdb",
     "q": "Which tables are on Snowflake?"},
    {"id":  7, "cat": "structural", "exp": "graphdb",
     "q": "Which tables are on Databricks?"},
    {"id":  8, "cat": "structural", "exp": "graphdb",
     "q": "What are all the fact tables?"},
    {"id":  9, "cat": "structural", "exp": "graphdb",
     "q": "What are all the dimension tables?"},
    {"id": 10, "cat": "structural", "exp": "graphdb",
     "q": "Show OWL classes in the ontology"},
    {"id": 11, "cat": "structural", "exp": "graphdb",
     "q": "What tables feed into the Gold layer?"},
    {"id": 12, "cat": "structural", "exp": "graphdb",
     "q": "List all tables in the Sales domain"},
    {"id": 13, "cat": "structural", "exp": "graphdb",
     "q": "What Silver tables exist?"},
    {"id": 14, "cat": "structural", "exp": "graphdb",
     "q": "Which tables have PII data?"},
    {"id": 15, "cat": "structural", "exp": "graphdb",
     "q": "What are all the KPIs available?"},
    {"id": 16, "cat": "structural", "exp": "graphdb",
     "q": "Show all dimension tables available"},
    {"id": 17, "cat": "structural", "exp": "graphdb",
     "q": "What fact tables do we have?"},
    {"id": 18, "cat": "structural", "exp": "graphdb",
     "q": "Which tables are in the Silver layer?"},
    {"id": 19, "cat": "structural", "exp": "graphdb",
     "q": "Show the data lineage across platforms"},
    {"id": 20, "cat": "structural", "exp": "graphdb",
     "q": "What aggregate tables exist in the Gold layer?"},

    # ── Databricks Gold ───────────────────────────────────────────────────────
    {"id": 21, "cat": "databricks", "exp": "databricks",
     "q": "What was total revenue by channel last quarter?"},
    {"id": 22, "cat": "databricks", "exp": "databricks",
     "q": "Which customers are at risk of churning?"},
    {"id": 23, "cat": "databricks", "exp": "databricks",
     "q": "What is the revenue by product over all months?"},
    {"id": 24, "cat": "databricks", "exp": "snowflake",
     "q": "Show top 5 employees by commission earned"},
    {"id": 25, "cat": "databricks", "exp": "databricks",
     "q": "What is the month over month revenue growth?"},
    {"id": 26, "cat": "databricks", "exp": "databricks",
     "q": "Which stores are underperforming vs their targets?"},
    {"id": 27, "cat": "databricks", "exp": "databricks",
     "q": "What is the customer lifetime value distribution?"},
    {"id": 28, "cat": "databricks", "exp": "databricks",
     "q": "Show revenue by channel by month for all months"},
    {"id": 29, "cat": "databricks", "exp": "databricks",
     "q": "Which product categories have the highest gross margin?"},
    {"id": 30, "cat": "databricks", "exp": "databricks",
     "q": "What is the inventory stockout rate by product?"},
    {"id": 31, "cat": "databricks", "exp": "snowflake",
     "q": "Show employee quota attainment this year"},
    {"id": 32, "cat": "databricks", "exp": "databricks",
     "q": "What are the top 10 products by revenue?"},
    {"id": 33, "cat": "databricks", "exp": "databricks",
     "q": "Which promotions had the highest ROI?"},
    {"id": 34, "cat": "snowflake",  "exp": "snowflake",
     "q": "What is the average order value by customer segment?"},
    {"id": 35, "cat": "databricks", "exp": "databricks",
     "q": "Show monthly revenue trend over all months"},
    {"id": 36, "cat": "databricks", "exp": "databricks",
     "q": "Which stores have the highest revenue per square foot?"},
    {"id": 37, "cat": "snowflake",  "exp": "snowflake",
     "q": "What is the churn rate by customer segment?"},
    {"id": 38, "cat": "databricks", "exp": "databricks",
     "q": "Show daily revenue for the last quarter"},
    {"id": 39, "cat": "databricks", "exp": "databricks",
     "q": "What is the gross margin percentage by product category?"},
    {"id": 40, "cat": "databricks", "exp": "snowflake",
     "q": "Which employees have the highest total commission?"},
    {"id": 41, "cat": "databricks", "exp": "databricks",
     "q": "Show total revenue by store by month"},
    {"id": 42, "cat": "databricks", "exp": "databricks",
     "q": "What is the average basket size by channel?"},
    {"id": 43, "cat": "databricks", "exp": "databricks",
     "q": "Which products have the highest return rate?"},
    {"id": 44, "cat": "databricks", "exp": "databricks",
     "q": "Show promotion revenue uplift vs baseline"},
    {"id": 45, "cat": "databricks", "exp": "databricks",
     "q": "What is the year over year revenue growth?"},
    {"id": 46, "cat": "databricks", "exp": "databricks",
     "q": "Show the top 10 customers by lifetime value"},
    {"id": 47, "cat": "databricks", "exp": "databricks",
     "q": "What is the average revenue per store?"},
    {"id": 48, "cat": "databricks", "exp": "databricks",
     "q": "Which product categories are growing fastest?"},
    {"id": 49, "cat": "databricks", "exp": "databricks",
     "q": "Show inventory health metrics by product category"},
    {"id": 50, "cat": "databricks", "exp": "databricks",
     "q": "What is the overall customer churn rate?"},

    # ── Snowflake Gold ────────────────────────────────────────────────────────
    {"id": 51, "cat": "snowflake", "exp": "snowflake",
     "q": "Which suppliers have a return rate above 10 percent?"},
    {"id": 52, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the revenue breakdown by state?"},
    {"id": 53, "cat": "snowflake", "exp": "snowflake",
     "q": "What payment methods generate the most revenue?"},
    {"id": 54, "cat": "snowflake", "exp": "snowflake",
     "q": "Show the executive summary for last quarter"},
    {"id": 55, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the geographic revenue by region?"},
    {"id": 56, "cat": "snowflake", "exp": "snowflake",
     "q": "Which suppliers have the best delivery reliability?"},
    {"id": 57, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the month over month growth for credit card payments?"},
    {"id": 58, "cat": "snowflake", "exp": "snowflake",
     "q": "Show customer segment retention rates"},
    {"id": 59, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the cohort CLV for customers acquired in 2022?"},
    {"id": 60, "cat": "databricks", "exp": "databricks",
     "q": "Which channels have the highest profit margin?"},
    {"id": 61, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the supplier performance by margin?"},
    {"id": 62, "cat": "snowflake", "exp": "snowflake",
     "q": "Show payment method revenue mix over all months"},
    {"id": 63, "cat": "snowflake", "exp": "snowflake",
     "q": "What states have the highest revenue share?"},
    {"id": 64, "cat": "snowflake", "exp": "snowflake",
     "q": "Which customer segments have the highest average order value?"},
    {"id": 65, "cat": "snowflake", "exp": "snowflake",
     "q": "Show executive KPIs for the year 2023"},
    {"id": 66, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the geographic revenue breakdown across all months?"},
    {"id": 67, "cat": "snowflake", "exp": "snowflake",
     "q": "Which payment methods are growing the fastest?"},
    {"id": 68, "cat": "snowflake", "exp": "snowflake",
     "q": "Show cohort analysis for all acquisition months"},
    {"id": 69, "cat": "databricks", "exp": "databricks",
     "q": "What is the channel performance by total revenue?"},
    {"id": 70, "cat": "snowflake", "exp": "snowflake",
     "q": "Which suppliers have improving reliability scores?"},
    {"id": 71, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the total revenue by payment method over all months?"},
    {"id": 72, "cat": "snowflake", "exp": "snowflake",
     "q": "Which states have the lowest revenue share?"},
    {"id": 73, "cat": "snowflake", "exp": "snowflake",
     "q": "Show the supplier return rate trend over all months"},
    {"id": 74, "cat": "snowflake", "exp": "snowflake",
     "q": "What is the premium customer segment average order value?"},
    {"id": 75, "cat": "snowflake", "exp": "snowflake",
     "q": "Show revenue by acquisition cohort"},

    # ── Silver / Dimension + Fact (schema questions) ──────────────────────────
    {"id": 76, "cat": "silver", "exp": "databricks",
     "q": "What kind of data does the dim_customer table hold?"},
    {"id": 77, "cat": "silver", "exp": "databricks",
     "q": "What columns does fct_sales have?"},
    {"id": 78, "cat": "silver",     "exp": "snowflake",
     "q": "Describe the dim_product table structure"},
    {"id": 79, "cat": "silver",     "exp": "snowflake",
     "q": "What data is in the dim_store table?"},
    {"id": 80, "cat": "silver",     "exp": "snowflake",
     "q": "What fields does dim_employee contain?"},
    {"id": 81, "cat": "silver",     "exp": "snowflake",
     "q": "What kind of data does the dimension customer hold?"},
    {"id": 82, "cat": "silver", "exp": "databricks",
     "q": "Show me the fct_returns table structure"},
    {"id": 83, "cat": "silver",     "exp": "snowflake",
     "q": "What does the dim_supplier table contain?"},
    {"id": 84, "cat": "silver",     "exp": "snowflake",
     "q": "What is in the dim_date table?"},
    {"id": 85, "cat": "silver", "exp": "databricks",
     "q": "What columns are in fct_inventory?"},
    {"id": 86, "cat": "silver",     "exp": "graphdb",
     "q": "Describe the sales fact table columns"},
    {"id": 87, "cat": "silver",     "exp": "snowflake",
     "q": "What data does the product dimension have?"},
    {"id": 88, "cat": "silver",     "exp": "snowflake",
     "q": "What are the columns in the store dimension?"},
    {"id": 89, "cat": "silver", "exp": "databricks",
     "q": "Show the structure of fct_sales_targets"},
    {"id": 90, "cat": "silver",     "exp": "snowflake",
     "q": "What fields are in the customer dimension table?"},
    {"id": 91, "cat": "silver",     "exp": "snowflake",
     "q": "Describe the dim_date columns"},
    {"id": 92, "cat": "silver", "exp": "databricks",
     "q": "What does fct_inventory track?"},
    {"id": 93, "cat": "silver",     "exp": "snowflake",
     "q": "Show me the employee dimension columns"},
    {"id": 94, "cat": "silver",     "exp": "graphdb",
     "q": "What data is in the returns fact table?"},
    {"id": 95, "cat": "silver", "exp": "databricks",
     "q": "What columns does the supplier dimension have?"},

    # ── Edge / mixed ──────────────────────────────────────────────────────────
    {"id": 96,  "cat": "databricks", "exp": "databricks",
     "q": "How many churned customers do we have?"},
    {"id": 97,  "cat": "snowflake",  "exp": "snowflake",
     "q": "What is the average supplier lead time?"},
    {"id": 98,  "cat": "databricks", "exp": "databricks",
     "q": "Show revenue by SKU over all months"},
    {"id": 99,  "cat": "databricks", "exp": "databricks",
     "q": "What is the best performing product category by margin?"},
    {"id": 100, "cat": "snowflake",  "exp": "snowflake",
     "q": "Which region has the highest revenue growth?"},
]

CAT_COLORS = {
    "structural": "#1f3a5f",
    "databricks": "#2a1800",
    "snowflake":  "#0d1e2d",
    "silver":     "#141f2a",
}
CAT_LABELS = {
    "structural": "🔷 Structural/GraphDB",
    "databricks": "🟠 Databricks Gold",
    "snowflake":  "❄️ Snowflake Gold",
    "silver":     "● Silver Layer",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _actual_platform(result: dict) -> str:
    if result.get("is_structural"):
        # Schema shortcut: is_structural=True but has real platform in source/platform
        plat = result.get("platform", "")
        if plat in ("databricks", "snowflake"):
            return plat
        source = result.get("source", "")
        if "Snowflake" in source: return "snowflake"
        if "Databricks" in source: return "databricks"
        return "graphdb"
    return result.get("platform", "unknown")


def _routing_ok(result: dict, expected: str) -> bool:
    actual = _actual_platform(result)
    if expected == "graphdb":
        return actual == "graphdb"
    return actual == expected


def _build_log_entry(test: dict, result: dict, elapsed: float) -> dict:
    """Full-verbosity log entry for one question."""
    return {
        "id":              test["id"],
        "category":        test["cat"],
        "expected_source": test["exp"],
        "question":        test["q"],
        "actual_platform": _actual_platform(result),
        "actual_source":   result.get("source", ""),
        "routing_correct": _routing_ok(result, test["exp"]),
        "table_matched":   result.get("source","").split("→")[-1].strip() if "→" in result.get("source","") else "",
        "sql_generated":   result.get("sql", ""),
        "row_count":       len(result.get("result_rows", [])),
        "answer_preview":  (result.get("answer","") or "")[:400],
        "error":           result.get("error",""),
        "elapsed_s":       elapsed,
        "steps":           result.get("steps", []),
        "timestamp":       datetime.datetime.utcnow().isoformat(),
    }


# ── Main render ───────────────────────────────────────────────────────────────

def render() -> None:
    st.markdown("## Test Suite — 100 Questions")
    st.caption(
        "Runs all 100 test questions against the live agent. "
        "Captures full verbosity — routing, SQL, answer, steps, timing — "
        "for tuning. Download the JSONL log for offline analysis."
    )

    if config.missing_keys():
        st.warning("⚠️ Configure secrets before running the test suite.")
        return

    # ── Controls ──────────────────────────────────────────────────────────────
    cats = ["All"] + list(CAT_LABELS.keys())
    col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
    with col1:
        cat_filter = st.selectbox("Category", cats,
            format_func=lambda x: "All categories" if x=="All" else CAT_LABELS[x])
    with col2:
        id_from = st.number_input("From Q#", min_value=1, max_value=100, value=1)
    with col3:
        id_to   = st.number_input("To Q#",   min_value=1, max_value=100, value=10)
    with col4:
        st.markdown("<br/>", unsafe_allow_html=True)
        run_btn = st.button("▶ Run selected", type="primary", use_container_width=True)

    # Filter
    subset = [t for t in TEST_SUITE
              if (cat_filter == "All" or t["cat"] == cat_filter)
              and id_from <= t["id"] <= id_to]
    st.caption(f"{len(subset)} questions selected")

    # ── Run ───────────────────────────────────────────────────────────────────
    if run_btn and subset:
        log_entries: list = []
        results_display: list = []

        progress   = st.progress(0.0, text="Starting…")
        status_box = st.empty()
        live_table = st.empty()

        for i, test in enumerate(subset):
            frac = i / len(subset)
            progress.progress(frac, text=f"Q{test['id']}: {test['q'][:60]}…")

            steps_collected: list = []
            def on_step(icon, msg, _t=test):
                steps_collected.append({"icon": icon, "msg": msg})

            t0 = time.time()
            try:
                result = agent.run(test["q"], on_step=on_step)
                result.setdefault("steps", steps_collected)
            except Exception as exc:
                result = agent._err(str(exc))
                result["steps"] = steps_collected
            elapsed = round(time.time() - t0, 2)

            entry = _build_log_entry(test, result, elapsed)
            log_entries.append(entry)

            ok   = entry["routing_correct"]
            err  = bool(entry["error"])
            icon = "✅" if ok and not err else ("⚠️" if not ok else "❌")
            results_display.append({
                "":        icon,
                "Q#":      test["id"],
                "Category":CAT_LABELS[test["cat"]],
                "Question":test["q"][:55]+"…" if len(test["q"])>55 else test["q"],
                "Routed to":entry["actual_platform"],
                "Expected": test["exp"],
                "Table":   entry["table_matched"][:30] if entry["table_matched"] else "—",
                "Rows":    entry["row_count"],
                "Secs":    elapsed,
                "Error":   entry["error"][:40] if entry["error"] else "",
            })

            # Update live table after each question
            import pandas as pd
            live_table.dataframe(
                pd.DataFrame(results_display),
                width='stretch', hide_index=True,
                column_config={"": st.column_config.TextColumn(width="small"),
                               "Q#": st.column_config.NumberColumn(width="small"),
                               "Secs": st.column_config.NumberColumn(format="%.1fs", width="small"),
                               "Rows": st.column_config.NumberColumn(width="small")})

        progress.progress(1.0, text="Done!")

        # Summary metrics
        n = len(log_entries)
        n_ok  = sum(1 for e in log_entries if e["routing_correct"] and not e["error"])
        n_err = sum(1 for e in log_entries if e["error"])
        n_wrong = sum(1 for e in log_entries if not e["routing_correct"] and not e["error"])
        avg_t = sum(e["elapsed_s"] for e in log_entries) / n if n else 0

        st.markdown("---")
        m1,m2,m3,m4,m5 = st.columns(5)
        m1.metric("Total run",    n)
        m2.metric("✅ Correct",   n_ok,    delta=f"{n_ok/n*100:.0f}%" if n else None)
        m3.metric("⚠️ Wrong route",n_wrong, delta=f"-{n_wrong/n*100:.0f}%" if n else None, delta_color="inverse")
        m4.metric("❌ Errors",    n_err,   delta_color="inverse")
        m5.metric("Avg time",     f"{avg_t:.1f}s")

        # ── Store results in session state ────────────────────────────────────
        st.session_state["test_log"]     = log_entries
        st.session_state["test_results"] = results_display

        # ── Download buttons ─────────────────────────────────────────────────
        st.markdown("### Download Logs")
        dc1, dc2 = st.columns(2)

        # JSONL — full verbosity
        jsonl_buf = io.BytesIO(
            "\n".join(json.dumps(e, default=str) for e in log_entries).encode())
        dc1.download_button(
            "📥 Full log (JSONL — high verbosity)",
            data=jsonl_buf,
            file_name=f"kg_poc_test_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl",
            mime="application/jsonl",
            use_container_width=True)

        # Human-readable text log
        txt_lines = [f"KG-POC Test Suite Log  —  {datetime.datetime.utcnow().isoformat()}\n",
                     f"Questions run: {n}  |  Correct: {n_ok}  |  Wrong route: {n_wrong}  |  Errors: {n_err}\n",
                     "="*100]
        for e in log_entries:
            ok_flag = "OK" if e["routing_correct"] and not e["error"] else ("WRONG_ROUTE" if not e["routing_correct"] else "ERROR")
            txt_lines += [
                f"\n[Q{e['id']:03d}] [{e['category'].upper():12s}] [{ok_flag}]  {e['question']}",
                f"  Expected : {e['expected_source']}",
                f"  Got      : {e['actual_platform']}  ({e['actual_source']})",
                f"  Table    : {e['table_matched'] or '—'}   Rows: {e['row_count']}   Time: {e['elapsed_s']}s",
            ]
            if e["sql_generated"]:
                txt_lines.append(f"  SQL      : {e['sql_generated'][:200].replace(chr(10),' ')}")
            if e["answer_preview"]:
                txt_lines.append(f"  Answer   : {e['answer_preview'][:200]}")
            if e["error"]:
                txt_lines.append(f"  ERROR    : {e['error']}")
            txt_lines.append(f"  Steps    : {' → '.join(s['msg'][:40] for s in e['steps'])}")
            txt_lines.append("-"*100)

        txt_buf = io.BytesIO("\n".join(txt_lines).encode())
        dc2.download_button(
            "📄 Human-readable log (TXT)",
            data=txt_buf,
            file_name=f"kg_poc_test_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain",
            use_container_width=True)

        # ── Inline full log (expandable per question) ─────────────────────────
        st.markdown("---")
        st.markdown("### Detailed Results")

        wrong = [e for e in log_entries if not e["routing_correct"] or e["error"]]
        if wrong:
            with st.expander(f"⚠️ Issues to fix ({len(wrong)} questions)", expanded=True):
                for e in wrong:
                    flag = "Wrong route" if not e["routing_correct"] else "Error"
                    st.markdown(
                        f"**Q{e['id']}** `{e['category']}` — {flag}\n\n"
                        f"> {e['question']}\n\n"
                        f"Expected **{e['expected_source']}** → got **{e['actual_platform']}**"
                        + (f"\n\nError: `{e['error']}`" if e["error"] else "")
                        + (f"\n\nSQL: `{e['sql_generated'][:300]}`" if e["sql_generated"] else ""))
                    st.markdown("---")

        with st.expander("📋 All results (verbose)", expanded=False):
            for e in log_entries:
                ok = e["routing_correct"] and not e["error"]
                icon = "✅" if ok else ("⚠️" if not e["routing_correct"] else "❌")
                st.markdown(
                    f"**{icon} Q{e['id']}** `{e['category']}` — "
                    f"*{e['question']}*")
                c1, c2, c3, c4 = st.columns(4)
                c1.caption(f"Expected: **{e['expected_source']}**")
                c2.caption(f"Got: **{e['actual_platform']}**")
                c3.caption(f"Table: {e['table_matched'] or '—'}")
                c4.caption(f"{e['row_count']} rows · {e['elapsed_s']}s")
                if e["sql_generated"]:
                    with st.expander("SQL", expanded=False):
                        st.code(e["sql_generated"], language="sql")
                if e["answer_preview"]:
                    st.markdown(
                        f'<div style="background:#161b22;border-left:3px solid #58a6ff;'
                        f'padding:8px 12px;border-radius:4px;font-size:0.82rem;color:#c9d1d9;'
                        f'margin-bottom:4px">{e["answer_preview"]}</div>',
                        unsafe_allow_html=True)
                if e["steps"]:
                    steps_txt = " → ".join(
                        f"{s['icon']} {s['msg'][:35]}" for s in e["steps"])
                    st.caption(f"Pipeline: {steps_txt}")
                if e["error"]:
                    st.error(e["error"])
                st.markdown("---")

    # ── Show previous run if available ────────────────────────────────────────
    elif "test_results" in st.session_state and not run_btn:
        import pandas as pd
        st.info("Showing last run results. Press **▶ Run selected** to run again.")
        st.dataframe(pd.DataFrame(st.session_state["test_results"]),
                     width='stretch', hide_index=True)

    else:
        st.info(
            "Select a category and question range above, then click **▶ Run selected**.\n\n"
            "Tip: start with Q1–10 (structural) to verify GraphDB connectivity, "
            "then Q21–30 (Databricks Gold) and Q51–60 (Snowflake)."
        )

        # Show full suite preview
        import pandas as pd
        preview = [{"Q#": t["id"], "Category": CAT_LABELS[t["cat"]],
                    "Expected": t["exp"],
                    "Question": t["q"][:70]+"…" if len(t["q"])>70 else t["q"]}
                   for t in TEST_SUITE]
        st.dataframe(pd.DataFrame(preview), width='stretch', hide_index=True,
                     height=400)
