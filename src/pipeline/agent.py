"""
src/pipeline/agent.py
─────────────────────
Graph-RAG agent pipeline — v2 with hybrid SPARQL + VS + FTS retrieval.

Pipeline:
  1.  Structural SPARQL check   → answer metadata Qs directly from GraphDB
  2a. Explicit table name       → direct SPARQL lookup
  2b. Hybrid retrieval          → FTS + VS + SPARQL combined
  3.  Schema-describe shortcut  → no SQL for "what columns does X have"
  4.  Real schema fetch         → GraphDB biz:hasColumn (works for all platforms)
                                  + Databricks INFORMATION_SCHEMA fallback
  5.  SQL generation            → LLM via ChatDatabricks
  6.  SQL fix                   → fix invalid INTERVAL syntax
  7.  Platform routing          → confirmed from GraphDB sourceSystemType
  8.  SQL execution             → Databricks spark.sql or Snowflake connector
  9.  Answer generation         → LLM narrative summary
"""

import re
import streamlit as st
from src import graphdb, sql_exec, config
from src.retrieval.hybrid_retriever import find_best_table


@st.cache_resource(show_spinner=False)
def _get_llm():
    from src.pipeline.llm import get_llm
    return get_llm()


def _fix_sql(sql: str) -> str:
    """Fix common LLM SQL generation errors — syntactic only, no business logic."""
    sql = re.sub(r"INTERVAL\s+'(\d+)\s+quarter[s]?'",
                 lambda m: "INTERVAL " + str(int(m.group(1))*3) + " MONTHS",
                 sql, flags=re.I)
    sql = re.sub(r"INTERVAL\s+'(\d+)\s+year[s]?'",
                 lambda m: "INTERVAL " + str(int(m.group(1))*12) + " MONTHS",
                 sql, flags=re.I)
    sql = re.sub(r"INTERVAL\s+'(\d+)\s+month[s]?'",
                 lambda m: "INTERVAL " + m.group(1) + " MONTHS",
                 sql, flags=re.I)
    sql = re.sub(r"INTERVAL\s+'(\d+)\s+day[s]?'",
                 lambda m: "INTERVAL " + m.group(1) + " DAYS",
                 sql, flags=re.I)
    return sql


def _get_schema(table_name: str, platform: str, layer: str, token: str) -> list:
    """
    Fetch column names and data types.
    Primary: GraphDB biz:hasColumn (works for ALL platforms including Snowflake)
    Fallback: Databricks INFORMATION_SCHEMA (for Databricks tables only)
    """
    from src.retrieval import sparql_retriever as sr
    # Primary — GraphDB schema (works for all platforms)
    rows = graphdb.query(
        'SELECT ?colName ?dataType WHERE {'
        ' ?t biz:tableName ?tname ; biz:hasColumn ?col .'
        ' FILTER (LCASE(STR(?tname)) = "' + table_name.lower() + '")'
        ' ?col biz:columnName ?colName .'
        ' OPTIONAL { ?col biz:dataType ?dataType }'
        '} ORDER BY ?colName', token) or []
    cols = [(r.get("colName",""), r.get("dataType",""))
            for r in rows if r.get("colName")]
    if cols:
        return cols
    # Fallback — Databricks INFORMATION_SCHEMA
    if platform == "databricks":
        try:
            return sql_exec.databricks_columns(table_name, layer)
        except Exception:
            pass
    return []


def run(question: str, on_step=None) -> dict:
    """
    Run the full Graph-RAG pipeline.
    on_step(icon, msg) called at each stage for live UI progress.
    Returns dict with: source, sql, answer, result_rows, result_cols,
                       platform, is_structural, error, steps, retrieval_scores
    """
    from langchain_core.messages import HumanMessage
    steps: list = []

    def record(icon: str, msg: str):
        steps.append({"icon": icon, "msg": msg})
        if on_step: on_step(icon, msg)

    CATALOG = config.get("DATABRICKS_CATALOG", "kg_vs_poc")

    # ── GraphDB connection ────────────────────────────────────────────────────
    record("🔍", "Connecting to knowledge graph…")
    token, err = graphdb.get_token()
    if err:
        record("❌", f"GraphDB: {err}")
        return _err(err, steps)

    llm = _get_llm()

    # ── 1. Structural SPARQL check ────────────────────────────────────────────
    from src.retrieval import sparql_retriever as sr
    record("🔍", "Is this a metadata question?")
    structural = sr.check_structural(question, token)
    if structural:
        record("📖", f"Yes — answering from GraphDB ({len(structural['rows'])} rows)")
        record("✅", "Generating summary…")
        try:
            answer = llm.invoke([HumanMessage(content=(
                f"Summarise these data warehouse results in 3-5 sentences:\n\n"
                f"{structural['text']}\n\nQuestion: {question}\nAnswer:"
            ))]).content.strip()
        except Exception:
            answer = structural["text"]
        return {"source":"GraphDB (knowledge graph)","sql":"",
                "raw_table":structural["text"],"answer":answer,
                "result_rows":[],"result_cols":[],
                "platform":"graphdb","is_structural":True,
                "error":"","steps":steps,"retrieval_scores":{}}

    record("🗺️", "Data question — running hybrid retrieval…")

    # ── 2. Hybrid retrieval ───────────────────────────────────────────────────
    tbl = find_best_table(question, token, record=record)
    if not tbl:
        record("❌", "Could not identify a relevant table")
        return _err("Could not identify a relevant table. Try rephrasing.", steps)

    pt_name    = tbl["name"]
    pt_plat    = tbl.get("platform", "databricks").lower()
    layer      = tbl.get("layer", "gold").lower()
    scores     = tbl.get("scores", {})

    if pt_plat == "snowflake":
        fqn = f"KG_VS_POC.{layer.upper()}.{pt_name.upper()}"
    else:
        fqn = f"{CATALOG}.{layer}.{pt_name}"

    plat_emoji = "❄️ Snowflake" if pt_plat == "snowflake" else "🟠 Databricks"
    record("🎯", f"Selected: **{pt_name}** ({layer}) on {plat_emoji}")

    # ── 3. Schema fetch ───────────────────────────────────────────────────────
    record("📋", f"Fetching schema — {fqn}")
    actual_cols = _get_schema(pt_name, pt_plat, layer, token)
    record("📋", f"Schema: {len(actual_cols)} columns")

    # ── 4. Schema-describe shortcut ───────────────────────────────────────────
    if sr.SCHEMA_PAT.search(question):
        record("📖", "Schema question — describing table (no SQL needed)")
        col_lines = [f"  - {cn} ({ct})" if ct else f"  - {cn}"
                     for cn, ct in actual_cols[:50]]
        col_desc  = "\n".join(col_lines) or "  (no column metadata available)"
        kpi_rows  = graphdb.query(
            f'SELECT ?kpiName ?direction WHERE {{'
            f' ?t biz:tableName ?tname ; biz:hasKPI ?k .'
            f' FILTER (LCASE(STR(?tname)) = "{pt_name.lower()}")'
            f' ?k biz:kpiName ?kpiName .'
            f' OPTIONAL {{ ?k biz:kpiDirection ?direction }} }}', token) or []
        kpi_text  = ("\n\nRegistered KPIs:\n" +
                     "\n".join("  - " + r.get("kpiName","") for r in kpi_rows[:20])
                     ) if kpi_rows else ""
        context   = (f"Table: {fqn}\nPlatform: {pt_plat}   Layer: {layer}\n"
                     f"Total columns: {len(actual_cols)}\nColumns:\n{col_desc}{kpi_text}")
        record("✅", f"Schema: {len(actual_cols)} columns, {len(kpi_rows)} KPIs")
        try:
            answer = llm.invoke([HumanMessage(content=(
                f"You are a data engineer explaining a warehouse table.\n\n"
                f"Question: {question}\n\n{context}\n\n"
                f"Describe in 3-5 sentences. Use actual column names.\n\nAnswer:"
            ))]).content.strip()
        except Exception:
            answer = context
        col_rows_out = [{"Column": cn, "Type": ct or "—"} for cn, ct in actual_cols[:50]]
        return {"source": f"{plat_emoji} → {fqn}", "sql": "",
                "raw_result":"","result_rows":col_rows_out,
                "result_cols":["Column","Type"],"answer":answer,
                "platform":pt_plat,"is_structural":True,
                "error":"","steps":steps,"retrieval_scores":scores}

    # ── 5. Build schema context ───────────────────────────────────────────────
    col_lines   = [f"  - {cn} [{ct}]" if ct else f"  - {cn}"
                   for cn, ct in actual_cols[:40]]
    schema_block = (f"TABLE: {fqn}\nPlatform: {pt_plat}   Layer: {layer}\n\n"
                    "ACTUAL COLUMNS — use ONLY these exact names:\n"
                    + ("\n".join(col_lines) if col_lines else "  (no column metadata)"))

    all_time = bool(re.search(
        r"\ball\s+(months?|time|years?|periods?|data|history|available)\b"
        r"|\bover\s+(all|entire|the\s+whole)\b|\bhistorical\b|\bevery\s+month\b",
        question, re.I))
    date_rule = (
        "3. DATE: NO date filter — return ALL rows."
        if all_time else
        "3. DATE: Only add a date filter if the question explicitly says "
        "'last quarter/month/year'. If no time qualifier → do NOT filter by date.")

    # ── 6. Generate SQL ───────────────────────────────────────────────────────
    record("✍️", "Generating SQL…")
    try:
        sql_raw = llm.invoke([HumanMessage(content=(
            f"You are a SQL expert for a retail Sales Data Warehouse.\n\n"
            f"Question: {question}\n\n{schema_block}\n\n"
            f"STRICT RULES — violating any rule makes the query fail:\n"
            f"1. ONLY use column names from ACTUAL COLUMNS above. "
            f"Do NOT invent column names. If a column does not appear above it does not exist.\n"
            f"2. Always qualify table as: {fqn}\n"
            f"{date_rule}\n"
            f"4. Never use current_date() — data covers 2020-2024.\n"
            f"5. ORDER BY must use alias names, not aggregate expressions.\n"
            f"6. Categorical filter values are snake_case e.g. 'credit_card'.\n"
            f"7. Add LIMIT 200 unless the question asks for a single total.\n"
            f"8. For Snowflake: use DATEADD(month,-3,MAX(month)) not INTERVAL syntax.\n"
            f"9. Return ONLY the SQL query. No explanation. No markdown. No backticks.\n\nSQL:"
        ))]).content.strip()
    except Exception as e:
        record("❌", f"LLM error: {str(e)[:60]}")
        return _err(f"LLM error: {e}", steps)

    for fence in ["```sql","```SQL","```"]:
        sql_raw = sql_raw.replace(fence,"")
    sql = sql_raw.strip()
    for kw in ("WITH","SELECT"):
        idx = sql.upper().find(kw)
        if idx > 0: sql = sql[idx:]; break

    sql = _fix_sql(sql)
    record("⚡", f"Executing on {plat_emoji}…")

    # ── 7. Execute SQL ────────────────────────────────────────────────────────
    sql_result = ""; result_rows: list = []; result_cols: list = []
    try:
        if pt_plat == "snowflake":
            sql_result, result_rows, result_cols = sql_exec.execute_snowflake(sql)
        else:
            sql_result, result_rows, result_cols = sql_exec.execute_databricks(sql)
    except Exception as e:
        sql_result = f"SQL error: {e}"

    n = len(result_rows)
    if sql_result.lower().startswith(("sql error","databricks error","snowflake error")):
        record("❌", "SQL error — see SQL expander")
    else:
        record("📊", f"Query returned {n} row{'s' if n != 1 else ''}")

    # ── 8. Generate answer ────────────────────────────────────────────────────
    record("💬", "Generating answer…")
    try:
        answer = llm.invoke([HumanMessage(content=(
            f"You are a senior business analyst.\n\n"
            f"Question: {question}\nTable: {fqn}\n"
            f"SQL Result:\n{sql_result[:3000]}\n\n"
            f"Write a concise 2-4 sentence narrative of the KEY insight. "
            f"Include the most important numbers. "
            f"Do NOT reproduce the full table.\n\nAnswer:"
        ))]).content.strip()
    except Exception as e:
        answer = sql_result[:500] if sql_result else f"Error: {e}"

    record("✅", "Done")

    plat_label = "Snowflake" if pt_plat == "snowflake" else "Databricks"
    return {"source": f"{plat_label} → {fqn}", "sql": sql,
            "raw_result": sql_result, "result_rows": result_rows,
            "result_cols": result_cols, "answer": answer,
            "platform": pt_plat, "is_structural": False,
            "error": "", "steps": steps, "retrieval_scores": scores}


def _err(msg: str, steps: list = None) -> dict:
    return {"source":"","sql":"","answer":msg,"platform":"",
            "result_rows":[],"result_cols":[],
            "is_structural":False,"error":msg,
            "steps":steps or [],"retrieval_scores":{}}
