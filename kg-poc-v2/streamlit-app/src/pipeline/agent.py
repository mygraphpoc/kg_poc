"""
src/pipeline/agent.py
─────────────────────
Graph-RAG agent pipeline — v2 with schema enrichment.

Pipeline:
  1.  Structural SPARQL check   → answer metadata Qs directly from GraphDB
  2.  Hybrid retrieval          → FTS + VS + SPARQL combined → best Gold table
  3.  Schema fetch              → Gold table columns from GraphDB
  4.  Schema enrichment         → detect missing columns (sku, product_name,
                                  customer_name, store_name, month etc.)
                                  → find Silver join tables from GraphDB
                                  → build multi-table schema context
  5.  Schema-describe shortcut  → no SQL for "what columns does X have"
  6.  SQL generation            → LLM with full enriched schema context
  7.  SQL fix                   → fix invalid INTERVAL syntax
  8.  SQL execution             → Databricks SQL Warehouse or Snowflake connector
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
    """Fix common LLM SQL generation errors — syntactic only."""
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
    Primary: GraphDB biz:hasColumn — works for ALL platforms.
    Fallback: Databricks INFORMATION_SCHEMA.
    """
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
    if platform == "databricks":
        try:
            return sql_exec.databricks_columns(table_name, layer)
        except Exception:
            pass
    return []


# ── Silver dimension catalogue (driven entirely from GraphDB) ─────────────────
# Maps: concept keyword → Silver dimension table + join key on the fact/Gold table
# These are discovered from GraphDB at runtime, not hardcoded per-query

SILVER_DIM_CONCEPTS = {
    # keyword in question → (silver_table, gold_fk, silver_pk, useful_columns)
    # useful_columns = columns worth adding to the SELECT
    "sku":          ("dim_product",  "product_sk", "product_id",
                     ["sku","product_name","category","sub_category","brand"]),
    "product_name": ("dim_product",  "product_sk", "product_id",
                     ["sku","product_name","category","sub_category","brand"]),
    "product":      ("dim_product",  "product_sk", "product_id",
                     ["sku","product_name","category","sub_category","brand"]),
    "customer_name":("dim_customer", "customer_sk","customer_id",
                     ["full_name","email","segment","city","state_code"]),
    "customer":     ("dim_customer", "customer_sk","customer_id",
                     ["full_name","segment","city","state_code","age_band"]),
    "store_name":   ("dim_store",    "store_sk",   "store_id",
                     ["store_name","city","state","region","store_type"]),
    "store":        ("dim_store",    "store_sk",   "store_id",
                     ["store_name","city","state","region"]),
    "employee":     ("dim_employee", "employee_sk","employee_id",
                     ["full_name","department","role"]),
    "supplier_name":("dim_supplier", "supplier_sk","supplier_id",
                     ["supplier_name","country","reliability_grade"]),
    "promotion":    ("dim_promotion","promotion_sk","promotion_id",
                     ["promo_name","promo_type","discount_pct"]),
}

# Time dimension — when question asks for monthly/daily breakdown
# but Gold table only has a date key (order_date, date_sk)
TIME_CONCEPTS = {
    "month":  "DATE_TRUNC('month', f.order_date) AS month",
    "year":   "YEAR(f.order_date) AS year",
    "quarter":"CONCAT(YEAR(f.order_date), '-Q', QUARTER(f.order_date)) AS quarter",
    "daily":  "f.order_date AS date",
}


def _enrich_schema(
    question: str,
    gold_table: str,
    gold_cols: list,
    platform: str,
    catalog: str,
    token: str,
) -> dict:
    """
    Detect whether the question needs columns that the Gold table doesn't have.
    If so, find appropriate Silver tables from GraphDB and return a join context.

    Returns:
        {
          "needs_join": bool,
          "join_tables": [{"silver_table", "gold_fk", "silver_pk",
                           "useful_cols", "silver_cols"}],
          "needs_time":  bool,
          "time_expr":   str,   # e.g. "DATE_TRUNC('month', f.order_date)"
          "fact_table":  str or None,  # e.g. "fct_sales" if needed
          "fact_cols":   list,
        }
    """
    q_lower  = question.lower()
    gold_col_names = {c.lower() for c, _ in gold_cols}

    joins_needed = []
    seen_tables  = set()

    # ── Check which dimension concepts are requested but missing ──────────────
    for keyword, (silver_tbl, gold_fk, silver_pk, useful_cols) in SILVER_DIM_CONCEPTS.items():
        if keyword not in q_lower:
            continue
        # Does the Gold table already have these columns?
        already_have = any(c in gold_col_names for c in useful_cols)
        if already_have:
            continue
        # Does the Gold table have the FK needed for the join?
        has_fk = gold_fk.lower() in gold_col_names
        if not has_fk:
            continue
        if silver_tbl in seen_tables:
            continue
        seen_tables.add(silver_tbl)

        # Verify the Silver table exists in GraphDB
        rows = graphdb.query(
            'SELECT ?tname WHERE { ?t biz:tableName ?tname . '
            'FILTER (LCASE(STR(?tname)) = "' + silver_tbl.lower() + '") '
            '} LIMIT 1', token) or []
        if not rows:
            continue

        # Fetch actual columns from GraphDB for this Silver table
        silver_col_rows = graphdb.query(
            'SELECT ?colName ?dataType WHERE {'
            ' ?t biz:tableName ?tname ; biz:hasColumn ?col .'
            ' FILTER (LCASE(STR(?tname)) = "' + silver_tbl.lower() + '")'
            ' ?col biz:columnName ?colName .'
            ' OPTIONAL { ?col biz:dataType ?dataType }'
            '} ORDER BY ?colName', token) or []
        silver_cols = [(r.get("colName",""), r.get("dataType",""))
                       for r in silver_col_rows if r.get("colName")]

        # Only keep useful_cols that actually exist in Silver
        silver_col_names = {c.lower() for c, _ in silver_cols}
        verified_useful  = [c for c in useful_cols if c in silver_col_names]

        if verified_useful:
            joins_needed.append({
                "silver_table":  silver_tbl,
                "gold_fk":       gold_fk,
                "silver_pk":     silver_pk,
                "useful_cols":   verified_useful,
                "silver_cols":   silver_cols,
            })

    # ── Check if time breakdown needed but Gold table has no time column ───────
    needs_time = False
    time_expr  = ""
    time_keywords = ["month", "monthly", "year", "yearly", "quarter", "daily",
                     "year-month", "year_month", "month-wise", "time", "trend",
                     "over time", "by month", "by year"]
    has_time_col = any(
        k in gold_col_names
        for k in ["month","year_month","order_date","date","day","quarter"]
    )
    if any(kw in q_lower for kw in time_keywords) and not has_time_col:
        # Gold table has no time column — need fct_sales for time dimension
        needs_time = True
        if "daily" in q_lower or "day" in q_lower:
            time_expr = "DATE_TRUNC('day', f.order_date) AS date"
        elif "quarter" in q_lower:
            time_expr = "CONCAT(YEAR(f.order_date), '-Q', QUARTER(f.order_date)) AS quarter"
        elif "year" in q_lower and "month" not in q_lower:
            time_expr = "YEAR(f.order_date) AS year"
        else:
            time_expr = "DATE_TRUNC('month', f.order_date) AS year_month"

    # ── If Gold table has no time and no FK to Silver, suggest fct_sales ─────
    fact_table = None
    fact_cols  = []
    if needs_time or joins_needed:
        # Check if fct_sales exists and has the FK to link to this Gold table
        fct_rows = graphdb.query(
            'SELECT ?tname WHERE { ?t biz:tableName ?tname . '
            'FILTER (LCASE(STR(?tname)) = "fct_sales") } LIMIT 1', token) or []
        if fct_rows:
            fct_col_rows = graphdb.query(
                'SELECT ?colName WHERE {'
                ' ?t biz:tableName "fct_sales" ; biz:hasColumn ?col .'
                ' ?col biz:columnName ?colName .'
                '} ORDER BY ?colName', token) or []
            fact_cols = [r.get("colName","") for r in fct_col_rows if r.get("colName")]
            # Only use fct_sales if Gold table columns are a subset of what fct_sales can provide
            # i.e. Gold has _sk columns that match fct_sales
            gold_sks = {c for c in gold_col_names if c.endswith("_sk")}
            fct_sks  = {c for c in fact_cols if c.endswith("_sk")}
            if gold_sks & fct_sks:  # intersection — at least one FK in common
                fact_table = "fct_sales"

    return {
        "needs_join": bool(joins_needed),
        "join_tables": joins_needed,
        "needs_time":  needs_time,
        "time_expr":   time_expr,
        "fact_table":  fact_table,
        "fact_cols":   fact_cols,
    }


def _build_schema_block(
    gold_table: str,
    gold_fqn: str,
    gold_cols: list,
    enrichment: dict,
    catalog: str,
    platform: str,
) -> str:
    """
    Build the schema context block to pass to the LLM.
    If enrichment has joins, include Silver table schemas and join instructions.
    """
    col_lines = [f"  - {cn} [{ct}]" if ct else f"  - {cn}"
                 for cn, ct in gold_cols[:40]]
    block = (f"PRIMARY TABLE (alias 'g'): {gold_fqn}\n"
             f"Platform: {platform}   Layer: gold\n\n"
             f"COLUMNS:\n" + ("\n".join(col_lines) if col_lines else "  (no metadata)"))

    if enrichment["needs_join"] or enrichment["needs_time"]:
        block += "\n\n── JOIN CONTEXT ──────────────────────────────\n"
        block += "The question requires columns not in the Gold table.\n"
        block += "You MUST join to the following Silver tables:\n"

        if enrichment["fact_table"] and enrichment["needs_time"]:
            fact_fqn = f"{catalog}.silver.{enrichment['fact_table']}"
            block += (f"\nFACT TABLE (alias 'f'): {fact_fqn}\n"
                      f"  Join: f.product_sk = g.product_sk "
                      f"(or whichever _sk column matches)\n"
                      f"  Time expression to add to SELECT: {enrichment['time_expr']}\n"
                      f"  Available fact columns: "
                      f"{', '.join(enrichment['fact_cols'][:20])}\n")

        for jt in enrichment["join_tables"]:
            dim_fqn = f"{catalog}.silver.{jt['silver_table']}"
            dim_cols_text = ", ".join(
                f"{c}" for c in jt["useful_cols"])
            block += (f"\nDIM TABLE (alias 'd_{jt['silver_table']}'): {dim_fqn}\n"
                      f"  Join: g.{jt['gold_fk']} = d_{jt['silver_table']}.{jt['silver_pk']}\n"
                      f"  USE these columns from dim: {dim_cols_text}\n"
                      f"  Full dim columns available: "
                      f"{', '.join(c for c,_ in jt['silver_cols'][:15])}\n")

        block += ("\n── IMPORTANT ───────────────────────────────\n"
                  "ONLY use columns confirmed above. Do NOT invent column names.\n"
                  "Always use table aliases (g, f, d_*) in your SQL.\n")
    else:
        block += "\n\nACTUAL COLUMNS — use ONLY these exact names:\n"
        block = block.replace(
            "PRIMARY TABLE (alias 'g')",
            "TABLE")

    return block


def run(question: str, on_step=None) -> dict:
    """
    Run the full Graph-RAG pipeline with schema enrichment.
    on_step(icon, msg) called at each stage for live UI progress.
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

    pt_name = tbl["name"]
    pt_plat = tbl.get("platform", "databricks").lower()
    layer   = tbl.get("layer", "gold").lower()
    scores  = tbl.get("scores", {})

    if pt_plat == "snowflake":
        fqn = f"KG_VS_POC.{layer.upper()}.{pt_name.upper()}"
    else:
        fqn = f"{CATALOG}.{layer}.{pt_name}"

    plat_emoji = "❄️ Snowflake" if pt_plat == "snowflake" else "🟠 Databricks"
    record("🎯", f"Selected: **{pt_name}** ({layer}) on {plat_emoji}")

    # ── 3. Schema fetch ───────────────────────────────────────────────────────
    record("📋", f"Fetching schema — {fqn}")
    gold_cols = _get_schema(pt_name, pt_plat, layer, token)
    record("📋", f"Schema: {len(gold_cols)} columns")

    # ── 4. Schema describe shortcut ───────────────────────────────────────────
    if sr.SCHEMA_PAT.search(question):
        record("📖", "Schema question — describing table (no SQL needed)")
        col_lines = [f"  - {cn} ({ct})" if ct else f"  - {cn}"
                     for cn, ct in gold_cols[:50]]
        col_desc = "\n".join(col_lines) or "  (no column metadata)"
        kpi_rows = graphdb.query(
            f'SELECT ?kpiName WHERE {{'
            f' ?t biz:tableName ?tname ; biz:hasKPI ?k .'
            f' FILTER (LCASE(STR(?tname)) = "{pt_name.lower()}")'
            f' ?k biz:kpiName ?kpiName }}', token) or []
        kpi_text = ("\n\nKPIs: " + ", ".join(r.get("kpiName","")
                    for r in kpi_rows[:10])) if kpi_rows else ""
        context  = (f"Table: {fqn}\nPlatform: {pt_plat}  Layer: {layer}\n"
                    f"Columns ({len(gold_cols)}):\n{col_desc}{kpi_text}")
        record("✅", f"{len(gold_cols)} columns, {len(kpi_rows)} KPIs")
        try:
            answer = llm.invoke([HumanMessage(content=(
                f"Describe this DWH table to a business analyst.\n\n"
                f"Question: {question}\n\n{context}\n\n"
                f"3-5 sentences. Use actual column names.\n\nAnswer:"
            ))]).content.strip()
        except Exception:
            answer = context
        return {"source": f"{plat_emoji} → {fqn}", "sql": "",
                "raw_result":"",
                "result_rows":[{"Column":c,"Type":t or "—"} for c,t in gold_cols[:50]],
                "result_cols":["Column","Type"],"answer":answer,
                "platform":pt_plat,"is_structural":True,
                "error":"","steps":steps,"retrieval_scores":scores}

    # ── 5. Schema enrichment ──────────────────────────────────────────────────
    # Only enrich Databricks tables — Snowflake tables are self-contained
    enrichment = {"needs_join":False,"join_tables":[],"needs_time":False,
                  "time_expr":"","fact_table":None,"fact_cols":[]}

    if pt_plat == "databricks":
        record("🔗", "Checking if question needs Silver table joins…")
        enrichment = _enrich_schema(
            question, pt_name, gold_cols, pt_plat, CATALOG, token)

        if enrichment["needs_join"]:
            joined = [jt["silver_table"] for jt in enrichment["join_tables"]]
            record("🔗", f"Enriching with Silver dims: {', '.join(joined)}")
        if enrichment["needs_time"] and enrichment["fact_table"]:
            record("🔗", f"Adding time dimension via {enrichment['fact_table']}")
        if not enrichment["needs_join"] and not enrichment["needs_time"]:
            record("✅", "Gold table has all required columns — no join needed")

    # ── 6. Build schema context ───────────────────────────────────────────────
    schema_block = _build_schema_block(
        pt_name, fqn, gold_cols, enrichment, CATALOG, pt_plat)

    all_time = bool(re.search(
        r"\ball\s+(months?|time|years?|periods?|data|history|available)\b"
        r"|\bover\s+(all|entire|the\s+whole)\b|\bhistorical\b|\bevery\s+month\b",
        question, re.I))
    date_rule = (
        "DATE: No date filter — return ALL rows."
        if all_time else
        "DATE: Only add a date filter if question explicitly says "
        "'last quarter/month/year'. No time qualifier → no date filter.")

    # ── 7. Generate SQL ───────────────────────────────────────────────────────
    record("✍️", "Generating SQL…")
    try:
        sql_raw = llm.invoke([HumanMessage(content=(
            f"You are a SQL expert for a retail Sales Data Warehouse.\n\n"
            f"Question: {question}\n\n{schema_block}\n\n"
            f"STRICT RULES:\n"
            f"1. ONLY use column names confirmed in the schema above. "
            f"Do NOT invent column names.\n"
            f"2. Use table aliases exactly as shown (g, f, d_*).\n"
            f"3. {date_rule}\n"
            f"4. Never use current_date() — data is from 2020-2024.\n"
            f"5. ORDER BY must use alias names not aggregate expressions.\n"
            f"6. Categorical values are snake_case e.g. 'credit_card'.\n"
            f"7. Add LIMIT 200 unless question asks for a single total.\n"
            f"8. Snowflake date filter: use DATEADD not INTERVAL syntax.\n"
            f"9. Return ONLY the SQL. No explanation. No markdown. No backticks.\n\nSQL:"
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

    # ── 8. Execute SQL ────────────────────────────────────────────────────────
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
        record("📊", f"Query returned {n} row{'s' if n!=1 else ''}")

    # ── 9. Generate answer ────────────────────────────────────────────────────
    record("💬", "Generating answer…")
    try:
        answer = llm.invoke([HumanMessage(content=(
            f"You are a senior business analyst.\n\n"
            f"Question: {question}\nTable: {fqn}\n"
            f"SQL Result:\n{sql_result[:3000]}\n\n"
            f"Write a concise 2-4 sentence insight. "
            f"Include the most important numbers. "
            f"Do NOT reproduce the full table.\n\nAnswer:"
        ))]).content.strip()
    except Exception as e:
        answer = sql_result[:500] if sql_result else f"Error: {e}"

    record("✅", "Done")

    plat_label = "Snowflake" if pt_plat == "snowflake" else "Databricks"
    return {"source":f"{plat_label} → {fqn}", "sql":sql,
            "raw_result":sql_result, "result_rows":result_rows,
            "result_cols":result_cols, "answer":answer,
            "platform":pt_plat, "is_structural":False,
            "error":"", "steps":steps, "retrieval_scores":scores}


def _err(msg: str, steps: list = None) -> dict:
    return {"source":"","sql":"","answer":msg,"platform":"",
            "result_rows":[],"result_cols":[],
            "is_structural":False,"error":msg,
            "steps":steps or [],"retrieval_scores":{}}
