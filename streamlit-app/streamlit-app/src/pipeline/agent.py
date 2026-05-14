"""
src/pipeline/agent.py — Graph-RAG agent v2 with dynamic schema enrichment.

Pipeline:
  1. Structural SPARQL check    → metadata questions answered from GraphDB
  2. Hybrid retrieval           → best Gold/Silver table via FTS+VS+SPARQL
  3. Schema fetch               → Gold table columns from GraphDB
  4. Dynamic schema enrichment  → compare user intent vs Gold columns;
                                   find missing columns in Silver dims via GraphDB;
                                   build multi-table join context automatically
  5. Schema-describe shortcut   → "what columns does X have" — no SQL needed
  6. SQL generation             → LLM with full enriched schema
  7. SQL fix                    → INTERVAL syntax, cleanup
  8. SQL execution              → Databricks SQL Warehouse or Snowflake connector
  9. Answer generation          → LLM narrative summary
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
    """Fetch columns from GraphDB. Fallback to Databricks INFORMATION_SCHEMA."""
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


@st.cache_data(show_spinner=False, ttl=600)
def _get_all_silver_tables(_token: str) -> dict:
    """
    Fetch ALL Silver tables and their columns from GraphDB.
    Returns: {table_name: {cols: [(name,type)], col_set: {name}}}
    Cached for 10 minutes.
    """
    rows = graphdb.query(
        'SELECT ?tname ?colName ?dataType WHERE {'
        ' ?t biz:tableName ?tname ;'
        '    biz:tableLayer "silver" ;'
        '    biz:hasColumn ?col .'
        ' ?col biz:columnName ?colName .'
        ' OPTIONAL { ?col biz:dataType ?dataType }'
        '} ORDER BY ?tname ?colName', _token) or []

    tables: dict = {}
    for r in rows:
        tn  = r.get("tname","")
        col = r.get("colName","")
        dt  = r.get("dataType","")
        if not tn or not col:
            continue
        if tn not in tables:
            tables[tn] = {"cols": [], "col_set": set()}
        tables[tn]["cols"].append((col, dt))
        tables[tn]["col_set"].add(col.lower())
    return tables


def _dynamic_enrich(
    question: str,
    gold_table: str,
    gold_cols: list,
    platform: str,
    catalog: str,
    token: str,
) -> dict:
    """
    Fully dynamic schema enrichment.

    Steps:
    1. Ask the LLM: "What dimensions or columns does this question need?"
       Returns a list of logical concepts: [product_name, sku, month, customer_name ...]
    2. Check which of those concepts exist in the Gold table columns.
    3. For missing concepts, search ALL Silver tables in GraphDB for a table
       that (a) has the needed column AND (b) can be joined to Gold via a FK.
    4. Build a join plan: list of {silver_table, gold_fk, silver_pk, needed_cols}.

    Everything is driven from GraphDB — no hardcoded table names.
    """
    q_lower = question.lower()
    gold_col_names = {c.lower() for c, _ in gold_cols}
    gold_fks = {c.lower() for c in gold_col_names
                if c.endswith("_sk") or c.endswith("_id") or c.endswith("_key")}

    # ── Entity noun patterns — simple single-word matching ────────────────────
    # Maps entity noun → identifier columns user implicitly expects to see.
    # "customer" alone means user wants full_name/email, not just customer_id.
    # Using simple \bword\b matching — no multi-word phrases required.
    ENTITY_NOUN_PATTERNS = {
    "customer":  ('\\bcustomer[s]?\\b', ["full_name", "first_name", "last_name", "customer_name", "email", "segment", "city", "state_code", "age_band"]),
    "product":  ('\\bproduct[s]?\\b', ["product_name", "sku", "brand", "category", "sub_category"]),
    "sku":  ('\\bsku[s]?\\b', ["product_name", "sku", "brand", "category", "sub_category"]),
    "store":  ('\\bstore[s]?\\b', ["store_name", "store_code", "city", "region", "store_type", "state"]),
    "employee":  ('\\bemployee[s]?\\b', ["full_name", "employee_name", "department", "role", "first_name", "last_name"]),
    "supplier":  ('\\bsupplier[s]?\\b', ["supplier_name", "country", "reliability_grade", "lead_time_band"]),
    "vendor":  ('\\bvendor[s]?\\b', ["supplier_name", "country", "reliability_grade"]),
    "promotion":  ('\\bpromotion[s]?\\b', ["promo_name", "promotion_name", "promo_type", "discount_pct"]),
    "brand":  ('\\bbrand[s]?\\b', ["brand", "product_name", "category"]),
}

    TIME_NOUNS = [
        r"\bmonth\b",r"\bmonthly\b",r"\byear\b",r"\byearly\b",
        r"\bquarter\b",r"\bquarterly\b",r"\bdaily\b",r"\btrend\b",
        r"\bover\s+time\b",r"\byear.month\b",r"\btime\s+series\b",
        r"\bby\s+date\b",r"\bperiod\b",r"\bweek\b",r"\bweekly\b",
    ]

    # Detect entity nouns — deduplicate (sku and product both map to dim_product)
    needed_entities: dict = {}  # noun → required_cols
    for noun, (pat, id_cols) in ENTITY_NOUN_PATTERNS.items():
        if re.search(pat, q_lower):
            already = any(set(id_cols) & set(existing)
                          for existing in needed_entities.values())
            if not already:
                needed_entities[noun] = id_cols

    needs_time = any(re.search(pat, q_lower) for pat in TIME_NOUNS)
    if needs_time:
        needed_entities["_time"] = ["month","year_month","order_date","date","quarter"]

    if not needed_entities:
        return {"needs_join": False, "join_plan": [], "log": "No additional dimensions needed"}

    # ── Check which are already satisfied by Gold table ───────────────────────
    missing: dict = {}
    for noun, id_cols in needed_entities.items():
        already_have = any(c.lower() in gold_col_names for c in id_cols)
        if not already_have:
            missing[noun] = id_cols

    if not missing:
        return {"needs_join": False, "join_plan": [],
                "log": "Gold table already has all required columns"}

    # ── Fetch Silver tables from GraphDB ──────────────────────────────────────
    silver_tables = _get_all_silver_tables(token)

    # ── Find best Silver table for each missing entity ────────────────────────
    join_plan   = []
    used_tables = set()

    for noun, required_cols in missing.items():
        best_table    = None
        best_cols     = []
        best_join_col = None
        best_gold_fk  = None

        for silver_tname, silver_info in silver_tables.items():
            if silver_tname in used_tables:
                continue

            silver_col_set = silver_info["col_set"]

            matching = [c for c in required_cols if c.lower() in silver_col_set]
            if not matching:
                continue

            # Find join key — match Gold FK base name to Silver column base name
            jk_gold = None; jk_silver = None
            for gfk in gold_fks:
                base_g = re.sub(r"(_sk|_id|_key)$","",gfk)
                for sfk in silver_col_set:
                    base_s = re.sub(r"(_sk|_id|_key)$","",sfk.lower())
                    if base_g == base_s or gfk == sfk.lower():
                        jk_gold   = gfk
                        jk_silver = sfk
                        break
                if jk_gold: break

            if not jk_gold:
                continue

            if len(matching) > len(best_cols):
                best_table    = silver_tname
                best_cols     = matching
                best_join_col = jk_silver
                best_gold_fk  = jk_gold

        if best_table:
            used_tables.add(best_table)
            join_plan.append({
                "entity":       entity,
                "silver_table": best_table,
                "gold_fk":      best_gold_fk,
                "silver_pk":    best_join_col,
                "needed_cols":  best_cols,
                "all_cols":     silver_tables[best_table]["cols"],
                "alias":        f"d_{best_table}",
            })

    return {
        "needs_join": bool(join_plan),
        "join_plan":  join_plan,
        "log": (f"Joining: {', '.join(j['silver_table'] for j in join_plan)}"
                if join_plan else "No matching Silver tables found for enrichment"),
    }


def _build_schema_block(
    gold_table: str,
    gold_fqn: str,
    gold_cols: list,
    enrichment: dict,
    catalog: str,
    platform: str,
) -> str:
    """Build the full schema context for the LLM including any Silver join tables."""
    gold_col_lines = [f"  - {cn} [{ct}]" if ct else f"  - {cn}"
                      for cn, ct in gold_cols[:40]]
    gold_block = ("\n".join(gold_col_lines)
                  if gold_col_lines else "  (no column metadata available)")

    if not enrichment.get("needs_join"):
        return (f"TABLE: {gold_fqn}\n"
                f"Platform: {platform}   Layer: gold\n\n"
                f"ACTUAL COLUMNS — use ONLY these exact names:\n{gold_block}")

    # Multi-table context
    block = (f"GOLD TABLE (alias 'g'): {gold_fqn}\n"
             f"Platform: {platform}   Layer: gold\n\n"
             f"Gold columns (use alias g.column_name):\n{gold_block}\n\n"
             f"{'─'*60}\n"
             f"REQUIRED JOINS — the question needs columns not in the Gold table.\n"
             f"Use the following Silver dimension tables:\n")

    for jt in enrichment["join_plan"]:
        dim_fqn     = f"{catalog}.silver.{jt['silver_table']}"
        alias       = jt["alias"]
        needed_text = ", ".join(jt["needed_cols"])
        all_col_text= ", ".join(c for c,_ in jt["all_cols"][:20])
        block += (
            f"\nDIM TABLE (alias '{alias}'): {dim_fqn}\n"
            f"  JOIN ON: g.{jt['gold_fk']} = {alias}.{jt['silver_pk']}\n"
            f"  COLUMNS NEEDED FOR THIS QUESTION: {needed_text}\n"
            f"  ALL AVAILABLE COLUMNS: {all_col_text}\n"
        )

    block += (
        f"\n{'─'*60}\n"
        f"RULES FOR THIS MULTI-TABLE QUERY:\n"
        f"1. Always use table aliases (g, {', '.join(j['alias'] for j in enrichment['join_plan'])}).\n"
        f"2. Only use column names confirmed above — do NOT invent columns.\n"
        f"3. Include JOIN clauses for every dim table listed above.\n"
    )
    return block


def run(question: str, on_step=None) -> dict:
    from langchain_core.messages import HumanMessage
    steps: list = []

    def record(icon: str, msg: str):
        steps.append({"icon": icon, "msg": msg})
        if on_step: on_step(icon, msg)

    CATALOG = config.get("DATABRICKS_CATALOG", "kg_vs_poc")

    record("🔍", "Connecting to knowledge graph…")
    token, err = graphdb.get_token()
    if err:
        record("❌", f"GraphDB: {err}")
        return _err(err, steps)

    llm = _get_llm()

    # ── 1. Structural SPARQL ──────────────────────────────────────────────────
    from src.retrieval import sparql_retriever as sr
    record("🔍", "Is this a metadata question?")
    structural = sr.check_structural(question, token)
    if structural:
        record("📖", f"Yes — answering from GraphDB ({len(structural['rows'])} rows)")
        try:
            answer = llm.invoke([HumanMessage(content=(
                f"Summarise these DWH results in 3-5 sentences:\n\n"
                f"{structural['text']}\n\nQuestion: {question}\nAnswer:"
            ))]).content.strip()
        except Exception:
            answer = structural["text"]
        return {"source":"GraphDB","sql":"","raw_table":structural["text"],
                "answer":answer,"result_rows":[],"result_cols":[],
                "platform":"graphdb","is_structural":True,
                "error":"","steps":steps,"retrieval_scores":{}}

    record("🗺️", "Data question — running hybrid retrieval…")

    # ── 2. Hybrid retrieval ───────────────────────────────────────────────────
    tbl = find_best_table(question, token, record=record)
    if not tbl:
        record("❌", "Could not identify a relevant table")
        return _err("Could not identify a relevant table. Try rephrasing.", steps)

    pt_name = tbl["name"]
    pt_plat = tbl.get("platform","databricks").lower()
    layer   = tbl.get("layer","gold").lower()
    scores  = tbl.get("scores", {})

    fqn        = (f"KG_VS_POC.{layer.upper()}.{pt_name.upper()}"
                  if pt_plat == "snowflake"
                  else f"{CATALOG}.{layer}.{pt_name}")
    plat_emoji = "❄️ Snowflake" if pt_plat == "snowflake" else "🟠 Databricks"
    record("🎯", f"Selected: **{pt_name}** ({layer}) on {plat_emoji}")

    # ── 3. Schema fetch ───────────────────────────────────────────────────────
    record("📋", f"Fetching schema — {fqn}")
    gold_cols = _get_schema(pt_name, pt_plat, layer, token)
    record("📋", f"Gold table: {len(gold_cols)} columns")

    # ── 4. Schema describe shortcut ───────────────────────────────────────────
    if sr.SCHEMA_PAT.search(question):
        record("📖", "Schema question — no SQL needed")
        col_lines = [f"  - {cn} ({ct})" if ct else f"  - {cn}"
                     for cn, ct in gold_cols[:50]]
        context = (f"Table: {fqn}\nPlatform: {pt_plat}  Layer: {layer}\n"
                   f"Columns ({len(gold_cols)}):\n" +
                   ("\n".join(col_lines) or "  (no column metadata)"))
        try:
            answer = llm.invoke([HumanMessage(content=(
                f"Describe this DWH table to a business analyst.\n\n"
                f"Question: {question}\n\n{context}\n\n"
                f"3-5 sentences. Use actual column names.\n\nAnswer:"
            ))]).content.strip()
        except Exception:
            answer = context
        return {"source":f"{plat_emoji} → {fqn}", "sql":"",
                "raw_result":"",
                "result_rows":[{"Column":c,"Type":t or "—"} for c,t in gold_cols[:50]],
                "result_cols":["Column","Type"],"answer":answer,
                "platform":pt_plat,"is_structural":True,
                "error":"","steps":steps,"retrieval_scores":scores}

    # ── 5. Dynamic schema enrichment ──────────────────────────────────────────
    # Only for Databricks — Snowflake Gold tables are standalone aggregates
    enrichment = {"needs_join": False, "join_plan": [], "log": ""}

    if pt_plat == "databricks":
        record("🔗", "Analysing what dimensions your question needs…")
        enrichment = _dynamic_enrich(
            question, pt_name, gold_cols, pt_plat, CATALOG, token)
        record("🔗", enrichment["log"])

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
        "DATE: Only add a date filter if the question explicitly says "
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
            f"2. Use table aliases exactly as shown.\n"
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
        record("❌", "SQL error — see SQL expander for details")
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
            "raw_result":sql_result,"result_rows":result_rows,
            "result_cols":result_cols,"answer":answer,
            "platform":pt_plat,"is_structural":False,
            "error":"","steps":steps,"retrieval_scores":scores}


def _err(msg: str, steps: list = None) -> dict:
    return {"source":"","sql":"","answer":msg,"platform":"",
            "result_rows":[],"result_cols":[],"is_structural":False,
            "error":msg,"steps":steps or [],"retrieval_scores":{}}
