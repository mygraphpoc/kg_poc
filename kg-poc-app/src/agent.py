"""
src/agent.py — Graph-RAG pipeline with live step callbacks.

agent.run(question, on_step=None)
  on_step(icon, message) is called at each pipeline stage so the UI can
  display a live progress log while the agent is running.
"""

import re
import streamlit as st
from src import config, graphdb, sql_exec

STOP = {
    "the","a","an","is","are","of","in","on","at","by","for","with","from","to",
    "and","or","but","not","this","that","what","which","who","how",
    "show","list","get","tell","year","month","quarter","week","day","time",
}

STRUCTURAL_PATTERNS = [
    (re.compile(r"fact\s+table", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:FactTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"dimension\s+table|all\s+dim", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:DimensionTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"gold\s+table|aggregate\s+table|kpi\s+table|all\s+gold", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "gold" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    (re.compile(r"pii|personally\s+identifiable", re.I),
     "SELECT DISTINCT ?tableName ?layer WHERE { ?t biz:tableName ?tableName ; biz:hasColumn ?col . ?col biz:isPII true . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName"),
    (re.compile(r"snowflake\s+table|tables?\s+on\s+snowflake", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "snowflake" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    (re.compile(r"databricks\s+table|tables?\s+on\s+databricks", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "databricks" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    (re.compile(r"kpis?\s+available|all\s+kpis?|list\s+kpis?", re.I),
     "SELECT ?kpiName ?domain ?direction WHERE { ?k a biz:KPI ; biz:kpiName ?kpiName . OPTIONAL { ?k biz:kpiDomain ?domain } OPTIONAL { ?k biz:kpiDirection ?direction } } ORDER BY ?domain ?kpiName"),
    (re.compile(r"silver\s+table|silver\s+layer", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "silver" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    (re.compile(r"lineage|feeds?\s+into|cross.platform", re.I),
     "SELECT ?srcName ?srcPlatform ?tgtName ?tgtPlatform ?transformType WHERE { ?src biz:feedsInto ?tgt . ?src biz:tableName ?srcName . ?tgt biz:tableName ?tgtName . OPTIONAL { ?src biz:sourceSystemType ?srcPlatform } OPTIONAL { ?tgt biz:sourceSystemType ?tgtPlatform } ?edge biz:sourceTable ?src ; biz:targetTable ?tgt . OPTIONAL { ?edge biz:lineageTransformType ?transformType } } ORDER BY ?srcName"),
]


@st.cache_resource(show_spinner=False)
def _get_llm():
    from databricks_langchain import ChatDatabricks
    return ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct", temperature=0)


def _q_tokens(question: str) -> set:
    return {t for t in re.findall(r"\b[a-zA-Z]{3,}\b", question.lower())
            if t not in STOP}


def _rows_to_text(rows: list) -> str:
    if not rows:
        return "No results."
    cols = list(rows[0].keys())
    w    = max(22, max(len(c) for c in cols))
    hdr  = " | ".join(f"{c:<{w}}" for c in cols)
    return f"{hdr}\n{'-'*len(hdr)}\n" + "\n".join(
        " | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols)
        for r in rows[:60])


def _find_gold_table(question: str, token: str) -> dict:
    rows = graphdb.query("""
        SELECT DISTINCT ?tname ?platform ?cname WHERE {
            ?t biz:tableName ?tname ; biz:tableLayer ?layer .
            FILTER (LCASE(STR(?layer)) = "gold")
            OPTIONAL { ?t biz:sourceSystemType ?platform }
            OPTIONAL { ?t biz:hasColumn ?col . ?col biz:columnName ?cname }
        }""", token) or []

    tables: dict = {}
    for r in rows:
        tn = r.get("tname", "")
        if not tn: continue
        if tn not in tables:
            tables[tn] = {"platform": r.get("platform",""), "tokens": set()}
        for part in re.findall(r"[a-zA-Z]+", tn.lower()):
            tables[tn]["tokens"].add(part)
        if r.get("cname"):
            for part in re.findall(r"[a-zA-Z]+", r["cname"].lower()):
                tables[tn]["tokens"].add(part)

    if not tables:
        return {}

    q_tok = _q_tokens(question)
    if not q_tok:
        return next(iter({"name": k, **v} for k, v in tables.items()), {})

    tok_to_tables: dict = {}
    for tn, info in tables.items():
        for qt in q_tok:
            if any(qt == s or (len(qt) >= 4 and len(s) >= 4
                               and (qt.startswith(s) or s.startswith(qt)))
                   for s in info["tokens"]):
                tok_to_tables.setdefault(qt, set()).add(tn)

    scores: dict = {}
    for qt, tnames in tok_to_tables.items():
        idf = 1.0 / len(tnames)
        for tn in tnames:
            scores[tn] = scores.get(tn, 0.0) + idf

    best = max(scores, key=scores.get) if scores else next(iter(tables))
    return {"name": best, **tables[best]}


def run(question: str, on_step=None) -> dict:
    """
    Run the Graph-RAG pipeline.
    on_step(icon, message) is called at each stage for live UI progress.
    Returns dict: source / sql / answer / result_rows / result_cols / error / steps
    """
    def step(icon: str, msg: str):
        if on_step:
            on_step(icon, msg)

    steps: list[dict] = []
    def record(icon, msg):
        steps.append({"icon": icon, "msg": msg})
        step(icon, msg)

    from langchain_core.messages import HumanMessage

    record("🔍", "Checking knowledge graph for structural patterns…")
    token, err = graphdb.get_token()
    if err:
        record("❌", f"GraphDB connection failed: {err}")
        return _err(err, steps)

    llm = _get_llm()

    # ── 1. Structural SPARQL ─────────────────────────────────────────────────
    for pat, sparql in STRUCTURAL_PATTERNS:
        if pat.search(question):
            record("📖", "Matched structural pattern — querying GraphDB directly")
            rows      = graphdb.query(sparql, token) or []
            tbl_text  = _rows_to_text(rows)
            record("✅", f"GraphDB returned {len(rows)} rows — generating summary")
            try:
                answer = llm.invoke([HumanMessage(content=(
                    f"Summarise these data warehouse results in 3-5 sentences:\n\n"
                    f"{tbl_text}\n\nQuestion: {question}\nAnswer:"
                ))]).content.strip()
            except Exception:
                answer = tbl_text
            return {"source":"GraphDB (knowledge graph)","sql":"",
                    "raw_table":tbl_text,"answer":answer,
                    "result_rows":[],"result_cols":[],
                    "platform":"graphdb","is_structural":True,"error":"","steps":steps}

    record("🗺️", "No structural match — searching vector index + knowledge graph")

    # ── 2. Find Gold table ───────────────────────────────────────────────────
    tbl = _find_gold_table(question, token)
    if not tbl:
        record("❌", "Could not identify a relevant table")
        return _err("Could not identify a relevant table. Try rephrasing.", steps)

    pt_name = tbl["name"]
    pt_plat = (tbl.get("platform") or "").lower()
    layer   = "gold"
    fqn     = (f"KG_POC.GOLD.{pt_name}" if pt_plat == "snowflake"
               else f"KG_POC.{layer}.{pt_name}")

    plat_emoji = "❄️ Snowflake" if pt_plat == "snowflake" else "🟠 Databricks"
    record("🎯", f"Identified table: **{pt_name}** on {plat_emoji}")

    # ── 3. Fetch real columns ────────────────────────────────────────────────
    record("📋", f"Fetching real schema from {plat_emoji} — {fqn}")
    actual_cols: list = []
    try:
        if pt_plat == "snowflake":
            actual_cols = sql_exec.snowflake_columns(pt_name)
        else:
            actual_cols = sql_exec.databricks_columns(pt_name, layer)
    except Exception:
        pass

    if not actual_cols:
        col_rows = graphdb.query(
            f'SELECT ?colName WHERE {{ ?t biz:tableName ?tname ; biz:hasColumn ?col . '
            f'FILTER (LCASE(STR(?tname)) = "{pt_name.lower()}") '
            f'?col biz:columnName ?colName . }} ORDER BY ?colName', token) or []
        actual_cols = [(r.get("colName",""), "") for r in col_rows if r.get("colName")]
        if actual_cols:
            record("📋", f"Schema from GraphDB — {len(actual_cols)} columns")
    else:
        record("📋", f"Schema confirmed — {len(actual_cols)} columns")

    # ── 4. Sample categorical columns ────────────────────────────────────────
    q_tok = _q_tokens(question)
    col_samples: dict = {}
    for cn, ct in actual_cols[:20]:
        col_parts = set(re.findall(r"[a-zA-Z]+", cn.lower()))
        if not any(len(qt) >= 4 and any(qt.startswith(p) or p.startswith(qt)
                   for p in col_parts if len(p) >= 4) for qt in q_tok):
            continue
        if not any(x in (ct or "").lower() for x in ("char","string","text","varchar")):
            continue
        try:
            vals = (sql_exec.snowflake_distinct(fqn, cn) if pt_plat == "snowflake"
                    else sql_exec.databricks_distinct(fqn, cn))
            if vals:
                col_samples[cn] = vals
        except Exception:
            pass

    # ── 5. Build schema block ────────────────────────────────────────────────
    col_lines = []
    for cn, ct in actual_cols[:40]:
        line = f"  - {cn} [{ct}]"
        if cn in col_samples:
            line += f"  (sample values: {', '.join(repr(v) for v in col_samples[cn])})"
        col_lines.append(line)

    schema_block = (
        f"TABLE: {fqn}\n"
        f"Platform: {pt_plat}   Layer: {layer}\n\n"
        f"ACTUAL COLUMNS — use ONLY these exact names:\n"
        + ("\n".join(col_lines) if col_lines else "  (no column metadata available)")
    )

    # ── 6. Generate SQL ──────────────────────────────────────────────────────
    all_time = bool(re.search(
        r"\ball\s+(months?|time|years?|periods?|data|history|available)\b"
        r"|\bover\s+(all|entire|the\s+whole)\b"
        r"|\bno\s+date\s+filter\b|\bhistorical\b|\bevery\s+month\b",
        question, re.I))

    date_rule = (
        "3. DATE: NO date filter — return ALL rows (question asks for all data/months)."
        if all_time else
        f"3. DATE: Only add a date filter if the question explicitly says 'last quarter/month/year'. "
        f"If no time qualifier is present, do NOT add any WHERE clause on date columns — return all rows."
    )

    record("✍️", "Generating SQL query…")
    sql_prompt = (
        f"You are a SQL expert for a retail Sales DWH.\n\n"
        f"Question: {question}\n\n"
        f"{schema_block}\n\n"
        f"Rules:\n"
        f"1. Use ONLY column names from ACTUAL COLUMNS above — exact spelling, exact case.\n"
        f"2. Always qualify: {fqn}\n"
        f"{date_rule}\n"
        f"4. No current_date() — data covers 2020–2024.\n"
        f"5. ORDER BY: use alias names, not aggregate expressions.\n"
        f"6. Categorical filter values are snake_case e.g. 'credit_card'.\n"
        f"7. Add LIMIT 200 unless the question asks for a single total.\n"
        f"8. Return ONLY the SQL — no explanation, no backticks, no markdown.\n\nSQL:"
    )
    sql_raw = llm.invoke([HumanMessage(content=sql_prompt)]).content.strip()
    for fence in ["```sql","```SQL","```"]:
        sql_raw = sql_raw.replace(fence, "")
    sql = sql_raw.strip()
    for kw in ("WITH","SELECT"):
        idx = sql.upper().find(kw)
        if idx > 0:
            sql = sql[idx:]; break

    record("⚡", f"Executing SQL on {plat_emoji}…")

    # ── 7. Execute ───────────────────────────────────────────────────────────
    sql_result = ""
    result_rows: list = []
    result_cols: list = []
    try:
        if pt_plat == "snowflake":
            sql_result, result_rows, result_cols = sql_exec.execute_snowflake(sql)
        else:
            sql_result, result_rows, result_cols = sql_exec.execute_databricks(sql)
    except Exception as e:
        sql_result = f"SQL error: {e}"

    n_rows = len(result_rows)
    if "error" in sql_result.lower()[:20]:
        record("❌", f"SQL execution error — check SQL expander")
    else:
        record("📊", f"Query returned {n_rows} row{'s' if n_rows!=1 else ''}")

    # ── 8. Generate answer ───────────────────────────────────────────────────
    record("💬", "Generating natural-language answer…")
    ans_prompt = (
        f"You are a senior business analyst.\n\n"
        f"Question: {question}\n"
        f"Table: {fqn}\n"
        f"SQL Result (sample rows):\n{sql_result[:3000]}\n\n"
        f"Write a concise 2-4 sentence narrative summary of the KEY insight. "
        f"Include the most important numbers. "
        f"Do NOT reproduce the full table — a data table is already shown separately.\n\nAnswer:"
    )
    answer = llm.invoke([HumanMessage(content=ans_prompt)]).content.strip()
    record("✅", "Done")

    plat_label = "Snowflake" if pt_plat == "snowflake" else "Databricks"
    return {
        "source":        f"{plat_label} → {fqn}",
        "sql":           sql,
        "raw_result":    sql_result,
        "result_rows":   result_rows,
        "result_cols":   result_cols,
        "answer":        answer,
        "platform":      pt_plat,
        "is_structural": False,
        "error":         "",
        "steps":         steps,
    }


def _err(msg: str, steps: list = None) -> dict:
    return {"source":"","sql":"","answer":msg,"platform":"",
            "result_rows":[],"result_cols":[],
            "is_structural":False,"error":msg,"steps":steps or []}
