"""
src/agent.py — Graph-RAG pipeline.

LLM: ChatDatabricks(endpoint=..., temperature=0)  — exact pattern from working app.
     env vars DATABRICKS_HOST + DATABRICKS_TOKEN set at module level in app.py.

Routing:
  1. Structural SPARQL patterns  → graph answer (fast)
  2. SPARQL keyword table-match  → primary Gold table
  3. SQL Warehouse column fetch   → real schema
  4. SQL generation via LLM
  5. Execute SQL → natural-language answer via LLM
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


# ── LLM — exact pattern from working reference app ────────────────────────────
# env vars are set at module level in app.py before this is ever called.

@st.cache_resource(show_spinner=False)
def _get_llm():
    from databricks_langchain import ChatDatabricks
    return ChatDatabricks(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        temperature=0,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _q_tokens(question: str) -> set:
    return {t for t in re.findall(r"\b[a-zA-Z]{3,}\b", question.lower())
            if t not in STOP}


def _rows_to_text(rows: list) -> str:
    if not rows:
        return "No results."
    cols = list(rows[0].keys())
    w    = max(22, max(len(c) for c in cols))
    hdr  = " | ".join(f"{c:<{w}}" for c in cols)
    sep  = "-" * len(hdr)
    body = "\n".join(" | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols)
                     for r in rows[:60])
    return f"{hdr}\n{sep}\n{body}"


def _find_gold_table(question: str, token: str) -> dict:
    """SPARQL keyword match against all Gold tables + their column names."""
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
        if not tn:
            continue
        if tn not in tables:
            tables[tn] = {"platform": r.get("platform", ""), "tokens": set()}
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

    # IDF-weighted token overlap
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

    if not scores:
        best = next(iter(tables))
    else:
        best = max(scores, key=scores.get)
    return {"name": best, **tables[best]}


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(question: str) -> dict:
    """Run the Graph-RAG pipeline. Returns dict: source/sql/answer/error."""
    from langchain_core.messages import HumanMessage

    token, err = graphdb.get_token()
    if err:
        return {"source":"","sql":"","answer":"","platform":"",
                "is_structural":False,"error":err}

    llm = _get_llm()

    # ── 1. Structural SPARQL patterns ────────────────────────────────────────
    for pat, sparql in STRUCTURAL_PATTERNS:
        if pat.search(question):
            rows      = graphdb.query(sparql, token) or []
            tbl_text  = _rows_to_text(rows)
            try:
                answer = llm.invoke([HumanMessage(content=(
                    f"Summarise these data warehouse results in 3-5 sentences:\n\n"
                    f"{tbl_text}\n\nQuestion: {question}\nAnswer:"
                ))]).content.strip()
            except Exception:
                answer = tbl_text
            return {"source":"GraphDB (knowledge graph)","sql":"",
                    "raw_table":tbl_text,"answer":answer,
                    "platform":"graphdb","is_structural":True,"error":""}

    # ── 2. Find best Gold table ──────────────────────────────────────────────
    tbl = _find_gold_table(question, token)
    if not tbl:
        return {"source":"","sql":"","answer":
                "Could not identify a relevant table. Try rephrasing.",
                "platform":"","is_structural":False,"error":""}

    pt_name = tbl["name"]
    pt_plat = (tbl.get("platform") or "").lower()
    layer   = "gold"
    fqn     = (f"KG_POC.GOLD.{pt_name}" if pt_plat == "snowflake"
               else f"KG_POC.{layer}.{pt_name}")

    # ── 3. Real column list from SQL Warehouse ───────────────────────────────
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
            f'?col biz:columnName ?colName . }} ORDER BY ?colName',
            token) or []
        actual_cols = [(r.get("colName",""), "") for r in col_rows if r.get("colName")]

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

    # ── 5. Build context block ────────────────────────────────────────────────
    col_lines = []
    for cn, ct in actual_cols[:40]:
        line = f"  - {cn} [{ct}]"
        if cn in col_samples:
            line += f"  (sample values: {', '.join(repr(v) for v in col_samples[cn])})"
        col_lines.append(line)

    schema_block = (
        f"TABLE: {fqn}\n"
        f"Platform: {pt_plat}   Layer: {layer}\n\n"
        f"Columns (use ONLY these — exact case):\n"
        + ("\n".join(col_lines) if col_lines else "  (no column metadata available)")
    )

    # ── 6. Generate SQL ──────────────────────────────────────────────────────
    sql_prompt = (
        f"You are a SQL expert for a retail Sales DWH.\n\n"
        f"Question: {question}\n\n"
        f"{schema_block}\n\n"
        f"Rules:\n"
        f"1. Use ONLY column names listed above.\n"
        f"2. Always qualify the table as: {fqn}\n"
        f"3. DATE: use subquery MAX — e.g. WHERE year_quarter = (SELECT MAX(year_quarter) FROM {fqn})\n"
        f"4. No current_date() — data is 2020–2024.\n"
        f"5. ORDER BY: use alias names, not aggregate expressions.\n"
        f"6. Categorical values are snake_case, e.g. 'credit_card'.\n"
        f"7. Return ONLY the SQL — no explanation, no backticks.\n\nSQL:"
    )
    sql_raw = llm.invoke([HumanMessage(content=sql_prompt)]).content.strip()
    # Clean fences
    for fence in ["```sql", "```SQL", "```"]:
        sql_raw = sql_raw.replace(fence, "")
    sql = sql_raw.strip()
    # Trim anything before SELECT/WITH
    for kw in ("WITH", "SELECT"):
        idx = sql.upper().find(kw)
        if idx > 0:
            sql = sql[idx:]
            break

    # ── 7. Execute SQL ────────────────────────────────────────────────────────
    try:
        if pt_plat == "snowflake":
            sql_result = sql_exec.execute_snowflake(sql)
        else:
            sql_result = sql_exec.execute_databricks(sql)
    except Exception as e:
        sql_result = f"SQL error: {e}"

    # ── 8. Generate answer ────────────────────────────────────────────────────
    ans_prompt = (
        f"You are a senior business analyst.\n\n"
        f"Question: {question}\n"
        f"Table: {fqn}\n"
        f"SQL Result:\n{sql_result[:3000]}\n\n"
        f"Write a clear 3-5 sentence answer. Include key numbers. "
        f"Do not repeat the SQL.\n\nAnswer:"
    )
    answer = llm.invoke([HumanMessage(content=ans_prompt)]).content.strip()

    plat_label = "Snowflake" if pt_plat == "snowflake" else "Databricks"
    return {
        "source":       f"{plat_label} → {fqn}",
        "sql":          sql,
        "raw_result":   sql_result,
        "answer":       answer,
        "platform":     pt_plat,
        "is_structural":False,
        "error":        "",
    }
