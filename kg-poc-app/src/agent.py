"""
src/agent.py — Graph-RAG pipeline with live step callbacks.

run(question, on_step=None) calls on_step(icon, msg) at each stage
so the UI can render a live progress log.
"""

import re
import math
import streamlit as st
from src import config, graphdb, sql_exec

# ── Constants ─────────────────────────────────────────────────────────────────

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

# Detects "what columns / what data does table X have" — no SQL needed
_SCHEMA_PAT = re.compile(
    r"what\s+(kind|type|sort|column|field|data|info|information|detail)"
    r"|what\s+does\s+.+\s+(table|have|contain|include|store|hold)"
    r"|describe\s+(the\s+)?(table|column|schema|structure)"
    r"|show\s+(me\s+)?(the\s+)?(column|schema|structure|field)"
    r"|what\s+is\s+(in|inside|stored|available)\s+(the\s+)?table"
    r"|columns?\s+(in|of|for|available)"
    r"|table\s+structure|schema\s+of",
    re.I,
)

# Direct keyword → table overrides (pattern, table_name_substring, score_boost)
_OVERRIDES = [
    (r"\bcustomer\b",                    "customer_360",       20.0),
    (r"\bchurn\b",                       "customer_360",       20.0),
    (r"\bclv\b|\blifetime\s+value",      "customer_360",       20.0),
    (r"\bsegment\b",                     "customer_segment",   20.0),
    (r"\bcohort\b",                      "cohort",             20.0),
    (r"\bsupplier\b",                    "supplier",           20.0),
    (r"\bpayment\b",                     "payment",            20.0),
    (r"\bgeograph\b|\bstate\b|\bregion\b","geographic",        20.0),
    (r"\bexecutive\b|\bc.suite\b",       "executive",          20.0),
    (r"\bchannel\b",                     "channel",            20.0),
    (r"\bemployee\b|\bcommission\b",     "employee",           20.0),
    (r"\bstore\b|\bretail\b",            "store_performance",  20.0),
    (r"\bpromo\b|\bpromotion\b",         "promotion",          20.0),
    (r"\binventor\b",                    "inventory",          20.0),
    (r"\brevenue.*month\b|\bmonthly.*revenue\b","revenue_monthly", 20.0),
    (r"\brevenue.*daily\b|\bdaily.*revenue\b",  "revenue_daily",   20.0),
    (r"\bproduct\b",                     "product_performance",15.0),
]


# ── LLM ───────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _get_llm():
    from databricks_langchain import ChatDatabricks
    return ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct", temperature=0)


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
    return f"{hdr}\n{'-'*len(hdr)}\n" + "\n".join(
        " | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols)
        for r in rows[:60])


def _find_best_table(question: str, token: str) -> dict:
    """
    Pick the best table across ALL layers.
    - "dim/dimension" in question  -> prefer Silver dim_ tables
    - "fact/fct" in question       -> prefer Silver fct_ tables
    - anything else                -> prefer Gold KPI aggregates
    Scoring: table-name match=8x IDF, column match=1x IDF, preferred layer=+12, overrides=+15-20
    """
    q_lower   = question.lower()
    want_dim  = bool(re.search(r"\bdim(ension)?\b", q_lower))
    want_fact = bool(re.search(r"\bfact\b|\bfct\b", q_lower))
    want_gold = not want_dim and not want_fact

    rows = graphdb.query("""
        SELECT DISTINCT ?tname ?layer ?platform ?cname WHERE {
            ?t biz:tableName ?tname .
            OPTIONAL { ?t biz:tableLayer ?layer }
            OPTIONAL { ?t biz:sourceSystemType ?platform }
            OPTIONAL { ?t biz:hasColumn ?col . ?col biz:columnName ?cname }
        }""", token) or []

    tables: dict = {}
    for r in rows:
        tn = r.get("tname", "")
        if not tn:
            continue
        if tn not in tables:
            tables[tn] = {
                "platform": r.get("platform", ""),
                "layer":    (r.get("layer") or "").lower(),
                "name_tok": set(re.findall(r"[a-zA-Z]+", tn.lower())),
                "col_tok":  set(),
            }
        if r.get("cname"):
            for p in re.findall(r"[a-zA-Z]+", r["cname"].lower()):
                tables[tn]["col_tok"].add(p)

    if not tables:
        return {}

    q_tok = _q_tokens(question)
    if not q_tok:
        return next(iter({"name": k, "platform": v["platform"], "layer": v["layer"]}
                         for k, v in tables.items()), {})

    def _matches(qt, surface):
        return any(qt == s or (len(qt) >= 4 and len(s) >= 4
                               and (qt.startswith(s) or s.startswith(qt)))
                   for s in surface)

    name_hit_count: dict = {}
    col_hit_count:  dict = {}
    for tn, info in tables.items():
        for qt in q_tok:
            if _matches(qt, info["name_tok"]):
                name_hit_count[qt] = name_hit_count.get(qt, 0) + 1
            elif _matches(qt, info["col_tok"]):
                col_hit_count[qt]  = col_hit_count.get(qt, 0) + 1

    scores: dict = {}
    for tn, info in tables.items():
        s = 0.0
        for qt in q_tok:
            if _matches(qt, info["name_tok"]):
                idf = 1.0 / math.log1p(name_hit_count.get(qt, 1))
                s += 8.0 * idf
            elif _matches(qt, info["col_tok"]):
                idf = 1.0 / math.log1p(col_hit_count.get(qt, 1))
                s += 1.0 * idf
        # Layer preference bonus (+12 points for correct layer)
        layer = info["layer"]
        tn_l  = tn.lower()
        if want_dim  and layer == "silver" and (tn_l.startswith("dim_") or "_dim_" in tn_l):
            s += 12.0
        elif want_fact and layer == "silver" and (tn_l.startswith("fct_") or tn_l.startswith("fact_")):
            s += 12.0
        elif want_gold and layer == "gold":
            s += 12.0
        if s > 0:
            scores[tn] = s

    # Keyword overrides (only for Gold/analytical questions)
    if want_gold:
        for pattern, tbl_sub, boost in _OVERRIDES:
            if re.search(pattern, q_lower):
                for tn in tables:
                    if tbl_sub.lower() in tn.lower():
                        scores[tn] = scores.get(tn, 0.0) + boost

    best = max(scores, key=scores.get) if scores else next(iter(tables))
    info = tables[best]
    return {"name": best, "platform": info["platform"], "layer": info["layer"]}


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(question: str, on_step=None) -> dict:
    """Run the Graph-RAG pipeline with live step callbacks."""
    from langchain_core.messages import HumanMessage

    steps: list = []

    def record(icon: str, msg: str):
        steps.append({"icon": icon, "msg": msg})
        if on_step:
            on_step(icon, msg)

    # ── 1. GraphDB connection ────────────────────────────────────────────────
    record("🔍", "Is this a metadata question? (tables, KPIs, lineage, PII…)")
    token, err = graphdb.get_token()
    if err:
        record("❌", f"GraphDB connection failed: {err}")
        return _err(err, steps)

    llm = _get_llm()

    # ── 2. Structural SPARQL check ───────────────────────────────────────────
    for pat, sparql in STRUCTURAL_PATTERNS:
        if pat.search(question):
            rows = graphdb.query(sparql, token) or []
            record("📖", f"Yes — answering from knowledge graph via SPARQL ({len(rows)} rows)")
            tbl_text = _rows_to_text(rows)
            record("✅", "Generating summary…")
            try:
                answer = llm.invoke([HumanMessage(content=(
                    f"Summarise these data warehouse results in 3-5 sentences:\n\n"
                    f"{tbl_text}\n\nQuestion: {question}\nAnswer:"
                ))]).content.strip()
            except Exception:
                answer = tbl_text
            return {"source": "GraphDB (knowledge graph)", "sql": "",
                    "raw_table": tbl_text, "answer": answer,
                    "result_rows": [], "result_cols": [],
                    "platform": "graphdb", "is_structural": True,
                    "error": "", "steps": steps}

    record("🗺️", "No — this needs real data. Searching for the best table…")

    # ── 3. Find best table (Gold / Silver / Bronze) ──────────────────────────
    tbl = _find_best_table(question, token)
    if not tbl:
        record("❌", "Could not identify a relevant table")
        return _err("Could not identify a relevant table. Try rephrasing.", steps)

    pt_name    = tbl["name"]
    pt_plat    = (tbl.get("platform") or "").lower()
    layer      = tbl.get("layer") or "gold"
    # Snowflake schemas are uppercase; Databricks lowercase
    if pt_plat == "snowflake":
        fqn = f"KG_POC.{layer.upper()}.{pt_name}"
    else:
        fqn = f"KG_POC.{layer}.{pt_name}"
    plat_emoji = "❄️ Snowflake" if pt_plat == "snowflake" else "🟠 Databricks"

    record("🎯", f"Best match: **{pt_name}** ({layer}) on {plat_emoji}")

    # ── 4. Fetch real schema ─────────────────────────────────────────────────
    record("📋", f"Fetching schema from {plat_emoji} — {fqn}")
    actual_cols: list = []
    try:
        actual_cols = (sql_exec.snowflake_columns(pt_name) if pt_plat == "snowflake"
                       else sql_exec.databricks_columns(pt_name, layer))
    except Exception:
        pass
    if not actual_cols:
        col_rows = graphdb.query(
            f'SELECT ?colName WHERE {{ ?t biz:tableName ?tname ; biz:hasColumn ?col . '
            f'FILTER (LCASE(STR(?tname)) = "{pt_name.lower()}") '
            f'?col biz:columnName ?colName . }} ORDER BY ?colName', token) or []
        actual_cols = [(r.get("colName",""), "") for r in col_rows if r.get("colName")]
    record("📋", f"Schema: {len(actual_cols)} columns")

    # ── 5. Schema-describe shortcut (no SQL needed) ──────────────────────────
    if _SCHEMA_PAT.search(question):
        record("📖", "Schema question — describing table structure (no SQL needed)")
        col_lines = [f"  - {cn} ({ct})" if ct else f"  - {cn}"
                     for cn, ct in actual_cols[:50]]
        col_desc = "\n".join(col_lines) or "  (no column metadata available)"

        kpi_rows = graphdb.query(
            f'SELECT ?kpiName ?direction ?benchmark WHERE {{'
            f' ?t biz:tableName ?tname ; biz:hasKPI ?k .'
            f' FILTER (LCASE(STR(?tname)) = "{pt_name.lower()}")'
            f' ?k biz:kpiName ?kpiName .'
            f' OPTIONAL {{ ?k biz:kpiDirection ?direction }}'
            f' OPTIONAL {{ ?k biz:kpiBenchmark ?benchmark }} }}',
            token) or []

        kpi_lines = []
        for r in kpi_rows[:20]:
            parts = [r.get("kpiName","")]
            if r.get("direction"):  parts.append(r["direction"])
            if r.get("benchmark"):  parts.append("benchmark: " + r["benchmark"])
            kpi_lines.append("  - " + " | ".join(parts))
        kpi_text = ("\n\nRegistered KPIs:\n" + "\n".join(kpi_lines)) if kpi_lines else ""

        context = (f"Table: {fqn}\n"
                   f"Platform: {pt_plat}   Layer: {layer}\n"
                   f"Total columns: {len(actual_cols)}\n"
                   f"Columns:\n{col_desc}{kpi_text}")

        record("✅", f"Schema loaded — {len(actual_cols)} columns, {len(kpi_rows)} KPIs registered")

        prompt = (f"You are a data engineer explaining a warehouse table to a business analyst.\n\n"
                  f"Question: {question}\n\n"
                  f"{context}\n\n"
                  f"Describe what this table contains in 3-5 sentences. "
                  f"Mention the key columns, what time grain it tracks, "
                  f"and what business questions it can answer. "
                  f"Be specific — use the actual column names.\n\nAnswer:")
        try:
            answer = llm.invoke([HumanMessage(content=prompt)]).content.strip()
        except Exception:
            answer = context

        col_rows_out = [{"Column": cn, "Type": ct or "—"} for cn, ct in actual_cols[:50]]
        return {"source": f"{plat_emoji} → {fqn}", "sql": "",
                "raw_result": "", "result_rows": col_rows_out,
                "result_cols": ["Column", "Type"], "answer": answer,
                "platform": pt_plat, "is_structural": True,
                "error": "", "steps": steps}

    # ── 6. Sample categorical columns ────────────────────────────────────────
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

    # ── 7. Build schema context for SQL generation ───────────────────────────
    col_lines = []
    for cn, ct in actual_cols[:40]:
        line = f"  - {cn} [{ct}]"
        if cn in col_samples:
            line += f"  (sample values: {', '.join(repr(v) for v in col_samples[cn])})"
        col_lines.append(line)

    schema_block = (f"TABLE: {fqn}\nPlatform: {pt_plat}   Layer: {layer}\n\n"
                    "ACTUAL COLUMNS — use ONLY these exact names:\n"
                    + ("\n".join(col_lines) if col_lines else "  (no column metadata available)"))

    # ── 8. Generate SQL ──────────────────────────────────────────────────────
    all_time = bool(re.search(
        r"\ball\s+(months?|time|years?|periods?|data|history|available)\b"
        r"|\bover\s+(all|entire|the\s+whole)\b"
        r"|\bno\s+date\s+filter\b|\bhistorical\b|\bevery\s+month\b",
        question, re.I))

    date_rule = (
        "3. DATE: NO date filter — return ALL rows (question asks for all data/months)."
        if all_time else
        "3. DATE: Only add a date filter if the question explicitly says "
        "'last quarter/month/year'. If no time qualifier is present, "
        "do NOT add any WHERE clause on date columns — return all rows."
    )

    record("✍️", "Generating SQL query…")
    sql_prompt = (f"You are a SQL expert for a retail Sales DWH.\n\n"
                  f"Question: {question}\n\n"
                  f"{schema_block}\n\n"
                  f"Rules:\n"
                  f"1. Use ONLY column names from ACTUAL COLUMNS above.\n"
                  f"2. Always qualify table as: {fqn}\n"
                  f"{date_rule}\n"
                  f"4. No current_date() — data covers 2020–2024.\n"
                  f"5. ORDER BY: use alias names, not aggregate expressions.\n"
                  f"6. Categorical filter values are snake_case e.g. 'credit_card'.\n"
                  f"7. Add LIMIT 200 unless the question asks for a single total.\n"
                  f"8. Return ONLY the SQL — no explanation, no backticks.\n\nSQL:")

    sql_raw = llm.invoke([HumanMessage(content=sql_prompt)]).content.strip()
    for fence in ["```sql", "```SQL", "```"]:
        sql_raw = sql_raw.replace(fence, "")
    sql = sql_raw.strip()
    for kw in ("WITH", "SELECT"):
        idx = sql.upper().find(kw)
        if idx > 0:
            sql = sql[idx:]; break

    record("⚡", f"Executing SQL on {plat_emoji}…")

    # ── 9. Execute SQL ───────────────────────────────────────────────────────
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
    if sql_result.lower().startswith(("sql error", "databricks error", "snowflake error")):
        record("❌", "SQL execution error — see SQL expander below")
    else:
        record("📊", f"Query returned {n_rows} row{'s' if n_rows != 1 else ''}")

    # ── 10. Generate answer ──────────────────────────────────────────────────
    record("💬", "Generating natural-language answer…")
    ans_prompt = (f"You are a senior business analyst.\n\n"
                  f"Question: {question}\n"
                  f"Table: {fqn}\n"
                  f"SQL Result:\n{sql_result[:3000]}\n\n"
                  f"Write a concise 2-4 sentence narrative of the KEY insight. "
                  f"Include the most important numbers. "
                  f"Do NOT reproduce the full table.\n\nAnswer:")
    answer = llm.invoke([HumanMessage(content=ans_prompt)]).content.strip()
    record("✅", "Done")

    plat_label = "Snowflake" if pt_plat == "snowflake" else "Databricks"
    return {"source": f"{plat_label} → {fqn}", "sql": sql,
            "raw_result": sql_result, "result_rows": result_rows,
            "result_cols": result_cols, "answer": answer,
            "platform": pt_plat, "is_structural": False,
            "error": "", "steps": steps}


def _err(msg: str, steps: list = None) -> dict:
    return {"source": "", "sql": "", "answer": msg, "platform": "",
            "result_rows": [], "result_cols": [],
            "is_structural": False, "error": msg, "steps": steps or []}
