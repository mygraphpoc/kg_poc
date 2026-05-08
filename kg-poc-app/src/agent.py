"""
src/agent.py — Graph-RAG pipeline.
run(question, on_step=None) calls on_step(icon, msg) at each stage.
"""

import re
import math
import streamlit as st
from src import config, graphdb, sql_exec

STOP = {
    "the","a","an","is","are","of","in","on","at","by","for","with","from","to",
    "and","or","but","not","this","that","what","which","who","how",
    "show","list","get","tell","year","month","quarter","week","day","time",
}

# ── Structural SPARQL patterns ────────────────────────────────────────────────
# Each regex is tuned against real failure cases from the test log.
# Key fixes: allow filler words ("are", "is") between key terms; plural forms.
STRUCTURAL_PATTERNS = [
    # "fact tables" / "all fact tables" — but NOT "sales fact table" (schema Q)
    # Require "all" or "what are" before to distinguish from specific table Qs
    (re.compile(r"\ball\s+fact\s+table|what\s+\w+\s+fact\s+table\s+(?:do|are|exist|available)", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:FactTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    # "what are the fact tables" / "list fact tables" (without specific table name)
    (re.compile(r"\bfact\s+tables?\b(?!\s+\w+\s+(?:structure|column|hold|contain|have|data))", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:FactTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    # "dimension tables" — same guard
    (re.compile(r"\ball\s+dim(?:ension)?\s+table|\bdim(?:ension)?\s+tables?\s+(?:available|do\s+we|exist|are\s+there)|all\s+dim\b", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:DimensionTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    # Gold / aggregate tables
    (re.compile(r"gold\s+table|aggregate\s+table|kpi\s+table|all\s+gold", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "gold" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    # PII
    (re.compile(r"\bpii\b|personally\s+identifiable", re.I),
     "SELECT DISTINCT ?tableName ?layer WHERE { ?t biz:tableName ?tableName ; biz:hasColumn ?col . ?col biz:isPII true . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName"),
    # Snowflake tables — allow filler words: "tables are on Snowflake", "tables on Snowflake"
    (re.compile(r"tables?\s+(?:\w+\s+)?on\s+snowflake|snowflake\s+table|on\s+snowflake\b", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "snowflake" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    # Databricks tables
    (re.compile(r"tables?\s+(?:\w+\s+)?on\s+databricks|databricks\s+table|on\s+databricks\b", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "databricks" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    # KPIs — allow "KPIs are available", "KPIs available", "list KPIs", "all KPIs"
    (re.compile(r"kpis?\s+(?:\w+\s+)?available|all\s+kpis?\b|list\s+kpis?\b|kpis?\s+(?:do\s+we\s+have|in\s+the\s+warehouse)", re.I),
     "SELECT ?kpiName ?domain ?direction WHERE { ?k a biz:KPI ; biz:kpiName ?kpiName . OPTIONAL { ?k biz:kpiDomain ?domain } OPTIONAL { ?k biz:kpiDirection ?direction } } ORDER BY ?domain ?kpiName"),
    # Silver tables
    (re.compile(r"silver\s+table|silver\s+layer|all\s+silver", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "silver" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    # Lineage
    (re.compile(r"\blineage\b|feeds?\s+into|cross.platform", re.I),
     "SELECT ?srcName ?srcPlatform ?tgtName ?tgtPlatform ?transformType WHERE { ?src biz:feedsInto ?tgt . ?src biz:tableName ?srcName . ?tgt biz:tableName ?tgtName . OPTIONAL { ?src biz:sourceSystemType ?srcPlatform } OPTIONAL { ?tgt biz:sourceSystemType ?tgtPlatform } ?edge biz:sourceTable ?src ; biz:targetTable ?tgt . OPTIONAL { ?edge biz:lineageTransformType ?transformType } } ORDER BY ?srcName"),
    # OWL / ontology classes — "OWL classes", "OWL class", "ontology classes"
    (re.compile(r"owl\s+class(?:es)?|ontolog(?:y|ical)\s+class(?:es)?|class(?:es)?\s+in\s+the\s+ontolog", re.I),
     "SELECT ?className ?comment WHERE { ?c a owl:Class . BIND(STRAFTER(STR(?c),'#') AS ?className) OPTIONAL { ?c rdfs:comment ?comment } } ORDER BY ?className"),
]

# Detect specific table names in the question — if present, skip structural
# patterns (user is asking about a specific table, not listing all tables)
_EXPLICIT_TABLE_PAT = re.compile(r"\b(?:dim|fct|fact|agg)_[a-z][a-z_]*", re.I)

# Schema/structure question — no SQL needed, describe the table
_SCHEMA_PAT = re.compile(
    r"what\s+(?:kind|type|sort|column|field|data|info|information|detail)"
    r"|what\s+does\s+.+?\s+(?:table|have|contain|include|store|hold|track)"
    r"|describe\s+(?:the\s+)?(?:table|column|schema|structure)"
    r"|show\s+(?:me\s+)?(?:the\s+)?(?:column|schema|structure|field)"
    r"|what\s+is\s+(?:in|inside|stored|available)\s+(?:the\s+)?table"
    r"|columns?\s+(?:in|of|for|available|does\s+\w+\s+have)"
    r"|table\s+structure|schema\s+of"
    r"|what\s+(?:data|fields?|columns?|information)\s+(?:is|are)\s+in"
    r"|what\s+(?:does|is\s+in)\s+the\s+\w+\s+(?:table|dimension|fact)",
    re.I,
)

# Keyword → table-name substring → score boost
# Higher boost = more specific; Snowflake-specific Qs get higher boosts to beat
# Databricks token-score overlap on shared words like "revenue", "channel".
_OVERRIDES = [
    # ── Databricks Gold ────────────────────────────────────────────────────
    (r"\bchurn\b",                              "customer_360",         25.0),
    (r"\bclv\b|\blifetime\s+value",             "customer_360",         25.0),
    (r"\binventor",                             "inventory",            20.0),
    (r"\bpromo\b|\bpromotion\b",                "promotion",            20.0),
    (r"\bstore\b",                              "store_performance",    18.0),
    (r"\bemployee.*perform|perform.*employee",  "employee_perform",     25.0),
    (r"\bquota\b",                              "employee_perform",     25.0),
    (r"\brevenue.*month\b|\bmonthly.*revenue",  "revenue_monthly",      20.0),
    (r"\brevenue.*daily\b|\bdaily.*revenue",    "revenue_daily",        20.0),
    (r"\bproduct\b",                            "product_performance",  15.0),
    (r"\bcustomer\b(?!.*segment)",              "customer_360",         20.0),
    # ── Snowflake Gold ─────────────────────────────────────────────────────
    (r"\bsupplier\b",                           "supplier",             25.0),
    (r"\bpayment\b",                            "payment",              25.0),
    (r"\bgeograph|\bstate\b|\bstates\b",        "geographic",           30.0),  # must beat revenue_daily (+20)
    (r"\bexecutive\b|\bc.suite\b",              "executive",            25.0),
    (r"\bcohort\b",                             "cohort",               25.0),
    # segment: only Snowflake has AGG_CUSTOMER_SEGMENT; must beat customer_360
    (r"\bsegment\b",                            "customer_segment",     28.0),
    # channel: Snowflake AGG_CHANNEL_PERFORMANCE vs Databricks agg_channel_mix
    # "channel performance" → Snowflake; "channel revenue/mix" → could be either
    (r"\bchannel.*perform|perform.*channel|\bchannel.*margin|margin.*channel", "channel_perform", 30.0),
    # employee commission/sales → Snowflake AGG_EMPLOYEE_SALES
    (r"\bemployee\b|\bcommission\b",            "employee",             20.0),
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
    if not rows: return "No results."
    cols = list(rows[0].keys())
    w    = max(22, max(len(c) for c in cols))
    hdr  = " | ".join(f"{c:<{w}}" for c in cols)
    return f"{hdr}\n{'-'*len(hdr)}\n" + "\n".join(
        " | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols) for r in rows[:60])


def _find_best_table(question: str, token: str) -> dict:
    """
    Find best table across ALL layers.
    - "dim/dimension" or "dim_xxx" in Q  → prefer Silver dim_ tables (+12)
    - "fact/fct" or "fct_xxx" in Q       → prefer Silver fct_ tables (+12)
    - otherwise                           → prefer Gold tables (+12)
    Table-name token match = 8x IDF; column match = 1x IDF; overrides add 15-30.
    """
    q_lower = question.lower()

    # Detect dim/fact by word boundary OR by explicit table-name prefix in question
    want_dim  = bool(re.search(r"\bdim(?:ension)?\b|\bdim_[a-z]", q_lower))
    want_fact = bool(re.search(r"\bfact\b|\bfct\b|\bfct_[a-z]|\bfact_[a-z]", q_lower))
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
        if not tn: continue
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

    nhc: dict = {}; chc: dict = {}
    for tn, info in tables.items():
        for qt in q_tok:
            if   _matches(qt, info["name_tok"]): nhc[qt] = nhc.get(qt, 0) + 1
            elif _matches(qt, info["col_tok"]):  chc[qt] = chc.get(qt, 0) + 1

    scores: dict = {}
    for tn, info in tables.items():
        s = 0.0
        for qt in q_tok:
            if _matches(qt, info["name_tok"]):
                s += 8.0 / math.log1p(nhc.get(qt, 1))
            elif _matches(qt, info["col_tok"]):
                s += 1.0 / math.log1p(chc.get(qt, 1))
        layer = info["layer"]
        tn_l  = tn.lower()
        if   want_dim  and layer == "silver" and (tn_l.startswith("dim_") or "_dim_" in tn_l): s += 12.0
        elif want_fact and layer == "silver" and (tn_l.startswith("fct_") or tn_l.startswith("fact_")): s += 12.0
        elif want_gold and layer == "gold": s += 12.0
        if s > 0: scores[tn] = s

    # Keyword overrides (only for Gold/analytical questions, not dim/fact schema Qs)
    if want_gold:
        for pattern, tbl_sub, boost in _OVERRIDES:
            if re.search(pattern, q_lower):
                for tn in tables:
                    if tbl_sub.lower() in tn.lower():
                        scores[tn] = scores.get(tn, 0.0) + boost

    # If dim/fact mode but no silver table scored, fall back to any match
    if (want_dim or want_fact) and not any(
            tables[tn]["layer"] == "silver" for tn in scores):
        want_gold = True
        for tn, info in tables.items():
            if info["layer"] == "gold" and tn not in scores:
                s = sum(8.0/math.log1p(nhc.get(qt,1))
                        for qt in q_tok if _matches(qt, info["name_tok"]))
                if s > 0: scores[tn] = s

    best = max(scores, key=scores.get) if scores else next(iter(tables))
    info = tables[best]
    return {"name": best, "platform": info["platform"], "layer": info["layer"]}


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(question: str, on_step=None) -> dict:
    from langchain_core.messages import HumanMessage
    steps: list = []

    def record(icon: str, msg: str):
        steps.append({"icon": icon, "msg": msg})
        if on_step: on_step(icon, msg)

    record("🔍", "Is this a metadata question? (tables, KPIs, lineage, PII…)")
    token, err = graphdb.get_token()
    if err:
        record("❌", f"GraphDB connection failed: {err}")
        return _err(err, steps)

    llm = _get_llm()

    # ── 1. Structural SPARQL — skip if question names a specific table ────────
    has_explicit_table = bool(_EXPLICIT_TABLE_PAT.search(question))
    if not has_explicit_table:
        # Also add dynamic domain pattern
        q_lower = question.lower()
        patterns = list(STRUCTURAL_PATTERNS)
        dm = re.search(
            r"(customer|product|sales|finance|hr|operations|marketing|supply)\s+domain",
            question, re.I)
        if dm:
            d = dm.group(1).capitalize()
            patterns.append((re.compile(re.escape(d), re.I),
                f'SELECT ?tableName ?layer ?platform WHERE {{ ?t biz:tableDomain "{d}" ; biz:tableName ?tableName . OPTIONAL {{ ?t biz:tableLayer ?layer }} OPTIONAL {{ ?t biz:sourceSystemType ?platform }} }} ORDER BY ?layer ?tableName'))

        for pat, sparql in patterns:
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
    else:
        record("🔍", f"Specific table named in question — skipping structural check")

    record("🗺️", "No — this needs real data. Searching for the best table…")

    # ── 2. Find best table ────────────────────────────────────────────────────
    tbl = _find_best_table(question, token)
    if not tbl:
        record("❌", "Could not identify a relevant table")
        return _err("Could not identify a relevant table. Try rephrasing.", steps)

    pt_name    = tbl["name"]
    pt_plat    = (tbl.get("platform") or "").lower()
    layer      = tbl.get("layer") or "gold"
    fqn        = (f"KG_POC.{layer.upper()}.{pt_name}" if pt_plat == "snowflake"
                  else f"KG_POC.{layer}.{pt_name}")
    plat_emoji = "❄️ Snowflake" if pt_plat == "snowflake" else "🟠 Databricks"

    record("🎯", f"Best match: **{pt_name}** ({layer}) on {plat_emoji}")

    # ── 3. Fetch real schema ──────────────────────────────────────────────────
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

    # ── 4. Schema-describe shortcut ───────────────────────────────────────────
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
        kpi_lines = [
            "  - " + " | ".join(filter(None, [r.get("kpiName"), r.get("direction"),
                ("benchmark: " + r["benchmark"]) if r.get("benchmark") else None]))
            for r in kpi_rows[:20]]
        kpi_text = ("\n\nRegistered KPIs:\n" + "\n".join(kpi_lines)) if kpi_lines else ""

        context = (f"Table: {fqn}\nPlatform: {pt_plat}   Layer: {layer}\n"
                   f"Total columns: {len(actual_cols)}\nColumns:\n{col_desc}{kpi_text}")
        record("✅", f"Schema loaded — {len(actual_cols)} columns, {len(kpi_rows)} KPIs")

        prompt = (f"You are a data engineer explaining a warehouse table to a business analyst.\n\n"
                  f"Question: {question}\n\n{context}\n\n"
                  f"Describe what this table contains in 3-5 sentences. "
                  f"Mention the key columns, time grain, and what business questions it answers. "
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

    # ── 5. Sample categorical columns ─────────────────────────────────────────
    q_tok = _q_tokens(question)
    col_samples: dict = {}
    for cn, ct in actual_cols[:20]:
        col_parts = set(re.findall(r"[a-zA-Z]+", cn.lower()))
        if not any(len(qt) >= 4 and any(qt.startswith(p) or p.startswith(qt)
                   for p in col_parts if len(p) >= 4) for qt in q_tok): continue
        if not any(x in (ct or "").lower() for x in ("char","string","text","varchar")): continue
        try:
            vals = (sql_exec.snowflake_distinct(fqn, cn) if pt_plat == "snowflake"
                    else sql_exec.databricks_distinct(fqn, cn))
            if vals: col_samples[cn] = vals
        except Exception: pass

    # ── 6. Build schema context ───────────────────────────────────────────────
    col_lines = []
    for cn, ct in actual_cols[:40]:
        line = f"  - {cn} [{ct}]"
        if cn in col_samples:
            line += f"  (sample values: {', '.join(repr(v) for v in col_samples[cn])})"
        col_lines.append(line)
    schema_block = (f"TABLE: {fqn}\nPlatform: {pt_plat}   Layer: {layer}\n\n"
                    "ACTUAL COLUMNS — use ONLY these exact names:\n"
                    + ("\n".join(col_lines) if col_lines else "  (no column metadata)"))

    # ── 7. Generate SQL ───────────────────────────────────────────────────────
    all_time = bool(re.search(
        r"\ball\s+(months?|time|years?|periods?|data|history|available)\b"
        r"|\bover\s+(all|entire|the\s+whole)\b|\bno\s+date\s+filter\b"
        r"|\bhistorical\b|\bevery\s+month\b", question, re.I))
    date_rule = (
        "3. DATE: NO date filter — return ALL rows (question asks for all data/months)."
        if all_time else
        "3. DATE: Only add a date filter if the question explicitly says "
        "'last quarter/month/year'. If no time qualifier → do NOT filter by date.")

    record("✍️", "Generating SQL query…")
    sql_raw = llm.invoke([HumanMessage(content=(
        f"You are a SQL expert for a retail Sales DWH.\n\n"
        f"Question: {question}\n\n{schema_block}\n\n"
        f"Rules:\n"
        f"1. Use ONLY column names from ACTUAL COLUMNS above.\n"
        f"2. Always qualify table as: {fqn}\n"
        f"{date_rule}\n"
        f"4. No current_date() — data covers 2020–2024.\n"
        f"5. ORDER BY: use alias names, not aggregate expressions.\n"
        f"6. Categorical filter values are snake_case e.g. 'credit_card'.\n"
        f"7. Add LIMIT 200 unless the question asks for a single total.\n"
        f"8. Return ONLY the SQL — no explanation, no backticks.\n\nSQL:"
    ))]).content.strip()
    for fence in ["```sql","```SQL","```"]: sql_raw = sql_raw.replace(fence, "")
    sql = sql_raw.strip()
    for kw in ("WITH","SELECT"):
        idx = sql.upper().find(kw)
        if idx > 0: sql = sql[idx:]; break

    record("⚡", f"Executing SQL on {plat_emoji}…")

    # ── 8. Execute SQL ────────────────────────────────────────────────────────
    sql_result = ""; result_rows: list = []; result_cols: list = []
    try:
        if pt_plat == "snowflake":
            sql_result, result_rows, result_cols = sql_exec.execute_snowflake(sql)
        else:
            sql_result, result_rows, result_cols = sql_exec.execute_databricks(sql)
    except Exception as e:
        sql_result = f"SQL error: {e}"

    n_rows = len(result_rows)
    if sql_result.lower().startswith(("sql error","databricks error","snowflake error")):
        record("❌", "SQL execution error — see SQL expander below")
    else:
        record("📊", f"Query returned {n_rows} row{'s' if n_rows != 1 else ''}")

    # ── 9. Generate answer ────────────────────────────────────────────────────
    record("💬", "Generating natural-language answer…")
    answer = llm.invoke([HumanMessage(content=(
        f"You are a senior business analyst.\n\n"
        f"Question: {question}\nTable: {fqn}\n"
        f"SQL Result:\n{sql_result[:3000]}\n\n"
        f"Write a concise 2-4 sentence narrative of the KEY insight. "
        f"Include the most important numbers. Do NOT reproduce the full table.\n\nAnswer:"
    ))]).content.strip()
    record("✅", "Done")

    plat_label = "Snowflake" if pt_plat == "snowflake" else "Databricks"
    return {"source": f"{plat_label} → {fqn}", "sql": sql,
            "raw_result": sql_result, "result_rows": result_rows,
            "result_cols": result_cols, "answer": answer,
            "platform": pt_plat, "is_structural": False,
            "error": "", "steps": steps}


def _err(msg: str, steps: list = None) -> dict:
    return {"source":"","sql":"","answer":msg,"platform":"",
            "result_rows":[],"result_cols":[],
            "is_structural":False,"error":msg,"steps":steps or []}
