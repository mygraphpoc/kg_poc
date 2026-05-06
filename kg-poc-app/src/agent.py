"""src/agent.py — Graph-RAG agent pipeline."""

import re
import streamlit as st
from src import config, graphdb, sql_exec

STOP_WORDS = {
    "the","a","an","is","are","of","in","on","at","by","for","with","from","to",
    "and","or","but","not","this","that","what","which","who","how","show","list",
    "get","tell","year","month","quarter","week","day","time",
}

STRUCTURAL_PATTERNS = [
    (re.compile(r"fact\s+table", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:FactTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"dimension\s+table|all\s+dim", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:DimensionTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"gold\s+table|aggregate\s+table|kpi\s+table", re.I),
     'SELECT ?tableName ?layer ?platform WHERE { ?t biz:tableLayer "gold" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    (re.compile(r"pii|personally\s+identifiable", re.I),
     "SELECT DISTINCT ?tableName ?layer ?platform WHERE { ?t biz:tableName ?tableName ; biz:hasColumn ?col . ?col biz:isPII true . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"snowflake\s+table|tables?\s+on\s+snowflake", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "snowflake" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    (re.compile(r"databricks\s+table|tables?\s+on\s+databricks", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "databricks" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    (re.compile(r"kpis?\s+available|all\s+kpis?|list\s+kpis?", re.I),
     "SELECT ?kpiName ?domain ?unit ?direction ?benchmark WHERE { ?k a biz:KPI ; biz:kpiName ?kpiName . OPTIONAL { ?k biz:kpiDomain ?domain } OPTIONAL { ?k biz:kpiUnit ?unit } OPTIONAL { ?k biz:kpiDirection ?direction } OPTIONAL { ?k biz:kpiBenchmark ?benchmark } } ORDER BY ?domain ?kpiName"),
    (re.compile(r"owl\s+class|ontology\s+class", re.I),
     "SELECT ?className ?comment WHERE { ?c a owl:Class . BIND(STRAFTER(STR(?c),'#') AS ?className) OPTIONAL { ?c rdfs:comment ?comment } } ORDER BY ?className"),
    (re.compile(r"silver\s+table|silver\s+layer", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "silver" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    (re.compile(r"lineage|feeds?\s+into|cross.platform", re.I),
     "SELECT ?srcName ?srcPlatform ?tgtName ?tgtPlatform ?transformType WHERE { ?src biz:feedsInto ?tgt . ?src biz:tableName ?srcName . ?tgt biz:tableName ?tgtName . OPTIONAL { ?src biz:sourceSystemType ?srcPlatform } OPTIONAL { ?tgt biz:sourceSystemType ?tgtPlatform } ?edge biz:sourceTable ?src ; biz:targetTable ?tgt . OPTIONAL { ?edge biz:lineageTransformType ?transformType } } ORDER BY ?srcName"),
]


@st.cache_resource(show_spinner=False)
def _get_vs_index():
    import os
    host = config.get("DATABRICKS_HOST")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    os.environ["DATABRICKS_HOST"]  = host
    os.environ["DATABRICKS_TOKEN"] = config.get("DATABRICKS_TOKEN")
    try:
        from databricks.vector_search.client import VectorSearchClient
        client = VectorSearchClient(
            workspace_url=host,
            personal_access_token=config.get("DATABRICKS_TOKEN"),
            disable_notice=True,
        )
        return client.get_index(
            endpoint_name=config.get("VS_ENDPOINT_NAME"),
            index_name=config.get("INDEX_NAME"),
        )
    except Exception:
        return None


@st.cache_resource(show_spinner=False)
def _get_embed():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


@st.cache_resource(show_spinner=False)
def _get_llm():
    import os
    from databricks_langchain import ChatDatabricks
    # Databricks SDK reads credentials from env vars, not st.secrets directly.
    # Bridge the gap so ChatDatabricks works on Streamlit Cloud.
    host = config.get("DATABRICKS_HOST")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    os.environ["DATABRICKS_HOST"]  = host
    os.environ["DATABRICKS_TOKEN"] = config.get("DATABRICKS_TOKEN")
    return ChatDatabricks(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        temperature=0,
        max_tokens=1024,
    )


def _q_tokens(question: str) -> set:
    return {t for t in re.findall(r"\b[a-zA-Z]{3,}\b", question.lower())
            if t not in STOP_WORDS}


def _sparql_table(rows: list) -> str:
    if not rows: return "No results."
    cols = list(rows[0].keys())
    w = max(24, max(len(c) for c in cols))
    hdr = " | ".join(f"{c:<{w}}" for c in cols)
    sep = "-" * (len(hdr) + 4)
    lines = [hdr, sep] + [" | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols) for r in rows[:60]]
    return "\n".join(f"  {l}" for l in lines)


def run(question: str) -> dict:
    """
    Returns:
      source       str  — 'GraphDB (knowledge graph)' or 'Platform → fqn'
      sql          str  — generated SQL (empty for structural)
      answer       str  — natural-language answer
      platform     str  — 'graphdb' | 'databricks' | 'snowflake'
      is_structural bool
      error        str  — non-empty if something went wrong
    """
    token, err = graphdb.get_token()
    if err:
        return {"source":"","sql":"","answer":"","platform":"","is_structural":False,"error":err}

    from langchain_core.messages import HumanMessage
    llm = _get_llm()

    # ── 1. Structural pre-check ────────────────────────────────────────────
    # Dynamic domain pattern
    dm = re.search(r"(customer|product|sales|finance|hr|operations|marketing|supply)\s+domain",
                   question, re.I)
    patterns = list(STRUCTURAL_PATTERNS)
    if dm:
        d = dm.group(1).capitalize()
        patterns.append((re.compile(re.escape(d), re.I),
            f'SELECT ?tableName ?layer ?platform WHERE {{ ?t biz:tableDomain "{d}" ; biz:tableName ?tableName . OPTIONAL {{ ?t biz:tableLayer ?layer }} OPTIONAL {{ ?t biz:sourceSystemType ?platform }} }} ORDER BY ?layer ?tableName'))

    for pat, sparql in patterns:
        if pat.search(question):
            rows = graphdb.query(sparql, token)
            table_str = _sparql_table(rows)
            prompt = (f"Summarise these data warehouse results clearly in 3-5 sentences:\n\n"
                      f"{table_str}\n\nQuestion: {question}\nAnswer:")
            ans = llm.invoke([HumanMessage(content=prompt)]).content.strip()
            return {"source":"GraphDB (knowledge graph)","sql":"","answer":ans,
                    "raw_table":table_str,"platform":"graphdb","is_structural":True,"error":""}

    # ── 2. Vector Search ──────────────────────────────────────────────────
    vs_results = []
    vs_index = _get_vs_index()
    if vs_index:
        try:
            embed = _get_embed()
            qvec  = embed.encode(question).tolist()
            res   = vs_index.similarity_search(
                query_vector=qvec,
                columns=["item_id","item_type","item_name","full_path","layer","source_system_type","document"],
                num_results=5)
            for h in [dict(zip([c["name"] for c in res.get("manifest",{}).get("columns",[])], r))
                      for r in res.get("result",{}).get("data_array",[])]:
                itype = (h.get("item_type") or "").lower()
                fp    = h.get("full_path","") or ""
                name  = h.get("item_name","") or ""
                if itype == "table": tn = name
                elif "." in fp:
                    parts = fp.split(".")
                    tn = parts[-2] if len(parts) >= 2 else name
                elif fp and itype in ("kpi","column"): tn = fp
                else: tn = name
                vs_results.append({"node_name":name,"node_type":itype,"table_name":tn,
                    "layer":h.get("layer",""),"platform":h.get("source_system_type",""),
                    "description":(h.get("document","") or "")[:200]})
        except Exception:
            pass

    # Dimension-match augment
    try:
        catalog_rows = graphdb.query(
            "SELECT DISTINCT ?tname ?platform WHERE { ?t biz:tableName ?tname ; biz:tableLayer ?layer . FILTER (LCASE(STR(?layer)) = \"gold\") OPTIONAL { ?t biz:sourceSystemType ?platform } }",
            token) or []
        q_tok = _q_tokens(question)
        scored = []
        for r in catalog_rows:
            tn = r.get("tname","")
            parts = re.findall(r"[a-zA-Z]+", tn.lower())
            if any(any(qt.startswith(p) or p.startswith(qt) for p in parts if len(p)>=4 and len(qt)>=4) for qt in q_tok):
                scored.append((tn, r.get("platform","")))
        existing = {(h.get("table_name") or "").lower() for h in vs_results}
        for tn, plat in scored[:3]:
            if tn.lower() not in existing:
                vs_results.append({"node_name":tn,"node_type":"table","table_name":tn,
                    "layer":"gold","platform":plat,"description":""})
                existing.add(tn.lower())
    except Exception:
        pass

    # ── 3. Pick primary table + fetch real columns ────────────────────────
    primary = vs_results[0] if vs_results else {}
    pt_name = primary.get("table_name","")
    pt_plat = (primary.get("platform") or "").lower()
    pt_layer= (primary.get("layer") or "gold").lower()

    actual_cols: list[tuple[str,str]] = []
    if pt_name:
        if pt_plat == "snowflake":
            actual_cols = sql_exec.snowflake_columns(pt_name)
        elif pt_plat == "databricks":
            actual_cols = sql_exec.databricks_columns(pt_name, pt_layer)
        if not actual_cols:
            col_rows = graphdb.query(
                f'SELECT ?colName WHERE {{ ?t biz:tableName ?tname ; biz:hasColumn ?col . FILTER (LCASE(STR(?tname)) = "{pt_name.lower()}") ?col biz:columnName ?colName . }} ORDER BY ?colName',
                token) or []
            actual_cols = [(r.get("colName",""),"") for r in col_rows if r.get("colName")]

    # Sample values for relevant string columns
    col_samples: dict[str,list[str]] = {}
    q_tok = _q_tokens(question)
    if pt_name and actual_cols:
        fqn_sample = (f"KG_POC.{pt_layer.upper()}.{pt_name}" if pt_plat == "snowflake"
                      else f"KG_POC.{pt_layer}.{pt_name}")
        for cn, ct in actual_cols[:20]:
            if not any(any(qt.startswith(p) or p.startswith(qt)
                           for p in re.findall(r"[a-zA-Z]+", cn.lower())
                           if len(p)>=4) for qt in q_tok if len(qt)>=4): continue
            if not any(x in (ct or "").lower() for x in ("char","string","text","varchar")): continue
            try:
                vals = (sql_exec.snowflake_distinct(fqn_sample, cn) if pt_plat == "snowflake"
                        else sql_exec.databricks_distinct(fqn_sample, cn))
                if vals: col_samples[cn] = vals
            except Exception: pass

    # Build context block
    if pt_plat == "snowflake":
        fqn = f"KG_POC.{pt_layer.upper()}.{pt_name}"
    else:
        fqn = f"KG_POC.{pt_layer}.{pt_name}"

    col_lines = []
    for cn, ct in actual_cols[:40]:
        line = f"  • {cn} [{ct}]"
        if cn in col_samples:
            line += f"  ← values: {', '.join(repr(v) for v in col_samples[cn])}"
        col_lines.append(line)

    context = f"""PRIMARY TABLE: {pt_name}
Fully qualified path: {fqn}   ← USE THIS EXACT PATH IN FROM/JOIN CLAUSES
Layer    : {pt_layer}
Platform : {pt_plat}

ACTUAL COLUMNS in {fqn} (USE ONLY THESE):
{chr(10).join(col_lines) if col_lines else '  (use column names from graph metadata)'}
"""

    # ── 4. Generate SQL ───────────────────────────────────────────────────
    sql_prompt = f"""You are a SQL expert for a retail Sales DWH.

CRITICAL RULES:
1. Use ONLY columns listed under ACTUAL COLUMNS above.
2. Fully qualify every table using the exact path shown.
   Databricks: KG_POC.gold.<name>   Snowflake: KG_POC.GOLD.<NAME>
3. DATE FILTERING: if "last quarter/year/month" → subquery:
   WHERE year_quarter = (SELECT MAX(year_quarter) FROM <same_fqn>)
4. NEVER use current_date() — data covers 2020-2024.
5. ORDER BY: use column aliases, never repeat aggregate functions.
6. Categorical values use snake_case: 'Credit Card' → 'credit_card'.
7. Return ONLY the SQL, no markdown, no backticks.

CONTEXT:
{context}

Question: {question}

SQL:"""

    sql = llm.invoke([HumanMessage(content=sql_prompt)]).content.strip()
    sql = re.sub(r"```sql\s*", "", sql)
    sql = re.sub(r"```\s*", "", sql).strip()

    # ── 5. Execute SQL (detect platform from SQL → graph) ─────────────────
    tbl_refs = re.findall(r"(?:FROM|JOIN)\s+([\w.]+)", sql, re.IGNORECASE)
    exec_plat = pt_plat
    if tbl_refs:
        rows = graphdb.query(
            f'SELECT ?platform WHERE {{ ?t biz:tableName ?tname ; biz:sourceSystemType ?platform . FILTER (LCASE(STR(?tname)) = "{tbl_refs[0].split(".")[-1].lower()}") }} LIMIT 1',
            token) or []
        if rows and rows[0].get("platform"):
            exec_plat = rows[0]["platform"].lower()

    sql_result = (sql_exec.execute_snowflake(sql) if exec_plat == "snowflake"
                  else sql_exec.execute_databricks(sql))

    # ── 6. Generate answer ────────────────────────────────────────────────
    ans_prompt = f"""You are a senior data analyst.
Answer the question concisely in 3-5 sentences. Focus on the insight, not on explaining the SQL.

Question: {question}

SQL Result:
{sql_result[:3000]}

Answer:"""
    answer = llm.invoke([HumanMessage(content=ans_prompt)]).content.strip()

    source_tbl = tbl_refs[0] if tbl_refs else fqn
    plat_label = "Snowflake" if exec_plat == "snowflake" else "Databricks"
    return {
        "source":       f"{plat_label} → {source_tbl}",
        "sql":          sql,
        "raw_result":   sql_result,
        "answer":       answer,
        "platform":     exec_plat,
        "is_structural":False,
        "error":        "",
    }
