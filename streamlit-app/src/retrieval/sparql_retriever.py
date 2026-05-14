"""
src/retrieval/sparql_retriever.py
─────────────────────────────────
v2 — GraphDB Full Text Search (FTS) as primary retrieval method.

Search hierarchy:
  1. Structural SPARQL patterns  — metadata questions answered directly
  2. Direct lookup               — explicit table name in question
  3. FTS (Lucene index)          — primary: fast, relevance-ranked, fuzzy
  4. Token-overlap scoring       — fallback if FTS index not available

FTS setup in GraphDB (run once in SPARQL editor):
─────────────────────────────────────────────────
  PREFIX luc: <http://www.ontotext.com/owlim/lucene#>

  INSERT DATA {
      luc:tableIndex luc:setParam "index" ;
          luc:setParam "uris=disabled" ;
          luc:setParam "predicates=biz:tableName biz:columnName biz:kpiName skos:prefLabel skos:altLabel skos:definition skos:scopeNote" ;
          luc:setParam "languages=en" .
  }
─────────────────────────────────────────────────
"""

import re
import math
from src import graphdb

# ── Stop words ────────────────────────────────────────────────────────────────
STOP = {
    "the","a","an","is","are","of","in","on","at","by","for","with","from","to",
    "and","or","but","not","this","that","what","which","who","how",
    "show","list","get","tell","year","month","quarter","week","day","time",
}

# GraphDB FTS namespace
LUC = "http://www.ontotext.com/owlim/lucene#"

# ── Structural SPARQL patterns ────────────────────────────────────────────────
STRUCTURAL_PATTERNS = [
    (re.compile(r"gold\s+table|aggregate\s+table|kpi\s+table|all\s+gold", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "gold" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    (re.compile(r"\ball\s+(?:the\s+)?fact\s+table|\bfact\s+tables?\s+(?:available|do\s+we|exist)", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:FactTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"\ball\s+(?:the\s+)?dim(?:ension)?\s+table|\bdim(?:ension)?\s+tables?\s+(?:available|do\s+we|exist|are\s+there)|all\s+dim\b", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t rdf:type/rdfs:subClassOf* biz:DimensionTable ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?tableName"),
    (re.compile(r"\bpii\b|personally\s+identifiable", re.I),
     "SELECT DISTINCT ?tableName ?layer WHERE { ?t biz:tableName ?tableName ; biz:hasColumn ?col . ?col biz:isPII true . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName"),
    (re.compile(r"tables?\s+(?:\w+\s+)*(?:on|in)\s+snowflake|snowflake\s+table|(?:on|in)\s+snowflake\b", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "snowflake" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    (re.compile(r"tables?\s+(?:\w+\s+)*(?:on|in)\s+databricks|databricks\s+table|(?:on|in)\s+databricks\b", re.I),
     'SELECT ?tableName ?layer WHERE { ?t biz:sourceSystemType "databricks" ; biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } } ORDER BY ?layer ?tableName'),
    (re.compile(r"kpis?\s+(?:\w+\s+)*(?:available|in\s+the)|all\s+kpis?\b|list\s+kpis?\b|kpis?\s+(?:do\s+we\s+have|registered)", re.I),
     "SELECT ?kpiName ?domain ?direction WHERE { ?k a biz:KPI ; biz:kpiName ?kpiName . OPTIONAL { ?k biz:kpiDomain ?domain } OPTIONAL { ?k biz:kpiDirection ?direction } } ORDER BY ?domain ?kpiName"),
    (re.compile(r"silver\s+table|silver\s+layer|all\s+silver", re.I),
     'SELECT ?tableName ?platform WHERE { ?t biz:tableLayer "silver" ; biz:tableName ?tableName . OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?platform ?tableName'),
    (re.compile(r"\blineage\b|feeds?\s+into|cross.platform", re.I),
     "SELECT ?srcName ?srcPlatform ?tgtName ?tgtPlatform ?transformType WHERE { ?src biz:feedsInto ?tgt . ?src biz:tableName ?srcName . ?tgt biz:tableName ?tgtName . OPTIONAL { ?src biz:sourceSystemType ?srcPlatform } OPTIONAL { ?tgt biz:sourceSystemType ?tgtPlatform } ?edge biz:sourceTable ?src ; biz:targetTable ?tgt . OPTIONAL { ?edge biz:lineageTransformType ?transformType } } ORDER BY ?srcName"),
    (re.compile(r"owl\s+class(?:es)?|ontolog(?:y|ical)\s+class(?:es)?", re.I),
     "SELECT ?className ?comment WHERE { ?c a owl:Class . BIND(STRAFTER(STR(?c),'#') AS ?className) OPTIONAL { ?c rdfs:comment ?comment } } ORDER BY ?className"),
    (re.compile(r"\bconcepts?\b|\bglossary\b|rdf\s+concepts?|in\s+the\s+knowledge\s+graph\b", re.I),
     "SELECT ?conceptName ?definition WHERE { ?c a skos:Concept ; skos:prefLabel ?conceptName . OPTIONAL { ?c skos:definition ?definition } } ORDER BY ?conceptName"),
    (re.compile(r"what\s+(?:the\s+)?tables?\s+(?:are\s+)?(?:available|in\s+the)|(?:all|list)\s+(?:the\s+)?tables?\s+(?:available|in\s+the\s+(?:warehouse|dwh)|we\s+have)", re.I),
     "SELECT ?tableName ?layer ?platform WHERE { ?t biz:tableName ?tableName . OPTIONAL { ?t biz:tableLayer ?layer } OPTIONAL { ?t biz:sourceSystemType ?platform } } ORDER BY ?layer ?platform ?tableName"),
    (re.compile(r"\bdomain\b.*\b(?:available|exist|do\s+we|have|in\s+the)|list\s+(?:all\s+)?domains?|what\s+(?:business\s+)?domains?", re.I),
     "SELECT DISTINCT ?domain WHERE { ?t biz:tableDomain ?domain } ORDER BY ?domain"),
]

EXPLICIT_TABLE_PAT = re.compile(r"\b((?:dim|fct|fact|agg)_[a-z][a-z0-9_]*)", re.I)

SCHEMA_PAT = re.compile(
    r"what\s+(?:kind|type|sort|column|field|data|info|information|detail)"
    r"|describe\s+(?:the\s+)?(?:\w+\s+)*(?:table|column|schema|structure)"
    r"|what\s+(?:does|is\s+in)\s+the\s+\w+\s+(?:table|dimension|fact)"
    r"|what\s+(?:data|fields?|columns?|information)\s+(?:is|are)\s+in"
    r"|columns?\s+(?:in|of|for|available|does\s+\w+\s+have)"
    r"|show\s+(?:me\s+)?(?:the\s+)?(?:column|schema|structure|field)",
    re.I,
)

OVERRIDES = [
    (r"\bchurn\b(?!.*segment)",            "customer_360",        25.0),
    (r"\bclv\b|\blifetime\s+value",        "customer_360",        25.0),
    (r"\binventor",                        "inventory",           20.0),
    (r"\bpromo\b|\bpromotion\b",           "promotion",           20.0),
    (r"\bstore\b",                         "store_performance",   18.0),
    (r"\bquota\b",                         "employee_perform",    25.0),
    (r"\brevenue.*month\b|\bmonthly.*revenue","revenue_monthly",  20.0),
    (r"\brevenue.*daily\b|\bdaily.*revenue","revenue_daily",      20.0),
    (r"\bproduct\b",                       "product_performance", 15.0),
    (r"\bcustomer\b(?!.*segment)",         "customer_360",        20.0),
    (r"\bsupplier\b",                      "supplier",            25.0),
    (r"\bpayment\b",                       "payment",             25.0),
    (r"\bregion\b|\bgeograph|\bstate\b|\bstates\b","geographic",  40.0),
    (r"\bexecutive\b|\bc.suite\b",         "executive",           25.0),
    (r"\bcohort\b",                        "cohort",              25.0),
    (r"\bsegment\b",                       "customer_segment",    28.0),
    (r"\bchannel\b.*\b(?:perform|margin|profit)\b|\b(?:perform|margin|profit)\b.*\bchannel\b",
                                           "channel_perform",     35.0),
    (r"\bemployee\b|\bcommission\b",       "employee",            20.0),
]


# ── Public helpers ─────────────────────────────────────────────────────────────

def q_tokens(question: str) -> set:
    return {t for t in re.findall(r"\b[a-zA-Z]{3,}\b", question.lower())
            if t not in STOP}


def rows_to_text(rows: list) -> str:
    if not rows: return "No results."
    cols = list(rows[0].keys())
    w    = max(22, max(len(c) for c in cols))
    hdr  = " | ".join(f"{c:<{w}}" for c in cols)
    return f"{hdr}\n{'-'*len(hdr)}\n" + "\n".join(
        " | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols) for r in rows[:60])


def check_structural(question: str, token: str) -> dict | None:
    """Check if question matches a structural pattern. Returns rows or None."""
    is_schema    = bool(SCHEMA_PAT.search(question))
    has_explicit = bool(EXPLICIT_TABLE_PAT.search(question))
    if is_schema or has_explicit:
        return None

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
            return {"rows": rows, "text": rows_to_text(rows)}
    return None


def direct_lookup(table_name: str, token: str) -> dict:
    """Exact lookup by table name when question explicitly names a table."""
    rows = graphdb.query(
        f'SELECT ?tname ?layer ?platform WHERE {{'
        f' ?t biz:tableName ?tname .'
        f' FILTER (LCASE(STR(?tname)) = "{table_name.lower()}")'
        f' OPTIONAL {{ ?t biz:tableLayer ?layer }}'
        f' OPTIONAL {{ ?t biz:sourceSystemType ?platform }}'
        f'}} LIMIT 1', token) or []
    if rows:
        r = rows[0]
        return {"name": r.get("tname",""), "platform": (r.get("platform") or "").lower(),
                "layer": (r.get("layer") or "gold").lower()}
    return {}


# ── FTS functions ─────────────────────────────────────────────────────────────

def fts_available(token: str) -> bool:
    """Check if the FTS index exists in GraphDB."""
    try:
        rows = graphdb.query(
            "SELECT ?idx WHERE { "
            "  <http://www.ontotext.com/owlim/lucene#tableIndex> "
            "  <http://www.ontotext.com/owlim/lucene#listIndexes> ?idx "
            "} LIMIT 1", token)
        return bool(rows)
    except Exception:
        return False


def fts_setup(token: str) -> bool:
    """
    Create the FTS index in GraphDB. Run once.
    Indexes: tableName, columnName, kpiName, prefLabel, altLabel, definition, scopeNote
    Returns True if successful.
    """
    sparql = """
        PREFIX luc: <http://www.ontotext.com/owlim/lucene#>

        INSERT DATA {
            luc:tableIndex luc:setParam "index" ;
                luc:setParam "uris=disabled" ;
                luc:setParam "predicates=biz:tableName biz:columnName biz:kpiName skos:prefLabel skos:altLabel skos:definition skos:scopeNote" ;
                luc:setParam "languages=en" .
        }
    """
    try:
        # FTS setup uses INSERT DATA — need POST endpoint
        import requests
        from src import config
        base  = config.get("GRAPHDB_BASE_URL")
        repo  = config.get("GRAPHDB_REPO")
        r = requests.post(
            f"{base}/repositories/{repo}/statements",
            params={"update": sparql},
            headers={"Authorization": token,
                     "Content-Type": "application/sparql-update"},
            timeout=30,
        )
        return r.status_code in (200, 204)
    except Exception:
        return False


def fts_search(question: str, token: str, top_k: int = 10) -> dict:
    """
    Search GraphDB using the Lucene FTS index.

    Returns {table_name: fts_score} — score is Lucene relevance (higher = better).

    The FTS query:
      - Searches across tableName, columnName, kpiName, prefLabel, altLabel,
        definition and scopeNote in one shot
      - Follows the node back to its parent table via biz:hasColumn / biz:hasKPI
      - Supports fuzzy matching: "churn~" matches "churning", "churned"
      - Supports phrase: '"gross margin"' matches exact phrase
    """
    # Clean question into FTS query string
    # Remove stop words, add fuzzy (~) to longer tokens for robustness
    tokens = q_tokens(question)
    if not tokens:
        return {}

    # Build Lucene query string — use OR between terms, fuzzy on long words
    fts_terms = []
    for t in tokens:
        if len(t) >= 6:
            fts_terms.append(f"{t}~0.8")   # fuzzy match e.g. "churning" finds "churn"
        else:
            fts_terms.append(t)
    fts_query = " OR ".join(fts_terms)

    sparql = f"""
        PREFIX luc: <http://www.ontotext.com/owlim/lucene#>

        SELECT ?tableName ?layer ?platform ?score WHERE {{

            # Search hits on ANY indexed predicate
            ?node luc:tableIndex "{fts_query}" ;
                  luc:score ?score .

            {{
                # Case 1: node IS a table — direct match on tableName
                ?node biz:tableName ?tableName .
                OPTIONAL {{ ?node biz:tableLayer ?layer }}
                OPTIONAL {{ ?node biz:sourceSystemType ?platform }}
            }}
            UNION
            {{
                # Case 2: node is a COLUMN — walk up to parent table
                ?table biz:hasColumn ?node .
                ?table biz:tableName ?tableName .
                OPTIONAL {{ ?table biz:tableLayer ?layer }}
                OPTIONAL {{ ?table biz:sourceSystemType ?platform }}
            }}
            UNION
            {{
                # Case 3: node is a KPI — walk up to parent table
                ?table biz:hasKPI ?node .
                ?table biz:tableName ?tableName .
                OPTIONAL {{ ?table biz:tableLayer ?layer }}
                OPTIONAL {{ ?table biz:sourceSystemType ?platform }}
            }}
            UNION
            {{
                # Case 4: node is a SKOS concept — resolve via mapsToColumn → table
                ?node a skos:Concept .
                ?col_node biz:columnName ?colName .
                ?table biz:hasColumn ?col_node ;
                       biz:tableName ?tableName .
                OPTIONAL {{ ?table biz:tableLayer ?layer }}
                OPTIONAL {{ ?table biz:sourceSystemType ?platform }}
            }}
        }}
        ORDER BY DESC(?score)
        LIMIT {top_k * 3}
    """

    rows = graphdb.query(sparql, token) or []
    if not rows:
        return {}

    # Aggregate scores per table (take max score if table appears multiple times)
    scores: dict = {}
    meta:   dict = {}
    for r in rows:
        tn = r.get("tableName","")
        if not tn: continue
        sc = float(r.get("score", 0))
        if sc > scores.get(tn, -1):
            scores[tn] = sc
            meta[tn]   = {
                "platform": (r.get("platform") or "").lower(),
                "layer":    (r.get("layer") or "").lower(),
            }

    # Apply layer preference bonus (same logic as token scoring)
    q_lower   = question.lower()
    want_dim  = bool(re.search(r"\bdim(?:ension)?\b|\bdim_[a-z]", q_lower))
    want_fact = bool(re.search(r"\bfact\b|\bfct\b|\bfct_[a-z]", q_lower))
    want_gold = not want_dim and not want_fact

    for tn, info in meta.items():
        layer = info["layer"]; tn_l = tn.lower()
        if   want_dim  and layer == "silver" and (tn_l.startswith("dim_") or "_dim_" in tn_l):
            scores[tn] += 5.0
        elif want_fact and layer == "silver" and (tn_l.startswith("fct_") or tn_l.startswith("fact_")):
            scores[tn] += 5.0
        elif want_gold and layer == "gold":
            scores[tn] += 5.0

    # Keyword overrides on top
    if want_gold:
        for pattern, tbl_sub, boost in OVERRIDES:
            if re.search(pattern, q_lower):
                for tn in scores:
                    if tbl_sub.lower() in tn.lower():
                        scores[tn] += boost * 0.5  # half weight — FTS already ranked it

    return {tn: round(sc, 4)
            for tn, sc in sorted(scores.items(), key=lambda x: -x[1])[:top_k]}


def fts_best_table(question: str, token: str) -> dict:
    """
    Use FTS to find the best matching table.
    Returns {name, platform, layer} or {} if FTS unavailable/no results.
    """
    scores = fts_search(question, token)
    if not scores:
        return {}

    best = max(scores, key=scores.get)

    # Re-fetch platform/layer for the winner
    rows = graphdb.query(
        f'SELECT ?tname ?layer ?platform WHERE {{'
        f' ?t biz:tableName ?tname .'
        f' FILTER (LCASE(STR(?tname)) = "{best.lower()}")'
        f' OPTIONAL {{ ?t biz:tableLayer ?layer }}'
        f' OPTIONAL {{ ?t biz:sourceSystemType ?platform }}'
        f'}} LIMIT 1', token) or []

    if rows:
        r = rows[0]
        return {"name": r.get("tname", best),
                "platform": (r.get("platform") or "").lower(),
                "layer":    (r.get("layer") or "gold").lower(),
                "fts_scores": scores}
    return {"name": best, "platform": "", "layer": "gold", "fts_scores": scores}


# ── Token scoring (fallback) ───────────────────────────────────────────────────

def fetch_all_tables(token: str) -> dict:
    """Fetch all tables + columns from GraphDB for token scoring fallback."""
    rows = graphdb.query("""
        SELECT DISTINCT ?tname ?layer ?platform ?cname WHERE {
            ?t biz:tableName ?tname .
            OPTIONAL { ?t biz:tableLayer ?layer }
            OPTIONAL { ?t biz:sourceSystemType ?platform }
            OPTIONAL { ?t biz:hasColumn ?col . ?col biz:columnName ?cname }
        }""", token) or []

    tables: dict = {}
    for r in rows:
        tn = r.get("tname","")
        if not tn: continue
        if tn not in tables:
            tables[tn] = {
                "platform": r.get("platform",""),
                "layer":    (r.get("layer") or "").lower(),
                "name_tok": set(re.findall(r"[a-zA-Z]+", tn.lower())),
                "col_tok":  set(),
            }
        if r.get("cname"):
            for p in re.findall(r"[a-zA-Z]+", r["cname"].lower()):
                tables[tn]["col_tok"].add(p)
    return tables


def score_tables(question: str, tables: dict,
                 candidate_names: list | None = None) -> dict:
    """Token-overlap scoring with IDF + layer preference + overrides. Fallback mode."""
    q_lower   = question.lower()
    want_dim  = bool(re.search(r"\bdim(?:ension)?\b|\bdim_[a-z]", q_lower))
    want_fact = bool(re.search(r"\bfact\b|\bfct\b|\bfct_[a-z]|\bfact_[a-z]", q_lower))
    want_gold = not want_dim and not want_fact

    if candidate_names:
        working = {k: v for k, v in tables.items()
                   if any(c.lower() in k.lower() or k.lower() in c.lower()
                          for c in candidate_names)}
        if not working: working = tables
    else:
        working = tables

    q_tok = q_tokens(question)
    if not q_tok:
        first = next(iter(working))
        return {first: 1.0}

    def _m(qt, surface):
        return any(qt == s or (len(qt) >= 4 and len(s) >= 4
                               and (qt.startswith(s) or s.startswith(qt)))
                   for s in surface)

    nhc: dict = {}; chc: dict = {}
    for tn, info in working.items():
        for qt in q_tok:
            if   _m(qt, info["name_tok"]): nhc[qt] = nhc.get(qt,0) + 1
            elif _m(qt, info["col_tok"]):  chc[qt] = chc.get(qt,0) + 1

    scores: dict = {}
    for tn, info in working.items():
        s = sum(8.0/math.log1p(nhc.get(qt,1)) if _m(qt,info["name_tok"]) else
                (1.0/math.log1p(chc.get(qt,1)) if _m(qt,info["col_tok"]) else 0)
                for qt in q_tok)
        layer = info["layer"]; tn_l = tn.lower()
        if   want_dim  and layer=="silver" and (tn_l.startswith("dim_") or "_dim_" in tn_l): s+=12.0
        elif want_fact and layer=="silver" and (tn_l.startswith("fct_") or tn_l.startswith("fact_")): s+=12.0
        elif want_gold and layer=="gold": s+=12.0

        # Column coverage boost — reward tables whose columns match question tokens
        # This breaks ties when table names score similarly
        # e.g. "revenue by product" → agg_product_performance has product_name, sku
        #       agg_revenue_monthly has year_month, total_revenue
        # product_name/sku tokens boost agg_product_performance decisively
        col_matches = sum(1 for qt in q_tok if _m(qt, info["col_tok"]))
        s += col_matches * 2.0

        if s > 0: scores[tn] = s

    if want_gold:
        for pattern, tbl_sub, boost in OVERRIDES:
            if re.search(pattern, q_lower):
                for tn in working:
                    if tbl_sub.lower() in tn.lower():
                        scores[tn] = scores.get(tn,0.0) + boost

    if (want_dim or want_fact) and not any(
            working.get(tn,{}).get("layer")=="silver" for tn in scores):
        for tn, info in working.items():
            if info["layer"]=="gold" and tn not in scores:
                s = sum(8.0/math.log1p(nhc.get(qt,1))
                        for qt in q_tok if _m(qt,info["name_tok"]))
                if s > 0: scores[tn] = s

    return scores
