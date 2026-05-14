"""
src/retrieval/hybrid_retriever.py
──────────────────────────────────
Three-way hybrid: FTS + VS + SPARQL token scoring

Combination:
    combined = FTS×0.5 + VS×0.3 + SPARQL×0.2   (when all three available)
    combined = FTS×0.6 + SPARQL×0.4             (when VS unavailable)
    combined = VS×0.6 + SPARQL×0.4              (when FTS unavailable)
    SPARQL only                                  (last resort)

Platform and layer always resolved from GraphDB — never hardcoded.
"""

import streamlit as st
from src import graphdb, config
from src.retrieval import sparql_retriever as sr
from src.retrieval import vs_retriever as vr

FTS_WEIGHT    = 0.5
VS_WEIGHT     = 0.3
SPARQL_WEIGHT = 0.2


@st.cache_data(show_spinner=False, ttl=300)
def _get_table_catalogue(_token: str) -> dict:
    return sr.fetch_all_tables(_token)


def _normalise(scores: dict) -> dict:
    if not scores: return {}
    mx = max(scores.values())
    return {k: v/mx for k,v in scores.items()} if mx > 0 else scores


def lookup_table_meta(table_name: str, token: str) -> dict:
    """
    Look up platform and layer for a table directly from GraphDB.
    Uses LCASE filter so uppercase/lowercase both resolve correctly.
    Returns {name, platform, layer} or {} if not found.
    Always called as final authority — never use cached catalogue for platform.
    """
    rows = graphdb.query(
        'SELECT ?tname ?layer ?platform WHERE {'
        ' ?t biz:tableName ?tname .'
        ' FILTER (LCASE(STR(?tname)) = "' + table_name.lower() + '")'
        ' OPTIONAL { ?t biz:tableLayer ?layer }'
        ' OPTIONAL { ?t biz:sourceSystemType ?platform }'
        '} LIMIT 1', token) or []
    if not rows:
        return {}
    r = rows[0]
    return {
        "name":     r.get("tname", table_name),
        "platform": (r.get("platform") or "databricks").lower(),
        "layer":    (r.get("layer") or "gold").lower(),
    }


def find_best_table(question: str, token: str, record=None) -> dict:
    """
    Find best table using FTS + VS + SPARQL in combination.
    Platform and layer always resolved from GraphDB.
    record(icon, msg) for live UI logging.
    """
    def log(icon, msg):
        if record: record(icon, msg)

    # ── Explicit table name → direct lookup ──────────────────────────────────
    explicit = sr.EXPLICIT_TABLE_PAT.search(question)
    if explicit:
        named = explicit.group(1)
        log("🎯", f"Explicit table named: **{named}** — direct lookup")
        meta = lookup_table_meta(named, token)
        if meta:
            meta["via"] = "direct_lookup"
            return meta
        log("⚠️", f"'{named}' not found in graph — falling back to search")

    tables = _get_table_catalogue(token)
    if not tables:
        log("❌", "No tables in GraphDB catalogue")
        return {}

    fts_scores    = {}
    vs_scores     = {}
    sparql_scores = {}

    # ── FTS ───────────────────────────────────────────────────────────────────
    log("🔍", "FTS — searching GraphDB Lucene index…")
    try:
        fts_scores = sr.fts_search(question, token, top_k=10)
        if fts_scores:
            top = sorted(fts_scores.items(), key=lambda x:-x[1])[:3]
            log("🔍", f"FTS top: {', '.join(f'{t}({s:.2f})' for t,s in top)}")
        else:
            log("⚠️", "FTS returned no results")
    except Exception as e:
        log("⚠️", f"FTS error: {str(e)[:60]}")

    # ── VS ────────────────────────────────────────────────────────────────────
    log("🔎", "VS — semantic search…")
    try:
        vs_scores = vr.vs_score_dict(question, top_k=10)
        if vs_scores:
            top = sorted(vs_scores.items(), key=lambda x:-x[1])[:3]
            log("🔎", f"VS top: {', '.join(f'{t}({s:.2f})' for t,s in top)}")
        else:
            log("⚠️", "VS returned no results")
    except Exception as e:
        log("⚠️", f"VS error: {str(e)[:60]}")

    # ── SPARQL token scoring ──────────────────────────────────────────────────
    log("📊", "SPARQL keyword scoring…")
    candidate_names = list({*fts_scores.keys(), *vs_scores.keys()}) or None
    sparql_scores   = sr.score_tables(question, tables, candidate_names)
    if sparql_scores:
        top = sorted(sparql_scores.items(), key=lambda x:-x[1])[:3]
        log("📊", f"SPARQL top: {', '.join(f'{t}({s:.2f})' for t,s in top)}")

    # ── Combine ───────────────────────────────────────────────────────────────
    fts_n    = _normalise(fts_scores)
    vs_n     = _normalise(vs_scores)
    sparql_n = _normalise(sparql_scores)
    all_tbls = set(fts_n) | set(vs_n) | set(sparql_n)

    if not all_tbls:
        log("❌", "No results from any retriever")
        return {}

    have_fts = bool(fts_n)
    have_vs  = bool(vs_n)

    if have_fts and have_vs:
        combined = {t: fts_n.get(t,0)*FTS_WEIGHT +
                        vs_n.get(t,0)*VS_WEIGHT   +
                        sparql_n.get(t,0)*SPARQL_WEIGHT
                    for t in all_tbls}
        mode = f"FTS×{FTS_WEIGHT}+VS×{VS_WEIGHT}+SPARQL×{SPARQL_WEIGHT}"
    elif have_fts:
        combined = {t: fts_n.get(t,0)*0.6 + sparql_n.get(t,0)*0.4
                    for t in all_tbls}
        mode = "FTS×0.6+SPARQL×0.4"
    elif have_vs:
        combined = {t: vs_n.get(t,0)*0.6 + sparql_n.get(t,0)*0.4
                    for t in all_tbls}
        mode = "VS×0.6+SPARQL×0.4"
    else:
        combined = sparql_scores
        mode = "SPARQL only"

    log("🔀", f"Combining: {mode}")

    # Deduplicate upper/lowercase — merge scores, keep lowercase name
    deduped = {}
    for tn, sc in combined.items():
        key = tn.lower()
        if key not in deduped:
            deduped[key] = {"name": tn.lower(), "score": sc}
        else:
            deduped[key]["score"] += sc
    combined = {v["name"]: v["score"] for v in deduped.values()}

    best = max(combined, key=combined.get)
    top5 = {k: round(v,3) for k,v in
            sorted(combined.items(), key=lambda x:-x[1])[:5]}

    # Always resolve platform/layer from GraphDB — single source of truth
    meta = lookup_table_meta(best, token)
    if not meta:
        meta = {"name": best, "platform": "databricks", "layer": "gold"}

    meta["scores"] = top5
    meta["mode"]   = mode
    return meta
