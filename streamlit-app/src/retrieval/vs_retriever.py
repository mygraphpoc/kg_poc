"""
src/retrieval/vs_retriever.py
─────────────────────────────
Responsibilities:
  1. Embed questions using the Databricks-hosted BGE-Large endpoint
     (no sentence-transformers / PyTorch — pure HTTP call)
  2. Query the Databricks Delta Sync Vector Search index
  3. Return ranked candidate table names with VS scores

Architecture note:
  This module is the PRIMARY retriever for 500+ tables.
  For the POC (60 tables) it is used in HYBRID mode alongside SPARQL scoring.
  The embedding call is cached per question using st.cache_data (TTL 1 hour).
"""

import streamlit as st
import requests
from src import config


# ── Embedding via Databricks hosted endpoint ───────────────────────────────────
# Uses databricks-bge-large-en (1024-dim) or text-embedding-3-small equivalent
# No sentence-transformers needed — pure HTTP POST to the serving endpoint

def embed(text: str) -> list[float] | None:
    """
    Embed a text string using the Databricks-hosted embedding endpoint.
    Returns a list of floats, or None on failure.
    """
    host  = config.get("DATABRICKS_HOST")
    token = config.get("DATABRICKS_TOKEN")
    endpoint = config.get("EMBEDDING_ENDPOINT", "databricks-bge-large-en")

    if not host.startswith("http"):
        host = f"https://{host}"
    try:
        r = requests.post(
            f"{host}/serving-endpoints/{endpoint}/invocations",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            json={"input": [text]},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()["data"][0]["embedding"]
        return None
    except Exception:
        return None


# ── Vector Search query ────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _get_vs_index():
    """Get VS index client — cached for session lifetime."""
    host  = config.get("DATABRICKS_HOST")
    token = config.get("DATABRICKS_TOKEN")
    if not host.startswith("http"):
        host = f"https://{host}"
    try:
        from databricks.vector_search.client import VectorSearchClient
        client = VectorSearchClient(
            workspace_url=host,
            personal_access_token=token,
            disable_notice=True,
        )
        return client.get_index(
            endpoint_name=config.get("VS_ENDPOINT_NAME"),
            index_name=config.get("VS_INDEX_NAME"),
        )
    except Exception:
        return None


def search(question: str, top_k: int = 10) -> list[dict]:
    """
    Search the VS index for the question.
    Returns list of dicts: {table_name, layer, platform, score, item_type}
    Score is normalised 0-1 (rank-based: rank1=1.0, rank2=0.9 … rank10=0.1)
    """
    vec = embed(question)
    if vec is None:
        return []

    idx = _get_vs_index()
    if idx is None:
        return []

    try:
        res = idx.similarity_search(
            query_vector=vec,
            columns=["item_id", "item_type", "item_name", "full_path",
                     "layer", "source_system_type", "doc_text"],
            num_results=top_k,
        )
    except Exception:
        return []

    manifest_cols = [c["name"] for c in res.get("manifest", {}).get("columns", [])]
    hits = [dict(zip(manifest_cols, row))
            for row in res.get("result", {}).get("data_array", [])]

    results = []
    seen: set = set()
    for rank, hit in enumerate(hits, 1):
        itype = (hit.get("item_type") or "").lower()
        fp    = hit.get("full_path", "") or ""
        name  = hit.get("item_name", "") or ""

        # Resolve to parent table name
        if itype == "table":
            tname = name
        elif "." in fp:
            # full_path = "kg_vs_poc.gold.agg_customer_360" → "agg_customer_360"
            parts = fp.split(".")
            tname = parts[-2] if len(parts) >= 3 else parts[-1]
        elif fp and itype in ("kpi", "column", "concept"):
            # full_path = "agg_product_performance" (parent table name stored directly)
            tname = fp
        else:
            tname = name

        tname = tname.strip()
        if not tname or tname in seen:
            continue
        seen.add(tname)

        # Rank-based normalised score: rank1=1.0, rank2=0.9, etc.
        score = max(0.0, 1.0 - (rank - 1) * 0.1)
        results.append({
            "table_name": tname,
            "layer":      (hit.get("layer") or "").lower(),
            "platform":   (hit.get("source_system_type") or "").lower(),
            "score":      score,
            "item_type":  itype,
        })

    return results


def vs_score_dict(question: str, top_k: int = 10) -> dict:
    """
    Returns {table_name: vs_score} normalised 0-1.
    Used for combining with SPARQL scores.
    """
    hits = search(question, top_k=top_k)
    return {h["table_name"]: h["score"] for h in hits}
