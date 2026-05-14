"""
src/config.py — Credential loading.
Priority: st.secrets → env vars → defaults.
"""

import os
import streamlit as st

REQUIRED_KEYS = [
    "GRAPHDB_BASE_URL", "GRAPHDB_USER", "GRAPHDB_PASSWORD", "GRAPHDB_REPO",
    "DATABRICKS_HOST",  "DATABRICKS_TOKEN",
    "VS_ENDPOINT_NAME", "VS_INDEX_NAME", "EMBEDDING_ENDPOINT",
    "SF_ACCOUNT", "SF_USER", "SF_PASSWORD",
]

DEFAULTS = {
    "GRAPHDB_REPO":        "kg_vs_poc_dbx_sf",
    "SF_DATABASE":         "KG_VS_POC",
    "SF_WAREHOUSE":        "COMPUTE_WH",
    "VS_INDEX_NAME":       "kg_vs_poc_metadata.vector_registry.embeddings_index",
    "EMBEDDING_ENDPOINT":  "databricks-bge-large-en",
    "LLM_ENDPOINT":        "databricks-meta-llama-3-3-70b-instruct",
    "TTYG_AGENT_ID":       "",
    "DATABRICKS_CATALOG":  "kg_vs_poc",
}


def get(key: str, fallback: str = "") -> str:
    try:
        val = st.secrets.get(key)
        if val: return str(val)
    except Exception:
        pass
    val = os.getenv(key)
    if val: return val
    return DEFAULTS.get(key, fallback)


def is_configured() -> bool:
    return all(get(k) for k in REQUIRED_KEYS)


def missing_keys() -> list[str]:
    return [k for k in REQUIRED_KEYS if not get(k)]
