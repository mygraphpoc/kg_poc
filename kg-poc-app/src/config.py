"""
src/config.py — Credential management.

Priority:
  1. Streamlit Cloud Secrets  (st.secrets)
  2. Environment variables    (os.getenv)
  3. Session state            (user-entered via UI, lives for the browser session)

Call `is_configured()` to check whether all required keys are present.
Call `get(key)` to retrieve any config value regardless of source.
Call `save_to_session(values_dict)` when the user submits the setup form.
"""

import os
import streamlit as st

REQUIRED_KEYS = [
    "GRAPHDB_BASE_URL",
    "GRAPHDB_USER",
    "GRAPHDB_PASSWORD",
    "GRAPHDB_REPO",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "SQL_WAREHOUSE_HTTP",
    "VS_ENDPOINT_NAME",
    "INDEX_NAME",
    "SF_ACCOUNT",
    "SF_USER",
    "SF_PASSWORD",
    "SF_DATABASE",
    "SF_WAREHOUSE",
    "ANTHROPIC_API_KEY",
]

DEFAULTS = {
    "GRAPHDB_REPO":    "KG_POC_DBX_SF",
    "VS_ENDPOINT_NAME":"kg-poc-dbx-sf-vs",
    "INDEX_NAME":      "KG_POC_metadata.vector_registry.embeddings_index",
    "SF_DATABASE":     "KG_POC",
    "SF_WAREHOUSE":    "COMPUTE_WH",
}


def _session_store() -> dict:
    if "_cfg" not in st.session_state:
        st.session_state["_cfg"] = {}
    return st.session_state["_cfg"]


def get(key: str, fallback: str = "") -> str:
    """Return a config value from secrets → env → session → default."""
    # 1. Streamlit secrets
    try:
        val = st.secrets.get(key)
        if val:
            return str(val)
    except Exception:
        pass
    # 2. Environment variable
    val = os.getenv(key)
    if val:
        return val
    # 3. Session state (user-entered via UI this session)
    val = _session_store().get(key)
    if val:
        return str(val)
    # 4. Hard-coded defaults
    return DEFAULTS.get(key, fallback)


def save_to_session(values: dict) -> None:
    """Persist user-entered credentials to session state."""
    store = _session_store()
    for k, v in values.items():
        if v:
            store[k] = str(v)


def is_configured() -> bool:
    """Return True if every required key has a non-empty value."""
    return all(get(k) for k in REQUIRED_KEYS)


def missing_keys() -> list[str]:
    return [k for k in REQUIRED_KEYS if not get(k)]


def as_dict() -> dict:
    """Return all config values as a plain dict (for display / passing around)."""
    return {k: get(k) for k in REQUIRED_KEYS}
