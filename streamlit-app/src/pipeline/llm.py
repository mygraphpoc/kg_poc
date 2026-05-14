"""
src/pipeline/llm.py
────────────────────
LLM configuration with automatic fallback.

Model priority (all available in the workspace screenshot):
  1. meta_llama_v3_1_8b       ← FREE, fast, good for SQL + summarisation
  2. mixtral_8x7b_v0_1        ← FREE, better reasoning, fallback
  3. databricks-claude-haiku-4-5  ← Paid, fast + accurate if budget allows
  4. databricks-claude-sonnet-4-6 ← Paid, best quality

Set LLM_ENDPOINT in secrets to override.
Default: meta_llama_v3_1_8b (free tier safe).
"""

import streamlit as st
from src import config

# Default to the free Llama model visible in the workspace
DEFAULT_ENDPOINT = "meta_llama_v3_1_8b"


@st.cache_resource(show_spinner=False)
def get_llm():
    """
    Return a ChatDatabricks LLM instance.
    Endpoint is read from secrets (LLM_ENDPOINT) or falls back to DEFAULT_ENDPOINT.
    """
    from databricks_langchain import ChatDatabricks
    endpoint = config.get("LLM_ENDPOINT", DEFAULT_ENDPOINT)
    return ChatDatabricks(endpoint=endpoint, temperature=0)


def get_endpoint_name() -> str:
    return config.get("LLM_ENDPOINT", DEFAULT_ENDPOINT)
