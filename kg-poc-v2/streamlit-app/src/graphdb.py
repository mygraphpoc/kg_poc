"""
src/graphdb.py — GraphDB connection + SPARQL helper.
All queries include the standard prefixes automatically.
"""

import requests
import streamlit as st
from src import config

PREFIXES = """
PREFIX biz:  <https://ontology.sales-dwh.example.com/ontology/biz#>
PREFIX bizs: <https://ontology.sales-dwh.example.com/skos/>
PREFIX meta: <https://ontology.sales-dwh.example.com/meta#>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
"""


@st.cache_resource(show_spinner=False)
def get_token() -> tuple[str | None, str | None]:
    base = config.get("GRAPHDB_BASE_URL")
    user = config.get("GRAPHDB_USER")
    pw   = config.get("GRAPHDB_PASSWORD")

    if not all([base, user, pw]):
        return None, "GraphDB credentials not configured."

    try:
        r = requests.post(
            f"{base}/rest/login",
            json={"username": user, "password": pw},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
    except Exception as exc:
        return None, f"GraphDB connection error: {exc}"

    if r.status_code != 200:
        return None, f"GraphDB login failed (HTTP {r.status_code})"

    # GraphDB returns the token in different places depending on version:
    # v10.x  → Authorization header: "GDB <token>"
    # v11.x  → Authorization header OR JSON body {"token": "..."}
    # Sandbox → sometimes X-Auth-Token header

    # 1. Check Authorization header first
    token = r.headers.get("Authorization", "").strip()
    if token:
        if not token.startswith("GDB ") and not token.startswith("Bearer "):
            token = "GDB " + token
        return token, None

    # 2. Check X-Auth-Token header
    token = r.headers.get("X-Auth-Token", "").strip()
    if token:
        if not token.startswith("GDB "):
            token = "GDB " + token
        return token, None

    # 3. Check JSON body
    if r.text:
        try:
            body  = r.json()
            token = (body.get("token") or
                     body.get("Authorization") or
                     body.get("access_token") or "").strip()
            if token:
                if not token.startswith("GDB ") and not token.startswith("Bearer "):
                    token = "GDB " + token
                return token, None
        except Exception:
            pass

    # 4. Last resort — use Basic auth for older GraphDB versions
    import base64
    basic = base64.b64encode(f"{user}:{pw}".encode()).decode()
    return f"Basic {basic}", None


def query(sparql: str, token: str) -> list[dict]:
    """Run SPARQL SELECT, return list of row dicts."""
    base = config.get("GRAPHDB_BASE_URL")
    repo = config.get("GRAPHDB_REPO")
    try:
        r = requests.get(
            f"{base}/repositories/{repo}",
            params={"query": PREFIXES + "\n" + sparql},
            headers={"Authorization": token,
                     "Accept": "application/sparql-results+json"},
            timeout=30,
        )
        if r.status_code != 200:
            return []
        if not r.text:
            return []
        data = r.json()
        rows = []
        for b in data.get("results", {}).get("bindings", []):
            row = {}
            for k, v in b.items():
                val = v.get("value", "")
                dt  = v.get("datatype", "")
                if   "integer" in dt or "long" in dt:
                    try: val = int(val)
                    except: pass
                elif "boolean" in dt:
                    val = val.lower() == "true"
                row[k] = val
            rows.append(row)
        return rows
    except Exception:
        return []
