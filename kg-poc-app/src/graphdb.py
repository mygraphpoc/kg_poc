"""src/graphdb.py — GraphDB connection and SPARQL query helper."""

import requests
import streamlit as st
from src import config

SPARQL_PREFIXES = """
PREFIX biz:  <https://ontology.sales-dwh.example.com/ontology/biz#>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
"""


@st.cache_resource(show_spinner=False)
def get_token() -> tuple[str | None, str | None]:
    """Login to GraphDB and return (token, error_message)."""
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
        if r.status_code == 200:
            token = r.headers.get("Authorization", "")
            if not token:
                body  = r.json() if r.text else {}
                token = body.get("token", "")
                if token and not token.startswith("GDB "):
                    token = f"GDB {token}"
            return token, None
        return None, f"GraphDB login failed ({r.status_code})"
    except Exception as exc:
        return None, str(exc)


def query(sparql: str, token: str) -> list[dict]:
    """Run a SPARQL SELECT against GraphDB, return list of row dicts."""
    base = config.get("GRAPHDB_BASE_URL")
    repo = config.get("GRAPHDB_REPO")
    try:
        r = requests.get(
            f"{base}/repositories/{repo}",
            params={"query": SPARQL_PREFIXES + "\n" + sparql},
            headers={"Authorization": token,
                     "Accept": "application/sparql-results+json"},
            timeout=30,
        )
        if r.status_code != 200:
            return []
        bindings = r.json().get("results", {}).get("bindings", [])
        rows = []
        for b in bindings:
            row = {}
            for k, v in b.items():
                val = v.get("value", "")
                dt  = v.get("datatype", "")
                if "integer" in dt or "long" in dt:
                    try: val = int(val)
                    except: pass
                elif "boolean" in dt:
                    val = val.lower() == "true"
                row[k] = val
            rows.append(row)
        return rows
    except Exception:
        return []
