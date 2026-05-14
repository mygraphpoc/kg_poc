"""
src/ui/ttyg_tab.py
──────────────────
Talk to Your Graph (TTYG) integration tab.

What this does:
  - Provides a chat interface that proxies questions directly to GraphDB's
    built-in Graph RAG agent (Talk to Your Graph)
  - TTYG handles: metadata questions, SPARQL generation, ontology traversal,
    concept lookup, lineage, KPI definitions — all from RDF triples
  - Complements the main SQL tab which handles actual data queries

GraphDB TTYG REST API endpoints used:
  POST /rest/ttyg/agents                    - list agents
  POST /rest/ttyg/chats                     - create a new chat
  POST /rest/ttyg/chats/{chatId}/messages   - send a message
  GET  /rest/ttyg/chats/{chatId}/messages   - get chat history

Prerequisites (one-time setup in GraphDB Workbench):
  1. Configure LLM: graphdb.llm.api + graphdb.llm.api-key + graphdb.llm.model
  2. Create a TTYG agent for repository kg_vs_poc_dbx_sf
     - Enable SPARQL query method (point at your ontology named graph)
     - Enable Full-text search (if FTS index is set up)
  3. Copy the agent ID into Streamlit secrets as TTYG_AGENT_ID
"""

import streamlit as st
import requests
import json
from src import config, graphdb

# ── TTYG API helpers ──────────────────────────────────────────────────────────

def _headers(token: str) -> dict:
    return {
        "Authorization": token,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def list_agents(token: str) -> list:
    """Return all TTYG agents for the configured repository."""
    base = config.get("GRAPHDB_BASE_URL")
    repo = config.get("GRAPHDB_REPO")
    try:
        r = requests.get(
            f"{base}/rest/ttyg/agents",
            params={"repositoryId": repo},
            headers=_headers(token),
            timeout=15,
        )
        if r.status_code == 200:
            return r.json() if isinstance(r.json(), list) else r.json().get("agents", [])
        return []
    except Exception:
        return []


def create_chat(agent_id: str, token: str) -> str | None:
    """Create a new TTYG chat session. Returns chatId."""
    base = config.get("GRAPHDB_BASE_URL")
    try:
        r = requests.post(
            f"{base}/rest/ttyg/chats",
            headers=_headers(token),
            json={"name": "KG-POC session", "agentId": agent_id},
            timeout=15,
        )
        if r.status_code in (200, 201):
            return r.json().get("id") or r.json().get("chatId")
        return None
    except Exception:
        return None


def send_message(chat_id: str, question: str, token: str) -> dict:
    """
    Send a message to TTYG and get the response.
    Returns {answer, sparql, method, error}
    """
    base = config.get("GRAPHDB_BASE_URL")
    try:
        r = requests.post(
            f"{base}/rest/ttyg/chats/{chat_id}/messages",
            headers=_headers(token),
            json={"message": question},
            timeout=60,
        )
        if r.status_code == 200:
            data = r.json()
            # Extract answer — field name varies by GraphDB version
            answer = (data.get("answer") or
                      data.get("message") or
                      data.get("content") or
                      data.get("response") or "")
            # Extract SPARQL query if TTYG used it
            sparql = ""
            tool_calls = data.get("toolCalls") or data.get("tool_calls") or []
            for tc in tool_calls:
                args = tc.get("arguments") or tc.get("input") or {}
                if isinstance(args, str):
                    try: args = json.loads(args)
                    except: pass
                q = args.get("query") or args.get("sparql") or ""
                if q: sparql = q; break
            method = data.get("queryMethod") or data.get("method") or "SPARQL"
            return {"answer": answer, "sparql": sparql,
                    "method": method, "error": ""}
        return {"answer": "", "sparql": "", "method": "",
                "error": f"TTYG error {r.status_code}: {r.text[:200]}"}
    except Exception as e:
        return {"answer": "", "sparql": "", "method": "",
                "error": f"Request failed: {str(e)}"}


# ── Render ─────────────────────────────────────────────────────────────────────

SAMPLE_QUESTIONS = [
    "What Gold tables are available in the knowledge graph?",
    "What KPIs are registered for the Customer domain?",
    "Explain the lineage of agg_customer_360",
    "Which tables contain PII columns?",
    "What are the synonyms for Gross Margin?",
    "What is the formula for Customer Lifetime Value?",
    "Which tables are on Snowflake?",
    "Show all SKOS concepts related to revenue",
    "What columns does the dim_supplier table have?",
    "Describe the relationship between fct_sales and agg_revenue_daily",
]

_ICON_COLOR = {
    "✅": "#3fb950", "❌": "#f85149", "💬": "#bc8cff",
    "🔍": "#58a6ff", "⚠️": "#f9a825",
}


def render() -> None:
    st.markdown("## 🧠 Talk to Your Graph")
    st.caption(
        "Ask metadata questions directly answered by GraphDB's built-in Graph RAG agent. "
        "TTYG reads your RDF knowledge graph — tables, KPIs, lineage, concepts, SKOS taxonomy."
    )

    # ── Config check ──────────────────────────────────────────────────────────
    token, gdb_err = graphdb.get_token()
    if gdb_err:
        st.error(f"GraphDB not connected: {gdb_err}")
        return

    agent_id = config.get("TTYG_AGENT_ID", "")

    # ── Agent selection ───────────────────────────────────────────────────────
    with st.expander("⚙️ Agent settings", expanded=not agent_id):
        st.caption(
            "Select a TTYG agent to use. Create agents in **GraphDB Workbench → Lab → Talk to Your Graph**."
        )
        agents = list_agents(token)
        if not agents:
            st.warning(
                "No TTYG agents found in GraphDB for this repository.\n\n"
                "**To set up:**\n"
                "1. Open GraphDB Workbench → Lab → Talk to Your Graph\n"
                "2. Click 'Create your first agent'\n"
                "3. Select repository: `kg_vs_poc_dbx_sf`\n"
                "4. Enable SPARQL query method\n"
                "5. Enable Full-text search (if FTS index is ready)\n"
                "6. Save — then add `TTYG_AGENT_ID` to Streamlit secrets"
            )
            return

        agent_options = {
            a.get("name", a.get("id", "Unknown")): a.get("id", "")
            for a in agents
        }
        selected_name = st.selectbox(
            "Agent", list(agent_options.keys()),
            index=0,
        )
        agent_id = agent_options[selected_name]
        st.caption(f"Agent ID: `{agent_id}`")
        st.caption(
            "💡 Add `TTYG_AGENT_ID = \"" + agent_id + "\"` "
            "to Streamlit secrets to skip this step."
        )

    if not agent_id:
        return

    # ── Session state ─────────────────────────────────────────────────────────
    if "ttyg_chat_id"  not in st.session_state: st.session_state["ttyg_chat_id"]  = None
    if "ttyg_messages" not in st.session_state: st.session_state["ttyg_messages"] = []
    if "ttyg_pending"  not in st.session_state: st.session_state["ttyg_pending"]  = None

    # ── Start / reset chat ────────────────────────────────────────────────────
    col1, col2 = st.columns([4, 1])
    with col2:
        if st.button("🔄 New chat", key="ttyg_new"):
            st.session_state["ttyg_chat_id"]  = None
            st.session_state["ttyg_messages"] = []
            st.rerun()

    if st.session_state["ttyg_chat_id"] is None:
        with st.spinner("Starting TTYG session…"):
            chat_id = create_chat(agent_id, token)
        if not chat_id:
            st.error(
                "Could not create TTYG chat session. "
                "Check that the agent exists and the LLM is configured in GraphDB."
            )
            return
        st.session_state["ttyg_chat_id"] = chat_id

    # ── Sample questions sidebar ──────────────────────────────────────────────
    with st.sidebar:
        st.markdown("---")
        st.markdown("**TTYG sample questions**")
        for q in SAMPLE_QUESTIONS:
            if st.button(q, key=f"ttyg_sq_{hash(q)}"):
                st.session_state["ttyg_pending"] = q

    # ── Display chat history ──────────────────────────────────────────────────
    for msg in st.session_state["ttyg_messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sparql"):
                with st.expander("🔍 SPARQL generated by TTYG", expanded=False):
                    st.code(msg["sparql"], language="sparql")
            if msg.get("method"):
                st.caption(f"Method: {msg['method']}")

    # ── Input ─────────────────────────────────────────────────────────────────
    question = None
    if st.session_state.get("ttyg_pending"):
        question = st.session_state.pop("ttyg_pending")
    user_input = st.chat_input(
        "Ask a metadata question… (tables, KPIs, lineage, concepts, SKOS)"
    )
    if user_input:
        question = user_input

    if not question:
        if not st.session_state["ttyg_messages"]:
            st.info(
                "👋 Ask a question about your knowledge graph metadata — "
                "tables, KPIs, lineage, SKOS concepts, PII columns.\n\n"
                "⚠️ **Note:** TTYG answers from RDF triples only. "
                "For actual sales data (revenue, churn numbers), "
                "use the **💬 Ask the DWH** tab."
            )
        return

    # Add user message
    st.session_state["ttyg_messages"].append(
        {"role": "user", "content": question}
    )
    with st.chat_message("user"):
        st.markdown(question)

    # Send to TTYG and show answer
    with st.chat_message("assistant"):
        with st.spinner("Asking the knowledge graph…"):
            result = send_message(
                st.session_state["ttyg_chat_id"], question, token
            )

        if result["error"]:
            st.error(result["error"])
            st.session_state["ttyg_messages"].append(
                {"role": "assistant", "content": f"⚠️ {result['error']}",
                 "sparql": "", "method": ""}
            )
            return

        answer = result["answer"] or "*(No response from TTYG)*"
        st.markdown(answer)

        if result["sparql"]:
            with st.expander("🔍 SPARQL generated by TTYG", expanded=False):
                st.code(result["sparql"], language="sparql")
            # Copy-to-app button — adds SPARQL to clipboard for adding to sparql_retriever.py
            st.caption(
                "💡 Copy this SPARQL query into `sparql_retriever.py` "
                "to add it as a structural pattern."
            )

        if result["method"]:
            st.caption(f"Method: {result['method']}")

    st.session_state["ttyg_messages"].append({
        "role":    "assistant",
        "content": answer,
        "sparql":  result["sparql"],
        "method":  result["method"],
    })
