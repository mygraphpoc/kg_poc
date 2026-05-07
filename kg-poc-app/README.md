# KG-POC Sales DWH Assistant

Interactive Streamlit app with two tabs:
- **💬 Ask the DWH** — Graph-RAG Q&A agent (GraphDB + Vector Search + Databricks + Snowflake + Claude)
- **🔷 Data Lineage** — Interactive knowledge graph showing all tables and lineage relationships

---

## Deploy to Streamlit Cloud in 5 steps

### 1. Fork this repository

Click **Fork** in the top-right corner of this GitHub page.  
Your fork URL will be: `https://github.com/<your-username>/kg-poc-app`

### 2. Sign in to Streamlit Cloud

Go to **[share.streamlit.io](https://share.streamlit.io)** and sign in with your GitHub account.

### 3. Create a new app

1. Click **New app**
2. Select **From existing repo**
3. Choose your fork: `<your-username>/kg-poc-app`
4. Set **Main file path** to: `app.py`
5. Click **Deploy!**

Streamlit Cloud will install dependencies from `requirements.txt` automatically.

### 4. (Optional) Add secrets for permanent credentials

If you want credentials to persist across browser sessions, add them as Streamlit Secrets:

1. In Streamlit Cloud, open your app's **Settings → Secrets**
2. Paste and fill in:

```toml
GRAPHDB_BASE_URL   = "https://z36c3a30fdf694bd5952.sandbox.graphwise.ai"
GRAPHDB_USER       = "your@email.com"
GRAPHDB_PASSWORD   = "your-graphdb-password"
GRAPHDB_REPO       = "KG_POC_DBX_SF"

DATABRICKS_HOST    = "your-workspace.azuredatabricks.net"
DATABRICKS_TOKEN   = "dapiXXXXXXXXXXXXX"
SQL_WAREHOUSE_HTTP = "/sql/1.0/warehouses/your-warehouse-id"
VS_ENDPOINT_NAME   = "kg-poc-dbx-sf-vs"
INDEX_NAME         = "KG_POC_metadata.vector_registry.embeddings_index"

SF_ACCOUNT         = "ISDZMOQ-GS51186"
SF_USER            = "your_snowflake_user"
SF_PASSWORD        = "your_snowflake_password"
SF_DATABASE        = "KG_POC"
SF_WAREHOUSE       = "COMPUTE_WH"

ANTHROPIC_API_KEY  = "sk-ant-..."
```

3. Click **Save** and the app will restart.

### 5. Use the app

Open your deployed app URL. If you skipped step 4, fill in the credentials via the **sidebar setup form** and click **Save & Connect**.

---

## Run locally

```bash
# Clone
git clone https://github.com/<your-username>/kg-poc-app.git
cd kg-poc-app

# Install
pip install -r requirements.txt

# Configure (copy and fill in)
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# edit .streamlit/secrets.toml with your values

# Run
streamlit run app.py
```

---

## Project structure

```
kg-poc-app/
├── app.py                   ← Streamlit entry point (two tabs)
├── requirements.txt
├── README.md
├── .streamlit/
│   └── config.toml          ← Dark theme (no secrets)
└── src/
    ├── config.py            ← Credential loading: Secrets → env → session
    ├── graphdb.py           ← GraphDB login + SPARQL helpers
    ├── sql_exec.py          ← Databricks SQL Warehouse + Snowflake execution
    ├── agent.py             ← Graph-RAG pipeline
    ├── lineage.py           ← Lineage data loader + pyvis graph builder
    └── ui/
        ├── setup.py         ← Sidebar credential wizard
        ├── chat.py          ← Tab 1: Ask the DWH
        └── lineage_tab.py   ← Tab 2: Data Lineage
```

## Credential priority

| Source | How |
|---|---|
| Streamlit Cloud Secrets | Set via App Settings → Secrets (persists forever) |
| Environment variables | `export KEY=value` before running locally |
| Session state (UI form) | Filled in the sidebar; lives for the browser session only |

---

## Architecture

```
User question
      │
      ▼
GraphDB structural check (16 SPARQL patterns, <0.5s)
      │ if matched → natural-language summary via Claude
      │ if not matched ↓
      ▼
Databricks Vector Search (semantic retrieval of tables/KPIs/columns)
      + dimension-match augment (Gold table name-token IDF scoring)
      │
      ▼
Graph context assembly
  · Primary table identified (Gold-first, TF-IDF column-overlap rerank)
  · Real columns from Databricks INFORMATION_SCHEMA or Snowflake
  · Categorical value samples for relevant string columns
      │
      ▼
SQL generation (Claude claude-3-5-sonnet-20241022, 12 rules)
      │
      ▼
SQL execution (platform detected from SQL → GraphDB lookup)
  Databricks SQL Warehouse  or  Snowflake connector
      │
      ▼
Natural-language answer (Claude, 3-5 sentences, insight-focused)
```
