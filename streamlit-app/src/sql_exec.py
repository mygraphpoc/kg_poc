"""
src/sql_exec.py — SQL execution on Databricks and Snowflake.

Databricks: uses databricks-sql-connector over HTTPS to SQL Warehouse.
            This works from Streamlit Cloud (outside Databricks).
            spark.sql() only works inside Databricks notebooks.

Snowflake:  uses snowflake-connector-python directly.

All execute_* functions return (text_summary, rows_as_dicts, col_names).
"""

from src import config


def _dbx_conn():
    from databricks import sql as dbsql
    host = config.get("DATABRICKS_HOST", "")
    if host.startswith("http"):
        host = host.split("//", 1)[-1]
    return dbsql.connect(
        server_hostname = host,
        http_path       = config.get("SQL_WAREHOUSE_HTTP"),
        access_token    = config.get("DATABRICKS_TOKEN"),
    )


def execute_databricks(sql: str) -> tuple:
    """Execute SQL on Databricks via SQL Warehouse (works from Streamlit Cloud)."""
    try:
        with _dbx_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                raw  = cur.fetchmany(200)
                cols = [d[0] for d in (cur.description or [])]
        if not raw:
            return "Query returned no results.", [], cols
        rows = [dict(zip(cols, r)) for r in raw]
        return _rows_to_text(cols, rows[:20]), rows, cols
    except Exception as e:
        return f"Databricks error: {e}", [], []


def databricks_columns(table_name: str, layer: str) -> list:
    """Fetch column names from Databricks INFORMATION_SCHEMA."""
    catalog = config.get("DATABRICKS_CATALOG", "kg_vs_poc")
    sql = (f"SELECT column_name, data_type "
           f"FROM {catalog}.information_schema.columns "
           f"WHERE LOWER(table_schema)='{layer.lower()}' "
           f"AND LOWER(table_name)='{table_name.lower()}' "
           f"ORDER BY ordinal_position")
    try:
        with _dbx_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [(r[0], r[1]) for r in (cur.fetchall() or [])]
    except Exception:
        return []


def databricks_distinct(fqn: str, col: str, limit: int = 12) -> list:
    try:
        with _dbx_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT {col} FROM {fqn} "
                            f"WHERE {col} IS NOT NULL LIMIT {limit}")
                return [str(r[0]) for r in (cur.fetchall() or [])]
    except Exception:
        return []


def _sf_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account  = config.get("SF_ACCOUNT"),
        user     = config.get("SF_USER"),
        password = config.get("SF_PASSWORD"),
        database = config.get("SF_DATABASE", "KG_VS_POC"),
        warehouse= config.get("SF_WAREHOUSE", "COMPUTE_WH"),
    )


def execute_snowflake(sql: str) -> tuple:
    """Execute SQL on Snowflake via connector."""
    if not config.get("SF_ACCOUNT"):
        return "Snowflake credentials not configured.", [], []
    try:
        conn = _sf_conn()
        cur  = conn.cursor()
        cur.execute(sql)
        raw  = cur.fetchmany(200)
        cols = [d[0] for d in (cur.description or [])]
        cur.close(); conn.close()
        if not raw:
            return "Query returned no results.", [], cols
        rows = [dict(zip(cols, r)) for r in raw]
        return _rows_to_text(cols, rows[:20]), rows, cols
    except Exception as e:
        return f"Snowflake error: {e}", [], []


def snowflake_columns(table_name: str) -> list:
    """Fetch column names from Snowflake INFORMATION_SCHEMA."""
    db = config.get("SF_DATABASE", "KG_VS_POC")
    if not config.get("SF_ACCOUNT"):
        return []
    try:
        conn = _sf_conn()
        cur  = conn.cursor()
        cur.execute(f"SELECT column_name, data_type "
                    f"FROM {db}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE UPPER(table_name)=UPPER('{table_name}') "
                    f"ORDER BY ordinal_position")
        result = [(r[0], r[1]) for r in (cur.fetchall() or [])]
        cur.close(); conn.close()
        return result
    except Exception:
        return []


def _rows_to_text(cols: list, rows: list) -> str:
    if not rows: return "No results."
    w   = max(18, max(len(c) for c in cols))
    hdr = " | ".join(f"{c:<{w}}" for c in cols)
    sep = "-" * len(hdr)
    return f"{hdr}\n{sep}\n" + "\n".join(
        " | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols)
        for r in rows)
