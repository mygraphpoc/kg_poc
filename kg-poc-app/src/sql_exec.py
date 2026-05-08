"""src/sql_exec.py — Execute SQL on Databricks (SQL Warehouse) and Snowflake.

All execute_* functions return a 3-tuple:
  (text_summary: str, rows: list[dict], col_names: list[str])
so the caller can render either a prose string or a dataframe.
"""

from src import config


# ─── Databricks ───────────────────────────────────────────────────────────────

def _dbx_conn():
    from databricks import sql as dbsql
    host = config.get("DATABRICKS_HOST")
    if host.startswith("http"):
        host = host.split("//", 1)[-1]
    return dbsql.connect(
        server_hostname=host,
        http_path=config.get("SQL_WAREHOUSE_HTTP"),
        access_token=config.get("DATABRICKS_TOKEN"),
    )


def execute_databricks(sql: str) -> tuple:
    """Returns (text_summary, rows_as_dicts, col_names)."""
    try:
        with _dbx_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                raw_rows = cur.fetchmany(200)
                cols = [d[0] for d in (cur.description or [])]
        if not raw_rows:
            return "Query returned no results.", [], cols
        rows = [dict(zip(cols, r)) for r in raw_rows]
        # text preview (first 20 rows)
        preview = _rows_to_text(cols, rows[:20])
        return preview, rows, cols
    except Exception as e:
        return f"Databricks error: {e}", [], []


def databricks_columns(table_name: str, layer: str) -> list:
    sql = (f"SELECT column_name, data_type "
           f"FROM KG_POC.information_schema.columns "
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


def databricks_distinct(fqn: str, col_name: str, limit: int = 12) -> list:
    try:
        with _dbx_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT {col_name} FROM {fqn} "
                            f"WHERE {col_name} IS NOT NULL LIMIT {limit}")
                return [str(r[0]) for r in (cur.fetchall() or [])]
    except Exception:
        return []


# ─── Snowflake ────────────────────────────────────────────────────────────────

def _sf_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account  =config.get("SF_ACCOUNT"),
        user     =config.get("SF_USER"),
        password =config.get("SF_PASSWORD"),
        database =config.get("SF_DATABASE"),
        warehouse=config.get("SF_WAREHOUSE"),
    )


def execute_snowflake(sql: str) -> tuple:
    """Returns (text_summary, rows_as_dicts, col_names)."""
    try:
        conn = _sf_conn()
        cur  = conn.cursor()
        cur.execute(sql)
        raw_rows = cur.fetchmany(200)
        cols = [d[0] for d in (cur.description or [])]
        cur.close(); conn.close()
        if not raw_rows:
            return "Query returned no results.", [], cols
        rows = [dict(zip(cols, r)) for r in raw_rows]
        preview = _rows_to_text(cols, rows[:20])
        return preview, rows, cols
    except Exception as e:
        return f"Snowflake error: {e}", [], []


def snowflake_columns(table_name: str) -> list:
    sf_db = config.get("SF_DATABASE")
    try:
        conn = _sf_conn()
        cur  = conn.cursor()
        cur.execute(f"SELECT column_name, data_type "
                    f"FROM {sf_db}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE UPPER(table_name)=UPPER('{table_name}') "
                    f"ORDER BY ordinal_position")
        result = [(r[0], r[1]) for r in (cur.fetchall() or [])]
        cur.close(); conn.close()
        return result
    except Exception:
        return []


def snowflake_distinct(fqn: str, col_name: str, limit: int = 12) -> list:
    try:
        conn = _sf_conn()
        cur  = conn.cursor()
        cur.execute(f"SELECT DISTINCT {col_name} FROM {fqn} "
                    f"WHERE {col_name} IS NOT NULL LIMIT {limit}")
        result = [str(r[0]) for r in (cur.fetchall() or [])]
        cur.close(); conn.close()
        return result
    except Exception:
        return []


# ─── Shared ───────────────────────────────────────────────────────────────────

def _rows_to_text(cols: list, rows: list) -> str:
    if not rows:
        return "No results."
    w = max(18, max(len(c) for c in cols))
    hdr = " | ".join(f"{c:<{w}}" for c in cols)
    sep = "-" * len(hdr)
    body = "\n".join(
        " | ".join(f"{str(r.get(c,'')):<{w}}" for c in cols)
        for r in rows
    )
    return f"{hdr}\n{sep}\n{body}"
