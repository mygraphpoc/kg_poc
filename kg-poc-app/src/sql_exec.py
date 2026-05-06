"""src/sql_exec.py — Execute SQL on Databricks (SQL Warehouse) and Snowflake."""

from src import config


# ─── Databricks ───────────────────────────────────────────────────────────────

def _dbx_conn():
    from databricks import sql as dbsql
    return dbsql.connect(
        server_hostname=config.get("DATABRICKS_HOST"),
        http_path=config.get("SQL_WAREHOUSE_HTTP"),
        access_token=config.get("DATABRICKS_TOKEN"),
    )


def execute_databricks(sql: str) -> str:
    try:
        with _dbx_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchmany(50)
                cols = [d[0] for d in (cur.description or [])]
                if not rows:
                    return "Query returned no results."
                h = " | ".join(cols)
                return h + "\n" + "-" * len(h) + "\n" + \
                       "\n".join(" | ".join(str(v) for v in r) for r in rows)
    except Exception as e:
        return f"Databricks error: {e}"


def databricks_columns(table_name: str, layer: str) -> list[tuple[str, str]]:
    """Fetch (column_name, data_type) from Databricks INFORMATION_SCHEMA."""
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


def databricks_distinct(fqn: str, col_name: str, limit: int = 12) -> list[str]:
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
        account  = config.get("SF_ACCOUNT"),
        user     = config.get("SF_USER"),
        password = config.get("SF_PASSWORD"),
        database = config.get("SF_DATABASE"),
        warehouse= config.get("SF_WAREHOUSE"),
    )


def execute_snowflake(sql: str) -> str:
    try:
        conn = _sf_conn()
        cur  = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(50)
        cols = [d[0] for d in (cur.description or [])]
        cur.close(); conn.close()
        if not rows:
            return "Query returned no results."
        h = " | ".join(cols)
        return h + "\n" + "-" * len(h) + "\n" + \
               "\n".join(" | ".join(str(v) for v in r) for r in rows)
    except Exception as e:
        return f"Snowflake error: {e}"


def snowflake_columns(table_name: str) -> list[tuple[str, str]]:
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


def snowflake_distinct(fqn: str, col_name: str, limit: int = 12) -> list[str]:
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
