"""
src/sql_exec.py — SQL execution on Databricks and Snowflake.
All execute_* functions return (text_summary, rows_as_dicts, col_names).
"""

import streamlit as st
from src import config


def _sf_opts():
    return {
        "sfURL":      config.get("SF_ACCOUNT") + ".snowflakecomputing.com",
        "sfUser":     config.get("SF_USER"),
        "sfPassword": config.get("SF_PASSWORD"),
        "sfDatabase": config.get("SF_DATABASE", "KG_VS_POC"),
        "sfWarehouse": config.get("SF_WAREHOUSE", "COMPUTE_WH"),
        "sfRole":     "ACCOUNTADMIN",
        "sfSchema":   "GOLD",
    }


@st.cache_resource(show_spinner=False)
def _get_spark():
    """Get active Spark session — only works when running inside Databricks."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:
        return None


def execute_databricks(sql: str) -> tuple:
    """Execute SQL using Spark session (serverless compatible)."""
    spark = _get_spark()
    if spark is None:
        return "No active Spark session.", [], []
    try:
        df    = spark.sql(sql)
        rows  = df.limit(200).collect()
        cols  = df.columns
        if not rows:
            return "Query returned no results.", [], []
        dicts = [row.asDict() for row in rows]
        return _rows_to_text(cols, dicts[:20]), dicts, cols
    except Exception as e:
        return f"Databricks error: {e}", [], []


def databricks_columns(table_name: str, layer: str) -> list:
    """Fetch column names from Databricks INFORMATION_SCHEMA via Spark."""
    spark = _get_spark()
    if spark is None:
        return []
    catalog = config.get("DATABRICKS_CATALOG", "kg_vs_poc")
    try:
        rows = spark.sql(
            f"SELECT column_name, data_type "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE LOWER(table_schema)='{layer.lower()}' "
            f"AND LOWER(table_name)='{table_name.lower()}' "
            f"ORDER BY ordinal_position"
        ).collect()
        return [(r["column_name"], r["data_type"]) for r in rows]
    except Exception:
        return []


def execute_snowflake(sql: str) -> tuple:
    """Execute SQL on Snowflake using Spark Snowflake connector."""
    spark = _get_spark()
    if spark is None:
        return "No active Spark session.", [], []
    opts = _sf_opts()
    if not opts["sfURL"].startswith(".") and not config.get("SF_ACCOUNT"):
        return "Snowflake credentials not configured.", [], []
    try:
        df   = (spark.read
                .format("snowflake")
                .options(**opts)
                .option("query", sql)
                .load())
        rows = df.limit(200).collect()
        cols = df.columns
        if not rows:
            return "Query returned no results.", [], []
        dicts = [row.asDict() for row in rows]
        return _rows_to_text(cols, dicts[:20]), dicts, cols
    except Exception as e:
        return f"Snowflake error: {e}", [], []


def snowflake_columns(table_name: str) -> list:
    """Fetch column names from Snowflake INFORMATION_SCHEMA via Spark."""
    spark = _get_spark()
    if spark is None:
        return []
    db = config.get("SF_DATABASE", "KG_VS_POC")
    opts = _sf_opts()
    try:
        df = (spark.read
              .format("snowflake")
              .options(**opts)
              .option("query",
                      f"SELECT column_name, data_type "
                      f"FROM {db}.INFORMATION_SCHEMA.COLUMNS "
                      f"WHERE UPPER(table_name)=UPPER('{table_name}') "
                      f"ORDER BY ordinal_position")
              .load())
        return [(r["COLUMN_NAME"], r["DATA_TYPE"]) for r in df.collect()]
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
