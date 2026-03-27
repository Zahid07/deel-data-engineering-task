# utils/db.py

import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)


def get_connection(pg_conn_config: dict):
    """
    Opens and returns a raw psycopg2 connection.
    Called inside foreachBatch — each micro-batch gets
    its own connection, uses it, then closes it.
    
    Why not a persistent connection?
    foreachBatch runs on Spark executors which may be
    different processes/machines each time — we can't
    hold a connection open across batches.
    """
    return psycopg2.connect(
        host=pg_conn_config["host"],
        port=pg_conn_config["port"],
        dbname=pg_conn_config["dbname"],
        user=pg_conn_config["user"],
        password=pg_conn_config["password"],
        connect_timeout=60,
        options="-c statement_timeout=300000" 
    )


def run_merge(pg_conn_config: dict, sql: str):
    """
    Executes a single MERGE/INSERT...ON CONFLICT SQL statement.
    Used by all 4 consumers after writing to staging.

    Commits on success, rolls back on failure.
    Raises the exception so the caller (foreachBatch) can log it.
    """
    conn = None
    try:
        conn = get_connection(pg_conn_config)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("SQL executed successfully.")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"SQL failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def truncate_staging(pg_conn_config: dict, staging_table: str):
    """
    Clears the staging table after a successful MERGE.
    Keeps staging clean between micro-batches.
    
    Called at the end of each foreachBatch write, after
    the MERGE has committed successfully.
    """
    conn = None
    try:
        conn = get_connection(pg_conn_config)
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {staging_table};")
        conn.commit()
        logger.info(f"Staging table {staging_table} truncated.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Truncate failed for {staging_table}: {e}")
        raise

    finally:
        if conn:
            conn.close()


def table_exists(pg_conn_config: dict, schema: str, table: str) -> bool:
    """
    Sanity check — verifies a table exists before we try to write.
    Useful for startup validation in main.py so we fail fast
    with a clear message rather than a cryptic JDBC error mid-stream.
    """
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
            AND   table_name   = %s
        );
    """
    conn = None
    try:
        conn = get_connection(pg_conn_config)
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return cur.fetchone()[0]
    finally:
        if conn:
            conn.close()


def validate_analytical_schema(pg_conn_config: dict):
    """
    Called once at startup from main.py.
    Checks all expected tables exist before starting any stream.
    Fails fast with a clear error if DDL hasn't been run yet.
    """
    expected_tables = [
        ("analytical", "dim_customer"),
        ("analytical", "dim_product"),
        ("analytical", "fact_orders"),
        ("analytical", "fact_order_items"),
        ("analytical", "dim_customer_staging"),
        ("analytical", "dim_product_staging"),
        ("analytical", "fact_orders_staging"),
        ("analytical", "fact_order_items_staging"),
    ]

    missing = []
    for schema, table in expected_tables:
        if not table_exists(pg_conn_config, schema, table):
            missing.append(f"{schema}.{table}")

    if missing:
        raise RuntimeError(
            f"Analytical schema validation failed. "
            f"Missing tables: {missing}. "
            f"Did you run the DDL script?"
        )

    logger.info("Analytical schema validation passed. All tables present.")