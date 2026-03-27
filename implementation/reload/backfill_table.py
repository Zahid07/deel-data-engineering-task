#!/usr/bin/env python

"""
backfill_table.py

Backfill specific tables from source data without full replay.

Useful for:
- Reloading data for a specific date range
- Recovering from data quality issues
- Syncing after source data corrections

Usage:
    python backfill_table.py --table dim_customer --from-date 2024-01-01
    python backfill_table.py --table fact_orders --from-date 2024-01-01 --to-date 2024-12-31
    python backfill_table.py --table dim_product --from-date 2024-06-01 --dry-run
"""

import argparse
import logging
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config
from utils.db import get_db_connection, run_merge
from reload_utils import truncate_staging_table, count_rows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Table mapping: user alias -> analytical table
TABLE_MAPPING = {
    "dim_customer": "analytical.dim_customer",
    "customers": "analytical.dim_customer",
    "dim_product": "analytical.dim_product",
    "products": "analytical.dim_product",
    "fact_orders": "analytical.fact_orders",
    "orders": "analytical.fact_orders",
    "fact_order_items": "analytical.fact_order_items",
    "order_items": "analytical.fact_order_items",
}

# Source table mapping
SOURCE_TABLE_MAPPING = {
    "analytical.dim_customer": "public.customers",
    "analytical.dim_product": "public.products",
    "analytical.fact_orders": "public.orders",
    "analytical.fact_order_items": "public.order_items",
}

# Staging table mapping
STAGING_MAPPING = {
    "analytical.dim_customer": "analytical.dim_customer_staging",
    "analytical.dim_product": "analytical.dim_product_staging",
    "analytical.fact_orders": "analytical.fact_orders_staging",
    "analytical.fact_order_items": "analytical.fact_order_items_staging",
}

# Known columns per source table used to derive _additional_columns.
# _additional_columns stores any source fields not listed here.
KNOWN_SOURCE_COLUMNS = {
    "analytical.dim_customer": [
        "customer_id", "customer_name", "is_active", "customer_address",
        "updated_at", "updated_by", "created_at", "created_by",
    ],
    "analytical.dim_product": [
        "product_id", "product_name", "barcode", "unity_price",
        "is_active", "updated_at", "updated_by", "created_at", "created_by",
    ],
    "analytical.fact_orders": [
        "order_id", "order_date", "delivery_date", "customer_id",
        "status", "updated_at", "updated_by", "created_at", "created_by",
    ],
    "analytical.fact_order_items": [
        "order_item_id", "order_id", "product_id", "quantity",
        "updated_at", "updated_by", "created_at", "created_by",
    ],
}

# MERGE SQL templates
MERGE_SQL_TEMPLATES = {
    "analytical.dim_customer": """
        INSERT INTO analytical.dim_customer (
            customer_id, customer_name, is_active, customer_address,
            updated_at, created_at, _additional_columns, _kafka_offset
        )
        SELECT
            customer_id, COALESCE(customer_name, ''), COALESCE(is_active, true),
            customer_address, updated_at, created_at,
            _additional_columns, _kafka_offset
        FROM analytical.dim_customer_staging
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_name = COALESCE(EXCLUDED.customer_name, ''),
            is_active = COALESCE(EXCLUDED.is_active, true),
            customer_address = EXCLUDED.customer_address,
            updated_at = EXCLUDED.updated_at,
            _additional_columns = EXCLUDED._additional_columns,
            _kafka_offset = EXCLUDED._kafka_offset;
    """,
    "analytical.dim_product": """
        INSERT INTO analytical.dim_product (
            product_id, product_name, barcode, unity_price, is_active,
            updated_at, created_at, _additional_columns, _kafka_offset
        )
        SELECT
            product_id, COALESCE(product_name, ''), COALESCE(barcode, ''),
            COALESCE(unity_price, 0.00), COALESCE(is_active, true),
            updated_at, created_at, _additional_columns, _kafka_offset
        FROM analytical.dim_product_staging
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = COALESCE(EXCLUDED.product_name, ''),
            barcode = COALESCE(EXCLUDED.barcode, ''),
            unity_price = COALESCE(EXCLUDED.unity_price, 0.00),
            is_active = COALESCE(EXCLUDED.is_active, true),
            updated_at = EXCLUDED.updated_at,
            _additional_columns = EXCLUDED._additional_columns,
            _kafka_offset = EXCLUDED._kafka_offset;
    """,
    "analytical.fact_orders": """
        INSERT INTO analytical.fact_orders (
            order_id, customer_id, order_date, delivery_date, status,
            updated_at, created_at, _additional_columns, _kafka_offset
        )
        SELECT
            order_id, COALESCE(customer_id, 0),
            COALESCE(order_date, CURRENT_DATE),
            COALESCE(delivery_date, CURRENT_DATE),
            COALESCE(status, 'unknown'),
            updated_at, created_at, _additional_columns, _kafka_offset
        FROM analytical.fact_orders_staging
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = COALESCE(EXCLUDED.customer_id, 0),
            order_date = COALESCE(EXCLUDED.order_date, CURRENT_DATE),
            delivery_date = COALESCE(EXCLUDED.delivery_date, CURRENT_DATE),
            status = COALESCE(EXCLUDED.status, 'unknown'),
            updated_at = EXCLUDED.updated_at,
            _additional_columns = EXCLUDED._additional_columns,
            _kafka_offset = EXCLUDED._kafka_offset;
    """,
    "analytical.fact_order_items": """
        INSERT INTO analytical.fact_order_items (
            order_item_id, order_id, product_id, quantity, order_status,
            updated_at, created_at, _additional_columns, _kafka_offset
        )
        SELECT
            s.order_item_id, COALESCE(s.order_id, 0),
            COALESCE(s.product_id, 0), COALESCE(s.quantity, 0),
            COALESCE(o.status, 'unknown'),
            s.updated_at, s.created_at, s._additional_columns, s._kafka_offset
        FROM analytical.fact_order_items_staging s
        LEFT JOIN analytical.fact_orders o ON o.order_id = s.order_id
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id = COALESCE(EXCLUDED.order_id, 0),
            product_id = COALESCE(EXCLUDED.product_id, 0),
            quantity = COALESCE(EXCLUDED.quantity, 0),
            order_status = COALESCE(EXCLUDED.order_status, 'unknown'),
            updated_at = EXCLUDED.updated_at,
            _additional_columns = EXCLUDED._additional_columns,
            _kafka_offset = EXCLUDED._kafka_offset;
    """,
}


def validate_date(date_str: str) -> datetime:
    """Validate date string format (YYYY-MM-DD)"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def get_backfill_query(table_name: str, from_date: datetime, to_date: datetime) -> str:
    """
    Generate the SQL query to extract source data for backfill.
    
    Args:
        table_name: Analytical table name (e.g., 'analytical.dim_customer')
        from_date: Start date (inclusive)
        to_date: End date (inclusive)
    
    Returns:
        SQL query string
    """
    source_table = SOURCE_TABLE_MAPPING.get(table_name)
    if not source_table:
        raise ValueError(f"No source table mapping for {table_name}")
    
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")
    
    known_cols = KNOWN_SOURCE_COLUMNS.get(table_name)
    if not known_cols:
        raise ValueError(f"No known source columns configured for {table_name}")

    additional_expr = "to_jsonb(src)"
    for col_name in known_cols:
        additional_expr += f" - '{col_name}'"
    additional_expr = f"NULLIF(({additional_expr})::text, '{{}}')"

    # Extract columns specific to each table
    if table_name == "analytical.dim_customer":
        return f"""
            SELECT
                src.customer_id,
                src.customer_name,
                src.is_active,
                src.customer_address,
                src.updated_at,
                src.created_at,
                {additional_expr} as _additional_columns,
                0::BIGINT as _kafka_offset
            FROM {source_table} src
            WHERE src.created_at::DATE >= '{from_str}'
              AND src.created_at::DATE <= '{to_str}'
        """
    
    elif table_name == "analytical.dim_product":
        return f"""
            SELECT
                src.product_id,
                src.product_name,
                src.barcode,
                src.unity_price,
                src.is_active,
                src.updated_at,
                src.created_at,
                {additional_expr} as _additional_columns,
                0::BIGINT as _kafka_offset
            FROM {source_table} src
            WHERE src.created_at::DATE >= '{from_str}'
              AND src.created_at::DATE <= '{to_str}'
        """
    
    elif table_name == "analytical.fact_orders":
        return f"""
            SELECT
                src.order_id,
                src.order_date,
                src.delivery_date,
                src.customer_id,
                src.status,
                src.updated_at,
                src.created_at,
                {additional_expr} as _additional_columns,
                0::BIGINT as _kafka_offset
            FROM {source_table} src
            WHERE src.created_at::DATE >= '{from_str}'
              AND src.created_at::DATE <= '{to_str}'
        """
    
    elif table_name == "analytical.fact_order_items":
        return f"""
            SELECT
                src.order_item_id,
                src.order_id,
                src.product_id,
                src.quantity,
                src.updated_at,
                src.created_at,
                {additional_expr} as _additional_columns,
                0::BIGINT as _kafka_offset
            FROM {source_table} src
            WHERE src.created_at::DATE >= '{from_str}'
              AND src.created_at::DATE <= '{to_str}'
        """
    
    else:
        raise ValueError(f"No backfill query defined for {table_name}")


def backfill_table(conn, table_name: str, staging_table: str, 
                   from_date: datetime, to_date: datetime) -> int:
    """
    Execute backfill for a specific table.
    
    Args:
        conn: PostgreSQL connection
        table_name: Analytical table name
        staging_table: Staging table name
        from_date: Start date
        to_date: End date
    
    Returns:
        Number of rows inserted into staging
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"BACKFILL: {table_name}")
    logger.info(f"{'='*60}")
    logger.info(f"Date Range: {from_date.date()} to {to_date.date()}")
    
    # Get the source data query
    extract_query = get_backfill_query(table_name, from_date, to_date)
    
    # Insert extracted data into staging
    try:
        with conn.cursor() as cur:
            insert_query = f"INSERT INTO {staging_table} {extract_query}"
            
            logger.info(f"\nExtracting source data from {SOURCE_TABLE_MAPPING.get(table_name)}...")
            cur.execute(insert_query)
            rows_inserted = cur.rowcount
            conn.commit()
            
            logger.info(f"✓ Inserted {rows_inserted} rows into staging table")
            
            return rows_inserted
    
    except Exception as e:
        conn.rollback()
        logger.error(f"✗ Failed to insert into staging: {e}")
        raise


def merge_backfilled_data(conn, table_name: str) -> None:
    """
    Run MERGE SQL to sync staged data into main table.
    
    Args:
        conn: PostgreSQL connection
        table_name: Analytical table name
    """
    merge_sql = MERGE_SQL_TEMPLATES.get(table_name)
    if not merge_sql:
        raise ValueError(f"No MERGE SQL template for {table_name}")
    
    try:
        logger.info(f"\nRunning MERGE into {table_name}...")
        run_merge(conn, merge_sql)
        logger.info(f"✓ MERGE successful")
    
    except Exception as e:
        logger.error(f"✗ MERGE failed: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Backfill data for a specific table and date range"
    )
    parser.add_argument(
        "--table",
        required=True,
        help=f"Table to backfill. Options: {', '.join(set(TABLE_MAPPING.keys()))}"
    )
    parser.add_argument(
        "--from-date",
        required=True,
        help="Start date (YYYY-MM-DD) - backfill from this date onwards"
    )
    parser.add_argument(
        "--to-date",
        help="End date (YYYY-MM-DD) - if not specified, uses today"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt"
    )
    
    args = parser.parse_args()
    
    # Validate and resolve table name
    table_key = args.table.lower()
    table_name = TABLE_MAPPING.get(table_key)
    if not table_name:
        logger.error(f"Unknown table: {args.table}")
        logger.error(f"Valid options: {', '.join(set(TABLE_MAPPING.keys()))}")
        sys.exit(1)
    
    # Validate dates
    try:
        from_date = validate_date(args.from_date)
        to_date = validate_date(args.to_date) if args.to_date else datetime.now()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    
    staging_table = STAGING_MAPPING.get(table_name)
    source_table = SOURCE_TABLE_MAPPING.get(table_name)
    
    # Confirmation
    if not args.dry_run and not args.confirm:
        print(f"\n{'='*60}")
        print(f"BACKFILL CONFIGURATION")
        print(f"{'='*60}")
        print(f"Analytical Table: {table_name}")
        print(f"Source Table:     {source_table}")
        print(f"Staging Table:    {staging_table}")
        print(f"Date Range:       {from_date.date()} to {to_date.date()}")
        print(f"{'='*60}")
        
        response = input("\nProceed with backfill? Type 'YES' to confirm: ")
        if response.strip() != "YES":
            logger.info("Backfill cancelled.")
            sys.exit(0)
    
    try:
        conn = get_db_connection(config["jdbc_url"], config["jdbc_props"])
        
        if args.dry_run:
            logger.info(f"\n{'='*60}")
            logger.info(f"DRY RUN - NO CHANGES WILL BE MADE")
            logger.info(f"{'='*60}")
            logger.info(f"\nAnalytical Table: {table_name}")
            logger.info(f"Source Table:     {source_table}")
            logger.info(f"Staging Table:    {staging_table}")
            logger.info(f"Date Range:       {from_date.date()} to {to_date.date()}")
            
            # Show what the extraction query would be
            try:
                extract_query = get_backfill_query(table_name, from_date, to_date)
                logger.info(f"\n--- EXTRACTION QUERY ---")
                logger.info(extract_query)
                logger.info(f"\n--- MERGE QUERY ---")
                logger.info(MERGE_SQL_TEMPLATES.get(table_name))
            except Exception as e:
                logger.warning(f"Could not generate query: {e}")
            
            logger.info(f"\n{'='*60}")
            logger.info("To execute, remove --dry-run flag and add --confirm")
            logger.info(f"{'='*60}")
        else:
            # Show counts before
            logger.info(f"\n{'='*60}")
            logger.info("BEFORE BACKFILL")
            logger.info(f"{'='*60}")
            count_before = count_rows(conn, table_name)
            count_staging_before = count_rows(conn, staging_table)
            
            # Clear staging before reload
            try:
                truncate_staging_table(conn, staging_table)
                logger.info(f"✓ Cleared staging table")
            except Exception as e:
                logger.error(f"✗ Could not clear staging: {e}")
                raise
            
            # Execute backfill
            try:
                rows_inserted = backfill_table(conn, table_name, staging_table, from_date, to_date)
                
                if rows_inserted == 0:
                    logger.warning(f"\n⚠️  No rows found in source for date range")
                    logger.info("   Backfill completed but 0 rows were extracted")
                else:
                    # Run MERGE to sync to main table
                    merge_backfilled_data(conn, table_name)
                    
                    # Show counts after
                    logger.info(f"\n{'='*60}")
                    logger.info("AFTER BACKFILL")
                    logger.info(f"{'='*60}")
                    count_after = count_rows(conn, table_name)
                    
                    logger.info(f"\n{'='*60}")
                    logger.info("✓ BACKFILL COMPLETE")
                    logger.info(f"{'='*60}")
                    logger.info(f"Rows Before:    {count_before}")
                    logger.info(f"Rows Extracted: {rows_inserted}")
                    logger.info(f"Rows After:     {count_after}")
                    logger.info(f"Net Change:     {count_after - count_before:+d}")
                    logger.info(f"{'='*60}")
                    
            except Exception as e:
                logger.error(f"\n✗ Backfill failed: {e}")
                raise
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
