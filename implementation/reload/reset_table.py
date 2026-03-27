#!/usr/bin/env python

"""
reset_table.py

Reset a specific table to an empty state and optionally reset its checkpoint.

Usage:
    python reset_table.py --table dim_customer
    python reset_table.py --table fact_orders --reset-checkpoint
    python reset_table.py --table dim_customer --show-count
"""

import argparse
import logging
import sys
import os

# Add parent directory to path to import config and utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config
from utils.db import get_db_connection
from reload_utils import truncate_table, truncate_staging_table, reset_checkpoint, count_rows, ReloadConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mapping of table aliases to actual table names
TABLE_MAPPING = {
    "customers": "analytical.dim_customer",
    "dim_customer": "analytical.dim_customer",
    "products": "analytical.dim_product",
    "dim_product": "analytical.dim_product",
    "orders": "analytical.fact_orders",
    "fact_orders": "analytical.fact_orders",
    "order_items": "analytical.fact_order_items",
    "fact_order_items": "analytical.fact_order_items",
    "orders_history": "analytical.fact_orders_history",
    "fact_orders_history": "analytical.fact_orders_history",
}

# Staging table mapping
STAGING_MAPPING = {
    "analytical.dim_customer": "analytical.dim_customer_staging",
    "analytical.dim_product": "analytical.dim_product_staging",
    "analytical.fact_orders": "analytical.fact_orders_staging",
    "analytical.fact_order_items": "analytical.fact_order_items_staging",
}

# Consumer checkpoint mapping
CHECKPOINT_MAPPING = {
    "analytical.dim_customer": "customers",
    "analytical.dim_product": "products",
    "analytical.fact_orders": "orders",
    "analytical.fact_order_items": "order_items",
}


def main():
    parser = argparse.ArgumentParser(
        description="Reset a specific table in the analytical warehouse"
    )
    parser.add_argument(
        "--table",
        required=True,
        help=f"Table to reset. Options: {', '.join(TABLE_MAPPING.keys())}"
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Also reset the Kafka checkpoint for this table's consumer"
    )
    parser.add_argument(
        "--show-count",
        action="store_true",
        help="Show row count before and after reset"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt (use with caution)"
    )
    
    args = parser.parse_args()
    
    # Resolve table name
    table_name = TABLE_MAPPING.get(args.table.lower())
    if not table_name:
        logger.error(f"Unknown table: {args.table}")
        logger.error(f"Valid options: {', '.join(TABLE_MAPPING.keys())}")
        sys.exit(1)
    
    # Get staging table name
    staging_table = STAGING_MAPPING.get(table_name)
    checkpoint_consumer = CHECKPOINT_MAPPING.get(table_name)
    
    # Confirmation
    if not args.confirm:
        print(f"\n⚠️  WARNING: This will TRUNCATE {table_name}")
        if staging_table:
            print(f"   and {staging_table}")
        if args.reset_checkpoint and checkpoint_consumer:
            checkpoint_path = os.path.join(config["checkpoint_base"], checkpoint_consumer)
            print(f"   and delete checkpoint at {checkpoint_path}")
        
        response = input("\nAre you sure? Type 'YES' to confirm: ")
        if response.strip() != "YES":
            logger.info("Reset cancelled.")
            sys.exit(0)
    
    try:
        # Connect to database
        conn = get_db_connection(config["jdbc_url"], config["jdbc_props"])
        reload_cfg = ReloadConfig(config)
        
        # Show counts before
        if args.show_count:
            logger.info("=== BEFORE RESET ===")
            count_rows(conn, table_name)
            if staging_table:
                count_rows(conn, staging_table)
        
        # Truncate main table
        truncate_table(conn, table_name)
        
        # Truncate staging table if it exists
        if staging_table:
            try:
                truncate_staging_table(conn, staging_table)
            except Exception as e:
                logger.warning(f"Could not truncate staging table (may not exist): {e}")
        
        # Reset checkpoint if requested
        if args.reset_checkpoint and checkpoint_consumer:
            checkpoint_path = os.path.join(config["checkpoint_base"], checkpoint_consumer)
            reset_checkpoint(checkpoint_path)
            logger.info(f"✓ Checkpoint reset for consumer: {checkpoint_consumer}")
        
        # Show counts after
        if args.show_count:
            logger.info("=== AFTER RESET ===")
            count_rows(conn, table_name)
            if staging_table:
                count_rows(conn, staging_table)
        
        logger.info(f"\n✓ Successfully reset {table_name}")
        
        if args.reset_checkpoint and checkpoint_consumer:
            logger.info(f"✓ Next stream restart will replay from earliest offset")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Reset failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
