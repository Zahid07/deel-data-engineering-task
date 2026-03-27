#!/usr/bin/env python

"""
full_warehouse_reset.py

Perform a complete reset of the analytical warehouse:
- Truncate all analytical tables
- Truncate all staging tables
- Reset all Kafka checkpoints
- Force full replay from earliest message

⚠️  USE WITH EXTREME CAUTION - This is destructive!

Usage:
    python full_warehouse_reset.py --confirm
"""

import argparse
import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import config
from utils.db import get_db_connection
from reload_utils import truncate_table, truncate_staging_table, reset_checkpoint, count_rows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# All tables to reset
MAIN_TABLES = [
    "analytical.dim_customer",
    "analytical.dim_product",
    "analytical.fact_orders",
    "analytical.fact_order_items",
    "analytical.fact_orders_history",
]

STAGING_TABLES = [
    "analytical.dim_customer_staging",
    "analytical.dim_product_staging",
    "analytical.fact_orders_staging",
    "analytical.fact_order_items_staging",
]

CHECKPOINT_CONSUMERS = ["customers", "products", "orders", "order_items"]


def main():
    parser = argparse.ArgumentParser(
        description="⚠️  FULL WAREHOUSE RESET - Complete reset of all analytical data"
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm the reset (REQUIRED - safety measure)"
    )
    parser.add_argument(
        "--show-counts",
        action="store_true",
        help="Show row counts before and after"
    )
    
    args = parser.parse_args()
    
    if not args.confirm:
        print("\n" + "="*70)
        print("⚠️  FULL WAREHOUSE RESET")
        print("="*70)
        print("\nThis operation will:")
        print("  1. TRUNCATE all analytical tables")
        print("  2. TRUNCATE all staging tables")
        print("  3. DELETE all Kafka checkpoints")
        print("  4. Force streams to replay from earliest message")
        print("\nThis is PERMANENT and CANNOT be undone!")
        print("="*70)
        
        response = input("\nTo confirm, type 'RESET WAREHOUSE': ")
        if response.strip() != "RESET WAREHOUSE":
            logger.info("Reset cancelled.")
            sys.exit(0)
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_db_connection(config["jdbc_url"], config["jdbc_props"])
        
        # Show counts before
        if args.show_counts:
            logger.info("\n=== ROW COUNTS BEFORE RESET ===")
            for table in MAIN_TABLES:
                try:
                    count_rows(conn, table)
                except Exception as e:
                    logger.warning(f"Could not count {table}: {e}")
        
        # Truncate main tables
        logger.info("\n=== TRUNCATING MAIN TABLES ===")
        for table in MAIN_TABLES:
            try:
                truncate_table(conn, table)
            except Exception as e:
                logger.warning(f"Could not truncate {table}: {e}")
        
        # Truncate staging tables
        logger.info("\n=== TRUNCATING STAGING TABLES ===")
        for table in STAGING_TABLES:
            try:
                truncate_staging_table(conn, table)
            except Exception as e:
                logger.warning(f"Could not truncate staging {table}: {e}")
        
        # Show counts after
        if args.show_counts:
            logger.info("\n=== ROW COUNTS AFTER TRUNCATION ===")
            for table in MAIN_TABLES:
                try:
                    count_rows(conn, table)
                except Exception as e:
                    logger.warning(f"Could not count {table}: {e}")
        
        # Reset checkpoints
        logger.info("\n=== RESETTING KAFKA CHECKPOINTS ===")
        checkpoint_base = config.get("checkpoint_base")
        if checkpoint_base:
            for consumer in CHECKPOINT_CONSUMERS:
                checkpoint_path = os.path.join(checkpoint_base, consumer)
                try:
                    reset_checkpoint(checkpoint_path)
                    logger.info(f"✓ Reset checkpoint for {consumer}")
                except Exception as e:
                    logger.warning(f"Could not reset checkpoint for {consumer}: {e}")
        
        logger.info("\n" + "="*70)
        logger.info("✓ WAREHOUSE RESET COMPLETE")
        logger.info("="*70)
        logger.info("\nNext steps:")
        logger.info("  1. Restart all Spark streaming jobs")
        logger.info("  2. Monitor logs for replay progress")
        logger.info("  3. This may take a while depending on message volume")
        logger.info("="*70)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"\n✗ Warehouse reset failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
