# reload/reload_utils.py

import logging
import shutil
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)


def truncate_table(conn, table_name: str):
    """
    Safely truncate a table.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Full table name (e.g., 'analytical.dim_customer')
    """
    try:
        with conn.cursor() as cur:
            logger.info(f"Truncating {table_name}...")
            cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(
                sql.Identifier(*table_name.split('.'))
            ))
            conn.commit()
            logger.info(f"Successfully truncated {table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to truncate {table_name}: {e}")
        raise


def truncate_staging_table(conn, table_name: str):
    """
    Truncate staging table without cascading.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Full staging table name
    """
    try:
        with conn.cursor() as cur:
            logger.info(f"Truncating staging {table_name}...")
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(
                sql.Identifier(*table_name.split('.'))
            ))
            conn.commit()
            logger.info(f"Successfully truncated staging {table_name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to truncate staging {table_name}: {e}")
        raise


def reset_checkpoint(checkpoint_path: str):
    """
    Delete a Spark checkpoint directory to force replay from the last committed offset.
    
    Args:
        checkpoint_path: Local or S3 path to the checkpoint directory
    """
    try:
        if os.path.exists(checkpoint_path):
            logger.info(f"Deleting checkpoint directory: {checkpoint_path}")
            shutil.rmtree(checkpoint_path)
            logger.info(f"Successfully deleted checkpoint")
        else:
            logger.warning(f"Checkpoint path does not exist: {checkpoint_path}")
    except Exception as e:
        logger.error(f"Failed to reset checkpoint {checkpoint_path}: {e}")
        raise


def get_checkpoint_info(checkpoint_path: str) -> dict:
    """
    Read metadata from a Spark checkpoint to understand its state.
    
    Returns:
        Dict with checkpoint metadata or empty dict if not found
    """
    try:
        metadata_file = os.path.join(checkpoint_path, "metadata")
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                logger.info(f"Checkpoint metadata: {f.read()}")
                return {"exists": True, "path": checkpoint_path}
        else:
            return {"exists": False, "path": checkpoint_path}
    except Exception as e:
        logger.error(f"Failed to read checkpoint metadata: {e}")
        return {"error": str(e)}


def count_rows(conn, table_name: str) -> int:
    """
    Get the current row count of a table.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Full table name
        
    Returns:
        Row count as integer
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(*table_name.split('.'))
                )
            )
            count = cur.fetchone()[0]
            logger.info(f"{table_name} row count: {count}")
            return count
    except Exception as e:
        logger.error(f"Failed to count rows in {table_name}: {e}")
        raise


class ReloadConfig:
    """Helper class to load reload configuration"""
    
    def __init__(self, config_dict: dict):
        self.jdbc_url = config_dict.get("jdbc_url")
        self.jdbc_props = config_dict.get("jdbc_props", {})
        self.checkpoint_base = config_dict.get("checkpoint_base")
        self.pg_conn = config_dict.get("pg_conn")
    
    def get_checkpoint_path(self, consumer_name: str) -> str:
        """Get checkpoint path for a specific consumer"""
        return os.path.join(self.checkpoint_base, consumer_name)
    
    def connect(self):
        """Establish PostgreSQL connection if not already connected"""
        if not self.pg_conn or self.pg_conn.closed:
            from utils.db import get_db_connection
            self.pg_conn = get_db_connection(self.jdbc_url, self.jdbc_props)
        return self.pg_conn
    
    def close(self):
        """Close PostgreSQL connection"""
        if self.pg_conn and not self.pg_conn.closed:
            self.pg_conn.close()
