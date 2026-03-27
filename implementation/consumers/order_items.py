# consumers/order_items.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType, TimestampType
)

from utils.debezium import parse_debezium_envelope
from utils.db import run_merge, truncate_staging

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Schema
# Mirrors the source order_items table exactly.
# -------------------------------------------------------------------

ORDER_ITEM_SCHEMA = StructType([
    StructField("order_item_id", LongType(),      False),
    StructField("order_id",      LongType(),      True),
    StructField("product_id",    LongType(),      True),
    StructField("quantity",      IntegerType(),   True),
    StructField("updated_at",    TimestampType(), True),
    StructField("updated_by",    LongType(),      True),
    StructField("created_at",    TimestampType(), True),
    StructField("created_by",    LongType(),      True)
])


STAGING_TABLE = "analytical.fact_order_items_staging"
TARGET_TABLE  = "analytical.fact_order_items"


# -------------------------------------------------------------------
# MERGE SQL
# Joins staging against fact_orders to denormalize order_status.
# This avoids a join at query time for Q3 (pending items by product).
# -------------------------------------------------------------------

MERGE_SQL = f"""
    INSERT INTO {TARGET_TABLE} (
        order_item_id,
        order_id,
        product_id,
        quantity,
        order_status,
        updated_at,
        created_at,
        _additional_columns,
        _kafka_offset
    )
    SELECT
        s.order_item_id,
        COALESCE(s.order_id, 0),
        COALESCE(s.product_id, 0),
        COALESCE(s.quantity, 0),
        COALESCE(o.status, 'unknown'),  -- denormalized from fact_orders
        s.updated_at,
        s.created_at,
        s._additional_columns,
        s._kafka_offset
    FROM {STAGING_TABLE} s
    LEFT JOIN analytical.fact_orders o ON o.order_id = s.order_id
    ON CONFLICT (order_item_id) DO UPDATE SET
        order_id     = COALESCE(EXCLUDED.order_id, 0),
        product_id   = COALESCE(EXCLUDED.product_id, 0),
        quantity     = COALESCE(EXCLUDED.quantity, 0),
        order_status = COALESCE(EXCLUDED.order_status, 'unknown'),
        updated_at   = EXCLUDED.updated_at,
        _additional_columns = EXCLUDED._additional_columns,
        _kafka_offset = EXCLUDED._kafka_offset;
"""


# -------------------------------------------------------------------
# STATUS SYNC SQL
# When an order status changes, fact_order_items rows for that
# order must also be updated to keep order_status in sync.
# Called after every batch to catch any status changes that came
# through the orders stream since the last micro-batch.
# -------------------------------------------------------------------

STATUS_SYNC_SQL = f"""
    UPDATE {TARGET_TABLE} oi
    SET order_status = o.status
    FROM analytical.fact_orders o
    WHERE oi.order_id = o.order_id
    AND   oi.order_status != o.status;
"""


# -------------------------------------------------------------------
# foreachBatch handler
# -------------------------------------------------------------------

def process_batch(batch_df: DataFrame, batch_id: int, config: dict):
    if batch_df.isEmpty():
        logger.info(f"[order_items] Batch {batch_id} is empty, skipping.")
        # Even on empty batch, sync status in case orders stream
        # updated statuses since last order_items batch
        try:
            run_merge(config["pg_conn"], STATUS_SYNC_SQL)
            logger.info(f"[order_items] Status sync completed on empty batch.")
        except Exception as e:
            logger.warning(f"[order_items] Status sync failed: {e}")
        return

    logger.info(f"[order_items] Processing batch {batch_id}.")

    # fact_df = batch_df.drop("op").coalesce(1)

    fact_df = (
        batch_df
        .drop("op")
        .dropDuplicates(["order_item_id"])  # final safety net after window dedup
        .coalesce(1)
    )

    # fact_df.show(truncate=False)
    # fact_df.printSchema()

    # Step 1: Write to staging
    try:
        logger.info(f"[order_items] Writing to staging table {STAGING_TABLE}...")
        (
            # fact_df.write
            # .jdbc(
            #     url=config["jdbc_url"],
            #     table=STAGING_TABLE,
            #     mode="overwrite",
            #     properties=config["jdbc_props"],
            # )

            fact_df.write.format("jdbc")
            .option("url", config["jdbc_url"])
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", STAGING_TABLE)
            .option("user", config["jdbc_props"]["user"])
            .option("password", config["jdbc_props"]["password"])
            .option("mode", "overwrite")
            .mode("overwrite")
            .save()
        )
        logger.info(f"[order_items] Staging write successful.")

    except Exception as e:
        logger.error(f"[order_items] Staging write failed: {e}")
        raise

    # Step 2: MERGE staging → fact_order_items
    # Joins against fact_orders to pull in current order_status
    try:
        logger.info(f"[order_items] Running MERGE into {TARGET_TABLE}...")
        run_merge(config["pg_conn"], MERGE_SQL)
        logger.info(f"[order_items] MERGE successful.")

    except Exception as e:
        logger.error(f"[order_items] MERGE failed: {e}")
        raise

    # Step 3: Sync order_status from fact_orders
    # Catches any orders whose status changed between batches
    try:
        logger.info(f"[order_items] Syncing order_status from fact_orders...")
        run_merge(config["pg_conn"], STATUS_SYNC_SQL)
        logger.info(f"[order_items] Status sync successful.")

    except Exception as e:
        logger.error(f"[order_items] Status sync failed: {e}")
        raise

    # Step 4: Truncate staging
    try:
        truncate_staging(config["pg_conn"], STAGING_TABLE)
        logger.info(f"[order_items] Staging truncated.")

    except Exception as e:
        logger.warning(f"[order_items] Staging truncate failed: {e}")


# -------------------------------------------------------------------
# Stream entry point
# Called from main.py
# -------------------------------------------------------------------

def start_order_items_stream(spark: SparkSession, config: dict):
    """
    Reads from Kafka topic → parses Debezium envelope
    → upserts into fact_order_items with denormalized order_status.
    Runs STATUS_SYNC_SQL every batch to stay in sync with
    status changes coming through the orders stream.
    """
    logger.info("[order_items] Starting order_items stream.")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka_bootstrap"])
        .option("subscribe", config["topics"]["order_items"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = parse_debezium_envelope(raw_df, ORDER_ITEM_SCHEMA)

    query = (
        parsed_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, config)
        )
        .option(
            "checkpointLocation",
            config["checkpoint_base"] + "/order_items"
        )
        .trigger(processingTime=config["trigger_interval"])
        .queryName("order_items_stream")
        .start()
    )

    logger.info("[order_items] Order items stream started.")
    return query