# consumers/orders.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, TimestampType, DateType
)

from utils.debezium import parse_debezium_envelope
from utils.db import run_merge, truncate_staging

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Schema
# Mirrors the source orders table exactly.
# -------------------------------------------------------------------

ORDER_SCHEMA = StructType([
    StructField("order_id",      LongType(),      False),
    StructField("order_date",    DateType(),      True),
    StructField("delivery_date", DateType(),      True),
    StructField("customer_id",   LongType(),      True),
    StructField("status",        StringType(),    True),
    StructField("updated_at",    TimestampType(), True),
    StructField("updated_by",    LongType(),      True),
    StructField("created_at",    TimestampType(), True),
    StructField("created_by",    LongType(),      True)
])


STAGING_TABLE  = "analytical.fact_orders_staging"
TARGET_TABLE   = "analytical.fact_orders"
HISTORY_TABLE  = "analytical.fact_orders_history"


# -------------------------------------------------------------------
# MERGE SQL — upserts into fact_orders (latest state)
# -------------------------------------------------------------------

MERGE_SQL = f"""
    INSERT INTO {TARGET_TABLE} (
        order_id,
        customer_id,
        order_date,
        delivery_date,
        status,
        updated_at,
        created_at,
        created_by,
        updated_by,
        _additional_columns,
        _kafka_offset
    )
    SELECT DISTINCT ON (order_id)
        order_id,
        COALESCE(customer_id, 0),
        COALESCE(order_date, CURRENT_DATE),
        COALESCE(delivery_date, CURRENT_DATE),
        COALESCE(status, 'unknown'),
        updated_at,
        created_at,
        created_by,
        updated_by,
        _additional_columns,
        _kafka_offset
    FROM {STAGING_TABLE}
    ORDER BY order_id, updated_at DESC NULLS LAST
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id         = COALESCE(EXCLUDED.customer_id, 0),
        order_date          = COALESCE(EXCLUDED.order_date, CURRENT_DATE),
        delivery_date       = COALESCE(EXCLUDED.delivery_date, CURRENT_DATE),
        status              = COALESCE(EXCLUDED.status, 'unknown'),
        updated_at          = EXCLUDED.updated_at,
        created_by          = EXCLUDED.created_by,
        updated_by          = EXCLUDED.updated_by,
        _additional_columns = EXCLUDED._additional_columns,
        _kafka_offset       = EXCLUDED._kafka_offset;
"""

# -------------------------------------------------------------------
# HISTORY INSERT SQL — appends every change to fact_orders_history
# No ON CONFLICT — every event is a new history row.
# recorded_at defaults to NOW() so we know when Spark captured it.
# -------------------------------------------------------------------

HISTORY_SQL = f"""
    INSERT INTO {HISTORY_TABLE} (
        order_id,
        customer_id,
        order_date,
        delivery_date,
        status,
        updated_at,
        created_at,
        created_by,
        updated_by,
        _additional_columns,
        _kafka_offset
    )
    SELECT
        order_id,
        COALESCE(customer_id, 0),
        COALESCE(order_date, CURRENT_DATE),
        COALESCE(delivery_date, CURRENT_DATE),
        COALESCE(status, 'unknown'),
        updated_at,
        created_at,
        created_by,
        updated_by,
        _additional_columns,
        _kafka_offset
    FROM {STAGING_TABLE};
"""


# -------------------------------------------------------------------
# foreachBatch handler
# SCD Type 4 pattern:
#   1. Write batch to staging
#   2. MERGE staging → fact_orders        (current state)
#   3. INSERT staging → fact_orders_history (historical record)
#   4. Truncate staging
# -------------------------------------------------------------------

def process_batch(batch_df: DataFrame, batch_id: int, config: dict):
    if batch_df.isEmpty():
        logger.info(f"[orders] Batch {batch_id} is empty, skipping.")
        return

    logger.info(f"[orders] Processing batch {batch_id}.")

    fact_df = batch_df.drop("op").coalesce(1)

    # fact_df.show(truncate=False)
    # fact_df.printSchema()

    # Step 1: Write micro-batch to staging
    try:
        logger.info(f"[orders] Writing to staging table {STAGING_TABLE}...")
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
        logger.info(f"[orders] Staging write successful.")

    except Exception as e:
        logger.error(f"[orders] Staging write failed: {e}")
        raise

    # Step 2: MERGE staging → fact_orders (latest state)
    try:
        logger.info(f"[orders] Running MERGE into {TARGET_TABLE}...")
        run_merge(config["pg_conn"], MERGE_SQL)
        logger.info(f"[orders] MERGE successful.")

    except Exception as e:
        logger.error(f"[orders] MERGE failed: {e}")
        raise

    # Step 3: INSERT staging → fact_orders_history (historical record)
    # This runs regardless of whether the order already exists in fact_orders
    # Every CDC event = one new history row
    try:
        logger.info(f"[orders] Inserting into history table {HISTORY_TABLE}...")
        run_merge(config["pg_conn"], HISTORY_SQL)  
        logger.info(f"[orders] History insert successful.")

    except Exception as e:
        logger.error(f"[orders] History insert failed: {e}")
        raise

    # Step 4: Truncate staging
    try:
        truncate_staging(config["pg_conn"], STAGING_TABLE)
        logger.info(f"[orders] Staging truncated.")

    except Exception as e:
        logger.warning(f"[orders] Staging truncate failed: {e}")


# -------------------------------------------------------------------
# Stream entry point
# Called from main.py
# -------------------------------------------------------------------

def start_orders_stream(spark: SparkSession, config: dict):
    """
    Reads from Kafka topic → parses Debezium envelope
    → upserts into fact_orders (current state)
    → appends into fact_orders_history (full change trail)
    """
    logger.info("[orders] Starting orders stream.")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka_bootstrap"])
        .option("subscribe", config["topics"]["orders"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = parse_debezium_envelope(raw_df, ORDER_SCHEMA)

    query = (
        parsed_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, config)
        )
        .option(
            "checkpointLocation",
            config["checkpoint_base"] + "/orders"
        )
        .trigger(processingTime=config["trigger_interval"])
        .queryName("orders_stream")
        .start()
    )

    logger.info("[orders] Orders stream started.")
    return query
