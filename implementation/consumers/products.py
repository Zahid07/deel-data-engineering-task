# consumers/products.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, BooleanType, TimestampType, DecimalType
)

from utils.debezium import parse_debezium_envelope
from utils.db import run_merge, truncate_staging

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Table schema
# Mirrors analytical.dim_product exactly.
# Passed to parse_debezium_envelope to build the envelope dynamically.
# -------------------------------------------------------------------

PRODUCT_SCHEMA = StructType([
    StructField("product_id",   LongType(),        False),
    StructField("product_name", StringType(),      True),
    StructField("barcode",      StringType(),      True),
    StructField("unity_price",  DecimalType(10,2), True),
    StructField("is_active",    BooleanType(),     True),
    StructField("updated_at",   TimestampType(),   True),
    StructField("updated_by",   LongType(),        True),
    StructField("created_at",   TimestampType(),   True),
    StructField("created_by",   LongType(),        True),
])


STAGING_TABLE = "analytical.dim_product_staging"
TARGET_TABLE  = "analytical.dim_product"


# -------------------------------------------------------------------
# MERGE SQL
# Runs after Spark writes the micro-batch to staging.
# ON CONFLICT handles both inserts and updates idempotently.
# -------------------------------------------------------------------

MERGE_SQL = f"""
    INSERT INTO {TARGET_TABLE} (
        product_id,
        product_name,
        barcode,
        unity_price,
        is_active,
        updated_at,
        created_at,
        _additional_columns,
        _kafka_offset
    )
    SELECT
        product_id,
        COALESCE(product_name, ''),
        COALESCE(barcode, ''),
        COALESCE(unity_price, 0.00),
        COALESCE(is_active, true),
        updated_at,
        created_at,
        _additional_columns,
        _kafka_offset
    FROM {STAGING_TABLE}
    ON CONFLICT (product_id) DO UPDATE SET
        product_name = COALESCE(EXCLUDED.product_name, ''),
        barcode      = COALESCE(EXCLUDED.barcode, ''),
        unity_price  = COALESCE(EXCLUDED.unity_price, 0.00),
        is_active    = COALESCE(EXCLUDED.is_active, true),
        updated_at   = EXCLUDED.updated_at,
        _additional_columns = EXCLUDED._additional_columns,
        _kafka_offset = EXCLUDED._kafka_offset;
"""


def process_batch(batch_df: DataFrame, batch_id: int, config: dict):
    if batch_df.isEmpty():
        logger.info(f"[products] Batch {batch_id} is empty, skipping.")
        return

    logger.info(f"[products] Processing batch {batch_id}.")

    # Keep one latest row per product_id so ON CONFLICT updates each target row once.
    latest_per_product = Window.partitionBy("product_id").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("created_at").desc_nulls_last(),
    )
    dedup_df = (
        batch_df
        .filter(F.col("product_id").isNotNull())
        .withColumn("_rn", F.row_number().over(latest_per_product))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if dedup_df.isEmpty():
        logger.info(f"[products] Batch {batch_id} has no valid product rows after dedup, skipping.")
        return

    # Coalesce to 1 partition before JDBC write
    # Multiple partitions = multiple concurrent JDBC connections
    # which can cause hangs on local mode
    dim_df = (
        dedup_df
        .drop("op")
        .dropDuplicates(["product_id"])  # final safety net after window dedup
        .coalesce(1)
    )

    # Debug: print rows and schema to terminal
    # dim_df.show(truncate=False)
    # dim_df.printSchema()

    # Step 1: Write micro-batch to staging
    try:
        logger.info(f"[products] Writing to staging table {STAGING_TABLE}...")
        (
            dim_df.write.format("jdbc")
            .option("url", config["jdbc_url"])
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", STAGING_TABLE)
            .option("user", config["jdbc_props"]["user"])
            .option("password", config["jdbc_props"]["password"])
            .option("mode", "overwrite")
            .mode("overwrite")
            .save()
        )
        logger.info(f"[products] Staging write successful.")

    except Exception as e:
        logger.error(f"[products] Staging write failed: {e}")
        raise

    # Step 2: MERGE staging → dim_product
    try:
        logger.info(f"[products] Running MERGE into {TARGET_TABLE}...")
        run_merge(config["pg_conn"], MERGE_SQL)
        logger.info(f"[products] MERGE successful.")

    except Exception as e:
        logger.error(f"[products] MERGE failed: {e}")
        raise

    # Step 3: Truncate staging
    try:
        truncate_staging(config["pg_conn"], STAGING_TABLE)
        logger.info(f"[products] Staging truncated.")

    except Exception as e:
        # Non-fatal — next batch overwrites staging anyway
        logger.warning(f"[products] Staging truncate failed: {e}")

# -------------------------------------------------------------------
# Stream entry point
# Called from main.py
# -------------------------------------------------------------------

def start_products_stream(spark: SparkSession, config: dict):
    """
    Reads from Kafka topic → parses Debezium envelope
    → upserts into dim_product every trigger interval.
    Returns the StreamingQuery so main.py can manage it.
    """
    logger.info("[products] Starting products stream.")

    # 1. Read raw Kafka stream
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka_bootstrap"])
        .option("subscribe", config["topics"]["products"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")      # don't crash if Kafka topic is reset
        .load()
    )

    # 2. Parse Debezium envelope → flat product fields
    parsed_df = parse_debezium_envelope(raw_df, PRODUCT_SCHEMA)

    # parsed_df.writeStream \
    # .format("console") \
    # .option("truncate", False) \
    # .outputMode("append") \
    # .start()

    # 3. Write stream
    query = (
        parsed_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, config)
        )
        .option(
            "checkpointLocation",
            config["checkpoint_base"] + "/products"
        )
        .trigger(processingTime=config["trigger_interval"])
        .queryName("products_stream")           # visible in Spark UI
        .start()
    )

    logger.info("[products] Products stream started.")
    return query