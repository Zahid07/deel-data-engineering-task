import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
	StructType, StructField,
	LongType, StringType, BooleanType, TimestampType
)

from utils.debezium import parse_debezium_envelope
from utils.db import run_merge, truncate_staging

logger = logging.getLogger(__name__)


CUSTOMER_SCHEMA = StructType([
	StructField("customer_id", LongType(), False),
	StructField("customer_name", StringType(), True),
	StructField("is_active", BooleanType(), True),
	StructField("customer_address", StringType(), True),
	StructField("updated_at", TimestampType(), True),
	StructField("updated_by", LongType(), True),
	StructField("created_at", TimestampType(), True),
	StructField("created_by", LongType(), True)
])


STAGING_TABLE = "analytical.dim_customer_staging"
TARGET_TABLE = "analytical.dim_customer"
HISTORY_TABLE = "analytical.dim_customer_history"


MERGE_SQL = f"""
	INSERT INTO {TARGET_TABLE} (
		customer_id,
		customer_name,
		is_active,
		customer_address,
		updated_at,
		created_at,
		created_by,
		updated_by,
		_additional_columns,
		_kafka_offset
	)
	SELECT DISTINCT ON (customer_id)
		customer_id,
		COALESCE(customer_name, ''),
		COALESCE(is_active, true),
		customer_address,
		updated_at,
		created_at,
		created_by,
		updated_by,
		_additional_columns,
		_kafka_offset
	FROM {STAGING_TABLE}
	ORDER BY customer_id, updated_at DESC NULLS LAST
	ON CONFLICT (customer_id) DO UPDATE SET
		customer_name    = COALESCE(EXCLUDED.customer_name, ''),
		is_active        = COALESCE(EXCLUDED.is_active, true),
		customer_address = EXCLUDED.customer_address,
		updated_at       = EXCLUDED.updated_at,
		created_by       = EXCLUDED.created_by,
		updated_by       = EXCLUDED.updated_by,
		_additional_columns = EXCLUDED._additional_columns,
		_kafka_offset    = EXCLUDED._kafka_offset;
"""


HISTORY_INSERT_SQL = f"""
	INSERT INTO {HISTORY_TABLE} (
		customer_id,
		customer_name,
		is_active,
		customer_address,
		updated_at,
		created_at,
		created_by,
		updated_by,
		_additional_columns,
		_kafka_offset
	)
	SELECT
		customer_id,
		COALESCE(customer_name, ''),
		COALESCE(is_active, true),
		customer_address,
		updated_at,
		created_at,
		created_by,
		updated_by,
		_additional_columns,
		_kafka_offset
	FROM {STAGING_TABLE};
"""	




def process_batch(batch_df: DataFrame, batch_id: int, config: dict):
	if batch_df.isEmpty():
		logger.info(f"[customers] Batch {batch_id} is empty, skipping.")
		return

	logger.info(f"[customers] Processing batch {batch_id}.")

	# Deduplicaing data by customer_id, keeping the latest record per customer based on updated_at and created_at.
	#latest_per_customer = Window.partitionBy("customer_id").orderBy(
	#	F.col("updated_at").desc_nulls_last(),
	#	F.col("created_at").desc_nulls_last(),
	#)
	#dedup_df = (
	#	batch_df
	#	.filter(F.col("customer_id").isNotNull())
	#	.withColumn("_rn", F.row_number().over(latest_per_customer))
	#	.filter(F.col("_rn") == 1)
	#	.drop("_rn")
	#)

	dim_df = batch_df.drop("op").coalesce(1)

	if dim_df.isEmpty():
		logger.info(f"[customers] Batch {batch_id} has no valid customer rows after dedup, skipping.")
		return

	# dim_df = dedup_df.drop("op").coalesce(1)
	#dim_df = (
    #    dedup_df
    #    .drop("op")
    #    .dropDuplicates(["customer_id"])  # final safety net after window dedup
    #    .coalesce(1)
    #)

	try:
		logger.info(f"[customers] Writing to staging table {STAGING_TABLE}...")
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
		logger.info("[customers] Staging write successful.")

	except Exception as e:
		logger.error(f"[customers] Staging write failed: {e}")
		raise

	try:
		logger.info(f"[customers] Running MERGE into {TARGET_TABLE}...")
		run_merge(config["pg_conn"], MERGE_SQL)
		logger.info("[customers] MERGE successful.")

	except Exception as e:
		logger.error(f"[customers] MERGE failed: {e}")
		raise

	try:
		logger.info(f"[customers] Inserting into history table {HISTORY_TABLE}...")
		run_merge(config["pg_conn"], HISTORY_INSERT_SQL)
		logger.info("[customers] History insert successful.")
	except Exception as e:
		logger.error(f"[customers] History insert failed: {e}")

	try:
		truncate_staging(config["pg_conn"], STAGING_TABLE)
		logger.info("[customers] Staging truncated.")

	except Exception as e:
		logger.warning(f"[customers] Staging truncate failed: {e}")


def start_customers_stream(spark: SparkSession, config: dict):
	"""
	Reads from Kafka topic -> parses Debezium envelope
	-> upserts into dim_customer every trigger interval.
	Returns the StreamingQuery so main.py can manage it.
	"""
	logger.info("[customers] Starting customers stream.")

	raw_df = (
		spark.readStream
		.format("kafka")
		.option("kafka.bootstrap.servers", config["kafka_bootstrap"])
		.option("subscribe", config["topics"]["customers"])
		.option("startingOffsets", "earliest")
		.option("failOnDataLoss", "false")
		.load()
	)

	parsed_df = parse_debezium_envelope(raw_df, CUSTOMER_SCHEMA)

	query = (
		parsed_df.writeStream
		.foreachBatch(
			lambda batch_df, batch_id: process_batch(batch_df, batch_id, config)
		)
		.option(
			"checkpointLocation",
			config["checkpoint_base"] + "/customers"
		)
		.trigger(processingTime=config["trigger_interval"])
		.queryName("customers_stream")
		.start()
	)

	logger.info("[customers] Customers stream started.")
	return query
