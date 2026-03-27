# utils/debezium.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField, LongType
import json


def parse_debezium_envelope(df: DataFrame, after_schema: StructType) -> DataFrame:
    """
    Every Debezium CDC event has the same outer envelope:
    {
        "before": { ...row state before the change... },
        "after":  { ...row state after the change...  },
        "op":     "c" | "u" | "d" | "r"
    }

    op values:
        c = INSERT  (create)
        u = UPDATE
        d = DELETE
        r = READ (snapshot — initial bulk load on connector start)

    We define the envelope schema dynamically by wrapping the
    caller-supplied after_schema, so each consumer only needs
    to define its own table schema, not the full envelope.

    Returns a DataFrame with:
        - all fields from "after" flattened to top level
        - "op" column retained so consumers can filter if needed
        - "_kafka_offset" column extracted from Kafka offset metadata
        - "_additional_columns" containing any columns in Kafka payload not in the schema (as JSON string)
    """

    # Build the full envelope schema dynamically
    # "before" and "after" share the same structure (the table schema)
    envelope_schema = StructType([
        StructField("before", after_schema, True),
        StructField("after",  after_schema, True),
        StructField("op",     StringType(), False),
    ])

    # Get the expected field names from after_schema
    expected_fields = {field.name for field in after_schema.fields}
    # remove _additional_columns and _kafka_offset if they are in the schema, since those are reserved for our use
    expected_fields.discard("_additional_columns")
    expected_fields.discard("_kafka_offset")

    # Parse Kafka message to extract payload, offset, and additional columns
    parsed_df = (
        df
        # Kafka delivers value as binary — cast to string first
        .select(
            F.col("offset").alias("_kafka_offset"),
            F.col("value").cast("string").alias("_raw_value"),
            F.from_json(
                F.col("value").cast("string"),
                envelope_schema
            ).alias("payload")
        )
        # Filter: keep inserts, updates, and snapshot reads
        # Drop deletes (op = "d") — dims use is_active for soft deletes
        .filter(F.col("payload.op").isin("c", "u", "r"))
    )

    # Create UDF to extract additional columns
    @F.udf(returnType=StringType())
    def extract_additional_columns(value_str):
        """
        Extract any columns from the Kafka message's 'after' object 
        that are not part of the expected schema.
        Returns JSON string of additional columns, or null if none.
        """
        try:
            if value_str is None:
                return None
            payload = json.loads(value_str)
            after_obj = payload.get("after", {})
            
            if not isinstance(after_obj, dict):
                return None
            
            # Find columns not in expected_fields
            additional = {k: v for k, v in after_obj.items() if k not in expected_fields}
            
            if additional:
                return json.dumps(additional)
            return None
        except Exception:
            # If JSON parsing fails, return null
            return None

    # Flatten "after" fields to top level + keep op + _kafka_offset + _additional_columns
    result_df = (
        parsed_df.select(
            F.col("payload.after.*"),
            F.col("payload.op"),
            F.col("_kafka_offset"),
            extract_additional_columns(F.col("_raw_value")).alias("_additional_columns"),
        )
    )
    
    return result_df




def is_delete(df: DataFrame, after_schema: StructType) -> DataFrame:
    """
    Separate helper for handling deletes if a consumer needs them.
    For example, if we ever want to set is_active=false on delete
    rather than just dropping the event, the consumer can call this
    to get a DataFrame of deleted row IDs.

    Returns a DataFrame with fields from "before" + op="d".
    """
    envelope_schema = StructType([
        StructField("before", after_schema, True),
        StructField("after",  after_schema, True),
        StructField("op",     StringType(), False),
    ])

    return (
        df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                envelope_schema
            ).alias("payload")
        )
        .filter(F.col("payload.op") == "d")
        # For deletes, "after" is null — we read from "before"
        .select(
            F.col("payload.before.*"),
            F.col("payload.op"),
        )
    )