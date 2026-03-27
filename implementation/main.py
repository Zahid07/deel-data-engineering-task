# test_products_stream.py

# import os
# import sys
# import logging

# logging.basicConfig(level=logging.INFO)

# os.environ['PYSPARK_PYTHON'] = os.path.join(sys.prefix, "Scripts", "python.exe")
# os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join(sys.prefix, "Scripts", "python.exe")

# from pyspark.sql import SparkSession
# from config import config
# from utils.db import validate_analytical_schema
# from consumers.products import start_products_stream
# # from consumers.customers import start_customers_stream

# # -------------------------------------------------------------------
# # SparkSession
# # -------------------------------------------------------------------

# spark = (
#     SparkSession.builder
#     .appName("acme-test-products")
#     .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
#         "org.postgresql:postgresql:42.7.3"       # let Spark download the JDBC jar too
#     )
#     .config("spark.sql.shuffle.partitions", "2")
#     .config("spark.driver.memory", "2g")
#     .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=C:/tmp/ivy -Divy.home=C:/tmp/ivy")
#     .master("local[4]")                          # 4 threads — enough for streaming + JDBC write
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("ERROR")

# # -------------------------------------------------------------------
# # Validate PostgreSQL schema before starting stream
# # -------------------------------------------------------------------

# validate_analytical_schema(config["pg_conn"])

# # -------------------------------------------------------------------
# # Start products and customers streams in parallel
# # -------------------------------------------------------------------

# products_query = start_products_stream(spark, config)
# # customers_query = start_customers_stream(spark, config)


def _is_spark_temp_delete_exception(exc: Exception) -> bool:
    return "Exception while deleting Spark temp" in str(exc)

# try:
#     spark.streams.awaitAnyTermination(timeout=120)
# finally:
#     try:
#         if products_query.isActive:
#             products_query.stop()
#         # if customers_query.isActive:
#         #     customers_query.stop()
#         spark.stop()
#     except Exception as e:
        # if _is_spark_temp_delete_exception(e):
        #     logging.warning("Suppressed Spark temp cleanup exception: %s", e)
        # else:
        #     raise



# main.py

import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Must be set before any PySpark imports
# -------------------------------------------------------------------

# os.environ['PYSPARK_PYTHON'] = os.path.join(sys.prefix, "Scripts", "python.exe")
# os.environ['PYSPARK_DRIVER_PYTHON'] = os.path.join(sys.prefix, "Scripts", "python.exe")

# -------------------------------------------------------------------
# PySpark imports
# -------------------------------------------------------------------

from pyspark.sql import SparkSession
from config import config
from utils.db import validate_analytical_schema
from consumers.products import start_products_stream
from consumers.customers import start_customers_stream
from consumers.order import start_orders_stream
from consumers.order_items import start_order_items_stream


# -------------------------------------------------------------------
# SparkSession
# -------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("acme-analytics")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=C:/tmp/ivy -Divy.home=C:/tmp/ivy")
        .master("local[4]")
        .getOrCreate()
    )


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting ACME Analytics Pipeline...")

    # Step 1: Validate PostgreSQL schema
    logger.info("Validating analytical schema...")
    validate_analytical_schema(config["pg_conn"])
    logger.info("Schema validation passed.")

    # Step 2: Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.")

    # Step 3: Start all 4 streams
    logger.info("Starting all streams...")
    queries = []

    try:
        products_query = start_products_stream(spark, config)
        queries.append(products_query)
        logger.info("Products stream started.")

        customers_query = start_customers_stream(spark, config)
        queries.append(customers_query)
        logger.info("Customers stream started.")

        orders_query = start_orders_stream(spark, config)
        queries.append(orders_query)
        logger.info("Orders stream started.")

        order_items_query = start_order_items_stream(spark, config)
        queries.append(order_items_query)
        logger.info("Order items stream started.")

        logger.info("All streams running. Awaiting termination...")

        # Step 4: Keep running until interrupted or a stream fails
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")

    except Exception as e:
        if _is_spark_temp_delete_exception(e):
            # logging.warning("Suppressed Spark temp cleanup exception: %s", e)
            logger.warning(f"Suppressed Spark temp cleanup exception: {e}")
        else:
            raise
        raise

    finally:
        logger.info("Stopping all streams...")
        for q in queries:
            try:
                q.stop()
            except Exception as e:
                logger.warning(f"Error stopping stream: {e}")
        spark.stop()
        logger.info("Pipeline stopped.")