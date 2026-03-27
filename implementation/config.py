import os

# Support for both local and Docker deployment
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9094")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "finance_db_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")
DB_NAME = os.getenv("DB_NAME", "finance_db")

config = {
    # -----------------------------------------------------------
    # Kafka
    # -----------------------------------------------------------
    "kafka_bootstrap": f"{KAFKA_HOST}:{KAFKA_PORT}",

    # Debezium topic names — format: <prefix>.<schema>.<table>
    "topics": {
        "products":    "finance_db.operations.products",
        "customers":   "finance_db.operations.customers",
        "orders":      "finance_db.operations.orders",
        "order_items": "finance_db.operations.order_items",
    },

    # -----------------------------------------------------------
    # PostgreSQL JDBC
    # -----------------------------------------------------------
    "jdbc_url": f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?TimeZone=Europe/Kyiv",

    "jdbc_props": {
        "user":                 DB_USER,
        "password":             DB_PASSWORD,
        "driver":               "org.postgresql.Driver",
        "options":              "-c TimeZone=Europe/Kyiv",
        "connectTimeout":       "60",
        "socketTimeout":        "300",
        "loginTimeout":         "60",
    },

    "pg_conn": {
        "host":     DB_HOST,
        "port":     int(DB_PORT),
        "dbname":   DB_NAME,
        "user":     DB_USER,
        "password": DB_PASSWORD,
    },

    # -----------------------------------------------------------
    # Spark
    # -----------------------------------------------------------
    "jdbc_jar":         "./jars/postgresql-42.7.3.jar",
    "checkpoint_base":  "./checkpoints",

    # Trigger interval for all streams
    "trigger_interval": "10 seconds",
}