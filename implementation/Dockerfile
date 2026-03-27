FROM apache/spark:3.5.0

# Switch to root to install additional dependencies
USER root

# Install Python development tools and PostgreSQL client
RUN apt-get update && apt-get install -y \
    python3-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the entire implementation folder
COPY . /app/

# Create checkpoint and temp directories
RUN mkdir -p /app/checkpoints /tmp/spark_temp

# Download log4j.properties or create placeholder
# If log4j.properties exists in implementation folder, it will be copied above
# Otherwise, create a default one
RUN if [ ! -f /app/log4j.properties ]; then \
    echo "log4j.rootCategory=ERROR, console" > /app/log4j.properties && \
    echo "log4j.logger.org.apache.spark=ERROR" >> /app/log4j.properties && \
    echo "log4j.appender.console=org.apache.log4j.ConsoleAppender" >> /app/log4j.properties && \
    echo "log4j.appender.console.layout=org.apache.log4j.PatternLayout" >> /app/log4j.properties; \
    fi

# Install Python requirements
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python \
    psycopg2-binary \
    jaydebeapi

# Set environment variables for Spark
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_DRIVER_HOST=127.0.0.1

# Expose Spark UI port (optional, for monitoring)
EXPOSE 4040

# Default command - can be overridden when running the container
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3", \
     "--conf", "spark.local.dir=/tmp/spark_temp", \
     "--conf", "spark.files.cleanup=false", \
     "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/app/log4j.properties", \
     "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/app/log4j.properties", \
     "main.py"]
