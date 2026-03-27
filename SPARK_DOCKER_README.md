# Spark Consumer Docker Setup Guide

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Main services running (Kafka, PostgreSQL)

### Step 1: Build the Spark Consumer Image

```bash
cd D:\Deel\deel-data-engineering-task
docker build -f implementation/Dockerfile -t deel-spark-consumer:latest implementation/
```

### Step 2: Start Infrastructure Services

```bash
docker-compose up -d
```

### Step 3: Start Spark Consumer

```bash
docker-compose -f docker-compose-spark.yaml up -d
```

### Step 4: Monitor the Logs

```bash
docker-compose -f docker-compose-spark.yaml logs -f spark-consumer
```

---

## Commands Summary

### Build Commands

```bash
# Build only the Spark image
docker build -f implementation/Dockerfile -t deel-spark-consumer:latest implementation/

# Build with Docker Compose
docker-compose -f docker-compose-spark.yaml build spark-consumer

# Build all services together
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml build
```

### Run Commands

```bash
# Start main infrastructure (Kafka, PostgreSQL, etc.)
docker-compose up -d

# Start Spark consumer
docker-compose -f docker-compose-spark.yaml up -d

# Start everything in one command
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml up -d --build
```

### Monitor Commands

```bash
# View Spark consumer logs (live)
docker-compose -f docker-compose-spark.yaml logs -f spark-consumer

# View last 100 lines
docker-compose -f docker-compose-spark.yaml logs --tail=100

# List all running containers
docker-compose ps

# Check specific container status
docker ps | grep spark-consumer
```

### Stop Commands

```bash
# Stop Spark consumer
docker-compose -f docker-compose-spark.yaml down

# Stop all services
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml down

# Stop and remove volumes (full cleanup)
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml down -v
```

### Access & Testing

```bash
# Access Spark UI
# Open in browser: http://localhost:4040

# Check container connectivity
docker exec spark-consumer ping kafka
docker exec spark-consumer ping transactions-db

# Test database connection
docker exec spark-consumer psql -h transactions-db -U finance_db_user -d finance_db -c "SELECT 1"

# View container details
docker inspect spark-consumer
```

---

## Environment Variables

The Spark consumer supports the following environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_HOST` | `kafka` | Kafka broker hostname |
| `KAFKA_PORT` | `9092` | Kafka broker port |
| `DB_HOST` | `transactions-db` | PostgreSQL hostname |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `finance_db_user` | Database user |
| `DB_PASSWORD` | `1234` | Database password |
| `DB_NAME` | `finance_db` | Database name |

---

## Typical Workflow

```bash
# 1. Open terminal from project root
cd D:\Deel\deel-data-engineering-task

# 2. Build the image (one time only)
docker build -f implementation/Dockerfile -t deel-spark-consumer:latest implementation/

# 3. Start infrastructure
docker-compose up -d

# 4. Start Spark consumer
docker-compose -f docker-compose-spark.yaml up -d

# 5. Monitor logs
docker-compose -f docker-compose-spark.yaml logs -f spark-consumer

# 6. Access Spark UI (in browser)
# http://localhost:4040

# 7. When done, stop services
docker-compose -f docker-compose-spark.yaml down
docker-compose down
```

---

## Troubleshooting

### Container fails to start

Check logs:
```bash
docker-compose -f docker-compose-spark.yaml logs spark-consumer
```

### Connection refused errors

Verify services are running:
```bash
docker-compose ps
```

Ensure Kafka and PostgreSQL containers are healthy before starting Spark.

### Out of memory

Increase memory in `docker-compose-spark.yaml`:
```yaml
deploy:
  resources:
    limits:
      memory: 8G
```

### Reset checkpoints

```bash
docker volume rm spark-checkpoints
docker-compose -f docker-compose-spark.yaml up -d
```

---

## File References

- **Dockerfile**: `implementation/Dockerfile` - Container image configuration
- **Docker Compose**: `docker-compose-spark.yaml` - Service definition
- **Config**: `implementation/config.py` - Application settings (supports environment variables)
- **Documentation**: `implementation/DOCKER.md` - Detailed Docker guide

---

## Network Architecture

```
Docker Network (local)
├── kafka (port 9092 internal, 9094 external)
├── transactions-db (port 5432)
├── zookeeper
└── spark-consumer (port 4040 for UI)
    └── Connects to kafka:9092 and transactions-db:5432
```

---

## Local Development (Without Docker)

To run the Spark consumer locally without Docker, use the original spark-submit command:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.local.dir="D:/spark_temp" \
  --conf spark.files.cleanup=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  implementation/main.py
```

Note: When running locally, update `config.py` to use `localhost` instead of Docker hostnames.

---

## Resources

- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)
- [Bitnami Spark Image](https://hub.docker.com/r/bitnami/spark)

