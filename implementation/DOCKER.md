# Spark Consumer Docker Implementation

This guide explains how to build and run the Spark consumer application in Docker.

## Prerequisites

- Docker installed and running
- Docker Compose installed (v1.29+)
- The main services running (Kafka, PostgreSQL) via `docker-compose.yaml`

## Building the Docker Image

### Option 1: Build from the root directory

```bash
# Build the Spark consumer image
docker build -f implementation/Dockerfile -t deel-spark-consumer:latest implementation/

# Or with a specific tag
docker build -f implementation/Dockerfile -t deel-spark-consumer:v1.0 implementation/
```

### Option 2: Use Docker Compose

```bash
# Build and run all services including Spark consumer
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml up -d --build

# Or build only the Spark service
docker-compose -f docker-compose-spark.yaml build spark-consumer
```

## Running the Container

### Option 1: Using Docker Compose (Recommended)

First, ensure the main services are running:

```bash
# Start the main infrastructure (Kafka, PostgreSQL, etc.)
docker-compose up -d

# In another terminal, start the Spark consumer
docker-compose -f docker-compose-spark.yaml up -d
```

### Option 2: Using Docker CLI

```bash
# Start main services first
docker-compose up -d

# Run the Spark consumer container
docker run -d \
  --name spark-consumer \
  --network deel-data-engineering-task_default \
  -e KAFKA_HOST=kafka \
  -e KAFKA_PORT=9092 \
  -e DB_HOST=transactions-db \
  -e DB_PORT=5432 \
  -e DB_USER=finance_db_user \
  -e DB_PASSWORD=1234 \
  -e DB_NAME=finance_db \
  -v spark-checkpoints:/app/checkpoints \
  -p 4040:4040 \
  deel-spark-consumer:latest
```

## Environment Variables

Configure the following environment variables when running the container:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_HOST` | `localhost` | Kafka broker hostname |
| `KAFKA_PORT` | `9092` | Kafka broker port (use 9092 for Docker, 9094 for localhost) |
| `DB_HOST` | `localhost` | PostgreSQL hostname |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `finance_db_user` | Database user |
| `DB_PASSWORD` | `1234` | Database password |
| `DB_NAME` | `finance_db` | Database name |

## Viewing Logs

### Using Docker Compose

```bash
# View real-time logs
docker-compose -f docker-compose-spark.yaml logs -f spark-consumer

# View last 100 lines
docker-compose -f docker-compose-spark.yaml logs --tail=100 spark-consumer
```

### Using Docker CLI

```bash
# View real-time logs
docker logs -f spark-consumer

# View last 100 lines with timestamps
docker logs --tail=100 --timestamps spark-consumer
```

## Accessing Spark UI

The Spark UI is available at:
```
http://localhost:4040
```

Note: The UI is only available while the Spark job is running.

## Stopping and Cleaning Up

### Using Docker Compose

```bash
# Stop the Spark consumer
docker-compose -f docker-compose-spark.yaml down

# Stop everything (main services + Spark)
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml down

# Full cleanup including volumes
docker-compose -f docker-compose.yaml -f docker-compose-spark.yaml down -v
```

### Using Docker CLI

```bash
# Stop the container
docker stop spark-consumer

# Remove the container
docker rm spark-consumer

# Remove the image (optional)
docker rmi deel-spark-consumer:latest

# Clean up volumes (optional)
docker volume rm spark-checkpoints spark-temp
```

## Troubleshooting

### Connection Issues to Kafka/Database

If the container cannot connect to Kafka or PostgreSQL:

1. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

2. **Check network connectivity:**
   ```bash
   # Inspect the Docker network
   docker network inspect deel-data-engineering-task_default
   
   # Test connection from container
   docker run -it --rm --network deel-data-engineering-task_default \
     centos:7 bash -c "nmap -p 9092 kafka && nmap -p 5432 transactions-db"
   ```

3. **Verify hostnames in docker-compose services section:**
   - Kafka: `kafka:9092` (internal) or `kafka:9094` (external)
   - PostgreSQL: `transactions-db:5432` (internal) or `localhost:5432` (external)

### Out of Memory

If Spark runs out of memory, increase memory allocation:

```bash
# Edit docker-compose-spark.yaml and add resource limits:
deploy:
  resources:
    limits:
      memory: 8G
    reservations:
      memory: 4G
```

### Checkpoint Issues

If you need to reset checkpoints:

```bash
# Remove the checkpoint volume
docker volume rm spark-checkpoints

# Recreate by running:
docker-compose -f docker-compose-spark.yaml up -d
```

## Development Workflow

For local development without Docker:

```bash
# Use the spark-submit command directly
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.local.dir="D:/spark_temp" \
  --conf spark.files.cleanup=false \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  implementation/main.py
```

## Configuration Files

- **Dockerfile**: Container image configuration
- **.dockerignore**: Files to exclude from Docker build context
- **docker-compose-spark.yaml**: Service definition for Docker Compose
- **config.py**: Application configuration (supports environment variables)
- **log4j.properties**: Logging configuration

## Next Steps

1. Build the image: `docker build -f implementation/Dockerfile -t deel-spark-consumer:latest implementation/`
2. Start infrastructure: `docker-compose up -d`
3. Start Spark consumer: `docker-compose -f docker-compose-spark.yaml up -d`
4. Monitor: `docker-compose -f docker-compose-spark.yaml logs -f spark-consumer`
5. Access Spark UI: http://localhost:4040
