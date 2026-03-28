# Project Execution Guide

This file explains the exact startup order for this repository.

## Run Order (High Level)

1. Start the core data stack from the project root (`docker-compose.yaml`).
2. Start the Spark consumer from the project root (`docker-compose-spark.yaml`).
3. Start the visualization app from `visualization/` (`visualization/docker-compose.yml`).

## Prerequisites

- Docker Desktop running
- Docker Compose available (`docker compose`)
- Ports available: `3000`, `5432`, `8001`, `8083`, `8087`, `9094`

## Step 0: Open terminal in repo root

```bash
cd /path/to/deel-data-engineering-task
```

On Windows PowerShell, for example:

```powershell
cd D:\Deel\deel-data-engineering-task
```

## Step 1: Start core infrastructure

From the repo root, run:

```bash
docker compose up -d --build
```

This starts:

- PostgreSQL (`transactions-db`)
- Kafka + Zookeeper
- Kafka Connect + Debezium init
- Conduktor + its PostgreSQL

Quick checks:

```bash
docker compose ps
```

Optional logs:

```bash
docker compose logs -f kafka-connect
```

## Step 2: Start Spark consumer

From the same repo root, run:

```bash
docker compose -f docker-compose-spark.yaml up -d --build
```

Quick checks:

```bash
docker compose -f docker-compose-spark.yaml ps
docker compose -f docker-compose-spark.yaml logs -f spark-consumer
```


## Step 3: Start visualization stack

Open a second terminal and move into `visualization/`:

```bash
cd visualization
docker compose up -d --build
```

Quick checks:

```bash
docker compose ps
docker compose logs -f backend
docker compose logs -f frontend
```

UIs:

- Frontend: http://localhost:3000
- Visualization backend API: http://localhost:8001
- Conduktor: http://localhost:8087

## Full Startup (copy/paste sequence)

```bash
# Terminal 1 (repo root)
docker compose up -d --build
docker compose -f docker-compose-spark.yaml up -d --build

# Terminal 2
cd visualization
docker compose up -d --build
```

## Stop Order (Recommended)

1. Stop visualization
2. Stop Spark consumer
3. Stop core infrastructure

Commands:

```bash
# from visualization/
docker compose down

# from repo root
docker compose -f docker-compose-spark.yaml down
docker compose down
```

## Full cleanup (containers + volumes)

Use this only when you want a full reset:

```bash
# from visualization/
docker compose down -v

# from repo root
docker compose -f docker-compose-spark.yaml down -v
docker compose down -v
```

## Notes

- The root `.env` provides required values for Debezium/Kafka image tags and advertised host.
- Start the core stack first so the `finance-shared-net` network exists before Spark starts.
- Visualization backend uses `host.docker.internal:5432`, so the root PostgreSQL must be running.
