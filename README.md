# Batch Ingestion System

This repository provides a turnkey batch file ingestion pipeline that writes CSV or Parquet data into **Redpanda** (Kafka‑compatible) topics.  
The stack is designed for local development on macOS using Docker Compose.

## Features

* **HTTP API** (`ingest-api`) in Go
  * Auto‑creates per‑job Kafka topics and DLQs
  * Accepts up to **1 GiB** CSV _or_ Parquet uploads
  * Starts cleanly even when Kafka is offline, returning actionable errors at runtime
* **CLI** (`batch`) in Go (Cobra)
  * Manage models and ingestion jobs
  * Pretty JSON output for easy piping
* **Scripts**
  * `scripts/build.sh` – build server + CLI images and extract a local binary
  * `scripts/up.sh` / `scripts/down.sh` – lifecycle control with optional volume cleanup
  * `scripts/test.sh` – comprehensive test suite (42 tests: HTTP API, CLI, DLQ, error scenarios)
* **Redpanda** broker and **Console** UI already wired up
* Fully containerised; **no host dependencies** beyond Docker + Bash
* Uses [`segmentio/kafka-go`] for Kafka I/O and [`apache/arrow`](https://github.com/apache/arrow) for Parquet magic‑byte detection.

## Quick‑Start

```bash
# 1. Build binaries & images
./scripts/build.sh

# 2. Launch stack
./scripts/up.sh

# 3. Upload sample data
./bin/batch model create default_model ./schemas/default.json
./bin/batch job create default_model ./samples/api_data.csv
./bin/batch job list
```

Visit <http://localhost:8080> for the Redpanda Console.

## Cleaning Up

```bash
./scripts/down.sh           # stop containers
./scripts/down.sh --clean   # stop + delete volumes
```

## File Layout

```
cmd/
  server/        -- ingest API service
  cli/           -- CLI entry‑point
internal/        -- shared helpers
samples/         -- example data
scripts/         -- lifecycle & test scripts
Dockerfile.server
Dockerfile.cli
docker-compose.yml
go.mod
```

## Build Matrix

| Component | Language | Binary | Image |
|-----------|----------|--------|-------|
| HTTP API  | Go 1.22  | `ingest-api` | `batch-ingest-api:latest` |
| CLI       | Go 1.22  | `batch`      | `batch-cli:latest` |

## Design Highlights

See **DESIGN.md** for end‑to‑end functional, non‑functional, and engineering design details including REST, Kafka topic contracts, and error taxonomy.