# Data Engineer Take-Home Assignment

## Overview

This repository implements an end-to-end data pipeline for a recruiting dataset. The solution covers:

- Raw data ingestion into PostgreSQL
- Warehouse-style SQL transformations
- Data quality checks
- Business analysis queries
- Unit and integration-style tests

The implementation is designed to satisfy all three task groups from the assignment with a simple local setup based on Python, PostgreSQL, Docker, SQL, and Pytest.

## How To Run

### Prerequisites

- Docker / Docker Compose
- Python 3
- `psql` client

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Set PostgreSQL Password

The shell scripts expect `PGPASSWORD` to be set. The repo already includes a local `.env` file for this.

If needed, export it manually:

```bash
export PGPASSWORD=postgres
```

### Run The Full Pipeline

```bash
bash run_all.sh
```

This will:

1. Start PostgreSQL in Docker
2. Create raw tables and indexes
3. Run ingestion
4. Run analysis queries
5. Build warehouse models
6. Run quality checks
7. Run the full test suite

### Run Task Groups Separately

```bash
bash task_group1_run_pipeline.sh
bash task_group2_run_pipeline.sh
bash task_group3_run_pipeline.sh
```

### Run Tests Only

```bash
pytest -v
```

## Architecture

The pipeline follows a layered warehouse-style design:

`source files -> raw PostgreSQL tables -> warehouse models -> quality checks -> metrics`

### Raw Layer

The raw layer stores data from:

- `jobs.csv`
- `candidates.json`
- `education.csv`
- `applications.csv`
- `workflow_events.jsonl`

These files are ingested into `raw.*` tables through [`src/ingestion.py`](/Users/amitkumarsahu/VSCode/data-engineer-assignment-globallogic-amitkumarsahu/src/ingestion.py).

### Warehouse Layer

The warehouse layer is built with SQL scripts in [`models/`](/Users/amitkumarsahu/VSCode/data-engineer-assignment-globallogic-amitkumarsahu/models):

- `dim_job`
- `dim_candidate`
- `fct_applications`
- `fct_workflow_events`

This keeps the transformation logic explicit, easy to review, and easy to rerun locally.

### Quality Layer

Quality checks are implemented in [`src/quality_checks.py`](/Users/amitkumarsahu/VSCode/data-engineer-assignment-globallogic-amitkumarsahu/src/quality_checks.py) and cover:

- Duplicate applications
- Null candidate IDs
- Data freshness
- Volume anomalies
- Empty departments
- Mixed source date formats
- `Hired before Applied` anomalies

## Trade-Offs And Design Decisions

### Database Choice

PostgreSQL was chosen because it supports:

- Primary keys and foreign keys
- `ON CONFLICT` upsert patterns
- JSONB for semi-structured candidate data
- SQL transformations and indexing in a single local environment

This was a practical choice for a take-home assignment because it keeps ingestion, transformation, and validation in one place.

### Transformation Approach

I used plain SQL model files instead of dbt or Spark.

Why:

- Lower setup overhead for a local assignment
- Easy to inspect and explain
- Good fit for the data volume in this repo
- Still supports idempotent warehouse rebuilds

The trade-off is that dbt would provide stronger dependency management, documentation, and testing conventions for a larger production-grade project.

### Idempotency Strategy

Idempotency is handled through:

- Primary keys on raw and warehouse entities
- `ON CONFLICT DO UPDATE` / `DO NOTHING`
- Unique indexes for workflow events
- Deterministic SQL transformations

This allows repeated pipeline runs without duplicating business entities or event rows.

### Data Quality Strategy

The solution favors preserving source truth and flagging anomalies instead of silently correcting them.

Examples:

- Mixed date formats are parsed during ingestion and audited separately
- Empty job departments are mapped to `Unknown` in the dimension layer
- `Hired before Applied` rows are flagged in the warehouse and excluded from time-to-hire metrics

This keeps the raw layer auditable while protecting downstream reporting.

## Answers To Written Questions

### How Mixed Date Formats Are Handled

Source files contain multiple date formats in `jobs.csv`, `applications.csv`, and workflow events. The ingestion layer uses a custom parser that supports:

- `YYYY-MM-DD`
- `YYYY/MM/DD`
- `YYYY.MM.DD`
- `DD-Mon-YYYY`
- `Month DD, YYYY`
- `Mon DD, YYYY`
- `DD-MM-YYYY`
- ISO timestamps such as `2025-11-08T00:00:00`

If parsing fails, the value is logged and loaded as `NULL` instead of crashing the pipeline.

### How The Hired-Before-Applied Anomaly Is Handled

The dataset contains at least one case where a `Hired` workflow event occurs before the application date.

Handling strategy:

- Keep the raw record unchanged
- Flag the row in `warehouse.fct_applications` with `is_hired_before_applied_anomaly = TRUE`
- Exclude flagged rows from the time-to-hire metric

I chose flagging instead of deletion or auto-correction because it preserves auditability and avoids inventing data.

## Performance And Scaling

For the current assignment-sized dataset, the local Python + PostgreSQL approach is appropriate.

If `workflow_events.jsonl` grew to 10TB, I would change the design as follows:

1. Use distributed processing such as Spark for parsing and batch ingestion.
2. Partition workflow events by event date.
3. Load incrementally instead of reprocessing the full history every run.
4. Use columnar intermediate storage such as Parquet for cheaper storage and faster scans.
5. Write into PostgreSQL through staging tables or move analytical serving to a warehouse/lakehouse engine better suited to that scale.

The current design optimizes for clarity and correctness. A 10TB design would optimize for partitioning, parallelism, fault tolerance, and incremental state management.

## Project Structure

```text
data/
models/
sql/
src/
tests/
docker-compose.yml
requirements.txt
run_all.sh
task_group1_run_pipeline.sh
task_group2_run_pipeline.sh
task_group3_run_pipeline.sh
```

## AI Statement

AI tools were used as a productivity aid for drafting boilerplate, exploring edge cases, and speeding up debugging. All generated ideas and code were reviewed, tested, and adjusted manually before being kept. In particular, AI assistance was useful for tightening parser behavior, refining data-quality handling, and improving repo documentation, while final design choices and validation were done through direct inspection and pipeline/test execution.
