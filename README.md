# Data Engineer Take-Home Assignment

## Overview

This project implements an end-to-end data engineering pipeline that ingests raw datasets, transforms them into a star schema, performs data quality validation, and calculates business metrics.

The solution emphasizes:

- Idempotent pipelines
- Data quality validation
- Reproducibility
- Scalability considerations
- Clean modular architecture

---

## Architecture

Raw Files
   ↓
Python Ingestion
   ↓
PostgreSQL (Raw Layer)
   ↓
SQL Transformations
   ↓
Star Schema (Warehouse Layer)
   ↓
Data Quality Checks
   ↓
Business Metrics

---

## Technology Stack

Python  
PostgreSQL  
Docker  
SQL  
Pytest  

---

## Why PostgreSQL

PostgreSQL was selected because:

- ACID-compliant
- Supports UPSERT
- Production-ready
- Widely used in data platforms
- Supports indexing and constraints

---

## Idempotency Strategy

All ingestion and transformation operations are designed to be idempotent.

Implemented using:

- Primary keys
- UPSERT logic
- ON CONFLICT handling
- Deterministic transformations

This ensures pipelines can be safely re-run without duplicating data.

---

## Data Quality Framework

Automated checks implemented:

- Duplicate detection
- Null value validation
- Referential integrity validation
- Anomaly detection

---

## Anomaly Detection

The dataset contains cases where:

Hired event occurs before Apply event.

Strategy implemented:

Records are flagged and logged rather than deleted to preserve auditability.

---

## Performance and Scaling Strategy

If workflow_events grows to 10TB:

The pipeline would be modified to:

- Use distributed processing (Spark)
- Partition data by date
- Use columnar storage
- Perform incremental ingestion
- Parallelize reads and writes

---

## Project Structure
data-engineer-assignment/

docker/
docker-compose.yml

sql/
schema.sql
analysis_queries.sql

src/
ingestion.py
transformations.py
quality_checks.py
config.py

tests/
test_ingestion.py
test_quality.py

README.md
requirements.txt


---

## Setup Instructions

### Step 1 — Start Database
docker compose up -d


---

### Step 2 — Install Dependencies
pip install -r requirements.txt


---

### Step 3 — Create Tables
psql -h localhost -U postgres -d de_assignment -f sql/schema.sql

Password:
postgres


---

### Step 4 — Run Ingestion
python src/ingestion.py


---

### Step 5 — Run Transformations
python src/transformations.py


---

### Step 6 — Run Data Quality Checks
python src/quality_checks.py


---

### Step 7 — Run Tests
pytest


---

## SQL Analysis Questions

### How many jobs are currently open?
SELECT COUNT(*)
FROM raw.raw_jobs
WHERE status = 'OPEN';


---

### Top 5 departments by applications
SELECT department,
COUNT(application_id)
FROM raw.raw_jobs
JOIN raw.raw_applications
USING (job_id)
GROUP BY department
ORDER BY COUNT DESC
LIMIT 5;


---

### Candidates who applied to more than 3 jobs
SELECT candidate_id
FROM raw.raw_applications
GROUP BY candidate_id
HAVING COUNT(*) > 3;


---

## Testing Strategy

Unit tests validate:

- Idempotent ingestion
- Data integrity
- Duplicate detection
- Anomaly detection

---

## AI Usage Statement

AI tools were used to assist with:

- Boilerplate code generation
- Syntax validation
- Architectural validation

All implementation logic was reviewed and verified manually.