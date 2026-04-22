#!/bin/bash
source .env

set -e

echo "\nStarting Task Group 1 pipeline..."

echo "\nStep 1 — Start PostgreSQL container"

docker compose up -d

echo "\nWaiting for database to be ready..."

sleep 5

echo "\nStep 2 — Create schema"

psql -h localhost -U postgres -d de_assignment -f sql/schema.sql

echo "\nStep 3 — Run ingestion"

python src/ingestion.py

echo "\nStep 4 — Run analysis queries"

psql -h localhost -U postgres -d de_assignment -f sql/analysis_queries.sql

# Only run tests if not called from run_all.sh
if [ "$SKIP_TESTS" != "true" ]; then
  echo "\nStep 5 - Run tests for ingestion"
  pytest tests/test_ingestion.py -v
fi

echo "\nTask Group 1 completed successfully."