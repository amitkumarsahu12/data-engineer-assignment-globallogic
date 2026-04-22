#!/bin/bash
source .env

set -e

echo "\nStarting Task Group 2 transformation..."

echo "\nStep 1 — Creating dimension and fact tables"

psql -h localhost -U postgres -d de_assignment -f models/dim_job.sql

psql -h localhost -U postgres -d de_assignment -f models/dim_candidate.sql

psql -h localhost -U postgres -d de_assignment -f models/fct_workflow_events.sql

psql -h localhost -U postgres -d de_assignment -f models/fct_applications.sql

echo "\nStep 2 — Getting the Metrics Calculation"

psql -h localhost -U postgres -d de_assignment -f sql/metrics.sql

# Only run tests if not called from run_all.sh
if [ "$SKIP_TESTS" != "true" ]; then
  echo "\nStep 3 — Running tests for transformations"
  pytest tests/test_transformations.py -v
fi

echo "\nTask Group 2 completed successfully."