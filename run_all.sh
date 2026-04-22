#!/bin/bash

set -e

echo "====================================================="
echo "Running Complete Data Engineering Pipeline"
echo "====================================================="
# Skip individual tests in task groups (will run once at the end)
export SKIP_TESTS=true
echo "\n[1/3] Running Task Group 1: Ingestion & Analysis"

sh task_group1_run_pipeline.sh

echo "\n[2/3] Running Task Group 2: Transformations"

sh task_group2_run_pipeline.sh

echo "\n[3/3] Running Task Group 3: Quality Checks"

sh task_group3_run_pipeline.sh

echo "\n====================================================="
echo "Running comprehensive test suite"
echo "====================================================="

pytest -v

echo "\n====================================================="
echo "✅ Pipeline completed successfully"
echo "====================================================="