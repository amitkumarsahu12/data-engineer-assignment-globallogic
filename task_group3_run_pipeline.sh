#!/bin/bash
source .env

set -e

echo "\nStarting Task Group 3 quality checks..."

echo "\nStep 1 — Running quality checks"

python src/quality_checks.py

# Only run tests if not called from run_all.sh
if [ "$SKIP_TESTS" != "true" ]; then
  echo "\nStep 2 — Running tests for quality checks"
  pytest tests/test_quality_checks.py -v
fi

echo "\nTask Group 3 completed successfully."