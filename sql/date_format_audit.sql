-- =========================================================
-- AUDIT QUERY: Normalized Date Load Quality
-- PURPOSE: Inspect parsed date completeness after ingestion.
-- NOTE: Original source formats are audited in src/quality_checks.py
--       by reading jobs.csv and applications.csv directly.
-- =========================================================

SELECT
    'raw_jobs' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(posted_date) AS parsed_dates,
    COUNT(*) - COUNT(posted_date) AS null_dates_after_parse,
    MIN(posted_date) AS min_date,
    MAX(posted_date) AS max_date
FROM raw.raw_jobs

UNION ALL

SELECT
    'raw_applications' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(apply_date) AS parsed_dates,
    COUNT(*) - COUNT(apply_date) AS null_dates_after_parse,
    MIN(apply_date) AS min_date,
    MAX(apply_date) AS max_date
FROM raw.raw_applications

UNION ALL

SELECT
    'raw_workflow_events' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(event_timestamp) AS parsed_dates,
    COUNT(*) - COUNT(event_timestamp) AS null_dates_after_parse,
    MIN(event_timestamp)::date AS min_date,
    MAX(event_timestamp)::date AS max_date
FROM raw.raw_workflow_events;
