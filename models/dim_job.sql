-- =========================================================
-- MODEL: dim_job
-- PURPOSE: Job dimension table
-- GRAIN: One record per job_id
-- IDEMPOTENT: Yes
-- =========================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_job (

    job_id TEXT PRIMARY KEY,

    title TEXT,

    department TEXT,

    posted_date DATE,

    status TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- =========================================================
-- LOAD / UPSERT
-- =========================================================

INSERT INTO warehouse.dim_job (

    job_id,
    title,
    department,
    posted_date,
    status

)

SELECT DISTINCT

    job_id,
    title,
    COALESCE(NULLIF(TRIM(department), ''), 'Unknown') AS department,
    posted_date,
    status

FROM raw.raw_jobs

WHERE job_id IS NOT NULL

ON CONFLICT (job_id)
DO UPDATE SET

    title = EXCLUDED.title,
    department = EXCLUDED.department,
    posted_date = EXCLUDED.posted_date,
    status = EXCLUDED.status,
    updated_at = CURRENT_TIMESTAMP;