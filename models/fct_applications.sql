-- =========================================================
-- MODEL: fct_applications
-- PURPOSE: Application lifecycle fact table
-- GRAIN: One record per application_id
-- IDEMPOTENT: Yes
-- =========================================================

CREATE TABLE IF NOT EXISTS warehouse.fct_applications (

    application_id TEXT PRIMARY KEY,

    job_id TEXT NOT NULL,

    candidate_id TEXT NOT NULL,

    apply_date DATE,

    hired_date TIMESTAMP,

    current_status TEXT,

    is_hired BOOLEAN,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE INDEX IF NOT EXISTS idx_fct_app_job
ON warehouse.fct_applications (job_id);

CREATE INDEX IF NOT EXISTS idx_fct_app_candidate
ON warehouse.fct_applications (candidate_id);

-- =========================================================
-- LOAD / UPSERT
-- =========================================================

INSERT INTO warehouse.fct_applications (

    application_id,
    job_id,
    candidate_id,
    apply_date,
    hired_date,
    current_status,
    is_hired

)

SELECT

    a.application_id,
    a.job_id,
    a.candidate_id,
    a.apply_date,

    MAX(
        CASE
            WHEN w.new_status = 'Hired'
            THEN w.event_timestamp
        END
    ) AS hired_date,

    MAX(w.new_status) AS current_status,

    CASE
        WHEN MAX(
            CASE
                WHEN w.new_status = 'Hired'
                THEN 1
                ELSE 0
            END
        ) = 1
        THEN TRUE
        ELSE FALSE
    END AS is_hired

FROM raw.raw_applications a

LEFT OUTER JOIN raw.raw_workflow_events w
ON a.application_id = w.application_id

WHERE a.application_id IS NOT NULL

GROUP BY

    a.application_id,
    a.job_id,
    a.candidate_id,
    a.apply_date

ON CONFLICT (application_id)
DO UPDATE SET

    hired_date = EXCLUDED.hired_date,
    current_status = EXCLUDED.current_status,
    is_hired = EXCLUDED.is_hired,
    updated_at = CURRENT_TIMESTAMP;