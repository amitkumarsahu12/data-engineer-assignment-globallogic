-- -------------------------------
-- Create schema
-- -------------------------------

CREATE SCHEMA IF NOT EXISTS raw;

-- =========================================================
-- RAW TABLE: JOBS
-- =========================================================

CREATE TABLE IF NOT EXISTS raw.raw_jobs (

    job_id TEXT PRIMARY KEY,

    title TEXT,

    department TEXT,

    posted_date DATE,

    status TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE INDEX IF NOT EXISTS idx_jobs_department
ON raw.raw_jobs (department);

CREATE INDEX IF NOT EXISTS idx_jobs_status
ON raw.raw_jobs (status);

-- =========================================================
-- RAW TABLE: CANDIDATES
-- =========================================================

CREATE TABLE IF NOT EXISTS raw.raw_candidates (

    candidate_id TEXT PRIMARY KEY,

    first_name TEXT,

    last_name TEXT,

    email TEXT,

    phone TEXT,

    skills JSONB,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE INDEX IF NOT EXISTS idx_candidates_email
ON raw.raw_candidates (email);

-- =========================================================
-- RAW TABLE: EDUCATION
-- =========================================================

CREATE TABLE IF NOT EXISTS raw.raw_education (

    education_id SERIAL PRIMARY KEY,

    candidate_id TEXT NOT NULL,

    degree TEXT,

    institution TEXT,

    graduation_year INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_education_candidate
    FOREIGN KEY (candidate_id)
    REFERENCES raw.raw_candidates(candidate_id)

);

CREATE INDEX IF NOT EXISTS idx_education_candidate
ON raw.raw_education (candidate_id);

-- =========================================================
-- RAW TABLE: APPLICATIONS
-- =========================================================

CREATE TABLE IF NOT EXISTS raw.raw_applications (

    application_id TEXT PRIMARY KEY,

    job_id TEXT NOT NULL,

    candidate_id TEXT NOT NULL,

    apply_date DATE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_application_job
    FOREIGN KEY (job_id)
    REFERENCES raw.raw_jobs(job_id),

    CONSTRAINT fk_application_candidate
    FOREIGN KEY (candidate_id)
    REFERENCES raw.raw_candidates(candidate_id)

);

CREATE INDEX IF NOT EXISTS idx_applications_job
ON raw.raw_applications (job_id);

CREATE INDEX IF NOT EXISTS idx_applications_candidate
ON raw.raw_applications (candidate_id);

-- =========================================================
-- RAW TABLE: WORKFLOW EVENTS
-- =========================================================

CREATE TABLE IF NOT EXISTS raw.raw_workflow_events (

    event_id BIGSERIAL PRIMARY KEY,

    application_id TEXT NOT NULL,

    old_status TEXT,

    new_status TEXT,

    event_timestamp TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_event_application
    FOREIGN KEY (application_id)
    REFERENCES raw.raw_applications(application_id)

);

CREATE INDEX IF NOT EXISTS idx_events_application
ON raw.raw_workflow_events (application_id);

CREATE INDEX IF NOT EXISTS idx_events_timestamp
ON raw.raw_workflow_events (event_timestamp);

-- =========================================================
-- DATA QUALITY / UNIQUENESS SAFETY
-- =========================================================

-- Prevent duplicate workflow events

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_unique
ON raw.raw_workflow_events (
    application_id,
    new_status,
    event_timestamp
);