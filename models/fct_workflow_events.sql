-- =========================================================
-- MODEL: fct_workflow_events
-- PURPOSE: Workflow event history
-- GRAIN: One record per event
-- IDEMPOTENT: Yes
-- =========================================================

CREATE TABLE IF NOT EXISTS warehouse.fct_workflow_events (

    event_id BIGSERIAL PRIMARY KEY,

    application_id TEXT NOT NULL,

    old_status TEXT,

    new_status TEXT,

    event_timestamp TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE INDEX IF NOT EXISTS idx_fct_events_application
ON warehouse.fct_workflow_events (application_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fct_events_unique
ON warehouse.fct_workflow_events (

    application_id,
    new_status,
    event_timestamp

);

-- =========================================================
-- CLEANUP
-- =========================================================

DELETE FROM warehouse.fct_workflow_events
WHERE application_id IS NULL
OR new_status IS NULL
OR event_timestamp IS NULL;

-- =========================================================
-- LOAD / UPSERT
-- =========================================================

INSERT INTO warehouse.fct_workflow_events (

    application_id,
    old_status,
    new_status,
    event_timestamp

)

SELECT

    application_id,
    old_status,
    new_status,
    event_timestamp

FROM raw.raw_workflow_events

WHERE application_id IS NOT NULL
AND new_status IS NOT NULL
AND event_timestamp IS NOT NULL

ON CONFLICT (
    application_id,
    new_status,
    event_timestamp
)
DO NOTHING;
