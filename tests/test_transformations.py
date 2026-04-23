import psycopg2

from src.config import DB_CONFIG


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def execute_sql_file(conn, filepath):
    """Execute SQL file and commit."""
    with open(filepath, 'r') as f:
        sql = f.read()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()


def test_dim_job_transformation():
    """Verify dim_job SQL model creates and populates the jobs dimension correctly."""

    conn = get_connection()

    # Execute dim_job transformation
    execute_sql_file(conn, "models/dim_job.sql")

    cursor = conn.cursor()

    # Verify dim_job table was created
    cursor.execute(
        """
        SELECT COUNT(*) FROM warehouse.dim_job
        """
    )
    job_count = cursor.fetchone()[0]

    assert job_count > 0, "No jobs were loaded into warehouse.dim_job"

    # Verify required columns are populated
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.dim_job
        WHERE job_id IS NULL OR title IS NULL
        """
    )
    null_count = cursor.fetchone()[0]

    assert null_count == 0, f"Found {null_count} jobs with null required fields"

    conn.close()


def test_dim_candidate_transformation():
    """Verify dim_candidate SQL model creates and populates candidate dimension with education."""

    conn = get_connection()

    # Execute dim_candidate transformation
    execute_sql_file(conn, "models/dim_candidate.sql")

    cursor = conn.cursor()

    # Verify dim_candidate table was created
    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_candidate")
    candidate_count = cursor.fetchone()[0]

    assert candidate_count > 0, "No candidates were loaded into warehouse.dim_candidate"

    # Verify required columns are populated
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.dim_candidate
        WHERE candidate_id IS NULL OR first_name IS NULL OR last_name IS NULL
        """
    )
    null_count = cursor.fetchone()[0]

    assert null_count == 0, f"Found {null_count} candidates with null required fields"

    # Verify education is aggregated as JSONB
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.dim_candidate
        WHERE education IS NOT NULL
        """
    )
    education_count = cursor.fetchone()[0]

    assert education_count > 0, "Education data should be populated as JSONB"

    conn.close()


def test_fct_applications_transformation():
    """Verify fct_applications SQL model populates fact table with hire status."""

    conn = get_connection()

    # Execute fct_applications transformation
    execute_sql_file(conn, "models/fct_applications.sql")

    cursor = conn.cursor()

    # Verify fct_applications table was created
    cursor.execute("SELECT COUNT(*) FROM warehouse.fct_applications")
    app_count = cursor.fetchone()[0]

    assert app_count > 0, "No applications were loaded into warehouse.fct_applications"

    # Verify required columns are populated
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.fct_applications
        WHERE application_id IS NULL OR job_id IS NULL OR candidate_id IS NULL
        """
    )
    null_count = cursor.fetchone()[0]

    assert null_count == 0, f"Found {null_count} applications with null required fields"

    # Verify is_hired flag is correctly set (boolean logic)
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.fct_applications
        WHERE is_hired = TRUE
        """
    )
    hired_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COUNT(DISTINCT is_hired)
        FROM warehouse.fct_applications
        """
    )
    distinct_values = cursor.fetchone()[0]

    assert distinct_values <= 2, "is_hired should only have TRUE/FALSE values"

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.fct_applications
        WHERE COALESCE(is_hired_before_applied_anomaly, FALSE) = TRUE
        """
    )
    anomaly_count = cursor.fetchone()[0]

    assert anomaly_count >= 1, "Expected at least one hired-before-applied anomaly to be flagged"

    conn.close()


def test_fct_applications_idempotent():
    """Verify SQL transformation is idempotent (ON CONFLICT DO NOTHING/UPDATE)."""

    conn = get_connection()

    # First execution
    execute_sql_file(conn, "models/fct_applications.sql")

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM warehouse.fct_applications")
    count_first = cursor.fetchone()[0]

    # Second execution (should be idempotent due to ON CONFLICT)
    execute_sql_file(conn, "models/fct_applications.sql")

    cursor.execute("SELECT COUNT(*) FROM warehouse.fct_applications")
    count_second = cursor.fetchone()[0]

    conn.close()

    assert count_first == count_second, "Transformation is not idempotent - created duplicates"


def test_fct_workflow_events_transformation():
    """Verify fct_workflow_events SQL model creates event history fact table."""

    conn = get_connection()

    # Execute fct_workflow_events transformation
    execute_sql_file(conn, "models/fct_workflow_events.sql")

    cursor = conn.cursor()

    # Verify fct_workflow_events table was created
    cursor.execute("SELECT COUNT(*) FROM warehouse.fct_workflow_events")
    event_count = cursor.fetchone()[0]

    assert event_count > 0, "No events were loaded into warehouse.fct_workflow_events"

    # Verify required columns
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM warehouse.fct_workflow_events
        WHERE application_id IS NULL OR new_status IS NULL OR event_timestamp IS NULL
        """
    )
    null_count = cursor.fetchone()[0]

    assert null_count == 0, f"Found {null_count} events with null required fields"

    conn.close()
