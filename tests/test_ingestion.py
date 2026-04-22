import psycopg2
import pytest

from src.ingestion import run_ingestion
from src.config import DB_CONFIG


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@pytest.fixture(scope="module")
def ingestion_run_once():
    """Run ingestion once for all tests in this module."""
    run_ingestion()
    yield
    # Cleanup: truncate tables after all tests
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw.raw_applications CASCADE")
    cursor.execute("TRUNCATE TABLE raw.raw_workflow_events CASCADE")
    cursor.execute("TRUNCATE TABLE raw.raw_education CASCADE")
    cursor.execute("TRUNCATE TABLE raw.raw_candidates CASCADE")
    cursor.execute("TRUNCATE TABLE raw.raw_jobs CASCADE")
    conn.commit()
    conn.close()


def test_ingestion_runs_without_error(ingestion_run_once):
    """Verify ingestion completes without errors."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_jobs")
    job_count = cursor.fetchone()[0]
    
    conn.close()
    
    assert job_count > 0, "Jobs table should have data after ingestion"


def test_ingestion_loads_jobs(ingestion_run_once):
    """Verify jobs were loaded correctly."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_jobs")
    count = cursor.fetchone()[0]
    
    cursor.execute(
        """
        SELECT COUNT(*) FROM raw.raw_jobs 
        WHERE job_id IS NULL OR title IS NULL
        """
    )
    null_count = cursor.fetchone()[0]
    
    conn.close()
    
    assert count > 0, "Jobs should be loaded"
    assert null_count == 0, "Jobs should have required fields"


def test_ingestion_loads_candidates(ingestion_run_once):
    """Verify candidates were loaded correctly."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_candidates")
    count = cursor.fetchone()[0]
    
    cursor.execute(
        """
        SELECT COUNT(*) FROM raw.raw_candidates
        WHERE candidate_id IS NULL OR first_name IS NULL OR last_name IS NULL
        """
    )
    null_count = cursor.fetchone()[0]
    
    conn.close()
    
    assert count > 0, "Candidates should be loaded"
    assert null_count == 0, "Candidates should have required fields"


def test_ingestion_loads_applications(ingestion_run_once):
    """Verify applications were loaded correctly."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_applications")
    count = cursor.fetchone()[0]
    
    cursor.execute(
        """
        SELECT COUNT(*) FROM raw.raw_applications
        WHERE application_id IS NULL OR job_id IS NULL OR candidate_id IS NULL
        """
    )
    null_count = cursor.fetchone()[0]
    
    conn.close()
    
    assert count > 0, "Applications should be loaded"
    assert null_count == 0, "Applications should have required fields"


def test_ingestion_loads_workflow_events(ingestion_run_once):
    """Verify workflow events were loaded correctly."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_workflow_events")
    count = cursor.fetchone()[0]
    
    cursor.execute(
        """
        SELECT COUNT(*) FROM raw.raw_workflow_events
        WHERE application_id IS NULL OR new_status IS NULL
        """
    )
    null_count = cursor.fetchone()[0]
    
    conn.close()
    
    assert count > 0, "Workflow events should be loaded"
    assert null_count == 0, "Workflow events should have required fields"


def test_ingestion_is_idempotent(ingestion_run_once):
    """Verify ingestion is idempotent (second run doesn't create duplicates)."""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get counts before second run
    cursor.execute("SELECT COUNT(*) FROM raw.raw_jobs")
    jobs_before = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_applications")
    apps_before = cursor.fetchone()[0]
    
    # Run ingestion again (second time)
    run_ingestion()
    
    # Get counts after second run
    cursor.execute("SELECT COUNT(*) FROM raw.raw_jobs")
    jobs_after = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM raw.raw_applications")
    apps_after = cursor.fetchone()[0]
    
    conn.close()
    
    # Counts should be the same (idempotent)
    assert jobs_before == jobs_after, "Second ingestion created duplicate jobs"
    assert apps_before == apps_after, "Second ingestion created duplicate applications"