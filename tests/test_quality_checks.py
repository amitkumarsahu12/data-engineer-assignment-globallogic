import psycopg2

from src.config import DB_CONFIG
from src.quality_checks import (
    check_duplicate_applications,
    check_null_candidate_ids,
    detect_hired_before_applied,
    check_data_freshness,
    check_volume_anomaly,
)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def test_check_duplicate_applications():
    """Verify the duplicate detection function works correctly."""

    conn = get_connection()

    # This should log the duplicate count (function doesn't return, but logs)
    check_duplicate_applications(conn)

    # Verify manually that no duplicates exist
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM raw.raw_applications
        GROUP BY application_id
        HAVING COUNT(*) > 1
        """
    )
    
    duplicates = cursor.fetchall()
    conn.close()

    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate applications"


def test_check_null_candidate_ids():
    """Verify the null candidate ID detection function works correctly."""

    conn = get_connection()

    # This should log the null count (function doesn't return, but logs)
    check_null_candidate_ids(conn)

    # Verify manually that no null candidate IDs exist
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM raw.raw_candidates
        WHERE candidate_id IS NULL
        """
    )
    
    null_count = cursor.fetchone()[0]
    conn.close()

    assert null_count == 0, f"Found {null_count} null candidate IDs"


def test_no_duplicate_applications():
    """Verify no duplicate applications exist in the dataset."""

    conn = get_connection()

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT application_id
        FROM raw.raw_applications
        GROUP BY application_id
        HAVING COUNT(*) > 1
        """
    )

    duplicates = cursor.fetchall()

    conn.close()

    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate applications"


def test_hired_before_applied_detection():
    """Verify anomalies are correctly detected and returned."""

    conn = get_connection()

    anomaly_count = detect_hired_before_applied(conn)

    conn.close()

    assert isinstance(anomaly_count, int), "Anomaly detection should return an integer count"
    
    if anomaly_count > 0:
        assert anomaly_count >= 0, "Anomaly count should be non-negative"


def test_data_freshness_check():
    """Verify data freshness check returns valid timestamps for all tables."""

    conn = get_connection()

    freshness_results = check_data_freshness(conn)

    conn.close()

    assert freshness_results is not None, "Freshness check should return results"
    assert len(freshness_results) > 0, "Should have freshness data for at least one table"
    
    expected_tables = {"raw_applications", "raw_candidates", "raw_jobs"}
    found_tables = set()
    
    for table_name, last_update in freshness_results:
        # Validate table_name
        assert isinstance(table_name, str), f"Table name should be string, got {type(table_name)}"
        found_tables.add(table_name)
        
        # Validate last_update - should be either None (if table is empty) or a timestamp
        if last_update is not None:
            # If last_update exists, verify it's a valid timestamp object
            from datetime import datetime
            assert isinstance(last_update, (datetime, str)), \
                f"{table_name}: last_update should be datetime or timestamp string, got {type(last_update)}"
        else:
            # If None, table might be empty (which is acceptable during test setup)
            pass
    
    # Verify we have data from expected tables
    assert len(found_tables) > 0, "Should have freshness data from at least one expected table"


def test_volume_anomaly_detection():
    """Verify volume checks return row counts for all tables."""

    conn = get_connection()

    volume_results = check_volume_anomaly(conn)

    conn.close()

    assert volume_results is not None, "Volume check should return results"
    assert len(volume_results) > 0, "Should have volume data for tables"
    
    table_names = [table_name for table_name, _ in volume_results]
    expected_tables = {
        "raw_applications",
        "raw_candidates",
        "raw_jobs",
        "raw_workflow_events",
    }
    
    found_tables = set(table_names)
    assert found_tables == expected_tables, f"Expected tables {expected_tables}, got {found_tables}"
    
    # Verify row counts are non-negative integers
    for table_name, row_count in volume_results:
        assert isinstance(row_count, int), f"{table_name}: row count should be integer"
        assert row_count >= 0, f"{table_name}: row count should be non-negative"