import logging
import psycopg2

from config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# CHECK 1: DUPLICATES
def check_duplicate_applications(conn):

    logger.info("Checking duplicate applications")

    query = """
    SELECT application_id,
           COUNT(*) AS cnt
    FROM raw.raw_applications
    GROUP BY application_id
    HAVING COUNT(*) > 1
    """

    cursor = conn.cursor()
    cursor.execute(query)

    results = cursor.fetchall()

    logger.info(f"Duplicate records found: {len(results)}")


# CHECK 2: NULL VALUES
def check_null_candidate_ids(conn):

    logger.info("Checking null candidate IDs")

    query = """
    SELECT COUNT(*)
    FROM raw.raw_candidates
    WHERE candidate_id IS NULL
    """

    cursor = conn.cursor()
    cursor.execute(query)

    count = cursor.fetchone()[0]

    logger.info(f"Null candidate IDs: {count}")


# CHECK 3: ANOMALY DETECTION
def detect_hired_before_applied(conn):

    logger.info("Detecting anomaly: Hired before Applied")

    query = """
    SELECT
        a.application_id,
        a.apply_date,
        w.event_timestamp

    FROM raw.raw_applications a

    INNER JOIN raw.raw_workflow_events w
    ON a.application_id = w.application_id

    WHERE w.new_status = 'Hired'
    AND w.event_timestamp < a.apply_date
    """

    cursor = conn.cursor()
    cursor.execute(query)

    anomalies = cursor.fetchall()

    logger.info(f"Anomalies detected: {len(anomalies)}")
    
    return len(anomalies)


# CHECK 4: DATA FRESHNESS
def check_data_freshness(conn):

    logger.info("Checking data freshness")

    query = """
    SELECT
        'raw_applications' AS table_name,
        MAX(updated_at) AS last_update
    FROM raw.raw_applications

    UNION ALL

    SELECT
        'raw_candidates' AS table_name,
        MAX(updated_at) AS last_update
    FROM raw.raw_candidates

    UNION ALL

    SELECT
        'raw_jobs' AS table_name,
        MAX(updated_at) AS last_update
    FROM raw.raw_jobs
    """

    cursor = conn.cursor()
    cursor.execute(query)

    results = cursor.fetchall()

    for table_name, last_update in results:
        if last_update is None:
            logger.warning(f"{table_name}: No data present")
        else:
            logger.info(f"{table_name}: Last updated at {last_update}")

    return results


# CHECK 5: VOLUME ANOMALY DETECTION
def check_volume_anomaly(conn):

    logger.info("Checking for volume anomalies")

    query = """
    SELECT
        'raw_applications' AS table_name,
        COUNT(*) AS row_count
    FROM raw.raw_applications

    UNION ALL

    SELECT
        'raw_candidates' AS table_name,
        COUNT(*) AS row_count
    FROM raw.raw_candidates

    UNION ALL

    SELECT
        'raw_jobs' AS table_name,
        COUNT(*) AS row_count
    FROM raw.raw_jobs

    UNION ALL

    SELECT
        'raw_workflow_events' AS table_name,
        COUNT(*) AS row_count
    FROM raw.raw_workflow_events
    """

    cursor = conn.cursor()
    cursor.execute(query)

    results = cursor.fetchall()

    for table_name, row_count in results:
        logger.info(f"{table_name}: {row_count} rows")
        
        # Flag as anomaly if empty
        if row_count == 0:
            logger.warning(f"{table_name}: No rows found - possible data loss")

    return results


# RUN ALL CHECKS
def run_quality_checks():

    logger.info("Running data quality checks")

    conn = get_connection()

    try:

        check_duplicate_applications(conn)
        check_null_candidate_ids(conn)
        detect_hired_before_applied(conn)
        check_data_freshness(conn)
        check_volume_anomaly(conn)

    finally:

        conn.close()


if __name__ == "__main__":
    run_quality_checks()