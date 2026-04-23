import logging
import psycopg2

from config import DB_CONFIG, JOBS_FILE, APPLICATIONS_FILE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


DATE_FORMAT_PATTERNS = {
    'YYYY-MM-DD (ISO standard)': r'^\d{4}-\d{2}-\d{2}$',
    'YYYY/MM/DD (slash)': r'^\d{4}/\d{2}/\d{2}$',
    'YYYY.MM.DD (dot)': r'^\d{4}\.\d{2}\.\d{2}$',
    'DD-Mon-YYYY (abbreviated month)': r'^\d{2}-[A-Za-z]{3}-\d{4}$',
    'Month DD, YYYY (full month)': r'^[A-Za-z]+ \d{1,2}, \d{4}$',
    'Mon DD, YYYY (abbreviated month)': r'^[A-Za-z]{3} \d{1,2}, \d{4}$',
    'DD-MM-YYYY': r'^\d{2}-\d{2}-\d{4}$',
    'ISO timestamp': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
}


def summarize_date_formats(csv_path, column_name, dataset_name):
    """Analyze a CSV source column and return counts by raw date format."""
    import csv
    import re

    logger.info(f"Checking date format consistency in {dataset_name}")

    format_summary = {fmt: 0 for fmt in DATE_FORMAT_PATTERNS.keys()}
    format_summary['UNMATCHED'] = 0

    with open(csv_path, 'r', encoding='utf-8') as source_file:
        reader = csv.DictReader(source_file)
        for row in reader:
            raw_value = row.get(column_name, '').strip().strip('"').strip("'")
            if not raw_value:
                continue

            matched = False
            for fmt, pattern in DATE_FORMAT_PATTERNS.items():
                if re.match(pattern, raw_value):
                    format_summary[fmt] += 1
                    matched = True
                    break

            if not matched:
                format_summary['UNMATCHED'] += 1

    for fmt, count in format_summary.items():
        if count > 0:
            logger.info(f"{dataset_name} date format '{fmt}': {count} records")

    return format_summary


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


# ======================================================
# CHECK 6: EMPTY DEPARTMENTS
# ======================================================

def check_empty_departments(conn):

    logger.info("Checking for empty departments")

    query = """
    SELECT COUNT(*)
    FROM raw.raw_jobs
    WHERE department IS NULL OR TRIM(department) = ''
    """

    cursor = conn.cursor()
    cursor.execute(query)

    count = cursor.fetchone()[0]

    logger.info(f"Jobs with empty departments: {count}")
    
    return count


# ======================================================
# CHECK 7: DATE FORMAT CONSISTENCY
# ======================================================

def check_job_date_format_consistency(conn):
    """Audit raw source date formats in jobs.csv."""
    del conn
    return summarize_date_formats(JOBS_FILE, 'posted_date', 'jobs.csv')


def check_application_date_format_consistency(conn):
    """Audit raw source date formats in applications.csv."""
    del conn
    return summarize_date_formats(APPLICATIONS_FILE, 'apply_date', 'applications.csv')


def check_date_format_consistency(conn):
    """
    Backward-compatible combined date format audit for source CSV files.
    Returns date format summaries for both jobs and applications.
    """
    return {
        'jobs': check_job_date_format_consistency(conn),
        'applications': check_application_date_format_consistency(conn),
    }


# ======================================================
# RUN ALL CHECKS
# ======================================================
def run_quality_checks():

    logger.info("Running data quality checks")

    conn = get_connection()

    try:

        check_duplicate_applications(conn)
        check_null_candidate_ids(conn)
        detect_hired_before_applied(conn)
        check_data_freshness(conn)
        check_volume_anomaly(conn)
        check_empty_departments(conn)
        check_job_date_format_consistency(conn)
        check_application_date_format_consistency(conn)

    finally:

        conn.close()


if __name__ == "__main__":
    run_quality_checks()
