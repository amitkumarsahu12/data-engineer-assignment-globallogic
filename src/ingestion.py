import csv
import json
import logging
from datetime import datetime
from dateutil import parser

import psycopg2
from psycopg2.extras import execute_batch, Json

from config import (
    DB_CONFIG,
    JOBS_FILE,
    APPLICATIONS_FILE,
    CANDIDATES_FILE,
    EDUCATION_FILE,
    WORKFLOW_EVENTS_FILE,
    BATCH_SIZE,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# DATABASE CONNECTION
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# DATE PARSER
def parse_date(value):
    if not value:
        return None
    try:
        return parser.parse(value)
    except Exception:
        return None


# JOBS INGESTION
def ingest_jobs(conn):

    logger.info("Ingesting jobs")

    query = """
    INSERT INTO raw.raw_jobs (
        job_id,
        title,
        department,
        posted_date,
        status
    )
    VALUES (%s, %s, %s, %s, %s)

    ON CONFLICT (job_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        department = EXCLUDED.department,
        posted_date = EXCLUDED.posted_date,
        status = EXCLUDED.status,
        updated_at = CURRENT_TIMESTAMP
    """

    batch = []

    with open(JOBS_FILE, encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:

            batch.append(
                (
                    row["job_id"],
                    row["title"],
                    row["department"],
                    parse_date(row["posted_date"]),
                    row["status"],
                )
            )

            if len(batch) >= BATCH_SIZE:
                execute_batch(conn.cursor(), query, batch)
                conn.commit()
                batch.clear()

    if batch:
        execute_batch(conn.cursor(), query, batch)
        conn.commit()

    logger.info("Jobs ingestion complete")


# CANDIDATES INGESTION
def ingest_candidates(conn):

    logger.info("Ingesting candidates")

    query = """
    INSERT INTO raw.raw_candidates (
        candidate_id,
        first_name,
        last_name,
        email,
        phone,
        skills
    )
    VALUES (%s, %s, %s, %s, %s, %s)

    ON CONFLICT (candidate_id)
    DO UPDATE SET
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        skills = EXCLUDED.skills,
        updated_at = CURRENT_TIMESTAMP
    """

    batch = []

    with open(CANDIDATES_FILE, encoding="utf-8") as file:
        data = json.load(file)

        for row in data:

            batch.append(
                (
                    row["candidate_id"],
                    row["first_name"],
                    row["last_name"],
                    row.get("email"),
                    row.get("phone"),
                    Json(row.get("skills")),
                )
            )

            if len(batch) >= BATCH_SIZE:
                execute_batch(conn.cursor(), query, batch)
                conn.commit()
                batch.clear()

    if batch:
        execute_batch(conn.cursor(), query, batch)
        conn.commit()

    logger.info("Candidates ingestion complete")


# EDUCATION INGESTION
def ingest_education(conn):

    logger.info("Ingesting education")

    query = """
    INSERT INTO raw.raw_education (
        candidate_id,
        degree,
        institution,
        graduation_year
    )
    VALUES (%s, %s, %s, %s)
    """

    with open(EDUCATION_FILE, encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:

            conn.cursor().execute(
                query,
                (
                    row["candidate_id"],
                    row["degree"],
                    row["institution"],
                    row["year"],
                ),
            )

    conn.commit()
    logger.info("Education ingestion complete")


# APPLICATIONS INGESTION
def ingest_applications(conn):

    logger.info("Ingesting applications")

    query = """
    INSERT INTO raw.raw_applications (
        application_id,
        job_id,
        candidate_id,
        apply_date
    )
    VALUES (%s, %s, %s, %s)

    ON CONFLICT (application_id)
    DO UPDATE SET
        apply_date = EXCLUDED.apply_date,
        updated_at = CURRENT_TIMESTAMP
    """

    with open(APPLICATIONS_FILE, encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:

            conn.cursor().execute(
                query,
                (
                    row["application_id"],
                    row["job_id"],
                    row["candidate_id"],
                    parse_date(row["apply_date"]),
                ),
            )

    conn.commit()
    logger.info("Applications ingestion complete")


# WORKFLOW EVENTS INGESTION
def ingest_workflow_events(conn):

    logger.info("Ingesting workflow events")

    query = """
    INSERT INTO raw.raw_workflow_events (
        application_id,
        old_status,
        new_status,
        event_timestamp
    )
    VALUES (%s, %s, %s, %s)

    ON CONFLICT DO NOTHING
    """

    with open(WORKFLOW_EVENTS_FILE, encoding="utf-8") as file:

        for line in file:

            event = json.loads(line)

            conn.cursor().execute(
                query,
                (
                    event["application_id"],
                    event["old_status"],
                    event["new_status"],
                    parse_date(event["event_timestamp"]),
                ),
            )

    conn.commit()
    logger.info("Workflow events ingestion complete")


# MAIN PIPELINE
def run_ingestion():

    logger.info("Starting ingestion pipeline")

    conn = get_connection()

    try:

        ingest_jobs(conn)
        ingest_candidates(conn)
        ingest_education(conn)
        ingest_applications(conn)
        ingest_workflow_events(conn)

        logger.info("Ingestion completed successfully")

    finally:

        conn.close()


if __name__ == "__main__":
    run_ingestion()