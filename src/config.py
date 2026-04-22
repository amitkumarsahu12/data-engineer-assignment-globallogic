import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "de_assignment"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

BASE_DATA_PATH = os.getenv("DATA_PATH", "data")

JOBS_FILE = os.path.join(BASE_DATA_PATH, "jobs.csv")
APPLICATIONS_FILE = os.path.join(BASE_DATA_PATH, "applications.csv")
CANDIDATES_FILE = os.path.join(BASE_DATA_PATH, "candidates.json")
EDUCATION_FILE = os.path.join(BASE_DATA_PATH, "education.csv")
WORKFLOW_EVENTS_FILE = os.path.join(BASE_DATA_PATH, "workflow_events.jsonl")


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

BATCH_SIZE = 1000