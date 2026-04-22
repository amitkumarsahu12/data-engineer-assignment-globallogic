# Data Engineer Take-Home Assignment

## Introduction

Welcome! This assignment is designed to evaluate your skills in data engineering, from basic ingestion to advanced architectural design. We've structured this as a tiered challenge:

- **Mid-Level**: Task Groups 1, 2
- **Senior**: Task Groups 1, 2, 3

**Note**: These levels are just recommendations. Feel free to complete all task groups if you'd like to showcase your full range of skills.

**Time Expectation**: We respect your time. You are not expected to spend days on this. Focus on quality over quantity. If you run out of time, document what you _would_ have done.

**AI Usage Policy**: We encourage the use of AI tools (ChatGPT, Copilot, etc.) to speed up boilerplate code. However, you must understand every line of code you submit. We value your ability to verify AI output, handle edge cases, and make architectural trade-offs.

## The Dataset

You will find the following files in the `dataset/` folder (provided in the package):

- `jobs.csv`: Job requisitions (job_id, title, department, posted_date, status).
- `candidates.json`: Candidate profiles (candidate_id, first_name, last_name, email, phone, skills).
- `education.csv`: Candidate education records (candidate_id, degree, institution, year).
- `applications.csv`: Link table (application_id, job_id, candidate_id, apply_date).
- `workflow_events.jsonl`: Event stream of status changes (application_id, old_status, new_status, event_timestamp).
- `dimensional_model_erd.png`: Star schema ERD image for Task Group 2.

**Note**: The data is synthetic and "dirty". Expect duplicates, missing values, and potential logical inconsistencies.

## Task Groups

### Task Group 1: Data Ingestion & Basic Analysis

1.  **Database Setup**: Define your database schema (DDL) and use a migration approach (e.g., SQL scripts, Alembic, or a simple Python runner) to create the tables.
2.  **Ingestion Script**: Write a Python script to read the provided files and load them into a local database (SQLite is fine, or a Dockerized PostgreSQL).
    - _Constraint_: Ensure your script handles mixed date formats and potential unicode characters.
    - _Constraint_: Ensure the script is **idempotent**
3.  **SQL Analysis**: Write SQL queries to answer:
    - How many jobs are currently open?
    - Top 5 departments by number of applications.
    - List candidates who applied to more than 3 jobs.

### Task Group 2: Data Modeling & Transformation

1.  **ETL Pipeline**: Create a transformation process to populate your Star Schema from the raw tables based on the given ERD.
    - _Tools_: dbt OR PySpark. Choose the tool that best demonstrates your strengths.
    - **Path A (dbt)**: Follow a **Data Warehousing** approach. Continue using the local database you set up in Task 1 as your target warehouse.
    - **Path B (PySpark)**: Follow a **Lakehouse** approach.
      - **Storage**: Choose an open format (e.g., Parquet, Iceberg, Delta Lake). **Explain your choice** in the README.
      - **Querying**: Set up a querying layer to verify your results. **DuckDB** is a recommended option, but feel free to use Spark SQL or others.
    - **Path C (Custom)**: If you have a different architectural preference (e.g., dbt with Spark, DuckDB only, etc.), feel free to use it. Just ensure you **justify your decision** in the README.
2.  **Metric Calculation**: Calculate "Time to Hire" (days from Apply to Hired) per job and department.

### Task Group 3: Engineering, Quality & Optimization

1.  **Idempotency**: Ensure your ingestion and transformation pipelines are idempotent (can be run multiple times without duplicating data).
2.  **Data Quality Framework**: Implement automated checks for:
    - Uniqueness, freshness, volume anomaly etc.
    - **Anomaly Detection**: The dataset contains "Hired" events that occur _before_ "Applied" events for a few records. Detect this anomaly and propose a handling strategy (drop, flag, or correct).
3.  **Performance & Scaling**:
    - **Scenario**: Imagine the `workflow_events` file is 10TB in size.
    - **Written Answer**: Explain how you would modify your pipeline to handle this scale.
4.  **Unit Tests**: Write unit tests for your code.

## Submission Guidelines

1.  **Code**: Submit your code in a zip file.
2.  **README**: Include a `README.md` with:
    - Instructions on how to run your code (setup, dependencies).
    - Answers to the written questions (Trade-offs, Performance, Architecture).
    - **AI Statement**: A brief statement explaining how you used AI tools. Did you use it for boilerplate? Debugging? etc.

Good luck!
