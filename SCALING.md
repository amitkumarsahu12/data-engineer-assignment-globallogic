# Scaling Strategy for 10TB Workflow Events

## Problem Statement

The current pipeline processes workflow events from a single JSONL file. If `workflow_events.jsonl` grows to **10TB in size**, the current Python-based approach will face significant challenges:

- **Memory constraints**: Loading 10TB into memory is infeasible
- **Processing time**: Sequential processing would take hours/days
- **I/O bottleneck**: Single-threaded file reading is inefficient
- **Database bottleneck**: Postgres ingestion becomes the limiting factor

## Current Architecture Limitations

```
Current Flow:
For each line in workflow_events.jsonl (single-threaded):
  → Parse JSON
  → Insert into PostgreSQL
  → Commit

Issues:
- No parallelization
- No batching optimization for large files
- Single database connection
- No fault tolerance
- No resumability
```

---

## Proposed Scaling Architecture

### 1. Distributed Processing with Apache Spark

**Why Spark?**
- Processes data in parallel across a cluster
- Handles large files efficiently
- Built-in optimizations and fault tolerance
- Integrates seamlessly with PostgreSQL

**Implementation:**

```python
# scaling/ingest_workflow_events_spark.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WorkflowEventsIngestion") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Define schema for workflow events
schema = StructType([
    StructField("application_id", StringType(), False),
    StructField("old_status", StringType(), True),
    StructField("new_status", StringType(), False),
    StructField("event_timestamp", TimestampType(), False)
])

# Read JSONL file in parallel
df = spark.read \
    .schema(schema) \
    .json("data/workflow_events.jsonl")

# Repartition for parallel writes (10TB → 1000 partitions)
# Each executor writes ~10GB in parallel
df_partitioned = df.repartition(1000)

# Write to PostgreSQL using JDBC
df_partitioned.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/de_assignment") \
    .option("dbtable", "raw.raw_workflow_events_staging") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("numPartitions", 1000) \
    .option("batchsize", 10000) \
    .mode("append") \
    .save()

# UPSERT from staging to main table
spark.sql("""
    INSERT INTO raw.raw_workflow_events
    SELECT DISTINCT *
    FROM raw.raw_workflow_events_staging
    ON CONFLICT (application_id, new_status, event_timestamp)
    DO NOTHING
""")

spark.stop()
```

**Benefits:**
- Processes 10TB in ~30-60 minutes (depending on cluster size)
- Fault tolerant: Can resume from failure
- Auto-optimized parallelization

---

### 2. Date-Based Partitioning Strategy

**Key Insight:** Partition data by date to enable incremental loading.

**Schema Modification:**

```sql
-- Partition raw_workflow_events by event date
CREATE TABLE raw.raw_workflow_events (
    event_id BIGSERIAL,
    application_id TEXT NOT NULL,
    old_status TEXT,
    new_status TEXT,
    event_timestamp TIMESTAMP NOT NULL,
    event_date DATE NOT NULL,  -- NEW: For partitioning
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_raw_workflow_events PRIMARY KEY (event_date, event_id),
    CONSTRAINT fk_event_application FOREIGN KEY (application_id)
        REFERENCES raw.raw_applications(application_id)
) PARTITION BY RANGE (event_date);

-- Create partitions for date ranges
CREATE TABLE raw_workflow_events_2024_01 
    PARTITION OF raw.raw_workflow_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE raw_workflow_events_2024_02 
    PARTITION OF raw.raw_workflow_events
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- ... continue for all months
```

**Incremental Loading Strategy:**

```python
# scaling/incremental_workflow_ingest.py

import json
from datetime import datetime, timedelta
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_batch

def get_last_processed_date():
    """Retrieve the last date successfully processed."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT MAX(event_date) FROM raw.raw_workflow_events
    """)
    
    result = cursor.fetchone()[0]
    conn.close()
    
    return result or datetime(2020, 1, 1).date()


def ingest_workflow_events_incremental():
    """Load only new events since last run."""
    
    last_date = get_last_processed_date()
    today = datetime.now().date()
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    with open("data/workflow_events.jsonl", encoding="utf-8") as file:
        batch = []
        
        for line in file:
            event = json.loads(line)
            event_date = parser.parse(event["event_timestamp"]).date()
            
            # Skip already-processed dates
            if event_date <= last_date:
                continue
            
            batch.append((
                event["application_id"],
                event.get("old_status"),
                event["new_status"],
                parser.parse(event["event_timestamp"]),
                event_date
            ))
            
            # Batch insert for efficiency
            if len(batch) >= 50000:
                execute_batch(conn.cursor(), 
                    "INSERT INTO raw.raw_workflow_events (...) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    batch
                )
                conn.commit()
                batch.clear()
        
        # Final batch
        if batch:
            execute_batch(conn.cursor(), 
                "INSERT INTO raw.raw_workflow_events (...) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                batch
            )
            conn.commit()
    
    conn.close()
```

**Benefits:**
- Only loads new data each run
- Partitions enable faster queries
- Can handle streaming data easily

---

### 3. Columnar Storage (Parquet Format)

**Why Columnar?**
- 10TB of text JSON → ~500GB-1TB Parquet (10x compression)
- Faster analytical queries
- Better for batch processing

**Implementation:**

```python
# scaling/convert_to_parquet.py

import pandas as pd
import pyarrow.parquet as pq

# Read JSONL in chunks
chunk_size = 100000
chunks = []

for chunk in pd.read_json("data/workflow_events.jsonl", 
                           lines=True, 
                           chunksize=chunk_size):
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)

# Convert to Parquet with compression
table = pa.Table.from_pandas(df)
pq.write_table(
    table,
    "data/workflow_events.parquet",
    compression='snappy',
    use_dictionary=['application_id', 'new_status']  # Dictionary encoding
)

# Read Parquet for ingestion (MUCH faster than JSON)
df = pd.read_parquet("data/workflow_events.parquet")
```

**Size Reduction:**
- JSON JSONL: 10,000 GB
- Parquet (snappy): ~1,000 GB (10x reduction)
- Processing time: ~50% faster I/O

---

### 4. Optimized Python Pipeline with Parallelization

For organizations not ready for Spark, parallel Python processing:

```python
# scaling/parallel_workflow_ingest.py

from multiprocessing import Pool, cpu_count
from functools import partial
import json
import psycopg2
from dateutil import parser
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_batch(batch_lines):
    """Process a batch of lines in a worker process."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    batch = []
    for line in batch_lines:
        event = json.loads(line)
        batch.append((
            event["application_id"],
            event.get("old_status"),
            event["new_status"],
            parser.parse(event["event_timestamp"])
        ))
    
    # Batch insert
    from psycopg2.extras import execute_batch
    execute_batch(cursor, """
        INSERT INTO raw.raw_workflow_events 
        (application_id, old_status, new_status, event_timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, batch)
    
    conn.commit()
    conn.close()
    
    return len(batch)


def ingest_workflow_events_parallel():
    """Parallel ingestion using process pool."""
    
    # Read file into batches (in memory chunking)
    batch_size = 100000
    batches = []
    current_batch = []
    
    with open("data/workflow_events.jsonl", encoding="utf-8") as file:
        for line in file:
            current_batch.append(line)
            if len(current_batch) >= batch_size:
                batches.append(current_batch)
                current_batch = []
        
        if current_batch:
            batches.append(current_batch)
    
    # Process batches in parallel
    num_workers = cpu_count()  # Use all CPU cores
    with Pool(processes=num_workers) as pool:
        results = pool.map(process_batch, batches)
    
    total_inserted = sum(results)
    logger.info(f"Inserted {total_inserted} records in parallel")


if __name__ == "__main__":
    ingest_workflow_events_parallel()
```

**Benefits:**
- No external dependencies (no Spark)
- Uses all CPU cores efficiently
- 4-8x faster than serial processing

---

### 5. Complete Scaled Architecture Diagram

```
10TB workflow_events.jsonl
        ↓
    [Split into 1000 partitions]
        ↓
[Spark Executors (Distributed Cluster)]
   ├─ Worker 1 (10GB) → Parse & Load
   ├─ Worker 2 (10GB) → Parse & Load
   ├─ Worker 3 (10GB) → Parse & Load
   └─ ... (10s more workers)
        ↓
[PostgreSQL Partitioned Table]
   ├─ raw_workflow_events_2024_01 (Partition 1)
   ├─ raw_workflow_events_2024_02 (Partition 2)
   └─ ... (monthly partitions)
        ↓
[Transformation Layer - Date-filtered queries]
   └─ Faster because partitions pruned
        ↓
[Star Schema Warehouse]
   └─ Aggregated + Indexed
```

---

## Performance Comparison

| Approach | File Size | Process Time | Memory | Cost |
|----------|-----------|--------------|--------|------|
| Current (Serial Python) | 10TB | 48+ hours | 20GB | Low |
| Parallel Python (4 cores) | 10TB | 8-12 hours | 40GB | Low |
| Spark (10 nodes) | 10TB | 30-60 min | 80GB | Medium |
| Parquet + Spark | 1TB | 10-20 min | 40GB | Medium |

---

## Recommended Implementation Roadmap

### Phase 1: Quick Win (0-1 month)
- Implement parallel Python processing
- Add date-based partitioning to schema
- Estimate improvement: 5-8x faster

### Phase 2: Medium Term (1-2 months)
- Deploy Spark cluster (AWS EMR, Databricks, or on-premise)
- Migrate to Parquet for storage
- Implement incremental loading
- Estimate improvement: 50-100x faster

### Phase 3: Long Term (2-6 months)
- Implement stream processing (Kafka + Spark Streaming)
- Real-time data quality checks
- Auto-scaling infrastructure
- Estimate: Sub-minute end-to-end latency

---

## Monitoring & Observability at Scale

```python
# monitoring/scale_monitor.py

import psycopg2
import logging

def monitor_ingestion_progress(conn):
    """Monitor real-time ingestion progress."""
    
    cursor = conn.cursor()
    
    # Check volume per hour
    cursor.execute("""
        SELECT 
            DATE_TRUNC('hour', event_timestamp) as hour,
            COUNT(*) as event_count
        FROM raw.raw_workflow_events
        GROUP BY DATE_TRUNC('hour', event_timestamp)
        ORDER BY hour DESC
        LIMIT 24
    """)
    
    for hour, count in cursor.fetchall():
        logging.info(f"Hour {hour}: {count} events ({count/3600:.0f} events/sec)")


def identify_bottlenecks(conn):
    """Identify processing bottlenecks."""
    
    cursor = conn.cursor()
    
    # Check table size
    cursor.execute("""
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
        FROM pg_tables
        WHERE tablename LIKE 'raw_workflow%'
    """)
    
    for schema, table, size in cursor.fetchall():
        logging.info(f"{schema}.{table}: {size}")
```

---

## Key Takeaways

1. **Sequential processing is not viable for 10TB** → Use distributed systems
2. **Partitioning is essential** → Query and load only what's needed
3. **Compression matters** → 10TB JSON → 1TB Parquet
4. **Parallelization is critical** → 50-100x speedup with proper architecture
5. **Monitoring is necessary** → Track throughput and bottlenecks

The recommended approach is **Spark + Date Partitioning + Parquet**, which achieves:
- ✅ 10TB ingestion in 30-60 minutes
- ✅ Fault tolerance and resumability
- ✅ Scalable to 100TB+ with more cluster nodes
- ✅ Foundation for real-time streaming later
