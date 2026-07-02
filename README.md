# Real-Time Streaming Pipeline

A production-grade real-time data pipeline built to learn Apache Spark Structured Streaming, Kafka, and distributed systems patterns. The project supports three ingestion modes — direct Avro, CDC via Debezium, and batch analytics — and includes full observability, fault tolerance, and auto-recovery.

---

## Architecture

### Mode 1 — Direct (Avro + Schema Registry)

```
producer.py
  └── confluent_kafka + AvroSerializer + Schema Registry
      ↓ Avro-serialized messages, keyed by order_id
Kafka topic: orders (3 partitions)
      ↓
spark_job.py (MODE="direct")
  └── process_avro_batch()
      → AvroDeserializer (confluent_kafka)
      → manual 1-minute windowing in Python
      → psycopg2 upsert (accumulating)
      ↓
PostgreSQL: pipeline.orders_agg
```

### Mode 2 — CDC (Change Data Capture via Debezium)

```
simulate_orders.py
  └── psycopg2 INSERT (placed) / UPDATE (shipped/delivered/cancelled/returned)
      ↓
PostgreSQL: pipeline_source.orders
  └── WAL logical replication enabled
      ↓
Debezium (Kafka Connect worker)
  └── watches WAL, publishes change events
      ↓
Kafka topic: pgserver.public.orders
      ↓
spark_job.py (MODE="cdc")
  ├── parsed_df (op="c" only) → agg_df → orders_agg
  ├── all_parsed_df (all ops) → analytics_df → orders_analytics
  └── bad_df (null fields)   → orders-dlq topic
      ↓
PostgreSQL: pipeline.orders_agg + pipeline.orders_analytics
```

### Batch Analytics

```
batch_job.py
  └── reads pipeline.orders_agg via JDBC
      → highest revenue window
      → lowest revenue window
      → average revenue per minute
      → cumulative revenue trend (Spark window functions)
```

### Monitoring

```
Kafka JMX → JMX Exporter (port 9101) → Prometheus (port 9091) → Grafana (port 3000)
Spark metrics servlet (port 4040/metrics/prometheus) → Prometheus → Grafana
```

---

## Stack

| Component | Version |
|---|---|
| Apache Spark | 3.5.0 |
| Apache Kafka | 7.4.0 (Confluent) |
| Confluent Schema Registry | 7.4.0 |
| Debezium | 2.3 |
| PostgreSQL | 15 |
| Prometheus | v2.45.0 |
| Grafana | 10.0.0 |
| Python | 3.11.9 |
| Java | 11 (Temurin) |
| PyFlink | 1.18.0 (documented, Windows limitation) |

---

## Project Structure

```
real-time-streaming-kafka/
├── producer.py              # Mode 1: Avro producer → Kafka (orders topic)
├── simulate_orders.py       # Mode 2: CDC generator → Postgres source DB
├── spark_job.py             # Spark Structured Streaming (MODE flag switches behavior)
├── batch_job.py             # Spark batch analytics on orders_agg
├── flink_job.py             # PyFlink job (documented, Windows PyFlink limitation)
├── metrics.properties       # Spark Prometheus metrics config
├── docker-compose.yml       # Full infrastructure definition
├── requirements.txt
├── .gitattributes           # Forces LF line endings for shell scripts
├── README.md
├── monitoring/
│   ├── prometheus.yml                    # Prometheus scrape config
│   ├── kafka-jmx-config.yml             # JMX exporter metric rules for Kafka
│   ├── jmx_prometheus_javaagent.jar     # JMX → Prometheus translator (gitignored)
│   ├── debezium/
│   │   └── register-connector.sh        # Auto-registers Debezium connector on startup
│   └── grafana/
│       ├── dashboards/
│       │   └── pipeline-dashboard.json  # Dashboard as code (auto-provisioned)
│       └── provisioning/
│           ├── dashboards/
│           │   └── dashboard.yml        # Tells Grafana where to find dashboard JSONs
│           └── datasources/
│               └── prometheus.yml       # Auto-provisions Prometheus datasource
└── flink_jars/              # Flink connector JARs (gitignored, see download instructions)
    ├── flink-sql-connector-kafka-3.0.2-1.18.jar
    ├── flink-connector-jdbc-3.1.2-1.18.jar
    └── postgresql-42.6.0.jar
```

---

## Docker Services

| Service | Port | Purpose |
|---|---|---|
| zookeeper | 2181 | Kafka cluster coordination |
| kafka | 9092 (host), 29092 (internal), 9101 (JMX) | Message broker |
| schema-registry | 8081 | Avro schema storage and validation |
| postgres | 5432 | Sink DB (pipeline) + Source DB (pipeline_source) |
| debezium | 8083 | Kafka Connect worker, CDC connector |
| debezium-init | — | One-shot container that auto-registers Debezium connector |
| prometheus | 9091 | Metrics collection |
| grafana | 3000 | Metrics dashboard |

**Volumes (persistent across restarts):**
- `postgres_data` → `/var/lib/postgresql/data`
- `kafka_data` → `/var/lib/kafka`
- `grafana_data` → `/var/lib/grafana`

**Kafka listeners:**
- `PLAINTEXT://localhost:9092` — for host machine (producer.py, spark_job.py)
- `PLAINTEXT_INTERNAL://kafka:29092` — for containers (Debezium, Schema Registry)

---

## PostgreSQL Databases

### pipeline (sink DB — analytics results)

```sql
-- Revenue aggregation (both modes write here)
CREATE TABLE orders_agg (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    total_revenue DOUBLE PRECISION,
    order_count  BIGINT,
    CONSTRAINT unique_window UNIQUE (window_start, window_end)
);

-- Status breakdown analytics (CDC mode only)
CREATE TABLE orders_analytics (
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    placed_count     BIGINT DEFAULT 0,
    shipped_count    BIGINT DEFAULT 0,
    delivered_count  BIGINT DEFAULT 0,
    cancelled_count  BIGINT DEFAULT 0,
    returned_count   BIGINT DEFAULT 0,
    CONSTRAINT unique_analytics_window UNIQUE (window_start, window_end)
);

-- Flink aggregation (separate from Spark results)
CREATE TABLE orders_flink_agg (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    total_revenue DOUBLE PRECISION,
    order_count   BIGINT,
    PRIMARY KEY (window_start, window_end)
);
```

### pipeline_source (CDC source DB)

```sql
-- WAL settings required for Debezium:
-- wal_level = logical
-- max_replication_slots = 4
-- max_wal_senders = 4

CREATE TABLE orders (
    order_id VARCHAR(36) PRIMARY KEY,
    user_id  INTEGER,
    amount   DOUBLE PRECISION,
    status   VARCHAR(20),
    ts       TIMESTAMP
);
```

---

## Order State Machine

Both `producer.py` and `simulate_orders.py` implement the same realistic lifecycle:

```
placed → shipped → delivered   (terminal, order closes)
placed → shipped → returned    (terminal, order closes)
placed → cancelled             (terminal, order closes)
```

**Probabilities:**
- 70% chance: progress an existing active order
  - placed → 80% shipped, 20% cancelled
  - shipped → 70% delivered, 30% returned
- 30% chance: place a brand new order

**Key design decisions:**
- `active_orders` keyed by `order_id` (not `user_id`) — allows multiple concurrent orders per user
- In `producer.py`: messages keyed by `order_id` for Kafka partition routing — all status transitions for the same order go to the same partition, guaranteeing ordered processing
- In `simulate_orders.py`: placed → INSERT, all other statuses → UPDATE (respects PRIMARY KEY constraint)

---

## spark_job.py — Design

### MODE flag

```python
MODE = "direct"  # or "cdc"
```

Switch at the top of the file. Controls which Kafka topic to subscribe to and how messages are parsed.

### Direct mode parsing (Avro)

Uses `confluent_kafka`'s `AvroDeserializer` directly inside `foreachBatch`, bypassing Spark's native Avro support. This is necessary because Confluent's Avro format includes a 5-byte schema ID header that Spark's built-in `from_avro` doesn't handle natively without the Schema Registry JAR.

The `process_avro_batch()` function:
1. Collects raw Avro bytes from Kafka
2. Deserializes each message using `AvroDeserializer`
3. Manually buckets events into 1-minute windows using a Python dict
4. Upserts to Postgres using `+=` accumulation (required because each micro-batch only sees a slice of events for any given window)

### CDC mode parsing (Debezium envelope)

```python
debezium_schema = StructType()
    .add("payload", StructType()
        .add("after", cdc_order_schema)   # LongType for ts (microseconds)
        .add("op", StringType())           # "c"=insert, "u"=update, "d"=delete
    )

# ts conversion: Debezium stores microseconds since epoch
.withColumn("ts", (col("ts") / 1000000).cast("timestamp"))
```

Two dataframes derived from the same raw stream:
- `parsed_df`: filters `op="c"` (inserts only) → revenue aggregation
- `all_parsed_df`: no op filter → status breakdown analytics

### Three streaming queries (CDC mode)

```python
query           → agg_df → orders_agg          (Spark stateful windowing + watermark)
analytics_query → analytics_df → orders_analytics (all status transitions per window)
dlq_query       → bad_df → orders-dlq topic     (null critical fields routed to DLQ)
```

### Postgres writes

- Uses `psycopg2` directly (not JDBC) for upsert support via `ON CONFLICT DO UPDATE`
- Single persistent connection with `ensure_connection()` health check (`SELECT 1` ping)
- Rollback + single retry on connection failure
- Direct mode uses `+= accumulation` upsert; CDC mode uses `= replacement` upsert

### Windows startup fix

Spark on Windows resolves `host.docker.internal` as its own hostname, causing executor connection failures. Always start with:

```cmd
set SPARK_LOCAL_IP=127.0.0.1
python spark_job.py
```

---

## Prerequisites

- Docker Desktop
- Python 3.11.x
- Java 11 or 17

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/tarunpatelofficial/real-time-streaming-kafka.git
cd real-time-streaming-kafka
```

### 2. Create virtual environment

```bash
# Windows
C:\Python311\python.exe -m venv .venv
.venv\Scripts\activate

# Mac/Linux
python3.11 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download monitoring JAR (not in repo)

```bash
curl -o monitoring/jmx_prometheus_javaagent.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar
```

### 5. Download Flink JARs (only needed for flink_job.py)

```bash
mkdir flink_jars
curl -o flink_jars/flink-sql-connector-kafka-3.0.2-1.18.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar
curl -o flink_jars/flink-connector-jdbc-3.1.2-1.18.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
curl -o flink_jars/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
```

### 6. Start infrastructure

```bash
docker-compose up -d
```

This starts all 8 services. The `debezium-init` container automatically registers the Debezium connector — no manual curl command needed.

### 7. Create Kafka topics

```bash
# orders topic (3 partitions for parallel processing)
docker exec -it kafka bash -c "KAFKA_OPTS='' kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1"

# DLQ topic
docker exec -it kafka bash -c "KAFKA_OPTS='' kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
```

Note: `KAFKA_OPTS=''` prefix is required on Windows to prevent the JMX exporter agent from conflicting with the CLI command.

### 8. Configure source database for CDC (first time only)

```bash
# Enable logical replication
docker exec -it postgres psql -U admin -d pipeline_source -c "ALTER SYSTEM SET wal_level = logical;"
docker exec -it postgres psql -U admin -d pipeline_source -c "ALTER SYSTEM SET max_replication_slots = 4;"
docker exec -it postgres psql -U admin -d pipeline_source -c "ALTER SYSTEM SET max_wal_senders = 4;"
docker restart postgres
```

### 9. Create sink tables

```bash
docker exec -it postgres psql -U admin -d pipeline -c "
CREATE TABLE orders_agg (
    window_start TIMESTAMP, window_end TIMESTAMP,
    total_revenue DOUBLE PRECISION, order_count BIGINT,
    CONSTRAINT unique_window UNIQUE (window_start, window_end)
);
CREATE TABLE orders_analytics (
    window_start TIMESTAMP, window_end TIMESTAMP,
    placed_count BIGINT DEFAULT 0, shipped_count BIGINT DEFAULT 0,
    delivered_count BIGINT DEFAULT 0, cancelled_count BIGINT DEFAULT 0,
    returned_count BIGINT DEFAULT 0,
    CONSTRAINT unique_analytics_window UNIQUE (window_start, window_end)
);
CREATE TABLE orders_flink_agg (
    window_start TIMESTAMP, window_end TIMESTAMP,
    total_revenue DOUBLE PRECISION, order_count BIGINT,
    PRIMARY KEY (window_start, window_end)
);"
```

---

## Running the Pipeline

### Mode 1 — Direct (Avro)

Set `MODE = "direct"` in `spark_job.py`.

**Terminal 1 — producer:**
```bash
python producer.py
```

**Terminal 2 — Spark streaming:**
```bash
# Windows
set SPARK_LOCAL_IP=127.0.0.1
python spark_job.py

# Mac/Linux
SPARK_LOCAL_IP=127.0.0.1 python spark_job.py
```

**What populates:**
- `orders_agg` — revenue per minute from placed orders
- `orders-dlq` — any malformed messages

### Mode 2 — CDC

Set `MODE = "cdc"` in `spark_job.py`. Delete stale checkpoints first when switching modes.

**Terminal 1 — order simulator:**
```bash
python simulate_orders.py
```

**Terminal 2 — Spark streaming:**
```bash
set SPARK_LOCAL_IP=127.0.0.1
python spark_job.py
```

**What populates:**
- `orders_agg` — revenue per minute (placed orders via `op="c"`)
- `orders_analytics` — full status breakdown per minute (all transitions)
- `orders-dlq` — malformed events

### Batch Analytics (any time)

```bash
set SPARK_LOCAL_IP=127.0.0.1
python batch_job.py
```

Reads from `orders_agg` and prints:
- Highest revenue window
- Lowest revenue window
- Average revenue per minute
- Cumulative revenue trend

### Switching Modes

Always delete checkpoints when switching `MODE`:

```bash
# Windows
rmdir /s /q "D:\real-time-streaming-kafka\tmp\spark_checkpoint"
rmdir /s /q "D:\real-time-streaming-kafka\tmp\dlq_checkpoint"
rmdir /s /q "D:\real-time-streaming-kafka\tmp\analytics_checkpoint"

# Mac/Linux
rm -rf tmp/spark_checkpoint tmp/dlq_checkpoint tmp/analytics_checkpoint
```

---

## Monitoring

### Access

| Service | URL | Credentials |
|---|---|---|
| Prometheus | http://localhost:9091 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Spark UI | http://localhost:4040 | — |
| Schema Registry | http://localhost:8081 | — |
| Debezium | http://localhost:8083 | — |

### Grafana Dashboard

The **Pipeline Monitoring** dashboard auto-provisions on startup — no manual setup needed. It includes:

| Panel | Query | What it shows |
|---|---|---|
| Kafka Messages In/sec | `rate(kafka_messages_in_total[1m])` | Live message throughput |
| Kafka Bytes In/sec | `rate(kafka_bytes_in_total[1m])` | Data volume through Kafka |
| Spark JVM Heap Used | `{__name__=~"metrics_.*_driver_jvm_heap_used_Value", job="spark"}` | Spark memory consumption |
| Partition Offsets | `kafka_log_end_offset{topic="pgserver.public.orders"}` | CDC topic message growth |

Note: Spark JVM metrics only appear while `spark_job.py` is actively running.

### Useful Prometheus Queries

```promql
-- all Spark metrics
{job="spark"}

-- find heap metrics
{__name__=~"metrics_.*_jvm_heap.*", job="spark"}

-- Kafka topic offsets
kafka_log_end_offset{topic="orders"}
kafka_log_end_offset{topic="pgserver.public.orders"}

-- Kafka throughput
rate(kafka_messages_in_total[1m])
```

### Monitoring Debugging Ladder

```
Panel shows no data
    ↓
Check: Prometheus targets UP? (localhost:9091/targets)
    ↓ if DOWN: is service running? is port exposed?
    ↓ if UP
Check: metric exists in Prometheus? ({job="yourjob"})
    ↓ if empty: is service exporting metrics? (curl service:port/metrics)
    ↓ if metrics exist
Check: query correct? (run in Prometheus UI, not Grafana)
    ↓ if works in Prometheus but not Grafana
Check: datasource UID mismatch in dashboard JSON
```

---

## Debezium

### Connector auto-registration

The `debezium-init` container runs `monitoring/debezium/register-connector.sh` on every `docker-compose up`. It:
1. Waits for Debezium's HTTP API to be ready
2. Checks if `orders-connector` already exists
3. Registers it if missing, skips if already present

Manual status check:
```bash
curl http://localhost:8083/connectors/orders-connector/status
```

### CDC message format

```json
{
  "payload": {
    "before": null,
    "after": {
      "order_id": "abc123",
      "user_id": 42,
      "amount": 99.0,
      "status": "placed",
      "ts": 1782383720672634
    },
    "op": "c"
  }
}
```

- `op`: `c` = insert, `u` = update, `d` = delete
- `ts`: microseconds since epoch (divide by 1,000,000 for Spark timestamp cast)
- `before`: null for inserts, previous row values for updates

### Debugging CDC

```bash
# watch Debezium logs live
docker logs debezium -f

# verify messages arriving in Kafka
docker exec -it kafka bash -c "KAFKA_OPTS='' kafka-console-consumer --bootstrap-server localhost:9092 --topic pgserver.public.orders --from-beginning --max-messages 1"

# check replication slot
docker exec -it postgres psql -U admin -d pipeline_source -c "SELECT * FROM pg_replication_slots;"

# check source table row count
docker exec -it postgres psql -U admin -d pipeline_source -c "SELECT COUNT(*) FROM orders;"
```

---

## Resilience Features

### Dead Letter Queue (DLQ)

Events with null `order_id`, `amount`, or `ts` are routed to `orders-dlq` Kafka topic instead of crashing the job or silently corrupting aggregations.

```
good events  → aggregation → Postgres
bad events   → orders-dlq  → for investigation
```

Test with:
```bash
docker exec -it kafka bash -c "KAFKA_OPTS='' kafka-console-producer --broker-list localhost:9092 --topic orders"
# paste: {"order_id": "bad-test", "user_id": 55, "status": "placed"}
# (missing amount and ts → routed to DLQ)
```

### Checkpoint-based crash recovery

Spark writes its state and Kafka offsets to the checkpoint directory after each successful micro-batch. On restart, it resumes from exactly where it left off.

In CDC mode, `failOnDataLoss=false` prevents crashes when Docker restarts cause Kafka offset gaps.

### Postgres reconnection

`ensure_connection()` pings Postgres with `SELECT 1` before each batch write. On failure: rollback → reconnect → retry once. Tested by running `docker restart postgres` mid-pipeline.

### Watermarking

Events more than 2 minutes behind the latest seen timestamp are silently dropped. This prevents unbounded state growth while tolerating realistic network delays.

- Within watermark → accepted, included in window
- Outside watermark → silently dropped, routed to nothing

---

## Schema Registry + Avro

`producer.py` uses Confluent's `AvroSerializer` with Schema Registry at `http://localhost:8081`.

The Avro schema for the Order record:
```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.pipeline",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id",  "type": "int"},
    {"name": "amount",   "type": "double"},
    {"name": "status",   "type": "string"},
    {"name": "ts",       "type": "string"}
  ]
}
```

Schema is auto-registered on first message. Subsequent messages are validated against the registered schema. Schema evolution (adding optional fields) is supported without breaking existing consumers.

`spark_job.py` uses `confluent_kafka`'s `AvroDeserializer` (not Spark's native `from_avro`) to handle the 5-byte Confluent wire format header automatically.

---

## Flink Comparison (Documented)

`flink_job.py` implements the same 1-minute windowed aggregation using PyFlink's Table API and SQL:

```sql
INSERT INTO orders_flink_agg
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE)   as window_end,
    SUM(amount)      as total_revenue,
    COUNT(order_id)  as order_count
FROM orders
WHERE status = 'placed'
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
```

**Known limitation:** PyFlink has significant stability issues on Windows due to py4j JVM bridge failures. The job is documented and committed but not runnable on Windows without WSL2 or Docker.

**Spark vs Flink tradeoffs:**

| | Spark Structured Streaming | Apache Flink |
|---|---|---|
| Processing model | Micro-batch (seconds latency) | True event-by-event (ms latency) |
| API style | DataFrame/Python chaining | SQL / DataStream API |
| Windows result | Multiple intermediate updates | One final result on close |
| State management | Checkpoint to HDFS/local | RocksDB state backend |
| Windows support | Stable | py4j bridge issues |
| Use case | Batch + streaming unified, team knows Spark | Ultra-low latency, complex event processing |

For 1-minute window aggregations, both produce identical final results. The difference is visible in intermediate states — Spark emits multiple partial updates per window as micro-batches arrive; Flink emits once when the window closes.

---

## Key Concepts Learned

**Bounded vs unbounded data** — Static files have a defined end; streams are infinite. Spark uses windowing to turn infinite streams into finite, processable chunks.

**Micro-batch model** — Spark Structured Streaming collects events every few seconds, processes them as a tiny batch job, writes results, commits offsets, then repeats. The same DataFrame API works for both batch and streaming.

**Event time vs processing time** — An order placed at 14:42 might arrive at Spark at 14:45. Windowing uses the event's own `ts` field, not arrival time, so orders land in the correct minute bucket.

**Watermarking** — Tells Spark how long to wait for late data before closing a window. Too tight → lose real events. Too loose → unbounded state growth. Set to 2 minutes in this project.

**Exactly-once semantics** — Achieved through Kafka offsets (tracks what's been read) + Spark checkpointing (tracks processing state) + Postgres upsert (prevents duplicate writes on retry).

**CDC (Change Data Capture)** — Reads the database's transaction log (WAL) rather than polling the table. Zero overhead on the source application. Used by LinkedIn, Airbnb, Shopify to replicate operational databases to analytics systems without touching application code.

**Schema Registry** — Centralized schema storage for Avro messages. Producers register schemas; consumers validate against them. Enables schema evolution without breaking consumers. Every Confluent Kafka message carries a 5-byte header with the schema ID.

**Dead Letter Queue** — Routes bad events to a separate topic instead of crashing the pipeline or silently corrupting aggregations. Production standard for any streaming pipeline.

**Consumer group offset management** — Spark Structured Streaming manages Kafka offsets via checkpoints rather than Kafka's native consumer group protocol. This makes standard Kafka monitoring tools (which track consumer lag via consumer groups) unable to see Spark's consumption — requires Spark's own metrics for observability.

---

## Common Issues and Fixes

### Offset out of range on restart

```
IllegalStateException: Cannot fetch offset X for pgserver.public.orders-0
```

**Cause:** Checkpoint remembers old offsets that no longer exist after Docker restart.
**Fix:** Delete checkpoints + set `failOnDataLoss=false` on the CDC Kafka read.

### Asia/Calcutta timezone rejection

```
FATAL: invalid value for parameter "TimeZone": "Asia/Calcutta"
```

**Cause:** Windows reports `Asia/Calcutta`; Postgres only recognizes `Asia/Kolkata`.
**Fix:** `-Duser.timezone=Asia/Kolkata` in Spark JVM options.

### Spark binds to host.docker.internal

```
Failed to connect to host.docker.internal/10.10.2.73:XXXXX
```

**Cause:** Docker Desktop registers `host.docker.internal` in Windows hosts file; Spark picks it up as its own hostname.
**Fix:** `set SPARK_LOCAL_IP=127.0.0.1` before running Spark.

### 200 shuffle partitions despite setting 4

**Cause:** Stale checkpoint locked in old partition count.
**Fix:** Delete checkpoint directory. Always delete checkpoints when changing schema, topic, or partition config.

### kafka-topics command fails with JMX port conflict

```
BindException: Address already in use (port 9101)
```

**Cause:** `KAFKA_OPTS` with JMX agent is inherited by CLI commands run inside the container.
**Fix:** Prefix every kafka CLI command with `KAFKA_OPTS=''`.

### Debezium connector missing after restart

**Cause:** Connector config stored in `debezium_configs` Kafka topic which resets on Docker restart.
**Fix:** Handled automatically by `debezium-init` container — registers connector on every `docker-compose up` if missing.

### PyFlink exits immediately (Windows)

**Cause:** py4j JVM bridge instability on Windows.
**Fix:** Use WSL2 or Docker to run the Flink job on Linux. Documented as known limitation.

---

## Session Startup Checklist

Run these every session before starting the pipeline:

```bash
# 1. start infrastructure
docker-compose up -d

# 2. verify Kafka topics exist
docker exec -it kafka bash -c "KAFKA_OPTS='' kafka-topics --list --bootstrap-server localhost:9092"
# should show: orders, orders-dlq, pgserver.public.orders

# 3. verify Debezium connector (auto-registered, just confirm)
curl http://localhost:8083/connectors/orders-connector/status
# should show: "state":"RUNNING"

# 4. start data generator (pick one)
python producer.py           # direct/Avro mode
python simulate_orders.py    # CDC mode

# 5. start Spark (Windows)
set SPARK_LOCAL_IP=127.0.0.1
python spark_job.py

# 6. verify data flowing
docker exec -it postgres psql -U admin -d pipeline -c "SELECT * FROM orders_agg ORDER BY window_start DESC LIMIT 5;"
```