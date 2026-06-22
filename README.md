# Real-Time Streaming Pipeline

A production-grade real-time data pipeline that simulates a live e-commerce order stream, processes it using Apache Spark Structured Streaming, and stores aggregated results in PostgreSQL. Built as a learning project to understand streaming vs batch processing, event ordering, and fault tolerance patterns.

## Architecture
producer.py → Kafka → spark_job.py → PostgreSQL
↓
batch_job.py (reads aggregated results)

**producer.py** — generates realistic fake e-commerce orders continuously. Maintains order lifecycle consistency — a user must place an order before cancelling or returning it, with the same order_id and amount across events.

**Kafka** — acts as a durable, ordered message buffer between producer and consumer. Decouples producer and Spark, enables offset-based replay on restart.

**spark_job.py** — Spark Structured Streaming job that reads from Kafka, parses JSON events, aggregates revenue and order count into 1-minute windows, and writes results to PostgreSQL via upsert.

**batch_job.py** — standalone Spark batch job that reads aggregated data from PostgreSQL and runs analytical queries (highest/lowest revenue window, average revenue, running cumulative total).

---

## Stack

| Component | Version |
|---|---|
| Apache Spark | 3.5.0 |
| Apache Kafka | 7.4.0 (Confluent) |
| PostgreSQL | 15 |
| Python | 3.11.9 |
| Java | 11 |
| Docker Compose | v3.8 |

---

## Features

**Streaming**
- 1-minute windowed aggregation of order revenue and count
- Watermarking (2-minute grace period) for late-arriving events
- Upsert writes to PostgreSQL — no duplicate windows
- Checkpoint-based crash recovery — resumes from exact offset on restart
- Persistent PostgreSQL connection with automatic reconnection on failure
- Stateful order lifecycle tracking in producer (place → cancel/return)

**Batch**
- Highest and lowest revenue windows
- Average revenue per minute across all windows
- Cumulative revenue trend using Spark window functions

**Resilience tested**
- Spark job killed mid-run → resumes from checkpoint, no data loss
- PostgreSQL restarted mid-run → auto-reconnects, retries batch, no data loss
- Late data within watermark → accepted and included in window
- Late data outside watermark → silently dropped as expected

---

## Project Structure
real-time-streaming-kafka/
├── docker-compose.yml    # Kafka, Zookeeper, PostgreSQL
├── producer.py           # Fake order event generator
├── spark_job.py          # Spark Structured Streaming job
├── batch_job.py          # Spark batch analytics
├── requirements.txt      # Python dependencies
└── tmp/                  # Spark checkpoint directory (gitignored)

---

## Prerequisites

- Docker Desktop
- Python 3.11.x
- Java 11 or 17

---

## Setup

**1. Clone the repository**

```bash
git clone https://github.com/YourUsername/real-time-streaming-kafka.git
cd real-time-streaming-kafka
```

**2. Create and activate virtual environment**

```bash
python -m venv .venv

# Mac/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Start infrastructure**

```bash
docker-compose up -d
```

**5. Create Kafka topic**

```bash
docker exec -it kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**6. Create PostgreSQL table**

```bash
docker exec -it postgres psql -U admin -d pipeline -c "
CREATE TABLE orders_agg (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_revenue DOUBLE PRECISION,
    order_count BIGINT,
    CONSTRAINT unique_window UNIQUE (window_start, window_end)
);"
```

---

## Running the Pipeline

Open three terminals, all with the virtual environment activated.

**Terminal 1 — start the producer**

```bash
python producer.py
```

**Terminal 2 — start the Spark streaming job**

```bash
python spark_job.py
```

First run downloads Spark JARs (~2 minutes). Subsequent runs start immediately.

**Terminal 3 — run batch analytics (anytime)**

```bash
python batch_job.py
```

---

## Key Concepts Learned

**Streaming vs Batch**
Spark Structured Streaming treats a live stream as an unbounded table that keeps growing. The same DataFrame API used in batch processing applies — the difference is the data never ends, so results must be computed incrementally over time windows rather than across the full dataset.

**Micro-batch model**
Spark processes streams as continuous micro-batches rather than event-by-event. Every few seconds it collects arrived events, runs a mini Spark job on them, writes results, commits offsets, and repeats. This trades true real-time latency (milliseconds) for simplicity and throughput.

**Event time vs processing time**
An order placed at 14:42:00 might arrive at Spark at 14:44:30 due to network delay. Windowing uses the event's own timestamp (`ts`), not the time Spark received it. This ensures orders land in the correct minute bucket regardless of processing delay.

**Watermarking**
Since the stream is infinite, Spark can't keep every window open forever waiting for late data. A 2-minute watermark tells Spark: accept events up to 2 minutes late, then close the window permanently. Events arriving after that are dropped.

**Exactly-once semantics**
Achieved through the combination of Kafka offsets (tracks what's been read), Spark checkpointing (tracks processing state), and PostgreSQL upsert (prevents duplicate writes on retry). If Spark crashes mid-batch, on restart it replays from the last committed offset and the upsert ensures the retry doesn't create duplicate rows.

**Checkpointing**
Spark writes its processing state and Kafka offsets to a checkpoint directory after each successful micro-batch. On restart, it reads this state and resumes from exactly where it left off — no data is lost or reprocessed.

---

## Resilience Testing

**Test 1 — Spark crash recovery**
```bash
# Kill spark_job.py with Ctrl+C
# Wait 20 seconds (messages accumulate in Kafka)
# Restart spark_job.py
# Spark resumes from checkpoint, processes backlog
```

**Test 2 — PostgreSQL restart**
```bash
docker restart postgres
# Spark detects dead connection, reconnects automatically
# In-flight batch is retried, no data loss
```

**Test 3 — late data**
```bash
# Stop producer
# Inject message within watermark → accepted
docker exec -it kafka kafka-console-producer --broker-list localhost:9092 --topic orders
# paste: {"order_id": "test-001", "user_id": 999, "amount": 999.99, "status": "placed", "ts": "<timestamp within 2 min>"}

# Inject message outside watermark → silently dropped
# paste: {"order_id": "test-002", "user_id": 998, "amount": 888.88, "status": "placed", "ts": "<timestamp older than 2 min>"}
```

---

## Notes

- On Windows, Spark prints temp file cleanup errors on shutdown — these are cosmetic and do not affect correctness
- The checkpoint directory (`tmp/`) must be deleted if you change windowing logic, schema, or shuffle partition count — stale checkpoints cause task count and performance issues
- `spark.sql.shuffle.partitions` is set to 4 (default is 200) — appropriate for single-machine development, not production clusters