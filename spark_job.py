from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import psycopg2
import time

KAFKA_BROKER = "localhost:9092"

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pipeline",
    "user": "admin",
    "password": "password"
}

pg_conn = None
CHECKPOINT_DIR = r"D:/real-time-streaming-kafka/tmp/spark_checkpoint"

spark = SparkSession.builder \
    .appName("OrderStreamProcessor") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "4")

print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("order_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("status", StringType()) \
    .add("ts", StringType())

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ts", col("ts").cast("timestamp"))

agg_df = parsed_df \
    .withWatermark("ts", "2 minutes") \
    .groupBy(window(col("ts"), "1 minute")) \
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_revenue"),
        col("order_count")
    )

def get_connection():
    global pg_conn
    max_retries = 5
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            if pg_conn is not None:
                try:
                    pg_conn.close()
                except:
                    pass
            pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
            print(f"Postgres connected (attempt {attempt + 1})")
            return pg_conn
        except psycopg2.OperationalError as e:
            print(f"Postgres connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Could not connect to Postgres after {max_retries} attempts") from e

def ensure_connection():
    global pg_conn
    if pg_conn is None or pg_conn.closed:
        return get_connection()
    try:
        # lightweight check — runs a trivial query to ping the connection
        cur = pg_conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return pg_conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        print("Postgres connection lost, reconnecting...")
        return get_connection()

def write_to_postgres(batch_df, batch_id):
    global pg_conn
    rows = batch_df.collect()
    if not rows:
        return

    conn = ensure_connection()
    cur = conn.cursor()

    try:
        for row in rows:
            cur.execute("""
                INSERT INTO orders_agg (window_start, window_end, total_revenue, order_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (window_start, window_end)
                DO UPDATE SET
                    total_revenue = EXCLUDED.total_revenue,
                    order_count = EXCLUDED.order_count
            """, (row.window_start, row.window_end, row.total_revenue, row.order_count))

        conn.commit()
        cur.close()
        print(f"Batch {batch_id} upserted to Postgres ({len(rows)} windows)")

    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        # connection dropped mid-write — rollback, reconnect, retry once
        print(f"Connection lost mid-batch, retrying batch {batch_id}...")
        try:
            conn.rollback()
        except:
            pass
        cur.close()

        conn = get_connection()
        cur = conn.cursor()

        for row in rows:
            cur.execute("""
                INSERT INTO orders_agg (window_start, window_end, total_revenue, order_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (window_start, window_end)
                DO UPDATE SET
                    total_revenue = EXCLUDED.total_revenue,
                    order_count = EXCLUDED.order_count
            """, (row.window_start, row.window_end, row.total_revenue, row.order_count))

        conn.commit()
        cur.close()
        print(f"Batch {batch_id} upserted after reconnection ({len(rows)} windows)")


get_connection()

query = agg_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .outputMode("update") \
    .start()

query.awaitTermination()
