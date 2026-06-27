import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import lit, col
from pyflink.common import Types

os.environ["JAVA_TOOL_OPTIONS"] = "-Duser.timezone=Asia/Kolkata"

KAFKA_BROKER = "localhost:9092"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/pipeline"
POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "password"

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)
table_env.get_config().set("table.exec.timezone", "Asia/Kolkata")

table_env.get_config().set("pipeline.jars", 
    "file:///D:/real-time-streaming-kafka/flink_jars/flink-sql-connector-kafka-3.0.2-1.18.jar;"
    "file:///D:/real-time-streaming-kafka/flink_jars/flink-connector-jdbc-3.1.2-1.18.jar;"
    "file:///D:/real-time-streaming-kafka/flink_jars/postgresql-42.6.0.jar"
)

table_env.execute_sql("""
    CREATE TABLE orders (
        order_id STRING,
        user_id INT,
        amount DOUBLE,
        status STRING,
        ts STRING,
        event_time AS TO_TIMESTAMP(ts, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS+05:30'),
        WATERMARK FOR event_time AS event_time - INTERVAL '2' MINUTE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'orders',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-consumer',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
""")

table_env.execute_sql("""
    CREATE TABLE orders_flink_agg (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        total_revenue DOUBLE,
        order_count BIGINT,
        PRIMARY KEY (window_start, window_end) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/pipeline',
        'table-name' = 'orders_flink_agg',
        'username' = 'admin',
        'password' = 'password',
        'driver' = 'org.postgresql.Driver'
    )
""")

table_env.execute_sql("""
    INSERT INTO orders_flink_agg
    SELECT
        TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
        TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
        SUM(amount) as total_revenue,
        COUNT(order_id) as order_count
    FROM orders
    WHERE status = 'placed'
    GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
""").wait()