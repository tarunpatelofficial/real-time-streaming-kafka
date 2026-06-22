from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import avg
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

spark = (
    SparkSession.builder
    .appName("PostgresRead")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0"
    )
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
    .getOrCreate()
)

df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        "jdbc:postgresql://localhost:5432/pipeline"
    )
    .option("dbtable", "orders_agg")
    .option("user", "admin")
    .option("password", "password")
    .option("driver", "org.postgresql.Driver")
    .load()
)

top_window = (
    df
    .orderBy(desc("total_revenue"))
    .limit(1)
)

lowest_window = (
    df
    .orderBy("total_revenue")
    .limit(1)
)


avg_revenue = df.agg(
    avg("total_revenue").alias("avg_revenue")
)

window_spec = Window.orderBy("window_start").rowsBetween(Window.unboundedPreceding, Window.currentRow)

revenue_trend = df \
    .orderBy("window_start") \
    .withColumn("cumulative_revenue", spark_sum("total_revenue").over(window_spec))

window_spec = Window.orderBy("window_start").rowsBetween(Window.unboundedPreceding, Window.currentRow)

revenue_trend = df \
    .orderBy("window_start") \
    .withColumn("cumulative_revenue", spark_sum("total_revenue").over(window_spec))

top_window.show(truncate=False)
lowest_window.show(truncate=False)
avg_revenue.show(truncate=False)
revenue_trend.select("window_start", "total_revenue", "cumulative_revenue").show(truncate=False)