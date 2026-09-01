from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("UrbanSensorsStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("air_quality_index", IntegerType()),
    StructField("timestamp", StringType())
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "urban_sensors") \
    .load()

json_df = raw_df.selectExpr(
    "CAST(value AS STRING)"
)

parsed_df = json_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

aggregated_df = parsed_df.groupBy(
    window(col("event_time"), "1 minute"),
    col("sensor_id")
).agg(
    avg("temperature").alias("avg_temperature"),
    avg("air_quality_index").alias("avg_air_quality")
)

query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
