 # Structured Streaming job (Kafka → Postgres)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
topic = os.getenv("KAFKA_TOPIC","events")
pg_url = os.getenv("POSTGRES_URL","jdbc:postgresql://postgres:5432/market_pulse")
pg_user = os.getenv("POSTGRES_USER","app")
pg_pass = os.getenv("POSTGRES_PASSWORD","app")

spark = (SparkSession.builder
         .appName("kafka-to-postgres")
         .getOrCreate())

schema = StructType([
    StructField("source", StringType()),
    StructField("value", IntegerType()),
    StructField("category", StringType()),
    StructField("ingested_at", LongType())
])

df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .load())

df = (df_raw
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
    .withColumn("processed_ts", current_timestamp()))

def foreach_batch(batch_df, batch_id):
    (batch_df.write
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "public.events_stream")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

(df.writeStream
    .outputMode("append")
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", "/tmp/spark-checkpoints/events")
    .start()
    .awaitTermination())

trigger = os.getenv("SPARK_STREAMING_TRIGGER")  # e.g., "5s"
writer = (df.writeStream
    .outputMode("append")
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", "/tmp/spark-checkpoints/events"))

if trigger:
    writer = writer.trigger(processingTime=trigger)

query = writer.start()
query.awaitTermination()
