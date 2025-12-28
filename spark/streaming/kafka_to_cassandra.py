from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StringType


# ---------- 1. Kafka message schema ----------
schema = StructType() \
    .add("create_time", StringType()) \
    .add("bid", StringType()) \
    .add("bn", StringType()) \
    .add("campaign_id", StringType()) \
    .add("cd", StringType()) \
    .add("custom_track", StringType()) \
    .add("de", StringType()) \
    .add("dl", StringType()) \
    .add("dt", StringType()) \
    .add("ev", StringType()) \
    .add("group_id", StringType()) \
    .add("id", StringType()) \
    .add("job_id", StringType()) \
    .add("md", StringType()) \
    .add("publisher_id", StringType()) \
    .add("rl", StringType()) \
    .add("sr", StringType()) \
    .add("ts", StringType()) \
    .add("tz", StringType()) \
    .add("ua", StringType()) \
    .add("uid", StringType()) \
    .add("utm_campaign", StringType()) \
    .add("utm_content", StringType()) \
    .add("utm_medium", StringType()) \
    .add("utm_source", StringType()) \
    .add("utm_term", StringType()) \
    .add("v", StringType()) \
    .add("vp", StringType())

# ---------- 2. Create SparkSession ----------
import os

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "tracking_events")

spark = (
    SparkSession.builder
        .appName("KafkaToCassandra")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            + os.getenv("CASSANDRA_SPARK_CONNECTOR", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
        )
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
        .getOrCreate()
)

kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
)

# ---------- 4. Parse JSON into structured columns ----------
parsed_df = (
    kafka_df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .withColumn("ts_ts", to_timestamp(col("ts")))  # convert to Spark timestamp
)

parsed_df = parsed_df.withColumn("event_date", to_date(col("ts")))

def write_to_cassandra(batch_df, batch_id):

    # Only keep records with valid ts
    cleaned_df = (
        batch_df.filter(col("ts_ts").isNotNull())
                .select("create_time",
                    "event_date",
                    col("ts_ts").alias("ts"),
                    "id",
                    "job_id",
                    "publisher_id",
                    "campaign_id",
                    "group_id",
                    "uid",
                    "ua",
                    "ev",
                    "dl",
                    "dt",
                    "de",
                    "cd",
                    "bn",
                    "bid",
                    "rl",
                    "sr",
                    "md",
                    "custom_track",
                    "utm_campaign",
                    "utm_content",
                    "utm_medium",
                    "utm_source",
                    "utm_term",
                    "v",
                    "vp",
                    "tz"
                )
    )

    cleaned_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "my_keyspace") \
        .option("table", "tracking_new") \
        .mode("append") \
        .save()


# ---------- 5. Write micro-batches into Cassandra ----------
# def write_to_cassandra(batch_df, batch_id):
#     # remove invalid create_time (just in case)
#     cleaned_df = batch_df.filter(col("ts_ts").isNotNull())
#     cleaned_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .option("keyspace", "my_keyspace") \
#         .option("table", "tracking_new") \
#         .mode("append") \
#         .save()

query = (
    parsed_df.writeStream
        .foreachBatch(write_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra")
        .start()
)

query.awaitTermination()
