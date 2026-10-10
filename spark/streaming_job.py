"""Kafka-to-MinIO and MongoDB real-time order analytics job.

Raw storage and analytics deliberately use separate Structured Streaming queries and
checkpoints. A failed sink therefore cannot be mistaken for success in the other.
"""

import os
import sys
import time
from datetime import timezone

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, from_json, lit, sum as _sum, to_timestamp, window
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "order-events")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "orders-raw")
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE", "order_analytics")
MONGODB_COLLECTION = os.environ.get("MONGODB_COLLECTION", "sales_metrics")

# Change a suffix when an incompatible stateful query is introduced.
RAW_CHECKPOINT = f"s3a://{MINIO_BUCKET}/checkpoints/raw-events-v1"
INVALID_CHECKPOINT = f"s3a://{MINIO_BUCKET}/checkpoints/invalid-events-v1"
ANALYTICS_CHECKPOINT = f"s3a://{MINIO_BUCKET}/checkpoints/analytics-v2"

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True), StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True), StructField("price", LongType(), True),
    StructField("city", StringType(), True), StructField("timestamp", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Create Spark with Kafka and S3A/MinIO support."""
    packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"]
    return (SparkSession.builder.appName("RealTimeOrderPipeline")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())


def source_orders(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Return valid orders and a separately persisted invalid-event stream."""
    kafka = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    decoded = kafka.select(col("value").cast("string").alias("raw_value"), col("topic"),
        col("partition"), col("offset"), col("timestamp").alias("kafka_timestamp"))
    parsed = decoded.withColumn("order", from_json(col("raw_value"), ORDER_SCHEMA))
    typed = (parsed.select("raw_value", "topic", "partition", "offset", "kafka_timestamp", "order.*")
        .withColumn("event_time", to_timestamp(col("timestamp"))).drop("timestamp"))
    valid = ((col("order_id").isNotNull()) & (col("product").isNotNull()) &
        (col("city").isNotNull()) & col("event_time").isNotNull() &
        (col("quantity") > 0) & (col("price") > 0))
    valid_orders = typed.filter(valid).withColumn("sales", col("quantity") * col("price"))
    invalid_orders = (typed.filter(~valid).select("raw_value", "topic", "partition", "offset", "kafka_timestamp")
        .withColumn("error", lit("Malformed JSON or invalid required order field")))
    return valid_orders, invalid_orders


def write_metrics_to_mongo(batch: DataFrame, batch_id: int) -> None:
    """Upsert changed windows, making live UPDATE-mode results safe to serve."""
    rows = batch.collect()
    if not rows:
        return
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
    collection = client[MONGODB_DATABASE][MONGODB_COLLECTION]
    operations = []
    for row in rows:
        document = row.asDict(recursive=True)
        interval = document.pop("window")
        start = interval["start"].replace(tzinfo=timezone.utc).isoformat()
        end = interval["end"].replace(tzinfo=timezone.utc).isoformat()
        document.update(window_start=start, window_end=end, batch_id=batch_id)
        identity = {"metric_type": document["metric_type"], "window_start": start,
                    "product": document.get("product")}
        operations.append(ReplaceOne(identity, document, upsert=True))
    collection.bulk_write(operations, ordered=False)
    collection.create_index([("window_start", -1), ("metric_type", 1)])
    print(f"MongoDB: upserted {len(operations)} metric document(s) from batch {batch_id}", flush=True)
    client.close()


def start_streaming_queries(spark: SparkSession):
    """Start independent raw, quarantine, and analytics queries."""
    valid_orders, invalid_orders = source_orders(spark)
    raw_query = (valid_orders.drop("raw_value").writeStream.outputMode("append").format("parquet")
        .option("path", f"s3a://{MINIO_BUCKET}/events").option("checkpointLocation", RAW_CHECKPOINT)
        .queryName("raw-events-to-minio").start())
    invalid_query = (invalid_orders.writeStream.outputMode("append").format("parquet")
        .option("path", f"s3a://{MINIO_BUCKET}/invalid-events").option("checkpointLocation", INVALID_CHECKPOINT)
        .queryName("invalid-events-to-minio").start())

    # This bounds state: events more than one minute behind the maximum observed
    # event time may be discarded from the five-minute windows.
    event_time = valid_orders.withWatermark("event_time", "1 minute")
    product_metrics = (event_time.groupBy(window(col("event_time"), "5 minutes"), col("product"))
        .agg(_sum("sales").alias("total_sales"), count("order_id").alias("order_count"),
             _sum("quantity").alias("total_quantity")).withColumn("metric_type", lit("product")))
    window_metrics = (event_time.groupBy(window(col("event_time"), "5 minutes"))
        .agg(_sum("sales").alias("total_sales"), count("order_id").alias("order_count"),
             _sum("quantity").alias("total_quantity"))
        .withColumn("product", lit(None).cast("string")).withColumn("metric_type", lit("window")))
    analytics = product_metrics.unionByName(window_metrics)
    analytics_query = (analytics.writeStream.outputMode("update").foreachBatch(write_metrics_to_mongo)
        .option("checkpointLocation", ANALYTICS_CHECKPOINT)
        .queryName("windowed-analytics-to-mongodb").start())
    print("Started independent raw, invalid-record, and analytics streaming queries.", flush=True)
    return raw_query, invalid_query, analytics_query


def main() -> None:
    spark = None
    queries = []
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        queries = start_streaming_queries(spark)
        while True:
            for query in queries:
                if query.exception() is not None:
                    raise RuntimeError(f"Streaming query {query.name} failed: {query.exception()}")
                progress = query.lastProgress
                if progress:
                    print(f"{query.name}: batch={progress['batchId']} input={progress['numInputRows']}", flush=True)
            time.sleep(10)
    except KeyboardInterrupt:
        print("Spark streaming stopped by user.")
    except Exception as error:
        print(f"Spark streaming error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
