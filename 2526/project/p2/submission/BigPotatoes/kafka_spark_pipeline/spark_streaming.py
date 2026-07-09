import time
import psutil
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# -----------------------------
# Paths
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

MODEL_PATH = PROJECT_DIR / "models" / "best_sentiment_model.pkl"
OUTPUT_CSV = BASE_DIR / "streaming_prediction.csv"
CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "sentiment_stream"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sentiment-topic"

ELASTICSEARCH_URL = "http://localhost:9200"
ELASTIC_INDEX = "sentiment_predictions"


# -----------------------------
# Load trained model
# -----------------------------
model = joblib.load(MODEL_PATH)

label_map = {
    0: "Negative",
    1: "Neutral",
    2: "Positive"
}


# -----------------------------
# Performance tracking
# -----------------------------
total_records_processed = 0
total_processing_time = 0.0
cpu_usage_values = []
peak_memory_mb = 0.0

current_process = psutil.Process(os.getpid())

def get_total_cpu_seconds(process):
    total = 0.0

    try:
        cpu_times = process.cpu_times()
        total += cpu_times.user + cpu_times.system

        for child in process.children(recursive=True):
            try:
                child_times = child.cpu_times()
                total += child_times.user + child_times.system
            except psutil.NoSuchProcess:
                pass

    except psutil.NoSuchProcess:
        pass

    return total

# -----------------------------
# Elasticsearch setup
# -----------------------------
def get_elasticsearch_client():
    try:
        es = Elasticsearch(ELASTICSEARCH_URL, request_timeout=120)

        if not es.ping():
            print("Warning: Elasticsearch is not reachable. CSV output will still be saved.")
            return None

        if not es.indices.exists(index=ELASTIC_INDEX):
            es.indices.create(
                index=ELASTIC_INDEX,
                mappings={
                    "properties": {
                        "review_id": {"type": "keyword"},
                        "rating": {"type": "integer"},
                        "review_date": {"type": "date", "ignore_malformed": True},
                        "app_version": {"type": "keyword"},
                        "thumbs_up_count": {"type": "integer"},
                        "original_text": {"type": "text"},
                        "cleaned_text": {"type": "text"},
                        "produced_timestamp": {"type": "date", "ignore_malformed": True},
                        "spark_received_timestamp": {"type": "date", "ignore_malformed": True},
                        "predicted_label": {"type": "integer"},
                        "predicted_sentiment": {"type": "keyword"},
                        "processed_timestamp": {"type": "date", "ignore_malformed": True}
                    }
                }
            )
            print(f"Created Elasticsearch index: {ELASTIC_INDEX}")

        return es

    except Exception as e:
        print(f"Warning: Elasticsearch setup failed: {e}")
        return None


es_client = get_elasticsearch_client()


# -----------------------------
# Clean data before Elasticsearch
# -----------------------------
def prepare_for_elasticsearch(pdf):
    pdf = pdf.copy()

    # Convert numeric fields safely
    for col_name in ["rating", "thumbs_up_count", "predicted_label"]:
        if col_name in pdf.columns:
            pdf[col_name] = pd.to_numeric(pdf[col_name], errors="coerce")

    # Convert timestamp/date fields safely
    date_cols = [
        "review_date",
        "produced_timestamp",
        "spark_received_timestamp",
        "processed_timestamp"
    ]

    for col_name in date_cols:
        if col_name in pdf.columns:
            pdf[col_name] = pd.to_datetime(pdf[col_name], errors="coerce", utc=True)
            pdf[col_name] = pdf[col_name].apply(
                lambda x: x.isoformat() if pd.notnull(x) else None
            )

    # Replace NaN / NaT with None
    pdf = pdf.replace({np.nan: None})

    return pdf


# -----------------------------
# Print performance summary
# -----------------------------
def print_performance_summary(batch_id, batch_count, batch_time, batch_throughput,
                              cpu_usage, memory_usage, es_success, es_failed):
    global total_records_processed, total_processing_time, cpu_usage_values, peak_memory_mb

    total_throughput = (
        total_records_processed / total_processing_time
        if total_processing_time > 0 else 0
    )

    average_cpu = (
        sum(cpu_usage_values) / len(cpu_usage_values)
        if len(cpu_usage_values) > 0 else 0
    )

    print("=" * 72)
    print("STREAMING MODE - PERFORMANCE METRICS")
    print("=" * 72)
    print(f"Batch ID                         : {batch_id}")
    print(f"Records processed in this batch  : {batch_count} records")
    print(f"Batch processing time            : {batch_time:.4f} seconds")
    print(f"Batch throughput                  : {batch_throughput:.2f} records/second")
    print("-" * 72)
    print(f"Total records processed          : {total_records_processed} records")
    print(f"Total processing time            : {total_processing_time:.4f} seconds")
    print(f"Overall throughput               : {total_throughput:.2f} records/second")
    print(f"CPU utilization rate             : {cpu_usage:.2f}%")
    print(f"Average CPU utilization          : {average_cpu:.2f}%")
    print(f"Peak memory consumption          : {peak_memory_mb:.2f} MB")
    print("-" * 72)
    print(f"Elasticsearch success documents  : {es_success}")
    print(f"Elasticsearch failed documents   : {es_failed}")
    print("=" * 72)


# -----------------------------
# Process each Spark micro-batch
# -----------------------------
def process_batch(batch_df, batch_id):
    global es_client
    global total_records_processed, total_processing_time, cpu_usage_values, peak_memory_mb

    batch_start_time = time.perf_counter()
    batch_cpu_start = get_total_cpu_seconds(current_process)

    count = batch_df.count()

    if count == 0:
        return

    print(f"\nProcessing batch {batch_id} with {count} records...")

    pdf = batch_df.toPandas()

    pdf["cleaned_text"] = pdf["cleaned_text"].fillna("").astype(str)

    predictions = model.predict(pdf["cleaned_text"])

    pdf["predicted_label"] = predictions
    pdf["predicted_sentiment"] = pdf["predicted_label"].map(label_map)
    pdf["processed_timestamp"] = pd.Timestamp.now(tz="UTC").isoformat()

    # Save to CSV
    file_exists = OUTPUT_CSV.exists()
    pdf.to_csv(
        OUTPUT_CSV,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8"
    )

    print(f"Saved batch {batch_id} to {OUTPUT_CSV}")

    # Save to Elasticsearch
    es_success = 0
    es_failed = 0

    if es_client is not None:
        try:
            es_pdf = prepare_for_elasticsearch(pdf)

            actions = []
            for i, row in es_pdf.iterrows():
                doc = row.to_dict()
                doc_id = doc.get("review_id") if doc.get("review_id") else f"{batch_id}_{i}"

                actions.append({
                    "_index": ELASTIC_INDEX,
                    "_id": doc_id,
                    "_source": doc
                })

            es_success, es_failed = helpers.bulk(
                es_client,
                actions,
                raise_on_error=False,
                raise_on_exception=False,
                stats_only=True,
                chunk_size=500,
                request_timeout=120
            )

            print(f"Saved {es_success} documents to Elasticsearch index: {ELASTIC_INDEX}")

            if es_failed > 0:
                print(f"Warning: {es_failed} documents failed to index.")

        except Exception as e:
            print(f"Warning: Failed to write batch {batch_id} to Elasticsearch: {e}")

    # Performance calculation
    batch_end_time = time.perf_counter()
    batch_time = batch_end_time - batch_start_time
    batch_throughput = count / batch_time if batch_time > 0 else 0

    batch_cpu_end = get_total_cpu_seconds(current_process)
    cpu_time_used = batch_cpu_end - batch_cpu_start
    cpu_usage = (cpu_time_used / batch_time) * 100 if batch_time > 0 else 0

    memory_usage = current_process.memory_info().rss / (1024 * 1024)

    total_records_processed += count
    total_processing_time += batch_time
    cpu_usage_values.append(cpu_usage)
    peak_memory_mb = max(peak_memory_mb, memory_usage)

    print_performance_summary(
        batch_id=batch_id,
        batch_count=count,
        batch_time=batch_time,
        batch_throughput=batch_throughput,
        cpu_usage=cpu_usage,
        memory_usage=memory_usage,
        es_success=es_success,
        es_failed=es_failed
    )


# -----------------------------
# Spark Streaming
# -----------------------------
spark = (
    SparkSession.builder
    .appName("RealTimeSentimentAnalysis")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_date", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("thumbs_up_count", IntegerType(), True),
    StructField("original_text", StringType(), True),
    StructField("cleaned_text", StringType(), True),
    StructField("produced_timestamp", StringType(), True)
])

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_stream = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
    .withColumn("spark_received_timestamp", current_timestamp())
)

query = (
    parsed_stream.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .start()
)

print("Spark streaming started.")
print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
print("Press Ctrl + C to stop.")

query.awaitTermination()