"""
Spark Structured Streaming consumer for real-time Touch 'n Go sentiment analysis.

Kafka topic -> Spark micro-batch -> TF-IDF + Naive Bayes prediction -> Elasticsearch.
"""

import os
import sys
import time
from datetime import datetime, UTC

import joblib
import pandas as pd
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sentiment_preprocess import clean_text


KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "tng_reviews"

MODEL_PATH = "models/naive_bayes_model.pkl"
VECTORIZER_PATH = "models/vectorizer.pkl"

ELASTICSEARCH_URL = "http://localhost:9200"
ELASTICSEARCH_INDEX = "tng_sentiment_stream"

CHECKPOINT_DIR = "checkpoints/tng_sentiment_stream"

# Streaming pipeline metrics output
RESULT_DIR = "results"
STREAM_METRICS_PATH = os.path.join(RESULT_DIR, "stream_pipeline_metrics.csv")

os.makedirs(RESULT_DIR, exist_ok=True)

# Start a fresh metrics file for each new Spark run
if os.path.exists(STREAM_METRICS_PATH):
    os.remove(STREAM_METRICS_PATH)


print("Loading vectorizer and model...")
vectorizer = joblib.load(VECTORIZER_PATH)
model = joblib.load(MODEL_PATH)
print("Model loaded successfully.")


es = Elasticsearch(ELASTICSEARCH_URL)

if not es.ping():
    raise ConnectionError("Cannot connect to Elasticsearch. Make sure Docker containers are running.")

if not es.indices.exists(index=ELASTICSEARCH_INDEX):
    es.indices.create(
        index=ELASTICSEARCH_INDEX,
        mappings={
            "properties": {
                "review_id": {"type": "keyword"},
                "review_text": {"type": "text"},
                "cleaned_review": {"type": "text"},
                "rating": {"type": "integer"},
                "actual_sentiment": {"type": "keyword"},
                "predicted_sentiment": {"type": "keyword"},
                "prediction_confidence": {"type": "float"},
                "review_datetime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
                },
                "processed_at": {"type": "date"},
                "source": {"type": "keyword"}
            }
        }
    )
    print(f"Created Elasticsearch index: {ELASTICSEARCH_INDEX}")
else:
    print(f"Using existing Elasticsearch index: {ELASTICSEARCH_INDEX}")


spark = (
    SparkSession.builder
    .appName("TNG Real-Time Sentiment Analysis")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


message_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("review_text", StringType(), True),
    StructField("cleaned_review", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("actual_sentiment", StringType(), True),
    StructField("review_datetime", StringType(), True)
])


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "200")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), message_schema).alias("data"))
    .select("data.*")
)


def process_batch(batch_df, batch_id):
    start_time = time.perf_counter()

    record_count = batch_df.count()
    if record_count == 0:
        return

    pdf = batch_df.toPandas()

    if pdf.empty:
        return

    def choose_cleaned(row):
        cleaned = row.get("cleaned_review", "")
        if pd.notna(cleaned) and str(cleaned).strip() != "":
            return str(cleaned)
        return clean_text(row.get("review_text", ""))

    pdf["cleaned_for_prediction"] = pdf.apply(choose_cleaned, axis=1)

    X_tfidf = vectorizer.transform(pdf["cleaned_for_prediction"].astype(str))
    predictions = model.predict(X_tfidf)
    pdf["predicted_sentiment"] = predictions

    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(X_tfidf)
        pdf["prediction_confidence"] = probabilities.max(axis=1)
    else:
        pdf["prediction_confidence"] = None

    pdf["processed_at"] = datetime.now(UTC).isoformat()
    pdf["source"] = "spark_streaming"

    documents = []

    for _, row in pdf.iterrows():
        rating_value = row.get("rating", 0)
        confidence_value = row.get("prediction_confidence", None)

        doc = {
            "review_id": str(row.get("review_id", "")),
            "review_text": str(row.get("review_text", "")),
            "cleaned_review": str(row.get("cleaned_for_prediction", "")),
            "rating": int(rating_value) if pd.notna(rating_value) else 0,
            "actual_sentiment": str(row.get("actual_sentiment", "")),
            "predicted_sentiment": str(row.get("predicted_sentiment", "")),
            "prediction_confidence": float(confidence_value) if pd.notna(confidence_value) else None,
            "review_datetime": str(row.get("review_datetime", "")),
            "processed_at": row.get("processed_at"),
            "source": "spark_streaming"
        }

        documents.append(doc)

    for doc in documents:
        es.index(index=ELASTICSEARCH_INDEX, document=doc)

    elapsed_time = time.perf_counter() - start_time
    throughput = len(documents) / elapsed_time if elapsed_time > 0 else 0

    # Save streaming pipeline metrics for later batch-vs-streaming comparison
    metrics_row = pd.DataFrame([{
        "mode": "Streaming",
        "batch_id": batch_id,
        "records_processed": len(documents),
        "processing_time_seconds": elapsed_time,
        "throughput_records_per_second": throughput,
        "processed_at": datetime.now(UTC).isoformat()
    }])

    metrics_row.to_csv(
        STREAM_METRICS_PATH,
        mode="a",
        header=not os.path.exists(STREAM_METRICS_PATH),
        index=False
    )

    print(
        f"Batch {batch_id}: processed {len(documents)} records | "
        f"time={elapsed_time:.2f}s | throughput={throughput:.2f} records/sec"
    )


query = (
    parsed_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark streaming started.")
print("Waiting for Kafka messages...")
print("Press Ctrl+C to stop.")

query.awaitTermination()
