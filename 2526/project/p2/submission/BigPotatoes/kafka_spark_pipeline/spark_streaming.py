import os
from pathlib import Path

import joblib
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
# Elasticsearch setup
# -----------------------------
def get_elasticsearch_client():
    try:
        es = Elasticsearch(ELASTICSEARCH_URL)

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
                        "predicted_label": {"type": "integer"},
                        "predicted_sentiment": {"type": "keyword"},
                        "produced_timestamp": {"type": "date", "ignore_malformed": True},
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
# Process each Spark micro-batch
# -----------------------------
def process_batch(batch_df, batch_id):
    global es_client

    count = batch_df.count()

    if count == 0:
        return

    print(f"\nProcessing batch {batch_id} with {count} records...")

    pdf = batch_df.toPandas()

    pdf["cleaned_text"] = pdf["cleaned_text"].fillna("").astype(str)

    predictions = model.predict(pdf["cleaned_text"])

    pdf["predicted_label"] = predictions
    pdf["predicted_sentiment"] = pdf["predicted_label"].map(label_map)
    pdf["processed_timestamp"] = pd.Timestamp.utcnow().isoformat()

    # Save to CSV
    file_exists = OUTPUT_CSV.exists()
    pdf.to_csv(OUTPUT_CSV, mode="a", header=not file_exists, index=False, encoding="utf-8")

    print(f"Saved batch {batch_id} to {OUTPUT_CSV}")

    # Save to Elasticsearch
    if es_client is not None:
        try:
            pdf = pdf.where(pd.notnull(pdf), None)

            actions = [
                {
                    "_index": ELASTIC_INDEX,
                    "_source": row
                }
                for row in pdf.to_dict(orient="records")
            ]

            helpers.bulk(es_client, actions)
            print(f"Saved batch {batch_id} to Elasticsearch index: {ELASTIC_INDEX}")

        except Exception as e:
            print(f"Warning: Failed to write batch {batch_id} to Elasticsearch: {e}")


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