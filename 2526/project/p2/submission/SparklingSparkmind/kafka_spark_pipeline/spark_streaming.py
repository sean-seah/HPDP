import sys
import pickle
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, struct, to_json, current_timestamp, 
    udf, explode, split, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import time
import os
import requests

print("=" * 70)
print("SPARK STREAMING JOB - SENTIMENT ANALYSIS (3-CLASS)")
print("Shopee Reviews: Negative | Neutral | Positive [REST-MODE RUN]")
print("=" * 70)

# [1/5] Initialize Spark Session with single-thread for Windows stability
print("\n[1/5] Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("SentimentAnalysisStreaming") \
    .master("local[1]") \
    .config("spark.streaming.kafka.maxRatePerPartition", "500") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark Session created")

# [2/5] Load trained models
print("\n[2/5] Loading trained models from Member 2...")
try:
    with open('naive_bayes_model.pkl', 'rb') as f:
        nb_saved = pickle.load(f)
        nb_model = nb_saved['model']
        tfidf_vectorizer = nb_saved['vectorizer']
    print("✓ Naive Bayes model + TF-IDF vectorizer loaded")
    label_to_sentiment = {0: 'negative', 1: 'neutral', 2: 'positive'}
    print("✓ Label mappings ready")
except FileNotFoundError as e:
    print(f"❌ ERROR: Model files not found!\n{e}")
    sys.exit(1)

kafka_schema = StructType([
    StructField("review_id", StringType()),
    StructField("review_text", StringType()),
    StructField("cleaned_text", StringType()),
    StructField("true_sentiment", StringType()),  
    StructField("star_rating", LongType()),
    StructField("review_date", StringType()),
    StructField("source", StringType())
])

# [3/5] Read from Kafka
print("\n[3/5] Connecting to Kafka topic 'sentiment-input'...")
try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sentiment-input") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    print("✓ Connected to Kafka")
except Exception as e:
    print(f"❌ ERROR: Cannot connect to Kafka\n{e}")
    sys.exit(1)

parsed_df = df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

nb_model_broadcast = spark.sparkContext.broadcast((nb_model, tfidf_vectorizer))
label_map_broadcast = spark.sparkContext.broadcast(label_to_sentiment)

@udf(returnType=StringType())
def predict_naive_bayes(text):
    try:
        model, vectorizer = nb_model_broadcast.value
        vec = vectorizer.transform([text])
        pred_label = model.predict(vec)[0]
        label_map = label_map_broadcast.value
        return label_map.get(int(pred_label), 'unknown')
    except Exception as e:
        return 'error'

@udf(returnType=DoubleType())
def predict_naive_bayes_confidence(text):
    try:
        model, vectorizer = nb_model_broadcast.value
        vec = vectorizer.transform([text])
        proba = model.predict_proba(vec)[0]
        return float(max(proba))
    except Exception as e:
        return 0.0

# [4/5] Apply predictions
print("\n[4/5] Building prediction pipeline...")
predictions_df = parsed_df \
    .withColumn("nb_sentiment", predict_naive_bayes(col("cleaned_text"))) \
    .withColumn("nb_confidence", predict_naive_bayes_confidence(col("cleaned_text"))) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("is_correct_nb", col("nb_sentiment") == col("true_sentiment"))

print("✓ Prediction pipeline ready")

# [5/5] Write to Elasticsearch
print("\n[5/5] Starting streaming to Elasticsearch via REST API...")

def send_batch_to_es(df, batch_id):
    records = df.toJSON().collect()
    if not records:
        return
    bulk_data = ""
    for record in records:
        data_dict = json.loads(record)
        review_id = data_dict.get("review_id")
        bulk_data += json.dumps({"index": {"_index": "sentiment-predictions", "_id": review_id}}) + "\n"
        bulk_data += json.dumps(data_dict) + "\n"
    try:
        url = "http://localhost:9200/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}
        response = requests.post(url, data=bulk_data, headers=headers)
        if response.status_code == 200:
            print(f"✓ [Batch {batch_id}] Successfully indexed records into Elasticsearch!")
    except Exception as e:
        print(f"❌ Failed to send batch {batch_id} to ES: {e}")

try:
    query = predictions_df \
        .writeStream \
        .foreachBatch(send_batch_to_es) \
        .option("checkpointLocation", r"C:\Users\safiy\Project-SentimentAnalysis\kafka_spark_pipeline\checkpoint_es") \
        .start()
    
    print("\n" + "=" * 70)
    print("STREAMING ACTIVE - Data is floating into Elasticsearch!")
    print("=" * 70)
    print("\nDashboard: http://localhost:5601")
    print("\nPress Ctrl+C to stop streaming")
    print("=" * 70 + "\n")
    query.awaitTermination()
except Exception as e:
    print(f"❌ ERROR: {e}")
    sys.exit(1)