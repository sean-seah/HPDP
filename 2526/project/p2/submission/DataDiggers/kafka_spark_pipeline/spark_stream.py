from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

import pandas as pd
import re
import joblib
import numpy as np

from elasticsearch import Elasticsearch


# =========================
# 1. Elasticsearch (FIXED)
# =========================
es = Elasticsearch("http://localhost:9200")
INDEX = "google_play_sentiment"


# =========================
# 2. Load ML Model (B)
# =========================
model = joblib.load("models/naive_bayes_model.pkl")
tfidf = joblib.load("models/tfidf_vectorizer.pkl")
label_encoder = joblib.load("models/label_encoder.pkl")


# =========================
# 3. Spark Session
# =========================
spark = SparkSession.builder \
    .appName("GooglePlaySentimentStream") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =========================
# 4. Kafka Schema
# =========================
schema = StructType() \
    .add("app", StringType()) \
    .add("review", StringType()) \
    .add("rating", DoubleType()) \
    .add("review_date", StringType()) \
    .add("stream_timestamp", DoubleType())


# =========================
# 5. Read Kafka Stream
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "google-play-reviews") \
    .load()


parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# =========================
# 6. Clean Text (A logic)
# =========================
def clean_text(text):
    text = str(text).lower()
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# =========================
# 7. Batch Processing
# =========================
def process_batch(batch_df, batch_id):

    pdf = batch_df.toPandas()

    if pdf.empty:
        return

    # preprocessing
    pdf["processed_text"] = pdf["review"].apply(clean_text)

    # vectorize
    X = tfidf.transform(pdf["processed_text"])

    # predict
    preds = model.predict(X)

    # convert label
    pdf["sentiment"] = label_encoder.inverse_transform(preds)

    print("\n================ Batch", batch_id, "================")
    print(pdf[["app", "review", "sentiment"]])

    # =========================
    # SEND TO ELASTICSEARCH
    # =========================
    for _, row in pdf.iterrows():

        doc = {
            "app": row["app"],
            "review": row["review"],
            "sentiment": row["sentiment"],
            "rating": float(row["rating"]),
            "review_date": row["review_date"],
            "stream_timestamp": float(row["stream_timestamp"])
        }

        try:
            es.index(index=INDEX, document=doc)
        except Exception as e:
            print("Elasticsearch error:", e)


# =========================
# 8. Start Streaming
# =========================
query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
