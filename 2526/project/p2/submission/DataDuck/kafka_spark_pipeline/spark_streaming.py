import os
import sys

# Use the same Python interpreter as this script (must have scikit-learn installed).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, StructField, StructType

from pipeline_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    LABEL_ENCODER_PATH,
    MODEL_PATH,
    SPARK_KAFKA_PACKAGE,
    VECTORIZER_PATH,
)

MODEL = None
VECTORIZER = None
LABEL_ENCODER = None


def load_model_artifacts():
    global MODEL, VECTORIZER, LABEL_ENCODER
    if MODEL is not None and VECTORIZER is not None and LABEL_ENCODER is not None:
        return

    import joblib

    MODEL = joblib.load(str(MODEL_PATH))
    VECTORIZER = joblib.load(str(VECTORIZER_PATH))
    LABEL_ENCODER = joblib.load(str(LABEL_ENCODER_PATH))
    print("Loaded Naive Bayes model artifacts")


def predict_review(review_text: str) -> str:
    load_model_artifacts()
    try:
        text = review_text or ""
        features = VECTORIZER.transform([text])
        label_index = int(MODEL.predict(features)[0])
        return LABEL_ENCODER.inverse_transform([label_index])[0]
    except Exception as exc:
        return f"error:{exc}"


@udf(StringType())
def predict_label_udf(review_text: str) -> str:
    return predict_review(review_text)


def run_stream():
    spark = (
        SparkSession.builder.appName("ReviewClassificationStream")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    json_schema = StructType(
        [
            StructField("review_id", StringType(), True),
            StructField("review_text", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("review_date", StringType(), True),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), json_schema).alias("payload"))
        .select(
            col("payload.review_id"),
            col("payload.review_text"),
            col("payload.rating"),
            col("payload.review_date"),
        )
    )

    classified = parsed.withColumn("predicted_label", predict_label_udf(col("review_text")))

    def write_to_elasticsearch(batch_df, batch_id):
        import requests

        rows = batch_df.collect()

        if not rows:
            print(f"Batch {batch_id}: no records")
            return

        print(f"Batch {batch_id}: writing {len(rows)} records to Elasticsearch")

        for row in rows:
            doc = {
                "review_id": row["review_id"],
                "review_text": row["review_text"],
                "rating": row["rating"],
                "review_date": row["review_date"],
                "predicted_label": row["predicted_label"],
            }

            requests.post(
                "http://localhost:9200/app-reviews-sentiment/_doc",
                json=doc,
                timeout=10,
            )

    query = (
        classified.writeStream
        .foreachBatch(write_to_elasticsearch)
        .outputMode("append")
        .start()
    )

    print("Spark streaming job started. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    run_stream()
