from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

MODEL_PATH = PROJECT_ROOT / "models" / "naive_bayes_model.pkl"
VECTORIZER_PATH = PROJECT_ROOT / "models" / "tfidf_vectorizer.pkl"
LABEL_ENCODER_PATH = PROJECT_ROOT / "models" / "label_encoder.pkl"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "app-reviews"
KAFKA_OUTPUT_TOPIC = "app-reviews-classified"

# Use cleaned_data.csv so the producer can send preprocessed review text (matches training).
REVIEW_SOURCE = PROJECT_ROOT / "data" / "cleaned_data.csv"

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
