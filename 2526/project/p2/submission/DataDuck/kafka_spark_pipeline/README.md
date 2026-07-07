# Kafka + Spark Streaming Pipeline

This folder contains the real-time ingestion and classification pipeline for app reviews.

## Components

- `kafka_producer.py`: publishes review messages from `data/cleaned_data.csv` into Kafka.
- `spark_streaming.py`: consumes Kafka review events and applies the trained Naive Bayes model in Spark Structured Streaming.
- `pipeline_config.py`: centralizes Kafka, Spark, and model artifact paths.
- `setup_kafka.ps1`: starts Kafka via Docker and creates the required topic (Windows).

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

Key packages for the streaming pipeline: `pyspark`, `kafka-python`, `scikit-learn`, `joblib`.

### 2. Start Kafka and create the topic

**Option A — run the setup script (Windows PowerShell):**

```powershell
.\kafka_spark_pipeline\setup_kafka.ps1
```

**Option B — manual Docker commands:**

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic app-reviews \
  --partitions 1 --replication-factor 1
```

If the container already exists, skip `docker run` and only create the topic.

## Run the pipeline

**Order matters:** Kafka running → topic created → Spark streaming → producer.

1. Start Spark streaming (leave this running):

   ```bash
   python kafka_spark_pipeline/spark_streaming.py
   ```

2. In a second terminal, start the Kafka producer:

   ```bash
   python kafka_spark_pipeline/kafka_producer.py --interval 0.5
   ```

   To send only a test batch of 10 messages:

   ```bash
   python kafka_spark_pipeline/kafka_producer.py --interval 0.5 --max-messages 10
   ```

## Notes

- The Spark job uses a UDF to load `models/naive_bayes_model.pkl` and `models/tfidf_vectorizer.pkl`. Each worker loads artifacts once per batch.
- The producer sends `clean_review` from `cleaned_data.csv` so inference matches notebook training.
- Spark console output shows at most ~20 rows per batch; all messages are still processed.
- `PYSPARK_PYTHON` is set to `sys.executable` so Spark workers use the same Python environment.
- DeprecationWarnings from `kafka-python` are harmless.
- The model rarely predicts "neutral" — this is a training limitation, not a pipeline bug.
