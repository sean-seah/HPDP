import json
import time
import pandas as pd
from kafka import KafkaProducer

DATA_PATH = "data/cleaned_data.csv"
KAFKA_TOPIC = "tng_reviews"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

df = pd.read_csv(DATA_PATH)

print(f"Loaded {len(df)} reviews")
print("Sending reviews to Kafka...")

for index, row in df.iterrows():
    message = {
        "review_id": row.get("review_id", ""),
        "review_text": row.get("review_text", ""),
        "cleaned_review": row.get("cleaned_review", ""),
        "rating": int(row.get("rating", 0)),
        "actual_sentiment": row.get("sentiment_label", ""),
        "review_datetime": row.get("review_datetime", "")
    }

    producer.send(KAFKA_TOPIC, value=message)

    if (index + 1) % 200 == 0:
        print(f"Sent {index + 1} reviews...")

    time.sleep(0.05)

producer.flush()
producer.close()

print("All reviews sent successfully.")