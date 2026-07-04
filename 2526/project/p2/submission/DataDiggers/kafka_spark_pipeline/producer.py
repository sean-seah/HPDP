import json
import time
import pandas as pd
from kafka import KafkaProducer
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "raw_data" / "raw_reviews.csv"

# ==========================
# Kafka Configuration
# ==========================
TOPIC = "google-play-reviews"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==========================
# Load Raw Dataset (A's output)
# ==========================
df = pd.read_csv(DATA_PATH)

print(f"Loaded {len(df)} reviews.")
print("Starting live stream...\n")

# ==========================
# Stream Reviews One-by-One
# ==========================
for index, row in df.iterrows():

    message = {
        "app": row["app"],
        "review": str(row["text"]),
        "rating": int(row["rating"]),
        "review_date": str(row["date"]),
        "stream_timestamp": time.time()
    }

    producer.send(TOPIC, value=message)
    producer.flush()

    print(f"[{index + 1}/{len(df)}] Sent:", message)

    # Simulate incoming reviews
    time.sleep(1.5)

print("\nStreaming completed.")
