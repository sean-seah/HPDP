import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer


def safe_value(value):
    """Convert pandas/numpy values into JSON-safe Python values."""
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def main():
    parser = argparse.ArgumentParser(description="Send cleaned review data to Kafka.")
    parser.add_argument("--csv", default="../data/cleaned_data.csv")
    parser.add_argument("--topic", default="sentiment-topic")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to send. Use 0 for all rows.")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between messages in seconds.")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    if args.limit > 0:
        df = df.head(args.limit)

    print(f"Sending {len(df)} records to Kafka topic: {args.topic}")

    sent_count = 0

    for _, row in df.iterrows():
        cleaned_text = safe_value(row.get("cleaned_text"))

        if not cleaned_text:
            continue

        message = {
            "review_id": safe_value(row.get("review_id")),
            "rating": safe_value(row.get("rating")),
            "review_date": safe_value(row.get("review_date")),
            "app_version": safe_value(row.get("app_version")),
            "thumbs_up_count": safe_value(row.get("thumbs_up_count")),
            "original_text": safe_value(row.get("original_text")),
            "cleaned_text": cleaned_text,
            "produced_timestamp": datetime.now(timezone.utc).isoformat()
        }

        producer.send(args.topic, value=message)
        sent_count += 1

        print(f"Sent {sent_count}: {message['cleaned_text'][:80]}...")
        time.sleep(args.delay)

    producer.flush()
    producer.close()

    print(f"\nDone. Total messages sent: {sent_count}")


if __name__ == "__main__":
    main()