import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer

from pipeline_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, REVIEW_SOURCE


def build_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: value.encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
    )


def load_reviews(source: Path):
    with source.open("r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield {
                "review_id": row.get("review_id"),
                # Use preprocessed text when available (matches Naive Bayes training).
                "review_text": row.get("clean_review") or row.get("review", ""),
                "rating": row.get("rating"),
                "review_date": row.get("review_date"),
            }


def publish_reviews(producer, topic: str, source: Path, interval: float, max_messages: int = None):
    for count, review in enumerate(load_reviews(source), start=1):
        payload = json.dumps(review)
        producer.send(topic, key=review.get("review_id"), value=payload)
        producer.flush()
        print(f"Published message {count} to topic '{topic}'")
        if max_messages and count >= max_messages:
            break
        time.sleep(interval)


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka producer for app review streaming")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--source", type=Path, default=REVIEW_SOURCE)
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between produced messages")
    parser.add_argument("--max-messages", type=int, default=None, help="Maximum number of messages to send")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = build_producer(args.bootstrap_servers)
    publish_reviews(producer, args.topic, args.source, args.interval, args.max_messages)
    producer.close()


if __name__ == "__main__":
    main()
