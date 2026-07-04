"""
performance_test_batch.py
--------------------------
Task D — Visualization & Performance Engineer

Measures BATCH-mode performance: takes a fixed volume of reviews and runs
them through the exact same preprocessing + model pipeline C's spark_stream.py
uses (same clean_text function, same TF-IDF vectorizer, same Naive Bayes
model), but as a single one-shot batch instead of through Kafka/Spark.

This gives you an apples-to-apples comparison point against streaming_monitor.py,
which measures the same fixed volume going through the real Kafka -> Spark ->
Elasticsearch pipeline.

Outputs: results/batch_results.json

Usage:
    python performance_test_batch.py
    python performance_test_batch.py --input data/raw_data/labeled_reviews.csv --n 500
"""

import argparse
import json
import re
import time
from pathlib import Path

import joblib
import pandas as pd
import psutil
from sklearn.metrics import accuracy_score, precision_recall_fscore_support

MODELS_DIR = Path("model_training/models")
RESULTS_DIR = Path("results")


# Identical to the clean_text() used in spark_stream.py -- keep these in sync
# so the batch and streaming runs are doing the same amount of work.
def clean_text(text):
    text = str(text).lower()
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw_data/labeled_reviews.csv",
                         help="Fixed volume of text to classify. Must have 'text' "
                              "and 'sentiment' columns (ground truth for accuracy).")
    parser.add_argument("--n", type=int, default=None,
                         help="Optional: cap the number of rows so batch and "
                              "streaming runs use the exact same volume.")
    args = parser.parse_args()

    print("Loading model artifacts...")
    model = joblib.load(MODELS_DIR / "naive_bayes_model.pkl")
    tfidf = joblib.load(MODELS_DIR / "tfidf_vectorizer.pkl")
    label_encoder = joblib.load(MODELS_DIR / "label_encoder.pkl")

    df = pd.read_csv(args.input)
    if args.n:
        df = df.head(args.n)
    n_records = len(df)
    print(f"Loaded {n_records} records from {args.input}")

    proc = psutil.Process()
    cpu_before = proc.cpu_percent(interval=0.5)  # prime the counter
    mem_before_mb = proc.memory_info().rss / (1024 ** 2)

    # =========================================================
    # Timed batch inference (mirrors process_batch() in spark_stream.py)
    # =========================================================
    t0 = time.perf_counter()

    df["processed_text"] = df["text"].apply(clean_text)
    X = tfidf.transform(df["processed_text"])
    preds = model.predict(X)
    df["predicted_sentiment"] = label_encoder.inverse_transform(preds)

    t1 = time.perf_counter()

    cpu_after = proc.cpu_percent(interval=0.5)
    mem_after_mb = proc.memory_info().rss / (1024 ** 2)

    elapsed_sec = t1 - t0
    throughput = n_records / elapsed_sec if elapsed_sec > 0 else float("inf")

    # =========================================================
    # Accuracy against ground truth
    # =========================================================
    y_true = df["sentiment"]
    y_pred = df["predicted_sentiment"]
    acc = accuracy_score(y_true, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="weighted", zero_division=0
    )

    results = {
        "mode": "batch",
        "n_records": n_records,
        "total_processing_time_sec": round(elapsed_sec, 4),
        "throughput_records_per_sec": round(throughput, 2),
        "accuracy": round(acc, 4),
        "precision_weighted": round(precision, 4),
        "recall_weighted": round(recall, 4),
        "f1_weighted": round(f1, 4),
        "cpu_percent_process": cpu_after,
        "memory_mb_process": round(mem_after_mb - mem_before_mb, 2),
        "memory_mb_total": round(mem_after_mb, 2),
    }

    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / "batch_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    print("\n=== BATCH MODE RESULTS ===")
    for k, v in results.items():
        print(f"  {k}: {v}")
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
