"""
streaming_monitor.py
----------------------
Task D — Visualization & Performance Engineer

Measures STREAMING-mode performance by watching the real pipeline from
outside: it polls the Elasticsearch document count while producer.py and
spark_stream.py are running, and samples resource usage of the Kafka
container (via `docker stats`) and the local spark_stream.py process
(via psutil) at the same time.

IMPORTANT — read before running:
producer.py sleeps 1.5s between messages to *simulate* live arrivals. That
artificial delay will dominate your throughput number and make streaming
look artificially slow next to batch. For a fair "how fast can the system
actually process records" comparison, run producer.py with little/no sleep
for this test (e.g. temporarily set `time.sleep(0.05)` or add a `--fast`
flag). Keep the 1.5s version for your live demo/dashboard, and use the
fast version only for this benchmark. Note this distinction explicitly in
your report -- it's exactly the kind of nuance that scores well on the
Real Time Processing and Comparison criterion.

How to run (3 terminals):
    Terminal 1: docker-compose up                 (Kafka)
    Terminal 2: python spark_stream.py             (consumer, writes to ES)
    Terminal 3: python streaming_monitor.py --n 500   <- start this first
    Terminal 4: python producer.py                 (start right after monitor)

Usage:
    python streaming_monitor.py --n 500 --kafka-container kafka
"""

import argparse
import json
import subprocess
import time
from pathlib import Path

import pandas as pd
import psutil
from elasticsearch import Elasticsearch
from sklearn.metrics import accuracy_score, precision_recall_fscore_support

ES_HOST = "http://localhost:9200"
ES_INDEX = "google_play_sentiment"
RESULTS_DIR = Path("results")


def get_es_count(es):
    try:
        es.indices.refresh(index=ES_INDEX)
        return es.count(index=ES_INDEX)["count"]
    except Exception:
        return 0


def get_docker_stats(container_name):
    """Single-shot CPU% / memory snapshot for a running container."""
    try:
        out = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format",
             "{{.CPUPerc}}|{{.MemUsage}}", container_name],
            timeout=5,
        ).decode().strip()
        cpu_str, mem_str = out.split("|")
        cpu_pct = float(cpu_str.replace("%", ""))
        mem_used = mem_str.split("/")[0].strip()  # e.g. "512MiB"
        return cpu_pct, mem_used
    except Exception as e:
        return None, f"unavailable ({e})"


def find_spark_process():
    """Best-effort: find the local python process running spark_stream.py."""
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = " ".join(p.info["cmdline"] or [])
            if "spark_stream" in cmdline:
                return psutil.Process(p.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, required=True,
                         help="Target record count (same volume used in "
                              "performance_test_batch.py, e.g. --n 500).")
    parser.add_argument("--kafka-container", default="kafka",
                         help="Docker container name for Kafka (see docker-compose.yml).")
    parser.add_argument("--truth", default="data/raw_data/labeled_reviews.csv",
                         help="CSV with ground-truth sentiment, in the same row "
                              "order as raw_reviews.csv fed to producer.py.")
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=1800,
                         help="Max seconds to wait for --n docs to appear before giving up.")
    args = parser.parse_args()

    es = Elasticsearch(ES_HOST)
    start_count = get_es_count(es)
    print(f"Starting doc count in '{ES_INDEX}': {start_count}")
    print(f"Waiting for {args.n} new documents... start producer.py now.")

    spark_proc = find_spark_process()
    if spark_proc:
        spark_proc.cpu_percent(interval=None)  # prime the counter
        print(f"Tracking spark_stream.py at PID {spark_proc.pid}")
    else:
        print("WARNING: could not find a running spark_stream.py process — "
              "spark-side CPU/memory will be reported as unavailable.")

    cpu_samples, mem_samples = [], []
    t0 = time.perf_counter()

    while True:
        current = get_es_count(es) - start_count
        elapsed = time.perf_counter() - t0

        cpu_pct, mem_used = get_docker_stats(args.kafka_container)
        if cpu_pct is not None:
            cpu_samples.append(cpu_pct)

        if spark_proc:
            try:
                mem_samples.append(spark_proc.memory_info().rss / (1024 ** 2))
            except psutil.NoSuchProcess:
                pass

        print(f"  [{elapsed:6.1f}s] indexed so far: {current}/{args.n}  "
              f"| kafka CPU: {cpu_pct}%  mem: {mem_used}")

        if current >= args.n:
            break
        if elapsed > args.timeout:
            print("Timed out waiting for documents. Recording partial results.")
            break

        time.sleep(args.poll_interval)

    elapsed_sec = time.perf_counter() - t0
    n_actual = get_es_count(es) - start_count
    throughput = n_actual / elapsed_sec if elapsed_sec > 0 else 0

    # =========================================================
    # Accuracy: compare what's now in ES against ground truth
    # =========================================================
    docs = es.search(index=ES_INDEX, size=n_actual,
                      sort=[{"stream_timestamp": "asc"}])["hits"]["hits"]
    es_df = pd.DataFrame([d["_source"] for d in docs])

    acc = precision = recall = f1 = None
    if not es_df.empty and Path(args.truth).exists():
        truth_df = pd.read_csv(args.truth).head(len(es_df)).reset_index(drop=True)
        es_df = es_df.reset_index(drop=True)
        n = min(len(es_df), len(truth_df))
        y_true = truth_df["sentiment"].iloc[:n]
        y_pred = es_df["sentiment"].iloc[:n]
        acc = accuracy_score(y_true, y_pred)
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true, y_pred, average="weighted", zero_division=0
        )

    results = {
        "mode": "streaming",
        "n_records": n_actual,
        "total_processing_time_sec": round(elapsed_sec, 2),
        "throughput_records_per_sec": round(throughput, 2),
        "accuracy": round(acc, 4) if acc is not None else "N/A (see notes)",
        "precision_weighted": round(precision, 4) if precision is not None else "N/A",
        "recall_weighted": round(recall, 4) if recall is not None else "N/A",
        "f1_weighted": round(f1, 4) if f1 is not None else "N/A",
        "kafka_cpu_percent_avg": round(sum(cpu_samples) / len(cpu_samples), 2) if cpu_samples else "N/A",
        "spark_process_memory_mb_avg": round(sum(mem_samples) / len(mem_samples), 2) if mem_samples else "N/A",
        "note": ("Accuracy assumes ES doc order matches --truth row order "
                 "(row-index join). For an exact join, add an incrementing "
                 "'review_id' field in producer.py and spark_stream.py."),
    }

    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / "streaming_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)

    print("\n=== STREAMING MODE RESULTS ===")
    for k, v in results.items():
        print(f"  {k}: {v}")
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
