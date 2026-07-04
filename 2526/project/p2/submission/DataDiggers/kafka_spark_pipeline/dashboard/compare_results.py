"""
compare_results.py
--------------------
Task D — Visualization & Performance Engineer

Reads results/batch_results.json and results/streaming_results.json
(produced by performance_test_batch.py and streaming_monitor.py) and
generates:
  - results/comparison_chart.png   (bar charts: time, throughput, accuracy)
  - results/comparison_table.md    (markdown table to paste into the report)

Usage:
    python compare_results.py
"""

import json
from pathlib import Path

import matplotlib.pyplot as plt

RESULTS_DIR = Path("results")


def load(name):
    path = RESULTS_DIR / name
    if not path.exists():
        raise SystemExit(f"Missing {path}. Run the corresponding benchmark script first.")
    with open(path) as f:
        return json.load(f)


def main():
    batch = load("batch_results.json")
    stream = load("streaming_results.json")

    metrics = [
        ("Total Processing Time (s)", "total_processing_time_sec"),
        ("Throughput (records/sec)", "throughput_records_per_sec"),
        ("Accuracy", "accuracy"),
    ]

    fig, axes = plt.subplots(1, len(metrics), figsize=(15, 4))
    for ax, (label, key) in zip(axes, metrics):
        b_val = batch.get(key)
        s_val = stream.get(key)
        vals = [b_val if isinstance(b_val, (int, float)) else 0,
                s_val if isinstance(s_val, (int, float)) else 0]
        bars = ax.bar(["Batch", "Streaming"], vals, color=["#4F46E5", "#DC2626"])
        ax.set_title(label, fontsize=11)
        ax.bar_label(bars, fmt="%.3f")
    plt.tight_layout()
    out_chart = RESULTS_DIR / "comparison_chart.png"
    plt.savefig(out_chart, dpi=150)
    print(f"Saved {out_chart}")

    # ---- Markdown table for the final report ----
    rows = [
        ("Total processing time (s)", batch.get("total_processing_time_sec"), stream.get("total_processing_time_sec")),
        ("Throughput (records/sec)", batch.get("throughput_records_per_sec"), stream.get("throughput_records_per_sec")),
        ("Accuracy", batch.get("accuracy"), stream.get("accuracy")),
        ("Precision (weighted)", batch.get("precision_weighted"), stream.get("precision_weighted")),
        ("Recall (weighted)", batch.get("recall_weighted"), stream.get("recall_weighted")),
        ("F1 (weighted)", batch.get("f1_weighted"), stream.get("f1_weighted")),
        ("CPU usage", batch.get("cpu_percent_process"), stream.get("kafka_cpu_percent_avg")),
        ("Memory usage (MB)", batch.get("memory_mb_process"), stream.get("spark_process_memory_mb_avg")),
        ("Records processed", batch.get("n_records"), stream.get("n_records")),
    ]

    lines = [
        "| Metric | Batch Mode | Streaming Mode |",
        "|---|---|---|",
    ]
    for label, b, s in rows:
        lines.append(f"| {label} | {b} | {s} |")

    out_table = RESULTS_DIR / "comparison_table.md"
    out_table.write_text("\n".join(lines))
    print(f"Saved {out_table}")
    print("\n" + "\n".join(lines))


if __name__ == "__main__":
    main()
