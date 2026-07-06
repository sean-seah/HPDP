from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(".matplotlib-cache").resolve()))

import matplotlib.pyplot as plt
import pandas as pd


CHART_DIR = Path("charts")
CRAWL_PERF = Path("data/raw/performance_crawl.csv")
PROCESS_PERF = Path("data/processed/performance_processing.csv")


CRAWLER_LABELS = {
    "basic": "Baseline Crawler",
    "sequential": "Baseline Crawler",
    "threaded": "Threaded Crawler",
}

PROCESSING_LABELS = {
    "basic": "Baseline Pandas",
    "multiprocessing": "Multiprocessing",
    "polars": "Polars Lazy Execution",
}

CRAWLER_ORDER = {
    "basic": 1,
    "sequential": 1,
    "threaded": 2,
}

PROCESSING_ORDER = {
    "basic": 1,
    "multiprocessing": 2,
    "polars": 3,
}

CRAWLER_COLORS = [
    "#8EC9E6",  # soft blue
    "#8DD7C0",  # soft teal
]

PROCESSING_COLORS = [
    "#F6C77A",  # soft orange
    "#9AD9A3",  # soft green
    "#C5A3E0",  # soft purple
]


def load_stage_performance(path: Path, stage: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    frame = pd.read_csv(path)

    if "dataset" in frame.columns:
        frame = frame[frame["dataset"].fillna("") == "nst"].copy()

    frame["stage"] = stage
    return frame


def get_latest_rows(frame: pd.DataFrame, order_map: dict[str, int]) -> pd.DataFrame:
    if frame.empty:
        return frame

    latest = frame.groupby("mode", as_index=False).tail(1).copy()
    latest["sort_order"] = latest["mode"].map(order_map).fillna(99)
    latest = latest.sort_values("sort_order")
    return latest


def save_bar(
    frame: pd.DataFrame,
    metric: str,
    title: str,
    ylabel: str,
    filename: str,
    colors: list[str],
) -> None:
    if frame.empty:
        print(f"Skipped {filename}: no data.")
        return

    if metric not in frame.columns:
        print(f"Skipped {filename}: missing column {metric}.")
        return

    CHART_DIR.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 5))
    plt.bar(frame["display_mode"], frame[metric], color=colors[: len(frame)], edgecolor="#555555", linewidth=0.5)

    plt.title(title, fontsize=14, fontweight="bold")
    plt.ylabel(ylabel)
    plt.xlabel("Method")
    plt.xticks(rotation=20, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.35)
    plt.tight_layout()
    plt.savefig(CHART_DIR / filename, dpi=180)
    plt.close()


def prepare_crawler_frame() -> pd.DataFrame:
    frame = load_stage_performance(CRAWL_PERF, "crawler")

    if frame.empty:
        return frame

    frame["display_mode"] = frame["mode"].map(CRAWLER_LABELS).fillna(frame["mode"].astype(str).str.title())
    frame = get_latest_rows(frame, CRAWLER_ORDER)

    return frame


def prepare_processing_frame() -> pd.DataFrame:
    frame = load_stage_performance(PROCESS_PERF, "processing")

    if frame.empty:
        return frame

    frame["display_mode"] = frame["mode"].map(PROCESSING_LABELS).fillna(frame["mode"].astype(str).str.title())
    frame = get_latest_rows(frame, PROCESSING_ORDER)

    return frame


def main() -> None:
    crawler_frame = prepare_crawler_frame()
    processing_frame = prepare_processing_frame()

    save_bar(
        crawler_frame,
        "seconds",
        "NST Crawler Execution Time Comparison",
        "Execution Time (Seconds)",
        "crawler_execution_time.png",
        CRAWLER_COLORS,
    )

    save_bar(
        crawler_frame,
        "records_per_second",
        "NST Crawler Throughput Comparison",
        "Records Processed per Second",
        "crawler_throughput.png",
        CRAWLER_COLORS,
    )

    save_bar(
        crawler_frame,
        "cpu_percent",
        "NST Crawler CPU Usage Comparison",
        "CPU Usage (%)",
        "crawler_cpu_usage.png",
        CRAWLER_COLORS,
    )

    save_bar(
        crawler_frame,
        "memory_mb",
        "NST Crawler Memory Usage Comparison",
        "Memory Usage (MB)",
        "crawler_memory_usage.png",
        CRAWLER_COLORS,
    )

    save_bar(
        processing_frame,
        "seconds",
        "NST Processing Execution Time Comparison",
        "Execution Time (Seconds)",
        "processing_execution_time.png",
        PROCESSING_COLORS,
    )

    save_bar(
        processing_frame,
        "records_per_second",
        "NST Processing Throughput Comparison",
        "Records Processed per Second",
        "processing_throughput.png",
        PROCESSING_COLORS,
    )

    save_bar(
        processing_frame,
        "cpu_percent",
        "NST Processing CPU Usage Comparison",
        "CPU Usage (%)",
        "processing_cpu_usage.png",
        PROCESSING_COLORS,
    )

    save_bar(
        processing_frame,
        "memory_mb",
        "NST Processing Memory Usage Comparison",
        "Memory Usage (MB)",
        "processing_memory_usage.png",
        PROCESSING_COLORS,
    )

    print(f"Saved charts to {CHART_DIR.resolve()}")


if __name__ == "__main__":
    main()
