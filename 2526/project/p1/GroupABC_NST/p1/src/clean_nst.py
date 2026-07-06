from __future__ import annotations

import argparse
import csv
import json
import re
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

import pandas as pd
import psutil


RAW_JSONL = Path("data/raw/nst_articles.jsonl")
PROCESSED_DIR = Path("data/processed")
CLEANED_CSV = PROCESSED_DIR / "nst_cleaned.csv"
PERF_CSV = PROCESSED_DIR / "performance_processing.csv"


def normalize_text(value: object) -> str:
    # Step 1: Standardize text by removing extra spaces, tabs, and newlines.
    return re.sub(r"\s+", " ", str(value or "")).strip()


def clean_record(record: dict) -> dict:
    # Step 2: Clean the main text fields from each raw article record.
    headline = normalize_text(record.get("headline", ""))
    summary = normalize_text(record.get("summary", ""))
    body_preview = normalize_text(record.get("body_preview", ""))
    combined_text = f"{headline} {summary} {body_preview}".strip()

    # Step 3: Keep useful columns and create new analysis features.
    return {
        "record_id": normalize_text(record.get("record_id", "")),
        "url": normalize_text(record.get("url", "")),
        "headline": headline,
        "publication_date": normalize_text(record.get("publication_date", "")),
        "modified_date": normalize_text(record.get("modified_date", "")),
        "section": normalize_text(record.get("section", "")),
        "author": normalize_text(record.get("author", "")),
        "summary": summary,
        "keywords": normalize_text(record.get("keywords", "")),
        "body_preview": body_preview,
        "headline_length": len(headline),
        "summary_length": len(summary),
        "word_count": len(re.findall(r"\w+", combined_text)),
        "collected_at": normalize_text(record.get("collected_at", "")),
    }


def load_records() -> list[dict]:
    # Step 4: Load raw JSONL data collected by the crawler.
    if not RAW_JSONL.exists():
        raise FileNotFoundError(f"Missing {RAW_JSONL}. Run crawl_nst.py first.")
    with RAW_JSONL.open("r", encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def save_cleaned(records: list[dict]) -> int:
    # Step 5: Convert cleaned records into a DataFrame for final filtering.
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records)

    # Step 6: Remove duplicated articles using URL as the unique key.
    frame = frame.drop_duplicates(subset=["url"])

    # Step 7: Remove invalid rows where the headline is empty.
    frame = frame[frame["headline"].str.len() > 0]

    # Step 8: Save the final cleaned dataset as CSV.
    frame.to_csv(CLEANED_CSV, index=False, encoding="utf-8-sig")
    return len(frame)


def append_performance(mode: str, records: int, seconds: float) -> None:
    # Step 9: Record processing performance for the report comparison.
    exists = PERF_CSV.exists()
    process = psutil.Process()
    memory_mb = process.memory_info().rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent(interval=0.2)
    throughput = records / seconds if seconds else 0
    with PERF_CSV.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["dataset", "mode", "records", "seconds", "records_per_second", "cpu_percent", "memory_mb"],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "dataset": "nst",
                "mode": mode,
                "records": records,
                "seconds": round(seconds, 3),
                "records_per_second": round(throughput, 3),
                "cpu_percent": cpu_percent,
                "memory_mb": round(memory_mb, 2),
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean and benchmark NST records")
    parser.add_argument("--mode", choices=["basic", "multiprocessing"], default="basic")
    parser.add_argument("--workers", type=int, default=max(1, cpu_count() - 1))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Step 10: Read all raw records before applying the selected cleaning mode.
    records = load_records()

    started = time.perf_counter()

    # Step 11: Compare basic sequential cleaning with multiprocessing cleaning.
    if args.mode == "basic":
        cleaned = [clean_record(record) for record in records]
    else:
        with Pool(processes=args.workers) as pool:
            cleaned = pool.map(clean_record, records)
    seconds = time.perf_counter() - started

    saved = save_cleaned(cleaned)
    append_performance(args.mode, len(cleaned), seconds)
    print(f"Processed {len(cleaned):,} records in {seconds:.2f}s using {args.mode}.")
    print(f"Saved {saved:,} cleaned records to {CLEANED_CSV}")


if __name__ == "__main__":
    main()
