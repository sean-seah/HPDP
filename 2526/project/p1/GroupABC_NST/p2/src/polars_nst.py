from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

import psutil
import polars as pl


PROCESSED_DIR = Path("data/processed")
CLEANED_CSV = PROCESSED_DIR / "nst_cleaned.csv"
PERF_CSV = PROCESSED_DIR / "performance_processing.csv"
POLARS_OUT_DIR = PROCESSED_DIR / "polars_outputs"


def append_performance(mode: str, records: int, seconds: float) -> None:
    """
    Append Polars benchmark result into the same performance CSV used by clean_nst.py.
    """
    exists = PERF_CSV.exists()
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent(interval=0.2)
    throughput = records / seconds if seconds else 0

    PERF_CSV.parent.mkdir(parents=True, exist_ok=True)

    with PERF_CSV.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "dataset",
                "mode",
                "records",
                "seconds",
                "records_per_second",
                "cpu_percent",
                "memory_mb",
            ],
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
    parser = argparse.ArgumentParser(description="Polars lazy processing benchmark for NST cleaned dataset")
    parser.add_argument("--input", default=str(CLEANED_CSV), help="Path to cleaned NST CSV file")
    parser.add_argument("--output-dir", default=str(POLARS_OUT_DIR), help="Directory for Polars output files")
    parser.add_argument("--mode-name", default="polars", help="Mode label saved into performance CSV")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_file = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_file.exists():
        raise FileNotFoundError(
            f"Missing {input_file}. Run cleaning first:\n"
            f"python src\\clean_nst.py --mode basic\n"
            f"or\n"
            f"python src\\clean_nst.py --mode multiprocessing --workers 4"
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()

    # Polars lazy scanning: avoids loading the whole CSV immediately and allows query optimization.
    # Date parsing is intentionally avoided here because the cleaned dataset already contains
    # structured text/date fields and this benchmark focuses on filtering, grouping, and aggregation.
    lf = (
        pl.scan_csv(str(input_file), infer_schema_length=10000)
        .select(
            [
                pl.col("url").cast(pl.Utf8),
                pl.col("headline").cast(pl.Utf8),
                pl.col("section").cast(pl.Utf8),
                pl.col("author").cast(pl.Utf8),
                pl.col("summary").cast(pl.Utf8),
                pl.col("keywords").cast(pl.Utf8),
                pl.col("headline_length").cast(pl.Int64, strict=False),
                pl.col("summary_length").cast(pl.Int64, strict=False),
                pl.col("word_count").cast(pl.Int64, strict=False),
                pl.col("publication_date").cast(pl.Utf8),
            ]
        )
        .with_columns(
            [
                pl.col("headline")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .alias("headline_clean"),

                pl.col("section")
                .str.to_lowercase()
                .str.strip_chars()
                .alias("section_clean"),

                pl.col("author")
                .fill_null("")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .alias("author_clean"),

                pl.col("summary")
                .fill_null("")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .alias("summary_clean"),

                # Extract year and month from ISO datetime string without full date parsing.
                pl.col("publication_date").str.slice(0, 4).alias("year"),
                pl.col("publication_date").str.slice(5, 2).alias("month"),
            ]
        )
        .filter(pl.col("headline_clean").is_not_null())
        .filter(pl.col("headline_clean").str.len_chars() > 0)
        .filter(pl.col("url").is_not_null())
        .unique(subset=["url"])
        .with_columns(
            [
                pl.col("headline_clean").str.len_chars().alias("headline_length_polars"),
                pl.col("summary_clean").str.len_chars().alias("summary_length_polars"),
                pl.col("headline_clean").str.split(" ").list.len().alias("headline_word_count_polars"),
            ]
        )
    )

    # Analytical outputs for report.
    category_summary = (
        lf.group_by("section_clean")
        .agg(
            [
                pl.len().alias("article_count"),
                pl.col("headline_length_polars").mean().alias("avg_headline_length"),
                pl.col("summary_length_polars").mean().alias("avg_summary_length"),
                pl.col("word_count").mean().alias("avg_word_count"),
            ]
        )
        .sort("article_count", descending=True)
        .collect()
    )

    monthly_summary = (
        lf.group_by(["year", "month"])
        .agg(pl.len().alias("article_count"))
        .sort(["year", "month"])
        .collect()
    )

    author_summary = (
        lf.filter(pl.col("author_clean").str.len_chars() > 0)
        .group_by("author_clean")
        .agg(pl.len().alias("article_count"))
        .sort("article_count", descending=True)
        .head(20)
        .collect()
    )

    records = lf.select(pl.len()).collect().item()

    category_summary.write_csv(output_dir / "category_summary.csv")
    monthly_summary.write_csv(output_dir / "monthly_summary.csv")
    author_summary.write_csv(output_dir / "top_authors.csv")

    seconds = time.perf_counter() - started

    append_performance(args.mode_name, records, seconds)

    print(f"Polars processed {records:,} records in {seconds:.3f}s.")
    print(f"Category summary saved to: {output_dir / 'category_summary.csv'}")
    print(f"Monthly summary saved to: {output_dir / 'monthly_summary.csv'}")
    print(f"Top authors saved to: {output_dir / 'top_authors.csv'}")
    print(f"Performance appended to: {PERF_CSV}")


if __name__ == "__main__":
    main()
