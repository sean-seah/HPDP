from __future__ import annotations

import csv
import json
from pathlib import Path


RAW_JSONL = Path("data/raw/nst_articles.jsonl")
RAW_CSV = Path("data/raw/nst_articles.csv")


def main() -> None:
    if not RAW_JSONL.exists():
        raise FileNotFoundError(f"Missing {RAW_JSONL}. Run crawl_nst.py first.")

    rows = []
    with RAW_JSONL.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                rows.append(json.loads(line))

    if not rows:
        print("No records found.")
        return

    fieldnames = sorted({field for row in rows for field in row.keys()})
    RAW_CSV.parent.mkdir(parents=True, exist_ok=True)
    with RAW_CSV.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows):,} records to {RAW_CSV}")


if __name__ == "__main__":
    main()
