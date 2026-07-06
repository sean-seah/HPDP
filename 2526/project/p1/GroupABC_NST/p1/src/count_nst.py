from __future__ import annotations

import json
from pathlib import Path


RAW_JSONL = Path("data/raw/nst_articles.jsonl")
CLEANED_CSV = Path("data/processed/nst_cleaned.csv")
TARGET = 100_000


def count_jsonl() -> tuple[int, int]:
    if not RAW_JSONL.exists():
        return 0, 0
    total = 0
    urls = set()
    with RAW_JSONL.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            total += 1
            urls.add(json.loads(line).get("url", ""))
    return total, len(urls)


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig") as file:
        return max(0, sum(1 for _ in file) - 1)


def main() -> None:
    raw, unique = count_jsonl()
    cleaned = count_csv_rows(CLEANED_CSV)
    print(f"Raw JSONL records: {raw:,}")
    print(f"Unique article URLs: {unique:,}")
    print(f"Cleaned CSV records: {cleaned:,}")
    print(f"Need for 100K target: {max(0, TARGET - raw):,} more raw records")


if __name__ == "__main__":
    main()
