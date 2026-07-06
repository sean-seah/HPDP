# GroupABC_NST - P1

## Description
This folder contains the Part 1 submission for the NST web crawler project. It includes the crawler source code, collected dataset, cleaned dataset, logs, generated charts, and Part 1 report.

## Folder Structure
```text
p1/
â”œâ”€â”€ charts/
â”œâ”€â”€ data/
â”œâ”€â”€ src/
â”œâ”€â”€ Group_ABC_Report_P1.pdf
â””â”€â”€ README.md
```

## Contents
- `data/raw/nst_raw.csv` - Raw NST article dataset.
- `data/processed/nst_cleaned.csv` - Cleaned NST article dataset.
- `data/logs/nst_crawling_log.csv` - Crawler log file.
- `src/` - Python scripts for crawling, cleaning, exporting, counting, benchmarking, Polars processing, and chart generation.
- `charts/` - Performance and processing charts.
- `Group_ABC_Report_P1.pdf` - Part 1 report.

## Setup
Install dependencies from the main project folder:

```bash
pip install -r requirements.txt
```

## How to Run
Run crawler:

```bash
python src/crawl_nst.py
```

Clean data:

```bash
python src/clean_nst.py
```

Generate charts:

```bash
python src/make_nst_charts.py
```
