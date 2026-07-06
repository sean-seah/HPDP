# GroupABC_NST - P1

## Project Overview
This folder contains the Part 1 implementation for the GroupABC_NST project. The project focuses on crawling article metadata from the New Straits Times website, cleaning the collected records, and preparing the dataset for performance evaluation.

The final report PDF is stored in the main `GroupABC_NST/` folder, not inside this `p1/` folder.

## Folder Structure
```text
p1/
|
|-- data/
|   |-- raw/
|   |   `-- nst_raw.csv
|   |
|   |-- processed/
|   |   `-- nst_cleaned.csv
|   |
|   `-- logs/
|       `-- nst_crawling_log.csv
|
|-- charts/
|   |-- crawler_cpu_usage.png
|   |-- crawler_execution_time.png
|   |-- crawler_memory_usage.png
|   |-- crawler_throughput.png
|   |-- processing_cpu_usage.png
|   |-- processing_execution_time.png
|   |-- processing_memory_usage.png
|   `-- processing_throughput.png
|
|-- src/
|   |-- benchmark_crawler.py
|   |-- clean_nst.py
|   |-- count_nst.py
|   |-- crawl_nst.py
|   |-- export_nst_csv.py
|   |-- make_nst_charts.py
|   `-- polars_nst.py
|
`-- README.md
```

## Dataset
- `data/raw/nst_raw.csv` contains the raw crawled NST article records.
- `data/processed/nst_cleaned.csv` contains the cleaned dataset used for analysis.
- `data/logs/nst_crawling_log.csv` contains crawler execution logs.

## Source Code
- `crawl_nst.py` crawls article metadata from NST.
- `export_nst_csv.py` exports collected crawler output into CSV format.
- `clean_nst.py` cleans and preprocesses the raw dataset.
- `count_nst.py` checks the number of records in the dataset.
- `benchmark_crawler.py` benchmarks crawler performance.
- `polars_nst.py` performs optimized processing using Polars.
- `make_nst_charts.py` generates the performance charts.

## Charts
The `charts/` folder contains visual comparisons for crawler and processing performance, including execution time, throughput, CPU usage, and memory usage.

## How to Run
Install the required Python packages from the main project folder:

```bash
pip install -r requirements.txt
```

Run the crawler:

```bash
python src/crawl_nst.py
```

Clean the dataset:

```bash
python src/clean_nst.py
```

Generate charts:

```bash
python src/make_nst_charts.py
```

## Notes
The report file `Group_ABC_Report_P1.pdf` is placed one level above this folder, at:

```text
GroupABC_NST/Group_ABC_Report_P1.pdf
```
