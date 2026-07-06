# GroupABC_NST - P2

## Project Overview
This folder contains the Part 2 implementation for the GroupABC_NST project. Part 2 focuses on performance benchmarking, optimized data processing, Polars-based analysis, and chart generation for the NST web crawler dataset.

The final report PDF is stored in the main `GroupABC_NST/` folder, not inside this `p2/` folder.

## Folder Structure
```text
p2/
|
|-- data/
|   |-- performance/
|   |   |-- performance_crawl.csv
|   |   `-- performance_processing.csv
|   |
|   `-- polars_output/
|       |-- category_summary.csv
|       |-- monthly_summary.csv
|       `-- top_authors.csv
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
|   |-- make_nst_charts.py
|   `-- polars_nst.py
|
`-- readme.md
```

## Performance Data
- `data/performance/performance_crawl.csv` stores crawler benchmark results.
- `data/performance/performance_processing.csv` stores data processing benchmark results.
- `data/polars_output/category_summary.csv` stores article counts by category.
- `data/polars_output/monthly_summary.csv` stores monthly article summaries.
- `data/polars_output/top_authors.csv` stores top author summary results.

## Source Code
- `benchmark_crawler.py` compares crawler performance between baseline and optimized approaches.
- `clean_nst.py` supports baseline and optimized data cleaning workflows.
- `polars_nst.py` runs optimized analytical processing using Polars.
- `make_nst_charts.py` generates visual performance comparison charts.

## Charts
The `charts/` folder contains performance visualizations for:
- Crawler execution time
- Crawler throughput
- Crawler CPU usage
- Crawler memory usage
- Processing execution time
- Processing throughput
- Processing CPU usage
- Processing memory usage


