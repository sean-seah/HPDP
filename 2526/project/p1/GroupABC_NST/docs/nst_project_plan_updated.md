# NST News Crawler Project Plan (Updated)

## Project Summary

This project develops a high-performance web crawler to collect over
**100,000** public news article metadata records from the **New Straits
Times (NST)** website. The system compares baseline and optimized
approaches for both web crawling and data processing, followed by
performance benchmarking.

------------------------------------------------------------------------

# Week 1 -- Planning and Setup

## Target Website

-   Website: https://www.nst.com.my/
-   Domain: Malaysian News
-   Data Source: Public news article pages discovered through NST
    sitemaps.

## Dataset Fields

The crawler collects the following structured metadata:

-   Record ID
-   URL
-   Headline
-   Publication Date
-   Modified Date
-   Section
-   Author
-   Summary
-   Keywords
-   Body Preview
-   Word Count
-   Collection Timestamp

## Project Architecture

NST Website

↓

Sitemap Discovery

↓

Article URL Collection

↓

Baseline / Threaded Crawler

↓

Raw JSONL Storage

↓

Data Cleaning

↓

Clean CSV

↓

Baseline Pandas

↓

Multiprocessing

↓

Polars Lazy Execution

↓

Performance Benchmark

↓

Charts & Evaluation

## Libraries

-   requests
-   BeautifulSoup4
-   pandas
-   polars
-   multiprocessing
-   concurrent.futures (ThreadPoolExecutor)
-   psutil
-   matplotlib

------------------------------------------------------------------------

# Week 2 -- Crawler Development

## File

`src/crawl_nst.py`

### Baseline Crawler

-   Sequential article crawling
-   Progressive JSONL writing
-   Request delay
-   Duplicate prevention using visited URLs

### Optimized Crawler

Uses:

-   ThreadPoolExecutor
-   Parallel page downloading
-   Faster I/O processing

Collected dataset:

-   102,265 NST article records

------------------------------------------------------------------------

# Week 3 -- Data Processing and Optimization

## Data Cleaning

File:

`src/clean_nst.py`

Processing steps:

-   Remove duplicate URLs
-   Remove empty headlines
-   Normalize whitespace
-   Generate:
    -   headline_length
    -   summary_length
    -   word_count

Output:

`data/processed/nst_cleaned.csv`

------------------------------------------------------------------------

# Optimization Techniques

## Optimization 1 --- Threaded Crawler

File:

`src/crawl_nst.py`

Technique:

-   ThreadPoolExecutor

Purpose:

-   Improve I/O-bound crawling performance through concurrent downloads.

------------------------------------------------------------------------

## Optimization 2 --- Multiprocessing

File:

`src/clean_nst.py`

Technique:

-   multiprocessing.Pool

Purpose:

-   Parallel data cleaning and transformation.

------------------------------------------------------------------------

## Optimization 3 --- Polars Lazy Execution

File:

`src/polars_nst.py`

Technique:

-   Polars LazyFrame

Purpose:

-   Faster analytical processing using lazy query optimization and
    parallel execution.

------------------------------------------------------------------------

# Performance Benchmark

## Crawler Benchmark

File:

`src/benchmark_crawler.py`

Purpose:

Compare:

-   Baseline Crawler
-   Threaded Crawler

Metrics:

-   Execution Time
-   Throughput
-   CPU Usage
-   Memory Usage

Results stored in:

`data/raw/performance_crawl.csv`

------------------------------------------------------------------------

## Processing Benchmark

Compare:

-   Baseline Pandas
-   Multiprocessing
-   Polars Lazy Execution

Dataset:

102,265 cleaned records

Metrics:

-   Execution Time
-   Throughput
-   CPU Usage
-   Memory Usage

Results stored in:

`data/processed/performance_processing.csv`

------------------------------------------------------------------------

# Visualization

File:

`src/make_nst_charts.py`

Generated Charts

## Crawler

-   Execution Time
-   Throughput
-   CPU Usage
-   Memory Usage

## Processing

-   Execution Time
-   Throughput
-   CPU Usage
-   Memory Usage

------------------------------------------------------------------------

# Final Deliverables

-   100,000+ raw NST article metadata records
-   Cleaned CSV dataset
-   Baseline crawler
-   Threaded crawler
-   Baseline Pandas processing
-   Multiprocessing processing
-   Polars Lazy Execution processing
-   Benchmark results
-   Performance comparison charts
-   Final technical report
