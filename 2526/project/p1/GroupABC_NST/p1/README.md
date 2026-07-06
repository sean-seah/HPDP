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
|-- src/
|   |-- clean_nst.py
|   |-- count_nst.py
|   |-- crawl_nst.py
|   `-- export_nst_csv.py
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

