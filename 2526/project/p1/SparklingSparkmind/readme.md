# Optimizing High Performance Data Processing for Large Scale Web Crawlers

**Course:** SECP3133-02, High Performance Data Processing
**Project:** Project 1
**Group:** Sparkling Sparkmind
**Prepared for:** Dr. Seah Choon Sen

## Group Members

| Name | Matric No. |
|---|---|
| Safiya Nursyahadah Binti Masnoor | A23CS0176 |
| Ain Nurnabila Binti Mohd Azhar | A23CS0207 |
| Farra Nurzahin Binti Zaharil Anuar | A23CS0079 |
| Dayang Farah Farzana Binti Abang Idham | A23CS0071 |

## Project Presentation Link
[Click here](https://youtu.be/J2cmHaFDi_4?si=uMqFGFMDTVEyOa6k)

## 1. Project Overview

This project builds a complete data pipeline that crawls, cleans, and processes job vacancy listings from **MyFutureJobs** (myfuturejobs.gov.my), a Malaysian government job portal. The goal isn't just to collect a large dataset. It's to show, with real numbers, how different processing strategies hold up once the data gets big.

We collected over 120,000 raw job listings, cleaned them down to **112,499 valid records**, and then ran the same analysis pipeline three different ways: a plain single-threaded Pandas baseline, a multiprocessing version using Joblib, and a Polars-based version using lazy evaluation. Each version was benchmarked on execution time, memory usage, CPU utilization, and throughput so we could compare them fairly.

## 2. Objectives

- Crawl at least 100,000 structured job records from a single Malaysian website, respecting the site's server load.
- Clean and structure the raw data into a consistent format ready for analysis.
- Apply at least two genuinely different HPC optimization techniques (we used two: multiprocessing and Polars) and compare them against a baseline.
- Measure and interpret performance using objective, repeatable metrics, not just a single run.
- Document everything clearly enough that another student could follow the same pipeline end to end.

## 3. Target Website and Data Fields

**Website:** MyFutureJobs (myfuturejobs.gov.my), a government job portal listing vacancies across sectors such as administrative support, technology, manufacturing, healthcare, and education.

**Fields extracted per listing:**

| Field | Description |
|---|---|
| `job_title` | Name/role of the vacancy |
| `occupation_category` | Sector the job belongs to |
| `salary_range` → `salary_min`, `salary_max`, `salary_average` | Salary parsed into numeric values |
| `contract_type` | Permanent, Contract, Part-Time, Internship |
| `working_hours` | e.g. Full-Time, Rotational Shift |
| `education_level` | Minimum qualification required |
| `location` | State/region of the job |
| `vacancy_id` | Unique reference number |
| `job_url` | Link to the original listing |

## 4. Repository Structure

```
p1/sparkling-sparkmind/
│
├── data/
│   ├── raw_data.json     # Raw crawled data (~120,000+ records)
│   ├── cleaned_data.csv            # Final cleaned dataset (112,499 records)
│   ├── cleaned_data.xlsx           # Cleaned dataset, Excel format
│   └── optimize_data.csv           # Supplementary benchmark notes
│
├── p1/
│   ├── main_crawler.ipynb          # Web crawler for MyFutureJobs
│   ├── clean_data.ipynb            # Data cleaning and transformation
│   └── optimize_pipeline.ipynb     # Baseline, Multiprocessing, and Polars benchmarking
│
├── report/
│   ├── Final_Report.pdf
│   └── Presentation_Slides.pptx
│
├── README.md
└── requirements.txt
```

> **Note:** The crawler notebook underwent a few iterations during development (visible as `Copy of main_crawler.ipynb` in earlier commits). The final, working version used for data collection is consolidated into `main_crawler.ipynb`.

## 5. How to Run

This project was developed and run entirely on **Google Colab**, with **Google Drive** used as shared storage between group members.

1. Open each notebook in Google Colab.
2. Mount Google Drive when prompted. All notebooks expect data to live under `/content/drive/MyDrive/HPDP_Project1/data/`.
3. Run the notebooks in order:
   1. `main_crawler.ipynb`: scrapes job listings and saves raw JSON progressively.
   2. `clean_data.ipynb`: loads the raw JSON, cleans it, and exports `cleaned_data.csv`.
   3. `optimize_pipeline.ipynb`: loads the cleaned CSV and runs all three processing approaches, printing and charting the performance comparison.

Install dependencies locally with:

```bash
pip install -r requirements.txt
```

## 6. Data Collection Approach

The crawler used `requests` and `BeautifulSoup` to pull listings across MyFutureJobs' sector categories (administrative support, tech, manufacturing, healthcare, and others), covering job postings across multiple Malaysian states.

To keep the crawl ethical and avoid overloading the server:
- A random delay of **2–4 seconds** was applied between requests.
- A browser-like `User-Agent` header was used on every request.
- Data was saved **progressively** to JSON rather than only at the end, so a crashed run wouldn't mean losing everything.

This resulted in over 120,000 raw records before cleaning.

## 7. Data Cleaning Summary

Cleaning was done in Pandas and included:

- Extracting the job title and vacancy ID from combined text fields.
- Parsing salary ranges (e.g. `"RM 2530 - RM 3946"`) into numeric `salary_min`, `salary_max`, and `salary_average` columns.
- Removing duplicate listings based on `vacancy_id`.
- Standardizing missing values (`"N/A"` → `NaN`) and dropping unused columns.

**Table 1: Record Count Before and After Cleaning**

| Stage | Record Count |
|---|---|
| Raw records collected | 120,000+ |
| Cleaned records (final dataset) | 112,499 |

As shown in Table 1, only a small share of records were dropped, mostly duplicates and listings missing salary data. This is a sign the crawler was extracting fields accurately from the start.

## 8. Optimization Techniques

Three approaches were implemented and benchmarked on the same analysis workflow (filter valid salaries → engineer `salary_gap`/`gap_percentage` → aggregate by location and contract type):

1. **Pandas (Baseline)**: standard single-threaded processing, used as the reference point.
2. **Multiprocessing (Joblib, loky backend)**: splits the dataset into chunks and processes them across multiple CPU cores in parallel.
3. **Polars**: a Rust-based dataframe library using lazy evaluation, where the whole query plan is optimized before running once.

## 9. Performance Evaluation

Each method was run 5 times and averaged, tracking execution time, peak memory, average CPU usage, and throughput.

**Table 2: Average Performance Across 5 Runs**

| Metric | Pandas (Baseline) | Multiprocessing | Polars |
|---|---|---|---|
| Avg Time (s) | 1.327 | 1.697 | 0.326 |
| Avg Memory (MB) | 62.5 | 104.4 | 10.7 |
| Avg CPU (%) | 81.4 | 59.0 | 89.6 |
| Avg Throughput (rows/s) | 87,734 | 66,313 | 345,663 |

As Table 2 shows, **Polars was the clear winner**, roughly 4x faster than the Pandas baseline and 5x faster than multiprocessing, while also using far less memory. Multiprocessing, somewhat surprisingly, ended up *slower* than the plain baseline. This came down to two things: only 2 CPU cores were available in our Colab environment, and the per-row computation (filtering and simple arithmetic) was too lightweight to outweigh the cost of splitting the data across processes and merging it back. Full details and charts are in the Final Report.

## 10. Challenges Faced

- **Multiprocessing + Jupyter on Windows:** worker functions had to be moved into a standalone `.py` file, since Windows can't pickle functions defined directly inside a notebook.
- **Multiprocessing underperforming:** expected it to beat the baseline given the dataset size, but overhead from chunking and inter-process communication outweighed the benefit on a lightweight task with only 2 cores available.
- **Inconsistent CPU readings:** `psutil` measures system-wide CPU, not per-process, and Colab's shared cloud environment caused noticeable variance between runs.

## 11. Possible Improvements

- Re-test multiprocessing on a machine with more CPU cores to see if it becomes worthwhile.
- Try a heavier, more compute-intensive task to give multiprocessing a fairer shot.
- Add multithreading or async requests to the crawling stage itself (currently sequential with delays).
- Measure CPU usage per worker process rather than system-wide, ideally on a dedicated (non-shared) machine.

## 12. Deliverables Checklist

- [x] Final Report (PDF)
- [x] Source Code (crawler, cleaning, optimization notebooks)
- [x] Cleaned Dataset (112,499 records, CSV/XLSX)
- [x] Performance Comparison (tables + charts)
- [x] Presentation Slides

## 13. Academic Integrity Note

All data was collected from publicly available job listings on MyFutureJobs, with crawl delays applied to avoid overloading the server. No login-protected content or personal applicant data was accessed at any point.

