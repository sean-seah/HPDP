# 📰 DataDiggers | High Performance News Crawling & Data Processing Pipeline

This repository contains the implementation of **Project 1** for **SECP3133 High Performance Data Processing**.

Our project focuses on developing a large-scale web crawling and data processing pipeline using news articles collected from the **BERNAMA English News Portal**. The project demonstrates how high-performance computing techniques can improve the efficiency of large-scale data collection and preprocessing while maintaining responsible web scraping practices.

---

## 📌 Project Title

**Optimizing High-Performance Data Processing for Large-Scale Web Crawlers**

---

## 👥 Team Members

| Name | Matric No |
|------|-----------|
| Nurul Ika Syafiny Binti Azhar | A23CS0164 |
| Lubna Al Haani Binti Radzuan | A23CS0107 |
| Nur Firzana Binti Badrus Hisham | A23CS0156 |
| Nuraisyah Binti Mohd Zikri | A23CS0160 |

---

## 📝 Project Summary

This project aims to develop a high-performance web crawling pipeline capable of collecting over **100,000** news articles from the BERNAMA English News Portal.

The complete pipeline consists of:

1. Crawling news articles from BERNAMA
2. Progressively storing raw data in CSV format
3. Cleaning and validating the collected dataset
4. Generating additional analytical features
5. Optimizing the cleaning pipeline using multiprocessing
6. Comparing the baseline and optimized implementations through performance benchmarking

---

## 🌐 Target Website

**Website:** BERNAMA English News Portal

**URL:** https://www.bernama.com/en/

**Data Type:** Public news articles

The crawler extracts structured information from archived BERNAMA news articles using sequential article identifiers.

---

## 📂 Data Collected

The dataset contains the following attributes:

| Attribute | Description |
|-----------|-------------|
| `headline` | News article title |
| `publication_date` | Date and time the article was published |
| `section` | News category |
| `article_summary` | First meaningful paragraph of the article |
| `url` | Unique article URL |

Additional engineered features include:

- `pub_date_only`
- `pub_year`
- `pub_month`
- `headline_word_count`

---

## 📊 Dataset Overview

| Description | Value |
|------------|------:|
| Raw records collected | 101,212 |
| Final cleaned records | 101,212 |
| Raw data format | CSV |
| Cleaned data format | CSV |
| Target website | BERNAMA English |

---

## 🔄 Data Processing Workflow

```text
BERNAMA News Portal
          │
          ▼
 URL Generation
          │
          ▼
Concurrent Web Crawling
(ThreadPoolExecutor)
          │
          ▼
 Progressive CSV Storage
          │
          ▼
 Data Cleaning & Validation
          │
          ▼
 Feature Engineering
          │
          ▼
 Multiprocessing Optimization
          │
          ▼
 Performance Evaluation
```

---

## 🧹 Data Cleaning Steps

The collected dataset undergoes several preprocessing steps before optimization:

- Remove duplicate records based on URL
- Strip unnecessary whitespace
- Standardize publication date format
- Validate article URLs
- Filter incomplete article summaries
- Standardize news section names
- Generate additional analytical features

---

## 🛠️ Tools & Frameworks

| Tool / Framework | Usage |
|------------------|------|
| Python | Main programming language |
| Requests | HTTP requests |
| BeautifulSoup | HTML parsing |
| ThreadPoolExecutor | Concurrent web crawling |
| Requests Session & Retry | Connection reuse and retry mechanism |
| Pandas | Data cleaning and preprocessing |
| Multiprocessing | Parallel data processing |
| psutil | CPU and memory profiling |
| Matplotlib | Performance visualization |
| Google Colab | Development environment |
| Google Drive | Storage Management |

---

## ⚡ Optimization Techniques

Two high-performance computing techniques were implemented throughout the project.

### 🌐 Concurrent Web Crawling

The web crawler uses **ThreadPoolExecutor** to download multiple webpages simultaneously, significantly reducing crawling time compared to sequential requests.

Main techniques:

- Multi-threaded HTTP requests
- Retry mechanism for failed requests
- Connection reuse using Requests Session
- Progressive checkpoint storage
- Duplicate URL filtering

---

### 🚀 Multiprocessing Data Cleaning

The data cleaning pipeline was optimized using Python's **multiprocessing** module.

Main techniques:

- Global duplicate removal before partitioning
- Dataset partitioning into multiple chunks
- Parallel execution across CPU cores
- Merge cleaned chunks into a final dataset
- Verification against baseline output

---

## 📈 Performance Evaluation

The baseline and optimized pipelines were evaluated using:

- Execution Time
- Throughput
- CPU Utilization
- Memory Usage

### Benchmark Results

| Metric | Baseline | Optimized |
|-------|---------:|----------:|
| Records Processed | 101,212 | 101,212 |
| Execution Time | 1.40 s | 1.46 s |
| Throughput | 72,060 records/s | 69,439 records/s |
| CPU Cores Used | 1 | 2 |
| Memory Usage | -29.1 MB | 96.35 MB |

---

## 🔍 Key Findings

The project demonstrates several important observations:

- ThreadPoolExecutor significantly improved web crawling efficiency through concurrent requests.
- The data cleaning pipeline successfully processed more than **101,000** records while preserving data quality.
- Multiprocessing produced identical results to the baseline implementation, ensuring correctness after optimization.
- For this workload, Pandas' highly optimized vectorized operations were already very efficient, meaning multiprocessing introduced additional overhead instead of reducing execution time.
- The project highlights that high-performance optimization should always be evaluated against workload characteristics rather than assumed to provide automatic performance improvements.

---

## 📁 Repository Structure

```text
DataDiggers/
│
├── data/
│   ├── cleaned_data.rar
│   ├── raw_data.zip
│   └── raw_data_preview.json
│
├── p1/
│   ├── clean_data.ipynb
│   ├── main_crawler.ipynb
│   └── optimize_pipeline.ipynb
│
├── p2/
│   ├── evaluation_charts.ipynb
│   ├── performance_after.csv
│   └── performance_before.csv
│
├── report/
│   ├── DataDiggers_Project1Report_HPDP.pdf
│   └── DataDiggers_HPDP_Project 1_Presentation Slides.pdf
│
├── README.md
└── requirements.txt
```

---

## 👩‍💻 Team Responsibilities

| Member | Main Responsibility |
|---------|---------------------|
| **Nurul Ika Syafiny Binti Azhar** | Developed the baseline web crawler and implemented the large-scale news collection pipeline |
| **Lubna Al Haani Binti Radzuan** | Optimized the web crawler using concurrent processing and improved crawling efficiency |
| **Nur Firzana Binti Badrus Hisham** | Developed the data cleaning and preprocessing pipeline |
| **Nuraisyah Binti Mohd Zikri** | Implemented multiprocessing optimization, conducted performance evaluation, and generated benchmarking results |

---

## 🎓 Course Information

**Course:** SECP3133 High Performance Data Processing (Section 2)

**Lecturer:** Dr. Seah Choon Sen

**Faculty:** Faculty of Computing, Universiti Teknologi Malaysia

**Semester:** Semester 2, 2025/2026

---

# 📅 Project Timeline

The project was completed over four weeks following the milestones outlined in the course requirements.

| Week | Milestone | Responsible Member(s) | Status |
|------|-----------|-----------------------|:------:|
| **Week 1** | Form project team | All Members | ✅ |
| **Week 1** | Select BERNAMA as target website and obtain approval | All Members | ✅ |
| **Week 1** | Identify data fields and project requirements | All Members | ✅ |
| **Week 1** | Design system architecture and workflow | Nurul Ika & Lubna Al Haani | ✅ |
| **Week 2** | Develop baseline web crawler | Nurul Ika | ✅ |
| **Week 2** | Optimize crawler using ThreadPoolExecutor and retry mechanism | Lubna Al Haani | ✅ |
| **Week 2** | Collect over 100,000 news articles with progressive storage | Nurul Ika & Lubna Al Haani | ✅ |
| **Week 3** | Develop data cleaning and preprocessing pipeline | Nur Firzana | ✅ |
| **Week 3** | Validate dataset and engineer additional features | Nur Firzana | ✅ |
| **Week 3** | Optimize data processing using Multiprocessing | Nuraisyah | ✅ |
| **Week 3** | Benchmark baseline vs optimized pipeline | Nuraisyah | ✅ |
| **Week 4** | Generate performance charts and analysis | Nuraisyah | ✅ |
| **Week 4** | Prepare technical report and documentation | All Members | ✅ |
| **Week 4** | Prepare presentation slides | All Members | ✅ |
| **Week 4** | Submit final report, source code, and presentation | All Members | ✅ |
| **Week 4** | Final project presentation | All Members | ✅ |

---
