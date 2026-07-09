# 🥔 BigPotato | Shopee Real-Time Sentiment High Performance Data Processing Pipeline

This repository contains the project work for **SECP3133 High Performance Data Processing**. Our project focuses on building an end-to-end real-time sentiment analysis data pipeline utilizing customer reviews from **Shopee Malaysia** collected via the Google Play Store.

Instead of traditional batch processing alone, this project implements a production-grade streaming pipeline and comprehensively evaluates system performance differences (throughput, resource usage, latency) between batch and real-time streaming architectures.

Video Link: [https://youtu.be/vumdt10_g0A ](https://youtu.be/vumdt10_g0A)

---

## 📌 Project Title

**Shopee Application Real-Time Sentiment Analysis Using Apache Spark and Kafka** 

---

## 👥 Team Members

| Name                    | Matric No |
| ----------------------- | --------- |
| Cheryl Cheong Kah Voon  | A23CS0060 |
| Chau Ying Jia           | A23CS0213 |
| Lau Yee Wen             | A23CS0099 |
| Poh Lok Yee             | A23CS0262 |


---

## 📝 Project Summary

The core objective of this project is to implement, evaluate, and optimize a highly available, real-time sentiment analysis infrastructure for massive numbers of e-commerce application reviews.

The project pipeline encompasses:
1. **Data Acquisition:** Scraped target public reviews from Shopee Malaysia's official Google Play Store application.
2. **NLP Preprocessing:** Rule-based tokenization, lowercase standardization, stop-word filtering (English & Malay), and root-word lemmatization.
3. **Sentiment Model Selection:** Vectorized text using TF-IDF and trained/evaluated three machine learning classifiers (Multinomial Naive Bayes, Logistic Regression, and Linear SVM).
4. **Streaming Architecture Deployment:** Dockerized ecosystem linking a Python Kafka Producer, an Apache Kafka Message Streaming layer, and an Apache Spark Structured Streaming processing frame.
5. **Storage & BI Analytics:** Structured outputs ingestion into Elasticsearch for real-time Kibana query monitoring and custom multi-page Power BI analytical dashboarding.
6. **Performance Benchmarking:** Evaluated and compared Batch vs. Streaming workloads regarding latency, computational usage, and system constraints.

---

## 🌐 Target Website & Source

* **Platform:** Google Play Store 
* **Target Application:** Shopee Malaysia (`com.shopee.my`) 
* **Data Type:** Public user ratings, metadata, and written textual reviews 

---

## 📂 Data Collected & Attributes

The ingestion tool safely filters out personal credentials (usernames, profile pictures) and retains analytical vectors:

| Attribute | Data Type | Description |
| :--- | :--- | :--- |
| `Review ID` | String | Unique hash identifier assigned to each individual review  |
| `Rating` | Integer | Customer experience score ranging from 1 to 5 stars  |
| `Review Date` | Date | Timestamp of submission  |
| `App Version` | String | Version string of the Shopee application used by the reviewer  |
| `Thumbs Up Count` | Integer | Volume of peer-voted helpful feedback marks  |
| `Original Text` | Text | Raw written textual commentary  |
| `Cleaned Text` | Text | Extracted target feature generated post-NLP cleaning steps |

---

## 📊 Dataset Overview

| Description | Value |
| :--- | :--- |
| **Raw Reviews Scraped** | 10,000 records  |
| **Final NLP Cleaned Base Records** | 6,395 records  |
| **Class Distribution Balance** | Positive: 3,945 (61.69%) \| Negative: 2,203 (34.45%) \| Neutral: 247 (3.86%)  |
| **Dataset Format** | Exported CSV formats (`cleaned_data.csv`)  |

---

## System Architecture


    Shopee Malaysia Reviews CSV (cleaned_data.csv)
                          ↓
              Kafka Producer (Python)
                          ↓
           Kafka Topic: sentiment-topic
                          ↓
          Apache Spark Structured Streaming
                          ↓
    Loads Persistent TF-IDF Vectorizer + MNB Model Pipeline
                          ↓
       Elasticsearch Index + Local CSV Output (Dual-Sink)
                          ↓
             Kibana / Power BI Dashboard

## 🧹 Data Cleaning & NLP Pipeline

A structured rule-based processing loop was developed in Python utilizing Pandas and NLTK:
* Eliminated redundant rows (matching review IDs or identical texts).
* Purged invalid inputs consisting solely of emojis, numeric symbols, or string fragments under 3 words.
* Enforced casing normalization into lowercase.
* Removed specific patterns: URLs, embedded HTML tags, special symbols, and punctuations.
* Dropped frequent English and common Malay stop-words.
* Derived clean root tokens using English structural lemmatization.

---

## 🛠️ Tools and Frameworks

| Ecosystem | Tool / Component | Usage Specification |
| :--- | :--- | :--- |
| **Core Language** | Python 3.x | System building, scripting, and scikit-learn training environment |
| **Environment** | Docker / Docker Compose | Containerized deployment of Kafka, Zookeeper, and Elasticsearch services |
| **Ingestion** | Google Play Scraper | Scraped real-world text comments via paginated requests |
| **Streaming** | Apache Kafka & Zookeeper | Real-time messaging queue, decoupling and event broker |
| **Processing** | Apache Spark Structured Streaming | Real-time micro-batch processor and ML model executor |
| **ML Engine** | Scikit-learn (TF-IDF Vectorizer) | Feature extraction mapping Unigrams/Bigrams up to 10,000 features |
| **Storage** | Elasticsearch (v8.13.4) | Indexing node for real-time document insertions |
| **Analytics** | Kibana & Power BI | Pipeline stream metric monitoring, and interactive analytical visualization |

---

## ⚡ Machine Learning Optimization & Sentiment Models

Ratings were converted into definitive target labels: **Negative** (1–2 Stars), **Neutral** (3 Stars), and **Positive** (4–5 Stars). Due to severe class imbalance, optimization included `class_weight='balanced'` for linear models.

### Offline Training Performance Metrics (Independent Test Set: 1,279 Samples)

| Model Candidate | Model Accuracy | Precision (Weighted) | Recall (Weighted) | F1-Score (Weighted) | Macro Avg F1 | Selection Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Multinomial Naive Bayes (MNB)** | **88.19%** | 0.8538 | 0.8819 | **0.8663** | 0.59 | **Selected (Best)** |
| **Logistic Regression** | 84.75% | **0.8629** | 0.8475 | 0.8475 | 0.63 | Candidate |
| **Linear Support Vector Machine** | 86.40% | 0.8574 | 0.8640 | 0.8592 | **0.64** | Candidate |

* **Selection Rationale:** MNB recorded the highest accuracy and weighted F1-score. Its low computational overhead makes it perfect for streaming inferences within high-throughput micro-batches.

---

## 📈 Performance Evaluation: Batch vs. Streaming Processing

Both modes were evaluated over the full set of 6,395 records to observe pipeline behavior:

| Evaluation Metric | Batch Processing Mode | Real-Time Streaming Processing Mode |
| :--- | :---: | :---: |
| **Total Records Evaluated** | 6,395 records | 6,395 records |
| **Total Execution Time** | **0.3968 seconds** | 9.0977 seconds |
| **System Throughput** | **16,116.71 rec/sec** | 702.92 rec/sec |
| **CPU Utilization Rate** | 95.90% (Short intensive burst) | **6.01% (Evenly distributed load)** |
| **Peak Memory Consumption** | **236.23 MB** | 240.29 MB |
| **Classification accuracy** | 88.19% | 95% (based on 20 streaming sample) |
| **Elasticsearch Write Status** | Not Applicable | 100% Success (0 Failed Docs) |

### 🔍 Core Structural Findings
* **Batch Mode Advantages:** Faster execution and high throughput since records are processed within a single, dedicated execution loop. Ideal for historical analytics.
* **Streaming Mode Advantages:** Far lower peak CPU utilization since workloads are safely distributed across sequential micro-batches over time. It is the only option suitable for live user monitoring and continuous real-time data integration.

---

## 📁 Repository Structure

```
  BigPotato/
  │
  ├── dashboard/                      # Power BI Dashboard .pbix source files
  ├── data/
  │   ├── raw_data.csv                 # Raw crawled review records from Google Play
  │   └── cleaned_data.csv            # Final NLP standardized dataset
  │
  ├── kafka_spark_pipeline/           # Real-time streaming source scripts
  │   ├── docker-compose.yml          # Infrastructure setup for Zookeeper, Kafka, ES, Kibana
  │   ├── kafka_producer.py           # Simulates live review ingestion streams
  │   ├── spark_streaming.py
  │   ├── test_model.py   
  │   └── streaming_prediction.csv
  │
  ├── models/
  │   └── best_sentiment_model.pkl                         
  │
  ├── notebooks/                     
  │   ├── preprocessing.ipynb
  │   ├── model_training.ipynb
  │   └── preprocessingAndModelTraining
  │
  ├── reports/
  │   └── HPDP_Project2_Report.pdf    # Full Project Documentation Report
  │   └── Project 2 Slides.pdf
  │
  └── README.md

```
---

## 👥 Team Members & Roles 

Based on our updated workflow diagram, system responsibilities are designated as follows:

| Member | Main Responsibility |
| :--- | :--- |
| **Poh Lok Yee** | Conducted data acquisition via Google Play Scraper and built the rule-based NLP text cleaning pipeline. |
| **Cheryl Cheong Kah Voon** | Built the machine learning workspace, evaluated classifiers, and exported the optimized MNB pipeline. |
| **Lau Yee Wen** | Architected the containerized infrastructure and coordinated the real-time Kafka-Spark streaming loop. |
| **Chau Ying Jia** | Developed interactive Power BI dashboards and conducted Batch vs. Streaming performance benchmarking. |
---

## 🎓 Course Information

* **Course:** SECP3133 High Performance Data Processing Section 2
* **Lecturer:** Dr. Seah Choon Sean
* **Faculty:** Faculty of Computing, Universiti Teknologi Malaysia
* **Semester:** Semester 2 2025/2026
