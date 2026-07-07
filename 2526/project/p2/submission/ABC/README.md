# Real-Time Sentiment Analysis of TNG eWallet Reviews using Apache Spark and Kafka

A real-time streaming sentiment analysis system that continuously processes Touch 'n Go eWallet customer reviews from the Google Play Store using Apache Kafka, Apache Spark Structured Streaming, Elasticsearch, and Kibana.

Developed for **SECP3133 High Performance Data Processing** at Universiti Teknologi Malaysia (UTM). :contentReference[oaicite:1]{index=1}

---

# Team Members

| Member | Responsibility |
|---------|----------------|
| Joanne Ching Yin Xuan | Data Collection & Preprocessing |
| Lim Yu Han | Apache Spark Streaming, Elasticsearch & Kibana Integration |
| Chua Jia Lin | Machine Learning Model Development |
| Evelyn Goh Yuan Qi | Model Evaluation & Performance Analysis |

---

# Project Overview

Customer reviews provide valuable feedback for improving digital payment applications. However, analysing thousands of reviews manually is time-consuming and unsuitable for real-time monitoring.

This project develops an end-to-end streaming analytics pipeline that automatically classifies customer reviews into Positive, Neutral, and Negative sentiments. Reviews are streamed through Apache Kafka, processed by Apache Spark Structured Streaming using a trained Naive Bayes model, stored in Elasticsearch, and visualized through Kibana dashboards for continuous sentiment monitoring. :contentReference[oaicite:2]{index=2}

---

# Technologies Used

| Category | Technology |
|----------|------------|
| Programming Language | Python |
| Data Collection | Google Play Scraper |
| Data Processing | Pandas |
| NLP | NLTK |
| Feature Extraction | TF-IDF |
| Machine Learning | Scikit-learn |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Search Engine | Elasticsearch |
| Dashboard | Kibana |
| Deployment | Docker Compose |

---

# Dataset

**Source:** Google Play Store (Touch 'n Go eWallet)

Approximately **20,000** customer reviews were collected and preprocessed before model development.

### Sentiment Labelling

| Rating | Sentiment |
|--------|-----------|
| 1–2 | Negative |
| 3 | Neutral |
| 4–5 | Positive |

---

# System Workflow

```text
Google Play Reviews
        │
        ▼
Data Collection
        │
        ▼
Text Cleaning & NLP Preprocessing
        │
        ▼
TF-IDF Feature Extraction
        │
        ▼
Model Training
(Naive Bayes & Linear SVM)
        │
        ▼
Best Model Selection
(Naive Bayes)
        │
        ▼
Kafka Producer
        │
        ▼
Kafka Topic
(tng_reviews)
        │
        ▼
Apache Spark Structured Streaming
        │
        ▼
Real-Time Sentiment Prediction
        │
        ▼
Elasticsearch
(tng_sentiment_stream)
        │
        ▼
Kibana Dashboard
```

---

# Machine Learning Model Performance

Two supervised machine learning algorithms were trained and evaluated for sentiment classification. The models were compared using Accuracy, Precision, Recall, and F1-score. Based on the experimental results, **Naive Bayes** achieved the best overall performance and was selected as the deployment model for the real-time streaming pipeline.

| Model | Accuracy | Precision | Recall | F1-score |
|------|---------:|----------:|-------:|---------:|
| **Naive Bayes** | **88.93%** | **86.01%** | **88.93%** | **87.35%** |
| Linear SVM | 86.84% | 86.18% | 86.84% | 86.43% |

The selected Naive Bayes model was integrated into the Apache Spark Structured Streaming application to perform real-time sentiment prediction on incoming TNG eWallet customer reviews received from Apache Kafka.

---

# Kibana Dashboard

The Kibana dashboard provides real-time visualization of processed sentiment data, including:

- Sentiment Distribution
- Actual vs Predicted Sentiment
- Prediction Confidence
- Prediction Confidence by Rating
- Real-Time Review Stream
- Sentiment Trend Over Time
- Positive Review Word Cloud
- Negative Review Word Cloud

---

# Project Highlights

✅ Collected approximately 20,000 Google Play reviews

✅ Applied NLP preprocessing and TF-IDF feature extraction

✅ Compared Naive Bayes and Linear SVM classifiers

✅ Built an Apache Kafka real-time streaming pipeline

✅ Processed streaming data using Apache Spark Structured Streaming

✅ Stored streaming results in Elasticsearch

✅ Developed interactive Kibana dashboards

✅ Evaluated streaming performance using processing time, throughput, and micro-batch statistics

---

# Repository Structure

```text
Project2/
│
├── dashboard/
│   ├── visual_analysis.md
│   ├── kibana_dashboard.ndjson
│   └── elastic_mappings.json
│
├── data/
│   ├── raw/
│   └── cleaned/
│
├── models/
│   ├── naive_bayes_model.pkl
│   ├── linear_svm_model.pkl
│   └── vectorizer.pkl
│
├── notebooks/
│   ├── 01_collect_reviews.ipynb
│   └── 02_nlp_preprocessing.ipynb
│   ├── 03_model_training.ipynb
│   └── 04_stream_pipeline_evaluation.ipynb
│
├── kafka_spark_pipeline/
│   ├── kafka_producer.py
│   ├── spark_streaming.py
│   ├── sentiment_preprocess.py
│
├── reports/
│   └── Group_ABC_Report_P2.pdf
│
├── docker-compose.yml
└── README.md
└── requirements.txt
```

---

# Course

**SECP3133 High Performance Data Processing**

Faculty of Computing

Universiti Teknologi Malaysia

Semester 2 2025/2026

---

# Project Status

**Completed**

An end-to-end real-time sentiment analysis system integrating Apache Kafka, Apache Spark Structured Streaming, Elasticsearch, and Kibana for continuous customer sentiment monitoring.
