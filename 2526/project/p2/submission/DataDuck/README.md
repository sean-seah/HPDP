# Project 2: Real-Time Sentiment Analysis of MAE by Maybank Reviews using Apache Spark and Kafka

**Course:** SECP3133 High Performance Data Processing  
**Semester:** 2025/2026 Semester 2

---

# Group Members

| Member | Matric No. | Responsibility |
|---------|------------|----------------|
| SABRINA HENG WEI QI | A23CS0265 | Data Collection & NLP Preprocessing |
| WOO CHENG SHUAN | A23CS0283 | Sentiment Model Development (Naive Bayes & LSTM) |
| LING YU QIAN | A23CS0301 | Model Evaluation & Performance Comparison |
| GUI KAH SIN | A23CS0080 | Elasticsearch Integration, Kibana Dashboard & Word Cloud |

---

# Project Overview

This project implements a real-time sentiment analysis pipeline for customer reviews of the **MAE by Maybank** mobile banking application.

Google Play Store reviews were collected and preprocessed before two sentiment classification models, **Naive Bayes** and **Long Short-Term Memory (LSTM)**, were developed and evaluated. The selected model was then integrated into a real-time streaming architecture using **Apache Kafka** and **Apache Spark Structured Streaming**. Prediction results were stored in **Elasticsearch** and visualized using **Kibana**, with additional keyword analysis performed using Python-generated word clouds.

The complete pipeline demonstrates how modern big data technologies can support real-time customer feedback monitoring and decision-making.

---

# Technology Stack

| Component | Technology |
|-----------|------------|
| Programming Language | Python |
| Data Collection | Google Play Scraper |
| Data Processing | Pandas, NumPy |
| NLP | NLTK, spaCy |
| Machine Learning | Scikit-learn (Naive Bayes) |
| Deep Learning | TensorFlow / Keras (LSTM) |
| Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Storage | Elasticsearch |
| Dashboard | Kibana |
| Visualization | Matplotlib, WordCloud |
| Deployment | Docker Compose |

---

# Dataset

**Source:** Google Play Store (MAE by Maybank)

| Description | Records |
|------------|--------:|
| Original Reviews Collected | 5,000 |
| Cleaned Dataset | 4,155 |
| Training Dataset | 4,130 |

Reviews were labelled according to the original star ratings.

| Rating | Sentiment |
|--------|-----------|
| 1 – 2 | Negative |
| 3 | Neutral |
| 4 – 5 | Positive |

---

# System Architecture

```text
Google Play Reviews (5,000)
            │
            ▼
Data Cleaning & Preprocessing
(4,155 cleaned reviews)
            │
            ▼
Kafka Producer
(kafka_producer.py)
            │
            ▼
Kafka Topic
app-reviews
            │
            ▼
Spark Structured Streaming
            │
            ▼
TF-IDF + Naive Bayes Prediction
            │
            ▼
Elasticsearch
(app-reviews-sentiment)
            │
            ▼
Kibana Dashboard
            │
            ▼
Python Word Cloud
```

---

# Model Performance

Two sentiment classification models were developed and compared.

| Model | Accuracy |
|------|---------:|
| Naive Bayes | **78.69%** |
| LSTM | 76.63% |

Naive Bayes achieved the better overall performance while requiring lower computational resources and was therefore selected for deployment within the real-time streaming pipeline.

---

# Dashboard Overview

The Kibana dashboard provides real-time monitoring of customer sentiment through the following visualizations:

- Total Reviews Processed
- Positive Percentage
- Negative Percentage
- Sentiment Distribution
- Rating Distribution
- Sentiment by Star Rating
- Sentiment Over Time

Additional Python-generated visualizations include:

- Positive Reviews Word Cloud
- Negative Reviews Word Cloud

During the streaming demonstration, **4,164 review records** were stored in Elasticsearch. This number includes **4,155 cleaned review records** together with several additional test messages published during pipeline verification.

---

# Repository Structure

```text
DataDuck_Project2/
│
├── data/
│   ├── raw_data/
│   └── cleaned_data.csv
│
├── models/
│   ├── naive_bayes_model.pkl
│   ├── tfidf_vectorizer.pkl
│   └── label_encoder.pkl
│
├── notebooks/
│   ├── 01_collect_reviews.ipynb
│   ├── 02_nlp_preprocessing.ipynb
│   └── 03_model_training.ipynb
│
├── kafka_spark_pipeline/
│   ├── kafka_producer.py
│   ├── spark_streaming.py
│   ├── pipeline_config.py
│   └── docker-compose.yml
│
├── generate_wordcloud.py
├── requirements.txt
├── README.md
└── reports/
    └── Final_Report.pdf
```

---

# Prerequisites

Before running the project, ensure the following software is installed:

- Python 3.11
- Java 17 or above
- Docker Desktop

Python dependencies:

```bash
pip install -r requirements.txt
```

---

# How to Run

### 1. Start Docker Services

```bash
docker compose up -d
```

---

### 2. Create Kafka Topic

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
--bootstrap-server localhost:9092 \
--create \
--topic app-reviews \
--partitions 1 \
--replication-factor 1
```

---

### 3. Start Spark Streaming

```bash
python kafka_spark_pipeline/spark_streaming.py
```

---

### 4. Publish Review Messages

Open another terminal.

```bash
python kafka_spark_pipeline/kafka_producer.py
```

---

### 5. Access Elasticsearch

```
http://localhost:9200
```

---

### 6. Access Kibana

```
http://localhost:5601
```

---

### 7. Generate Word Clouds

```bash
python generate_wordcloud.py
```

---

# Deliverables

- ✅ Data Collection
- ✅ NLP Preprocessing
- ✅ Naive Bayes Model
- ✅ LSTM Model
- ✅ Model Evaluation
- ✅ Apache Kafka Streaming
- ✅ Apache Spark Structured Streaming
- ✅ Elasticsearch Storage
- ✅ Kibana Dashboard
- ✅ Python Word Clouds
- ✅ Final Report
- ✅ GitHub Repository

---

# Project Status

This project has been successfully completed as an end-to-end real-time sentiment analysis system integrating Apache Kafka, Apache Spark Structured Streaming, Elasticsearch, and Kibana for continuous customer review monitoring.
