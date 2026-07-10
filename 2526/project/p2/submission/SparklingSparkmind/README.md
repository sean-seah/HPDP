# Project 2: Real-Time Sentiment Analysis using Apache Spark and Kafka

**Course:** SECP3133 High Performance Data Processing
**Semester:** 2025/2026, Semester 2
**Deadline:** -

## Group Members

| Name | Matric No. | Role |
|---|---|---|
| DAYANG FARAH FARZANA BINTI ABANG IDHAM | A23CS0071 | Data & NLP Engineer (Acquisition + Preprocessing) |
| FARRA NURZAHIN BINTI ZAHARIL ANUAR | A23CS0079 | Model Engineer (Sentiment Classification) |
| SAFIYA NURSYAHADAH BINTI MASNOOR | A23CS0176 | Pipeline Engineer (Kafka + Spark + Storage) |
| AIN NURNABILA BINTI MOHD AZHAR | A23CS0207 | Visualization & Reporting Engineer |

## Presentation Link
[Click here](https://youtu.be/ivEmCZQMBds?si=yJZh569muOXt-5wK)

## Project Summary

Real-time sentiment analysis pipeline on Malaysian-relevant text data, using:
- **Kafka** for streaming ingestion
- **Spark Structured Streaming** for parallel processing and model inference
- **Elasticsearch / Apache Druid** for storage
- **Kibana / Apache Superset** for visualization

## Data Source

- **Source:** 
- **Collection tool:** 
- **Approx. volume collected:** 


## System Workflow

```
Raw Review Dataset
        |
        v
Data Cleaning and Preprocessing (notebooks/preprocessing.ipynb)
        |
        v
TF-IDF Feature Extraction
        |
        v
Model Training and Evaluation (Naive Bayes + LSTM)
        |
        v
Kafka Producer (kafka_producer.py)
        |
        v
Kafka Topic: sentiment-input
        |
        v
Spark Structured Streaming (spark_streaming.py)
        |
        v
Sentiment Prediction (Naive Bayes + TF-IDF, via broadcast UDF)
        |
        v
Elasticsearch Index: sentiment-predictions
        |
        v
Kibana Dashboard

```

## Technology Used
| Layer | Technology |
|---|---|
| Streaming ingestion | Apache Kafka (Confluent images) + Zookeeper |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Sentiment models | scikit-learn (Naive Bayes + TF-IDF), TensorFlow/Keras (LSTM) |
| Storage / indexing | Elasticsearch |
| Visualization | Kibana |
| Containerization | Docker & Docker Compose |
| Language / libraries | Python, pandas, kafka-python, requests |
| Development | Jupyter Notebook |

## Models Compared
| Model | Category | Library |
|---|---|---|
| Naive Bayes (TF-IDF) | Machine Learning | scikit-learn |
| LSTM | Deep Learning | TensorFlow / Keras |

Evaluated using accuracy, precision, recall, F1 score, and confusion matrix (70/20/10 train/test/validation split).

## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the infrastructure (Kafka, Zookeeper, Elasticsearch, Kibana)
```bash
cd kafka_spark_pipeline
docker-compose up -d
```
This spins up:
- `zookeeper` (port 2181)
- `kafka` (port 9092)
- `elasticsearch` (port 9200)
- `kibana` (port 5601)

Wait until all containers are healthy before continuing (`docker ps` to check).

### 3. Run preprocessing and train the models (if not already done)
```bash
jupyter notebook notebooks/preprocessing.ipynb
jupyter notebook model_training.ipynb
```
This produces `data/cleaned_data.csv`, `naive_bayes_model.pkl`, and `lstm_model.keras` used by the streaming job.

### 4. Start the Kafka producer
```bash
cd kafka_spark_pipeline
python kafka_producer.py
```
This reads `data/cleaned_data.csv` and streams each review as a JSON message to the Kafka topic `sentiment-input`.

### 5. Start the Spark Structured Streaming job
```bash
cd kafka_spark_pipeline
spark-submit spark_streaming.py
```
This consumes from `sentiment-input`, runs real-time Naive Bayes predictions, and writes each batch to Elasticsearch's `sentiment-predictions` index via REST API.

### 6. View the dashboard
- Open Kibana at [http://localhost:5601](http://localhost:5601) and import `kibana_visualizations.ndjson` for the pre-built dashboards, **or**
- Open `kafka_spark_pipeline/dashboard/dashboard_prototype.html` directly in a browser for the static prototype view.

### 7. Stop the infrastructure
```bash
docker-compose down
```

## Repository Structure

```
HPDP/2526/project/Sparkling Sparkmind/
├── README.md
├── data/
│   ├── raw_data/
│   └── cleaned_data.csv
├── notebooks/
│   └── preprocessing.ipynb
├── model_training.ipynb
├── kafka_spark_pipeline/
│   ├── spark_streaming.py
│   ├── docker-compose.yml
│   ├── kafka_producer.py
│   ├── lstm_model.keras
│   ├── lstm_tokenizer.pkl
│   ├── naive_bayes_model.pkl
│   ├── dashboard/
│   └── elastic_mappings.json
|
├── kibana_visualizations.ndjson
├── reports/
│   └── final_report.pdf
├── presentation_slides.pptx
└── requirements.txt
```

## Deliverables Status

Mapped to the 5 required submission items (Section 7 of the brief):

| # | Deliverable | Status | Location |
|---|---|---|---|
| 1 | Final Report (PDF) | Done ✅ | `reports/final_report.pdf` |
| 2 | Source Code | Done ✅ | `kafka_spark_pipeline/`, `notebooks/`, `model_training.ipynb` |
| 3 | Dashboard + Dataset | Done ✅ | `kafka_spark_pipeline/dashboard/`, `data/cleaned_data.csv` |
| 4 | Model Comparison |Done ✅ | `model_training.ipynb`, `reports/final_report.pdf` (Section 3) |
| 5 | Presentation Slides | Done ✅ | `presentation_slides.pptx` |

## Progress Checklist

- [✅] Data source confirmed with lecturer (Week 1)
- [✅] Data collection complete
- [✅] Preprocessing pipeline complete
- [✅] At least 2 sentiment models trained and evaluated
- [✅] Kafka broker + topic configured
- [✅] Spark Structured Streaming job integrated with trained model
- [✅] Storage layer (Elasticsearch/Druid) connected
- [✅] Batch vs. streaming comparison complete
- [✅] Dashboards built (pie chart, sentiment-over-time, word clouds)
- [✅] Final report compiled
- [✅] Presentation slides finalized

## Academic Integrity

All work in this repository is original and produced by the listed group members. Public datasets and open-source libraries used for reference or training are credited in the final report's References section, per the course's academic integrity policy.
