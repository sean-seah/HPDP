# Real-Time Sentiment Analysis of Malaysian Telecommunication App Reviews

# Team Members
1. Brendan Chia Yan Fei
2. Choh Jing Yi
3. Lee Yin Shen
4. Tan Zhi Ming

## Video Presentation
- https://youtu.be/VIeWSsURQiE
  
## Project Overview

This project implements a real-time sentiment analysis pipeline for Malaysian telecommunication app reviews. The system uses Google Play review data from Malaysian telco providers such as Celcom, Maxis, Digi, and U Mobile.

The workflow includes data preprocessing, sentiment model training, Kafka streaming, Spark Structured Streaming, Elasticsearch storage, and Kibana dashboard visualization. The final system classifies review messages into **Positive**, **Neutral**, or **Negative** sentiment.

---

## Technology Stack

| Component          | Technology                        |
| ------------------ | --------------------------------- |
| Data Processing    | Python, Pandas                    |
| Feature Extraction | TF-IDF                            |
| Machine Learning   | scikit-learn                      |
| Streaming          | Apache Kafka                      |
| Stream Processing  | Apache Spark Structured Streaming |
| Storage            | Elasticsearch                     |
| Visualization      | Kibana                            |
| Deployment         | Docker Compose                    |

---

## Dataset

The dataset used is the **Malaysian Telecommunication Google Play Reviews** dataset.

The raw dataset contains **159,444 records** with review text, rating score, review date, provider name, helpful vote count, and username. After preprocessing, the final cleaned dataset contains **155,902 records**.

Sentiment labels were generated using the rating score:

| Rating Score | Sentiment |
| ------------ | --------- |
| 1-2          | Negative  |
| 3            | Neutral   |
| 4-5          | Positive  |

---

## System Workflow

```text
Raw Review Dataset
        |
        v
Data Cleaning and Sentiment Labelling
        |
        v
TF-IDF Feature Extraction
        |
        v
Model Training and Evaluation
        |
        v
Kafka Producer
        |
        v
Kafka Topic: telecom-reviews
        |
        v
Spark Structured Streaming
        |
        v
Sentiment Prediction
        |
        v
Elasticsearch Index: telecom-sentiment
        |
        v
Kibana Dashboard
```

---

## Main Results

Two sentiment classification models were trained and compared.

| Model               | Accuracy | Weighted F1-score |
| ------------------- | -------: | ----------------: |
| Naive Bayes         |   83.06% |            80.56% |
| Logistic Regression |   86.46% |            83.95% |

Logistic Regression achieved the better performance and was selected as the final model for the streaming pipeline.

During the streaming test, the system produced the following results:

| Metric                        |             Value |
| ----------------------------- | ----------------: |
| Producer messages sent        |            17,633 |
| Producer throughput           |  19.5 records/sec |
| Spark records processed       |            17,251 |
| Spark micro-batches processed |             1,176 |
| Spark throughput              |  28.1 records/sec |
| Elasticsearch index           | telecom-sentiment |

Kibana was used to visualize the stored prediction results, including sentiment distribution, provider comparison, review trends, rating distribution, and negative feedback analysis.

---

## Folder Structure

```text
DataAnalysis/
|-- README.md
|-- dashboard/
|   |-- 01_sentiment_distribution.png
|   |-- 02_sentiment_breakdown_by_provider.png
|   |-- 03_correct_prediction_records.png
|   |-- 04_reviews_over_time.png
|   |-- 05_top_negative_providers.png
|   |-- 06_rating_distribution.png
|   |-- 07_dashboard_overview.png
|   |-- elastic_mappings.json
|   |-- kibana_visualizations.ndjson
|   `-- visual_analysis.md
|-- data/
|   |-- cleaned_data.csv
|   `-- raw_data/
|-- kafka-spark-pipeline/
|   |-- config.py
|   |-- docker-compose.yml
|   |-- Dockerfile
|   |-- kafka_producer.py
|   |-- README.md
|   |-- requirements.txt
|   `-- spark_streaming.py
|-- models/
|   |-- sentiment_model.pkl
|   `-- vectorizer.pkl
|-- notebooks/
|   |-- preprocessing.ipynb
|   `-- model_training.ipynb
|-- reports/
|   `-- Project_2_Final_Report.pdf
`-- screenshots member2/
```

---

## How to Run

Go to the pipeline folder:

```bash
cd kafka-spark-pipeline
```

Start the required services:

```bash
docker compose up -d zookeeper kafka elasticsearch kibana
```

Start Spark Streaming:

```bash
docker compose run spark-streaming
```

In another terminal, start the Kafka producer:

```bash
docker compose run kafka-producer
```

Elasticsearch can be accessed at:

```text
http://localhost:9200
```

Kibana can be accessed at:

```text
http://localhost:5601
```

---

## Requirements Checklist

| Requirement                                        | Status    |
| -------------------------------------------------- | --------- |
| Malaysian-relevant text dataset                    | Completed |
| Text preprocessing                                 | Completed |
| Positive, Neutral, Negative sentiment labelling    | Completed |
| At least two sentiment models trained and compared | Completed |
| Apache Kafka streaming                             | Completed |
| Apache Spark Structured Streaming                  | Completed |
| Model integrated into streaming pipeline           | Completed |
| Elasticsearch storage                              | Completed |
| Kibana dashboard visualization                     | Completed |
| Batch vs streaming comparison                      | Completed |
| Final report and GitHub submission files           | Completed |

---

## Limitations

This project was implemented as a local proof of concept. Due to local runtime constraints, only a subset of the cleaned dataset was streamed during the recorded test run.

The Kibana dashboard evidence mainly shows correctly predicted records because the exported dashboard includes a `match:true` validation filter. Therefore, the dashboard percentages should be interpreted as validation-view results rather than a full summary of the entire cleaned dataset.

---

## Project Status

The project is completed as a working local demonstration of a real-time sentiment analysis pipeline using Apache Kafka, Apache Spark, Elasticsearch, and Kibana.
