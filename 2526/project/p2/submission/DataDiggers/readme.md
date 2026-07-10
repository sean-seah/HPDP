# 👀 Project 2: Real-Time Sentiment Analysis using Apache Spark and Kafka

✦ Data source: Google Play reviews — Touch 'n Go, Grab, foodpanda

| Name | Matric Number | Role
|------|------|------|
| NUR FIRZANA BINTI BADRUS HISHAM | A23CS0156 | Data Engineer |
| NURAISYAH BINTI MOHD ZIKRE | A23CS0160 | Model Engineer |
| LUBNA AL HAANI BINTI RADZUAN | A23CS0107 |  Pipeline Engineer |
| NURUL IKA SYAFINY BINTI AZHAR | A23CS0164 | Visualization & Performance Engineer |

Video Presentation: [youtube link]( )
Slide Presentation: [Slide]( )


## 1. Overview

This project implements an end-to-end **real-time sentiment analysis pipeline** for Malaysian mobile app reviews. It combines Natural Language Processing (NLP) with a big-data streaming architecture to classify user reviews from three widely used Malaysian apps — **Touch 'n Go eWallet**, **Grab**, and **foodpanda** — as **Positive**, **Neutral**, or **Negative** in real time.

The system integrates:
- **Apache Kafka** — message broker / stream ingestion
- **Apache Spark Structured Streaming** — micro-batch stream processing and inference engine
- **Elasticsearch** — storage and indexing of classified results
- **Kibana** — interactive dashboard for sentiment visualization
- **Docker Compose** — containerized deployment of the full stack

## 2. Objectives

- Collect and preprocess a text dataset of Malaysian app reviews (Touch 'n Go eWallet, Grab, foodpanda) from the Google Play Store.
- Design, train, and evaluate two sentiment classification models — a traditional ML model (**Naive Bayes**) and a deep learning model (**LSTM**) — and compare them on standard NLP metrics.
- Build a real-time streaming pipeline (Kafka → Spark Structured Streaming) that applies the best-performing model to incoming reviews.
- Store and visualize sentiment results in an interactive **Kibana** dashboard (distribution chart, sentiment-over-time chart, word clouds).
- Evaluate and compare **batch vs. streaming** processing modes on throughput, latency, accuracy, and resource utilization.

## 3. Scope

- English-language reviews only, scraped from the Malaysia (MY) Google Play Store.
- Sentiment labels are derived automatically from star ratings: 1–2★ = Negative, 3★ = Neutral, 4–5★ = Positive.
- Public datasets (Sentiment140, IMDb) were used only as supplementary training references; the final pipeline processes Malaysian app reviews only.
- All tools used are open source and containerized via Docker Compose.
- **Out of scope:** production cloud deployment, live social media API integration, multilingual sentiment analysis.

## 4. Repository Structure

```
HPDP/2526/project/p2/submission/DataDiggers/
├── readme.md
├── data/
│   ├── raw_data/
│   |   ├── cleaned_step1.csv
│   |   ├── labeled_reviews.csv
│   |   ├── lemmatized.csv
│   |   ├── raw_reviews.csv
│   |   └── tokenized.csv
│   └── cleaned_data.csv        
|
├── data_cleaning/              
│   ├── clean_text.ipynb        # step 3
│   ├── export_dataset.ipynb    # step 6
│   ├── label_data.ipynb        # step 2
│   ├── lemmatize.ipynb         # step 5
│   ├── scrape_reviews.ipynb    # step 1
│   └── tokenize_review.ipynb   # step 4
|
├── model_training/
│   └── model_training.ipynb
│   └── model/                      
│       └── naive_bayes_model.pkl
│       └── tfidf_vectorizer.pkl
│       └── label_encoder.pkl   
│       └── lstm_tokenizer.pkl      
│       └── lstm_model.h5           
|
├── kafka_spark_pipeline/
│   ├── docker-compose.yml
│   ├── elastic_mappings.json
│   ├── producer.py
|   └── spark_stream.py
│   └── dashboard/
│       ├── wordcloud_generator.py
│       ├── performance_test_batch.py
│       ├── streaming_monitor.py
│       ├── compare_results.py
│       └── results/                    
│           ├── batch_results.json
│           ├── streaming_results.json
│           ├── comparison_chart.png
│           ├── comparison_table.md
│           ├── wordcloud_positive.png
│           └── wordcloud_negative.png
|
├── kibana_visualizations.ndjson
|
├── reports/
│   └── final_report.pdf
|
├── presentation_slides.pdf
└── requirements.txt
└── requirements_dashboard.txt
```

## 5. Data Pipeline

**Source:** Google Play Store reviews, collected via the `google-play-scraper` Python library (no API key required), filtered to English-language reviews from the Malaysia store.

| Application | Package ID | Reviews Collected |
|---|---|---|
| Touch 'n Go eWallet | `my.com.tngdigital.ewallet` | 5,000 |
| Grab | `com.grabtaxi.passenger` | 5,000 |
| foodpanda | `com.global.foodpanda.android` | 5,000 |
| **Total** | | **15,000** |

**Sentiment labeling** (rule-based, from star rating):

| Star Rating | Label |
|---|---|
| 1–2 | Negative |
| 3 | Neutral |
| 4–5 | Positive |

**Preprocessing pipeline (6 stages):**
1. Lowercasing
2. Noise removal (URLs, HTML tags, mentions, digits, punctuation, emojis)
3. Tokenization (NLTK `word_tokenize`)
4. Stop word removal (NLTK stop words, with negation words such as *not, no, never, but* preserved)
5. Lemmatization (spaCy `en_core_web_sm`)
6. Export — records with <5 characters removed, final dataset saved as `cleaned_data.csv`

**Final dataset:** 12,567 labeled reviews (Negative: 6,589 · Positive: 5,480 · Neutral: 498)

## 6. Model Development

Two classifiers were trained and compared on a 70/20/10 stratified train/test/validation split:

| Model | Type | Key Configuration |
|---|---|---|
| **Naive Bayes** | Machine Learning | `MultinomialNB`, alpha = 0.1, TF-IDF (unigrams + bigrams, max 20,000 features) |
| **LSTM** | Deep Learning | Embedding (64-dim) → LSTM(128) → Dropout(0.3) → LSTM(64) → Dropout(0.3) → Dense(32, ReLU) → Dense(3, Softmax); Adam optimizer, 10 epochs, batch size 64 |

### Evaluation Results (test set, n = 2,516)

| Metric | Naive Bayes | LSTM |
|---|---|---|
| Accuracy | 0.88 | 0.84 |
| Weighted F1 | 0.86 | 0.83 |
| Model size | ~5 MB | Larger (TensorFlow overhead) |
| Inference speed | Microseconds | Slower |

**Naive Bayes was selected for production deployment** in the Spark streaming pipeline due to its higher accuracy, smaller footprint, and faster inference — all critical for real-time throughput. Both models struggled on the minority **Neutral** class (only 4.1% of records).

## 7. System Architecture

```
Google Play Reviews (CSV) → Kafka Producer → Kafka Topic (google-play-reviews)
        → Spark Structured Streaming (preprocess → TF-IDF → Naive Bayes)
        → Elasticsearch (index: google_play_sentiment)
        → Kibana Dashboard
```

- **Kafka:** each review is published individually as a JSON message to the `google-play-reviews` topic, simulating real-time arrival.
- **Spark Structured Streaming:** consumes micro-batches, applies the same cleaning logic used in training, vectorizes with TF-IDF, classifies with the trained Naive Bayes model, and forwards results to Elasticsearch.
- **Elasticsearch:** stores each classified review as a document (`app`, `review`, `sentiment`, `rating`, `review_date`, `stream_timestamp`) under the `google_play_sentiment` index.
- **Kibana:** visualizes the indexed data via a live-refreshing dashboard.

## 8. Dashboard

The Kibana dashboard includes:
- **Sentiment Distribution** (pie chart) — overall Positive/Neutral/Negative split
- **Sentiment Over Time** (line chart) — daily review volume per sentiment class, highlighting a negative-sentiment surge from late March/early April 2026 onward
- **Sentiment Breakdown by App** (data table) — per-app sentiment counts and average ratings
- **Word Clouds** (generated separately via Python `wordcloud`/`matplotlib`) — most frequent terms in positive vs. negative reviews

## 9. Batch vs. Streaming Comparison

| Metric | Batch Mode | Streaming Mode |
|---|---|---|
| Records processed | 200 | 201 |
| Total processing time | 0.012 s | 860.93 s |
| Throughput | 17,310.62 rec/s | 0.23 rec/s |
| Accuracy | 0.840 | 0.592* |

\* Streaming accuracy is affected by a row-order alignment limitation in the benchmark (see report §6.2) rather than an actual drop in model quality.

Batch mode maximizes raw throughput, while streaming mode is required for low-latency, real-time monitoring use cases.

### Optimization Opportunities
1. Bulk Elasticsearch indexing (replace per-row `es.index()` calls)
2. Exact-match evaluation via a `review_id` field (removes row-order assumption)
3. Explicit Elasticsearch index mapping applied at creation time
4. Configurable Kafka producer delay for flexible benchmarking

## 10. Tech Stack

| Component | Tool |
|---|---|
| Data collection | `google-play-scraper` (Python) |
| Text preprocessing | NLTK, spaCy |
| ML model | scikit-learn (Naive Bayes, TF-IDF) |
| DL model | TensorFlow / Keras (LSTM) |
| Streaming | Apache Kafka, Apache Spark Structured Streaming |
| Storage & search | Elasticsearch |
| Visualization | Kibana, Python (`wordcloud`, `matplotlib`) |
| Deployment | Docker Compose |


## 11. Future Work

- Address poor Neutral-class performance using SMOTE or class-weighted loss functions
- Support multilingual/code-mixed feedback (e.g., mBERT for Malay-English text)
- Adopt the Elasticsearch Bulk API to improve streaming throughput
- Migrate deployment to cloud-based Kubernetes with live scraping instead of simulated streams

