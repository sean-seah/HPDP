# Task D — Visualization & Performance Engineer: How to Use These Files

## Where each file goes in the repo

```
DataDiggers/
├── kibana_visualizations.json          <- place at repo root (matches brief's structure)
├── kafka_spark_pipeline/
│   └── dashboard/
│       ├── wordcloud_generator.py
│       ├── performance_test_batch.py
│       ├── streaming_monitor.py
│       ├── compare_results.py
│       └── results/                    <- created automatically when scripts run
│           ├── batch_results.json
│           ├── streaming_results.json
│           ├── comparison_chart.png
│           ├── comparison_table.md
│           ├── wordcloud_positive.png
│           └── wordcloud_negative.png
└── requirements.txt                    <- merge requirements_dashboard.txt into this
```

## Run order

### 1. Import the Kibana dashboard
Kibana → Stack Management → Saved Objects → Import → select `kibana_visualizations.json`.
This creates the index pattern, the pie chart, the line chart, the app breakdown table,
and a dashboard combining all three. Make sure at least one document already exists in
`google_play_sentiment` (run producer.py + spark_stream.py once) before importing, so the
index pattern picks up field mappings correctly.

### 2. Word clouds
```bash
cd kafka_spark_pipeline/dashboard
python wordcloud_generator.py --source csv     # offline, reproducible
# or, once the live pipeline has run at least once:
python wordcloud_generator.py --source es
```

### 3. Batch benchmark (run anytime, no Kafka needed)
```bash
python performance_test_batch.py --input ../../data/raw_data/labeled_reviews.csv --n 500
```
Pick an `--n` you can realistically also push through the live pipeline (500 records
at 1.5s/message = ~12.5 minutes for the streaming run — plan your test session accordingly,
or temporarily shrink the sleep in producer.py just for this benchmark, per the note at
the top of streaming_monitor.py).

### 4. Streaming benchmark (needs Kafka + Spark running)
```bash
# Terminal 1
docker-compose up

# Terminal 2
python spark_stream.py

# Terminal 3 — start this BEFORE the producer
python streaming_monitor.py --n 500 --kafka-container kafka

# Terminal 4 — start right after terminal 3 is waiting
python producer.py
```

### 5. Generate the comparison chart + table
```bash
python compare_results.py
```
Paste `comparison_table.md` and `comparison_chart.png` into the report's
"Optimization and Comparison" section, with a short interpretation of *why*
the numbers differ (e.g. streaming's per-record latency is dominated by
Kafka/Spark micro-batch overhead and any artificial producer delay, while
batch amortizes model-loading cost across the whole volume).

## Notes / things worth double-checking with your teammates
- `streaming_monitor.py` assumes Elasticsearch and the raw CSV rows land in the
  same order it can join by index. For a bulletproof join, ask C to add an
  incrementing `review_id` to the Kafka message in `producer.py` and store it
  in `spark_stream.py`'s `doc` dict — five-minute change, makes the accuracy
  comparison exact instead of order-dependent.
- The word cloud script has a small custom Malay stopword list — glance at the
  generated PNGs and add any noisy leftover words you spot to `MALAY_STOPWORDS`.
- `docker-compose.yml` only shows the Kafka service in what I received — if
  Elasticsearch/Kibana also run in Docker, add their container names so you
  can extend `streaming_monitor.py`'s resource sampling to them too.
