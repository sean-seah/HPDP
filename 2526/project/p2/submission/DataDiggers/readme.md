# 👀 Project 2: Real-Time Sentiment Analysis using Apache Spark and Kafka

✦ Data source: Google Play reviews — Touch 'n Go, Grab, foodpanda

| Name | Matric Number | Role
|------|------|------|
| NUR FIRZANA BINTI BADRUS HISHAM | A23CS0156 | Data Engineer |
| NURAISYAH BINTI MOHD ZIKRE | A23CS0160 | Model Engineer |
| LUBNA AL HAANI BINTI RADZUAN | A23CS0107 |  Pipeline Engineer |
| NURUL IKA SYAFINY BINTI AZHAR | A23CS0164 | Visualization & Performance Engineer |

Video Presentation: [youtube link]( )

## 📂Repository Structure

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
