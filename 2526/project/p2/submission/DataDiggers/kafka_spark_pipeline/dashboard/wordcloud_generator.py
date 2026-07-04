"""
wordcloud_generator.py
-----------------------
Task D — Visualization & Performance Engineer

Generates word clouds for the most frequent terms in POSITIVE and NEGATIVE
reviews. Kibana's native Tag Cloud visualization needs fielddata enabled on
the analyzed `review` text field to do word-level terms aggregations, which
is an extra index-settings change that isn't worth the risk this late in the
project. Generating the clouds in Python instead is simpler, fully
reproducible, and works whether or not the live stream is running --
you can point it at the CSV (offline) or at Elasticsearch (live data).

Usage:
    python wordcloud_generator.py --source csv
    python wordcloud_generator.py --source es
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CSV_PATH = Path("data/raw_data/labeled_reviews.csv")   # app,text,rating,date,sentiment
ES_HOST = "http://localhost:9200"
ES_INDEX = "google_play_sentiment"
OUTPUT_DIR = Path("kafka_spark_pipeline/dashboard")

# Malay stopwords aren't covered by the default (English) WordCloud STOPWORDS,
# so we extend the set. Add more terms here as you inspect the clouds.
MALAY_STOPWORDS = {
    "yang", "dan", "ini", "itu", "untuk", "dengan", "ada", "tidak", "tak",
    "saya", "kami", "kita", "dia", "mereka", "juga", "pada", "dari", "ke",
    "di", "atau", "jadi", "kalau", "kena", "sangat", "lah", "kan", "je",
    "nak", "boleh", "sudah", "dah", "akan", "masih", "hanya", "aplikasi",
    "app", "apps", "guna", "pakai",
}
ALL_STOPWORDS = STOPWORDS.union(MALAY_STOPWORDS)


def clean_for_wordcloud(text: str) -> str:
    """Light cleaning tuned for word-cloud readability (keeps real words only)."""
    text = str(text).lower()
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"[^a-z\s]", " ", text)   # drop punctuation, emoji, numbers
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_from_csv() -> pd.DataFrame:
    if not CSV_PATH.exists():
        sys.exit(f"Could not find {CSV_PATH}. Run from the project root, "
                  f"or pass --csv-path to point at the file.")
    df = pd.read_csv(CSV_PATH)
    return df[["text", "sentiment"]].rename(columns={"text": "review"})


def load_from_es() -> pd.DataFrame:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import scan

    es = Elasticsearch(ES_HOST)
    if not es.indices.exists(index=ES_INDEX):
        sys.exit(f"Index '{ES_INDEX}' not found on {ES_HOST}. "
                  f"Make sure spark_stream.py has written at least one batch.")

    rows = []
    for hit in scan(es, index=ES_INDEX, query={"query": {"match_all": {}}}):
        src = hit["_source"]
        rows.append({"review": src.get("review", ""), "sentiment": src.get("sentiment", "")})
    if not rows:
        sys.exit(f"Index '{ES_INDEX}' is empty. Run producer.py + spark_stream.py first.")
    return pd.DataFrame(rows)


def make_cloud(text_blob: str, title: str, out_path: Path):
    wc = WordCloud(
        width=1000, height=600,
        background_color="white",
        stopwords=ALL_STOPWORDS,
        collocations=False,
        max_words=150,
    ).generate(text_blob)

    plt.figure(figsize=(10, 6))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title, fontsize=16)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["csv", "es"], default="csv",
                         help="Pull reviews from the labeled CSV (offline/reproducible) "
                              "or live from Elasticsearch (real streamed data).")
    args = parser.parse_args()

    df = load_from_csv() if args.source == "csv" else load_from_es()
    df["clean"] = df["review"].apply(clean_for_wordcloud)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for sentiment in ["positive", "negative"]:
        subset = df[df["sentiment"] == sentiment]
        if subset.empty:
            print(f"No '{sentiment}' reviews found, skipping.")
            continue
        blob = " ".join(subset["clean"].tolist())
        make_cloud(
            blob,
            f"Most Frequent Terms — {sentiment.capitalize()} Reviews (n={len(subset)})",
            OUTPUT_DIR / f"wordcloud_{sentiment}.png",
        )


if __name__ == "__main__":
    main()
