import requests
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS

ES_URL = "http://localhost:9200/app-reviews-sentiment/_search"

query = {
    "size": 5000,
    "_source": ["review_text", "predicted_label"],
    "query": {"match_all": {}}
}

response = requests.get(ES_URL, json=query)
response.raise_for_status()

hits = response.json()["hits"]["hits"]
docs = [hit["_source"] for hit in hits]

extra_stopwords = {
    "app", "mae", "maybank", "bank", "use", "using", "user",
    "please", "pls", "also", "can", "cannot", "cant", "will",
    "one", "time", "need", "make", "get", "go", "open"
}

stopwords = STOPWORDS.union(extra_stopwords)

for sentiment in ["positive", "negative"]:
    text = " ".join(
        d.get("review_text", "")
        for d in docs
        if d.get("predicted_label") == sentiment
    )

    wordcloud = WordCloud(
        width=1400,
        height=800,
        background_color="white",
        stopwords=stopwords,
        collocations=False
    ).generate(text)

    plt.figure(figsize=(14, 8))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(f"{sentiment.capitalize()} Reviews Word Cloud", fontsize=22)
    plt.savefig(f"{sentiment}_wordcloud.png", dpi=300, bbox_inches="tight")
    plt.close()

print("Done: positive_wordcloud.png and negative_wordcloud.png created.")