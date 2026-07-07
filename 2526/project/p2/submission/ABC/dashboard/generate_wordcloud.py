import os
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS

# ======================================================
# Create output folder
# ======================================================
os.makedirs("dashboard", exist_ok=True)

# ======================================================
# Load cleaned dataset
# ======================================================
df = pd.read_csv("data/cleaned_data.csv")

# ======================================================
# Custom stopwords
# ======================================================

custom_stopwords = set(STOPWORDS)

custom_stopwords.update({

    # Generic English
    "app","apps","application","using","use","used","user","users",
    "good","great","nice","best","easy","excellent","awesome",
    "really","very","also","still","just","can","could","would",
    "will","one","thing","things","make","made","much","many",
    "even","always","already","want","need","try","trying",
    "please","say","said","got","get","keep","work","working",
    "time","day","back","new","old","every","everything",

    # Touch 'n Go specific
    "tng","touch","go","ewallet","wallet","service",
    "account","money","payment","cashless","card",

    # Malay stopwords
    "tidak","tak","yang","dan","untuk","dengan","ini","itu",
    "saya","kami","anda","dia","mereka","boleh","dapat",
    "lagi","baru","semua","bagi","kenapa","apa","macam",
    "sangat","betul","lebih","pun","je","lah","kah",
    "buat","guna","pakai","baik","terbaik","mantap",
    "bagus","mudah","cepat",

    # Common filler words
    "phone","mobile","review","reviews","experience",

    # Improvement after analyzing the word cloud
    "customer","customers","can","pay","payment","open",
    "number","daily","online","life","bank","malaysia",
    "problem","feature","update","helpful", "issue",
    "way","give","lot","system","top", "code", "version",
    "show","know","take","help","first","year","month"
})

# ======================================================
# Function to generate word cloud
# ======================================================

def create_wordcloud(text, title, cmap, output_path):

    wc = WordCloud(
        width=1400,
        height=700,
        background_color="white",
        stopwords=custom_stopwords,
        max_words=200,
        colormap=cmap,
        collocations=False
    ).generate(text)

    plt.figure(figsize=(14,7))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title, fontsize=18)
    plt.tight_layout()

    plt.savefig(output_path, dpi=300)
    plt.close()

# ======================================================
# Positive reviews
# ======================================================

positive_text = " ".join(
    df[df["sentiment_label"]=="positive"]["cleaned_review"]
    .dropna()
    .astype(str)
)

create_wordcloud(
    positive_text,
    "Positive Review Word Cloud",
    "Greens",
    "dashboard/positive_wordcloud.png"
)

# ======================================================
# Negative reviews
# ======================================================

negative_text = " ".join(
    df[df["sentiment_label"]=="negative"]["cleaned_review"]
    .dropna()
    .astype(str)
)

create_wordcloud(
    negative_text,
    "Negative Review Word Cloud",
    "Reds",
    "dashboard/negative_wordcloud.png"
)

print("="*60)
print("Word clouds generated successfully!")
print("Saved:")
print("dashboard/positive_wordcloud.png")
print("dashboard/negative_wordcloud.png")
print("="*60)