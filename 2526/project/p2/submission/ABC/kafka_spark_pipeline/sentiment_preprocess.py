"""
Reusable text preprocessing functions for the real-time sentiment pipeline.

This file keeps the streaming preprocessing consistent with the preprocessing
used during model training.
"""

import re
import string
import pandas as pd
import contractions
import nltk

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer


# Download NLTK resources if they are not available
try:
    stopwords.words("english")
except LookupError:
    nltk.download("stopwords")

try:
    WordNetLemmatizer().lemmatize("testing")
except LookupError:
    nltk.download("wordnet")
    nltk.download("omw-1.4")


english_stopwords = set(stopwords.words("english"))

# Custom stopwords for Malaysian Google Play app reviews
custom_stopwords = {
    # Malay common words
    "yang", "dan", "ini", "itu", "untuk", "dengan", "dari", "pada", "dalam",
    "saya", "aku", "kami", "kita", "dia", "mereka", "anda", "kamu",
    "ada", "jadi", "akan", "sudah", "dah", "telah", "masih", "belum",
    "boleh", "nak", "mahu", "perlu", "kena", "guna", "pakai",
    "sebab", "kerana", "kalau", "jika", "tapi", "tetapi", "namun",
    "pun", "lah", "kan", "je", "saja", "sahaja", "ni", "tu",

    # Malay short forms
    "yg", "dgn", "utk", "dlm", "pd", "dr", "sbb", "krn", "jgn",
    "sy", "aq", "korang", "diorang",

    # App review filler words
    "app", "apps", "application", "tng", "touch", "go", "ewallet",
    "please", "pls", "plz", "thank", "thanks", "ok", "okay"
}

stopword_list = english_stopwords.union(custom_stopwords)
lemmatizer = WordNetLemmatizer()


# Normalization dictionary for Malaysian-style app reviews
TEXT_NORMALIZATION = {
    # Malay negation and short forms
    r"\btak\b": "tidak",
    r"\btk\b": "tidak",
    r"\btx\b": "tidak",
    r"\bx\b": "tidak",
    r"\bxde\b": "tiada",
    r"\btakde\b": "tidak ada",
    r"\btkde\b": "tidak ada",
    r"\btade\b": "tiada",

    # Common Malay abbreviations
    r"\bdpt\b": "dapat",
    r"\bblh\b": "boleh",
    r"\bboleh2\b": "boleh",
    r"\bsgt\b": "sangat",
    r"\bbg\b": "bagi",
    r"\bbrg\b": "barang",
    r"\bmsuk\b": "masuk",

    # English app review shortcuts
    r"\bpls\b": "please",
    r"\bplz\b": "please",
    r"\bmsg\b": "message",
    r"\botp\b": "otp",
    r"\blogin\b": "log in",
    r"\bloggedin\b": "logged in",

    # Common noisy expressions
    r"\bhaha+\b": "",
    r"\bhehe+\b": "",
    r"\blol+\b": "",
    r"\bwah+\b": "",
    r"\bwei+\b": "",
    r"\bweh+\b": ""
}


def normalize_text_terms(text: str) -> str:
    """Normalize common Malay short forms and noisy expressions."""
    for pattern, replacement in TEXT_NORMALIZATION.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def clean_text(text) -> str:
    """
    Clean one review text for sentiment prediction.

    Steps:
    lowercase, contractions, Malay normalization, URL/email/HTML removal,
    emoji/non-ASCII removal, repeated character normalization, number removal,
    punctuation removal, tokenization, stopword removal, and lemmatization.
    """
    if pd.isna(text):
        return ""

    text = str(text).lower()
    text = contractions.fix(text)
    text = normalize_text_terms(text)

    # Remove noise
    text = re.sub(r"http\S+|www\S+|https\S+", " ", text)
    text = re.sub(r"\S+@\S+", " ", text)
    text = re.sub(r"<.*?>", " ", text)

    # Remove emojis and non-ASCII characters
    text = text.encode("ascii", "ignore").decode()

    # Normalize repeated characters, e.g. slowwww -> sloww
    text = re.sub(r"(.)\1{2,}", r"\1\1", text)

    # Remove numbers and punctuation
    text = re.sub(r"\d+", " ", text)
    text = text.translate(str.maketrans("", "", string.punctuation))

    # Remove extra spaces
    text = re.sub(r"\s+", " ", text).strip()

    # Tokenization, stopword removal, and lemmatization
    tokens = text.split()
    tokens = [word for word in tokens if word not in stopword_list and len(word) > 2]
    tokens = [lemmatizer.lemmatize(word) for word in tokens]

    return " ".join(tokens)


if __name__ == "__main__":
    sample = "TNG app ni very slowwww!!! I can't login, pls fix 😭"
    print("Original:", sample)
    print("Cleaned :", clean_text(sample))
