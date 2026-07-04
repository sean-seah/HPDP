import pandas as pd
import joblib

# Load data and model
data_path = "../data/cleaned_data.csv"
model_path = "../models/best_sentiment_model.pkl"

df = pd.read_csv(data_path)
model = joblib.load(model_path)

# Check dataset columns
print("Dataset columns:")
print(df.columns.tolist())

# Use cleaned_text column
sample_texts = df["cleaned_text"].dropna().head(10)

# Predict sentiment
predictions = model.predict(sample_texts)

label_map = {
    0: "Negative",
    1: "Neutral",
    2: "Positive"
}

print("\nSample Predictions:")
for text, pred in zip(sample_texts, predictions):
    sentiment = label_map.get(pred, "Unknown")
    print("--------------------------------")
    print("Text:", text[:150])
    print("Prediction:", pred, "-", sentiment)