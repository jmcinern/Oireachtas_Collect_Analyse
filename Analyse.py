import pandas as pd
import fasttext
from tqdm import tqdm
import os

# Load data
df = pd.read_csv("debates_all.csv")
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df['month'] = df['date'].dt.to_period('M')
df['year'] = df['date'].dt.year

# Load fastText language identification model
FASTTEXT_MODEL_PATH = "lid.176.bin"
if not os.path.exists(FASTTEXT_MODEL_PATH):
    import urllib.request
    print("Downloading fastText language ID model...")
    url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
    urllib.request.urlretrieve(url, FASTTEXT_MODEL_PATH)

ft_model = fasttext.load_model(FASTTEXT_MODEL_PATH)

def detect_language(text):
    if not isinstance(text, str) or not text.strip():
        return "unk"
    prediction = ft_model.predict(text.replace('\n', ' '), k=1)
    lang = prediction[0][0].replace("__label__", "")
    return lang

# Detect language for each text
tqdm.pandas(desc="Detecting language")
df['lang'] = df['text'].progress_apply(detect_language)

# Proportion of Irish to English by month and source_type
lang_counts = df.groupby(['month', 'source_type', 'lang']).size().unstack(fill_value=0)
lang_counts['total'] = lang_counts.sum(axis=1)
lang_counts['prop_irish'] = lang_counts.get('ga', 0) / lang_counts['total']
lang_counts['prop_english'] = lang_counts.get('en', 0) / lang_counts['total']

print(lang_counts[['prop_irish', 'prop_english']])

# Save Irish text examples
irish_examples = df[df['lang'] == 'ga']['text'].dropna().unique()
with open("irish_text_examples.txt", "w", encoding="utf-8") as f:
    for example in irish_examples:
        f.write(example.strip() + "\n")