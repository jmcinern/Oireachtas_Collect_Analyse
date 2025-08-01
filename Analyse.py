import pandas as pd
import fasttext
from tqdm import tqdm
import os

# Load data
df = pd.read_csv("debates_all.csv")
df['date'] = pd.to_datetime(df['date'], errors='coerce')
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

# Calculate proportions by year and source_type
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df['year'] = df['date'].dt.year
df = df[df['year'].notnull()]  # Drop rows with missing year

# Now proceed as before
counts = df.groupby(['year', 'source_type', 'lang']).size().unstack(fill_value=0)
totals = counts.sum(axis=1)
prop_ga = (counts.get('ga', 0) / totals).unstack(fill_value=0)
prop_en = (counts.get('en', 0) / totals).unstack(fill_value=0)
prop_other = (totals - counts.get('ga', 0) - counts.get('en', 0)) / totals
prop_other = prop_other.unstack(fill_value=0)

# Save to CSVs
prop_ga.to_csv("prop_ga.csv")
prop_en.to_csv("prop_en.csv")
prop_other.to_csv("prop_other.csv")

# Save examples for each language group
def save_examples(lang_code, filename, n=20):
    examples = df[df['lang'] == lang_code]['text'].dropna().unique()[:n]
    with open(filename, "w", encoding="utf-8") as f:
        for example in examples:
            f.write(example.strip() + "\n")

save_examples('ga', "irish_text_examples.txt")
save_examples('en', "english_text_examples.txt")
# For 'other', get examples not ga or en
other_examples = df[~df['lang'].isin(['ga', 'en'])]['text'].dropna().unique()[:20]
with open("other_text_examples.txt", "w", encoding="utf-8") as f:
    for example in other_examples:
        f.write(example.strip() + "\n")