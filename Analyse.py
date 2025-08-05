import pandas as pd
import fasttext
from tqdm import tqdm
import os

# Load data
print("Loading data...")
df = pd.read_csv("debates_all.csv", nrows=1000)
print(" COLS AND DATES ")
print(df.columns)
print(df['date'].head(100))

print("First few rows of loaded data:")
print(df.head())
print("Columns:", df.columns)

# just get first 1000 rows
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df['year'] = df['date'].dt.year
print("Unique years after date parsing:", df['year'].unique())

# Load fastText language identification model
FASTTEXT_MODEL_PATH = "lid.176.bin"
if not os.path.exists(FASTTEXT_MODEL_PATH):
    import urllib.request
    print("Downloading fastText language ID model...")
    url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
    urllib.request.urlretrieve(url, FASTTEXT_MODEL_PATH)

print("Loading fastText model...")
ft_model = fasttext.load_model(FASTTEXT_MODEL_PATH)

def detect_language(text):
    if not isinstance(text, str) or not text.strip():
        return "unk"
    prediction = ft_model.predict(text.replace('\n', ' '), k=1)
    lang = prediction[0][0].replace("__label__", "")
    return lang

# Detect language for each text
print("Detecting language for each text...")
tqdm.pandas(desc="Detecting language")
df['lang'] = df['text'].progress_apply(detect_language)
print("Unique detected languages:", df['lang'].unique())
print("Language counts:\n", df['lang'].value_counts())

# Calculate proportions by year and source_type
df = df[df['year'].notnull()]  # Drop rows with missing year
print("Rows after dropping missing years:", len(df))
print("Unique source_types:", df['source_type'].unique())

# Calculate proportions by year and source_type
counts = df.groupby(['year', 'source_type', 'lang']).size().unstack(fill_value=0)
print("Counts table head:\n", counts.head())
totals = counts.sum(axis=1)
print("Totals head:\n", totals.head())

# Always include all source_types as columns
source_types = df['source_type'].unique()
print("Source types for reindexing:", source_types)

# Proportion for Irish
prop_ga = (counts.get('ga', 0) / totals).unstack(fill_value=0).reindex(columns=source_types, fill_value=0)
print("prop_ga head:\n", prop_ga.head())
# Proportion for English
prop_en = (counts.get('en', 0) / totals).unstack(fill_value=0).reindex(columns=source_types, fill_value=0)
print("prop_en head:\n", prop_en.head())
# Proportion for Other
prop_other = (totals - counts.get('ga', 0) - counts.get('en', 0)) / totals
prop_other = prop_other.unstack(fill_value=0).reindex(columns=source_types, fill_value=0)
print("prop_other head:\n", prop_other.head())

# Save to CSVs
print("Saving prop_ga.csv, prop_en.csv, prop_other.csv ...")
prop_ga.to_csv("prop_ga.csv")
prop_en.to_csv("prop_en.csv")
prop_other.to_csv("prop_other.csv")

# Save examples for each language group
def save_examples(lang_code, filename, n=20):
    examples = df[df['lang'] == lang_code]['text'].dropna().unique()[:n]
    print(f"Saving {len(examples)} examples for {lang_code} to {filename}")
    with open(filename, "w", encoding="utf-8") as f:
        for example in examples:
            f.write(example.strip() + "\n")

save_examples('ga', "irish_text_examples.txt")
save_examples('en', "english_text_examples.txt")
# For 'other', get examples not ga or en
other_examples = df[~df['lang'].isin(['ga', 'en'])]['text'].dropna().unique()[:20]
print(f"Saving {len(other_examples)} examples for other to other_text_examples.txt")
with open("other_text_examples.txt", "w", encoding="utf-8") as f:
    for example in other_examples:
        f.write(example.strip() + "\n")