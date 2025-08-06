import pandas as pd
import fasttext
from tqdm import tqdm
import os

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

# Parameters
csv_path = "debates_all_1919-01-01_to_2025-07-31.csv"
chunksize = 100000  # adjust as needed

# Aggregation containers
all_counts = []
all_examples = {'ga': [], 'en': [], 'other': []}

print("Processing CSV in chunks...")
reader = pd.read_csv(csv_path, chunksize=chunksize)

for i, chunk in enumerate(reader):
    print(f"\n--- Processing chunk {i+1} ---")
    chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
    chunk['year'] = chunk['date'].dt.year
    chunk = chunk[chunk['year'].notnull()]
    print("Rows after dropping missing years:", len(chunk))

    tqdm.pandas(desc=f"Detecting language (chunk {i+1})")
    chunk['lang'] = chunk['text'].progress_apply(detect_language)
    print("Unique detected languages:", chunk['lang'].unique())
    print("Language counts:\n", chunk['lang'].value_counts())

    # Save examples for each language group (up to 20 per chunk)
    for lang_code in ['ga', 'en']:
        examples = chunk[chunk['lang'] == lang_code]['text'].dropna().unique()
        all_examples[lang_code].extend(examples[:20])
    other_examples = chunk[~chunk['lang'].isin(['ga', 'en'])]['text'].dropna().unique()
    all_examples['other'].extend(other_examples[:20])

    # Group and aggregate
    counts = chunk.groupby(['year', 'source_type', 'lang']).size().unstack(fill_value=0)
    all_counts.append(counts)

# Concatenate all chunk results
print("\nConcatenating chunk results...")
if all_counts:
    counts_full = pd.concat(all_counts)
    counts_full = counts_full.groupby(['year', 'source_type']).sum()
    counts_full = counts_full.unstack(fill_value=0)
else:
    counts_full = pd.DataFrame()

totals = counts_full.sum(axis=1)
source_types = counts_full.index.get_level_values('source_type').unique()

# Proportion for Irish
prop_ga = (counts_full.get('ga', 0) / totals).unstack(fill_value=0).reindex(columns=source_types, fill_value=0)
print("prop_ga head:\n", prop_ga.head())
# Proportion for English
prop_en = (counts_full.get('en', 0) / totals).unstack(fill_value=0).reindex(columns=source_types, fill_value=0)
print("prop_en head:\n", prop_en.head())
# Proportion for Other
prop_other = (totals - counts_full.get('ga', 0) - counts_full.get('en', 0)) / totals
prop_other = prop_other.unstack(fill_value=0).reindex(columns=source_types, fill_value=0)
print("prop_other head:\n", prop_other.head())

# Save to CSVs
print("Saving prop_ga.csv, prop_en.csv, prop_other.csv ...")
prop_ga.to_csv("prop_ga.csv")
prop_en.to_csv("prop_en.csv")
prop_other.to_csv("prop_other.csv")

# Save examples for each language group (deduplicated, up to 20 total)
def save_examples(lang_code, filename, n=20):
    examples = pd.Series(all_examples[lang_code]).drop_duplicates().head(n)
    print(f"Saving {len(examples)} examples for {lang_code} to {filename}")
    with open(filename, "w", encoding="utf-8") as f:
        for example in examples:
            f.write(str(example).strip() + "\n")

save_examples('ga', "irish_text_examples.txt")
save_examples('en', "english_text_examples.txt")
save_examples('other',