import pandas as pd
import fasttext
from tqdm import tqdm
import os
import swifter

# Load fastText language identification model
FASTTEXT_MODEL_PATH = "lid.176.bin"
if not os.path.exists(FASTTEXT_MODEL_PATH):
    import urllib.request
    print("Downloading fastText language ID model...")
    url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
    urllib.request.urlretrieve(url, FASTTEXT_MODEL_PATH)

print("Loading fastText model...")
ft_model = fasttext.load_model(FASTTEXT_MODEL_PATH)

# Language detection helper
def detect_language(text):
    if not isinstance(text, str) or not text.strip():
        return "unk"
    label, _ = ft_model.predict(text.replace("\n", " "), k=1)
    return label[0].replace("__label__", "")

# Parameters
CSV_PATH = "debates_all_1919-01-01_to_2025-07-31.csv"
OUTPUT_CSV_PATH = "debates_all_with_lang.csv"
CHUNKSIZE = 100_000
MAX_ROWS = None  # set to an int for testing, or None to read all rows

# If output already exists, remove it
if os.path.exists(OUTPUT_CSV_PATH):
    os.remove(OUTPUT_CSV_PATH)

# Aggregation containers
all_counts = []
all_examples = {'ga': [], 'en': [], 'other': []}

print("Processing CSV in chunks...")
reader = pd.read_csv(CSV_PATH, chunksize=CHUNKSIZE, nrows=MAX_ROWS)
for i, chunk in enumerate(reader, start=1):
    # 1) Detect language on the full chunk
    tqdm.pandas(desc=f"Detecting language (chunk {i})")
    chunk['lang'] = chunk['text'].swifter.apply(detect_language)

    # 2) Stream‐write this augmented chunk
    write_mode = 'w' if i == 1 else 'a'
    write_header = (i == 1)
    chunk.to_csv(OUTPUT_CSV_PATH, index=False, mode=write_mode, header=write_header)

    # 3) Existing processing for counts & examples
    chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
    chunk = chunk[chunk['date'].notnull()]
    chunk['year'] = chunk['date'].dt.year

    # Collect example texts
    for lc in ['ga', 'en']:
        samples = chunk.loc[chunk['lang'] == lc, 'text'].dropna().unique()
        all_examples[lc].extend(samples[:20])
    others = chunk.loc[~chunk['lang'].isin(['ga', 'en']), 'text'].dropna().unique()
    all_examples['other'].extend(others[:20])

    # Count by year, source_type, lang
    grp = chunk.groupby(['year', 'source_type', 'lang']).size()
    counts = grp.unstack(fill_value=0)
    all_counts.append(counts)

# Combine all counts
print("Concatenating counts...")
counts_full = pd.concat(all_counts).groupby(level=['year', 'source_type']).sum()
print("counts_full columns:", counts_full.columns)

# Compute proportions per row
props = counts_full.div(counts_full.sum(axis=1), axis=0)

# Save proportions for 'ga' and 'en'
for lang in ['ga', 'en']:
    if lang in props.columns:
        df_lang = props[lang].unstack(fill_value=0)
        df_lang.index.name = 'year'
        df_lang.reset_index(inplace=True)
        df_lang.to_csv(f"prop_{lang}.csv", index=False)
    else:
        print(f"Warning: No data for language '{lang}'")

if 'ga' in props.columns and 'en' in props.columns:
    other_props = 1 - props['ga'] - props['en']
    other_df = other_props.unstack(fill_value=0)
    other_df.index.name = 'year'
    other_df.reset_index(inplace=True)
    other_df.to_csv("prop_other.csv", index=False)
else:
    print("Warning: 'ga' or 'en' columns missing, cannot compute 'other' proportions.")

# Save examples
def save_examples(lang_code, fname, n=20):
    unique_ex = pd.Series(all_examples[lang_code]).drop_duplicates().head(n)
    print(f"Saving {len(unique_ex)} examples for {lang_code} to {fname}")
    with open(fname, 'w', encoding='utf-8') as f:
        for line in unique_ex:
            f.write(line.strip() + "\n")

save_examples('ga', 'irish_text_examples.txt')
save_examples('en', 'english_text_examples.txt')
save_examples('other', 'other_text_examples.txt')