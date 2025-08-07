import pandas as pd

INPUT_CSV = "debates_all_with_lang.csv"
OUTPUT_TXT = "inspect_1957_ga.txt"

# Read only relevant columns to save memory
df = pd.read_csv(INPUT_CSV, usecols=["year", "lang", "text"])

# Filter for year 1957 and Irish language
filtered = df[(df["year"] == 1957) & (df["lang"] == "ga")]

# Save the text column to a file, one entry per line
filtered["text"].dropna().to_csv(OUTPUT_TXT, index=False, header=False)

print(f"Saved {len(filtered)} rows