import pandas as pd

INPUT_CSV = "debates_all_with_lang.csv"
OUTPUT_TXT = "inspect_1957_ga.txt"

# Read only relevant columns to save memory
df = pd.read_csv(INPUT_CSV, usecols=["date", "lang", "text"])

# Parse date and filter for year 1957 and Irish language
df['date'] = pd.to_datetime(df['date'], errors='coerce')
filtered = df[(df['date'].dt.year == 1957) & (df['lang'] == "ga")]

# Save date and text columns to a file, tab-separated, one entry per line
filtered[['date', 'text']].dropna().to_csv(OUTPUT_TXT, index=False, header=True, sep='\t')

print(f"Saved {len(filtered)} rows to {OUTPUT_TXT}")