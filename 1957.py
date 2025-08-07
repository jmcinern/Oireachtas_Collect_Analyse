import pandas as pd
from tqdm import tqdm

INPUT_CSV = "debates_all_with_lang.csv"
OUTPUT_TXT = "inspect_1957_ga.txt"
CHUNKSIZE = 100_000

header_written = False
with open(OUTPUT_TXT, "w", encoding="utf-8") as out:
    # Wrap the chunk iterator with tqdm for a progress bar
    for chunk in tqdm(pd.read_csv(INPUT_CSV, usecols=["date", "lang", "text"], chunksize=CHUNKSIZE), desc="Processing chunks"):
        chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
        filtered = chunk[(chunk['date'].dt.year == 1957) & (chunk['lang'] == "ga")]
        if not filtered.empty:
            filtered[['date', 'text']].dropna().to_csv(
                out, index=False, header=not header_written, sep='\t', mode='a'
            )
            header_written = True

print(f"Done! Output written to {OUTPUT_TXT}")