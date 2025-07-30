import os
import calendar
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

LIMIT = 50
MAX_WORKERS = os.cpu_count() or 4

# 1. Generic pager for Oireachtas XML endpoints
def fetch_all_xml(
    endpoint: str,
    base_params: Dict,
    extract: Callable[[dict], Optional[Tuple]],
    limit: int = 200,
    desc: str = None
) -> List[Tuple]:
    seen = set()
    out = []
    skip = 0
    pbar = tqdm(desc=desc or endpoint, unit="xml")
    while True:
        params = {**base_params, "skip": skip, "limit": limit}
        resp = requests.get(f"https://api.oireachtas.ie/v1/{endpoint}", params=params)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            break
        for item in results:
            tup = extract(item)
            if not tup:
                continue
            url = tup[0]
            if url in seen:
                continue
            seen.add(url)
            out.append(tup)
            pbar.update(1)
        skip += limit
    pbar.close()
    return out

# 2. Extraction callbacks
def extract_debate(item: dict) -> Optional[Tuple[str, str]]:
    rec = item.get("debateRecord", {})
    fmt = rec.get("formats", {}).get("xml", {})
    url = fmt.get("uri") if isinstance(fmt, dict) else None
    date = rec.get("date") or item.get("contextDate")
    return (url, date) if url else None

def extract_committee(item: dict) -> Optional[Tuple[str, str, str]]:
    rec = item.get("debateRecord", {})
    fmt = rec.get("formats", {}).get("xml")
    url = fmt.get("uri") if isinstance(fmt, dict) else fmt
    if not url:
        return None
    parts = url.split("/")
    slug = parts[6] if len(parts) > 6 else ""
    date = parts[7] if len(parts) > 7 else ""
    return (url, date, slug)

def extract_question(item: dict) -> Optional[Tuple[str, str, str]]:
    q = item.get("question", {})
    fmt = q.get("debateSection", {}).get("formats", {}).get("xml")
    url = fmt.get("uri") if isinstance(fmt, dict) else fmt
    if not url:
        return None
    date = q.get("date")
    num = q.get("questionNumber")
    return (url, date, num)

# 3. Download worker
def download_xml_worker(meta: Tuple, out_dir: Path, name_mode: str):
    try:
        if name_mode == 'debate':
            url, date = meta
            filename = f"{date}.xml"
        elif name_mode == 'committee':
            url, date, slug = meta
            filename = f"{date}__{slug}.xml"
        elif name_mode == 'question':
            url, date, num = meta
            filename = f"{date}__pq_{num}.xml"
        else:
            return None

        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / filename
        with open(file_path, "wb") as f:
            f.write(resp.content)
        return (file_path, *meta[1:])
    except Exception as e:
        print(f"Failed {url}: {e}")
        return None

# 4. Parallel download
def parallel_download(
    meta_list: List[Tuple],
    out_dir: Path,
    desc: str,
    max_workers: int,
    name_mode: str
) -> List[Tuple]:
    files = []
    pbar = tqdm(total=len(meta_list), desc=desc, unit="file")
    with ThreadPoolExecutor(max_workers=max_workers) as exec:
        futures = [
            exec.submit(download_xml_worker, meta, out_dir, name_mode)
            for meta in meta_list
        ]
        for future in as_completed(futures):
            res = future.result()
            if res:
                files.append(res)
            pbar.update(1)
    pbar.close()
    return files

# 5. Combine all XML into one
def combine_to_single_xml(
    dail_files, seanad_files, committee_files, question_files, output_path: Path
):
    def strip_header(text: str) -> str:
        return text.lstrip('\ufeff') \
                   .replace('<?xml version="1.0" encoding="UTF-8"?>', '') \
                   .strip()

    with open(output_path, "w", encoding="utf-8") as out:
        out.write('<all_debates>\n')
        for file_path, date in dail_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="dail" date="{date}">\n    <data>\n')
            out.write('\n'.join('      '+line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')
        for file_path, date in seanad_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="seanad" date="{date}">\n    <data>\n')
            out.write('\n'.join('      '+line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')
        for file_path, date, slug in committee_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="committee" date="{date}" slug="{slug}">\n    <data>\n')
            out.write('\n'.join('      '+line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')
        for file_path, date, num in question_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="questions" question_type="written" date="{date}" number="{num}">\n    <data>\n')
            out.write('\n'.join('      '+line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')
        out.write('</all_debates>\n')
    print(f"Combined all XMLs into {output_path}")

# 6. Monthly slicer
def daterange_monthly(start_str: str, end_str: str):
    start = datetime.fromisoformat(start_str).date()
    end   = datetime.fromisoformat(end_str).date()
    curr  = start
    while curr <= end:
        last_day = calendar.monthrange(curr.year, curr.month)[1]
        chunk_end = min(end, datetime(curr.year, curr.month, last_day).date())
        yield curr.isoformat(), chunk_end.isoformat()
        curr = chunk_end + timedelta(days=1)

if __name__ == "__main__":
    DATE_START = "2025-07-01"
    DATE_END = "2025-07-31"

    all_dail_files = []
    for s, e in daterange_monthly(DATE_START, DATE_END):
        metas = fetch_all_xml(
            "debates",
            {"chamber": "dail", "chamber_type": "house", "date_start": s, "date_end": e},
            extract_debate,
            limit=LIMIT,
            desc=f"Dáil meta {s}→{e}"
        )
        files = parallel_download(
            metas,
            Path("data/dail_debates"),
            desc=f"Dáil download {s}→{e}",
            max_workers=MAX_WORKERS,
            name_mode="debate"
        )
        all_dail_files.extend(files)

    all_seanad_files = []
    for s, e in daterange_monthly(DATE_START, DATE_END):
        metas = fetch_all_xml(
            "debates",
            {"chamber": "seanad", "chamber_type": "house", "date_start": s, "date_end": e},
            extract_debate,
            limit=LIMIT,
            desc=f"Seanad meta {s}→{e}"
        )
        files = parallel_download(
            metas,
            Path("data/seanad_debates"),
            desc=f"Seanad download {s}→{e}",
            max_workers=MAX_WORKERS,
            name_mode="debate"
        )
        all_seanad_files.extend(files)

    all_committee_files = []
    for s, e in daterange_monthly(DATE_START, DATE_END):
        metas = fetch_all_xml(
            "debates",
            {"chamber_type": "committee", "date_start": s, "date_end": e},
            extract_committee,
            limit=LIMIT,
            desc=f"Committee meta {s}→{e}"
        )
        files = parallel_download(
            metas,
            Path("data/committee_debates"),
            desc=f"Committee download {s}→{e}",
            max_workers=MAX_WORKERS,
            name_mode="committee"
        )
        all_committee_files.extend(files)

    all_question_files = []
    for s, e in daterange_monthly(DATE_START, DATE_END):
        metas = fetch_all_xml(
            "questions",
            {"qtype": "written", "date_start": s, "date_end": e},
            extract_question,
            limit=LIMIT,
            desc=f"PQ meta {s}→{e}"
        )
        files = parallel_download(
            metas,
            Path("data/written_questions"),
            desc=f"PQ download {s}→{e}",
            max_workers=MAX_WORKERS,
            name_mode="question"
        )
        all_question_files.extend(files)

    combine_to_single_xml(
        all_dail_files,
        all_seanad_files,
        all_committee_files,
        all_question_files,
        Path("all_debates.xml")
    )
