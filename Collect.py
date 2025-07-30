import requests
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timedelta
import calendar


# Generic function to page through Oireachtas API and extract XML URLs

def fetch_all_xml(
    endpoint: str,
    base_params: Dict,
    extract: Callable[[dict], Optional[Tuple]],
    limit: int = 200,
    desc: str = None
) -> List[Tuple]:
    """
    Generic pager for Oireachtas XML endpoints, with progress tracking.

    - endpoint:      "debates" or "questions"
    - base_params:   fixed params (dates, chamber/qtype, etc.)
    - extract(item): returns a tuple like (xml_url, date, ...) or None
    - limit:         page size (max 200)
    - desc:          description for the progress bar

    Returns a list of tuples as returned by extract(), deduplicated.
    """
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


# Extraction callbacks for the three sources

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


# Worker for downloading a single XML

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


# Parallel download of XML files with a progress bar

def parallel_download(
    meta_list: List[Tuple],
    out_dir: Path,
    desc: str = "XMLs",
    max_workers: int = 8,
    name_mode: str = "debate"
) -> List[Tuple]:
    files = []
    pbar = tqdm(total=len(meta_list), desc=desc, unit="file")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_xml_worker, meta, out_dir, name_mode)
                   for meta in meta_list]
        for future in as_completed(futures):
            res = future.result()
            if res:
                files.append(res)
            pbar.update(1)
    pbar.close()
    return files


# Combine all downloaded XMLs into one file

def combine_to_single_xml(
    dail_files: List[Tuple],
    seanad_files: List[Tuple],
    committee_files: List[Tuple],
    question_files: List[Tuple],
    output_path: Path
):
    def strip_header(text: str) -> str:
        return text.lstrip('\ufeff').replace('<?xml version="1.0" encoding="UTF-8"?>', '').strip()

    with open(output_path, "w", encoding="utf-8") as out:
        out.write('<all_debates>\n')

        for file_path, date in dail_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="dail" date="{date}">\n    <data>\n')
            out.write('\n'.join('      ' + line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')

        for file_path, date in seanad_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="seanad" date="{date}">\n    <data>\n')
            out.write('\n'.join('      ' + line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')

        for file_path, date, slug in committee_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="committee" date="{date}" slug="{slug}">\n    <data>\n')
            out.write('\n'.join('      ' + line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')

        for file_path, date, num in question_files:
            xml = strip_header(file_path.read_text(encoding="utf-8"))
            out.write(f'  <debate type="questions" question_type="written" date="{date}" number="{num}">\n    <data>\n')
            out.write('\n'.join('      ' + line for line in xml.splitlines()))
            out.write('\n    </data>\n  </debate>\n')

        out.write('</all_debates>\n')
    print(f"Combined all XMLs into {output_path}")

def daterange_monthly(start_str: str, end_str: str):
    """
    Yield (chunk_start, chunk_end) stepping one calendar month at a time.
    """
    start = datetime.fromisoformat(start_str).date()
    end   = datetime.fromisoformat(end_str).date()
    curr  = start

    while curr <= end:
        # compute next month
        year  = curr.year + (curr.month // 12)
        month = curr.month % 12 + 1
        # last day of current month
        last_day = calendar.monthrange(curr.year, curr.month)[1]
        chunk_end = min(end, datetime(curr.year, curr.month, last_day).date())
        yield curr.isoformat(), chunk_end.isoformat()
        curr = chunk_end + timedelta(days=1)

def collect_by_month(endpoint, base_params, extractor, desc):
    all_meta = []
    for s, e in daterange_monthly(base_params["date_start"], base_params["date_end"]):
        metas = fetch_all_xml(
            endpoint=endpoint,
            base_params={**base_params, "date_start": s, "date_end": e},
            extract=extractor,
            limit=LIMIT,
            desc=f"{desc} {s}→{e}"
        )
        all_meta.extend(metas)
    return all_meta

if __name__ == "__main__":
    LIMIT     = 50
    DATE_END  = "2025-07-31"

    
    # Written Questions (PQ) (monthly)
    question_metas = collect_by_month(
        "questions",
        {"qtype": "written",
         "date_start": "2012-01-01", "date_end": DATE_END},
        extract_question,
        desc="PQ metadata"
    )

    # Committees (monthly)
    committee_metas = collect_by_month(
        "debates",
        {"chamber_type": "committee",
         "date_start": "1924-01-01", "date_end": DATE_END},
        extract_committee,
        desc="Committee metadata"
    )

    # Dáil debates (you could leave these yearly if you want, 
    # but monthly also works)
    dail_metas = collect_by_month(
        "debates",
        {"chamber": "dail", "chamber_type": "house",
         "date_start": "1919-01-01", "date_end": DATE_END},
        extract_debate,
        desc="Dáil metadata"
    )

    # Seanad debates
    seanad_metas = collect_by_month(
        "debates",
        {"chamber": "seanad", "chamber_type": "house",
         "date_start": "1929-01-01", "date_end": DATE_END},
        extract_debate,
        desc="Seanad metadata"
    )


    # Now download and combine exactly as before...
    dail_files      = parallel_download(dail_metas,      Path("data/dail_debates"),      desc="Downloading Dáil XMLs",      name_mode="debate")
    seanad_files    = parallel_download(seanad_metas,    Path("data/seanad_debates"),    desc="Downloading Seanad XMLs",    name_mode="debate")
    committee_files = parallel_download(committee_metas, Path("data/committee_debates"), desc="Downloading Committee XMLs", name_mode="committee")
    question_files  = parallel_download(question_metas,  Path("data/written_questions"),  desc="Downloading PQ XMLs",        name_mode="question")

    combine_to_single_xml(
        dail_files, seanad_files, committee_files, question_files,
        Path("all_debates.xml")
    )

