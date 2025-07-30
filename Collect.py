'''
Plan
Download Dáil and Seanad XMLs:

For each date, if an XML exists, save the filename and date in memory.

Download all committee XMLs:

For each API result (can be multiple per day), if an XML exists, record its slug and date and download it.

Combine everything:

For each downloaded file (from all three sources), read its XML, strip the XML header, and wrap in a <debate> element with proper attributes (type, date, slug if committee).

Write all <debate>...</debate> elements inside <all_debates> in one output XML file.

Result
One XML file (e.g. all_debates.xml) containing:

<debate type="dail" date="YYYY-MM-DD"><data>...</data></debate>

<debate type="seanad" date="YYYY-MM-DD"><data>...</data></debate>

<debate type="committee" date="YYYY-MM-DD" slug="..."><data>...</data></debate>

No duplicated files, no XML header pollution, easily filtered by type/date/slug.
'''
import requests
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def download_debate_xmls(chamber, chamber_type, date_start, date_end, out_dir, N):
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    skip = 0
    limit = 50  # batch size
    seen_xmls = set()
    files = []
    with tqdm(total=N, desc=f"{chamber.capitalize()} API XMLs", unit="xml") as pbar:
        while downloaded < N:
            params = {
                "chamber": chamber,
                "chamber_type": chamber_type,
                "date_start": date_start,
                "date_end": date_end,
                "skip": skip,
                "limit": limit
            }
            resp = requests.get("https://api.oireachtas.ie/v1/debates", params=params)
            if resp.status_code != 200:
                print(f"{chamber} API error on skip {skip}: {resp.status_code}")
                break
            results = resp.json().get("results", [])
            if not results:
                break
            for item in results:
                debate_record = item.get("debateRecord", {})
                formats = debate_record.get("formats", {})
                xml_info = formats.get("xml", {})
                xml_url = xml_info.get("uri") if isinstance(xml_info, dict) else None
                date_str = debate_record.get("date") or item.get("contextDate")
                if xml_url and xml_url not in seen_xmls:
                    file_path = out_dir / f"{date_str}.xml"
                    resp2 = requests.get(xml_url)
                    if resp2.status_code == 200:
                        with open(file_path, "wb") as f:
                            f.write(resp2.content)
                        files.append((file_path, date_str))
                        seen_xmls.add(xml_url)
                        downloaded += 1
                        pbar.update(1)
                        if downloaded >= N:
                            break
            skip += limit
    return files

def download_committee_xmls(date_start, date_end, out_dir, N):
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    skip = 0
    limit = 50  # or whatever batch size you want
    seen_xmls = set()
    files = []
    with tqdm(total=N, desc="Committee XMLs", unit="xml") as pbar:
        while downloaded < N:
            params = {
                "chamber_type": "committee",
                "date_start": date_start,
                "date_end": date_end,
                "skip": skip,
                "limit": limit
            }
            resp = requests.get("https://api.oireachtas.ie/v1/debates", params=params)
            if resp.status_code != 200:
                print(f"API error on skip {skip}: {resp.status_code}")
                break
            results = resp.json().get("results", [])
            if not results:
                break
            for item in results:
                rec = item.get("debateRecord", {})
                formats = rec.get("formats", {})
                xml_info = formats.get("xml")
                xml_url = xml_info.get("uri") if isinstance(xml_info, dict) else xml_info
                if xml_url and xml_url not in seen_xmls:
                    slug = xml_url.split('/')[6]
                    date_str = xml_url.split('/')[7]
                    file_path = out_dir / f"{date_str}__{slug}.xml"
                    resp2 = requests.get(xml_url)
                    if resp2.status_code == 200:
                        with open(file_path, "wb") as f:
                            f.write(resp2.content)
                        files.append((file_path, date_str, slug))
                        seen_xmls.add(xml_url)
                        downloaded += 1
                        pbar.update(1)
                        if downloaded >= N:
                            break
            skip += limit
    return files

def download_written_questions_xmls(date_start, date_end, q_type, out_dir, N):
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    skip = 0
    limit = 50  # or whatever batch size you want
    seen_xmls = set()
    files = []
    with tqdm(total=N, desc="Written PQ XMLs", unit="xml") as pbar:
        while downloaded < N:
            params = {
                "date_start": date_start,
                "date_end": date_end,
                "qtype": q_type,
                "skip": skip,
                "limit": limit
            }
            resp = requests.get("https://api.oireachtas.ie/v1/questions", params=params)
            if resp.status_code != 200:
                print(f"PQ API error on skip {skip}: {resp.status_code}")
                break
            results = resp.json().get("results", [])
            if not results:
                break
            for item in results: # each element of resutlt is a question
                question = item.get("question", {})
                debateSection = question.get("debateSection", {})
                formats = debateSection.get("formats", {})
                xml_info = formats.get("xml")
                xml_url = xml_info.get("uri") if isinstance(xml_info, dict) else xml_info
                #print(xml_url)
                if xml_url and xml_url not in seen_xmls:
                    date_str = question.get("date")
                    qnum = question.get("questionNumber", "NA")
                    file_path = out_dir / f"{date_str}__pq_{qnum}.xml"
                    resp2 = requests.get(xml_url)
                    if resp2.status_code == 200:
                        with open(file_path, "wb") as f:
                            f.write(resp2.content)
                        files.append((file_path, date_str, qnum))
                        seen_xmls.add(xml_url)
                        downloaded += 1
                        pbar.update(1)
                        if downloaded >= N:
                            break
            skip += limit
    return files

def combine_to_single_xml(dail_files, seanad_files, committee_files, question_files, output_path):
    def strip_xml_header(text):
        return text.lstrip('\ufeff').replace('<?xml version="1.0" encoding="UTF-8"?>', '').strip()

    with open(output_path, "w", encoding="utf-8") as out:
        out.write('<all_debates>\n')
        # Dáil
        for file_path, date_str in dail_files:
            xml = Path(file_path).read_text(encoding="utf-8")
            xml = strip_xml_header(xml)
            out.write(f'  <debate type="dail" date="{date_str}">\n    <data>\n')
            out.write(xml.replace('\n', '\n      '))
            out.write('\n    </data>\n  </debate>\n')
        # Seanad
        for file_path, date_str in seanad_files:
            xml = Path(file_path).read_text(encoding="utf-8")
            xml = strip_xml_header(xml)
            out.write(f'  <debate type="seanad" date="{date_str}">\n    <data>\n')
            out.write(xml.replace('\n', '\n      '))
            out.write('\n    </data>\n  </debate>\n')
        # Committees
        for file_path, date_str, slug in committee_files:
            xml = Path(file_path).read_text(encoding="utf-8")
            xml = strip_xml_header(xml)
            out.write(f'  <debate type="committee" date="{date_str}" slug="{slug}">\n    <data>\n')
            out.write(xml.replace('\n', '\n      '))
            out.write('\n    </data>\n  </debate>\n')
        # Written PQs
        for file_path, date_str, qnum in question_files:
            xml = Path(file_path).read_text(encoding="utf-8")
            xml = strip_xml_header(xml)
            out.write(f'  <debate type="questions" question_type="written" date="{date_str}" number="{qnum}">\n    <data>\n')
            out.write(xml.replace('\n', '\n      '))
            out.write('\n    </data>\n  </debate>\n')
        out.write('</all_debates>\n')
    print(f"Combined all XMLs into {output_path}")

if __name__ == "__main__":
    N = 100  # Or whatever number you want

    dail_files = download_debate_xmls(
    "dail",
    "house",
    datetime(1919, 1, 1),
    datetime(2025, 7, 31),
    Path("data/dail_debates"),
    N
    )
    seanad_files = download_debate_xmls(
        "seanad",
        "house",
        datetime(1929, 1, 1),
        datetime(2025, 7, 31),
        Path("data/seanad_debates"),
        N
    )

    committee_files = download_committee_xmls(
        datetime(1924, 1, 1),
        datetime(2025, 7, 31),
        Path("data/committee_debates"),
        N
    )

    question_files = download_written_questions_xmls(
        datetime(2012, 1, 1),
        datetime(2025, 7, 31),
        "written", # only download written Qs as oral one's overlap with Dáil debates
        Path("data/written_questions"),
        N 
    )

    
    
    combine_to_single_xml(dail_files, seanad_files, committee_files, question_files, "all_debates.xml")