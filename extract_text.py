#!/usr/bin/env python3
import os
import csv
import requests
from tqdm import tqdm
from itertools import zip_longest
from lxml import etree

# -- CONSTANTS -----------------------------------------------------------

HF_URL     = "https://huggingface.co/datasets/jmcinern/Oireachtas_XML/resolve/main/Oireachtas_XML_1919-01-01_to_2025-07-31.xml"
CACHE_PATH = "Oireachtas_XML_1919-01-01_to_2025-07-31.xml"
CSV_PATH   = "debates_all.csv"

# Namespace map for Akoma-Ntoso
NS_AKN = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}

# -- HELPERS -------------------------------------------------------------

def download_from_hf(hf_url, cache_path):
    """Download file from HuggingFace if not in cache, with progress bar."""
    if not os.path.exists(cache_path):
        print(f"Downloading {hf_url} → {cache_path}")
        resp = requests.get(hf_url, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get('content-length', 0))
        with open(cache_path, "wb") as fout, tqdm(
            desc="Downloading XML",
            total=total,
            unit="B", unit_scale=True, unit_divisor=1024
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fout.write(chunk)
                    bar.update(len(chunk))
        print("Download complete.\n")
    else:
        print(f"Using cached XML: {cache_path}\n")

def extract_debate_rows(debate):
    """Given one <debate> element, return a list of flat dict rows."""
    rows = []
    source_type = debate.get('type','')
    date        = debate.get('date','')

    # Find the <akomaNtoso> subtree
    akn = debate.find('.//akn:akomaNtoso', namespaces=NS_AKN)
    if akn is None:
        akn = debate.find('.//akn:data/akn:akomaNtoso', namespaces=NS_AKN)

    # Pull document‐level FRBR ID
    doc_id = ""
    if akn is not None:
        el = akn.find('.//{*}FRBRWork/{*}FRBRthis')
        doc_id = el.get('value','') if el is not None else ""

    # Pull <preface> blocks
    pre = akn.find('.//{*}preface') if akn is not None else None
    def blk(name, tag):
        if pre is None: return ""
        node = pre.find(f".//{{*}}block[@name='{name}']/{tag}")
        return node.text.strip() if node is not None and node.text else ""

    title_ga      = blk('title_ga','{*}docTitle')
    title_en      = blk('title_en','{*}docTitle')
    proponent_ga  = blk('proponent_ga','{*}docProponent')
    proponent_en  = blk('proponent_en','{*}docProponent')
    status_ga     = blk('status_ga','{*}docStatus')
    status_en     = blk('status_en','{*}docStatus')

    date_block    = pre.find(".//{*}block[@name='date_en']/{*}docDate") if pre is not None else None
    document_date = date_block.get('date','') if date_block is not None else ""

    vol = pre.find(".//{*}docNumber[@refersTo='#vol_1062']") if pre is not None else None
    num = pre.find(".//{*}docNumber[@refersTo='#no_2']")    if pre is not None else None
    volume = vol.text.strip() if vol is not None and vol.text else ""
    number = num.text.strip() if num is not None and num.text else ""

    # Base template dict
    base = {
        'doc_id': doc_id, 'source_type': source_type, 'date': date,
        'title_ga': title_ga, 'title_en': title_en,
        'proponent_ga': proponent_ga, 'proponent_en': proponent_en,
        'status_ga': status_ga, 'status_en': status_en,
        'document_date': document_date, 'volume': volume, 'number': number,
        'committee_name': debate.get('slug',''),
        'question_type': debate.get('question_type',''),
        'question_number': debate.get('number',''),
        # placeholders for all other columns
        'section_name':'','section_id':'','element_type':'','element_id':'',
        'speaker_id':'','speaker_name':'','speaker_role':'','recorded_time':'',
        'topic':'','question':'','written_answer':'','text':'',
        'heading_text':'','heading_time':'','attendance':''
    }

    # --- Attendance (committees only) ---
    if akn is not None:
        rolls = akn.xpath('.//akn:rollCall', namespaces=NS_AKN)
        if rolls:
            roll   = rolls[0]
            hdr    = roll.xpath('./akn:summary[1]/text()', namespaces=NS_AKN)
            header = hdr[0].strip() if hdr else ""
            persons = roll.xpath('.//akn:table//akn:person', namespaces=NS_AKN)
            # one row per person; if you want a single combined row, join them instead
            for p in persons:
                pid  = p.get('refersTo','').lstrip('#')
                name = p.text.strip() if p.text else ""
                row  = base.copy()
                row.update({
                    'element_type':'attendance',
                    'text':         header,
                    'attendance':   name,
                    'speaker_id':   pid,
                    'speaker_name': name
                })
                rows.append(row)

    # --- Sections & Speech ---
    for sec in debate.findall('.//{*}debateSection'):
        sec_name = sec.get('name','')
        sec_id   = sec.get('eId','')

        # extract heading
        hd = sec.find('{*}heading')
        heading_text = hd.text.strip() if hd is not None and hd.text else ""
        rt = hd.find('{*}recordedTime') if hd is not None else None
        heading_time = rt.get('time','') if rt is not None else ""

        # summary rows
        for summ in sec.findall('{*}summary'):
            row = base.copy()
            row.update({
                'section_name':sec_name, 'section_id':sec_id,
                'element_type':'summary','element_id':summ.get('eId',''),
                'text':summ.text.strip() if summ.text else '',
                'heading_text':heading_text,'heading_time':heading_time
            })
            rows.append(row)

        # speech rows
        for spk in sec.findall('{*}speech'):
            spk_id  = spk.get('eId','')
            spkr_id = spk.get('by','')
            role    = spk.get('as','')
            fr      = spk.find('{*}from')
            name    = fr.text.strip() if fr is not None and fr.text else ""
            rt2     = fr.find('{*}recordedTime') if fr is not None else None
            rec_time= rt2.get('time','') if rt2 is not None else ""
            for p in spk.findall('{*}p'):
                txt = ''.join(p.itertext()).strip()
                row = base.copy()
                row.update({
                    'section_name':sec_name, 'section_id':sec_id,
                    'element_type':'speech','element_id':spk_id,
                    'speaker_id':spkr_id,'speaker_name':name,
                    'speaker_role':role,'recorded_time':rec_time,
                    'text':txt,'heading_text':heading_text,'heading_time':heading_time
                })
                rows.append(row)

    # --- Written Questions & Answers ---
    if source_type == 'questions' and akn is not None:
        akn_ns = akn.nsmap.get(None)
        ns = {'ns':akn_ns} if akn_ns else {}
        qs  = akn.xpath('.//ns:question', namespaces=ns)
        ans = akn.xpath('.//ns:speech',   namespaces=ns)
        for q_elem, a_elem in zip_longest(qs, ans, fillvalue=None):
            q_text = ' '.join(''.join(p.itertext()).strip() for p in (q_elem.findall('{*}p') if q_elem is not None else []))
            a_text = ' '.join(''.join(p.itertext()).strip() for p in (a_elem.findall('{*}p') if a_elem is not None else []))
            combined = f"{q_text} {a_text}".strip()
            topic = q_elem.get('to','') if q_elem is not None else ""

            # question row
            row = base.copy()
            row.update({
                'element_type':'question',
                'element_id': q_elem.get('eId','') if q_elem is not None else '',
                'topic':       topic,
                'question':    q_text,
                'written_answer': a_text,
                'text':        combined
            })
            rows.append(row)

    return rows

def stream_and_write(xml_path, csv_path):
    """Iterparse the big XML, write each debate’s rows immediately to CSV."""
    with open(csv_path, 'w', newline='', encoding='utf-8') as fout:
        writer = None

        # only fire on end-of-debate tags
        context = etree.iterparse(xml_path, events=('end',), tag='{*}debate', recover=True)
        for _, debate in tqdm(context, desc="Streaming debates", unit="debate"):

            # SKIP nested/inner <debate> tags that lack a top-level "type"
            if debate.get('type') is None:
                debate.clear()
                # purge previous siblings to free memory
                while debate.getprevious() is not None:
                    del debate.getparent()[0]
                continue

            # extract and write rows
            try:
                rows = extract_debate_rows(debate)
                if rows:
                    if writer is None:
                        writer = csv.DictWriter(fout, fieldnames=list(rows[0].keys()))
                        writer.writeheader()
                    writer.writerows(rows)
            except Exception as e:
                print(f"Error parsing debate: {e}")

            # free this subtree
            debate.clear()
            while debate.getprevious() is not None:
                del debate.getparent()[0]

def main():
    print("1) Ensure XML is available…")
    download_from_hf(HF_URL, CACHE_PATH)
    print("2) Streaming parse → CSV…")
    stream_and_write(CACHE_PATH, CSV_PATH)
    print(f"All done! Output in `{CSV_PATH}`.")

if __name__ == '__main__':
    main()
