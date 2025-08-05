from lxml import etree
import csv
from itertools import zip_longest
import os
from tqdm import tqdm
import requests

def download_from_hf(hf_url, cache_path):
    """Download file from HuggingFace if not present in cache, with progress bar."""
    if not os.path.exists(cache_path):
        print(f"Downloading {hf_url} to {cache_path} ...")
        response = requests.get(hf_url, stream=True)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        with open(cache_path, "wb") as f, tqdm(
            desc=cache_path,
            total=total,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))
        print("Download complete.")
    else:
        print(f"Using cached file: {cache_path}")

def extract_debate_rows(debate):
    # Replicates your element logic, with some safety checks
    source_type = debate.get('type','')
    date = debate.get('date','')
    NS = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}
    rows = []

    akn = debate.find('.//akn:akomaNtoso', namespaces=NS)
    if akn is None:
        akn = debate.find('.//akn:data/akn:akomaNtoso', namespaces=NS)
    doc_id = akn.find('.//{*}FRBRWork/{*}FRBRthis').get('value','') if akn is not None else ''
    pre = akn.find('.//{*}preface') if akn is not None else None

    def blk(name, tag):
        b = pre.find(f".//{{*}}block[@name='{name}']/{tag}") if pre is not None else None
        return b.text.strip() if b is not None and b.text else ''

    title_ga       = blk('title_ga','{*}docTitle')
    title_en       = blk('title_en','{*}docTitle')
    proponent_ga   = blk('proponent_ga','{*}docProponent')
    proponent_en   = blk('proponent_en','{*}docProponent')
    status_ga      = blk('status_ga','{*}docStatus')
    status_en      = blk('status_en','{*}docStatus')
    date_block     = pre.find(".//{*}block[@name='date_en']/{*}docDate") if pre is not None else None
    document_date  = date_block.get('date','') if date_block is not None else ''
    vol = pre.find(".//{*}docNumber[@refersTo='#vol_1062']") if pre is not None else None
    num = pre.find(".//{*}docNumber[@refersTo='#no_2']")   if pre is not None else None
    volume = vol.text.strip() if vol is not None and vol.text else ''
    number = num.text.strip() if num is not None and num.text else ''

    base = {
        'doc_id': doc_id, 'source_type': source_type, 'date': date,
        'title_ga': title_ga, 'title_en': title_en,
        'proponent_ga': proponent_ga, 'proponent_en': proponent_en,
        'status_ga': status_ga, 'status_en': status_en,
        'document_date': document_date, 'volume': volume, 'number': number,
        'committee_name': debate.get('slug',''),
        'question_type': debate.get('question_type',''),
        'question_number': debate.get('number',''),
        # placeholders
        'section_name':'','section_id':'','element_type':'',
        'element_id':'','speaker_id':'','speaker_name':'',
        'speaker_role':'','recorded_time':'','topic':'',
        'question':'','written_answer':'','text':'','heading_text':'','heading_time':'','attendance':''
    }

    # Attendance
    rolls = akn.xpath('.//akn:rollCall', namespaces=NS) if akn is not None else []
    roll = rolls[0] if rolls else None
    if roll is not None:
        hdr_list = roll.xpath('./akn:summary[1]/text()', namespaces=NS)
        header = hdr_list[0].strip() if hdr_list else ''
        persons = roll.xpath('.//akn:table//akn:person', namespaces=NS)
        for p in persons:
            pid  = p.get('refersTo','').lstrip('#')
            name = p.text.strip() if p.text else ''
            row = base.copy()
            row.update({
                'element_type': 'attendance',
                'text':         header,
                'attendance':   name,
                'speaker_id':   pid,
                'speaker_name': name
            })
            rows.append(row)

    # Sections
    for sec in debate.findall('.//{*}debateSection'):
        sec_name = sec.get('name','')
        sec_id   = sec.get('eId','')
        hd = sec.find('{*}heading')
        heading_text = hd.text.strip() if hd is not None and hd.text else ''
        rt = hd.find('{*}recordedTime') if hd is not None else None
        heading_time = rt.get('time','') if rt is not None else ''

        # Summaries
        for summ in sec.findall('{*}summary'):
            row = base.copy()
            row.update({
                'section_name':sec_name,'section_id':sec_id,
                'element_type':'summary','element_id':summ.get('eId',''),
                'text':summ.text.strip() if summ.text else '',
                'heading_text':heading_text,'heading_time':heading_time
            })
            rows.append(row)

        # Speeches
        for spk in sec.findall('{*}speech'):
            spk_id = spk.get('eId','')
            spkr_id = spk.get('by','')
            role = spk.get('as','')
            fr = spk.find('{*}from')
            name = fr.text.strip() if fr is not None and fr.text else ''
            rt = fr.find('{*}recordedTime') if fr is not None else None
            rec_time = rt.get('time','') if rt is not None else ''
            for p in spk.findall('{*}p'):
                txt = ''.join(p.itertext()).strip()
                row = base.copy()
                row.update({
                    'section_name':sec_name,'section_id':sec_id,
                    'element_type':'speech','element_id':spk_id,
                    'speaker_id':spkr_id,'speaker_name':name,
                    'speaker_role':role,'recorded_time':rec_time,
                    'text':txt,'heading_text':heading_text,'heading_time':heading_time
                })
                rows.append(row)

    # Questions
    if source_type=='questions' and akn is not None:
        akn_ns = akn.nsmap.get(None)
        ns = {'ns':akn_ns} if akn_ns else {}
        qs = akn.xpath('.//ns:question',namespaces=ns)
        ans = akn.xpath('.//ns:speech',namespaces=ns)
        for q_elem,a_elem in zip_longest(qs,ans,fillvalue=None):
            q_text = ' '.join(''.join(p.itertext()).strip() for p in q_elem.findall('{*}p')) if q_elem is not None else ''
            a_text = ' '.join(''.join(p.itertext()).strip() for p in a_elem.findall('{*}p')) if a_elem is not None else ''
            combined=f"{q_text} {a_text}".strip()
            row=base.copy()
            row.update({
                'element_type':'question','element_id':q_elem.get('eId','') if q_elem is not None else '',
                'topic':q_elem.get('to','') if q_elem is not None else '',
                'question':q_text,'written_answer':a_text,'text':combined
            })
            rows.append(row)

    return rows

def stream_and_write(path, csv_path):
    with open(csv_path, 'w', newline='', encoding='utf-8') as f_out:
        writer = None
        context = etree.iterparse(path, events=('end',), tag='{*}debate', recover=True)
        for _, debate in tqdm(context, desc="Parsing debates (streaming)", unit="debate"):
            try:
                rows = extract_debate_rows(debate)
                if rows:
                    # Setup fieldnames on first actual row
                    if writer is None:
                        writer = csv.DictWriter(f_out, fieldnames=list(rows[0].keys()))
                        writer.writeheader()
                    writer.writerows(rows)
                # Free memory
                debate.clear()
                # Also clear parent elements to really drop memory
                while debate.getprevious() is not None:
                    del debate.getparent()[0]
            except Exception as e:
                #print(f"Error parsing debate: {e}")
                continue

def main():
    # Oireachtas_XML_2020-01-01_to_2025-01-01.xml
    hf_url = "https://huggingface.co/datasets/jmcinern/Oireachtas_XML/resolve/main/Oireachtas_XML_2020-01-01_to_2025-01-01.xml"
    cache_path = "Oireachtas_XML_2020-01-01_to_2025-01-01.xml"
    download_from_hf(hf_url, cache_path)
    csv_path = "debates_all_2020_2025.csv"
    stream_and_write(cache_path, csv_path)
    print("Done. Output written to debates_all_2020_2025.csv")

if __name__=='__main__':
    print("Parsing Oireachtas XML file... (streaming, low RAM mode)")
    main()
