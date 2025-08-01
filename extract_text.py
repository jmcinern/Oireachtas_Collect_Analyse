from lxml import etree
import csv
from itertools import zip_longest
import os

def download_from_hf(hf_url, cache_path):
    """Download file from HuggingFace if not present in cache."""
    if not os.path.exists(cache_path):
        print(f"Downloading {hf_url} to {cache_path} ...")
        import requests
        response = requests.get(hf_url)
        response.raise_for_status()
        with open(cache_path, "wb") as f:
            f.write(response.content)
        print("Download complete.")
    else:
        print(f"Using cached file: {cache_path}")

def parse_file(path):
    tree = etree.parse(path)
    root = tree.getroot()
    # Iterate each debate node in file
    rows = []
    for debate in root.findall('.//{*}debate[@type]'):
        source_type = debate.get('type','')
        date = debate.get('date','')
        # Document-level
        NS = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}

        # then inside parse_file, for each debate:
        akn = debate.find('.//akn:akomaNtoso', namespaces=NS)
        if akn is None:
            # maybe it's nested under <data>
            akn = debate.find('.//akn:data/akn:akomaNtoso', namespaces=NS)
        # doc_id from FRBRWork
        doc_id = akn.find('.//{*}FRBRWork/{*}FRBRthis').get('value','') if akn is not None else ''
        # Preface blocks
        pre = akn.find('.//{*}preface') if akn is not None else None
        # Helper to find block
        def blk(name, tag):
            b = pre.find(f".//{{*}}block[@name='{name}']/{tag}") if pre is not None else None
            return b.text.strip() if b is not None and b.text else ''
        title_ga       = blk('title_ga','{*}docTitle')
        title_en       = blk('title_en','{*}docTitle')
        proponent_ga   = blk('proponent_ga','{*}docProponent')
        proponent_en   = blk('proponent_en','{*}docProponent')
        status_ga      = blk('status_ga','{*}docStatus')
        status_en      = blk('status_en','{*}docStatus')
        # date block
        date_block     = pre.find(".//{*}block[@name='date_en']/{*}docDate") if pre is not None else None
        document_date  = date_block.get('date','') if date_block is not None else ''
        # volume & number
        vol = pre.find(".//{*}docNumber[@refersTo='#vol_1062']") if pre is not None else None
        num = pre.find(".//{*}docNumber[@refersTo='#no_2']")   if pre is not None else None
        volume = vol.text.strip() if vol is not None and vol.text else ''
        number = num.text.strip() if num is not None and num.text else ''

        # Base fields
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

        # namespace map at top of parse_file:
        NS = {'akn':'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}

        # after you’ve found `akn`:
        rolls = akn.xpath('.//akn:rollCall', namespaces=NS)
        roll = rolls[0] if rolls else None

        if roll is not None:
            # get the first summary’s text
            hdr_list = roll.xpath('./akn:summary[1]/text()', namespaces=NS)
            header = hdr_list[0].strip() if hdr_list else ''

            # find every <person> under the table
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
            # heading per section
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
        if source_type=='questions':
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

def main():
    # HuggingFace raw file URL
    hf_url = "https://huggingface.co/datasets/jmcinern/Oireachtas_XML/resolve/main/Oireachtas_XML_2020-01-01_to_2025-01-01.xml"
    cache_path = "Oireachtas_XML_2020-01-01_to_2025-01-01.xml"
    download_from_hf(hf_url, cache_path)
    all_rows=[]
    for row in parse_file(cache_path):
        all_rows.append(row)
    # Write CSV
    fields=list(all_rows[0].keys()) if all_rows else []
    with open('debates_all.csv','w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)

if __name__=='__main__':
    print("Parsing Oireachtas XML file...")
    main()
    print("Done. Output written to debates_all.csv")