from lxml import etree
import csv
from itertools import zip_longest
import os
from tqdm import tqdm
import requests

def download_from_hf(hf_url, cache_path):
    if not os.path.exists(cache_path):
        print(f"Downloading {hf_url} to {cache_path} …")
        resp = requests.get(hf_url, stream=True); resp.raise_for_status()
        total = int(resp.headers.get('content-length', 0))
        with open(cache_path, "wb") as f, tqdm(total=total, unit='B', unit_scale=True) as bar:
            for chunk in resp.iter_content(8192):
                f.write(chunk); bar.update(len(chunk))
        print("Download complete.")
    else:
        print(f"Using cached file: {cache_path}")

def parse_file(path):
    NS = {'akn':'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}
    ctx = etree.iterparse(path, events=('end',), tag='debate')

    for _, debate in ctx:
        base = {
            'doc_id':'','source_type':debate.get('type',''),'date':debate.get('date',''),
            'title_ga':'','title_en':'','proponent_ga':'','proponent_en':'',
            'status_ga':'','status_en':'','document_date':'','volume':'','number':'',
            'committee_name':debate.get('slug',''),'question_type':debate.get('question_type',''),
            'question_number':debate.get('number',''),
            'section_name':'','section_id':'','element_type':'','element_id':'',
            'speaker_id':'','speaker_name':'','speaker_role':'','recorded_time':'',
            'topic':'','question':'','written_answer':'','text':'','heading_text':'',
            'heading_time':'','attendance':''
        }

        # locate akn without chaining or/
        akn = debate.find('.//akn:akomaNtoso', namespaces=NS)
        if akn is None:
            akn = debate.find('.//akn:data/akn:akomaNtoso', namespaces=NS)

        if akn is not None:
            tw = akn.find('.//{*}FRBRWork/{*}FRBRthis')
            base['doc_id'] = tw.get('value','') if tw is not None else ''

            pre = akn.find('.//{*}preface')
            def blk(name, tag):
                if pre is None:
                    return ''
                b = pre.find(f".//{{*}}block[@name='{name}']/{tag}")
                return b.text.strip() if (b is not None and b.text) else ''

            base.update({
                'title_ga':     blk('title_ga','{*}docTitle'),
                'title_en':     blk('title_en','{*}docTitle'),
                'proponent_ga': blk('proponent_ga','{*}docProponent'),
                'proponent_en': blk('proponent_en','{*}docProponent'),
                'status_ga':    blk('status_ga','{*}docStatus'),
                'status_en':    blk('status_en','{*}docStatus'),
            })

            db = pre.find(".//{*}block[@name='date_en']/{*}docDate") if pre is not None else None
            base['document_date'] = db.get('date','') if db is not None else ''

            vol = pre.find(".//{*}docNumber[@refersTo='#vol_1062']") if pre is not None else None
            num = pre.find(".//{*}docNumber[@refersTo='#no_2']")   if pre is not None else None
            base['volume'] = (vol.text or '').strip() if vol is not None else ''
            base['number'] = (num.text or '').strip() if num is not None else ''

        # attendance (always)
        rolls = akn.xpath('.//akn:rollCall', namespaces=NS) if akn is not None else []
        if rolls:
            hdr = rolls[0].xpath('./akn:summary[1]/text()', namespaces=NS)
            header = hdr[0].strip() if hdr else ''
            for p in rolls[0].xpath('.//akn:table//akn:person', namespaces=NS):
                row = base.copy()
                row.update({
                    'element_type':'attendance','text':header,
                    'attendance':(p.text or '').strip(),
                    'speaker_id':p.get('refersTo','').lstrip('#'),
                    'speaker_name':(p.text or '').strip()
                })
                yield row

        # sections: summaries, speeches (always)
        for sec in debate.findall('.//{*}debateSection'):
            sec_name, sec_id = sec.get('name',''), sec.get('eId','')
            hd = sec.find('{*}heading')
            heading_text = (hd.text or '').strip() if hd is not None else ''
            rt = hd.find('{*}recordedTime') if hd is not None else None
            heading_time = rt.get('time','') if rt is not None else ''

            # summaries
            for summ in sec.findall('{*}summary'):
                row = base.copy()
                row.update({
                    'section_name':sec_name,'section_id':sec_id,
                    'element_type':'summary','element_id':summ.get('eId',''),
                    'text':(summ.text or '').strip(),
                    'heading_text':heading_text,'heading_time':heading_time
                })
                yield row

            # speeches
            for spk in sec.findall('{*}speech'):
                spk_id, spkr_id = spk.get('eId',''), spk.get('by','')
                role = spk.get('as','')
                fr = spk.find('{*}from')
                name = (fr.text or '').strip() if fr is not None else ''
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
                        'text':txt,'heading_text':heading_text,
                        'heading_time':heading_time
                    })
                    yield row

        # questions & answers (only for type='questions')
        if debate.get('type','') == 'questions' and akn is not None:
            akn_ns = akn.nsmap.get(None)
            ns = {'ns':akn_ns} if akn_ns else {}
            qs  = akn.xpath('.//ns:question', namespaces=ns)
            ans = akn.xpath('.//ns:speech',   namespaces=ns)

            for q_elem, a_elem in zip_longest(qs, ans, fillvalue=None):
                if q_elem is not None:
                    q_text = ' '.join(
                        ''.join(p.itertext()).strip()
                        for p in q_elem.findall('{*}p')
                    )
                else:
                    q_text = ''

                if a_elem is not None:
                    a_text = ' '.join(
                        ''.join(p.itertext()).strip()
                        for p in a_elem.findall('{*}p')
                    )
                else:
                    a_text = ''

                row = base.copy()
                row.update({
                    'element_type':'question',
                    'element_id':    q_elem.get('eId','') if q_elem is not None else '',
                    'topic':         q_elem.get('to','')  if q_elem is not None else '',
                    'question':      q_text,
                    'written_answer':a_text,
                    'text':          f"{q_text} {a_text}".strip()
                })
                yield row

        # cleanup to free memory
        debate.clear()
        while debate.getprevious() is not None:
            del debate.getparent()[0]
    del ctx



def main():
    date_range = "2025-01-01_to_2025-07-31"
    hf_url = f"https://huggingface.co/datasets/jmcinern/Oireachtas_XML/resolve/main/Oireachtas_XML_{date_range}.xml"
    cache_path = f"Oireachtas_XML_{date_range}.xml"
    download_from_hf(hf_url, cache_path)

    out_csv = f"debates_all_{date_range}.csv"
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = None
        for row in parse_file(cache_path):
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                writer.writeheader()
            writer.writerow(row)

if __name__=='__main__':
    print("Parsing Oireachtas XML file…")
    main()
    print("Done.")


