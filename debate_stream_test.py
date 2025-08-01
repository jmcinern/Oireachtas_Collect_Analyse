from lxml import etree
from itertools import islice

def test_first_debates(xml_path, n=3):
    context = etree.iterparse(xml_path, events=('end',), tag='{*}debate', recover=True)
    count = 0
    for _, debate in context:
        # only top-level debates carry a 'type' attribute
        dtype = debate.get('type')
        if not dtype:
            debate.clear()
            while debate.getprevious() is not None:
                del debate.getparent()[0]
            continue

        # run your extractor
        rows = extract_debate_rows(debate)
        print(f"[{count}] type={dtype!r} date={debate.get('date')!r} → {len(rows)} rows")

        count += 1
        if count >= n:
            break

        # clear to keep memory minimal
        debate.clear()
        while debate.getprevious() is not None:
            del debate.getparent()[0]

# Example call:
test_first_debates("Oireachtas_XML_1919-01-01_to_2025-07-31.xml", n=3)
