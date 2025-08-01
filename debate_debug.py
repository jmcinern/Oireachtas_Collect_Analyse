#!/usr/bin/env python3
from lxml import etree
from itertools import zip_longest

# namespace for akn:
NS_AKN = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}

def extract_debate_rows(debate):
    """Minimal stub: just count sections + speeches for now."""
    rows = []
    # count sections
    secs = debate.findall('.//{*}debateSection')
    for sec in secs:
        # one summary row per <summary>
        for summ in sec.findall('{*}summary'):
            rows.append(('summary', summ.get('eId','')))
        # one speech row per <speech>
        for spk in sec.findall('{*}speech'):
            rows.append(('speech', spk.get('eId','')))
    return rows

def test_first_debates(xml_path, n=3):
    context = etree.iterparse(xml_path, events=('end',), tag='{*}debate', recover=True)
    seen = 0
    for _, debate in context:
        # skip any inner <debate> that lacks a top-level 'type'
        if debate.get('type') is None:
            debate.clear()
            while debate.getprevious() is not None:
                del debate.getparent()[0]
            continue

        dtype = debate.get('type')
        ddate = debate.get('date')
        print(f"\n-- Debate {seen+1}: type={dtype!r}, date={ddate!r}")

        # 1) find akomaNtoso
        akn = debate.find('.//akn:akomaNtoso', namespaces=NS_AKN) \
           or debate.find('.//akn:data/akn:akomaNtoso', namespaces=NS_AKN)
        print("   akomaNtoso:", "FOUND" if akn is not None else "MISSING")

        # 2) count sections
        secs = debate.findall('.//{*}debateSection')
        print(f"   # debateSection elements: {len(secs)}")

        # 3) run our stub extractor
        rows = extract_debate_rows(debate)
        print(f"   extractor would emit {len(rows)} rows")

        seen += 1
        if seen >= n:
            break

        # clear subtree to free mem
        debate.clear()
        while debate.getprevious() is not None:
            del debate.getparent()[0]

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python debate_debug.py <path-to-xml>")
        sys.exit(1)
    test_first_debates(sys.argv[1])
