#!/usr/bin/env python3
from lxml import etree

NS_AKN = {'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13'}

def debug_first_debate(xml_path):
    ctx = etree.iterparse(xml_path, events=('end',), tag='{*}debate', recover=True)
    for _, debate in ctx:
        if debate.get('type') is None:
            debate.clear(); 
            while debate.getprevious() is not None: 
                del debate.getparent()[0]
            continue

        print(f"=== Debugging top‐level debate type={debate.get('type')} date={debate.get('date')} ===")
        # Locate the akomaNtoso
        akn = debate.find('.//akn:akomaNtoso', namespaces=NS_AKN) \
           or debate.find('.//*/data/akn:akomaNtoso', namespaces=NS_AKN)

        if akn is None:
            print("No <akomaNtoso> found at all.")
        else:
            # Print the tags of direct children under <akomaNtoso>
            print("Children of <akomaNtoso>:")
            for child in akn:
                print("  ", child.tag)
            # If there is a <debate> under it, also list its children
            inner = akn.find('{*}debate')
            if inner is not None:
                print("Children of inner <debate>:")
                for c2 in inner:
                    print("  ", c2.tag)

        break  # only first debate
    print("Done.")
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python debate_debug2.py <Oireachtas XML>")
        sys.exit(1)
    debug_first_debate(sys.argv[1])
