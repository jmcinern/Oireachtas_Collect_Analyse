import xml.etree.ElementTree as ET

def load_debates_xml(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    return root

def list_debate_types(root):
    print("Debate types and counts:")
    types = {}
    for debate in root.findall('debate'):
        dtype = debate.get('type')
        types[dtype] = types.get(dtype, 0) + 1
    for dtype, count in types.items():
        print(f"{dtype}: {count}")

def list_dates(root, debate_type=None):
    print("Dates of debates:")
    for debate in root.findall('debate'):
        if debate_type and debate.get('type') != debate_type:
            continue
        print(debate.get('date'))

if __name__ == "__main__":
    xml_path = "all_debates.xml"
    root = load_debates_xml(xml_path)
    list_debate_types(root)
    # Example: list all Dáil debate dates
    list_dates(root, debate_type="questions")