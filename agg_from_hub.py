import os
from pathlib import Path
from huggingface_hub import HfApi, login, hf_hub_download
import re

# SETUP
repo_id = "jmcinern/Oireachtas_XML"
login(token=os.environ["HF_TOKEN"])

# List of XML slice filenames (update if needed)
all_slice_files = [
    "Oireachtas_XML_1919-01-01_to_1950-12-31.xml",
    "Oireachtas_XML_1951-01-01_to_1959-12-31.xml",
    "Oireachtas_XML_1960-01-01_to_1969-12-31.xml",
    "Oireachtas_XML_1970-01-01_to_1980-01-01.xml",
    "Oireachtas_XML_1980-01-01_to_1995-01-01.xml",
    "Oireachtas_XML_1995-01-01_to_2005-01-01.xml",
    "Oireachtas_XML_2005-01-01_to_2015-01-01.xml",
    "Oireachtas_XML_2015-01-01_to_2020-01-01.xml",
    "Oireachtas_XML_2020-01-01_to_2025-01-01.xml",
    "Oireachtas_XML_2025-01-01_to_2025-07-31.xml",
]

def strip_outer_wrapper(xml_str):
    # Remove BOM and XML header
    xml_str = xml_str.lstrip('\ufeff')
    xml_str = re.sub(r'<\?xml[^>]+\?>', '', xml_str)
    # Remove <all_debates> wrapper (open/close tags)
    xml_str = re.sub(r'^\s*<all_debates>\s*', '', xml_str)
    xml_str = re.sub(r'\s*</all_debates>\s*$', '', xml_str)
    return xml_str.strip()

aggregate_xml_path = Path("Oireachtas_XML_1919-01-01_to_2025-07-31.xml")

with open(aggregate_xml_path, "w", encoding="utf-8") as out:
    out.write('<all_debates>\n')
    for fname in all_slice_files:
        print(f"Processing {fname}...")
        local_fp = hf_hub_download(repo_id=repo_id, filename=fname, repo_type="dataset")
        xml = Path(local_fp).read_text(encoding="utf-8")
        inner = strip_outer_wrapper(xml)
        if inner:
            out.write(inner)
            out.write('\n')
    out.write('</all_debates>\n')

print(f"Aggregated file written to {aggregate_xml_path}")

# Push to hub
api = HfApi()
api.upload_file(
    path_or_fileobj=aggregate_xml_path,
    path_in_repo=aggregate_xml_path.name,
    repo_id=repo_id,
    repo_type="dataset"
)
print(f"Pushed {aggregate_xml_path.name} to {repo_id}")
