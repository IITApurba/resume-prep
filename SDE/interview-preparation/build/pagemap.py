"""Print a JSON map {named destination -> 1-based page number} for a PDF.

Chrome writes a named destination for every element id that is the target of an
internal link, so every TOC / list-of-figures entry can be resolved here.
"""
import json
import sys

import pymupdf

doc = pymupdf.open(sys.argv[1])
out = {name: d["page"] + 1 for name, d in doc.resolve_names().items()}
out["__pages"] = doc.page_count
print(json.dumps(out))
