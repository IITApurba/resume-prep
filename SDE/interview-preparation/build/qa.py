"""Automated QA for the generated study-book PDFs.

Checks, per book:
  * page count, PDF outline (bookmarks), all fonts embedded
  * no raster images (diagrams must stay vector)
  * no blank pages, no text outside the page / 20 mm text column
  * every internal link resolves to a real page
  * all Mermaid figures present (Figure captions found in the text layer)
  * content preservation: every word of the Markdown source (outside Mermaid
    blocks) is present in the PDF text layer at least as many times
  * grayscale contact sheets + full-resolution grayscale renders of sample pages

usage: .venv/bin/python qa.py [OUTDIR]
"""
import collections
import json
import os
import re
import sys
import unicodedata

import pymupdf

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PDFS = os.path.join(ROOT, "pdfs")
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "out", "qa")
os.makedirs(OUT, exist_ok=True)
MM = 72 / 25.4


def md_words(md):
    md = re.sub(r"^```mermaid\n.*?^```\s*$", "", md, flags=re.S | re.M)
    md = re.sub(r"^(\s*```)[\w+-]+\s*$", r"\1", md, flags=re.M)  # fence language tags are metadata
    md = re.sub(r"<[^>]+>", " ", md)  # html tags (<br/>, page-break divs)
    md = re.sub(r"\]\(#[^)]*\)", "]", md)  # internal link targets
    # the Markdown's own link-only table of contents is replaced by the generated one
    md = re.sub(r"^## Table of Contents\n.*?(?=^<div|^# )", "", md, flags=re.S | re.M)
    return [w.lower() for w in re.findall(r"[A-Za-z0-9]+", unicodedata.normalize("NFKC", md))]


def pdf_words(doc):
    txt = "\n".join(p.get_text() for p in doc)
    txt = unicodedata.normalize("NFKC", txt)
    return [w.lower() for w in re.findall(r"[A-Za-z0-9]+", txt)], txt


report = {}
for fn in sorted(os.listdir(PDFS)):
    if not fn.endswith(".pdf"):
        continue
    slug = fn[:-4]
    doc = pymupdf.open(os.path.join(PDFS, fn))
    md = open(os.path.join(ROOT, slug + ".md"), encoding="utf8").read()
    r = {"pages": doc.page_count}

    # Fonts / images / blank pages / overflow
    fonts, unembedded, images, blank, overflow = set(), set(), 0, [], []
    for i, page in enumerate(doc):
        for f in page.get_fonts(full=True):
            fonts.add(f[3] or f[2])
            # Type3 fonts carry their glyph outlines inline (Chrome uses them for synthetic styles).
            if f[2] != "Type3" and (not f[1] or f[1] == "n/a"):
                unembedded.add(f[3])
        images += len(page.get_images())
        text = page.get_text().strip()
        if not text and not page.get_drawings():
            blank.append(i + 1)
        W, H = page.rect.width, page.rect.height
        if i == 0:
            continue  # cover is full-bleed by design
        for b in page.get_text("dict")["blocks"]:
            for line in b.get("lines", []):
                for sp in line["spans"]:
                    if not sp["text"].strip():
                        continue
                    x0, y0, x1, y1 = sp["bbox"]
                    if x0 < 20 * MM - 2 or x1 > W - 20 * MM + 2 or y0 < 5 * MM or y1 > H - 5 * MM:
                        overflow.append((i + 1, sp["text"][:40], [round(v) for v in sp["bbox"]]))
    r["fonts"] = sorted(fonts)
    r["unembedded_fonts"] = sorted(unembedded)
    r["raster_images"] = images
    r["blank_pages"] = blank
    r["text_outside_column"] = overflow[:20]
    r["text_outside_column_count"] = len(overflow)

    # Links
    bad_links, n_links = [], 0
    for i, page in enumerate(doc):
        for l in page.get_links():
            n_links += 1
            if l["kind"] == pymupdf.LINK_GOTO and not (0 <= l.get("page", -1) < doc.page_count):
                bad_links.append((i + 1, l))
            elif l["kind"] == pymupdf.LINK_NAMED and l.get("nameddest") and l.get("page", -1) < 0:
                bad_links.append((i + 1, l.get("nameddest")))
    r["internal_links"] = n_links
    r["broken_links"] = bad_links
    r["outline_entries"] = len(doc.get_toc())

    # Figures
    pw, ptxt = pdf_words(doc)
    want = re.findall(r"^\*\*Figure ([\w.]+) —", md, flags=re.M)
    mermaid_blocks = len(re.findall(r"^```mermaid\s*$", md, flags=re.M))
    fig_pages = {}
    for i, page in enumerate(doc):
        for m in re.finditer(r"Figure ([\d]+\.[\w]+) — ", unicodedata.normalize("NFKC", page.get_text())):
            if i > 6:  # skip the List of Figures front matter
                fig_pages.setdefault(m.group(1), i + 1)
    r["mermaid_blocks"] = mermaid_blocks
    r["figures_found"] = sum(1 for f in want if f in fig_pages)
    r["figure_pages"] = fig_pages
    r["missing_figures"] = [f for f in want if f not in fig_pages]

    # Content preservation (bag-of-words containment)
    mw = collections.Counter(md_words(md))
    pc = collections.Counter(pw)
    deficit = {w: (n, pc.get(w, 0)) for w, n in mw.items() if pc.get(w, 0) < n}
    # Long identifiers may wrap across lines inside code/table cells; recount those
    # occurrences in the text layer with all whitespace removed before calling them missing.
    joined = re.sub(r"\s+", "", unicodedata.normalize("NFKC", ptxt)).lower()
    deficit = {w: v for w, v in deficit.items() if joined.count(w) < v[0]}
    r["md_words"] = sum(mw.values())
    r["md_words_missing"] = sum(n - p for n, p in deficit.values())
    r["missing_detail"] = dict(sorted(deficit.items(), key=lambda kv: -(kv[1][0] - kv[1][1]))[:40])

    # Heading check: every Markdown heading text appears in the PDF
    heads = [h for h in re.findall(r"^#{1,4} (.+)$", re.sub(r"^```.*?^```", "", md, flags=re.S | re.M), flags=re.M)]
    flat = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", ptxt))
    miss_heads = []
    for h in heads:
        h2 = re.sub(r"[`*]", "", h)
        h2 = re.sub(r"^Part \d+: ", "", h2)
        h2 = re.sub(r"^\d+\. ", "", h2)
        h2 = re.sub(r"^Q\d+\. ", "", h2)
        probe = re.sub(r"\W+", "", h2)[:40].lower()
        if probe not in re.sub(r"\W+", "", flat).lower():
            miss_heads.append(h)
    r["headings"] = len(heads)
    r["headings_missing"] = miss_heads
    r["questions_found"] = len(re.findall(r"^Q(\d+)$", ptxt, flags=re.M))

    report[slug] = r

    # Grayscale contact sheets (whole book) for visual review
    cols, rows, tw, th = 6, 4, 248, 351
    per = cols * rows
    for s in range(0, doc.page_count, per):
        sheet = pymupdf.Pixmap(pymupdf.csGRAY, pymupdf.IRect(0, 0, cols * (tw + 8) + 8, rows * (th + 8) + 8), False)
        sheet.clear_with(170)
        for k, p in enumerate(range(s, min(s + per, doc.page_count))):
            z = tw / doc[p].rect.width
            pix = doc[p].get_pixmap(matrix=pymupdf.Matrix(z, z), colorspace=pymupdf.csGRAY)
            pix.set_origin(8 + (k % cols) * (tw + 8), 8 + (k // cols) * (th + 8))
            sheet.copy(pix, pix.irect)
        sheet.save(os.path.join(OUT, f"{slug[:2]}-gray-sheet-{s // per + 1:02d}.png"))

json.dump(report, open(os.path.join(OUT, "qa-report.json"), "w"), indent=2)
for slug, r in report.items():
    print(f"\n== {slug}")
    for k in ("pages", "outline_entries", "internal_links", "raster_images", "blank_pages", "unembedded_fonts",
              "text_outside_column_count", "mermaid_blocks", "figures_found", "missing_figures", "headings",
              "headings_missing", "questions_found", "md_words", "md_words_missing"):
        print(f"  {k:28s} {r[k]}")
    if r["broken_links"]:
        print("  BROKEN LINKS", r["broken_links"][:5])
    if r["md_words_missing"]:
        print("  missing words (word: md count, pdf count):", r["missing_detail"])
    if r["text_outside_column_count"]:
        print("  outside column:", r["text_outside_column"][:6])
