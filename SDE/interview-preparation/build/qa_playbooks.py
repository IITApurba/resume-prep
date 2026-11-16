"""Automated QA for the interview playbooks in ../final-playbooks/.

Checks, per PDF:
  * page count (hard limit 30, target <= 25)
  * all fonts embedded, no raster images (diagrams stay vector), no blank pages
  * no text outside the 18 mm text column
  * every internal link (cover contents) resolves to a page
  * figure captions present; question counts by difficulty; concept-card count
  * resume coverage: every key term from each resume bullet appears in the booklet
Writes grayscale contact sheets to out/playbooks/qa/.

usage: .venv/bin/python qa_playbooks.py
"""
import json
import os
import re
import sys

import pymupdf

HERE = os.path.dirname(os.path.abspath(__file__))
FINAL = os.path.join(os.path.dirname(HERE), "final-playbooks")
SRC = os.path.join(FINAL, "src")
OUT = os.path.join(HERE, "out", "playbooks", "qa")
os.makedirs(OUT, exist_ok=True)
MM = 72 / 25.4

# Key technical terms from each resume bullet (case-insensitive regexes) that the booklet must explain.
RESUME_TERMS = {
    "01-payment-transaction-processing": {
        "Bullet 1": [r"spring boot", r"microservice", r"\bACID\b", r"OAuth2", r"\bJWT\b", r"idempotency key", r"optimistic locking", r"concurren"],
        "Bullet 2": [r"\bKafka\b", r"retr(y|ies)", r"consumer group", r"\bDLQ|dead-letter", r"10K\+? requests/min", r"200 ms", r"p95"],
        "Bullet 3": [r"index", r"connection pool", r"redis cach|cache-aside", r"40%", r"30%", r"CPU", r"Kubernetes"],
    },
    "02-distributed-object-storage": {
        "Bullet 1": [r"\bgRPC\b", r"consistent hashing", r"virtual node", r"replication", r"checksum", r"CRC32C", r"concurrent I/O"],
        "Bullet 2": [r"write-ahead log", r"\bWAL\b", r"replica repair|repair", r"hint", r"read repair", r"anti-entropy", r"node fail", r"availab"],
        "Bullet 3": [r"1,000 concurrent", r"10K ops/min", r"200 ms", r"p95", r"25%", r"CPU", r"RocksDB", r"group commit", r"append-only", r"quorum"],
    },
    "03-real-time-event-processing": {
        "Bullet 1": [r"\bKafka\b", r"\bFlink\b", r"event-time|event time", r"window", r"checkpoint", r"dedup", r"watermark"],
        "Bullet 2": [r"consumer-group|consumer group", r"backpressure", r"10K\+? events/s", r"1 s(ec)? p95|under 1", r"concurrent I/O", r"partition", r"offset"],
        "Bullet 3": [r"checkpoint recovery|recovery", r"replay", r"30 s", r"partitioning|partitions", r"parallelism", r"2×|2x", r"Redis", r"PostgreSQL", r"tumbling", r"sliding", r"keyed state", r"at-least-once", r"idempotent"],
    },
    "04-llm-gateway-inference-router": {
        "Bullet 1": [r"unified", r"authenticat", r"rate limit", r"routing", r"stream", r"circuit breaker", r"multiple|several", r"adapter", r"token bucket", r"\bSSE\b|Server-Sent Events", r"fallback", r"timeout"],
        "Bullet 2": [r"Redis", r"cach", r"concurrent scheduling|scheduler", r"bulkhead", r"fair queu", r"2×|2x", r"25%", r"p95", r"50\+? concurrent"],
        "Bullet 3": [r"token", r"cost", r"Prometheus", r"Grafana", r"Kubernetes", r"autoscal", r"p99", r"error rate", r"\bKafka\b", r"aggregator"],
    },
}

report = {}
fail = False
for slug in RESUME_TERMS:
    pdf = os.path.join(FINAL, slug + ".pdf")
    md = open(os.path.join(SRC, slug + ".md"), encoding="utf8").read()
    doc = pymupdf.open(pdf)
    r = {"pages": doc.page_count}
    fonts, unembedded, images, blank, overflow = set(), set(), 0, [], []
    for i, page in enumerate(doc):
        for f in page.get_fonts(full=True):
            fonts.add(f[3])
            # Type3 fonts carry their glyph outlines inline (Chrome uses them for some symbols).
            if f[2] != "Type3" and (not f[1] or f[1] == "n/a"):
                unembedded.add(f[3])
        images += len(page.get_images(full=True))
        text = page.get_text().strip()
        if len(text) < 40 and not page.get_drawings():
            blank.append(i + 1)
        if i == 0:
            continue  # the cover is full-bleed by design
        for b in page.get_text("blocks"):
            x0, y0, x1, y1 = b[:4]
            if x0 < 17 * MM or x1 > page.rect.width - 17 * MM:
                overflow.append((i + 1, round(x0 / MM), round(x1 / MM), b[4][:40]))
    links_bad = []
    for i, page in enumerate(doc):
        for l in page.get_links():
            if l.get("kind") in (pymupdf.LINK_GOTO, pymupdf.LINK_NAMED) and l.get("page", 0) < 0:
                links_bad.append(i + 1)
    toc_links = sum(1 for l in doc[0].get_links())
    full = "\n".join(p.get_text() for p in doc)
    figs = sorted(set(int(n) for n in re.findall(r"Figure (\d+) —", full)))
    mermaid_blocks = len(re.findall(r"^```mermaid", md, flags=re.M))
    q = {lvl: len(re.findall(rf"^#### Q\d+\..*\[{lvl}\]\s*$", md, flags=re.M)) for lvl in ("Easy", "Medium", "Hard")}
    concepts = len(re.findall(r"^- \*\*Simple:\*\*", md, flags=re.M))
    coverage = {}
    for bullet, terms in RESUME_TERMS[slug].items():
        missing = [t for t in terms if not re.search(t, md, flags=re.I)]
        coverage[bullet] = {"terms": len(terms), "missing": missing}
    words = len(re.findall(r"[A-Za-z0-9']+", re.sub(r"```mermaid.*?```", "", md, flags=re.S)))
    r.update(
        fonts_embedded=not unembedded, raster_images=images, blank_pages=blank,
        overflow=overflow[:5], bad_links=links_bad, cover_links=toc_links,
        figures_in_pdf=figs, mermaid_blocks=mermaid_blocks, questions=q,
        concepts=concepts, coverage=coverage, words=words,
    )
    ok = (doc.page_count <= 30 and not unembedded and images == 0 and not blank and not overflow
          and not links_bad and len(figs) == mermaid_blocks and all(not c["missing"] for c in coverage.values()))
    r["status"] = "PASS" if ok else "CHECK"
    fail |= not ok
    report[slug] = r
    print(f"\n== {slug}: {r['status']}")
    print(f"   pages {doc.page_count} (limit 30, target <= 25) | words {words} | figures {figs} / mermaid {mermaid_blocks}")
    print(f"   questions {q} = {sum(q.values())} | concept cards {concepts} | cover links {toc_links}")
    print(f"   fonts embedded {not unembedded} | raster images {images} | blank {blank} | overflow {overflow[:3]} | bad links {links_bad}")
    for b, c in coverage.items():
        print(f"   {b}: {c['terms'] - len(c['missing'])}/{c['terms']} terms" + (f"  MISSING {c['missing']}" if c["missing"] else ""))
    # grayscale contact sheet
    cols, rows, tw, th = 7, 4, 248, 351
    sheet = pymupdf.Pixmap(pymupdf.csGRAY, pymupdf.IRect(0, 0, cols * (tw + 8) + 8, rows * (th + 8) + 8), False)
    sheet.clear_with(170)
    for k in range(min(doc.page_count, cols * rows)):
        z = tw / doc[k].rect.width
        pix = doc[k].get_pixmap(matrix=pymupdf.Matrix(z, z), colorspace=pymupdf.csGRAY)
        pix.set_origin(8 + (k % cols) * (tw + 8), 8 + (k // cols) * (th + 8))
        sheet.copy(pix, pix.irect)
    sheet.save(os.path.join(OUT, f"{slug}-sheet.png"))


# Stranded headings: the last text line on a page must not be a heading (part title,
# section heading, concept-card title or question).
def stranded(doc):
    H = doc[0].rect.height
    out = []
    for i, p in enumerate(doc):
        if i == 0:
            continue
        L = []
        for b in p.get_text("dict")["blocks"]:
            for l in b.get("lines", []):
                sp = [s for s in l["spans"] if s["text"].strip()]
                if sp and l["bbox"][3] < H - 16 * MM:
                    L.append((l["bbox"][3], sp[0]))
        if L:
            y, s0 = max(L, key=lambda t: t[0])
            if "SourceSans" in s0["font"] and (s0["flags"] & 16) and s0["size"] >= 10.5 and not re.fullmatch(r"\d+\.\s*", s0["text"]):
                out.append((i + 1, s0["text"][:40]))
    return out

for slug in RESUME_TERMS:
    st = stranded(pymupdf.open(os.path.join(FINAL, slug + ".pdf")))
    report[slug]["stranded_headings"] = st
    print(f"{slug}: stranded headings {st or 'none'}")
    if st:
        fail = True

json.dump(report, open(os.path.join(OUT, "qa-report.json"), "w"), indent=2)
print("\nOVERALL:", "PASS" if not fail else "CHECK ITEMS ABOVE")
sys.exit(1 if fail else 0)
