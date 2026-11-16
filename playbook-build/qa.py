"""Automated QA for the DATA and QUANT interview playbooks.

Checks, per PDF:
  * page count (hard limit 25; ideal 12-18, acceptable <= 20)
  * all fonts embedded, no raster images (diagrams and equations stay vector), no blank pages
  * no text outside the 18 mm text column (pages 2+)
  * every cover-contents link resolves to a page
  * figure captions present; question counts by difficulty; answer lengths; concept-card count
  * README <-> PDF consistency: every concept, question and section heading of the README is in the PDF
  * resume coverage: key terms of every resume bullet appear in the README
  * stranded headings (a heading as the last line of a page)
Writes grayscale contact sheets and qa-report.json to out/qa/.

usage: .venv/bin/python qa.py
"""
import json
import os
import re
import sys

import pymupdf

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(HERE, "out", "qa")
os.makedirs(OUT, exist_ok=True)
MM = 72 / 25.4

BOOKS = {
    "DATA/01-risk-aware-process-reward-modeling": {
        "Bullet 1": [r"AffordRanker", r"process reward model", r"Qwen2\.5-7B-Instruct", r"step-level", r"safety", r"coherence", r"trajector"],
        "Bullet 2": [r"208K", r"15K", r"GPT-4o", r"DeepSpeed", r"ZeRO-2", r"3 GPUs", r"0\.88", r"0\.82", r"\bF1\b"],
        "Bullet 3": [r"reward-guided decoding", r"vLLM", r"5 policy|five policy", r"43 points", r"jailbreak", r"\bASR\b", r"50%"],
    },
    "DATA/02-llm-post-training-sft-dpo": {
        "Bullet 1": [r"\bSFT\b", r"\bDPO\b", r"1\.5B", r"causal", r"50K", r"20K", r"preference pair", r"\bTRL\b"],
        "Bullet 2": [r"LoRA", r"PEFT", r"8\.4M", r"3 GB", r"34 MB", r"W&B|Weights & Biases"],
        "Bullet 3": [r"67\.8%", r"win rate", r"GSM8K", r"MMLU", r"\+8\.3", r"\+3\.1", r"pairwise", r"LLM-as-a-Judge"],
    },
    "DATA/03-fraud-detection-risk-analysis": {
        "Bullet 1": [r"284K", r"0\.17%", r"XGBoost", r"0\.914", r"PR-AUC", r"0\.82", r"1% FPR|1% false-positive"],
        "Bullet 2": [r"\bRFM\b", r"temporal", r"velocity", r"fold-local SMOTE", r"Optuna", r"stratified 5-fold"],
        "Bullet 3": [r"SHAP", r"UMAP", r"threshold optimisation", r"\bPSI\b", r"87%", r"pytest", r"GitHub Actions", r"MLflow", r"LightGBM"],
    },
    "DATA/04-leakage-safe-time-series-forecasting": {
        "Bullet 1": [r"walk-forward", r"S&P 500", r"60-day", r"purg", r"AR\(1\)", r"ARIMA", r"XGBoost", r"\bGRU\b", r"BiLSTM"],
        "Bullet 2": [r"attention", r"ensemble", r"Optuna", r"\bMSE\b", r"8\.7%", r"horizon"],
        "Bullet 3": [r"transaction cost", r"position sizing", r"0\.78", r"Sharpe", r"9\.3%", r"drawdown", r"110%", r"turnover", r"embargo"],
    },
    "QUANT/01-limit-order-book-alpha-research": {
        "Bullet 1": [r"order-flow imbalance|\bOFI\b", r"microprice", r"queue depth", r"100\+", r"120[ -]tick"],
        "Bullet 2": [r"Logistic Regression", r"XGBoost", r"\bGRU\b", r"purged walk-forward", r"0\.66", r"\bAUC\b", r"0\.031", r"Information Coefficient"],
        "Bullet 3": [r"threshold", r"turnover", r"slippage", r"transaction cost", r"0\.71", r"OOS Sharpe|out-of-sample", r"7\.4%", r"drawdown", r"C\+\+", r"MLflow", r"Optuna"],
    },
    "QUANT/02-cross-sectional-alpha-discovery": {
        "Bullet 1": [r"point-in-time", r"500\+", r"momentum", r"mean reversion", r"volatility", r"liquidity", r"50\+"],
        "Bullet 2": [r"purged K-fold", r"embargo", r"neutrali[sz]", r"multiple-testing", r"Sharpe decay", r"18%"],
        "Bullet 3": [r"volatility-targeted|volatility target", r"turnover", r"exposure", r"cost", r"0\.82", r"11\.6%", r"Polars", r"SciPy", r"Statsmodels", r"PyTorch"],
    },
    "QUANT/03-market-impact-optimal-execution": {
        "Bullet 1": [r"Almgren-Chriss", r"square-root", r"\bVWAP\b", r"\bTWAP\b", r"\bPOV\b", r"spread", r"volatility", r"liquidity"],
        "Bullet 2": [r"queue-aware", r"partial fill", r"latency", r"30K\+", r"implementation shortfall", r"18%"],
        "Bullet 3": [r"adaptive participation", r"11 bps", r"95%\+", r"completion", r"Redis", r"C\+\+", r"NumPy", r"SciPy"],
    },
    "QUANT/04-portfolio-risk-stress-testing": {
        "Bullet 1": [r"500\+ positions", r"Value at Risk|\bVaR\b", r"Expected Shortfall", r"beta", r"marginal risk", r"factor exposure"],
        "Bullet 2": [r"historical", r"parametric", r"Monte Carlo", r"Ledoit-Wolf", r"18%", r"covariance"],
        "Bullet 3": [r"FastAPI", r"C\+\+", r"NumPy", r"scenario caching|cache", r"PostgreSQL", r"stress test", r"1\.5 s|1\.5s"],
    },
    "QUANT/05-fraud-detection-risk-analysis": {
        "Bullet 1": [r"284K", r"0\.17%", r"XGBoost", r"0\.914", r"PR-AUC", r"0\.82", r"1% FPR"],
        "Bullet 2": [r"\bRFM\b", r"temporal", r"velocity", r"fold-local SMOTE", r"Optuna", r"stratified 5-fold"],
        "Bullet 3": [r"SHAP", r"UMAP", r"\bPSI\b", r"87%", r"GitHub Actions", r"threshold", r"calibrat", r"expected cost|expected loss"],
    },
}

norm = lambda t: re.sub(r"\s+", " ", re.sub(r"[`*$\\]|\{|\}", "", t)).strip().lower()


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


report, fail = {}, False
for key, terms in BOOKS.items():
    series, slug = key.split("/")
    md_path = os.path.join(ROOT, series, slug, "README.md")
    pdf_path = os.path.join(ROOT, series, slug, slug + ".pdf")
    md = open(md_path, encoding="utf8").read()
    doc = pymupdf.open(pdf_path)
    r = {"pages": doc.page_count}
    unembedded, images, blank, overflow = set(), 0, [], []
    for i, page in enumerate(doc):
        for f in page.get_fonts(full=True):
            if f[2] != "Type3" and (not f[1] or f[1] == "n/a"):
                unembedded.add(f[3])
        images += len(page.get_images(full=True))
        if len(page.get_text().strip()) < 40 and not page.get_drawings():
            blank.append(i + 1)
        if i == 0:
            continue
        for b in page.get_text("blocks"):
            if b[0] < 17 * MM or b[2] > page.rect.width - 17 * MM:
                overflow.append((i + 1, b[4][:40]))
    bad_links = [i + 1 for i, p in enumerate(doc) for l in p.get_links() if l.get("kind") in (pymupdf.LINK_GOTO, pymupdf.LINK_NAMED) and l.get("page", 0) < 0]
    full = "\n".join(p.get_text() for p in doc)
    fulln = norm(full)
    figs = sorted(set(int(n) for n in re.findall(r"Figure (\d+) —", full)))
    mermaid = len(re.findall(r"^```mermaid", md, flags=re.M))
    q = {lvl: len(re.findall(rf"^#### Q\d+\..*\[{lvl}\]\s*$", md, flags=re.M)) for lvl in ("Easy", "Medium", "Hard")}
    # answer lengths
    ans = [(m.group(1), len(re.findall(r"[A-Za-z0-9']+", m.group(2)))) for m in re.finditer(r"^#### (Q\d+)\..*?\n\n(.*?)(?=\n#### |\n## |\n<div)", md, re.S | re.M)]
    short = [a for a in ans if a[1] < 90]
    concepts = len(re.findall(r"^- \*\*Simple:\*\*", md, flags=re.M))
    # README -> PDF consistency (headings)
    heads = re.findall(r"^#{2,4} (.+)$", md, flags=re.M)
    missing_heads = []
    for h in heads:
        h2 = re.sub(r"^Q\d+\.\s*|\s*\[(Easy|Medium|Hard)\]$|^\d+\.\d+\s+|^\d+\.\s+", "", h)
        if norm(h2)[:35] not in fulln:
            missing_heads.append(h[:50])
    coverage = {b: [t for t in ts if not re.search(t, md, flags=re.I)] for b, ts in terms.items()}
    st = stranded(doc)
    words = len(re.findall(r"[A-Za-z0-9']+", re.sub(r"```mermaid.*?```", "", md, flags=re.S)))
    ok = (doc.page_count <= 25 and not unembedded and images == 0 and not blank and not overflow and not bad_links
          and len(figs) == mermaid and all(not m for m in coverage.values()) and not st and not missing_heads)
    r.update(status="PASS" if ok else "CHECK", words=words, fonts_embedded=not unembedded, raster_images=images,
             blank_pages=blank, overflow=overflow[:5], bad_links=bad_links, figures=figs, mermaid=mermaid,
             questions=q, answers_under_90_words=short, answer_words_min=min(a[1] for a in ans), answer_words_mean=round(sum(a[1] for a in ans) / len(ans)),
             concepts=concepts, missing_headings_in_pdf=missing_heads, coverage_missing=coverage, stranded=st)
    report[key] = r
    fail |= not ok
    print(f"\n== {key}: {r['status']}")
    print(f"   pages {doc.page_count} (max 25) | words {words} | figures {figs}/{mermaid} | concept cards {concepts}")
    print(f"   questions {q} = {sum(q.values())} | answer words min {r['answer_words_min']} mean {r['answer_words_mean']} | short {short}")
    print(f"   fonts embedded {not unembedded} | raster {images} | blank {blank} | overflow {overflow[:2]} | bad links {bad_links} | stranded {st}")
    print(f"   README headings missing from PDF: {missing_heads or 'none'}")
    for b, m in coverage.items():
        print(f"   {b}: {len(terms[b]) - len(m)}/{len(terms[b])} terms" + (f"  MISSING {m}" if m else ""))
    cols, rows, tw, th = 7, 3, 248, 351
    sheet = pymupdf.Pixmap(pymupdf.csGRAY, pymupdf.IRect(0, 0, cols * (tw + 8) + 8, rows * (th + 8) + 8), False)
    sheet.clear_with(170)
    for k in range(min(doc.page_count, cols * rows)):
        z = tw / doc[k].rect.width
        pix = doc[k].get_pixmap(matrix=pymupdf.Matrix(z, z), colorspace=pymupdf.csGRAY)
        pix.set_origin(8 + (k % cols) * (tw + 8), 8 + (k // cols) * (th + 8))
        sheet.copy(pix, pix.irect)
    sheet.save(os.path.join(OUT, f"{series}-{slug}-sheet.png"))

json.dump(report, open(os.path.join(OUT, "qa-report.json"), "w"), indent=2)
print("\nOVERALL:", "PASS" if not fail else "CHECK ITEMS ABOVE")
sys.exit(1 if fail else 0)
