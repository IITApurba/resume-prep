# DATA + QUANT playbook build

Builds the nine interview playbooks in `../DATA/*/` and `../QUANT/*/` from each project's `README.md`.
It is a copy of the SDE playbook toolchain (`../SDE/interview-preparation/build`, which is never read
from or written to at build time), extended with KaTeX equations and two series.

```sh
node playbooks.mjs              # all nine
node playbooks.mjs DATA         # one series
node playbooks.mjs QUANT/03     # one book (series/slug prefix)
.venv/bin/python qa.py          # QA report + grayscale contact sheets -> out/qa/
.venv/bin/python pagefill.py ../QUANT/03-market-impact-optimal-execution/03-market-impact-optimal-execution.pdf
```

Pipeline: markdown-it (+ a small `$…$` / `$$…$$` plugin rendered with KaTeX at build time) → Mermaid
SVGs (neutral theme, white background, cached in `cache/mermaid/`) → book HTML with cover, contents,
concept cards, question badges and labelled boxes → headless Chrome PDF, printed twice so the cover
contents carries real page numbers. Styles: `styles/playbook.css` (A4, 18 mm margins, 10.5 pt body,
grayscale-safe). Cover motifs: `motifs.mjs`. The build warns about overflowing equations or tables
and about Part 2 headings that did not become concept cards.

Markdown conventions: `# Part N: Title`; `## N.N Title`; `### Concept` followed by a list with
**Simple / Technical / Math / Why it matters / In this project / Interview angle**; `#### QN. Question [Easy|Medium|Hard]`;
`> **Label:**` boxes (Say it aloud, Example, Evidence status, Resume flag, Confirm, Note);
`**Figure N — caption** [scale=0.72]` before a Mermaid block; `<div class="page-break"></div>`;
`<div class="rev2">` for the two-column revision sheet.

Setup (already done): `node_modules` cloned from the SDE build; `python3 -m venv .venv && .venv/bin/pip install pymupdf`.
Chrome is expected at `/Applications/Google Chrome.app` (override with `CHROME_PATH`).
