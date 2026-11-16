# Study-book PDF build

Regenerates the four print-ready PDFs in `../pdfs/` from the Markdown study guides in
`../` (the Markdown files are read-only inputs and are never modified).

## Pipeline

1. `build.mjs` parses each Markdown file with markdown-it.
2. Every ```` ```mermaid ```` block is rendered to SVG with Mermaid CLI
   (`@mermaid-js/mermaid-cli`, **neutral theme, white background**, settings in
   `mermaid-config.json`, equivalent to `mmdc -t neutral -b white`). Drop shadows are
   disabled so Chrome keeps the diagrams as pure vectors, and sequence-diagram notes use a
   light-grey fill with dark text instead of dark grey with white text. Rendered SVGs are
   cached in `cache/mermaid/`, keyed by a hash of the source and config.
3. The HTML is restructured into a book: cover, contents, list of figures, preface,
   part openers, chapter openers with a mini table of contents, numbered tables,
   figure captions, and information boxes (concept / say it aloud / technical detail /
   remember this / interview tip / caution / worked example / note). Each box type has
   its own label and border style, so it stays distinguishable in black and white.
4. Diagram placement: inline at ≤ 82 % scale (≈ 9.8 pt labels). If the label size would
   fall below 8 pt, a wide diagram gets a rotated landscape page and a tall diagram gets
   a dedicated page at the largest scale that fits.
5. Headless Chrome prints the PDF twice. Pass 1 uses placeholder page numbers. The
   page of every heading and figure is then read back from the PDF's named destinations
   (`pagemap.py`), and pass 2 prints the final PDF with the real numbers. The build
   fails if any page reference changes between the passes.

Styles: `styles/book.css` (A4, 20 mm margins, running headers via CSS named pages,
grayscale-safe palette). Per-book identity (accent tone, cover motif, short title) is in
`books.mjs`.

## Commands

```sh
npm install                      # once (set PUPPETEER_SKIP_DOWNLOAD=1 to use system Chrome)
python3 -m venv .venv && .venv/bin/pip install pymupdf   # once, for page-map + QA
node build.mjs                   # all four books
node build.mjs 03                # just one book (slug prefix)
.venv/bin/python qa.py           # QA report + grayscale contact sheets -> out/qa/
.venv/bin/python render_pages.py ../pdfs/<book>.pdf out/pages --gray --dpi 110 12 40-45
```

Chrome is expected at `/Applications/Google Chrome.app` (override with `CHROME_PATH`).
Fonts: Source Serif 4 (body), Source Sans 3 (headings, tables), Menlo (code). They come
from `@fontsource` packages and macOS.

`out/` holds the intermediate HTML/SVG for each book and the QA output. It is regenerated
on every build.

## Interview playbooks (concise 25-page booklets)

`playbooks.mjs` builds the four booklets in `../final-playbooks/` from
`../final-playbooks/src/*.md` with the same toolchain (Mermaid CLI, headless Chrome,
two-pass page numbers) and a compact stylesheet, `styles/playbook.css` (A4, 18 mm
margins, 10.5 pt body, no opener pages). See `../final-playbooks/README.md`.

```sh
node playbooks.mjs [01|02|03|04]
.venv/bin/python qa_playbooks.py     # limits, fonts, links, stranded headings, resume-term coverage
.venv/bin/python pagefill.py ../final-playbooks/<book>.pdf   # per-page fill, to find gaps
```
