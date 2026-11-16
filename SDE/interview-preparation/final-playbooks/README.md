# SDE Project Interview Playbooks

Four concise booklets (25 A4 pages each) that prepare one resume project each for a
30–45 minute SDE interview. They replace the 140–160-page study guides for day-to-day
preparation; the long guides in `../pdfs/` remain available as deep reference.

| PDF | Source Markdown |
|---|---|
| `01-payment-transaction-processing.pdf` | `src/01-payment-transaction-processing.md` |
| `02-distributed-object-storage.pdf` | `src/02-distributed-object-storage.md` |
| `03-real-time-event-processing.pdf` | `src/03-real-time-event-processing.md` |
| `04-llm-gateway-inference-router.pdf` | `src/04-llm-gateway-inference-router.md` |

Every booklet has the same six parts: (1) project fundamentals, built from the exact
resume bullets, one concept card per term (simple / technical / why it matters / in this
project / interview angle); (2) high-level design (stack table, simple and detailed HLD
diagrams, components, data flow, storage, scaling, failures, key decisions); (3) how the
project works (end-to-end flow, three alternative scenarios, guarantees and limitations);
(4) 2-, 5- and 10-minute spoken explanations; (5) 28 interview questions with teaching
answers (18 easy, 9 medium, 1 hard); (6) a one-page revision sheet. The cover lists the
resume bullets the booklet defends and a linked table of contents.

Technical content comes from the project documents in `../../0N-*/` and the resume. All
resume numbers are presented as benchmark targets, never as measured results. Each
booklet ends Part 1 with a "Resume flag" box listing wording to state precisely.

## Rebuilding

The Markdown in `src/` is the source of truth. After editing it:

```sh
cd ../build
node playbooks.mjs              # all four (or: node playbooks.mjs 03)
.venv/bin/python qa_playbooks.py
```

`qa_playbooks.py` checks page count (hard limit 30, target ≤ 25), embedded fonts, vector-only
diagrams, internal links, text overflow, stranded headings, figure captions, question
counts, and that every key term of each resume bullet appears. It also writes grayscale
contact sheets to `../build/out/playbooks/qa/`. `pagefill.py <pdf>` prints how full each
page is, which helps when a figure leaves a gap.

Markdown conventions understood by `playbooks.mjs`: `# Part N: Title` part banners;
`## N.N Title` sections (listed on the cover); `### Name` followed by a list with
**Simple / Technical / Why it matters / In this project / Interview angle** becomes a
concept card; `#### QN. Question [Easy|Medium|Hard]`; `> **Label:**` boxes (Analogy,
Example, Say it aloud, Resume flag, Note); `**Figure N — caption** [scale=0.72]` before a
Mermaid block (the optional scale caps the diagram size; labels stay ≥ 8 pt);
`<div class="page-break"></div>`; `<div class="rev2">` for the two-column revision sheet.
