#!/usr/bin/env node
// Builds the four concise SDE Project Interview Playbooks:
//   final-playbooks/src/*.md -> HTML (markdown-it) -> Mermaid SVGs (neutral theme,
//   white background) -> compact book HTML + CSS -> PDF (headless Chrome, two passes
//   so the cover contents carries real page numbers).
//
// Usage:  node playbooks.mjs        (all four)
//         node playbooks.mjs 02     (only playbooks whose slug starts with "02")
//
// Markdown conventions (see final-playbooks/README.md):
//   # Title — Subtitle            first line: cover title
//   *italic paragraph*            cover description
//   # Part N: Title               part banner (flows; no blank opener page)
//   ## N.N Title                  numbered section (listed in the cover contents)
//   ### Concept                   + list with **Simple / Technical / Why / In this project / Interview angle** -> concept card
//   #### Q7. Question [Easy]      question heading with a difficulty badge
//   > **Label:** text             information box (label decides the box style)
//   **Figure N — caption** + ```mermaid  -> numbered figure
//   <div class="page-break"></div> explicit page break

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer';
import { renderMermaid } from '@mermaid-js/mermaid-cli';
import { motifSvg } from './books.mjs';

const BUILD = path.dirname(fileURLToPath(import.meta.url));
const PREP = path.resolve(BUILD, '..');
const FINAL = path.join(PREP, 'final-playbooks');
const SRC = path.join(FINAL, 'src');
const OUT = path.join(BUILD, 'out', 'playbooks');
const CACHE = path.join(BUILD, 'cache', 'mermaid');
const PY = path.join(BUILD, '.venv', 'bin', 'python');
const CHROME = process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const MERMAID_CONFIG = JSON.parse(fs.readFileSync(path.join(BUILD, 'mermaid-config.json'), 'utf8'));

export const PLAYBOOKS = [
  { slug: '01-payment-transaction-processing', no: 1, short: 'Payment & Transaction Processing', theme: 'Financial correctness · Transaction processing', accent: '#2E4657', tint: '#EAF0F4', motif: 'ledger' },
  { slug: '02-distributed-object-storage', no: 2, short: 'Distributed Object Storage', theme: 'Distributed storage · Replication', accent: '#3B3F55', tint: '#ECEDF3', motif: 'ring' },
  { slug: '03-real-time-event-processing', no: 3, short: 'Real-Time Event Processing', theme: 'Stream processing · Event time', accent: '#34493F', tint: '#EBF1EE', motif: 'stream' },
  { slug: '04-llm-gateway-inference-router', no: 4, short: 'LLM Gateway & Inference Router', theme: 'AI inference · Request routing', accent: '#47404F', tint: '#F0EEF4', motif: 'router' },
];

// ---- Page geometry (CSS px: 96 per inch) -----------------------------------
const MM = 96 / 25.4;
const CONTENT_W = 174 * MM; // A4 210mm - 2 x 18mm
const MAX_FIG_H = 150 * MM; // a figure never takes more than ~60% of a page
const MAX_SCALE = 0.78; // 16px Mermaid text -> ~9.4pt on paper
const MIN_LABEL_PT = 7.6;

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

function slugger() {
  const seen = new Map();
  return (text, prefix = '') => {
    let s = prefix + String(text).toLowerCase().replace(/<[^>]+>/g, '').replace(/[^a-z0-9\s-]/g, '').trim().replace(/\s+/g, '-').replace(/-+/g, '-');
    if (s.length > 60) s = s.slice(0, 60).replace(/-$/, '');
    const n = seen.get(s) || 0;
    seen.set(s, n + 1);
    return n ? `${s}-${n}` : s;
  };
}

function makeMd(sink) {
  const md = new MarkdownIt({ html: true, linkify: false, typographer: false });
  md.renderer.rules.fence = (tokens, idx) => {
    const t = tokens[idx];
    const lang = (t.info || '').trim().split(/\s+/)[0];
    if (lang === 'mermaid') {
      sink.push(t.content);
      return `<div class="mermaid-src" data-i="${sink.length - 1}"></div>\n`;
    }
    return `<div class="code"><pre><code>${esc(t.content.replace(/\n$/, ''))}</code></pre></div>\n`;
  };
  return md;
}

async function renderDiagrams(sources, browser) {
  fs.mkdirSync(CACHE, { recursive: true });
  const cfg = {
    ...MERMAID_CONFIG,
    themeCSS: 'g, rect, path, polygon, circle, ellipse, line, .node, .cluster, .actor, .note { filter: none !important; }',
  };
  const key = crypto.createHash('sha256').update(JSON.stringify(cfg)).digest('hex').slice(0, 12);
  const results = [];
  for (const src of sources) {
    const h = crypto.createHash('sha256').update(key + src).digest('hex').slice(0, 20);
    const file = path.join(CACHE, h + '.svg');
    if (!fs.existsSync(file)) {
      const { data } = await renderMermaid(browser, src, 'svg', { backgroundColor: 'white', mermaidConfig: cfg, svgId: 'd' + h.slice(0, 8) });
      fs.writeFileSync(file, Buffer.from(data));
    }
    let svg = fs.readFileSync(file, 'utf8');
    svg = svg.replace(/filter:\s*drop-shadow\([^;"}]*\)\s*;?/g, '').replace(/\sfilter="url\([^)]*\)"/g, '');
    const vb = svg.match(/viewBox="([-\d.]+)\s+([-\d.]+)\s+([\d.]+)\s+([\d.]+)"/);
    if (!vb) throw new Error('Mermaid SVG without viewBox');
    const w = parseFloat(vb[3]), hgt = parseFloat(vb[4]);
    svg = svg.replace(/<svg\b[^>]*>/, (tag) =>
      tag.replace(/\s(width|height)="[^"]*"/g, '').replace(/max-width:\s*[\d.]+px;?/, '').replace(/^<svg/, `<svg width="${w}" height="${hgt}"`));
    results.push({ svg, w, h: hgt, hash: h });
  }
  return results;
}

const BOX_RULES = [
  [/^(Say it aloud|Spoken script)/i, 'say', 'Say it aloud'],
  [/^Analogy/i, 'analogy', 'Analogy'],
  [/^(Example|Worked example|Concrete example)/i, 'example', 'Example'],
  [/^(Remember|Key point|Must remember)/i, 'remember', 'Remember'],
  [/^(Resume flag|Honesty|Caution|Careful|Evidence status)/i, 'caution', null],
  [/^(Interview tip|Tip|If asked)/i, 'tip', 'Interview tip'],
  [/^Note/i, 'note', 'Note'],
];

function assemble(book, mdText, diagrams) {
  const sink = [];
  const html = makeMd(sink).render(mdText);
  const $ = cheerio.load(`<main>${html}</main>`);
  const main = $('main');
  const slug = slugger();
  const log = [];

  // Title + description -> cover
  const h1 = main.children('h1').first();
  const [mainTitle, subTitle] = h1.text().trim().split(/\s+—\s+/);
  h1.remove();
  let description = '';
  const firstP = main.children().first();
  if (firstP.is('p') && firstP.children('em').length) { description = firstP.html(); firstP.remove(); }

  // Anything between the description and Part 1 (the resume bullets) goes on the cover.
  let coverExtra = '';
  while (main.children().length && !main.children().first().is('h1')) {
    const n = main.children().first();
    coverExtra += $.html(n);
    n.remove();
  }

  const parts = [];
  let curPart = null;
  let figNo = 0;
  const qStats = { Easy: 0, Medium: 0, Hard: 0 };
  let concepts = 0;

  // Part banners
  main.children('h1').each((_, el) => {
    const t = $(el).text().trim();
    const m = t.match(/^Part (\d+):\s*(.*)$/);
    if (!m) throw new Error(`${book.slug}: unexpected h1 "${t}"`);
    const id = `part-${m[1]}`;
    curPart = { no: +m[1], title: m[2], id, sections: [] };
    parts.push(curPart);
    $(el).replaceWith(`<div class="part-banner" id="${id}" data-part="${m[1]}"><span class="part-kicker">Part ${m[1]}</span><h1 class="part-title">${esc(m[2])}</h1></div>`);
  });

  // Sections (h2) -> ids, numbers, contents entries (attach to the preceding part)
  let partIdx = -1;
  main.children().each((_, el) => {
    if ($(el).hasClass('part-banner')) { partIdx++; return; }
    if (el.tagName !== 'h2') return;
    const t = $(el).text().trim();
    const id = slug(t, 's-');
    $(el).attr('id', id);
    const m = $(el).html().match(/^(\d+\.\d+)\s+/);
    if (m) $(el).html(`<span class="secnum">${m[1]}</span>${$(el).html().slice(m[0].length)}`);
    if (partIdx < 0) throw new Error(`${book.slug}: section before first part: ${t}`);
    parts[partIdx].sections.push({ id, num: m ? m[1] : '', title: m ? t.slice(m[0].length) : t });
  });

  // Concept cards: h3 + ul with Simple/Technical leads
  main.children('h3').each((_, el) => {
    const ul = $(el).next();
    if (!ul.is('ul')) return;
    const leads = ul.children('li').toArray().map((li) => $(li).children('strong').first().text().trim().replace(/:$/, ''));
    if (!(leads.includes('Simple') && leads.includes('Technical'))) return;
    concepts++;
    ul.children('li').each((__, li) => {
      const s = $(li).children('strong').first();
      const lab = s.text().trim().replace(/:$/, '');
      s.replaceWith(`<span class="cl">${esc(lab)}</span>`);
    });
    const head = $(el).html();
    ul.replaceWith(`<div class="concept"><div class="concept-head"><span class="concept-no">${concepts}</span><span class="concept-name">${head}</span></div><ul class="concept-list">${ul.html()}</ul></div>`);
    $(el).remove();
  });

  // Questions
  main.find('h4').each((_, el) => {
    const h = $(el).html().trim();
    const m = h.match(/^Q(\d+)\.\s+([\s\S]*?)\s*\[(Easy|Medium|Hard)\]$/);
    if (!m) return;
    qStats[m[3]]++;
    $(el).addClass('question').attr('id', `q${m[1]}`).html(
      `<span class="qnum">Q${m[1]}</span><span class="qtext">${m[2]}</span><span class="qlevel q-${m[3].toLowerCase()}">${m[3]}</span>`);
  });

  // Labelled paragraphs
  main.find('p').each((_, el) => {
    const s = $(el).children('strong').first();
    if (!s.length || ($(el).html() || '').trim().indexOf('<strong>') !== 0) return;
    const lab = s.text().trim();
    if (/^Related follow-up/i.test(lab)) $(el).addClass('followup');
    else if (/^Diagram to draw/i.test(lab)) $(el).addClass('draw');
  });

  // Blockquotes -> boxes
  main.find('blockquote').each((_, el) => {
    const $b = $(el);
    const firstStrong = $b.find('p').first().children('strong').first();
    const lead = firstStrong.length && ($b.find('p').first().html() || '').trim().indexOf('<strong>') === 0 ? firstStrong.text().trim() : '';
    let kind = 'note', label = 'Note';
    for (const [re, k, l] of BOX_RULES) {
      if (re.test(lead)) { kind = k; label = l || lead.replace(/[:.]$/, ''); break; }
    }
    if (lead && BOX_RULES.some(([re]) => re.test(lead))) {
      const p = $b.find('p').first();
      firstStrong.remove();
      p.html((p.html() || '').replace(/^\s*[:.—–-]?\s*/, ''));
      if (!p.text().trim()) p.remove();
    }
    const textLen = $b.text().length;
    $b.replaceWith(`<div class="box box-${kind}${textLen < (kind === 'say' ? 700 : 1500) ? ' keep' : ''}"><div class="box-label">${esc(label)}</div>${$b.html()}</div>`);
  });

  // Figures
  main.find('div.mermaid-src').each((_, el) => {
    const i = +$(el).attr('data-i');
    const d = diagrams[i];
    const prev = $(el).prev();
    let cap = '';
    let hint = MAX_SCALE;
    figNo++;
    if (prev.is('p') && /^Figure\s/.test(prev.text().trim()) && prev.children('strong').length) {
      cap = prev.children('strong').html().replace(/^Figure\s+[\w.]+\s*[—–-]\s*/, '');
      const hm = prev.text().match(/\[scale=([\d.]+)\]\s*$/);
      if (hm) hint = +hm[1];
      prev.remove();
    } else console.warn(`  ! uncaptioned diagram ${figNo}`);
    const scale = Math.min(MAX_SCALE, hint, CONTENT_W / d.w, MAX_FIG_H / d.h);
    const label = 12 * scale;
    log.push(`    Figure ${figNo}  ${Math.round(d.w)}x${Math.round(d.h)}  scale ${scale.toFixed(2)}  label ≈ ${label.toFixed(1)}pt${label < MIN_LABEL_PT ? '  << TOO SMALL' : ''}`);
    const file = `diagrams/figure-${figNo}.svg`;
    d.file = file;
    $(el).replaceWith(`<figure class="diagram" id="fig-${figNo}"><img src="${file}" alt="Figure ${figNo}" style="width:${(d.w * scale).toFixed(1)}px"><figcaption><span class="fig-num">Figure ${figNo}</span><span class="fig-sep"> — </span>${cap}</figcaption></figure>`);
  });

  // Tables
  main.find('table').each((_, el) => {
    const rows = $(el).find('tr').length;
    const cols = $(el).find('tr').first().children().length;
    const keep = rows <= 12 && $(el).text().length < 420;
    $(el).wrap(`<div class="table-block${keep ? ' keep' : ''}${cols >= 5 ? ' wide' : ''}"></div>`);
  });

  // Keep headings with what follows (Chrome does not always honour break-after: avoid):
  // a section heading with a short first block or a question heading; a part banner with
  // the section heading below it. Long paragraphs are never wrapped (that would act like
  // "keep the whole answer together" and leave gaps).
  const small = (n) => n.length && ((n.is('p') && n.text().length < 420) || n.is('h3') || n.is('h4.question') || (n.is('.box.keep') && n.text().length < 600));
  main.children('h2').each((_, el) => {
    const nx = $(el).next();
    if (small(nx)) { $(el).before('<div class="keephead"></div>'); const w = $(el).prev(); w.append($(el)); w.append(nx); }
  });
  main.children('.part-banner').each((_, el) => {
    const nx = $(el).next();
    if (nx.is('h2') || nx.hasClass('keephead')) { $(el).before('<div class="keephead"></div>'); const w = $(el).prev(); w.append($(el)); w.append(nx); }
  });

  // Inline code: allow breaks in long identifiers
  main.find('code').each((_, el) => {
    if ($(el).closest('.code').length) return;
    const t = $(el).text();
    if (t.length < 18) return;
    $(el).html(t.split(/(?<=[./_(,=:>\-])/).map(esc).join('<wbr>'));
  });

  // ---- Cover with contents --------------------------------------------------
  const pn = (id) => `<span class="pn" data-t="${id}">000</span>`;
  const entry = (cls, id, label) => `<div class="toc-entry ${cls}"><a href="#${id}"><span class="toc-text">${label}</span></a><span class="leader"></span>${pn(id)}</div>`;
  let toc = '';
  for (const p of parts) {
    toc += `<div class="toc-part-block">${entry('toc-part', p.id, `<span class="toc-num">Part ${p.no}</span>${esc(p.title)}`)}`;
    for (const s of p.sections) toc += entry('toc-sec', s.id, `<span class="toc-snum">${esc(s.num)}</span>${esc(s.title)}`);
    toc += `</div>`;
  }
  const series = PLAYBOOKS.map((b) => `<li class="${b.no === book.no ? 'current' : ''}"><span class="sq"></span>${b.no}. ${esc(b.short)}</li>`).join('');
  const qTotal = qStats.Easy + qStats.Medium + qStats.Hard;
  const cover = `
<section class="cover">
  <div class="cover-bar"></div>
  <div class="cover-inner">
    <div class="cover-top"><span>SDE Project Interview Playbook</span><span>Book ${book.no} of ${PLAYBOOKS.length}</span></div>
    <div class="cover-motif">${motifSvg(book.motif)}</div>
    <div class="cover-theme">${esc(book.theme)}</div>
    <h1 class="cover-title">${esc(mainTitle)}</h1>
    <div class="cover-sub">${esc(subTitle || 'Interview Playbook')}</div>
    <div class="cover-rule"></div>
    <p class="cover-desc">${description}</p>
    ${coverExtra ? `<div class="cover-extra">${coverExtra}</div>` : ''}
    <div class="cover-contents"><div class="cc-head">Contents</div><div class="toc">${toc}</div>
      <div class="cc-extra">${concepts} fundamentals explained · ${figNo} diagrams · ${qTotal} interview questions (${qStats.Easy} easy, ${qStats.Medium} medium, ${qStats.Hard} hard) · one-page revision sheet</div></div>
    <div class="cover-bottom"><div class="series-head">The series</div><ul class="series">${series}</ul></div>
  </div>
</section>`;

  const doc = `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>${esc(mainTitle)} — Interview Playbook</title>
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-serif-4/400.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-serif-4/400-italic.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-serif-4/600.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-serif-4/700.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-sans-3/400.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-sans-3/400-italic.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-sans-3/600.css">
<link rel="stylesheet" href="../../../node_modules/@fontsource/source-sans-3/700.css">
<link rel="stylesheet" href="../../../styles/playbook.css">
<style>
:root { --accent: ${book.accent}; --tint: ${book.tint}; }
@page { @top-left { content: "Book ${book.no} · ${book.short.replace(/"/g, '\\"')}"; } }
</style>
</head>
<body class="book-${book.no}">
${cover}
<div class="body">${main.html()}</div>
</body></html>`;

  return { html: doc, log, stats: { concepts, figures: figNo, ...qStats, questions: qTotal }, parts };
}

async function printPdf(browser, htmlFile, pdfFile) {
  const page = await browser.newPage();
  await page.goto('file://' + htmlFile, { waitUntil: 'networkidle0', timeout: 0 });
  await page.evaluate(async () => {
    await document.fonts.ready;
    await Promise.all([...document.images].map((img) => (img.complete ? null : new Promise((r) => { img.onload = img.onerror = r; }))));
  });
  const broken = await page.evaluate(() => [...document.images].filter((i) => !i.naturalWidth).map((i) => i.src));
  if (broken.length) throw new Error('Broken images: ' + broken.join(', '));
  await page.pdf({ path: pdfFile, preferCSSPageSize: true, printBackground: true, outline: true, tagged: true, timeout: 0 });
  await page.close();
}

const pageMap = (pdf) => JSON.parse(execFileSync(PY, [path.join(BUILD, 'pagemap.py'), pdf], { encoding: 'utf8' }));

function fillPageNumbers(html, map) {
  const missing = [];
  const res = html.replace(/<span class="pn" data-t="([^"]+)">000<\/span>/g, (_, id) => {
    if (!(id in map)) missing.push(id);
    return `<span class="pn" data-t="${id}">${map[id] ?? '?'}</span>`;
  });
  if (missing.length) throw new Error('Unresolved contents targets: ' + missing.join(', '));
  return res;
}

async function main() {
  const filter = process.argv[2];
  const books = PLAYBOOKS.filter((b) => !filter || b.slug.startsWith(filter));
  const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--font-render-hinting=none'] });
  const report = [];
  try {
    for (const book of books) {
      console.log(`\n=== ${book.slug}`);
      const mdText = fs.readFileSync(path.join(SRC, book.slug + '.md'), 'utf8');
      const sources = [];
      makeMd(sources).render(mdText);
      const diagrams = await renderDiagrams(sources, browser);
      const dir = path.join(OUT, book.slug);
      fs.rmSync(dir, { recursive: true, force: true });
      fs.mkdirSync(path.join(dir, 'diagrams'), { recursive: true });
      const A = assemble(book, mdText, diagrams);
      for (const d of diagrams) fs.writeFileSync(path.join(dir, d.file), d.svg);
      console.log(A.log.join('\n'));
      const htmlFile = path.join(dir, 'index.html');
      fs.writeFileSync(htmlFile, A.html);
      const pass1 = path.join(dir, 'pass1.pdf');
      await printPdf(browser, htmlFile, pass1);
      const map1 = pageMap(pass1);
      fs.writeFileSync(htmlFile, fillPageNumbers(A.html, map1));
      const pdfFile = path.join(FINAL, book.slug + '.pdf');
      await printPdf(browser, htmlFile, pdfFile);
      const map2 = pageMap(pdfFile);
      const drift = Object.keys(map1).filter((k) => map1[k] !== map2[k]);
      if (drift.length) throw new Error('Page numbers drifted between passes: ' + drift.slice(0, 5).join(', '));
      fs.rmSync(pass1);
      const pages = map2.__pages;
      const partPages = Object.fromEntries(A.parts.map((p) => [p.no, map2[p.id]]));
      console.log(`  PDF: ${path.relative(PREP, pdfFile)}  (${pages} pages)  parts start at ${JSON.stringify(partPages)}`);
      console.log(`  ${A.stats.concepts} concepts, ${A.stats.figures} figures, Q: ${A.stats.Easy} easy / ${A.stats.Medium} medium / ${A.stats.Hard} hard`);
      if (pages > 30) console.log('  !!! OVER THE 30-PAGE LIMIT');
      report.push({ book: book.slug, pages, partPages, ...A.stats });
    }
  } finally {
    await browser.close();
  }
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, 'build-report.json'), JSON.stringify(report, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
