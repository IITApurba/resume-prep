#!/usr/bin/env node
// Builds the four print-ready study books:
//   Markdown -> HTML (markdown-it) -> Mermaid SVGs (mermaid-cli, neutral theme,
//   white background) -> book HTML + CSS -> PDF (headless Chrome, two passes so
//   that the table of contents carries real page numbers).
//
// Usage:  node build.mjs            (all four books)
//         node build.mjs 02         (only books whose slug starts with "02")
//
// The source Markdown files are read-only inputs and are never modified.

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';
import taskLists from 'markdown-it-task-lists';
import hljs from 'highlight.js';
import * as cheerio from 'cheerio';
import puppeteer from 'puppeteer';
import { renderMermaid } from '@mermaid-js/mermaid-cli';
import { BOOKS, motifSvg } from './books.mjs';

const BUILD = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(BUILD, '..');
const OUT = path.join(BUILD, 'out');
const CACHE = path.join(BUILD, 'cache', 'mermaid');
const PDF_DIR = path.join(ROOT, 'pdfs');
const PY = path.join(BUILD, '.venv', 'bin', 'python');
const CHROME = process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const MERMAID_CONFIG = JSON.parse(fs.readFileSync(path.join(BUILD, 'mermaid-config.json'), 'utf8'));

// ---- Page geometry (CSS px: 96 per inch) -----------------------------------
const MM = 96 / 25.4;
const CONTENT_W = 170 * MM; // A4 210mm - 2 x 20mm
const CONTENT_H = 257 * MM; // A4 297mm - 2 x 20mm
const MAX_SCALE = 0.82; // 16px Mermaid text -> ~9.8pt on paper
const MIN_SCALE = 8 / 12; // 16px (=12pt) Mermaid text must stay >= 8pt
const PORTRAIT_FIG_H = CONTENT_H - 105; // leave room for caption + spacing
const FULLPAGE_FIG_H = CONTENT_H - 48; // caption ≈ 36px; bottom margin is truncated at the page end
const LAND_W = 253 * MM, LAND_H = 166 * MM - 60; // rotated frame minus caption

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const cssStr = (s) => '"' + String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"';
const NUM_WORDS = ['Zero', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine'];

function slugger() {
  const seen = new Map();
  return (text, prefix = '') => {
    let s = prefix + String(text).toLowerCase().replace(/<[^>]+>/g, '').replace(/[^a-z0-9\s-]/g, '').trim().replace(/\s+/g, '-').replace(/-+/g, '-');
    if (!s) s = prefix + 'x';
    const n = seen.get(s) || 0;
    seen.set(s, n + 1);
    return n ? `${s}-${n}` : s;
  };
}

// ---- Code blocks -------------------------------------------------------------
const LANG_ALIAS = { promql: null, text: null, '': null, dockerfile: 'dockerfile', protobuf: 'protobuf' };

function splitHighlighted(html) {
  // Split highlighted HTML into lines, closing/reopening spans across newlines.
  const lines = [];
  const stack = [];
  let cur = '';
  let last = 0;
  for (const m of html.matchAll(/<span[^>]*>|<\/span>|\n/g)) {
    cur += html.slice(last, m.index);
    last = m.index + m[0].length;
    const t = m[0];
    if (t === '\n') {
      cur += '</span>'.repeat(stack.length);
      lines.push(cur);
      cur = stack.join('');
    } else if (t.startsWith('</')) {
      stack.pop();
      cur += t;
    } else {
      stack.push(t);
      cur += t;
    }
  }
  cur += html.slice(last);
  lines.push(cur);
  return lines;
}

function renderCode(code, lang) {
  const raw = code.replace(/\n$/, '');
  let language = lang in LANG_ALIAS ? LANG_ALIAS[lang] : lang;
  if (language && !hljs.getLanguage(language)) language = null;
  const html = language ? hljs.highlight(raw, { language, ignoreIllegals: true }).value : esc(raw);
  const hl = splitHighlighted(html);
  const rawLines = raw.split('\n');
  const body = hl
    .map((h, i) => {
      const ind = (rawLines[i] || '').match(/^ */)[0].length;
      return `<span class="ln" style="--ind:${ind}">${h === '' ? ' ' : h}</span>`;
    })
    .join('');
  const cls = rawLines.length <= 18 ? ' keep' : '';
  const tag = lang ? `<span class="code-lang">${esc(lang)}</span>` : '';
  return `<div class="code${cls}">${tag}<pre><code>${body}</code></pre></div>\n`;
}

// ---- Markdown ------------------------------------------------------------------
function makeMd(mermaidSink) {
  const md = new MarkdownIt({ html: true, linkify: false, typographer: false });
  md.use(taskLists, { enabled: false, label: false });
  md.renderer.rules.fence = (tokens, idx) => {
    const t = tokens[idx];
    const lang = (t.info || '').trim().split(/\s+/)[0];
    if (lang === 'mermaid') {
      mermaidSink.push(t.content);
      return `<div class="mermaid-src" data-i="${mermaidSink.length - 1}"></div>\n`;
    }
    return renderCode(t.content, lang);
  };
  md.renderer.rules.code_block = (tokens, idx) => renderCode(tokens[idx].content, '');
  return md;
}

// ---- Mermaid -----------------------------------------------------------------
async function renderDiagrams(sources, browser) {
  fs.mkdirSync(CACHE, { recursive: true });
  const cfg = {
    ...MERMAID_CONFIG,
    // Drop shadows would be rasterised by Chrome's PDF backend; keep pure vectors.
    themeCSS: 'g, rect, path, polygon, circle, ellipse, line, .node, .cluster, .actor, .note { filter: none !important; }',
  };
  const key = crypto.createHash('sha256').update(JSON.stringify(cfg)).digest('hex').slice(0, 12);
  const results = [];
  for (const src of sources) {
    const h = crypto.createHash('sha256').update(key + src).digest('hex').slice(0, 20);
    const file = path.join(CACHE, h + '.svg');
    if (!fs.existsSync(file)) {
      const { data } = await renderMermaid(browser, src, 'svg', {
        backgroundColor: 'white',
        mermaidConfig: cfg,
        svgId: 'd' + h.slice(0, 8),
      });
      fs.writeFileSync(file, Buffer.from(data));
    }
    let svg = fs.readFileSync(file, 'utf8');
    svg = svg.replace(/filter:\s*drop-shadow\([^;"}]*\)\s*;?/g, '').replace(/\sfilter="url\([^)]*\)"/g, '');
    const vb = svg.match(/viewBox="([-\d.]+)\s+([-\d.]+)\s+([\d.]+)\s+([\d.]+)"/);
    if (!vb) throw new Error('Mermaid SVG without viewBox');
    const w = parseFloat(vb[3]), hgt = parseFloat(vb[4]);
    // Give the SVG an explicit intrinsic size so <img> keeps the exact aspect ratio.
    svg = svg.replace(/<svg\b[^>]*>/, (tag) =>
      tag
        .replace(/\s(width|height)="[^"]*"/g, '')
        .replace(/max-width:\s*[\d.]+px;?/, '')
        .replace(/^<svg/, `<svg width="${w}" height="${hgt}"`),
    );
    results.push({ svg, w, h: hgt, hash: h });
  }
  return results;
}

// headH: vertical space (px) reserved for a section heading kept on the figure's page.
function layoutDiagram(w, h, headH = 0) {
  const sp = Math.min(MAX_SCALE, CONTENT_W / w, (PORTRAIT_FIG_H - headH) / h);
  if (sp >= MIN_SCALE) return { mode: 'inline', scale: sp };
  const sl = Math.min(MAX_SCALE, (LAND_W - headH) / w, LAND_H / h);
  const sf = Math.min(MAX_SCALE, CONTENT_W / w, (FULLPAGE_FIG_H - headH) / h);
  if (sl > sf * 1.05) return { mode: 'landscape', scale: sl };
  // Width-limited diagrams do not fill the page: keep them in the text flow so
  // the following text can use the space beneath. Tall (height-limited) diagrams
  // get a dedicated page at the largest scale that fits.
  if (CONTENT_W / w <= (FULLPAGE_FIG_H - headH) / h) return { mode: 'inline', scale: Math.min(MAX_SCALE, CONTENT_W / w, (PORTRAIT_FIG_H - headH) / h) };
  return { mode: 'fullpage', scale: sf };
}

// ---- Callout classification -------------------------------------------------
const BOXES = {
  concept: 'Concept',
  say: 'Say it aloud',
  remember: 'Remember this',
  tip: 'Interview tip',
  caution: 'Caution',
  example: 'Worked example',
  technical: 'Technical detail',
  note: 'Note',
};
const LABEL_RULES = [
  [/^What to say:?$/i, 'say', true],
  [/^Key points to remember:?$/i, 'remember', true],
  [/^Technical details to understand:?$/i, 'technical', true],
  [/^Likely (interruption|follow-up)/i, 'tip', false],
  [/^(Interview line|Key line|Closing line|Honesty line)/i, 'tip', false],
  [/^(Honesty rule|The most important honesty rule|Ground rules for answering|First, the status)/i, 'caution', false],
  [/^Where (the analogy|it) breaks/i, 'caution', false],
  [/^(Caveat|Limitations?):?$/i, 'caution', false],
  [/^(Worked example|Typical latency \(ILLUSTRATIVE\))/i, 'example', false],
];

function boxHtml(kind, inner) {
  // Short boxes never split across pages; long ones may break (frame is cloned on both sides).
  const textLen = inner.replace(/<[^>]+>/g, '').length;
  const keep = textLen < 1100 ? ' keep' : '';
  return `<div class="box box-${kind}${keep}"><div class="box-label">${BOXES[kind]}</div>${inner}</div>`;
}

// ---- Book assembly -----------------------------------------------------------
function assemble(book, mdText, diagrams, mermaidSources) {
  const md = makeMd(mermaidSources);
  const sinkStart = mermaidSources.length;
  const bodyHtml = md.render(mdText);
  const $ = cheerio.load(bodyHtml);
  const slug = slugger();
  const nodes = $('body').children().toArray();

  // --- Title + subtitle (become the cover) ---
  const h1 = nodes.shift();
  const fullTitle = $(h1).text().trim();
  const [mainTitle, subTitle] = fullTitle.split(/\s+—\s+/);
  let description = '';
  if (nodes[0] && nodes[0].tagName === 'p' && $(nodes[0]).children('em').length) description = $(nodes.shift()).html();
  if (nodes[0] && nodes[0].tagName === 'hr') nodes.shift();

  // --- Split into preface / skipped md-TOC / parts / chapters ---
  const preface = [];
  const parts = [];
  let mode = 'pre';
  let curPart = null, curCh = null;
  let pageBreaksSeen = 0, pageBreaksKept = 0;
  const isPB = (el) => el.tagName === 'div' && /page-break-before/.test($(el).attr('style') || '');
  for (let i = 0; i < nodes.length; i++) {
    const el = nodes[i];
    const tag = el.tagName;
    const text = $(el).text().trim();
    if (isPB(el)) {
      pageBreaksSeen++;
      const nxt = nodes[i + 1];
      const nt = nxt ? $(nxt).text().trim() : '';
      const structural = nxt && ((nxt.tagName === 'h1' && /^Part \d+/.test(nt)) || (nxt.tagName === 'h2' && /^\d+\.\s/.test(nt)));
      if (structural) continue; // honoured by the chapter/part page break itself
      if (mode === 'toc' || mode === 'pre') continue;
      pageBreaksKept++;
      curCh.nodes.push('<div class="page-break"></div>');
      continue;
    }
    if (tag === 'h2' && /^How to Use/i.test(text)) { mode = 'preface'; preface.push(el); continue; }
    if (tag === 'h2' && /^Table of Contents$/i.test(text)) { mode = 'toc'; continue; }
    if (tag === 'h1' && /^Part (\d+):\s*(.*)$/.test(text)) {
      const m = text.match(/^Part (\d+):\s*(.*)$/);
      curPart = { no: +m[1], title: m[2], id: `part-${m[1]}`, chapters: [] };
      parts.push(curPart);
      mode = 'body';
      curCh = null;
      continue;
    }
    if (tag === 'h2' && /^(\d+)\.\s+(.*)$/.test(text) && mode === 'body') {
      const m = text.match(/^(\d+)\.\s+(.*)$/);
      curCh = { no: +m[1], title: m[2], id: `ch-${m[1]}`, part: curPart, nodes: [], sections: [] };
      curPart.chapters.push(curCh);
      continue;
    }
    if (mode === 'preface') preface.push(el);
    else if (mode === 'toc') continue;
    else if (mode === 'body') {
      if (!curCh) throw new Error(`${book.slug}: content between part heading and first chapter: ${text.slice(0, 60)}`);
      curCh.nodes.push(el);
    } else throw new Error(`${book.slug}: unexpected content before preface: ${text.slice(0, 60)}`);
  }

  const chapters = parts.flatMap((p) => p.chapters);
  const figures = [];
  const layoutLog = [];
  let tableTotal = 0;

  // --- Transform each chapter's content ---
  for (const ch of chapters) {
    const $c = cheerio.load('<section></section>');
    const sec = $c('section');
    for (const n of ch.nodes) sec.append(typeof n === 'string' ? n : $.html(n));

    // Section headings: ids + split number for styling
    sec.children('h3').each((_, el) => {
      const t = $c(el).text().trim();
      const id = slug(t, 's-');
      $c(el).attr('id', id);
      ch.sections.push({ id, title: t, html: $c(el).html() });
      const m = $c(el).html().match(/^(\d+\.\d+[a-z]?)\s+/);
      if (m) $c(el).html(`<span class="secnum">${m[1]}</span> ${$c(el).html().slice(m[0].length)}`);
    });
    sec.find('h4').each((_, el) => {
      const h = $c(el).html();
      const m = h.match(/^Q(\d+)\.\s+/);
      if (m) {
        $c(el).addClass('question').html(`<span class="qnum">Q${m[1]}</span><span class="qtext">${h.slice(m[0].length)}</span>`);
      }
    });

    // Figures: caption paragraph + mermaid placeholder -> <figure>
    sec.find('div.mermaid-src').each((_, el) => {
      const i = +$c(el).attr('data-i');
      const d = diagrams[i];
      const prev = $c(el).prev();
      let num = null, cap = '';
      if (prev.is('p') && /^Figure\s+\S+/.test(prev.text().trim()) && prev.children().length === 1 && prev.children('strong').length) {
        const t = prev.children('strong').html();
        const m = t.match(/^Figure\s+([\w.]+)\s*[—–-]\s*([\s\S]*)$/);
        if (m) { num = m[1]; cap = m[2]; } else { num = t.replace(/^Figure\s+/, ''); }
        prev.remove();
      }
      if (!num) { num = `${ch.no}.${figures.filter((f) => f.ch === ch.no).length + 1}`; console.warn(`  ! uncaptioned diagram in ch ${ch.no}`); }
      const fid = 'fig-' + num.replace(/[^\w]/g, '-').toLowerCase();
      // A heading directly above the figure travels with it (never stranded at a page foot).
      const hp = $c(el).prev().is('h3, h4') ? $c(el).prev() : null;
      const L = layoutDiagram(d.w, d.h, hp ? 13 * MM : 0);
      const wpx = d.w * L.scale;
      const file = `diagrams/figure-${num.replace(/[^\w.]/g, '_')}.svg`;
      d.file = file;
      figures.push({ ch: ch.no, num, cap, id: fid, mode: L.mode, scale: L.scale, w: d.w, h: d.h, file });
      layoutLog.push(`    Figure ${num.padEnd(6)} ${String(Math.round(d.w)).padStart(5)}x${String(Math.round(d.h)).padEnd(5)} ${L.mode.padEnd(9)} scale ${L.scale.toFixed(2)}  label ≈ ${(12 * L.scale).toFixed(1)}pt`);
      const figHtml = `<figure class="diagram" id="${fid}"><img src="${file}" alt="Figure ${esc(num)}" style="width:${wpx.toFixed(1)}px"><figcaption><span class="fig-num">Figure ${esc(num)}</span>${cap ? `<span class="fig-sep"> — </span>${cap}` : ''}</figcaption></figure>`;
      const headHtml = hp ? $c.html(hp) : '';
      let out;
      if (L.mode === 'landscape') out = `<div class="landscape-page${hp ? ' with-head' : ''}">${headHtml}<div class="landscape-box"><div class="landscape-frame">${figHtml}</div></div></div>`;
      else if (L.mode === 'fullpage') out = `<div class="figure-page">${headHtml}${figHtml}</div>`;
      else out = hp ? `<div class="keep-with">${headHtml}${figHtml}</div>` : figHtml;
      if (hp) hp.remove();
      $c(el).replaceWith(out);
    });

    // Tables: numbered captions (number per chapter; text from the nearest label/heading)
    let tno = 0;
    let lastHeading = null, lastHeadingCount = 0;
    const walker = sec.find('h3, h4, table').toArray();
    for (const el of walker) {
      if (el.tagName !== 'table') { lastHeading = el; lastHeadingCount = 0; continue; }
      tno++; tableTotal++;
      const $t = $c(el);
      let capText = '';
      const prev = $t.prev();
      if (prev.is('p') && prev.children().length === 1 && prev.children('strong').length && prev.text().trim() === prev.children('strong').text().trim() && !/^Answer:?$/i.test(prev.text().trim())) {
        capText = prev.children('strong').html().replace(/:\s*$/, '');
        prev.remove();
      } else if (lastHeading) {
        const $h = $c(lastHeading);
        capText = $h.hasClass('question') ? `Answer to ${$h.find('.qnum').text()}` : $h.clone().find('.secnum').remove().end().html().trim();
        if (lastHeadingCount > 0) capText += ' (continued)';
      }
      lastHeadingCount++;
      const cols = $t.find('tr').first().children().length;
      const rows = $t.find('tr').length;
      $t.wrap(`<div class="table-block${cols >= 6 ? ' wide' : ''}${rows <= 14 ? ' keep' : ''}"></div>`);
      $t.before(`<div class="table-caption"><span class="tab-num">Table ${ch.no}.${tno}</span>${capText ? `<span class="tab-sep"> — </span>${capText}` : ''}</div>`);
    }

    // Labelled paragraphs -> information boxes
    sec.children('p').each((_, el) => {
      const $p = $c(el);
      const first = el.children.find((n) => !(n.type === 'text' && !n.data.trim()));
      if (!first || first.tagName !== 'strong') return;
      const label = $c(first).text().trim();
      const labelOnly = $p.text().trim() === label;
      for (const [re, kind, takesNext] of LABEL_RULES) {
        if (!re.test(label)) continue;
        if (labelOnly && takesNext) {
          const nxt = $p.next();
          if (nxt.length && /^(blockquote|ul|ol|div|table|pre)$/i.test(nxt[0].tagName)) {
            const inner = kind === 'say' && nxt.is('blockquote') ? nxt.html() : $c.html(nxt);
            $p.replaceWith(boxHtml(kind, `<p class="box-lead">${$p.html()}</p>${inner}`));
            nxt.remove();
            return;
          }
        }
        if (!labelOnly) { $p.replaceWith(boxHtml(kind, $c.html($p))); return; }
      }
      if (/^Diagram to draw/i.test(label)) $p.addClass('para-draw');
      else if (/^Transition/i.test(label)) $p.addClass('para-transition');
      else if (/^Answer:?$/i.test(label) && labelOnly) $p.addClass('answer-label');
    });

    // Remaining blockquotes -> boxes
    sec.children('blockquote').each((_, el) => {
      const $b = $c(el);
      const lead = $b.find('strong').first().text();
      const txt = $b.text().trim();
      let kind = 'note';
      if (/honesty|status|unverified/i.test(lead) || /\bUNVERIFIED\b/.test(txt)) kind = 'caution';
      else if (/Likely follow-up/i.test(lead)) kind = 'tip';
      else if (/^["“]/.test(txt)) kind = 'say';
      $b.replaceWith(boxHtml(kind, $b.html()));
    });

    // Concept cards: lists made of Simple / Technical / Why needed / How used items
    sec.children('ul').each((_, el) => {
      const leads = $c(el).children('li').toArray().map((li) => $c(li).children('strong').first().text().trim().replace(/:$/, ''));
      if (leads.includes('Simple') && leads.includes('Technical')) $c(el).replaceWith(boxHtml('concept', `<ul class="concept-list">${$c(el).html()}</ul>`));
    });

    // Long inline code: allow line breaks only at natural boundaries (no visible change).
    sec.find('code').each((_, el) => {
      if ($c(el).closest('.code').length) return;
      const t = $c(el).text();
      if (t.length <= 20 && !/\s/.test(t) && $c(el).closest('td, th').length) { $c(el).addClass('nw'); return; }
      if (t.length < 16 || $c(el).children().length) return;
      const parts = t.split(/(?<=[./_(,=:>\-])|(?<=[a-z0-9])(?=[A-Z])/);
      $c(el).html(parts.map(esc).join('<wbr>'));
    });

    // Task-list checkboxes -> printable empty squares
    sec.find('input[type=checkbox]').each((_, el) => $c(el).replaceWith('<span class="checkbox" aria-hidden="true"></span>'));
    sec.find('li.task-list-item').each((_, el) => $c(el).closest('ul').addClass('checklist'));

    ch.html = sec.html();
  }

  // Each mermaid placeholder must have been turned into a figure exactly once.
  const used = figures.length;
  if (used !== mermaidSources.length - sinkStart) throw new Error(`${book.slug}: ${mermaidSources.length - sinkStart} mermaid blocks but ${used} figures`);

  // --- Preface ---
  const $p = cheerio.load('<section></section>');
  for (const n of preface) $p('section').append($.html(n));
  $p('section h2').first().remove();
  $p('section table').each((_, el) => $p(el).wrap('<div class="table-block"></div>'));
  $p('section blockquote').each((_, el) => $p(el).replaceWith(boxHtml('caution', $p(el).html())));
  const prefaceHtml = $p('section').html();

  // --- Generated navigation ---
  const pn = (id) => `<span class="pn" data-t="${id}">000</span>`;
  const entry = (cls, id, label) => `<div class="toc-entry ${cls}"><a href="#${id}"><span class="toc-text">${label}</span></a><span class="leader"></span>${pn(id)}</div>`;

  let toc = '';
  toc += entry('toc-front', 'how-to-use', 'How to Use This Booklet');
  toc += entry('toc-front', 'list-of-figures', 'List of Figures');
  for (const p of parts) {
    toc += `<div class="toc-part-block">` + entry('toc-part', p.id, `<span class="toc-num">Part ${p.no}</span>${esc(p.title)}`);
    for (const c of p.chapters) {
      toc += `<div class="toc-ch-block">` + entry('toc-ch', c.id, `<span class="toc-num">${c.no}</span>${esc(c.title)}`);
      for (const s of c.sections) toc += entry('toc-sec', s.id, s.html);
      toc += `</div>`;
    }
    toc += `</div>`;
  }
  const lof = figures.map((f) => entry('toc-fig', f.id, `<span class="toc-num">${esc(f.num)}</span>${f.cap}`)).join('');

  const partNav = (p) =>
    p.chapters.map((c) => entry('pt-ch', c.id, `<span class="toc-num">Chapter ${c.no}</span>${esc(c.title)}`)).join('');
  const chapterNav = (c) => (c.sections.length ? `<nav class="mini-toc"><div class="mini-title">In this chapter</div>${c.sections.map((s) => entry('mini', s.id, s.html)).join('')}</nav>` : '');

  const qCount = chapters.reduce((n, c) => n + (c.html.match(/<h4 class="question"/g) || []).length, 0);

  // --- Cover ---
  const coverParts = parts
    .map((p) => {
      const a = p.chapters[0].no, b = p.chapters[p.chapters.length - 1].no;
      return `<li><span class="cp-no">Part ${p.no}</span><span class="cp-title">${esc(p.title)}</span><span class="cp-ch">Ch. ${a}–${b}</span></li>`;
    })
    .join('');
  const series = BOOKS.map((b) => `<li class="${b.no === book.no ? 'current' : ''}"><span class="sq"></span>${b.no}. ${esc(b.short)}</li>`).join('');
  const cover = `
<section class="cover">
  <div class="cover-bar"></div>
  <div class="cover-inner">
    <div class="cover-top"><span>SDE Interview Preparation</span><span>Book ${book.no} of ${BOOKS.length}</span></div>
    <div class="cover-motif">${motifSvg(book.motif)}</div>
    <div class="cover-theme">${esc(book.theme)}</div>
    <h1 class="cover-title">${esc(mainTitle)}</h1>
    <div class="cover-sub">${esc(subTitle || 'Study Guide')}</div>
    <div class="cover-rule"></div>
    <p class="cover-desc">${description}</p>
    <div class="cover-parts"><div class="cp-head">Inside this book</div><ol>${coverParts}</ol>
      <div class="cp-extra">${chapters.length} chapters · ${figures.length} figures · ${tableTotal} tables · ${qCount} interview questions with answers · revision sheets</div></div>
    <div class="cover-bottom"><div class="series-head">The series</div><ul class="series">${series}</ul></div>
  </div>
</section>`;

  // --- Body ---
  const NUMW = (n) => NUM_WORDS[n] || String(n);
  let body = '';
  for (const p of parts) {
    body += `
<section class="part-opener" id="${p.id}">
  <div class="part-kicker">Part ${NUMW(p.no)}</div>
  <h1 class="part-title"><span class="vh">Part ${p.no}: </span>${esc(p.title)}</h1>
  <div class="part-rule"></div>
  <div class="part-nav">${partNav(p)}</div>
</section>`;
    for (const c of p.chapters) {
      body += `
<section class="chapter" id="${c.id}" style="page: ch${c.no}">
  <header class="chapter-head">
    <div class="ch-kicker">Part ${p.no} · ${esc(p.title)}</div>
    <div class="ch-num-row"><span class="ch-word">Chapter</span><span class="ch-num">${c.no}</span></div>
    <h2 class="ch-title"><span class="vh">${c.no}. </span>${esc(c.title)}</h2>
    ${chapterNav(c)}
  </header>
  <div class="chapter-body">${c.html}</div>
</section>`;
    }
  }

  // --- Running headers (named pages) ---
  const pageCss = chapters
    .map((c) => `@page ch${c.no} { @top-right { content: ${cssStr(`Chapter ${c.no} · ${c.title}`)}; } }`)
    .join('\n');

  const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>${esc(mainTitle)} — Study Guide</title>
<link rel="stylesheet" href="../../node_modules/@fontsource/source-serif-4/400.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-serif-4/400-italic.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-serif-4/600.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-serif-4/700.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-sans-3/400.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-sans-3/400-italic.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-sans-3/600.css">
<link rel="stylesheet" href="../../node_modules/@fontsource/source-sans-3/700.css">
<link rel="stylesheet" href="../../styles/book.css">
<style>
:root { --accent: ${book.accent}; --tint: ${book.tint}; }
@page { @top-left { content: ${cssStr(`Book ${book.no} · ${book.short}`)}; } }
${pageCss}
</style>
</head>
<body class="book-${book.no}">
${cover}
<section class="front contents" id="contents">
  <h2 class="front-title">Contents</h2>
  <div class="toc">${toc}</div>
</section>
<section class="front lof" id="list-of-figures">
  <h2 class="front-title">List of Figures</h2>
  <div class="toc">${lof}</div>
</section>
<section class="front preface" id="how-to-use">
  <h2 class="front-title">How to Use This Booklet</h2>
  ${prefaceHtml}
</section>
${body}
</body></html>`;

  return { html, figures, chapters, parts, layoutLog, pageBreaksSeen, pageBreaksKept, tableTotal };
}

// ---- PDF -------------------------------------------------------------------------
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

function pageMap(pdfFile) {
  const out = execFileSync(PY, [path.join(BUILD, 'pagemap.py'), pdfFile], { encoding: 'utf8' });
  return JSON.parse(out);
}

function fillPageNumbers(html, map) {
  const missing = [];
  const res = html.replace(/<span class="pn" data-t="([^"]+)">000<\/span>/g, (_, id) => {
    if (!(id in map)) missing.push(id);
    return `<span class="pn" data-t="${id}">${map[id] ?? '?'}</span>`;
  });
  if (missing.length) throw new Error('Unresolved TOC targets: ' + missing.join(', '));
  return res;
}

// ---- Main ------------------------------------------------------------------------
async function main() {
  const filter = process.argv[2];
  const books = BOOKS.filter((b) => !filter || b.slug.startsWith(filter));
  fs.mkdirSync(PDF_DIR, { recursive: true });
  const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--font-render-hinting=none'] });
  const report = [];
  try {
    for (const book of books) {
      console.log(`\n=== ${book.slug}`);
      const mdText = fs.readFileSync(path.join(ROOT, book.slug + '.md'), 'utf8');

      // Pass 0: collect Mermaid sources and render them to SVG.
      const sources = [];
      makeMd(sources).render(mdText);
      const fenceCount = (mdText.match(/^```mermaid\s*$/gm) || []).length;
      if (fenceCount !== sources.length) throw new Error(`mermaid count mismatch ${fenceCount} vs ${sources.length}`);
      const diagrams = await renderDiagrams(sources, browser);
      console.log(`  Mermaid: ${diagrams.length} diagrams rendered to SVG`);

      const dir = path.join(OUT, book.slug);
      fs.rmSync(dir, { recursive: true, force: true });
      fs.mkdirSync(path.join(dir, 'diagrams'), { recursive: true });
      const A = assemble(book, mdText, diagrams, []);
      for (const d of diagrams) fs.writeFileSync(path.join(dir, d.file), d.svg);
      console.log(A.layoutLog.join('\n'));
      console.log(`  Page-break hints: ${A.pageBreaksSeen} found, ${A.pageBreaksSeen - A.pageBreaksKept} coincide with part/chapter breaks, ${A.pageBreaksKept} kept as explicit breaks`);

      // Pass 1: placeholders -> learn where every heading/figure lands.
      const htmlFile = path.join(dir, 'index.html');
      fs.writeFileSync(htmlFile, A.html);
      const pass1 = path.join(dir, 'pass1.pdf');
      await printPdf(browser, htmlFile, pass1);
      const map1 = pageMap(pass1);

      // Pass 2: real page numbers. Placeholders are fixed-width, so layout is stable.
      fs.writeFileSync(htmlFile, fillPageNumbers(A.html, map1));
      const pdfFile = path.join(PDF_DIR, book.slug + '.pdf');
      await printPdf(browser, htmlFile, pdfFile);
      const map2 = pageMap(pdfFile);
      const drift = Object.keys(map1).filter((k) => map1[k] !== map2[k]);
      if (drift.length) throw new Error('Page numbers drifted between passes: ' + drift.slice(0, 5).join(', '));
      fs.rmSync(pass1);
      const pages = map2.__pages;
      console.log(`  PDF: ${path.relative(ROOT, pdfFile)}  (${pages} pages, TOC verified)`);
      report.push({ book: book.slug, pages, figures: A.figures.length, tables: A.tableTotal, layouts: A.figures.map((f) => ({ num: f.num, mode: f.mode, scale: +f.scale.toFixed(3) })) });
    }
  } finally {
    await browser.close();
  }
  fs.writeFileSync(path.join(OUT, 'build-report.json'), JSON.stringify(report, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
