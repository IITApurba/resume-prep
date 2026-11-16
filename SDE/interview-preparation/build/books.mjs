// Per-book identity: everything here is presentation only (cover motif, accent
// tone, running-header short title). Content always comes from the Markdown.

export const BOOKS = [
  {
    slug: '01-payment-transaction-processing-study-guide',
    no: 1,
    short: 'Payment & Transaction Processing',
    theme: 'Financial systems · Transaction processing',
    accent: '#2E4657', // deep slate blue-grey (reads as ~28% grey)
    tint: '#EAF0F4',
    motif: 'ledger',
  },
  {
    slug: '02-distributed-object-storage-study-guide',
    no: 2,
    short: 'Distributed Object Storage',
    theme: 'Distributed infrastructure · Storage',
    accent: '#3B3F55', // slate indigo-grey
    tint: '#ECEDF3',
    motif: 'ring',
  },
  {
    slug: '03-real-time-event-processing-study-guide',
    no: 3,
    short: 'Real-Time Event Processing',
    theme: 'Streaming data · Pipelines',
    accent: '#34493F', // slate green-grey
    tint: '#EBF1EE',
    motif: 'stream',
  },
  {
    slug: '04-llm-gateway-inference-router-study-guide',
    no: 4,
    short: 'LLM Gateway & Inference Router',
    theme: 'AI inference · Request routing',
    accent: '#47404F', // slate plum-grey
    tint: '#F0EEF4',
    motif: 'router',
  },
];

// Minimal line-art cover motifs. Light strokes only: almost no toner.
const S = 'stroke="#9A9A9A" fill="none" stroke-width="1.1"';
const D = 'stroke="#6E6E6E" fill="none" stroke-width="1.4"';

export function motifSvg(kind) {
  const W = 600, H = 190;
  let g = '';
  if (kind === 'ledger') {
    // Ruled ledger page: debit | credit columns with a double rule.
    for (let y = 20; y <= 170; y += 18) g += `<line x1="20" y1="${y}" x2="580" y2="${y}" ${S}/>`;
    g += `<line x1="300" y1="10" x2="300" y2="180" ${D}/><line x1="306" y1="10" x2="306" y2="180" ${D}/>`;
    g += `<line x1="80" y1="10" x2="80" y2="180" ${S}/><line x1="366" y1="10" x2="366" y2="180" ${S}/>`;
    const rows = [[38, 150, 0], [56, 180, 1], [74, 120, 0], [92, 160, 1], [110, 90, 0], [128, 140, 1]];
    for (const [y, w, side] of rows) {
      const x0 = side ? 380 : 95;
      g += `<line x1="${x0}" y1="${y - 6}" x2="${x0 + w}" y2="${y - 6}" stroke="#7A7A7A" stroke-width="3" stroke-linecap="round" stroke-dasharray="1 7"/>`;
    }
    g += `<line x1="200" y1="160" x2="290" y2="160" ${D}/><line x1="200" y1="164" x2="290" y2="164" ${D}/>`;
    g += `<line x1="486" y1="160" x2="576" y2="160" ${D}/><line x1="486" y1="164" x2="576" y2="164" ${D}/>`;
  } else if (kind === 'ring') {
    // Consistent-hashing ring with virtual nodes + stacked disks.
    const cx = 170, cy = 95, r = 72;
    g += `<circle cx="${cx}" cy="${cy}" r="${r}" ${D}/>`;
    g += `<circle cx="${cx}" cy="${cy}" r="${r - 16}" ${S} stroke-dasharray="2 5"/>`;
    for (let i = 0; i < 12; i++) {
      const a = (i / 12) * 2 * Math.PI, x = cx + r * Math.cos(a), y = cy + r * Math.sin(a);
      g += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${i % 3 === 0 ? 6 : 4}" fill="#FFFFFF" stroke="#6E6E6E" stroke-width="1.3"/>`;
    }
    for (let k = 0; k < 3; k++) {
      const x = 330 + k * 85, y = 40;
      for (let j = 0; j < 3; j++) {
        const yy = y + j * 34;
        g += `<ellipse cx="${x + 30}" cy="${yy}" rx="30" ry="8" ${D}/><path d="M${x} ${yy} v18 a30 8 0 0 0 60 0 v-18" ${S}/>`;
      }
    }
    g += `<path d="M250 95 H320" ${S} stroke-dasharray="4 4"/>`;
  } else if (kind === 'stream') {
    // Event timeline: ticks (events) under tumbling-window brackets.
    g += `<line x1="20" y1="120" x2="580" y2="120" ${D}/><path d="M572 114 L582 120 L572 126" ${D}/>`;
    const xs = [34, 52, 61, 90, 104, 131, 150, 158, 196, 214, 240, 252, 281, 300, 318, 349, 362, 391, 410, 436, 452, 480, 497, 521, 548];
    xs.forEach((x, i) => {
      const h = 14 + ((i * 37) % 30);
      g += `<line x1="${x}" y1="120" x2="${x}" y2="${120 - h}" stroke="#7A7A7A" stroke-width="2"/>`;
      g += `<circle cx="${x}" cy="${120 - h}" r="2.6" fill="#7A7A7A"/>`;
    });
    for (let w = 0; w < 4; w++) {
      const x = 24 + w * 140;
      g += `<path d="M${x} 140 v10 h130 v-10" ${S}/>`;
    }
    for (let k = 0; k < 3; k++) g += `<path d="M20 ${40 + k * 14} C120 ${20 + k * 14}, 200 ${60 + k * 14}, 300 ${40 + k * 14} S480 ${20 + k * 14}, 580 ${40 + k * 14}" ${S}/>`;
  } else if (kind === 'router') {
    // One gateway node fanning out to several model backends.
    const gx = 150, gy = 95;
    g += `<rect x="${gx - 50}" y="${gy - 26}" width="100" height="52" rx="8" ${D}/>`;
    g += `<line x1="20" y1="${gy}" x2="${gx - 50}" y2="${gy}" ${D}/><path d="M${gx - 60} ${gy - 6} L${gx - 50} ${gy} L${gx - 60} ${gy + 6}" ${D}/>`;
    const ys = [22, 59, 95, 131, 168];
    ys.forEach((y, i) => {
      g += `<path d="M${gx + 50} ${gy} C${gx + 140} ${gy}, ${gx + 140} ${y}, 390 ${y}" ${i === 1 ? D : S} ${i === 1 ? '' : 'stroke-dasharray="5 4"'}/>`;
      g += `<rect x="392" y="${y - 13}" width="120" height="26" rx="5" ${i === 1 ? D : S}/>`;
      for (let b = 0; b < 4; b++) g += `<line x1="${404 + b * 26}" y1="${y}" x2="${418 + b * 26}" y2="${y}" stroke="#9A9A9A" stroke-width="3" stroke-linecap="round"/>`;
    });
    g += `<circle cx="${gx}" cy="${gy}" r="9" ${D}/><circle cx="${gx}" cy="${gy}" r="3" fill="#6E6E6E"/>`;
  }
  return `<svg class="motif" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">${g}</svg>`;
}
