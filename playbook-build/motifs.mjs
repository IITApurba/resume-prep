// Minimal line-art cover motifs for the DATA and QUANT playbooks.
// Same visual language as the SDE series (books.mjs): light grey strokes, almost no toner.
const S = 'stroke="#9A9A9A" fill="none" stroke-width="1.1"';
const D = 'stroke="#6E6E6E" fill="none" stroke-width="1.4"';
const W = 600, H = 190;

const motifs = {
  // Reasoning trajectory: a chain of steps, each with a small score bar underneath.
  prm() {
    let g = '';
    const xs = [30, 140, 250, 360, 470];
    const scores = [0.9, 0.8, 0.85, 0.3, 0.75];
    xs.forEach((x, i) => {
      g += `<rect x="${x}" y="50" width="96" height="44" rx="6" ${i === 3 ? D + ' stroke-dasharray="5 4"' : D}/>`;
      for (let k = 0; k < 3; k++) g += `<line x1="${x + 12}" y1="${62 + k * 10}" x2="${x + 84 - k * 14}" y2="${62 + k * 10}" stroke="#9A9A9A" stroke-width="3" stroke-linecap="round"/>`;
      if (i < xs.length - 1) g += `<line x1="${x + 96}" y1="72" x2="${x + 110}" y2="72" ${D}/><path d="M${x + 104} 67 L${x + 110} 72 L${x + 104} 77" ${D}/>`;
      g += `<rect x="${x + 8}" y="118" width="80" height="10" ${S}/><rect x="${x + 8}" y="118" width="${80 * scores[i]}" height="10" fill="#B8B8B8"/>`;
      g += `<line x1="${x + 48}" y1="94" x2="${x + 48}" y2="116" ${S} stroke-dasharray="2 3"/>`;
    });
    g += `<path d="M30 160 H566" ${S}/>`;
    return g;
  },
  // Preference pair: one prompt node, a chosen (solid) and a rejected (dashed) response.
  dpo() {
    let g = `<rect x="40" y="70" width="110" height="50" rx="8" ${D}/>`;
    for (let k = 0; k < 2; k++) g += `<line x1="56" y1="${88 + k * 14}" x2="${132 - k * 20}" y2="${88 + k * 14}" stroke="#9A9A9A" stroke-width="3" stroke-linecap="round"/>`;
    g += `<path d="M150 95 C230 95, 230 45, 310 45" ${D}/><path d="M150 95 C230 95, 230 145, 310 145" ${S} stroke-dasharray="5 4"/>`;
    for (const [y, dash] of [[45, false], [145, true]]) {
      g += `<rect x="312" y="${y - 22}" width="200" height="44" rx="6" ${dash ? S + ' stroke-dasharray="5 4"' : D}/>`;
      for (let k = 0; k < 2; k++) g += `<line x1="326" y1="${y - 6 + k * 12}" x2="${490 - k * 50}" y2="${y - 6 + k * 12}" stroke="#9A9A9A" stroke-width="3" stroke-linecap="round"/>`;
    }
    g += `<path d="M530 45 l8 8 l14 -16" ${D}/><path d="M532 137 l16 16 M548 137 l-16 16" ${S}/>`;
    return g;
  },
  // Transactions: a dense cloud of normal points, a few circled outliers, and a threshold line.
  fraud() {
    let g = '';
    let s = 7;
    const rnd = () => ((s = (s * 9301 + 49297) % 233280) / 233280);
    for (let i = 0; i < 150; i++) {
      const x = 40 + rnd() * 360, y = 60 + rnd() * 90 * (0.5 + rnd() * 0.5);
      g += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="1.8" fill="#A8A8A8"/>`;
    }
    for (const [x, y] of [[470, 40], [510, 70], [545, 32], [490, 120]]) g += `<circle cx="${x}" cy="${y}" r="3" fill="#6E6E6E"/><circle cx="${x}" cy="${y}" r="9" ${D}/>`;
    g += `<line x1="430" y1="15" x2="430" y2="175" ${D} stroke-dasharray="6 4"/>`;
    return g;
  },
  // Time series with walk-forward train / test windows underneath.
  series() {
    let g = '';
    let y = 90, s = 3, d = 'M20 90';
    const rnd = () => ((s = (s * 9301 + 49297) % 233280) / 233280);
    for (let x = 24; x <= 580; x += 6) { y += (rnd() - 0.47) * 12; y = Math.max(30, Math.min(120, y)); d += ` L${x} ${y.toFixed(1)}`; }
    g += `<path d="${d}" ${D}/>`;
    for (let k = 0; k < 4; k++) {
      const x0 = 20 + k * 110;
      g += `<rect x="${x0}" y="${140 + (k % 2) * 0}" width="160" height="10" ${S}/><rect x="${x0 + 168}" y="140" width="40" height="10" fill="#B8B8B8"/>`;
      g += `<line x1="${x0}" y1="${158 + k * 6}" x2="${x0 + 208}" y2="${158 + k * 6}" ${S} stroke-dasharray="2 3"/>`;
    }
    return g;
  },
  // Limit order book: bid depth bars on the left, ask depth bars on the right, spread gap in the middle.
  lob() {
    let g = '';
    const bids = [60, 95, 80, 130, 110, 150], asks = [50, 70, 120, 90, 140, 125];
    bids.forEach((w, i) => { const y = 25 + i * 24; g += `<rect x="${280 - w}" y="${y}" width="${w}" height="16" ${i === 0 ? D : S}/>`; });
    asks.forEach((w, i) => { const y = 25 + i * 24; g += `<rect x="320" y="${y}" width="${w}" height="16" ${i === 0 ? D : S}/>`; });
    g += `<line x1="300" y1="15" x2="300" y2="175" ${D} stroke-dasharray="4 4"/>`;
    g += `<path d="M40 175 C150 160, 200 168, 300 150 S460 120, 560 135" ${S}/>`;
    return g;
  },
  // Cross-section: many stocks ranked by a signal; long the top bars, short the bottom bars.
  xsec() {
    let g = `<line x1="30" y1="95" x2="570" y2="95" ${D}/>`;
    const n = 30;
    for (let i = 0; i < n; i++) {
      const v = (n / 2 - i - 0.5) * 4.4, x = 34 + i * 18, top = i < 6, bot = i >= n - 6;
      g += `<rect x="${x}" y="${v > 0 ? 95 - v : 95}" width="12" height="${Math.abs(v)}" ${top || bot ? D : S}/>`;
    }
    g += `<path d="M34 18 h104" ${D}/><path d="M462 172 h104" ${D}/>`;
    return g;
  },
  // Execution trajectories: front-loaded Almgren-Chriss curve versus a straight TWAP line.
  exec() {
    let g = `<line x1="40" y1="170" x2="570" y2="170" ${S}/><line x1="40" y1="20" x2="40" y2="170" ${S}/>`;
    g += `<path d="M40 25 L560 168" ${S} stroke-dasharray="6 5"/>`;
    g += `<path d="M40 25 C150 120, 300 158, 560 168" ${D}/>`;
    for (let k = 0; k < 12; k++) { const x = 60 + k * 42; g += `<rect x="${x}" y="${150 - (12 - k) * 6}" width="22" height="${(12 - k) * 6}" fill="#E2E2E2" stroke="#A8A8A8" stroke-width="0.8"/>`; }
    g += `<path d="M40 25 C150 120, 300 158, 560 168" ${D}/>`;
    return g;
  },
  // Portfolio P&L distribution with the left tail marked (VaR line, ES region).
  risk() {
    let d = '';
    for (let x = 30; x <= 570; x += 5) {
      const z = (x - 320) / 70, y = 165 - 130 * Math.exp(-0.5 * z * z) - (x < 200 ? 6 * Math.exp(-0.5 * ((x - 140) / 30) ** 2) : 0);
      d += `${x === 30 ? 'M' : ' L'}${x} ${y.toFixed(1)}`;
    }
    let g = `<line x1="20" y1="166" x2="580" y2="166" ${S}/><path d="${d}" ${D}/>`;
    g += `<line x1="185" y1="30" x2="185" y2="166" ${D} stroke-dasharray="5 4"/>`;
    for (let x = 36; x < 185; x += 8) g += `<line x1="${x}" y1="166" x2="${x}" y2="${158 - (x > 120 ? (x - 120) * 0.12 : 0)}" ${S}/>`;
    return g;
  },
  // Two score distributions (legitimate vs fraud) overlapping, cut by a decision threshold.
  threshold() {
    const curve = (mu, sd, amp) => {
      let d = '';
      for (let x = 30; x <= 570; x += 5) { const z = (x - mu) / sd; d += `${x === 30 ? 'M' : ' L'}${x} ${(166 - amp * Math.exp(-0.5 * z * z)).toFixed(1)}`; }
      return d;
    };
    let g = `<line x1="20" y1="166" x2="580" y2="166" ${S}/>`;
    g += `<path d="${curve(200, 60, 135)}" ${D}/><path d="${curve(450, 55, 60)}" ${D} stroke-dasharray="6 4"/>`;
    g += `<line x1="370" y1="20" x2="370" y2="170" stroke="#4A4A4A" stroke-width="1.6"/>`;
    g += `<path d="M370 28 h40 M402 22 l8 6 l-8 6" ${D}/>`;
    return g;
  },
};

export function motifSvg(kind) {
  const g = motifs[kind] ? motifs[kind]() : '';
  return `<svg class="motif" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">${g}</svg>`;
}
