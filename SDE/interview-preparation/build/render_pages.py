"""Render PDF pages to PNG for visual QA.

usage: render_pages.py PDF OUTDIR [--gray] [--dpi N] [pages...]   (1-based; 'all' or ranges a-b)
--gray converts to 8-bit grayscale (luminance), a fair proxy for a B&W photocopy.
"""
import sys, os
import pymupdf

args = sys.argv[1:]
pdf, outdir = args[0], args[1]
gray = '--gray' in args
dpi = 70
if '--dpi' in args:
    dpi = int(args[args.index('--dpi') + 1])
rest = [a for i, a in enumerate(args[2:], 2) if not a.startswith('--') and args[i - 1] != '--dpi']
doc = pymupdf.open(pdf)
pages = []
for r in rest or ['all']:
    if r == 'all':
        pages += range(1, doc.page_count + 1)
    elif '-' in r:
        a, b = map(int, r.split('-')); pages += range(a, b + 1)
    else:
        pages.append(int(r))
os.makedirs(outdir, exist_ok=True)
cs = pymupdf.csGRAY if gray else pymupdf.csRGB
for p in pages:
    pix = doc[p - 1].get_pixmap(dpi=dpi, colorspace=cs)
    pix.save(os.path.join(outdir, f"p{p:03d}{'-g' if gray else ''}.png"))
print(len(pages), 'pages rendered')
