"""Print how much of each page's text column is used (mm from top), to spot gaps."""
import sys, pymupdf
d = pymupdf.open(sys.argv[1])
H = d[0].rect.height
out = []
for i, p in enumerate(d):
    ys = [b[3] for b in p.get_text('blocks') if b[3] < H - 15 * 2.835]
    ys += [r.y1 for r in [it['rect'] for it in p.get_drawings()] if r.y1 < H - 15 * 2.835]
    out.append(f"{i+1}:{round(max(ys, default=0) / 2.835)}")
print(' '.join(out))
