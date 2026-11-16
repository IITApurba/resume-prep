"""Tile page renders into contact sheets for fast whole-book review.
usage: contact_sheet.py PDF OUT_PREFIX [cols] [rows] [--gray]"""
import sys, pymupdf
pdf, prefix = sys.argv[1], sys.argv[2]
cols = int(sys.argv[3]) if len(sys.argv) > 3 else 6
rows = int(sys.argv[4]) if len(sys.argv) > 4 else 4
gray = '--gray' in sys.argv
doc = pymupdf.open(pdf)
tw, th = 248, 351
per = cols * rows
cs = pymupdf.csGRAY if gray else pymupdf.csRGB
for s in range(0, doc.page_count, per):
    sheet = pymupdf.Pixmap(cs, pymupdf.IRect(0, 0, cols * (tw + 8) + 8, rows * (th + 8) + 8), False)
    sheet.clear_with(170)
    for k, p in enumerate(range(s, min(s + per, doc.page_count))):
        z = tw / doc[p].rect.width
        pix = doc[p].get_pixmap(matrix=pymupdf.Matrix(z, z), colorspace=cs)
        x = 8 + (k % cols) * (tw + 8); y = 8 + (k // cols) * (th + 8)
        pix.set_origin(x, y)
        sheet.copy(pix, pix.irect)
    sheet.save(f"{prefix}-{s // per + 1:02d}.png")
print('ok')
