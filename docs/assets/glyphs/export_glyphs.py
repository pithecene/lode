import fontforge, json, os

import sys
# usage: fontforge -lang=py -script export_glyphs.py <dir holding fonts/EBGaramond[-Italic].ttf>
S = sys.argv[1] if len(sys.argv) > 1 else "."
os.makedirs(S + "/glyphs", exist_ok=True)
jobs = [
    ("regular", S + "/fonts/EBGaramond.ttf", "Lode"),
    ("italic", S + "/fonts/EBGaramond-Italic.ttf", "Structured persistence for object storage."),
]


def contour_path(c):
    """Path data (font units, y-up) for one fontforge contour, quadratic or cubic."""
    pts = [(p.x, p.y, p.on_curve) for p in c]
    if not pts:
        return ""
    if c.is_quadratic:
        # rotate so the list starts on an on-curve point (insert an implied one if none)
        i = next((k for k, p in enumerate(pts) if p[2]), None)
        if i is None:
            (x0, y0, _), (x1, y1, _) = pts[0], pts[1]
            pts = [((x0 + x1) / 2, (y0 + y1) / 2, True)] + pts[1:] + pts[:1]
        else:
            pts = pts[i:] + pts[:i]
        d = ["M%.1f %.1f" % (pts[0][0], pts[0][1])]
        k = 1
        n = len(pts)
        while k <= n:
            p = pts[k % n]
            if p[2]:
                d.append("L%.1f %.1f" % (p[0], p[1]))
                k += 1
            else:
                q = pts[(k + 1) % n]
                if q[2]:
                    d.append("Q%.1f %.1f %.1f %.1f" % (p[0], p[1], q[0], q[1]))
                    k += 2
                else:
                    mx, my = (p[0] + q[0]) / 2, (p[1] + q[1]) / 2
                    d.append("Q%.1f %.1f %.1f %.1f" % (p[0], p[1], mx, my))
                    k += 1
        d.append("Z")
        return " ".join(d)
    # cubic: on, off, off, on, ...
    i = next(k for k, p in enumerate(pts) if p[2])
    pts = pts[i:] + pts[:i]
    d = ["M%.1f %.1f" % (pts[0][0], pts[0][1])]
    k = 1
    n = len(pts)
    while k <= n:
        p = pts[k % n]
        if p[2]:
            d.append("L%.1f %.1f" % (p[0], p[1]))
            k += 1
        else:
            c1, c2, e = p, pts[(k + 1) % n], pts[(k + 2) % n]
            d.append("C%.1f %.1f %.1f %.1f %.1f %.1f" % (c1[0], c1[1], c2[0], c2[1], e[0], e[1]))
            k += 3
    d.append("Z")
    return " ".join(d)


for fam, path, text in jobs:
    f = fontforge.open(path)
    info = {"em": f.em, "ascent": f.ascent, "descent": f.descent, "glyphs": {}, "kern": {}}
    for ch in sorted(set(text)):
        if ch == " ":
            info["glyphs"][ch] = {"width": f["space"].width, "d": None}
            continue
        g = f[ord(ch)]
        g.unlinkRef()
        layer = g.foreground
        d = " ".join(contour_path(c) for c in layer)
        info["glyphs"][ch] = {"width": g.width, "d": d, "name": g.glyphname}
    # pair kerning for adjacent characters in the text
    for a, b in zip(text, text[1:]):
        if a == " " or b == " ":
            continue
        try:
            ga = f[ord(a)]
            for lookup in f.gpos_lookups:
                for sub in f.getLookupSubtables(lookup):
                    if f.getLookupInfo(lookup)[0] == "gpos_pair":
                        try:
                            k = ga.getPosSub(sub)
                        except Exception:
                            k = []
                        for rec in k:
                            if rec[1] == "Pair" and rec[2] == f[ord(b)].glyphname:
                                info["kern"][a + b] = rec[5]
        except Exception as e:
            pass
    json.dump(info, open("%s/glyphs/%s.json" % (S, fam), "w"))
    print(fam, "ok", len(info["glyphs"]), "kern", info["kern"])
