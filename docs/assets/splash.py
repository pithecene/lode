"""Lode splash — an engraved geological cross-section.

Folded strata as fine contour lines, one luminous ore vein following the fold
crest, a few crystal facets, risograph grain; the wordmark and tagline are
EB Garamond outlines. One geometry drives the splash (light + dark) and the
mark. Everything is deterministic: run again, get the same bytes.
"""
import json, math, pathlib, sys

S = pathlib.Path(__file__).resolve().parent
OUT = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else S
OUT.mkdir(parents=True, exist_ok=True)
# Glyph outlines of EB Garamond (SIL Open Font License 1.1), extracted by
# glyphs/export_glyphs.py; only the letters used here are embedded.
REG = json.load(open(S / "glyphs/regular.json"))
ITA = json.load(open(S / "glyphs/italic.json"))

W, H = 1280, 520

PAL = {
    "dark": dict(
        bg0="#191713", bg1="#100f0c",
        bands=["#1f1c17", "#262219", "#1b1915", "#2b261d", "#211e18", "#2e2920"],
        line="#f2efe7", line_op=0.13,
        vein0="#b86f3c", vein1="#e3b878", core="#fbe9c4", glow="#d9955a", glow_op=0.38,
        crystal="#ead2a1", crystal_op=0.72, facet="#7a4f2a",
        word="#f2efe7", tag="#cfc6b4", tag_op=0.85,
        grain_op=0.09, fault="#f2efe7", fault_op=0.16,
    ),
    "light": dict(
        bg0="#f4f1e9", bg1="#ebe6da",
        bands=["#ece7db", "#e4ded0", "#efeadf", "#ded7c7", "#e8e2d5", "#d9d1c0"],
        line="#1c1a16", line_op=0.12,
        vein0="#a8602e", vein1="#d9a45a", core="#f6dca6", glow="#c98a4c", glow_op=0.12,
        crystal="#d8b06c", crystal_op=0.78, facet="#6e4320",
        word="#1c1a16", tag="#4a4034", tag_op=0.9,
        grain_op=0.08, fault="#1c1a16", fault_op=0.14,
    ),
}


# --- geometry ---------------------------------------------------------------

def smooth_path(pts, closed=False):
    """Catmull-Rom through pts rendered as cubic Béziers."""
    if closed:
        pts = [pts[-1]] + pts + [pts[0], pts[1]]
    else:
        pts = [pts[0]] + pts + [pts[-1]]
    d = ["M%.1f %.1f" % pts[1]]
    for i in range(1, len(pts) - 2):
        p0, p1, p2, p3 = pts[i - 1], pts[i], pts[i + 1], pts[i + 2]
        c1 = (p1[0] + (p2[0] - p0[0]) / 6, p1[1] + (p2[1] - p0[1]) / 6)
        c2 = (p2[0] - (p3[0] - p1[0]) / 6, p2[1] - (p3[1] - p1[1]) / 6)
        d.append("C%.1f %.1f %.1f %.1f %.1f %.1f" % (c1 + c2 + p2))
    if closed:
        d.append("Z")
    return " ".join(d)


FAULTS = [(650, 12, 0.10), (1040, -9, -0.08)]  # x at surface, throw (px), inclination dx/dy


def fault_x(fx, incl, y):
    return fx + incl * (y - 300)


def throw(x, y):
    return sum(dy for fx, dy, incl in FAULTS if x >= fault_x(fx, incl, y))


def boundary(i, n, x):
    """y of stratum boundary i (0 = top) at x: a gentle anticline whose crest
    sits right of centre, tightening with depth, plus low-frequency waviness."""
    base = 150 + i * (H - 150) / (n - 1) * 1.02
    dome = 1.0 - i / (n + 2)              # upper layers arch more
    crest = math.exp(-((x - 840) / 300) ** 2)
    y = base - 95 * dome * crest
    y += 7 * math.sin(x / 210 + i * 0.9) + 4 * math.sin(x / 73 + i * 2.1)
    return y


def boundary_pts(i, n, step=16):
    pts = []
    x = -40
    while x <= W + 40:
        y = boundary(i, n, x)
        pts.append((x, y + throw(x, y)))
        x += step
    return pts


def strata(p, n=15):
    out = []
    bounds = [boundary_pts(i, n) for i in range(n)]
    for i in range(n):
        top = bounds[i]
        bot = bounds[i + 1] if i + 1 < n else [(x, H + 60) for x, _ in top]
        d = smooth_path(top) + " " + smooth_path(list(reversed(bot))).replace("M", "L", 1) + " Z"
        out.append('<path d="%s" fill="%s"/>' % (d, p["bands"][i % len(p["bands"])]))
    for i in range(n):
        out.append('<path d="%s" fill="none" stroke="%s" stroke-opacity="%.2f" stroke-width="%s"/>'
                   % (smooth_path(bounds[i]), p["line"], p["line_op"] * (1.25 if i % 3 == 0 else 0.8), "1.1" if i % 3 == 0 else "0.7"))
    for fx, dy, incl in FAULTS:
        y0, y1 = boundary(0, n, fx) + 2, H
        out.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-opacity="%.2f" stroke-width="1"/>'
                   % (fault_x(fx, incl, y0), y0, fault_x(fx, incl, y1), y1, p["fault"], p["fault_op"]))
    return "\n".join(out)


def vein_outline(center, width):
    """Closed path around a centerline with a per-point half-width."""
    left, right = [], []
    for k, (x, y) in enumerate(center):
        x0, y0 = center[max(k - 1, 0)]
        x1, y1 = center[min(k + 1, len(center) - 1)]
        tx, ty = x1 - x0, y1 - y0
        L = math.hypot(tx, ty) or 1
        nx, ny = -ty / L, tx / L
        w = width(k / (len(center) - 1))
        left.append((x + nx * w, y + ny * w))
        right.append((x - nx * w, y - ny * w))
    return smooth_path(left + list(reversed(right)), closed=True)


def vein_center(n=15):
    """The vein rides the crest of the fold, just under the second boundary,
    thickening where the fold is tightest."""
    pts = []
    for x in range(20, W + 1, 24):
        y = boundary(1, n, x) + 18 + 9 * math.sin(x / 140)
        pts.append((x, y))
    return pts


def vein(p, uid):
    c = vein_center()
    main = vein_outline(c, lambda t: 1.2 + 16 * math.exp(-((t - 0.63) / 0.26) ** 2) + 5 * math.exp(-((t - 0.22) / 0.12) ** 2))
    # two veinlets peeling off downward
    def branch(start, dx, dy, bend, n=9, w0=5):
        bx, by = c[start]
        pts = [(bx + dx * k / n + bend * math.sin(math.pi * k / n), by + dy * k / n + 6 * math.sin(2.4 * k / n)) for k in range(n + 1)]
        return vein_outline(pts, lambda t: w0 * (1 - t) ** 1.3 + 0.4)
    b1 = branch(27, -150, 120, -20)
    b2 = branch(36, 110, 150, 18, w0=4)
    core = smooth_path(c[6:-4])
    g = [
        f'<defs><linearGradient id="vg-{uid}" x1="0" x2="1" y1="0" y2="0"><stop offset="0" stop-color="{p["vein0"]}"/><stop offset="0.6" stop-color="{p["vein1"]}"/><stop offset="1" stop-color="{p["vein0"]}"/></linearGradient>'
        f'<filter id="glow-{uid}" x="-10%" y="-40%" width="120%" height="180%"><feGaussianBlur stdDeviation="9"/></filter></defs>',
        f'<g filter="url(#glow-{uid})" opacity="{p["glow_op"]}"><path d="{main}" fill="{p["glow"]}"/></g>',
        f'<path d="{b1}" fill="url(#vg-{uid})" opacity="0.9"/>',
        f'<path d="{b2}" fill="url(#vg-{uid})" opacity="0.9"/>',
        f'<path d="{main}" fill="url(#vg-{uid})"/>',
        f'<path d="{core}" fill="none" stroke="{p["core"]}" stroke-opacity="0.75" stroke-width="1.4" stroke-linecap="round"/>',
    ]
    return "\n".join(g)


def crystals(p, seed=7):
    """Small faceted shards clustered along the crest of the vein."""
    out = []
    c = vein_center()
    rnd = seed
    def r():
        nonlocal rnd
        rnd = (rnd * 1103515245 + 12345) % (2 ** 31)
        return rnd / 2 ** 31
    for k in range(11):
        i = int(len(c) * (0.44 + 0.40 * r()))
        x, y = c[i]
        y += (r() - 0.55) * 40
        x += (r() - 0.5) * 30
        s = 6 + r() * 9
        a = r() * math.pi
        pts = []
        for j in range(5):
            ang = a + j * 2 * math.pi / 5 + (r() - 0.5) * 0.5
            rad = s * (0.6 + 0.6 * r())
            pts.append((x + rad * math.cos(ang), y + rad * math.sin(ang) * 0.75))
        d = "M" + " L".join("%.1f %.1f" % q for q in pts) + " Z"
        out.append(f'<path d="{d}" fill="{p["crystal"]}" fill-opacity="{p["crystal_op"] * (0.55 + 0.45 * r()):.2f}"/>')
        out.append(f'<line x1="{pts[0][0]:.1f}" y1="{pts[0][1]:.1f}" x2="{pts[2][0]:.1f}" y2="{pts[2][1]:.1f}" stroke="{p["facet"]}" stroke-opacity="0.5" stroke-width="0.8"/>')
    return "\n".join(out)


# --- typography -----------------------------------------------------------------

def set_text(font, text, x, y, size, fill, opacity=1.0, tracking=0.0, kern=None):
    """Render text as outlines. x,y = baseline origin. tracking in em."""
    s = size / font["em"]
    kern = kern or {}
    out = [f'<g fill="{fill}" fill-opacity="{opacity}">']
    pen = 0.0
    for i, ch in enumerate(text):
        g = font["glyphs"][ch]
        if i and (text[i - 1] + ch) in kern:
            pen += kern[text[i - 1] + ch]
        if g["d"]:
            out.append(f'<path transform="translate({x + pen * s:.2f},{y:.2f}) scale({s:.5f},{-s:.5f})" d="{g["d"]}"/>')
        pen += g["width"] + tracking * font["em"]
    out.append("</g>")
    return "\n".join(out), pen * s


def grain(uid, opacity):
    return (f'<filter id="grain-{uid}" x="0" y="0" width="100%" height="100%">'
            f'<feTurbulence type="fractalNoise" baseFrequency="0.85" numOctaves="2" seed="11" stitchTiles="stitch"/>'
            f'<feColorMatrix type="matrix" values="0 0 0 0 0.5 0 0 0 0 0.5 0 0 0 0 0.5 0 0 0 {opacity * 3:.2f} 0"/></filter>')


def splash(variant):
    p = PAL[variant]
    uid = variant
    word, adv = set_text(REG, "Lode", 92, 436, 220, p["word"], kern={"Lo": -14, "od": 2, "de": -2})
    tag, _ = set_text(ITA, "Structured persistence for object storage.", 99, 482, 29, p["tag"], p["tag_op"], tracking=0.012)
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t-{uid} d-{uid}">',
        f'<title id="t-{uid}">Lode — structured persistence for object storage</title>',
        f'<desc id="d-{uid}">An engraved geological cross-section: folded strata drawn as fine contour lines, two faults offsetting them, and one luminous ore vein following the crest of the fold. The strata are immutable snapshots, the faults are partitions, the vein is the lode.</desc>',
        f'<defs><linearGradient id="bg-{uid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{p["bg0"]}"/><stop offset="1" stop-color="{p["bg1"]}"/></linearGradient>{grain(uid, p["grain_op"])}</defs>',
        f'<rect width="{W}" height="{H}" fill="url(#bg-{uid})"/>',
        strata(p),
        vein(p, uid),
        crystals(p),
        word, tag,
        f'<rect width="{W}" height="{H}" filter="url(#grain-{uid})" opacity="{p["grain_op"]}"/>',
        '</svg>',
    ])


def mark(variant, size=512):
    """The crest of the fold with the vein, cropped square, no type."""
    p = PAL[variant]
    uid = "m" + variant
    # crop window around the crest in splash coordinates
    cx, cy, half = 850, 190, 200
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{cx - half} {cy - half} {2 * half} {2 * half}" width="{size}" height="{size}" role="img" aria-label="Lode">',
        f'<defs><clipPath id="clip-{uid}"><rect x="{cx - half}" y="{cy - half}" width="{2 * half}" height="{2 * half}" rx="{half * 0.22:.0f}"/></clipPath>'
        f'<linearGradient id="bg-{uid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{p["bg0"]}"/><stop offset="1" stop-color="{p["bg1"]}"/></linearGradient>{grain(uid, p["grain_op"])}</defs>',
        f'<g clip-path="url(#clip-{uid})">',
        f'<rect x="{cx - half}" y="{cy - half}" width="{2 * half}" height="{2 * half}" fill="url(#bg-{uid})"/>',
        strata(p),
        vein(p, uid),
        crystals(p),
        f'<rect x="{cx - half}" y="{cy - half}" width="{2 * half}" height="{2 * half}" filter="url(#grain-{uid})" opacity="{p["grain_op"]}"/>',
        '</g></svg>',
    ])


for v in ("dark", "light"):
    (OUT / f"lode-splash-{v}.svg").write_text(splash(v) + "\n")
    (OUT / f"lode-mark-{v}.svg").write_text(mark(v) + "\n")
print("written", sorted(x.name for x in OUT.iterdir()))
