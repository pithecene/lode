"""Generate the Lode splash (dark + light) and mark from one geometry.

Grammar (Pithecene house): horizontal strata = immutable snapshots, vertical
breaks = partitions, one bright line = the head pointer (`latest`). Palette is
the pithecene.sh token set (ink / cream / azulejo / douro) plus one ore metal.
"""
import pathlib, sys

OUT = pathlib.Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)

TOKENS = {
    "dark": dict(ground="#1C1A16", rock="#2A2722", lattice="#948270", strata=["#2E5A7A", "#4A7A9B", "#8AAFC4", "#4A7A9B", "#2E5A7A"],
                 ore="#C07A3E", latest="#F2EFE7", word="#F2EFE7", tag="#948270", fault="#1C1A16"),
    "light": dict(ground="#F2EFE7", rock="#E8E3D8", lattice="#625749", strata=["#2E5A7A", "#4A7A9B", "#8AAFC4", "#4A7A9B", "#2E5A7A"],
                  ore="#B5692E", latest="#1C1A16", word="#1C1A16", tag="#625749", fault="#F2EFE7"),
}

W, H = 1280, 400


def hex_pattern(pid, stroke, size=28, opacity=0.09):
    # pointy-top hexagon tile; pattern cell = size*sqrt(3) wide, size*1.5 tall
    import math
    w = size * math.sqrt(3)
    h = size * 1.5
    pts = []
    for i in range(6):
        a = math.radians(60 * i - 30)
        pts.append((w / 2 + size * math.cos(a), size + size * math.sin(a)))
    d = "M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in pts) + " Z"
    # second hexagon offset half a cell so the lattice tiles seamlessly
    d2 = "M" + " L".join(f"{x + w / 2:.2f},{y - h:.2f}" for x, y in pts) + " Z"
    d3 = "M" + " L".join(f"{x - w / 2:.2f},{y - h:.2f}" for x, y in pts) + " Z"
    return (f'<pattern id="{pid}" patternUnits="userSpaceOnUse" width="{w:.2f}" height="{h * 2:.2f}">'
            f'<g fill="none" stroke="{stroke}" stroke-opacity="{opacity}" stroke-width="1">'
            f'<path d="{d}"/><path d="{d2}"/><path d="{d3}"/>'
            f'<path d="{"M" + " L".join(f"{x:.2f},{y + h * 2:.2f}" for x, y in pts) + " Z"}"/>'
            f'<path d="{"M" + " L".join(f"{x + w / 2:.2f},{y + h:.2f}" for x, y in pts) + " Z"}"/>'
            f'<path d="{"M" + " L".join(f"{x - w / 2:.2f},{y + h:.2f}" for x, y in pts) + " Z"}"/>'
            f'</g></pattern>')


def seam(t, x0, x1, top, weights, faults, dip=0.38, lens_layers=(1, 4), bed_lines=True, thick0=88):
    """A layered seam from x0..x1. top(x) is the top edge (piecewise linear);
    weights are relative stratum thicknesses (total = thickness(x)); faults are
    (x, dy, thickness_scale) breaks: everything right of x drops by dy. Faults
    dip (slant) rather than stand vertical."""
    def offset(x):
        return sum(dy for fx, dy in faults if x >= fx)
    total_w = sum(weights)
    segs = [x0] + [fx for fx, _ in faults] + [x1]
    out = []
    for i in range(len(segs) - 1):
        a, b = segs[i], segs[i + 1]
        off = offset(a + 1)
        gap = 7
        # edge x as a function of depth below the seam top: slanted at faults
        def xl(depth, a=a, i=i):
            return a + (gap if i > 0 else 0) + (dip * depth if i > 0 else 0)
        def xr(depth, b=b, i=i):
            return b - (gap if i < len(segs) - 2 else 0) + (dip * depth if i < len(segs) - 2 else 0)
        y_acc_a = top(a) + off
        y_acc_b = top(b) + off
        thick = thick0 * (1 + 0.18 * i)  # the seam thickens to the right
        d_acc = 0.0
        for k, w in enumerate(weights):
            h = thick * w / total_w
            ya0, yb0 = y_acc_a, y_acc_b
            ya1, yb1 = ya0 + h, yb0 + h
            col = t["strata"][k % len(t["strata"])]
            out.append(f'<polygon points="{xl(d_acc):.1f},{ya0:.1f} {xr(d_acc):.1f},{yb0:.1f} {xr(d_acc + h):.1f},{yb1:.1f} {xl(d_acc + h):.1f},{ya1:.1f}" fill="{col}"/>')
            if k > 0:
                out.append(f'<line x1="{xl(d_acc):.1f}" y1="{ya0:.1f}" x2="{xr(d_acc):.1f}" y2="{yb0:.1f}" stroke="{t["ground"]}" stroke-opacity="0.5" stroke-width="1.2"/>')
            if k in lens_layers:
                # ore lenses: a few elongated parallelograms following the bedding
                n = 1 + i
                for j in range(n):
                    fx = xl(d_acc) + (j + 0.6) * (xr(d_acc) - xl(d_acc)) / (n + 0.4)
                    L = 16 + (j * 11 + k * 7) % 14
                    hh = h * 0.3
                    slope = (yb0 - ya0) / max(1, (b - a))
                    cy = ya0 + (fx - a) * slope + h * 0.5
                    out.append(f'<polygon points="{fx - L:.1f},{cy - slope * L:.1f} {fx + L:.1f},{cy + slope * L:.1f} {fx + L - 6:.1f},{cy + slope * L + hh:.1f} {fx - L - 6:.1f},{cy - slope * L + hh:.1f}" fill="{t["ore"]}"/>')
            y_acc_a, y_acc_b = ya1, yb1
            d_acc += h
        # the head pointer: a bright line along the top of the seam
        out.append(f'<line x1="{xl(0):.1f}" y1="{top(a) + off:.1f}" x2="{xr(0):.1f}" y2="{top(b) + off:.1f}" stroke="{t["latest"]}" stroke-width="4" stroke-linecap="butt"/>')
    if bed_lines:
        # faint bedding in the surrounding rock, parallel to the seam
        for dy in (-70, -40, 150, 190, 240):
            pts = " ".join(f"{x:.0f},{top(x) + (thick0 * 1.5 if dy > 0 else 0) + dy:.1f}" for x in range(int(x0), int(x1) + 1, 40))
            out.append(f'<polyline points="{pts}" fill="none" stroke="{t["lattice"]}" stroke-opacity="0.18" stroke-width="1.2"/>')
    return "\n".join(out)


def wordmark(t, x, y, h, sw):
    """LODE in geometric strokes. h = cap height, sw = stroke width."""
    c = t["word"]
    g = []
    # L
    g.append(f'<path d="M{x},{y} V{y + h} H{x + h * 0.62}" fill="none" stroke="{c}" stroke-width="{sw}" stroke-linejoin="miter"/>')
    x += h * 0.62 + sw * 1.4
    # O — a rounded rectangle ring
    g.append(f'<rect x="{x + sw / 2}" y="{y + sw / 2 - sw / 2}" width="{h * 0.78 - sw}" height="{h - sw + sw / 2 - sw / 2}" rx="{h * 0.22}" fill="none" stroke="{c}" stroke-width="{sw}" transform="translate(0,{sw / 2})"/>')
    x += h * 0.78 + sw * 1.4
    # D — stem plus half-round
    r = h / 2
    g.append(f'<path d="M{x + sw / 2},{y} V{y + h} M{x + sw / 2},{y + sw / 2} H{x + h * 0.32} A{r - sw / 2},{r - sw / 2} 0 0 1 {x + h * 0.32},{y + h - sw / 2} H{x + sw / 2}" fill="none" stroke="{c}" stroke-width="{sw}" stroke-linejoin="miter"/>')
    x += h * 0.32 + r + sw * 1.1
    # E — stem and three bars; middle bar shorter (the seam reads through it)
    g.append(f'<path d="M{x + sw / 2},{y} V{y + h} M{x},{y + sw / 2} H{x + h * 0.6} M{x},{y + h / 2} H{x + h * 0.48} M{x},{y + h - sw / 2} H{x + h * 0.6}" fill="none" stroke="{c}" stroke-width="{sw}" stroke-linejoin="miter"/>')
    return "\n".join(g)


def splash(variant):
    t = TOKENS[variant]
    pid = f"hex-{variant}"
    # seam geometry: enters low at left under the wordmark, rises to the right
    ctrl = [(0, 298), (300, 284), (640, 234), (1000, 188), (1280, 158)]
    def top(x):
        for (xa, ya), (xb, yb) in zip(ctrl, ctrl[1:]):
            if xa <= x <= xb:
                return ya + (yb - ya) * (x - xa) / (xb - xa)
        return ctrl[-1][1]
    body = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t d">',
        '<title id="t">Lode</title>',
        '<desc id="d">A geological cross-section: a layered seam of ore running through dark rock. The strata are immutable snapshots, the vertical breaks are partitions, the bright line along the top of the seam is the latest pointer.</desc>',
        f'<defs>{hex_pattern(pid, t["lattice"], opacity=0.10 if variant == "dark" else 0.055)}</defs>',
        f'<rect width="{W}" height="{H}" fill="{t["ground"]}"/>',
        f'<rect width="{W}" height="{H}" fill="url(#{pid})"/>',
        # a slightly lighter rock mass below the seam, so the seam sits *in* something
        f'<polygon points="0,{top(0) + 120:.0f} 520,{top(520) + 128:.0f} 1280,{top(1280) + 150:.0f} {W},{H} 0,{H}" fill="{t["rock"]}" fill-opacity="0.7"/>',
        seam(t, 0, W, top, [1.2, 2.2, 0.8, 1.6, 2.6, 1.0, 1.4], [(520, 26), (900, -34)]),
        wordmark(t, 84, 58, 118, 24),
        f'<text x="86" y="236" font-family="JetBrains Mono, ui-monospace, SFMono-Regular, Menlo, monospace" font-size="19" letter-spacing="0.08em" fill="{t["tag"]}">STRUCTURED PERSISTENCE FOR OBJECT STORAGE</text>',
        '</svg>',
    ]
    return "\n".join(body)


def mark(variant, size=512):
    t = TOKENS[variant]
    s = size
    def top(x):
        return s * 0.40 - (x - s * 0.1) * 0.15
    body = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {s} {s}" width="{s}" height="{s}" role="img" aria-label="Lode">',
        f'<rect width="{s}" height="{s}" rx="{s * 0.12:.0f}" fill="{t["ground"]}"/>',
        f'<clipPath id="sq"><rect width="{s}" height="{s}" rx="{s * 0.12:.0f}"/></clipPath><g clip-path="url(#sq)">',
        seam(t, -s * 0.2, s * 1.2, top, [1.6, 2.6, 1.8], [(s * 0.5, s * 0.08)], dip=0.42, lens_layers=(1,), bed_lines=False, thick0=s * 0.24).replace("stroke-width=\"4\"", f"stroke-width=\"{s * 0.02:.0f}\""),
        '</g></svg>',
    ]
    return "\n".join(body)


for v in ("dark", "light"):
    (OUT / f"lode-splash-{v}.svg").write_text(splash(v) + "\n")
    (OUT / f"lode-mark-{v}.svg").write_text(mark(v) + "\n")
print("written", sorted(p.name for p in OUT.iterdir()))
