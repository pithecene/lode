"""Lode README plate: an engraved section of folded, faulted strata cut by a
gold-bearing lode, hand-coloured in tarnished metals.

The engraving is laid down as explicit strokes, not a fill pattern: evenly
spaced streamlines follow the bedding, packed closer and drawn heavier where
the rock is in shadow and ending in the light; a cross-hatch set enters only in
deep shadow; the sky is ruled in horizontal lines that thin out to the right.
Under the strokes sits a raster of metallic washes, one metal per bed, with a
sheen where each ledge catches the light.

    python3 plate.py OUT       # writes OUT-{light,dark}.svg and OUT-{light,dark}-base.ppm

PLATE_SCALE (pixels per design unit, default 3) and STROKE_K (stroke spacing
and weight, default 1.6) are the only knobs. Deterministic for fixed seeds.
"""
import math
import sys
import numpy as np

W, H = 1760, 1104          # design units
S = int(__import__('os').environ.get('PLATE_SCALE', '3'))   # pixels per unit
PW, PH = W * S, H * S
K = float(__import__('os').environ.get('STROKE_K', '1.6'))  # stroke spacing/weight factor
rng = np.random.default_rng(11)
jitter = np.random.default_rng(12)   # stroke spacing jitter; seeded so renders are reproducible
yy, xx = (np.mgrid[0:PH, 0:PW].astype(np.float64) + 0.5) / S


def fbm(scale, octaves=3, seed=0):
    r = np.random.default_rng(seed)
    out = np.zeros((PH, PW)); amp = 1.0; tot = 0.0
    for o in range(octaves):
        s = scale / (2 ** o)
        g = r.random((int(H / s) + 3, int(W / s) + 3))
        fx, fy = xx / s, yy / s
        x0, y0 = np.floor(fx).astype(int), np.floor(fy).astype(int)
        tx, ty = fx - x0, fy - y0
        tx, ty = tx * tx * (3 - 2 * tx), ty * ty * (3 - 2 * ty)
        a = g[y0, x0]; b = g[y0, x0 + 1]; c = g[y0 + 1, x0]; d = g[y0 + 1, x0 + 1]
        out += amp * (a + (b - a) * tx + (c - a) * ty + (a - b - c + d) * tx * ty)
        tot += amp; amp *= 0.5
    return out / tot


def ridge_line(xs, base, amp, rough, seed):
    r = np.random.default_rng(seed)
    pts = np.array([0.0, 1.0]); vals = np.array([0.0, 0.0])
    for lvl in range(9):
        mid = (pts[:-1] + pts[1:]) / 2
        mv = (vals[:-1] + vals[1:]) / 2 + (r.random(len(mid)) - 0.5) * rough * 0.55 ** lvl
        pts = np.insert(pts, np.arange(1, len(pts)), mid)
        vals = np.insert(vals, np.arange(1, len(vals)), mv)
    return base(xs) + amp * np.interp(xs / W, pts, vals)


# --- composition ---------------------------------------------------------------------
cols = (np.arange(PW, dtype=float) + 0.5) / S
top = ridge_line(cols, lambda x: 120 + 380 * (1 / (1 + np.exp(-(x - 1150) / 140))), 180, 1.0, 3)
rock = yy > top[None, :]

n = 34
sx = rng.uniform(-60, W + 60, n); sy = rng.uniform(80, H + 40, n)
d = np.full((PH, PW), 1e12); d2 = np.full((PH, PW), 1e12); cell = np.zeros((PH, PW), int)
wx = 50 * (fbm(170, 2, 5) - 0.5); wy = 50 * (fbm(170, 2, 6) - 0.5)
for i in range(n):
    dist = (xx + wx - sx[i]) ** 2 + 1.5 * (yy + wy - sy[i]) ** 2
    m = dist < d
    d2 = np.where(m, d, np.minimum(d2, dist)); d = np.where(m, dist, d); cell = np.where(m, i, cell)
edge = np.sqrt(d2) - np.sqrt(d)
nx = rng.normal(0, 0.5, n); ny = rng.normal(-0.1, 0.4, n)
L = np.array([-0.55, -0.6, 0.58]); L /= np.linalg.norm(L)
lam = (nx * L[0] + ny * L[1] + L[2]) / np.sqrt(nx ** 2 + ny ** 2 + 1)
light = lam[cell]

fold = 150 * np.exp(-((xx - 520) / 430) ** 2) - 70 * np.exp(-((xx - 1250) / 330) ** 2) + 0.18 * xx
depth = yy - fold + 18 * (fbm(260, 2, 7) - 0.5) + 7 * (fbm(46, 3, 8) - 0.5)
fault = (xx - 980) - 0.5 * (yy - 560)
depth = depth + np.where(fault > 0, 38.0, 0.0)
METALS = [(186, 112, 62), (112, 120, 130), (172, 132, 64), (84, 92, 104), (196, 196, 192),
          (160, 98, 58), (150, 112, 70), (190, 158, 82)]   # copper, gunmetal, bronze, iron, silver, red copper, patina, brass
thick = rng.uniform(40, 110, 30)
bounds = np.concatenate([[-3000.0], np.cumsum(thick) - 500.0, [1e6]])
bed = np.clip(np.searchsorted(bounds, depth) - 1, 0, len(bounds) - 2)
lo = bounds[bed]; hi = bounds[bed + 1]
within = np.clip((depth - lo) / np.maximum(hi - lo, 1), 0, 1)
bed_dark = np.array([rng.choice([0.0, 0.03, 0.12, 0.2]) for _ in range(len(bounds))])[bed]

# relief: a weathered cliff. Hard beds stand proud as ledges, soft beds are
# recessed; the whole face bulges in broad masses. Lit from the upper left.
hard_bed = np.array([rng.choice([0.0, 1.0, 0.4]) for _ in range(len(bounds))])[bed]
ledge = hard_bed * np.sin(np.pi * within) ** 0.6
hgt = 260 * fbm(520, 2, 21) + 18 * ledge
gyh, gxh = np.gradient(hgt, 1 / S)
Ld = np.array([-0.62, -0.62, 0.48]); Ld /= np.linalg.norm(Ld)
lam = (-gxh * Ld[0] - gyh * Ld[1] + Ld[2]) / np.sqrt(gxh ** 2 + gyh ** 2 + 1)
# cast shadow below each proud ledge
under = hard_bed * np.exp(-((1 - within) / 0.18) ** 2)
tone = 0.78 - 1.0 * lam + 0.30 * under + 0.5 * bed_dark
tone += 0.16 * (yy / H) + 0.10 * ((xx - 500) / W) - 0.05
tone += 0.28 * np.exp(-np.clip(yy - top[None, :], 0, None) / 24) * rock
tone += 0.03 * (fbm(28, 2, 9) - 0.5)
tone = np.clip(tone, 0.02, 0.96)

# the lode: a main vein swelling and pinching, with two stringers branching off
def vein_dist(t0, t1, ax, ay, bx, by, wob, seed, w0, w1):
    t = np.linspace(0, 1, 400)
    r = np.random.default_rng(seed)
    ph = r.uniform(0, 6, 3)
    px = ax + (bx - ax) * t + wob * np.sin(t * 6 + ph[0]) + 0.3 * wob * np.sin(t * 17 + ph[1])
    py = ay + (by - ay) * t + 0.6 * wob * np.sin(t * 11 + ph[2])
    best = np.full((PH, PW), 1e12); tp = np.zeros((PH, PW))
    for i in range(len(t)):
        q = (xx - px[i]) ** 2 + (yy - py[i]) ** 2
        m = q < best; best = np.where(m, q, best); tp = np.where(m, t[i], tp)
    dd = np.sqrt(best)
    wv = (w0 + (w1 - w0) * np.sin(np.pi * tp) ** 0.8) * (0.7 + 0.6 * fbm(60, 2, seed + 1))
    wv *= np.clip(np.minimum(tp, 1 - tp) * 12, 0.15, 1)       # pinch out at the ends
    return dd, wv, tp

dv, vw, tpar = vein_dist(0, 1, 40, 1010, 1260, 200, 36, 31, 5, 15)
d1, w1_, _ = vein_dist(0, 1, 470, 690, 820, 930, 8, 41, 1.2, 4)
d2_, w2_, _ = vein_dist(0, 1, 900, 430, 1180, 560, 6, 51, 1.0, 3.2)
u_all = np.minimum.reduce([dv / np.maximum(vw, 0.5), d1 / np.maximum(w1_, 0.5), d2_ / np.maximum(w2_, 0.5)])
vein = (u_all < 1) & rock
u = np.where(vein, u_all, 1.0)
quartz = vein & (fbm(5, 2, 9) > 0.72)
gold = vein & ~quartz
# across-vein direction, for its own strokes
gvy, gvx = np.gradient(np.minimum.reduce([dv - vw, d1 - w1_, d2_ - w2_]), 1 / S)

# --- direction field: along the bedding ---------------------------------------------
gy, gx = np.gradient(depth, 1 / S)
gl = np.maximum(np.hypot(gx, gy), 1e-6)
dirx, diry = gy / gl, -gx / gl          # tangent to the bedding isolines
dirx = np.where(dirx < 0, -dirx, dirx); diry = np.where(gy / gl < 0, -diry, diry)
rot = 0.0
dirx, diry = dirx * np.cos(rot) - diry * np.sin(rot), dirx * np.sin(rot) + diry * np.cos(rot)


def sample(a, x, y):
    xi = min(max(int(x * S), 0), PW - 1); yi = min(max(int(y * S), 0), PH - 1)
    return a[yi, xi]


class Grid:
    """Spatial hash of stroke points, partitioned by region so strokes in
    neighbouring facets run right up to the shared edge."""
    def __init__(self, cs):
        self.cs = cs; self.cells = {}

    def near(self, x, y, r, reg):
        cs = self.cs; cx, cy = int(x // cs), int(y // cs); k = int(math.ceil(r / cs))
        r2 = r * r
        for i in range(cx - k, cx + k + 1):
            for j in range(cy - k, cy + k + 1):
                for (px, py) in self.cells.get((reg, i, j), ()):
                    if (px - x) ** 2 + (py - y) ** 2 < r2:
                        return True
        return False

    def add(self, pts, reg):
        cs = self.cs
        for (x, y) in pts[::2]:
            self.cells.setdefault((reg, int(x // cs), int(y // cs)), []).append((x, y))


def streamlines(mask, fdir, tone_of, sep_of, max_len, seed, step=1.6, region=None):
    """Evenly spaced streamlines (Jobard-Lefer style) inside mask."""
    r = np.random.default_rng(seed)
    grid = Grid(4.0)
    lines = []
    cand = np.argwhere(mask)
    r.shuffle(cand)
    for (yi, xi) in cand[: len(cand) // (2 * S * S)]:
        x, y = (float(xi) + r.random()) / S, (float(yi) + r.random()) / S
        reg = int(sample(region, x, y)) if region is not None else 0
        tn = tone_of(x, y)
        sep = sep_of(tn)
        if sep is None or grid.near(x, y, sep, reg):
            continue
        pts = [(x, y)]
        hard = {}
        for sgn in (1, -1):
            px, py = x, y
            seg = []
            hard[sgn] = False
            for _ in range(int(max_len / step)):
                dx, dy = fdir(px, py)
                px += sgn * dx * step; py += sgn * dy * step
                if not (0 <= px < W and 0 <= py < H) or not sample(mask, px, py):
                    hard[sgn] = True
                    break
                if region is not None and int(sample(region, px, py)) != reg:
                    hard[sgn] = True
                    break
                tt = tone_of(px, py); ss = sep_of(tt)
                if ss is None or grid.near(px, py, 0.55 * ss, reg):
                    break
                seg.append((px, py))
            pts = (seg[::-1] + pts) if sgn == -1 else (pts + seg)
        if len(pts) < 6:
            continue
        grid.add(pts, reg)
        lines.append((pts, hard[-1], hard[1]))
    return lines


def rock_dir(x, y):
    return sample(dirx, x, y), sample(diry, x, y)


def rock_tone(x, y):
    return sample(tone, x, y)


def rock_sep(tn):
    if tn < 0.07:
        return None
    return K * (2.2 + 6.0 * (1 - tn) ** 1.8) * (0.8 + 0.4 * jitter.random())


face = rock & ~vein
lines_main = streamlines(face, rock_dir, rock_tone, rock_sep, 420, 1, region=bed)

# cross-hatch in deep shadow, at 60 degrees to the bedding
cos60, sin60 = 0.5, 0.866


def cross_dir(x, y):
    dx, dy = rock_dir(x, y)
    return dx * cos60 - dy * sin60, dx * sin60 + dy * cos60


def cross_sep(tn):
    return None if tn < 0.56 else K * (2.8 + 9 * (1 - (tn - 0.56) / 0.44))


lines_cross = streamlines(face & (tone > 0.56), cross_dir, rock_tone, cross_sep, 160, 2, region=bed)

gn = np.maximum(np.hypot(gvx, gvy), 1e-6)
vdx, vdy = -gvy / gn, gvx / gn          # along the vein
vein_tone = np.clip(0.3 + 0.25 * (fbm(14, 2, 90) - 0.5) + 0.35 * (u > 0.82), 0, 1)
vein_tone = np.where(quartz, 0.0, vein_tone)


def vdir(x, y):
    return sample(vdx, x, y), sample(vdy, x, y)


def vtone(x, y):
    return sample(vein_tone, x, y)


def vsep(tn):
    return None if tn < 0.08 else K * (2.0 + 3.5 * (1 - tn))


lines_vein = streamlines(vein, vdir, vtone, vsep, 60, 3, step=1.0)

# sky: ruled horizontal lines, darker high up, thinning and stopping toward the right
sky_tone = np.clip(0.30 - 0.26 * (yy / 560) - 0.16 * (xx / W) + 0.10 * (fbm(260, 2, 50) - 0.5), 0, 1)
sky_lines = []
y = 6.0
while y < H:
    xs = np.arange(0, W, 2.0)
    run = []
    for x in xs:
        if sample(rock, x, y) or sample(sky_tone, x, y) < 0.06:
            if len(run) > 5:
                sky_lines.append(run)
            run = []
            continue
        run.append((x, y + 1.2 * math.sin(x / 140 + y / 37)))
    if len(run) > 5:
        sky_lines.append(run)
    y += K * (3.4 + 2.0 * (y / H))


def to_svg(lines, width_of, color, fh, taper=0.18):
    for item in lines:
        if isinstance(item, tuple):
            pts, hard_start, hard_end = item
        else:
            pts, hard_start, hard_end = item, False, False
        n_ = len(pts)
        for k in range(0, n_ - 1, 3):
            seg = pts[k:k + 4]
            if len(seg) < 2:
                break
            x, y = seg[0]
            tn = sample(tone, x, y) if sample(rock, x, y) else sample(sky_tone, x, y)
            pos = k / max(n_ - 1, 1)
            a = 1.0 if hard_start else min(1.0, pos / taper)
            b = 1.0 if hard_end else min(1.0, (1 - pos) / taper)
            w = width_of(tn) * (0.25 + 0.75 * min(a, b))
            if w < 0.15:
                continue
            dpath = "M" + " L".join(f"{px + 0.5 * math.sin(py * 0.37 + px * 0.11):.1f} {py + 0.5 * math.sin(px * 0.29):.1f}" for px, py in seg)
            fh.write(f'<path d="{dpath}" stroke-width="{w:.2f}"/>\n')


def main_w(tn):
    return K * (0.45 + 2.3 * tn ** 1.1)


def cross_w(tn):
    return K * (0.35 + 1.4 * max(0.0, tn - 0.56) / 0.44)


def sky_w(tn):
    return K * (0.25 + 2.2 * tn)


for theme, ink, vink in (("light", "#2a2219", "#5c3a0c"), ("dark", "#0b1020", "#4a2e08")):
    with open(f"{sys.argv[1]}-{theme}.svg", "w") as fh:
        fh.write(f'<svg xmlns="http://www.w3.org/2000/svg" width="{PW}" height="{PH}" viewBox="0 0 {W} {H}">'
                 f'<g fill="none" stroke="{ink}" stroke-linecap="round" stroke-linejoin="round">\n')
        to_svg(lines_main, main_w, ink, fh)
        to_svg(lines_cross, cross_w, ink, fh, taper=0.3)
        to_svg(sky_lines, sky_w, ink, fh, taper=0.08)
        fh.write(f'</g><g fill="none" stroke="{vink}" stroke-linecap="round">\n')
        for item in lines_vein:
            pts = item[0]
            x, y = pts[len(pts) // 2]
            w = K * (0.3 + 1.0 * vtone(x, y))
            dpath = "M" + " L".join(f"{px:.1f} {py:.1f}" for px, py in pts[::2])
            fh.write(f'<path d="{dpath}" stroke-width="{w:.2f}"/>\n')
        # cliff outline and selvage of the vein
        ol = " L".join(f"{x:.1f} {top[int(x * S)]:.1f}" for x in range(0, W, 3))
        fh.write(f'<path d="M{ol}" stroke-width="1.6"/>\n')
        fh.write("</g></svg>\n")

# --- underlayer: paper, metallic washes, the vein --------------------------------------
def underlayer(paper, wash, out):
    paper = np.array(paper, float)
    metal = np.array(METALS, float)[bed % len(METALS)]
    grey = metal.mean(axis=-1, keepdims=True)
    metal = grey + 0.72 * (metal - grey)          # tarnished, not polished: an old hand-coloured plate
    # metallic wash: brighter where the bed is lit (lam), with a bright band along each ledge's top
    k = (wash * rock)[..., None]
    # polished-metal response: a dark body, a broad sheen where lit, a narrow specular streak on each ledge
    spec = np.exp(-((within - 0.12) / 0.05) ** 2) * np.clip(lam - 0.2, 0, 1)
    shine = np.clip(0.62 + 0.8 * np.clip(lam, 0, 1) ** 1.5 + 0.6 * spec, 0.5, 1.5)[..., None]
    base = paper * (1 - k) + np.clip(metal * shine, 0, 255) * k
    sheen = np.clip(1.0 + 0.35 * (fbm(6, 2, 80) - 0.5) + 0.2 * (fbm(40, 2, 81) - 0.5), 0.7, 1.3)[..., None]
    base = np.where(gold[..., None], np.array([196, 150, 52.0]) * sheen, base)
    base = np.where(quartz[..., None], paper * 0.97 + 6, base)
    selv = vein & (u > 0.86)
    base = np.where(selv[..., None], base * 0.4, base)
    img = np.clip(base, 0, 255).astype(np.uint8)
    with open(out, "wb") as fh:
        fh.write(b"P6 %d %d 255\n" % (PW, PH)); fh.write(img.tobytes())


underlayer((244, 236, 218), 0.5, f"{sys.argv[1]}-light-base.ppm")
underlayer((242, 227, 194), 0.48, f"{sys.argv[1]}-dark-base.ppm")
print(len(lines_main), len(lines_cross), len(sky_lines))
