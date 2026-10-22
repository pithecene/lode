#!/usr/bin/env bash
# Renders the README banner: plate.py generates the engraved, metal-tinted
# section (strokes as SVG, washes as a raster), headless Chromium rasterizes
# the strokes and then sets the plate under the wordmark in banner.html at 4x,
# and the result is downsampled to 2560x960.
#
#   docs/assets/source/render.sh [path-to-chrome-or-chrome-headless-shell]
#
# Needs: Python 3 with NumPy, a Chromium build, ImageMagick (`magick`), curl.
# The plate takes several minutes to generate. Fonts (Space Grotesk, Inter,
# EB Garamond; SIL OFL) are fetched into a temp directory; they are not committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$(cd "$here/.." && pwd)"
chrome="${1:-$(command -v chrome-headless-shell || command -v chromium || command -v google-chrome)}"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
cp "$here/banner.html" "$work/"
mkdir -p "$work/fonts"
curl -sfL -o "$work/fonts/SpaceGrotesk.ttf" "https://github.com/google/fonts/raw/main/ofl/spacegrotesk/SpaceGrotesk%5Bwght%5D.ttf"
curl -sfL -o "$work/fonts/Inter.ttf" "https://github.com/google/fonts/raw/main/ofl/inter/Inter%5Bopsz,wght%5D.ttf"
curl -sfL -o "$work/fonts/EBGaramond.ttf" "https://github.com/google/fonts/raw/main/ofl/ebgaramond/EBGaramond%5Bwght%5D.ttf"

(cd "$work" && python3 "$here/plate.py" plate >/dev/null)
read -r pw ph < <(head -c 64 "$work/plate-light-base.ppm" | awk 'NR==1{print $2, $3}')
for mode in light dark; do
  printf '<!doctype html><html><body style="margin:0;background:transparent"><img src="plate-%s.svg" width="%s" height="%s"></body></html>' \
    "$mode" "$pw" "$ph" > "$work/strokes-$mode.html"
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=1 \
    --default-background-color=00000000 --window-size="$pw,$ph" \
    --screenshot="$work/ink-$mode.png" "file://$work/strokes-$mode.html" >/dev/null 2>&1
  magick "$work/plate-$mode-base.ppm" "$work/ink-$mode.png" -compose over -composite "$work/lode-$mode.png"
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=4 \
    --default-background-color=00000000 --virtual-time-budget=15000 --window-size=1280,480 \
    --screenshot="$work/raw-$mode.png" "file://$work/banner.html#lode-$mode" >/dev/null 2>&1
  magick "$work/raw-$mode.png" -filter Lanczos -resize 2560x960 -strip "$out/lode-banner-$mode.png"
done
echo "rendered into $out"
