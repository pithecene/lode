#!/usr/bin/env bash
# Renders the README splash from splash.html (an illustrated geological section drawn in SVG, set in EB Garamond)
# with headless Chromium at 2x, light and dark.
#
#   docs/assets/source/render.sh [path-to-chrome-or-chrome-headless-shell]
#
# Needs: a Chromium build, ImageMagick (`magick`), curl. EB Garamond (SIL OFL)
# is fetched into a temp directory next to the page; it is not committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$(cd "$here/.." && pwd)"
chrome="${1:-$(command -v chrome-headless-shell || command -v chromium || command -v google-chrome)}"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
cp "$here/splash.html" "$work/"
mkdir -p "$work/fonts"
curl -sfL -o "$work/fonts/EBGaramond.ttf" "https://github.com/google/fonts/raw/main/ofl/ebgaramond/EBGaramond%5Bwght%5D.ttf"
curl -sfL -o "$work/fonts/EBGaramond-Italic.ttf" "https://github.com/google/fonts/raw/main/ofl/ebgaramond/EBGaramond-Italic%5Bwght%5D.ttf"
for mode in light dark; do
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=2 \
    --virtual-time-budget=5000 --window-size=1280,560 --screenshot="$work/$mode.png" "file://$work/splash.html#$mode" >/dev/null 2>&1
  magick "$work/$mode.png" -strip -define png:compression-level=9 "$out/lode-splash-$mode.png"
done
echo "rendered into $out"
