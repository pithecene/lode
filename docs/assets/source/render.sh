#!/usr/bin/env bash
# Renders the README splash and mark from splash.html (a WebGL fragment shader
# plus EB Garamond type) with headless Chromium, then encodes them as JPEG.
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
shot() { # mode width height output
  "$chrome" --headless --use-angle=swiftshader --enable-unsafe-swiftshader --allow-file-access-from-files \
    --hide-scrollbars --virtual-time-budget=8000 --window-size="$2,$3" --screenshot="$work/$1.png" "file://$work/splash.html#$1" >/dev/null 2>&1
  magick "$work/$1.png" -strip -quality 88 -sampling-factor 4:4:4 "$out/$4"
}
shot dark 2560 1040 lode-splash-dark.jpg
shot light 2560 1040 lode-splash-light.jpg
shot mark-dark 1024 1024 lode-mark-dark.jpg
shot mark-light 1024 1024 lode-mark-light.jpg
echo "rendered into $out"
