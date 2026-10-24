#!/usr/bin/env bash
#
# release-notes.sh — Write the release notes that an annotated tag carries
#
# The notes of a release are the message of its annotated tag. A maintainer
# writes them with verbatim cleanup, because Git otherwise strips each line
# that begins with "#" and so removes every Markdown heading:
#
#   git tag -a vX.Y.Z -F notes.md --cleanup=verbatim
#
# The release workflow checks the notes before it packages the release and
# publishes them as the release body.
#
# Usage: release-notes.sh <tag> <output-file>
#
# Exit codes:
#   0 — notes written
#   1 — the tag is missing or is not annotated
#   2 — usage error
#
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: release-notes.sh <tag> <output-file>" >&2
    exit 2
fi

tag="$1"
output="$2"

# A lightweight tag has no message, so it cannot carry release notes.
if [ "$(git cat-file -t "refs/tags/${tag}" 2>/dev/null)" != "tag" ]; then
    echo "release-notes.sh: ${tag} is not an annotated tag" >&2
    exit 1
fi

# The subject and the body omit any signature that a signed tag carries.
git for-each-ref --format='%(contents:subject)%0a%0a%(contents:body)' \
    "refs/tags/${tag}" > "${output}"
