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

# The complete message keeps every line break, which the subject field of
# for-each-ref folds within its first paragraph. A signed tag appends its
# signature to the message, so the notes end before the signature begins.
# for-each-ref also ends each entry with a newline of its own, so trailing
# blank lines are dropped.
git for-each-ref --format='%(contents)' "refs/tags/${tag}" |
    awk '
        /^-----BEGIN [A-Z ]*SIGNATURE-----$/ { exit }
        { lines[count++] = $0 }
        END {
            while (count > 0 && lines[count - 1] == "") count--
            for (i = 0; i < count; i++) print lines[i]
        }
    ' > "${output}"
