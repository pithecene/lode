#!/usr/bin/env bash
#
# verify-snippets.sh — Verify runnable markdown snippets
#
# Extracts code fences marked with <!-- runnable --> and validates them.
# - bash: syntax check with bash -n
# - go: compile check (wrapped in temp package)
#
# Exit codes:
#   0 — all runnable snippets valid
#   1 — one or more snippets failed validation
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Files to check (relative to repo root)
FILES=(
    "README.md"
    "PUBLIC_API.md"
)

ERRORS=0
CHECKED=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

# Verify bash snippet syntax
verify_bash() {
    local file="$1"
    local line="$2"
    local content="$3"

    local tmpfile
    tmpfile=$(mktemp)
    echo "$content" > "$tmpfile"

    if bash -n "$tmpfile" 2>/dev/null; then
        log_info "$file:$line — bash syntax OK"
    else
        log_error "$file:$line — bash syntax error"
        echo "    Content:"
        echo "$content" | sed 's/^/    /'
        ((ERRORS++)) || true
    fi

    rm -f "$tmpfile"
}

# Verify go snippet compiles
verify_go() {
    local file="$1"
    local line="$2"
    local content="$3"

    local tmpdir
    tmpdir=$(mktemp -d)

    # Check if content already has package declaration
    if echo "$content" | grep -q '^package '; then
        echo "$content" > "$tmpdir/main.go"
    else
        # Wrap in package main
        cat > "$tmpdir/main.go" <<GOEOF
package main

import (
    "context"
    "fmt"
    _ "github.com/pithecene/lode/lode"
)

var _ = context.Background
var _ = fmt.Sprintf

func main() {
$content
}
GOEOF
    fi

    # Initialize module and try to build
    (
        cd "$tmpdir"
        go mod init snippet 2>/dev/null
        go mod edit -replace github.com/pithecene/lode="$REPO_ROOT" 2>/dev/null
    ) || true

    if (cd "$tmpdir" && go build -o /dev/null ./... 2>/dev/null); then
        log_info "$file:$line — go compiles OK"
    else
        log_error "$file:$line — go compilation error"
        echo "    Content:"
        echo "$content" | sed 's/^/    /'
        ((ERRORS++)) || true
    fi

    rm -rf "$tmpdir"
}

# Extract and verify runnable snippets from a file using awk
verify_file() {
    local file="$1"
    local filepath="$REPO_ROOT/$file"

    if [[ ! -f "$filepath" ]]; then
        log_warn "File not found: $file"
        return
    fi

    # Use awk to extract runnable snippets
    # Output format: LINE_NUM:LANG:CONTENT (with content base64 encoded)
    local snippets
    snippets=$(awk '
        /^<!-- runnable/ { runnable = 1; next }
        runnable && /^```[a-z]+/ {
            lang = $0
            gsub(/^```/, "", lang)
            gsub(/[^a-z].*/, "", lang)
            start = NR
            content = ""
            in_fence = 1
            runnable = 0
            next
        }
        in_fence && /^```$/ {
            # Base64 encode to preserve newlines (portable: pipe through tr to remove newlines)
            cmd = "printf %s " shquote(content) " | base64 | tr -d " shquote("\n")
            cmd | getline encoded
            close(cmd)
            print start ":" lang ":" encoded
            in_fence = 0
            next
        }
        in_fence {
            content = content $0 "\n"
        }
        !in_fence && runnable && /^[^[:space:]]/ {
            runnable = 0
        }
        function shquote(s) {
            gsub(/'\''/, "'\''\\'\'''\''", s)
            return "'\''" s "'\''"
        }
    ' "$filepath")

    # Process each snippet
    while IFS=: read -r line_num lang encoded_content; do
        [[ -z "$line_num" ]] && continue

        ((CHECKED++)) || true
        local content
        # Portable base64 decode: try -d (GNU), then -D (BSD/macOS)
        content=$(echo "$encoded_content" | base64 -d 2>/dev/null || echo "$encoded_content" | base64 -D 2>/dev/null || echo "")

        case "$lang" in
            bash|sh)
                verify_bash "$file" "$line_num" "$content"
                ;;
            go)
                verify_go "$file" "$line_num" "$content"
                ;;
            *)
                log_warn "$file:$line_num — unknown language '$lang', skipping"
                ;;
        esac
    done <<< "$snippets"
}

# Main
echo "Verifying runnable snippets..."
echo ""

for file in "${FILES[@]}"; do
    verify_file "$file"
done

echo ""
echo "Checked $CHECKED runnable snippet(s)"

if [[ $ERRORS -gt 0 ]]; then
    echo -e "${RED}$ERRORS error(s) found${NC}"
    exit 1
else
    echo -e "${GREEN}All snippets valid${NC}"
    exit 0
fi
