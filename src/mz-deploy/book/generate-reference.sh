#!/usr/bin/env bash
# Regenerates the book's command reference appendix.
#
# Copies every *.md file from $HELP_DIR (default: ../src/cli/help) into
# $BOOK_SRC/reference/, then rewrites the marker-delimited block in
# $BOOK_SRC/SUMMARY.md to list one entry per file, alphabetically sorted.
#
# Markers in SUMMARY.md must look like:
#   <!-- BEGIN REFERENCE -->
#   <!-- END REFERENCE -->

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HELP_DIR="${HELP_DIR:-$SCRIPT_DIR/../src/cli/help}"
BOOK_SRC="${BOOK_SRC:-$SCRIPT_DIR/src}"

REF_DIR="$BOOK_SRC/reference"
SUMMARY="$BOOK_SRC/SUMMARY.md"

if [ ! -d "$HELP_DIR" ]; then
    echo "error: HELP_DIR does not exist: $HELP_DIR" >&2
    exit 1
fi
if [ ! -f "$SUMMARY" ]; then
    echo "error: SUMMARY.md not found: $SUMMARY" >&2
    exit 1
fi

# 1. Mirror help/*.md into book/src/reference/.
rm -rf "$REF_DIR"
mkdir -p "$REF_DIR"
for f in "$HELP_DIR"/*.md; do
    cp "$f" "$REF_DIR/"
done

# 2. Build the new SUMMARY block.
BLOCK="$(mktemp)"
trap 'rm -f "$BLOCK"' EXIT
{
    echo '<!-- BEGIN REFERENCE -->'
    for f in "$REF_DIR"/*.md; do
        name="$(basename "$f" .md)"
        printf -- '- [%s](./reference/%s.md)\n' "$name" "$name"
    done \
        | sed 's/-/~/g' \
        | LC_ALL=C sort \
        | sed 's/~/-/g'
    # Sort rationale: LC_ALL=C byte-sorts '-' (0x2D) before '.' (0x2E), which
    # puts "apply-clusters" before "apply" when bare command names are mixed
    # with subcommands.  Replacing '-' with '~' (0x7E > all alphanumerics)
    # before sorting makes "apply" sort before "apply~clusters", then we
    # restore '-' afterwards.
    echo '<!-- END REFERENCE -->'
} > "$BLOCK"

# 3. Splice the new block into SUMMARY.md between the markers.
awk -v block_file="$BLOCK" '
    BEGIN { in_block = 0; printed = 0 }
    /<!-- BEGIN REFERENCE -->/ {
        if (!printed) {
            while ((getline line < block_file) > 0) print line
            close(block_file)
            printed = 1
        }
        in_block = 1
        next
    }
    /<!-- END REFERENCE -->/ {
        in_block = 0
        next
    }
    !in_block { print }
' "$SUMMARY" > "$SUMMARY.tmp"

mv "$SUMMARY.tmp" "$SUMMARY"
