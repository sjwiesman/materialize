#!/usr/bin/env bash
# Smoke test for generate-reference.sh.
# Sets up a tempdir with a fake help/ tree and a fake SUMMARY.md,
# runs the generator, asserts the outputs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GENERATOR="$SCRIPT_DIR/../generate-reference.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Fake input: two help files.
mkdir -p "$WORK/help"
cat >"$WORK/help/compile.md" <<'EOF'
# compile — Validate
Body of compile.
EOF
cat >"$WORK/help/stage.md" <<'EOF'
# stage — Stage a deployment
Body of stage.
EOF

# Fake SUMMARY with markers.
mkdir -p "$WORK/book/src"
cat >"$WORK/book/src/SUMMARY.md" <<'EOF'
# The Book

[Introduction](./introduction.md)

# Appendix

<!-- BEGIN REFERENCE -->
<!-- END REFERENCE -->

[Glossary](./appendix-b-glossary.md)
EOF

# Run the generator.
HELP_DIR="$WORK/help" BOOK_SRC="$WORK/book/src" "$GENERATOR"

# Assertions.
[ -f "$WORK/book/src/reference/compile.md" ] || { echo "FAIL: compile.md not copied"; exit 1; }
[ -f "$WORK/book/src/reference/stage.md" ]   || { echo "FAIL: stage.md not copied"; exit 1; }

diff "$WORK/help/compile.md" "$WORK/book/src/reference/compile.md" \
    || { echo "FAIL: compile.md content differs"; exit 1; }

grep -q '\- \[compile\](./reference/compile.md)' "$WORK/book/src/SUMMARY.md" \
    || { echo "FAIL: SUMMARY missing compile entry"; exit 1; }
grep -q '\- \[stage\](./reference/stage.md)'     "$WORK/book/src/SUMMARY.md" \
    || { echo "FAIL: SUMMARY missing stage entry"; exit 1; }

# Entries must be alphabetical.
LINES="$(sed -n '/BEGIN REFERENCE/,/END REFERENCE/p' "$WORK/book/src/SUMMARY.md" \
        | grep -oE '\(./reference/[a-z-]+\.md\)' || true)"
SORTED="$(printf '%s\n' "$LINES" | LC_ALL=C sort)"
[ "$LINES" = "$SORTED" ] || { echo "FAIL: reference entries not alphabetical"; exit 1; }

echo "OK"
