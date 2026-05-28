#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# lint-mz-deploy-book.sh — fails if the mz-deploy book's generated
# reference appendix is out of sync with src/mz-deploy/src/cli/help/.
# Only the SUMMARY.md reference block is checked in; the mirrored
# reference/*.md files are gitignored and regenerated on every build.

set -euo pipefail

. misc/shlib/shlib.bash

# Regenerate; only SUMMARY.md is in tree, so that's all we can diff.
try src/mz-deploy/book/generate-reference.sh

if ! git diff --exit-code -- src/mz-deploy/book/src/SUMMARY.md; then
    cat <<'EOF' >&2

The mz-deploy book's reference appendix entries in SUMMARY.md are stale.

Regenerate locally with:

    src/mz-deploy/book/generate-reference.sh

then commit the updated SUMMARY.md.
EOF
    exit 1
fi

# Smoke test the generator itself.
try src/mz-deploy/book/tests/generate-reference-test.sh

try_status_report
