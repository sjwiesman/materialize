# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so dependabot refreshes the
# digest for security fixes.
FROM python:3.15.2@sha256:46837ed3105db0029b6d656816185c6258e3b39690532fb782428e0a5fd76182

# The official image can lag Debian security updates by weeks, so a digest bump
# alone does not pick them up. libbrotli is CVE-2026-111497.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade libbrotli1 libbrotli-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . dbt-materialize/

RUN pip install --no-cache-dir -r dbt-materialize/requirements.txt --find-links /dbt-materialize
