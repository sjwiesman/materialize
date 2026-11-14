# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""A stateless external autoscaler for managed clusters.

`decision` holds the pure decision logic, `sampling` the pure arithmetic that
turns two introspection reads into a load sample, `catalog` all SQL, and
`runner` the orchestration of one run.
"""
