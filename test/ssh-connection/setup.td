# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Basic connection setup for most workflows.
# Done here instead of in the workflow to ensure the state is reset.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true
ALTER SYSTEM SET enable_connection_validation_syntax = true

> CREATE CONNECTION IF NOT EXISTS thancred TO SSH TUNNEL (
    HOST 'ssh-bastion-host',
    USER 'mz',
    PORT 22
  );

# We don't authorize this connection's keys, so we
# can test error handling.
> CREATE CONNECTION IF NOT EXISTS keyless TO SSH TUNNEL (
    HOST 'ssh-bastion-host',
    USER 'mz',
    PORT 22
  );
