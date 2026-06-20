---
source: src/mz-deploy/src/client/errors.rs
revision: 592cecbfd1
---

# mz_deploy::client::errors

Error types for the client layer, split into two enums.

`ConnectionError` (a `thiserror` enum) covers transport and query failures:
`Config`, `Connect`, the TLS-specific `TlsRequiredNotSupported`,
`TlsVerification`, and `TlsCaNotFound` (each with multi-line `help:` text),
`Query` (formatted by the private `format_query_error`, which unpacks
`tokio_postgres` db-error message/detail/hint/code), `Dependency`, the DDL
failures `DatabaseCreationFailed` / `SchemaCreationFailed` /
`ClusterCreationFailed` / `ClusterAlreadyExists`, `IntrospectionFailed`,
`ClusterNotFound`, the deployment-state errors `DeploymentAlreadyExists` /
`DeploymentNotFound` / `DeploymentAlreadyPromoted`,
`UnsupportedStatementType`, and a catch-all `Message`. `From` impls convert
`tokio_postgres::Error` and `mz_postgres_util::PostgresError` into it.

`DatabaseValidationError` enumerates semantic mismatches found during
pre-deployment validation: missing databases/schemas/clusters/sources/
connections, `CompilationFailed` (object with missing external deps),
`Multiple` (aggregated failures), `ClusterConflict` (compute and storage objects
sharing a cluster), `InsufficientPrivileges`, `SchemaOwnershipMismatch`,
`ClusterOwnershipMismatch`, `MissingTableDependencies`, and `QueryError`. Its
hand-written `Display` impl renders rich, colored, rustc-style messages with
`help:` footers and copy-pasteable `GRANT` / `ALTER ... OWNER TO` remediation,
and it implements `std::error::Error`. The free function `format_relative_path`
truncates a path to its last three components for readable error output.
