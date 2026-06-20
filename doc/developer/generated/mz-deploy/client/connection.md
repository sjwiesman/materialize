---
source: src/mz-deploy/src/client/connection.rs
revision: 673fdb9d44
---

# mz_deploy::client::connection

The `Client` struct: a `tokio_postgres` connection to Materialize plus the
`Profile` it was built from. It exposes thin query primitives (`execute`,
`query`, `query_one`, `simple_query`, `batch_execute`, `begin_transaction`) over
`mz_postgres_util`, mapping errors to `ConnectionError`, and accessor methods
that hand out domain sub-clients: `deployments`, `deployments_mut`,
`introspection`, `validation`, `types`, `provisioning`, and `dev_overlays`. Each
sub-client (`DeploymentsClient`, `DeploymentsClientMut`, `IntrospectionClient`,
`ValidationClient`, `TypeInfoClient`, `ProvisioningClient`, `DevOverlaysClient`)
borrows the `Client` and is defined here but implemented in its own module.

Connection setup runs through `connect_with_profile` and the crate-private
`connect_with_profile_no_pin`, both delegating to `connect_with_profile_inner`.
The pinned path sets the session cluster to `_mz_deploy_server` via libpq
`options`; the no-pin path (ephemeral test container, `setup::run`) uses the
profile/server default. Options are serialized by `build_options_string` /
`escape_options_value`, which emit sorted `-c key=value` tokens with libpq
space/backslash escaping. Connections are spawned onto an `mz_ore` task.

The bulk of the module is TLS policy. `default_sslmode` picks `Prefer` for
loopback hosts (recognized by `is_loopback_host`) and `Require` otherwise.
`plan_connector` is a pure function turning `(SslMode, sslrootcert, host)` into a
`ConnectorSpec` (`NoTls`, or `Tls` with an OpenSSL verify mode, optional
`HostCheck` for `verify-full`, and a `CaSource`); `resolve_ca_source` chooses an
explicit `sslrootcert`, then hunts `DEFAULT_CA_PATHS`, then falls back to
OpenSSL's default verify paths. `build_connector` materializes the spec into a
runtime `Connector` by wiring the OpenSSL context (all CA filesystem I/O happens
here). `classify_connect_error` maps a raw `tokio_postgres::Error` into the most
specific `ConnectionError` — `TlsVerification`, `TlsRequiredNotSupported`, or
`Connect` — using `ssl_error_in_chain` and the substring heuristic
`matches_tls_refused_message` / `message_indicates_tls_refused`.
`tokio_ssl_mode` translates the crate's `SslMode` to the `tokio_postgres` one.
