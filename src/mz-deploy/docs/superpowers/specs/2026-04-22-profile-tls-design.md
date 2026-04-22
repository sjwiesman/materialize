# Per-Profile TLS Configuration

**Date:** 2026-04-22
**Status:** Implemented

## Problem

`Client::connect_with_profile` picks its TLS behavior by inspecting the profile's host string. Loopback and RFC1918 private addresses get `NoTls`; everything else gets TLS with `SslVerifyMode::PEER` (chain verified, hostname not checked). The user has no way to override this — a self-hosted deployment on a private IP that requires TLS, or a cloud deployment pointed at by an IP that would otherwise fail hostname verification, cannot be expressed in a profile.

The heuristic also silently picks one verification strictness (`verify-ca`-equivalent) and silently downgrades to plaintext on private IPs. Both are policy choices the profile should be able to state.

## Design

Add two optional fields to a profile: `sslmode` (enum matching libpq's values minus `allow`) and `sslrootcert` (path to a CA bundle). Route the profile through `tokio_postgres::Config::ssl_mode` and an `SslConnector` whose verification is configured from `sslmode`.

When `sslmode` is unset, the default is derived from the host:

- `localhost`, `127.0.0.1`, `::1` → `prefer`
- everything else → `require`

That is the only host-based decision left in the system. Once `sslmode` is set on a profile, the host is irrelevant to TLS behavior. No other auto-fallback or retry logic runs.

### Non-goals

- `sslmode = "allow"`. Rarely used; omitting it keeps the vocabulary tight.
- Per-option env-var overrides for `sslmode` / `sslrootcert`. Users set them in the profile file.
- Client certificates (`sslcert` / `sslkey`). Out of scope; add if a concrete user need appears.
- Mutable CA trust at runtime (e.g., appending a cert via CLI flag).

## TOML schema

```toml
[default]
host = "localhost"
port = 6875
username = "materialize"
# sslmode defaults to "prefer" because host is localhost

[prod]
host = "foo.materialize.cloud"
username = "seth@materialize.com"
password = "${MZ_PROFILE_PROD_PASSWORD}"
sslmode = "verify-full"
# sslrootcert left unset; platform CA hunt runs

[self_hosted]
host = "mz.internal.example.com"
username = "deploy_bot"
password = "${MZ_PROFILE_SELF_HOSTED_PASSWORD}"
sslmode = "verify-full"
sslrootcert = "/etc/ssl/internal-ca.pem"
```

Omitting `sslmode` behaves exactly per the default rule above. Omitting `sslrootcert` triggers the CA hunt (see §CA resolution) for the `verify-*` modes only.

### Accepted `sslmode` values

Five variants, mirroring libpq's vocabulary (minus `allow`):

| Value | Encrypt | Verify chain | Verify hostname |
|---|---|---|---|
| `disable` | no | n/a | n/a |
| `prefer` | if offered | no | no |
| `require` | yes | no | no |
| `verify-ca` | yes | yes | no |
| `verify-full` | yes | yes | yes |

Invalid values are rejected at config load with a message listing the accepted set.

## Rust schema

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

pub struct Profile {
    // existing fields: name, host, port, username, password, options
    pub sslmode: Option<SslMode>,
    pub sslrootcert: Option<PathBuf>,
}
```

A `ProfileData` companion (the serde-facing type, as with `options`) carries `sslmode: Option<SslMode>` and `sslrootcert: Option<PathBuf>`, both with `#[serde(default)]`.

## Mode mapping

At connect time, the profile's effective `sslmode` (either the user's value or the host-derived default) is translated to a `(tokio_postgres::config::SslMode, Connector)` pair:

| `sslmode` | `tokio_postgres::SslMode` | Connector | OpenSSL verify config |
|---|---|---|---|
| `disable` | `Disable` | `NoTls` | — |
| `prefer`  | `Prefer`  | `MakeTlsConnector` | `SslVerifyMode::NONE` |
| `require` | `Require` | `MakeTlsConnector` | `SslVerifyMode::NONE` |
| `verify-ca`   | `Require` | `MakeTlsConnector` | `SslVerifyMode::PEER`, no hostname param |
| `verify-full` | `Require` | `MakeTlsConnector` | `SslVerifyMode::PEER` + `X509VerifyParam::set_host(host)` (or `set_ip` when the host parses as an IP literal) |

`tokio_postgres` handles the `Prefer` TLS-then-plaintext fallback natively; we don't retry manually.

When `sslmode = "verify-full"` and the profile `host` parses as an IPv4 or IPv6 literal, use `X509VerifyParam::set_ip` instead of `set_host` so the cert's `subjectAltName` IP entries match. The implementation selects one or the other based on a single `host.parse::<IpAddr>()` check.

One code path replaces the current `if is_local { NoTls } else { TLS }` branch:

```rust
let effective = profile.sslmode.unwrap_or_else(|| default_sslmode(&profile.host));
let (ssl_mode, connector) = build_connector(effective, profile.sslrootcert.as_deref(), &profile.host)?;
config.ssl_mode(ssl_mode);
let (client, connection) = match connector {
    Connector::NoTls => config.connect(NoTls).await,
    Connector::Tls(c) => config.connect(c).await,
};
```

## CA resolution

CA certificates are only loaded for `verify-ca` and `verify-full`. Other modes never touch the filesystem for CAs.

The resolution order for a `verify-*` mode:

1. If `sslrootcert` is set on the profile, load exactly that path. Missing or unreadable → `TlsCaNotFound` error. No further fallback.
2. Otherwise, walk the platform-path hunt (preserved from today's code, gated behind `verify-*`):
   - `/etc/ssl/cert.pem`
   - `/opt/homebrew/etc/openssl@3/cert.pem`
   - `/usr/local/etc/openssl@3/cert.pem`
   - `/opt/homebrew/etc/openssl/cert.pem`
   - `/usr/local/etc/openssl/cert.pem`
   - `/etc/ssl/certs/ca-certificates.crt`
   - `/etc/pki/tls/certs/ca-bundle.crt`
   - `/etc/ssl/ca-bundle.pem`
3. If the hunt finds nothing, fall back to `SslConnectorBuilder::set_default_verify_paths()`.
4. If none of the above succeed, return `TlsCaNotFound`.

Hunt and default-paths fallback only apply when `sslrootcert` is unset — an explicit path is never silently replaced.

## Error handling

Four distinct error cases, each with an actionable message:

### `ConnectionError::TlsRequiredNotSupported`

Raised when `sslmode` (effective) is `require` or `verify-*` and the server refuses TLS (would have been fine in `disable` / `prefer`).

```
TLS required by profile but server at {host}:{port} does not support TLS

help: The server did not offer TLS. To connect without encryption, set
      sslmode = "disable" on the profile. To use TLS if available but fall
      back to plaintext otherwise, set sslmode = "prefer".
```

Detected by matching on `tokio_postgres::Error` kind plus the absence of an OpenSSL cause. This is the main upgrade-path pain point for users running plaintext Materialize on a private IP: today they get `NoTls`; tomorrow they get `require` by default and must set `sslmode = "disable"`. The message directs them there.

### `ConnectionError::TlsVerification`

Raised when `sslmode` is `verify-ca` or `verify-full` and the OpenSSL handshake fails cert verification. Message differentiates chain versus hostname when the underlying OpenSSL error code makes that possible:

```
TLS certificate verification failed for {host}:{port}: {underlying}

help: The server's certificate could not be verified against the trusted CA
      bundle{hostname_suffix}. To skip verification, set sslmode = "require"
      or sslmode = "prefer". To use a custom CA bundle, set
      sslrootcert = "/path/to/ca.pem" on the profile.
```

Where `{hostname_suffix}` is `" (hostname mismatch)"` when the OpenSSL error indicates `X509_V_ERR_HOSTNAME_MISMATCH` (or the openssl-rs equivalent), otherwise empty.

### `ConnectionError::TlsCaNotFound`

Raised at connector construction — before attempting any network I/O — when `sslmode` is `verify-*` and no CA bundle can be loaded (explicit path invalid, hunt empty, `set_default_verify_paths` returned no usable store).

```
no CA bundle found for TLS verification

help: Set sslrootcert = "/path/to/ca.pem" on the profile to point at a
      specific CA bundle, or install the system CA bundle at one of:
      /etc/ssl/cert.pem, /etc/ssl/certs/ca-certificates.crt, or the
      platform-appropriate equivalent.
```

### `ConfigError::InvalidSslMode`

Raised at config load when the TOML contains an unrecognized `sslmode` string:

```
invalid sslmode "yolo" in profile "prod"

help: Valid values are: disable, prefer, require, verify-ca, verify-full.
```

### Generic `Connect`

All other connection failures continue to use the existing `ConnectionError::Connect` variant unchanged. The three TLS-specific variants are additive; they handle cases we can identify with high confidence and fall through to `Connect` otherwise.

## Migration / behavior changes

All changes are to the default path; profiles that set `sslmode` explicitly are unaffected.

| Host | Today | After change (default) | Net |
|---|---|---|---|
| `localhost`, `127.0.0.1`, `::1` | NoTls | `prefer`, server rejects TLS, falls back to plaintext | Same end result |
| Private IP running plaintext Mz | NoTls | `require`, fails with `TlsRequiredNotSupported` | **Breaks on upgrade.** User must set `sslmode = "disable"`; error message points them there |
| Private IP running TLS Mz | NoTls (fails — server rejects plaintext) | `require`, succeeds | **Previously unreachable, now works.** |
| Materialize Cloud | TLS, `verify-ca`-equivalent | `require`, no verification | Encryption preserved; chain verification dropped — **release-note item**. Users who want the old (or stronger) behavior set `sslmode = "verify-ca"` or `"verify-full"` |

The release note calls out both the `verify-ca` → `require` default shift for off-loopback hosts and the recommendation to set `sslmode = "verify-full"` on cloud profiles for production use. Example profile configs in `src/mz-deploy/docs/` are updated to show `sslmode = "verify-full"` on cloud examples.

## Testing

Focus on the connector-construction and error-message layers, where real behavior lives.

**Connector construction unit tests** (against a new `build_connector(mode, sslrootcert, host)` helper):

- `disable` → `NoTls` variant, no CA work attempted.
- `prefer`, `require` → `MakeTlsConnector` with `SslVerifyMode::NONE`; no CA load attempted.
- `verify-ca` → `MakeTlsConnector` with `SslVerifyMode::PEER`, no hostname param set.
- `verify-full` → `MakeTlsConnector` with `SslVerifyMode::PEER` and a hostname param matching the profile host.
- `verify-ca` with explicit `sslrootcert` pointing at a test fixture PEM → loads that file.
- `verify-full` with explicit but nonexistent `sslrootcert` → `TlsCaNotFound` (never attempts hunt).
- `verify-ca` with no `sslrootcert` and a stubbed hunt returning empty + no default verify paths → `TlsCaNotFound`.

**Integration test** (local Mz via docker, existing harness):

- Happy path with `sslmode = "disable"` — confirms the plain connection still works.
- Happy path with `sslmode = "prefer"` against a local Mz that doesn't offer TLS — confirms the fallback path reaches plaintext end-to-end.

The `verify-ca` / `verify-full` paths require real certificates and are left to manual verification against a staging Materialize Cloud environment during PR review.

`default_sslmode` is trivial string comparison and `config.rs` parsing is serde-derived; no dedicated unit tests.

## Open questions

None at spec approval time. Implementation is expected to proceed without further design input.
