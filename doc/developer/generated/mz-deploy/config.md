---
source: src/mz-deploy/src/config.rs
revision: a647094cc4
---

# mz_deploy::config

Configuration loading for connection profiles and per-project settings, plus the
resolved `Settings` bundle threaded through every command.

Profiles come from `profiles.toml` (resolved via `--profiles-dir`,
`MZ_DEPLOY_PROFILES_DIR`, or `~/.mz`). `ProfilesConfig::load` parses the file into
`Profile`s (name, optional SQL `host`, port, username, password, libpq `options`,
`sslmode`, `sslrootcert`, optional `http_host`), validating option keys and
requiring at least one of `host`/`http_host`. It always injects the built-in
`EMULATOR_PROFILE_NAME` ("emulator", localhost:6875 as user materialize) unless a
user entry overrides it. `expand_env_vars` resolves passwords from an inline
`${VAR}` and from `MZ_PROFILE_<NAME>_PASSWORD` (name sanitized via
`sanitize_profile_for_env`); `resolve_profile` is the convenience load + expand
that also lets the built-in emulator resolve with no file present. `Profile`
exposes `require_host` / `require_http_host` for clear errors when the needed
endpoint is absent. `SslMode` mirrors libpq's `sslmode` vocabulary
(`libpq_name`).

Project settings come from `project.toml`. `ProjectSettings` holds an optional
`mz_version`, a flattened map of per-profile `ProfileConfig` sections, and a raw
`dependencies` array. `ProfileConfig` carries an optional `profile_suffix`,
`SecurityConfig` (AWS profile for secret resolution), psql-style `variables`, and
an `emulator` flag. `config_for_profile` returns a profile's config, defaulting
the built-in emulator to `emulator = true`; `suffix_for_profile` and
`docker_image` (deriving the `materialize/materialized` image tag from
`mz_version`) round it out. `validate_dependencies` parses each dependency string
into an `ObjectId`, rejecting malformed and duplicate entries. The active profile
is not stored in `project.toml`; it is resolved from `--profile`,
`MZ_DEPLOY_PROFILE`, or the per-checkout `.mzprofile` pointer (`MZPROFILE_FILENAME`)
via `read_mzprofile` / `write_mzprofile`.

`Settings` is constructed once in `main.rs` by `Settings::load` from CLI args plus
both config files. `needs_connection` controls whether a connection `Profile` is
loaded: commands that connect hard-error with `NoProfileConfigured` when no
profile resolves, while non-connecting commands (`compile`, `test`, `explain`)
proceed with `profile_name: None` and a default `ProfileConfig`. Accessors expose
`directory`, `profile_name`, `profile_suffix`, `variables`, `emulator`,
`docker_image`, validated `dependencies`, and `connection` (which panics if called
on a non-connected `Settings`). `ConfigError` enumerates the load/parse/resolve
failure modes.
