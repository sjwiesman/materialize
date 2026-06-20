---
source: src/mz-deploy/src/cli/commands/sql.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::sql

Implements the `sql` command, which launches an interactive `psql` session
connected via the active profile.

`run` translates the resolved profile into `PG*` environment variables —
`PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE` (always `materialize`), `PGPASSWORD`,
`PGSSLMODE` (from the profile's sslmode or `default_sslmode` for the host),
`PGSSLROOTCERT`, `PGOPTIONS` (from `build_options_string`), and `PGAPPNAME`
(`mz-deploy-sql`) — then forwards any trailing `psql_args` and `exec`s `psql`,
replacing the current process. Unlike most commands, it does not pin the session
to `_mz_deploy_server`, leaving cluster selection to the profile or server
default.

Because `exec` only returns on failure, the tail of `run` maps the error to a
`CliError::Message`: a `NotFound` error becomes an install hint (`brew install
libpq` / `apt install postgresql-client`), and any other error is reported
verbatim.
