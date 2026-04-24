# sql — Launch an interactive psql session

Starts `psql` connected to Materialize using the active profile. Any arguments
after `sql` are forwarded to `psql` unchanged.

## Usage

    mz-deploy sql [-- PSQL_ARGS...]

## Behavior

1. Loads the connection profile from `profiles.toml`.
2. Sets these environment variables from the profile:
   - `PGHOST`, `PGPORT`, `PGUSER`
   - `PGPASSWORD` (if the profile sets a password)
   - `PGSSLMODE` (profile `sslmode`, or `prefer` for loopback / `require` otherwise)
   - `PGSSLROOTCERT` (if the profile sets `sslrootcert`)
   - `PGOPTIONS` (profile `[options]` rendered as `-c key=value` tokens)
   - `PGAPPNAME=mz-deploy-sql`
3. Replaces the current process with `psql`, forwarding any trailing args.

Unlike other `mz-deploy` commands, `sql` does **not** pin the session to the
`_mz_deploy_server` cluster — it's an interactive shell, so the cluster comes
from the profile's `options.cluster` (or the server default) just like any
other `psql` connection.

## Examples

    mz-deploy sql                              # Interactive shell
    mz-deploy sql --profile staging            # Specific profile
    mz-deploy sql -- -c "SELECT 1"             # Run a single query and exit
    mz-deploy sql -- -f migrations.sql         # Execute a SQL script

## Error Recovery

- **`psql: binary not found on PATH`** — Install the PostgreSQL client:
  `brew install libpq` (macOS) or `apt install postgresql-client` (Debian/Ubuntu).
- **Authentication failed** — Check credentials in `profiles.toml`; passwords
  can be inlined as `${VAR}` or overridden with `MZ_PROFILE_<NAME>_PASSWORD`.
- **Profile not found** — Run `mz-deploy profile list` to see available
  profiles, or `mz-deploy profile set <name>` to activate one.

## Exit Codes

- **0** — `psql` exited normally.
- Non-zero — `psql`'s own exit code, or `1` if `psql` could not be launched.

## Related Commands

- `mz-deploy debug` — Test the profile's connection non-interactively.
- `mz-deploy profile list` — Show profiles defined in `profiles.toml`.
