# Chapter 11 — Profiles & environments

*What you'll learn: how connection profiles work, how secrets get resolved, and how the same project deploys to different environments.*

## What is a profile?

A **profile** is a named set of connection details for a Materialize instance. You define as many profiles as you have environments — one for local development, one for staging, one for production — and every mz-deploy command uses exactly one profile at a time.

Each profile contains:

| Field | Required | Default |
|---|---|---|
| `host` | yes | — |
| `port` | no | `6875` |
| `username` | yes | — |
| `password` | no | *(none)* |
| `sslmode` | no | `prefer` on loopback, `require` elsewhere |
| `sslrootcert` | no | platform CA bundle |
| `options` | no | *(none)* |

The `options` map is covered in [Per-profile session options](#per-profile-session-options).

## Where `profiles.toml` lives

Profiles are defined in a `profiles.toml` file. mz-deploy looks for the directory that contains it using this resolution order:

1. **`--profiles-dir` flag** — highest priority; overrides everything.
2. **`MZ_DEPLOY_PROFILES_DIR` env var** — used when the flag is absent.
3. **`~/.mz`** — the default fallback; the same directory `mz-deploy setup` targets.

```bash
# Explicit flag
mz-deploy profiles --profiles-dir /path/to/config

# Or set once in your shell profile
export MZ_DEPLOY_PROFILES_DIR=/path/to/config
```

A minimal `profiles.toml` for three environments looks like this:

```toml
[default]
host = "localhost"
port = 6875
username = "materialize"

[staging]
host = "staging.example.com"
username = "deploy_bot"
password = "${STAGING_PASSWORD}"

[production]
host = "production.example.com"
username = "deploy_bot"
password = "${PROD_PASSWORD}"
```

## Active profile resolution

Having profiles defined is not enough — you also need to tell mz-deploy which one to use. The active profile is resolved in this order:

1. **`--profile` flag** — highest priority; overrides everything.
2. **`MZ_DEPLOY_PROFILE` env var** — useful in CI pipelines where you want a single variable to govern every step.
3. **`.mzprofile` file in the project root** — per-checkout default written by `mz-deploy profile set <name>`; gitignored so each teammate can pick their own without touching shared config.

**Flag example** — override for a single command:

```bash
mz-deploy apply --profile staging
```

**Env-var example** — in a CI job that deploys to production:

```bash
export MZ_DEPLOY_PROFILE=production
mz-deploy apply
mz-deploy wait
```

**`.mzprofile` example** — set your personal default once per checkout:

```bash
mz-deploy profile set staging
# creates .mzprofile containing "staging"
# subsequent commands pick up staging automatically
```

## Profile-name-driven file overrides

Profile names also participate in *file-level overrides*: a file named `name__<profile>.sql` (double underscore) is used in place of the default `name.sql` whenever that profile is active. This is the idiomatic way to vary a connection definition or cluster spec between environments without duplicating view logic.

[Chapter 4 — Writing](./ch04-writing.md) covers how these files are laid out and validated at compile time. The short version for the customer project:

```text
models/materialize/public/
├── pg_conn.sql              # production replica
└── pg_conn__staging.sql     # staging replica
```

Running `mz-deploy compile --profile staging` selects `pg_conn__staging.sql`; all other profiles fall back to `pg_conn.sql`. Both files are validated regardless of which profile is active.

## TLS configuration

Two fields control TLS: `sslmode` and `sslrootcert`.

`sslmode` follows PostgreSQL vocabulary:

| Value | Encrypts | Verifies chain | Verifies hostname |
|---|---|---|---|
| `disable` | no | n/a | n/a |
| `prefer` | if offered | no | no |
| `require` | yes | no | no |
| `verify-ca` | yes | yes | no |
| `verify-full` | yes | yes | yes |

When `sslmode` is not set, mz-deploy defaults to `prefer` for loopback hosts (`localhost`, `127.0.0.1`, `::1`) and `require` for everything else.

`sslrootcert` is an optional absolute path to a CA bundle in PEM format. It is only consulted when `sslmode` is `verify-ca` or `verify-full`. When not set, mz-deploy walks a short list of platform CA paths (macOS system, Homebrew, Debian/Ubuntu, RHEL, OpenSUSE) before falling back to OpenSSL's compiled-in defaults.

**Materialize Cloud** — use `verify-full` so the server certificate is fully validated:

```toml
[production]
host = "foo.materialize.cloud"
username = "deploy@example.com"
password = "${MZ_PROFILE_PRODUCTION_PASSWORD}"
sslmode = "verify-full"
```

**Self-hosted with a private CA** — point `sslrootcert` at your internal bundle:

```toml
[staging]
host = "mz.internal.example.com"
username = "deploy_bot"
password = "${MZ_PROFILE_STAGING_PASSWORD}"
sslmode = "verify-full"
sslrootcert = "/etc/ssl/internal-ca.pem"
```

**Private-network plaintext** — set `sslmode = "disable"` explicitly; without it the default `require` will fail with a connection error:

```toml
[internal]
host = "10.0.1.42"
username = "deploy_bot"
sslmode = "disable"
```

## Passwords and secret resolvers

The `password` field supports `${VAR}` substitution — the variable is expanded at connection time, not stored in `profiles.toml`. Separately, you can override any profile's password at runtime by setting `MZ_PROFILE_<NAME>_PASSWORD` (profile name uppercased); this takes precedence over the value in the file, including `${VAR}` references.

Beyond `profiles.toml` passwords, mz-deploy has a richer secret-resolution system for `CREATE SECRET` objects in your project. Secret values can reference *provider functions* that are resolved at `apply secrets` time, so `compile` and `test` work without touching live credentials.

### `env_var`

Reads from a process environment variable. The simplest option — no cloud credentials needed.

```sql
CREATE SECRET pg_password AS env_var('PG_PASSWORD');
```

Set the variable before running `mz-deploy apply secrets`, or export it in your CI environment.

### `file` (env-var shorthand)

For the connection password in `profiles.toml`, `${VAR}` is the equivalent of `env_var` at the profile layer. There is no separate `file()` provider — use an environment variable that holds the file's content, or pipe it in:

```bash
export STAGING_PASSWORD=$(cat /run/secrets/staging-pw)
```

### `aws_secret`

Reads from AWS Secrets Manager. Requires `aws_profile` to be configured under the profile's `[<name>.security]` table in `project.toml`:

```toml
[production.security]
aws_profile = "prod-account"
```

Use the secret name as-is for string secrets, or pass a second argument to extract a single field from an RDS-style JSON blob:

```sql
-- plain string secret
CREATE SECRET api_key AS aws_secret('customer-data/api-key');

-- RDS-style JSON: {"username":"…","password":"…"}
CREATE SECRET db_pw AS aws_secret('customer-data/rds-creds', 'password');
```

### `gcp_secret`

Reads from Google Cloud Secret Manager. Credentials are resolved via Application Default Credentials (`gcloud auth application-default login`, `GOOGLE_APPLICATION_CREDENTIALS`, or workload identity on GCE/GKE).

Configure the default project once per profile in `project.toml`:

```toml
[production.security]
gcp_project = "my-gcp-project"
```

Then reference secrets by bare name (resolves to `latest`) or by full resource path to override the project or pin a version:

```sql
-- bare name — resolves under configured gcp_project at latest
CREATE SECRET api_key AS gcp_secret('shared-api-key');

-- full path — works without gcp_project, or overrides it
CREATE SECRET pinned AS gcp_secret('projects/other-proj/secrets/db-pw/versions/3');

-- JSON field extraction, same pattern as aws_secret
CREATE SECRET db_pw AS gcp_secret('rds-creds', 'password');
```

## Per-profile session options

Each profile may include an `[options]` subtable whose key–value pairs are applied as session variables on every connection. This is the cleanest way to pin a default cluster or `search_path` without adding flags to every command.

```toml
[staging]
host = "staging.example.com"
username = "deploy_bot"
password = "${STAGING_PASSWORD}"

[staging.options]
cluster = "staging_cluster"
search_path = "public,reporting"
```

A few rules to keep in mind:

- Keys must be valid identifiers (letter or underscore, then letters/digits/underscores). Invalid keys produce a config-load error.
- Values are verbatim — no `${VAR}` expansion. Use `password` for secrets.
- The `cluster` key is reserved: mz-deploy pins every connection to its own internal `_mz_deploy_server` cluster, so any `cluster` value you set here is silently overridden.

---

You can now:

- Add a new profile and select it via flag, env var, or `.mzprofile`.
- Resolve passwords from env vars, files, AWS Secrets Manager, or GCP Secret Manager.
- Use profile-named file overrides to vary SQL per environment.
- Configure TLS appropriately for local and remote Materialize.
