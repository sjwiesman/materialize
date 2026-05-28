# Installation & setup

*What you'll learn: how to install mz-deploy, configure your first connection profile, and initialize the deployment infrastructure.*

## Installing the binary

The easiest way to install mz-deploy on macOS is via the Homebrew tap:

```bash
brew tap sjwiesman/mz-deploy
brew install mz-deploy
```

On Linux, download the tarball for your platform from the [GitHub releases page](https://github.com/sjwiesman/mz-deploy/releases), extract the binary, and place it on your `PATH`:

```bash
tar -xzf mz-deploy-v0.11.0-x86_64-unknown-linux-gnu.tar.gz
sudo mv mz-deploy /usr/local/bin/
```

If you are working from a clone of the repository, you can build and install from source:

```bash
cargo install --path src/mz-deploy
```

Confirm the binary is reachable:

```bash
mz-deploy --version
```

```text
mz-deploy 0.11.0
```

## Configuring your first profile

mz-deploy does not manage your connection configuration — you write it yourself in a `profiles.toml` file. Create the directory and file:

```bash
mkdir -p ~/.mz
```

Then write `~/.mz/profiles.toml`. Here is a minimal example for a locally running Materialize on the default port:

```toml
[default]
host = "localhost"
port = 6875
username = "materialize"
```

For a remote Materialize Cloud instance, add credentials and tighten TLS:

```toml
[production]
host = "mz.example.com"
username = "deploy_bot"
password = "${PROD_PASSWORD}"
sslmode = "verify-full"
```

The `${PROD_PASSWORD}` syntax expands an environment variable at connection time, keeping the secret out of the file.

List your profiles to confirm the file loads:

```bash
mz-deploy profiles
```

```text
  default  (active)
  production
```

The active profile is the one commands use unless you override it. See the next section for how the active profile is resolved.

### Profile lookup order

mz-deploy searches for `profiles.toml` in this order:

1. The directory given by `--profiles-dir` (highest priority).
2. The `MZ_DEPLOY_PROFILES_DIR` environment variable.
3. `~/.mz` (default).

To keep separate configs per team or project, pass `--profiles-dir /path/to/config` or set `MZ_DEPLOY_PROFILES_DIR`.

## Testing connectivity with `mz-deploy debug`

Before initializing anything in Materialize, confirm that your profile can reach the instance:

```bash
mz-deploy debug
```

A healthy response looks like this:

```text
profile:     default
host:        localhost:6875
environment: local
version:     v0.128.0
role:        materialize
cluster:     missing
docker:      installed, running
```

The `cluster` field shows `missing` on a fresh instance — that is expected. The cluster is created by `mz-deploy setup` in the next step. Everything else should be green before you proceed.

If `debug` fails:

- **Connection refused** — Verify `host` and `port` in `profiles.toml`. Make sure Materialize is running.
- **Authentication failed** — Check `username` and `password`. For remote instances, confirm the password environment variable is exported.
- **Profile not found** — Run `mz-deploy profiles` to list what is configured and check for typos.

## Initializing the deployment infrastructure with `mz-deploy setup`

`mz-deploy setup` creates everything mz-deploy needs inside your Materialize instance. Run it once per environment.

**You must run `setup` as a Materialize superuser when RBAC is enabled.** Only a superuser can grant system privileges to other roles. On a local development instance with RBAC disabled, any role can run it.

```bash
mz-deploy setup
```

Setup creates the following, skipping anything that already exists:

- The `_mz_deploy` database, with tracking tables and the `production` view.
- Three roles:
  - `materialize_deployer` — can stage, promote, and abort deployments.
  - `materialize_developer` — read-only access to deployment state; also used by the `dev` command.
  - `materialize_monitor` — read-only monitoring access.
- The `_mz_deploy_server` cluster, which mz-deploy pins all its own connections to. It is not intended for general use; resize it with `ALTER CLUSTER` if needed.

After setup, grant the appropriate role to each database user that will run mz-deploy commands:

```sql
GRANT materialize_deployer TO my_deploy_user;
GRANT materialize_developer TO my_dev_user;
GRANT materialize_monitor TO my_monitor_user;
```

Each user should hold exactly one mz-deploy role. Having multiple roles on a single user is an error — use separate profiles with distinct users for deploying, developing, and monitoring.

`setup` is idempotent. Running it again on an already-initialized environment is safe and has no effect.

## Where things live

| Item | Location |
|---|---|
| Connection profiles | `~/.mz/profiles.toml` (or `MZ_DEPLOY_PROFILES_DIR`) |
| Active profile (per checkout) | `.mzprofile` file in the project root |

The active profile is resolved in this order:

1. `--profile <name>` CLI flag — overrides everything.
2. `MZ_DEPLOY_PROFILE` environment variable — useful in CI.
3. `.mzprofile` file in the project root — per-checkout default, set by `mz-deploy profile set <name>`. This file is gitignored so each teammate can pick their own without touching shared config.

---

You can now:

- Install `mz-deploy` and confirm the binary is on your PATH.
- Write a `profiles.toml` and verify it loads with `mz-deploy profiles`.
- Test connectivity with `mz-deploy debug`.
- Initialize Materialize for deployments with `mz-deploy setup`.
