# setup — Initialize deployment tracking database and tables

Creates the `_mz_deploy` database and all tracking tables, views, and roles
used by mz-deploy's deployment commands. This is idempotent — running it
multiple times is safe and has no effect if the infrastructure already exists.

## Usage

    mz-deploy setup

## Behavior

1. Connects to Materialize using the active profile.
2. Creates the `_mz_deploy` database (if it doesn't exist).
3. Creates the following tables in `_mz_deploy.public`:
   - `deployments` — deployment metadata (deploy ID, timestamps, commit, kind)
   - `objects` — deployed objects and their content hashes
   - `clusters` — clusters used by each deployment
   - `pending_statements` — deferred statements (e.g. sinks)
   - `replacement_mvs` — replacement materialized view tracking
4. Creates the `production` view for querying current production state.
5. Creates three roles (if they don't exist):
   - `materialize_deployer` — can stage, promote, and abort deployments
   - `materialize_developer` — read-only access to deployment state
   - `materialize_monitor` — read-only monitoring access to deployment state
6. Grants USAGE on the database and schema, and SELECT, INSERT, UPDATE,
   DELETE on all tables to each role.

## Roles

Each database user that runs mz-deploy commands must be a member of exactly
one of the three roles above. After running setup, grant the appropriate role:

    GRANT materialize_deployer TO my_deploy_user;
    GRANT materialize_developer TO my_dev_user;
    GRANT materialize_monitor TO my_monitor_user;

Having multiple mz-deploy roles on a single user is an error — use separate
profiles with distinct users for deploying, developing, and monitoring.

## Examples

    mz-deploy setup                              # Use default profile
    mz-deploy setup --profile production          # Use a specific profile

## Error Recovery

- **Connection refused** — Verify the host and port in `profiles.toml`.
- **Authentication failed** — Check your credentials or app-password.
- **Insufficient privileges** — The user running setup must have permission
  to create databases and roles.

## Exit Codes

- **0** — Setup completed successfully (or infrastructure already exists).
- **1** — Connection or permission error.

## Related Commands

- `mz-deploy debug` — Test connectivity before running setup.
- `mz-deploy stage` — Stage a deployment (requires setup to have been run).
