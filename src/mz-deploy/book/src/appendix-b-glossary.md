# Appendix B — Glossary

Definitions of terms used throughout this book. Entries are alphabetical.

- **apply** — The `mz-deploy` command that converges Materialize
  infrastructure (clusters, roles, network policies, secrets,
  connections, sources, tables) toward what your project declares.

- **deploy ID** — The suffix used on staging schemas and clusters during
  `stage`. Defaults to the first seven characters of the current git
  commit SHA.

- **deferred sink** — A sink defined in a staging deployment whose
  creation is held back until `promote`, so it does not start producing
  rows from staging data.

- **dirty (object)** — An object whose definition has changed since the
  last promoted snapshot, or whose dependencies have changed. Dirty
  objects are redeployed by `stage`.

- **profile** — A named set of connection details for a Materialize
  instance, defined in `profiles.toml`.

- **profile variant file** — A `name__<profile>.sql` file that overrides
  `name.sql` when the active profile matches.

- **project** — A directory tree containing `project.toml` and SQL files
  that declares the desired state of a Materialize installation.

- **promote** — The `mz-deploy` command that atomically swaps a staged
  deployment into production.

- **replacement MV** — A materialized view in a `SET api = stable`
  schema whose changes are applied in place via
  `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` rather than schema
  swap.

- **schema modifier file** — A `models/<database>/<schema>.sql` file
  containing directives (e.g. `SET api = stable;`) that apply to the
  entire schema.

- **stable API schema** — A schema marked with `SET api = stable`,
  restricted to materialized views only, whose objects can be safely
  depended on by other teams or other `mz-deploy` projects.

- **stage** — The `mz-deploy` command that builds a staging deployment
  containing only objects that have changed since the last promoted
  snapshot.

- **types.lock** — A project file that pins external type information
  used by `compile` for type-checking without contacting a live
  Materialize.

- **variable** — A psql-style `:name` substitution defined per-profile
  in `project.toml`, resolved before the SQL parser sees the file.
