---
source: src/mz-deploy/src/project/clusters.rs
revision: a647094cc4
---

# mz_deploy::project::clusters

Loads and validates cluster definitions from the optional `<root>/clusters/`
directory. Each `.sql` file defines one cluster via a required `CREATE CLUSTER`
statement plus optional `GRANT` and `COMMENT` statements.

`ClusterDefinition` holds the resolved cluster `name`, the `create_stmt`, and the
`grants` and `comments` targeting it. `load_clusters` is the entry point: it
returns an empty vec if `clusters/` is absent, otherwise it collects file
variants with `collect_all_sql_files`, validates *every* variant independently
(so an invalid non-active profile still errors), then resolves the active variant
per profile (override match, falling back to default) into a definition. All
errors are accumulated and returned as `ValidationErrors`. After validation, if a
`profile_suffix` is given, `apply_cluster_suffix` rewrites the definition name,
the CREATE statement name, and the GRANT and COMMENT target idents — the suffix
is applied last so the filename-vs-declared-name check runs against original
names.

`classify_cluster_statements` sorts the parsed `LocatedStatement`s into the
definition, emitting offset-positioned `ValidationError`s: GRANTs and COMMENTs
must target this cluster (case-insensitively) or raise target-mismatch errors,
anything else raises `InvalidClusterStatement`, and the file must contain exactly
one `CREATE CLUSTER` whose name matches the filename. `extract_size` and
`extract_replication_factor` pull the `SIZE` and `REPLICATION FACTOR` options out
of a `CreateClusterStatement`.
