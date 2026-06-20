---
source: src/mz-deploy/src/project/compiler/typecheck/catalog.rs
revision: 75d653ae9e
---

# mz_deploy::project::compiler::typecheck::catalog

The stub catalog that lets the real `mz_sql` planner typecheck project objects
without a running Materialize. It implements Materialize's catalog traits over
in-memory data, so `mz_sql::names::resolve` and `mz_sql::plan::plan` run
exactly as they would in production, at the cost of lower fidelity than the
Docker backend.

`CatalogRuntime` is the shared base catalog and the central
`SessionCatalog`/`ExprHumanizer`/`ConnectionResolver` implementation. `new`
pre-populates it with all system schemas and every builtin type, function,
table, view, source, index, and connection from `mz_catalog::builtin::BUILTINS`
(`seed_system_schemas`, `seed_builtins`, `insert_builtin`, and
`resolve_builtin_type_references`, which rewrites name-based type references to
id-based ones). It holds databases, schemas, and items in several lookup
indexes — notably `items_by_name`, a map to a `Vec` of ids because builtin
types and functions can share a qualified name, disambiguated by a predicate
at lookup time. `IdAllocator` hands out fresh database/schema/item/global/oid
identifiers. `bootstrap_namespaces` pre-creates every database/schema namespace
the project and external types reference (`ensure_user_schema` lazily creating
databases and schemas and keeping the active database and `refresh_search_path`
up to date). `create_item` parses, resolves, plans, and inserts a statement,
returning its `RelationDesc`; `resolve_plan_and_insert` is the resolve→plan→
insert core, `insert_item_from_plan` and the free function `build_local_item`
construct the catalog item from a planned table/view/MV; `create_stub_table`
inserts a placeholder table from a cached column map.

`TaskCatalog` is a per-task copy-on-write overlay over an `Arc<CatalogRuntime>`:
reads consult the overlay's own item and schema maps first and fall through to
the shared base, and writes lift only the affected schema into the overlay on
first mutation, so unrelated tasks never pay a full clone. It exposes
`create_item`, `create_item_from_ast` (skipping the SQL render/reparse), and
`create_stub_table`, and re-implements `SessionCatalog`, `ExprHumanizer`, and
`ConnectionResolver` by overlaying its local state on top of `base`. This is
what gives the executor's parallel per-object tasks isolated catalog views that
share the expensive builtin base.

The trait implementations supply only what the planner needs. The supporting
types `LocalDatabase`, `LocalSchema`, `LocalItem`, and the single-instance
stubs `StubRole` (always `MZ_SYSTEM_ROLE_ID`) and `StubCluster` (one cluster,
`mz_deploy`) satisfy the catalog traits. Operations outside project SQL —
cluster replicas, network policies, inline connection resolution,
`system_vars_mut` on the overlay — are `unreachable!`, and many privilege,
notice, and dependency methods return empty or trivial results. `build_error`
synthesizes a placeholder file path on each `ObjectTypeCheckError` that callers
replace with the real source path. Constants `DEFAULT_CLUSTER_NAME` and
`FIRST_USER_OID` set the implicit cluster name and the starting OID for
user objects.
