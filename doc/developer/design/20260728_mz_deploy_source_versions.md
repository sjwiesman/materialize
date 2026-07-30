# mz-deploy: versioned sources and tables

- Associated: (TBD)

## The Problem

Materialize locks the schema of a source or a source-fed table at creation
time. There is no `ALTER` that adds or removes a column from a
`CREATE TABLE ... FROM SOURCE`. When an upstream system changes its schema,
the only way to pick up the change is to create a second object against the
same upstream reference, because multiple sources and multiple tables may
point at the same upstream table with different schemas.

Our own documentation prescribes this workflow. The zero-downtime schema
change guides tell users to create a `v2` schema, recreate the table in it,
wait for the new snapshot, and then rebuild every downstream model against
the new table.

`mz-deploy` has no notion of this. A project is a tree of `.sql` files where
each path maps to exactly one object name and every reference between objects
is a literal SQL name. So a user following the documented workflow inside an
`mz-deploy` project has to:

1. Hand-name the second table something like `orders_v2`.
2. Find every downstream model that reads `orders` and edit it.
3. Remember which of those edits were meant to be permanent pins and which
   were only meant to track whatever the current table happens to be.
4. Repeat all of it on the next upstream change.

Step 3 is where this actually hurts. Some consumers genuinely want to stay on
the old schema until they are ready to move. Others want to follow the newest
table automatically. Today both are spelled the same way, as a literal name,
so the intent is invisible in the source and the tool cannot act on it.

## Success Criteria

- A source or source-fed table can have several concurrently deployed
  versions, each with its own locked schema, declared in the project tree.
- A model can express "read the newest version" or "read this exact version"
  and the difference is visible in the SQL.
- When a new version is created, models that track the newest version are
  marked dirty and are redeployed by the existing `stage` and `promote` flow.
  A model that pinned an exact version keeps reading that version.
- Retiring a version is safe. The tool refuses to drop a version that still
  has readers it can see, including readers outside the project.
- Adopting versioning on an object that already exists does not require
  editing the models that read it.

## Out of Scope

- **Versioning views and materialized views.** Their definitions can be
  changed in place, and stable-API schemas already have
  `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` for changing a published
  materialized view without disturbing consumers. Versioning them would be a
  second mechanism for a problem that already has one, and each extra live
  version would be a standing dataflow rather than a batch job that finishes.
- **Versioning sinks, connections, and secrets.** Versioning exists so that
  consumers can pin. A sink has no consumers, it is a leaf that writes
  outward, so pinning has no meaning for it. Connections and secrets have no
  schema for a consumer to depend on.
- **Versioning user-populated `CREATE TABLE`.** Its schema is not locked by an
  upstream system, so the motivating constraint does not apply.
- **Automatic migration of consumers.** Moving a pinned consumer from one
  version to the next is a human decision. The tool reports and validates,
  it does not rewrite user SQL.
- **Detecting upstream schema drift.** Noticing that the upstream added a
  column, and prompting for a new version, is a separate problem.
- **Reducing the cost of running two versions.** Two live versions of a table
  are two ingests. This design surfaces that cost, it does not remove it.

## Solution Proposal

Versions are declared by filename and resolved at compile time. A bare
reference means the newest version. A quoted reference with an explicit
version pins it. Nothing else in the language changes.

### Declaring a version

The version is a filename suffix, mirroring the existing `object#profile.sql`
convention for profile overrides:

```
models/materialize/public/orders@1.sql   ->  materialize.public."orders@1"
models/materialize/public/orders@2.sql   ->  materialize.public."orders@2"
```

`@` becomes a reserved character in model filenames alongside `#`, and the two
compose as `orders@2#staging.sql`. Version numbers are positive integers and
need not be contiguous, so retiring version 1 leaves 2 and 3 with 3 as the
newest.

The physical name is the quoted identifier `"orders@2"`. Deriving it from the
filename with no mangling preserves the project's path-equals-name rule. It
also means the tool reserves no part of the unquoted identifier namespace, so
a user is still free to hand-name an object `orders_v2` for unrelated reasons.

Only `CREATE SOURCE` and `CREATE TABLE ... FROM SOURCE` may carry a version
suffix. Any other statement type in a versioned file is a compile error.

Note that both `CREATE TABLE` and `CREATE TABLE ... FROM SOURCE` classify as
`ObjectType::Table`, so this restriction has to be enforced by matching on the
statement variant rather than on the object type.

An unversioned `orders.sql` and a versioned `orders@1.sql` may not coexist in
the same directory. Without that rule a bare reference to `orders` would have
two defensible meanings.

### What a versioned file contains

The file declares the object under its **base** name. The version appears only
in the filename:

```sql
-- models/materialize/public/orders@1.sql
CREATE TABLE orders
    FROM SOURCE "my_source@1" (REFERENCE public.orders);

COMMENT ON TABLE orders IS 'Raw order stream.';
GRANT SELECT ON orders TO analyst;
```

This is how profile overrides already behave. `parse_file_stem` splits the
stem on `#` and the object keeps its base name, with the suffix reapplied to
the compiled statement after assembly. Version resolution reuses that hook,
splitting on `#` first and then on `@`, so `orders@2#staging.sql` yields base
name `orders`, version 2, profile `staging`. Name validation continues to
compare the declared name against the base name.

The consequence that matters: copying `orders@1.sql` to `orders@2.sql`
requires no edits beyond the schema change itself.

Companion statements divide into one case that cannot arise and one that needs
a dedicated rewrite.

- `CREATE INDEX` cannot appear, because `validate_indexes_supported` already
  rejects indexes on tables and sources, and those are the only versionable
  statement types. Index names therefore cannot collide across versions. This
  is worth stating explicitly, because it is a consequence of the scope
  restriction rather than an independent property. Widening versioning to
  views or materialized views later would reintroduce the collision and
  require suffixing declared index names too.
- `COMMENT ON` and `GRANT` need a rewrite of their own. Ordinary
  qualification only prepends the database and schema and keeps whatever
  object identifier the user wrote, which is the base name. A versioned
  object therefore requires a separate pass that retargets the companion's
  self-reference onto the object's own deployed name, so that each version
  carries its own comment and its own privileges. Without it, a grant written
  in `orders@1.sql` lands on a bare `orders`, which either fails at apply time
  or, during the adoption window when an unversioned object still exists,
  silently grants on the wrong object and is never revoked.

  That pass must use the object's own name rather than newest-version
  resolution. Resolving a self-reference through the version map would send
  `orders@1.sql`'s grant to `orders@2`.

A versioned `CREATE SOURCE` also produces a progress collection named after
the source, so `"my_source@1"` yields `"my_source@1_progress"`. It is unique
per version without any additional rule.

### Interaction with profile overrides

The canonical filename is `<base>@<version>#<profile>.sql`. Parsing splits on
`#` first, reusing the existing `rsplit_once('#')`, and then on `@`. The
grouping key used by `collect_all_sql_files` widens from the object name to
the pair of object name and version, so `orders@1.sql` and
`orders@1#staging.sql` form one variant group while `orders@2.sql` and
`orders@2#staging.sql` form another.

The reversed spelling `orders#staging@1.sql` parses as the profile
`staging@1`, which is silently wrong. It is a hard error rather than a second
accepted ordering.

The two features apply to compatible object types without needing a new rule.
Profile overrides are already forbidden on views and materialized views, and
versioning is permitted only on sources and tables from source, so every
versionable object is one that already accepts overrides.

Overriding a single version behaves like any other override. An
`orders@1#staging.sql` can point version 1 at a different upstream reference
for the staging environment.

**Version sets may differ per profile.** Variant resolution is
`overrides.get(profile).or(default)`, and an override with no default means the
object does not exist for other profiles at all. So `orders@3#dev.sql` with no
corresponding `orders@3.sql` gives the `dev` profile a version 3 that
production does not have. This is a useful workflow, because it allows trying
an upstream schema change without provisioning a production ingest for it.

The consequence is that **the newest version is profile-dependent**. The same
unedited model compiles to `"orders@3"` under `dev` and `"orders@2"` under
`prod`. This is not new in kind, since profile overrides already allow
environments to diverge, but it is new in reach. It changes the meaning of a
bare name inside files that have no profile variant of their own and therefore
carry no local signal that anything is environment-specific. Editor hover
resolves against the active profile for the same reason, so switching profiles
changes what hover reports.

Compile output and `mz-deploy list` must therefore state which version a bare
name resolves to under the active profile. Without that, profile-dependent
resolution is invisible.

**A profile override can break a version's schema promise.** Pinning is
valuable because a version's schema is frozen, but nothing prevents
`orders@1#staging.sql` from declaring a different column set than
`orders@1.sql`. Cross-variant validation checks object type consistency and
not columns, and for a table from source the columns originate upstream and
are not statically knowable. `types.lock` is fetched against one profile, so
there is nothing to compare across profiles either. This hazard is documented
rather than enforced.

`profile_suffix` does not interact with versioning. It renames databases and
clusters, not objects, so `"orders@2"` under a suffixed profile is
`materialize_dev.public."orders@2"`. The two naming axes are independent.

### Referencing a version

```sql
FROM orders           -- newest version
FROM "orders@1"       -- pinned to version 1
```

Because there is no object actually named `orders` in the catalog, only
`"orders@1"` and `"orders@2"`, the bare name is unambiguous by construction.
Resolution is a fallback rule: resolve the name normally, and if no
unversioned object exists but versions do, take the highest.

Both forms are ordinary SQL identifiers, so they work in every position an
object name is legal. Views, materialized views, sinks, and indexes all use
the same two spellings. Resolution applies across schema and database
boundaries exactly as it does within a schema.

Newest-version resolution only applies to objects the project owns, because
versions are declared by filename. A reference to an object outside the
project resolves as an ordinary external dependency, so consumers in other
projects always name a version explicitly and declare it in `project.toml`
as `db.schema."orders@2"`.

The one exception is `CREATE TABLE ... FROM SOURCE`. A table may not track the
newest version of its source, because a table's definition is what pins its
schema, and silently repointing it at a new source would trigger a full
re-snapshot of that table. Naming a versioned source without an explicit
version in that position is a compile error directing the user to pin.

### Where the resolution happens

The rewrite belongs in `project::resolve::normalize`, running before
dependency extraction. After it, `project::analysis::deps` sees an ordinary
`TableFactor::Table` and needs no changes, and the compiled AST handed to
`project::analysis::deployment_snapshot` already carries the resolved physical
name. Errors report a span using the byte offsets that `LocatedStatement` and
the variable resolver already track.

A versioned name is one name with internal structure, not two names. There is
no object called `orders` in the catalog, in the dependency graph, or in any
content hash. `orders@1` is the name, and `orders` is a component of it in the
same way that `public` is the schema component.

`FullyQualifiedName` therefore carries the base name and the version as
fields, rather than the project constructing a second parallel name for
deployment. Its `object()` accessor keeps returning the deployed name, so
every existing caller continues to get identity and needs no audit, and for an
unversioned object the base and deployed names are the same string. Only
identifier validation reads the base name, because `validate_identifier_format`
rejects characters outside lowercase letters, digits, and underscores, and the
user never writes the suffix.

Carrying the version as a number rather than only inside the name is what
makes the later work possible. Reporting "pinned to version 1, newest is 3",
enumerating versions during retirement, and listing valid versions in an error
message all need the version as data. If it lived only inside the name, each
of those would recover it by splitting on `@`, which is the stringly-typed
trap this naming scheme otherwise avoids. The `@` spelling stays confined to a
single `physical_name` function.

### Editor support

The LSP resolves identifiers by lexing the raw file, extracting the
dot-qualified identifier under the cursor, and looking the result up in
`ProjectCache` via `goto_definition::resolve_object_id`. Hover, go-to-definition,
and find-references all funnel through that one path.

A pinned reference needs no work. `"orders@1"` lexes as a single quoted
identifier whose value is `orders@1`, which matches a real object in the cache.

A bare latest-tracking reference breaks without changes, because the cache
holds `"orders@1"` and `"orders@2"` and nothing named `orders`. The lookup
returns nothing, so hover and go-to-definition silently do nothing.

**The newest-version fallback must be a single shared resolver**, called both
by `project::resolve::normalize` during compilation and by
`resolve_object_id` in the LSP. Two implementations would eventually disagree,
and a disagreement here means the editor reports a reference as pointing at one
version while the deployment builds another, with nothing on screen to reveal
it.

With that shared resolver in place:

- **Go-to-definition** on a bare name opens the newest version's file.
- **Find-references** is already correct with no further work. Dependents are
  recorded against the resolved physical name, because normalization runs
  before dependency extraction, so asking who uses `"orders@2"` returns every
  consumer including those that wrote a bare name. This is the same question
  retirement safety depends on.
- **Hover** shows the resolved version's columns, and states the version
  relationship. A latest-tracking reference reports which version it currently
  resolves to and that it will repoint when a newer version is created. A
  pinned reference reports its version and whether a newer one exists, which is
  the prompt that gets stale pins migrated.

Hover is also where the design regains what it gave up by using bare names
instead of a resolver function. The source text alone does not reveal that a
reference is a moving target. The editor does.

**Completion** should offer the bare name as an explicit newest-version
candidate alongside the concrete versions. Without it the latest-tracking
spelling is undiscoverable, and users pin by default because pinning is what
the editor suggested.

### Dirtiness

The semantics come for free from where content hashes are taken. They are
computed from the compiled AST, not from the source file. A model that reads
`orders` compiles to `"orders@2"`. Adding `orders@3.sql` makes that same
unedited source file compile to `"orders@3"`, which changes its hash, which
places it in `ChangedStmt(O)`. The existing fixed point in
`project::analysis::changeset` propagates dirtiness downstream from there,
exactly as it would for a hand edit. A pinned model's compiled AST does not
change, so it keeps reading the version it named.

Pinning preserves which version a consumer reads. It does not exempt the
consumer from redeployment. Dirtiness propagates at schema granularity, by the
existing rule that every object in a dirty schema is dirty, and `promote` swaps
whole schemas. So a pinned model sharing a schema with a latest-tracking model
is rebuilt alongside it, and a materialized view rebuilt this way rehydrates. A
consumer that needs to avoid rebuilds has to live in a schema of its own, or in
a separate project.

Observing those semantics across compiles does take a new mechanism. An
object's compile-cache fingerprint was built from that object's own files, so
a latest-tracking model whose bytes never changed reported a cache hit and its
already-normalized artifact, still naming `"orders@2"`, was reused verbatim.
The compiled AST that the content hash reads is then the stale one, and the
dirtiness rule above never gets the chance to fire. Which name a file resolves
cannot be known without parsing it, which is the work the cache exists to
skip, so the fingerprint folds in a hash of every version declared anywhere in
the project. Adding or removing any version therefore invalidates every cached
object. That is deliberately coarse and deliberately load-bearing. Narrowing
it to the versions an object's own files declare would restore the stale-hit
bug.

### Deployment ordering

Sources and tables are managed by `apply` and are deliberately excluded from
deployment snapshots, so creating a version is not a `stage` operation. The
sequence is `apply tables`, then `stage`, then `promote`, which is already the
order `apply-all` uses.

The gap is readiness. `"orders@3"` must finish its initial snapshot before
anything is staged against it, and `wait` currently understands dataflow
hydration but not snapshot progress. Teaching `wait` about snapshot progress
is the only new runtime capability this design requires.

### Adoption

Turning on versioning for an object that already exists is a rename, and
`ALTER TABLE ... RENAME` and `ALTER SOURCE ... RENAME` are both supported:

```sql
ALTER TABLE orders RENAME TO "orders@1";
```

The rename preserves the ingestion and the data, so there is no re-snapshot.

Models that already read `orders` need no edits at all, because the bare name
now resolves to `"orders@1"`. Their compiled SQL changes, so they are marked
dirty and rebuilt once.

Consumers outside the project break at the rename, since they reference the
old literal name. The tool should list those readers from the catalog before
performing the rename so the user knows who to warn.

### Retirement

Deleting `orders@1.sql` retires version 1. In-project references to
`"orders@1"` become unresolved-name compile errors before anything is
executed, which comes for free.

Readers outside the project are found by querying `mz_object_dependencies`.
Retiring a version that still has live external readers is a hard error,
overridable with `--force`. This is the capability a warehouse-oriented tool
structurally cannot offer, because it has no live view of the catalog it
deploys into.

### Cost, and why the source rule is asymmetric

Two versions of a table fed by the same PostgreSQL source share one
replication slot, because a source ingests the replication stream for its
whole publication through a single slot. The upstream cost of a second table
version is therefore small.

Two versioned sources are two slots and two independent replication streams
against the upstream database. That asymmetry is the reason tables must pin
their source explicitly, and it is worth surfacing in the tooling rather than
leaving it for users to discover.

## Minimal Viable Prototype

An `mzcompose` workflow under `test/mz-deploy`, running against the emulator:

1. Create `orders@1.sql` and a materialized view reading bare `orders`.
   Deploy and confirm the MV resolves to `"orders@1"`.
2. Add a second MV pinning `"orders@1"`.
3. Add `orders@2.sql` with an additional column. Confirm `stage --dry-run`
   reports the latest-tracking MV as dirty and the pinned MV as unchanged.
4. Promote. Confirm the latest-tracking MV now reads `"orders@2"` and the
   pinned MV still reads `"orders@1"`.
5. Delete `orders@1.sql` while the pinned MV still exists. Confirm the
   compile error names the pinned MV.
6. Remove the pinned MV, retire version 1, and confirm the external-reader
   check runs.

Steps 1 through 4 are the whole value proposition and are worth building
before anything else.

## Alternatives

### A resolver function, `version(orders)` and `version(orders, 2)`

Considered at length and rejected. A function call parses as
`TableFactor::Function` and would need no parser changes, and it makes the
intent explicit at the call site in a way a bare name does not.

Three problems sank it. It only parses where a table factor is legal, so
`CREATE SINK s FROM version(orders)` fails and sinks could never track the
newest version. Adoption would require editing every consumer to say
`version(orders)` purely to preserve existing behavior. And it puts the
visually distinctive spelling on the common case rather than on the
exceptional one, when pinning is the deliberate act that deserves to stand
out.

### An `@latest` selector, `"orders@latest"`

A magic identifier that parses in every position, unlike a function. Rejected
because once bare names resolve to the newest version, `"orders@latest"` is a
second spelling for something already expressible, and two ways to say the
same thing is a cost reviewers pay forever.

### Whole-schema versioning

Version the schema rather than the object, so `public_v1` and `public_v2` each
hold a complete copy of the table and everything downstream of it. This is
what our zero-downtime guides describe, and it lets a consumer pin one
coherent set of objects.

Rejected because it duplicates every model in the schema, including models
that have nothing to do with the changed table, and each duplicate is a
standing dataflow rather than a batch job that finishes. It also forces the
project tree to either hold N copies of every model or grow a templating layer
to generate them.

### Reserving the `_v<N>` suffix, so the physical name is `orders_v2`

More pleasant to type than `"orders@2"`, at the cost of making `*_v<digits>` a
reserved pattern across the whole project. Rejected because that is a common
hand-naming convention, including in our own documentation, and taking it away
would produce compile errors that feel arbitrary to whoever hits them.

### Freezing old definitions into a generated lockfile

Snapshot prior definitions into a checked-in artifact so old versions are
immutable by construction. Rejected because it solves duplication, and a
source or table definition is a few lines rather than a subgraph. Old
versions sitting untouched in git are already immutable enough.

## Open questions

- Should a code lens above a versioned object's `CREATE` statement show its
  position in the version set, for example "version 2 of 3, newest"? Hover
  covers the reference site, but the definition site has no equivalent.
- Should a profile-only version, meaning an `orders@3#dev.sql` with no
  default variant, be allowed silently or require an explicit opt-in? It is
  useful for trying an upstream change without a production ingest, but it
  makes the newest version profile-dependent in files that show no sign of it.
- Should the tool warn when a project has more than some number of live
  versions of one object, as a nudge toward retirement?
- Does `stage` need to refuse to run when a newly created version has not
  finished snapshotting, or is reporting it through `wait` sufficient?
- Is a compile error the right response to a table naming a versioned source
  without a version, or should it be a warning with a default of pinning to
  the newest version at creation time?
