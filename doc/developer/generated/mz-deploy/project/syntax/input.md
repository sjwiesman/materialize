---
source: src/mz-deploy/src/project/syntax/input.rs
revision: a647094cc4
---

# mz_deploy::project::syntax::input

Source-owned data types holding the parsed statements and source locations for a
single database object before any semantic validation. Loading these files and
assembling them into per-schema groupings happens in
`crate::project::compiler`.

`ObjectVariant` is one file variant of an object: a `path`, an optional
`profile` name (`None` for the default `name.sql`, `Some` for a
`name#<profile>.sql` override), and the `statements` parsed from it as a
`Vec<LocatedStatement>` (each statement paired with its byte offset).

`DatabaseObject` is one logical object name in a schema directory. It carries the
object `name` (no extension or profile suffix), the directory-derived `database`
and `schema` names, and all of its `variants` (at least one). The `database`
field is the name without any profile suffix applied; per-object validation
matches it against the database declared in the user's SQL, and the suffix is
reapplied to the compiled statement after assembly. A typical object file holds
one primary CREATE statement plus zero or more supporting statements (indexes,
grants, comments); all are parsed into `statements` without validating their
relationships or correctness — that is the compiler's job.
