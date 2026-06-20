---
source: src/mz-deploy/src/project/resolve/normalize/tests.rs
revision: 673fdb9d44
---

# mz_deploy::project::resolve::normalize::tests

Unit tests for the name-normalization machinery in `resolve::normalize`. Each
test parses a `CREATE VIEW` via `parse_statements`, runs a `NormalizingVisitor`
over the view's query with a fixed test fqn (typically `materialize.public.…`),
formats the result with `to_ast_string(FormatMode::Simple)`, and asserts on which
references were rewritten.

The `fully_qualifying` cases verify that external tables become
`database.schema.object` while CTE names stay unqualified across a wide range of
shapes: single and multiple CTEs, nested CTE scopes, CTEs in joins, a simple CTE
that shadows a catalog object (only the inner reference is qualified), the
mutually-recursive form, subqueries in `HAVING` and inside `AND`/`OR`/comparison/
arithmetic operators, and `LATERAL` joins. A dedicated group checks implicit
aliasing — unqualified, schema-qualified, and fully-qualified tables each receive
an `AS <table>` alias, explicit aliases are preserved, and CTEs receive none.

The `flattening` cases assert names collapse to quoted `"db.schema.object"`
identifiers (with CTEs left alone). The `staging` cases exercise
`StagingTransformer`: schema suffixing of unqualified and schema-qualified names,
exemption of external dependencies and of objects absent from
`objects_to_deploy`, the keyword-named-object regression (a quoted reserved name
such as `order` is still recognized as deployed and suffixed), and replacement
objects — references to a replacement object are not suffixed, while an empty
replacement set on a first/full deploy suffixes everything. Tests that exercise
the SQL parser are annotated to be skipped under miri.
