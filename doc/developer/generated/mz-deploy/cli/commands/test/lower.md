---
source: src/mz-deploy/src/cli/commands/test/lower.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::test::lower

Validates a `UnitTest` and lowers it into SQL that Materialize can execute. The
lowered form is a sequence of `CREATE TEMPORARY VIEW` statements (one per mock,
one for `expected`, one for the flattened target) followed by an assertion query
whose rows describe mismatches — an empty result means the test passed.

`validate_unit_test` performs three checks against the known column schemas
(supplied by a `get_columns` closure backed by `types.lock` and the build
artifacts): every dependency of the target view is mocked; each mock's columns
match the mocked object's actual schema; and the expected output columns match
the target's schema. Failures are reported through the `TestValidationError` enum
and its per-case structs (`UnmockedDependencyError`, `MockSchemaMismatchError`,
`ExpectedSchemaMismatchError`, `InvalidAtTimeError`), each with a rich,
color-coded `Display` impl for terminal output. `compare_columns` computes the
extra/missing/type-mismatch sets; `normalize_type` canonicalizes Materialize type
aliases (recursing into `list`, `[]`, and `map[...]` containers) and
`types_match_with_bare_containers` tolerates the bare container types that
`SHOW COLUMNS` returns. `normalize_fqn` expands partial mock names against the
target's database/schema.

`lower_unit_test` produces the ordered SQL: `create_mock_view_sql` and
`create_expected_view_sql` build `CREATE TEMPORARY VIEW ... WITH MUTUALLY
RECURSIVE data(...)` statements; `create_target_view_sql` rewrites the target's
`CREATE VIEW`/`CREATE MATERIALIZED VIEW` into a flattened temporary view via
`NormalizingVisitor::flattening` (erroring for other object kinds); and
`create_test_query_sql` builds the MISSING/UNEXPECTED `EXCEPT`/`UNION ALL`
assertion, appending an `AS OF ...::mz_timestamp` clause when the test specifies
`AT TIME`. `flatten_fqn` quotes a dotted name as a single identifier. A unit-test
module covers the SQL builders, `normalize_fqn`, `normalize_type`, and the
validation paths.
