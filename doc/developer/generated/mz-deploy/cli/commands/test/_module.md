---
source: src/mz-deploy/src/cli/commands/test.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::test

The `test` subcommand: run the SQL unit tests attached to project objects via
`EXECUTE UNIT TEST` statements. This module file is both the command's entry
point and the overview that owns the `lower` child.

`run` drives the full pipeline and owns all presentation. `run_tests` compiles
the project (`project::plan`), loads or regenerates the type metadata cache
(`load_or_generate_types_cache`, which runs the offline typechecker and opens a
`ProjectCache`), parses the optional filter, and iterates the project's tests.
`TestFilter` parses `database.schema.object#test_name` patterns with `*` and
omitted trailing segments acting as wildcards; `matches` selects which tests to
run. Each test executes through `run_single_test` against a `TestTarget` —
either an ephemeral Docker container (the default, `DockerRuntime`) or, under
`--no-docker`, a fresh pinned connection to the profile's region. Every test
gets its own connection so its `CREATE TEMPORARY VIEW` objects and session
state are discarded on drop, keeping tests isolated.

`run_single_test` validates the definition (`lower::validate_unit_test`),
pre-checks any `AT TIME` expression by casting to `mz_timestamp`, locates the
target view, lowers the test into SQL statements (`lower::lower_unit_test`),
runs the setup statements, then runs the final assertion query. A test passes
when the assertion query returns zero rows; returned rows mean expected data is
MISSING or unexpected data appeared. `TestOutcome` classifies each result as
`Passed`, `Failed` (an `ExecutionFailure` — a runtime error or an
`AssertionFailed` carrying structured missing/unexpected row data), or
`ValidationFailed` (a broken definition, tracked separately so the summary
distinguishes definition errors from runtime failures).

`TestResults` and `TestResultEntry` are serializable and form the canonical
intermediate from which `to_junit_report` builds a JUnit XML report (one
`TestSuite` per target object). `format_assertion_rows` renders failing rows as
an ANSI-colored terminal table; `format_assertion_rows_for_junit` renders them
as plain `column=value` text for CI reports; `extract_assertion_data` pulls the
status column and data columns out of the query rows.

## Child

- **`lower`** — Lowers a `UnitTest` into the temporary mocked views plus the
  assertion query that `run_single_test` executes, and defines the
  test-validation error types (`TestValidationError`, `InvalidAtTimeError`).
