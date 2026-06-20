---
source: src/mz-deploy/src/cli/commands/test.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::test

Implements the `test` command, which runs `EXECUTE UNIT TEST` assertions
attached to project objects. The `lower` submodule does validation and SQL
lowering; this file owns orchestration and presentation.

`run` calls `run_tests`, prints per-test outcomes and the summary, optionally
writes a JUnit XML report, and returns `CliError::TestsFailed` when any test
failed or failed validation. `run_tests` compiles the project (optionally over an
overlay filesystem), parses an optional `TestFilter` (a
`database.schema.object#test_name` pattern with `*` and omitted trailing segments
acting as wildcards), loads/generates the type cache via
`load_or_generate_types_cache`, and iterates the project's tests. Each test runs
through `run_single_test`; `TestTarget` selects where — an ephemeral Docker
container (default) or the profile's region (`--no-docker`) — and either way the
test gets its own fresh connection so its temporary objects and session state die
with it.

`run_single_test` validates the test (`lower::validate_unit_test`, then
`validate_at_time` against `mz_timestamp` casting), lowers it to SQL
(`lower::lower_unit_test`), executes the setup statements, runs the final
assertion query, and classifies the result as `TestOutcome::Passed` (zero rows),
`Failed` (rows or a runtime error, via `ExecutionFailure`), or `ValidationFailed`
(`ValidationFailure`). `extract_assertion_data` splits failing rows into MISSING
and UNEXPECTED groups. `TestResultEntry` / `TestResults` are serializable and
build the JUnit report (one `TestSuite` per target object); failure messages are
regenerated from structured row data by `format_assertion_rows_for_junit`, while
`format_assertion_rows` renders the colored terminal table. `print_summary` and
`print_test_outcome` handle terminal output.
