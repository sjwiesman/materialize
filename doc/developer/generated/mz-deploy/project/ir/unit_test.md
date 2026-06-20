---
source: src/mz-deploy/src/project/ir/unit_test.rs
revision: a647094cc4
---

# mz_deploy::project::ir::unit_test

IR representation of a parsed `EXECUTE UNIT TEST` statement, stored on the project
graph by the compiler. Validation and SQL lowering happen later in the test runner
(`cli::commands::test::lower`).

`UnitTest` captures a test definition: its `name`, the `target_view` being tested
(a fully qualified name), an optional `at_time` (a timestamp for `mz_now()` during
execution), a list of `mocks`, and an `expected` result. `from_execute_statement`
builds a `UnitTest` from the parsed `ExecuteUnitTestStatement` AST node, rendering
the target, at-time, mock definitions, and expected definition to strings via
`AstDisplay` in `FormatMode::Simple`.

`MockView` is a mock that replaces a real dependency during the test: a fully
qualified `fqn`, `columns` as `(name, type)` pairs, and a SQL `query` body.
`ExpectedResult` holds the test's expected `columns` (again `(name, type)` pairs)
and `query` body.
