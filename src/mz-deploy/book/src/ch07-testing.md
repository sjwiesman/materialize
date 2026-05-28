# Testing

*What you'll learn: how to write `EXECUTE UNIT TEST` blocks that pin view logic without touching production.*

## Why unit tests for SQL

A materialized view encodes business logic. That logic — SLA thresholds, eligibility rules, rollup conditions — is often the part of your system most likely to be subtly wrong, and the hardest to inspect once data is flowing through it.

`EXECUTE UNIT TEST` lets you write tests for individual views with no streaming infrastructure involved. Each test supplies controlled data through `MOCK` clauses, then asserts the exact rows the view should produce. Tests run against a local Docker-hosted Materialize container. Remote databases are never touched.

- **Fast feedback.** Tests complete in seconds, without waiting for sources to hydrate.
- **Determinism.** You control every input row. There are no race conditions.
- **Regression coverage.** A refactor that accidentally changes output rows fails the test before it reaches CI.

Tests are not a substitute for integration tests against real sources and sinks. What they cover — and what they do not — is addressed at the end of this chapter.

## `EXECUTE UNIT TEST` syntax

Tests live inline in the same `.sql` file as the view they test. The general form is:

```sql
EXECUTE UNIT TEST test_name
FOR database.schema.view_name
MOCK database.schema.dependency(col1 TYPE, col2 TYPE) AS (
  SELECT * FROM VALUES (val1, val2), (val3, val4)
),
EXPECTED(col1 TYPE, col2 TYPE) AS (
  SELECT * FROM VALUES (expected1, expected2)
);
```

One file can contain a `CREATE VIEW` or `CREATE MATERIALIZED VIEW` followed by any number of `EXECUTE UNIT TEST` blocks.

### Syntax reference

| Clause | Required | Description |
|--------|----------|-------------|
| `EXECUTE UNIT TEST name` | Yes | Name shown in test output |
| `FOR database.schema.view` | Yes | Fully qualified target view |
| `AT TIME 'expr'` | No | Value for `mz_now()` during test |
| `MOCK fqn(cols) AS (query)` | Yes\* | One per dependency of the target |
| `EXPECTED(cols) AS (query)` | Yes | Expected output rows and types |

\*Every dependency of the target view must have a corresponding `MOCK`. Mock names can be unqualified (`accounts`), schema-qualified (`raw.accounts`), or fully qualified (`materialize.raw.accounts`). Partial names are resolved relative to the target view.

### `AT TIME`

Views that call `mz_now()` for temporal filters need a pinned timestamp for tests to be deterministic. Supply it with `AT TIME`:

```sql
EXECUTE UNIT TEST test_recent_events
FOR materialize.public.recent_events
AT TIME '2024-01-15T12:00:00Z'
MOCK ...
EXPECTED ...;
```

Without `AT TIME`, `mz_now()` returns the wall clock at test execution. Tests that depend on the current time will pass or fail inconsistently. Always use `AT TIME` for views that reference `mz_now()`.

## A worked example

The `customer` view from [Your first project](./ch03-first-project.md) joins three raw tables into a single canonical customer entity:

```sql
CREATE MATERIALIZED VIEW customer
IN CLUSTER app
AS
SELECT
    a.id              AS account_id,
    a.signed_up_at,
    a.status,
    addr.line1        AS address_line1,
    addr.city         AS address_city,
    addr.region       AS address_region,
    addr.country      AS address_country,
    email.value       AS primary_email,
    phone.value       AS phone_number
FROM raw.accounts a
LEFT JOIN raw.addresses addr
       ON addr.account_id = a.id
LEFT JOIN raw.contact_methods email
       ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN raw.contact_methods phone
       ON phone.account_id = a.id AND phone.kind = 'phone';
```

All joins are LEFT JOINs, so an account that has no address or no contact methods still appears in the output — with NULL in the missing columns. The three test cases below pin each of those outcomes.

The `customer` MV does not call `mz_now()`, so no `AT TIME` clause is needed.

### Test case 1 — account with full data

An account exists with one address row, one email contact method, and one phone contact method. The expected output is one row with all nine columns populated.

```sql
EXECUTE UNIT TEST test_account_with_full_data
FOR materialize.public.customer
MOCK materialize.raw.accounts(
    id           bigint,
    signed_up_at timestamptz,
    status       text
) AS (
    SELECT * FROM VALUES (
        1::bigint,
        '2024-01-15T09:00:00Z'::timestamptz,
        'active'::text
    )
),
MOCK materialize.raw.addresses(
    account_id bigint,
    line1      text,
    city       text,
    region     text,
    country    text
) AS (
    SELECT * FROM VALUES (
        1::bigint,
        '123 Main St'::text,
        'Springfield'::text,
        'IL'::text,
        'US'::text
    )
),
MOCK materialize.raw.contact_methods(
    account_id bigint,
    kind       text,
    value      text
) AS (
    SELECT * FROM VALUES
        (1::bigint, 'email'::text, 'alice@example.com'::text),
        (1::bigint, 'phone'::text, '+1-555-0100'::text)
),
EXPECTED(
    account_id    bigint,
    signed_up_at  timestamptz,
    status        text,
    address_line1 text,
    address_city  text,
    address_region text,
    address_country text,
    primary_email text,
    phone_number  text
) AS (
    SELECT * FROM VALUES (
        1::bigint,
        '2024-01-15T09:00:00Z'::timestamptz,
        'active'::text,
        '123 Main St'::text,
        'Springfield'::text,
        'IL'::text,
        'US'::text,
        'alice@example.com'::text,
        '+1-555-0100'::text
    )
);
```

### Test case 2 — account with no address

An account exists but has no row in `raw.addresses`. The address columns should be NULL; contact method columns are still populated.

```sql
EXECUTE UNIT TEST test_account_with_no_address
FOR materialize.public.customer
MOCK materialize.raw.accounts(
    id           bigint,
    signed_up_at timestamptz,
    status       text
) AS (
    SELECT * FROM VALUES (
        2::bigint,
        '2024-03-01T14:00:00Z'::timestamptz,
        'active'::text
    )
),
MOCK materialize.raw.addresses(
    account_id bigint,
    line1      text,
    city       text,
    region     text,
    country    text
) AS (
    SELECT * FROM VALUES (NULL::bigint, NULL::text, NULL::text, NULL::text, NULL::text)
    WHERE false
),
MOCK materialize.raw.contact_methods(
    account_id bigint,
    kind       text,
    value      text
) AS (
    SELECT * FROM VALUES
        (2::bigint, 'email'::text, 'bob@example.com'::text)
),
EXPECTED(
    account_id    bigint,
    signed_up_at  timestamptz,
    status        text,
    address_line1 text,
    address_city  text,
    address_region text,
    address_country text,
    primary_email text,
    phone_number  text
) AS (
    SELECT * FROM VALUES (
        2::bigint,
        '2024-03-01T14:00:00Z'::timestamptz,
        'active'::text,
        NULL::text,
        NULL::text,
        NULL::text,
        NULL::text,
        'bob@example.com'::text,
        NULL::text
    )
);
```

### Test case 3 — account with no contact methods

An account exists with an address but no rows in `raw.contact_methods`. Both `primary_email` and `phone_number` should be NULL.

```sql
EXECUTE UNIT TEST test_account_with_no_contact_methods
FOR materialize.public.customer
MOCK materialize.raw.accounts(
    id           bigint,
    signed_up_at timestamptz,
    status       text
) AS (
    SELECT * FROM VALUES (
        3::bigint,
        '2024-06-10T08:30:00Z'::timestamptz,
        'pending'::text
    )
),
MOCK materialize.raw.addresses(
    account_id bigint,
    line1      text,
    city       text,
    region     text,
    country    text
) AS (
    SELECT * FROM VALUES (
        3::bigint,
        '456 Oak Ave'::text,
        'Portland'::text,
        'OR'::text,
        'US'::text
    )
),
MOCK materialize.raw.contact_methods(
    account_id bigint,
    kind       text,
    value      text
) AS (
    SELECT * FROM VALUES (NULL::bigint, NULL::text, NULL::text)
    WHERE false
),
EXPECTED(
    account_id    bigint,
    signed_up_at  timestamptz,
    status        text,
    address_line1 text,
    address_city  text,
    address_region text,
    address_country text,
    primary_email text,
    phone_number  text
) AS (
    SELECT * FROM VALUES (
        3::bigint,
        '2024-06-10T08:30:00Z'::timestamptz,
        'pending'::text,
        '456 Oak Ave'::text,
        'Portland'::text,
        'OR'::text,
        'US'::text,
        NULL::text,
        NULL::text
    )
);
```

All three tests belong in the same file as the `CREATE MATERIALIZED VIEW` statement. The file grows naturally: add a new test block whenever you find or fix a case worth pinning.

## What runs where

Tests run against a local Docker-hosted Materialize container named `mz-deploy-typecheck`. This is the same container used by `mz-deploy explain` (described in [Compiling](./ch06-compiling.md)). If you have already run `explain` locally, the container is probably already present.

The test runner:

1. Compiles the project and discovers all `EXECUTE UNIT TEST` statements.
2. Starts the container if it is not already running, or reuses it if it is.
3. For each test, creates temporary views for the mocks, the expected result, and a rewritten copy of the target view that references the mocks instead of real tables.
4. Runs a symmetric-difference query. Rows in expected but not in actual are labeled `MISSING`; rows in actual but not in expected are labeled `UNEXPECTED`. The test passes only if no rows are returned.
5. Runs `DISCARD ALL` between tests to avoid state leakage.

Your remote Materialize environment is never queried. `mz-deploy test` does not require a profile, credentials, or a network connection beyond reaching Docker.

## Filtering and CI

Run all tests:

```bash
mz-deploy test
```

Run tests for a specific view:

```bash
mz-deploy test 'materialize.public.customer'
```

Run a single named test:

```bash
mz-deploy test 'materialize.public.customer#test_account_with_full_data'
```

Run all tests in a schema:

```bash
mz-deploy test 'materialize.public.*'
```

A trailing `*` matches all values at that position and beyond. Without a filter, all tests are run.

### Exit codes

| Exit code | Meaning |
|-----------|---------|
| `0` | All tests passed, or no test files were found (without a filter) |
| `1` | One or more tests failed, validation errors were found, or a filter matched no tests |

### JUnit XML output for CI

```bash
mz-deploy test --junit-xml results.xml
```

The `--junit-xml` flag writes test results in JUnit XML format. GitHub Actions, Jenkins, and GitLab CI all accept JUnit XML for test result annotations and trend tracking. Add the flag to your CI step and point the CI system at the output file.

## What tests don't cover

`EXECUTE UNIT TEST` pins view logic against controlled inputs. It does not cover:

- **Sources.** Whether a Kafka topic, Postgres publication, or webhook delivers data as expected is outside the scope of unit tests. Source correctness belongs in integration tests against real infrastructure.
- **Sinks.** Whether output reaches a downstream system — and in what format — cannot be verified by testing a view in isolation.
- **Mutation timing.** The symmetric-difference check is a set comparison. It does not verify the order in which rows appear, how views behave as data arrives incrementally, or how retractions propagate through a pipeline.
- **Cluster behavior.** Memory usage, hydration time, and query performance under load are not observable from a unit test.

Use `EXECUTE UNIT TEST` to pin the transformation logic inside each view. Use a staging environment or integration tests against a live Materialize region to verify end-to-end behavior.

---

You can now:

- Write a unit test for any view or materialized view in your project.
- Mock each dependency a view reads from.
- Run the full suite or a single test, locally or in CI.
- Decide what belongs in `EXECUTE UNIT TEST` vs. integration.
