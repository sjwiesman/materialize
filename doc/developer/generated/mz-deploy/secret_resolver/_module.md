---
source: src/mz-deploy/src/secret_resolver.rs
revision: a647094cc4
---

# mz_deploy::secret_resolver

Resolves the client-side provider expressions in `CREATE SECRET` statements at
apply time rather than at compile time, so `compile` works without access to
secret material. A secret value may reference a provider function such as
`env_var('MY_VAR')` in place of an inline string literal; this module rewrites
those calls into resolved string literals and passes everything else through to
Materialize unchanged.

`SecretResolver` owns a map of `SecretProvider` implementations keyed by
function name. `new` always registers the `env_var` provider; for AWS it
registers the real Secrets Manager provider when an `aws_profile` is configured,
or a placeholder that errors clearly when it is not. `resolve_expr` matches a
single-segment `Expr::Function` against the registered providers, validates the
argument count against the provider's `accepted_args` range, requires string
literal arguments, and replaces the call with the resolved value; non-matching
expressions are returned untouched. `resolve_create_secret`,
`resolve_secret_for_cli`, and `resolve_statement_for_cli` apply this to
`CREATE SECRET` statements, the latter two mapping failures onto `CliError`.
`SecretResolveError` enumerates the failure modes (wrong argument count,
non-literal argument, resolution failure).

`SecretProvider` is the trait each provider implements: it reports its function
`name`, its `accepted_args` count range, and an async `resolve`.

## Submodules

- **`env_var`** — `EnvVarProvider`, which reads a secret from an environment
  variable.
- **`aws_secret`** — `AwsSecretProvider`, which reads from AWS Secrets Manager,
  and `UnconfiguredAwsProvider`, the placeholder registered when `aws_profile`
  is unset.
- **`json_field`** — Shared helper for extracting a top-level string field from
  a JSON-shaped secret, used by providers that support the
  `name(secret, field)` shape.
