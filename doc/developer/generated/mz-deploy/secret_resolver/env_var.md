---
source: src/mz-deploy/src/secret_resolver/env_var.rs
revision: a647094cc4
---

# mz_deploy::secret_resolver::env_var

The `env_var` secret provider, resolving `CREATE SECRET … AS env_var('MY_ENV_VAR')`
expressions from process environment variables.

`EnvVarProvider` is a unit struct implementing `SecretProvider`. Its `name` is
`"env_var"`, it accepts exactly one argument, and `resolve` reads that environment
variable via `std::env::var`, returning its value or
`SecretResolveError::ResolutionFailed` with a message noting the variable is not
set.
