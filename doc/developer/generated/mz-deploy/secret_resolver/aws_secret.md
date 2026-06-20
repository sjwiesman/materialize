---
source: src/mz-deploy/src/secret_resolver/aws_secret.rs
revision: a647094cc4
---

# mz_deploy::secret_resolver::aws_secret

The `aws_secret` secret provider, resolving `CREATE SECRET … AS aws_secret(…)`
expressions against AWS Secrets Manager. With one argument it returns the raw
secret string; with a second argument it parses the secret as JSON and returns
that top-level field (for RDS-style `{"username":…,"password":…}` blobs).

`AwsSecretProvider` implements `SecretProvider`, accepting 1 or 2 arguments. It
holds the AWS `profile` name and a `OnceCell<Client>`; the SDK config (including
credential resolution) is loaded lazily on the first `resolve` call via
`client()`, so projects that set `aws_profile` but never call `aws_secret()` pay
no startup cost. `resolve` fetches the secret with `get_secret_value`, raising
`SecretResolveError::ResolutionFailed` on fetch failure or when the secret is
binary rather than text; when a JSON key is supplied it delegates to
`json_field::extract_json_field`.

`UnconfiguredAwsProvider` is the placeholder registered when `aws_profile` is not
set in `project.toml`. It reports the same provider name and argument arity but
always fails with a message directing the user to set `aws_profile` under
`[<profile>.security]`. Both providers share the `PROVIDER_NAME` constant
`"aws_secret"`.
