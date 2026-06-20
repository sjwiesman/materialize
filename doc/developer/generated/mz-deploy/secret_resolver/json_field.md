---
source: src/mz-deploy/src/secret_resolver/json_field.rs
revision: a647094cc4
---

# mz_deploy::secret_resolver::json_field

Shared helper for extracting a top-level string field from a JSON-shaped secret,
used by providers that support the `name(secret, field)` shape (such as
`aws_secret` for RDS-style credential blobs).

`extract_json_field` parses the secret string with `serde_json`, looks up the
named key, and returns its value when it is a JSON string. Each failure path —
invalid JSON, a missing key, or a non-string value — yields a descriptive reason
string (mentioning the secret name and key) suitable for wrapping in
`SecretResolveError::ResolutionFailed`.
