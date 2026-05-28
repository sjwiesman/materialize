// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS Secrets Manager secret provider.
//!
//! Resolves secret values by reading from AWS Secrets Manager. Returns the
//! raw secret string, or — when a second argument is given — extracts that
//! top-level field from a JSON-shaped secret (e.g. the `{"username":"…",
//! "password":"…"}` blobs that RDS-style secrets use).
//!
//! Requires `aws_profile` to be set in `project.toml`.

use super::{SecretProvider, SecretResolveError};
use async_trait::async_trait;
use aws_sdk_secretsmanager::Client;
use std::ops::RangeInclusive;
use tokio::sync::OnceCell;

/// Function name shared by both the real and unconfigured providers.
const PROVIDER_NAME: &str = "aws_secret";

/// Resolves secrets from AWS Secrets Manager.
///
/// Usage in SQL:
///
/// - `CREATE SECRET x AS aws_secret('my-secret-name')` — returns the raw
///   secret string.
/// - `CREATE SECRET x AS aws_secret('my-secret-name', 'password')` — parses
///   the secret as JSON and returns the value of the top-level `password`
///   field.
///
/// The AWS SDK config (including credential resolution) is loaded lazily
/// on the first `resolve()` call, so projects that set `aws_profile` but
/// never use `aws_secret()` pay no startup cost.
pub(super) struct AwsSecretProvider {
    profile: String,
    client: OnceCell<Client>,
}

impl AwsSecretProvider {
    pub(super) fn new(profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> &Client {
        self.client
            .get_or_init(|| async {
                let config = mz_aws_util::defaults()
                    .profile_name(&self.profile)
                    .load()
                    .await;
                Client::new(&config)
            })
            .await
    }
}

#[async_trait]
impl SecretProvider for AwsSecretProvider {
    fn name(&self) -> &str {
        PROVIDER_NAME
    }

    fn accepted_args(&self) -> RangeInclusive<usize> {
        1..=2
    }

    async fn resolve(&self, args: &[String]) -> Result<String, SecretResolveError> {
        let secret_name = &args[0];
        let client = self.client().await;
        let result = client
            .get_secret_value()
            .secret_id(secret_name)
            .send()
            .await
            .map_err(|e| SecretResolveError::ResolutionFailed {
                name: self.name().to_string(),
                reason: format!("failed to fetch secret '{}': {}", secret_name, e),
            })?;

        let secret_string =
            result
                .secret_string()
                .ok_or_else(|| SecretResolveError::ResolutionFailed {
                    name: self.name().to_string(),
                    reason: format!(
                        "secret '{}' is a binary secret; only text secrets are supported",
                        secret_name
                    ),
                })?;

        match args.get(1) {
            None => Ok(secret_string.to_string()),
            Some(json_key) => {
                extract_json_field(secret_string, json_key, secret_name).map_err(|reason| {
                    SecretResolveError::ResolutionFailed {
                        name: self.name().to_string(),
                        reason,
                    }
                })
            }
        }
    }
}

/// Extract a top-level string field from a JSON secret.
///
/// Returns the field's string content on success. On failure, returns a
/// reason string suitable for [`SecretResolveError::ResolutionFailed`].
fn extract_json_field(
    secret_string: &str,
    json_key: &str,
    secret_name: &str,
) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(secret_string).map_err(|e| {
        format!(
            "secret '{}' is not valid JSON; cannot extract field '{}': {}",
            secret_name, json_key, e
        )
    })?;

    let field = value
        .get(json_key)
        .ok_or_else(|| format!("secret '{}' has no field '{}'", secret_name, json_key))?;

    match field {
        serde_json::Value::String(s) => Ok(s.clone()),
        _ => Err(format!(
            "field '{}' in secret '{}' is not a string",
            json_key, secret_name
        )),
    }
}

/// Placeholder provider registered when `aws_profile` is not set in `project.toml`.
///
/// Always returns an error directing the user to configure `aws_profile`.
pub(super) struct UnconfiguredAwsProvider;

#[async_trait]
impl SecretProvider for UnconfiguredAwsProvider {
    fn name(&self) -> &str {
        PROVIDER_NAME
    }

    fn accepted_args(&self) -> RangeInclusive<usize> {
        1..=2
    }

    async fn resolve(&self, _args: &[String]) -> Result<String, SecretResolveError> {
        Err(SecretResolveError::ResolutionFailed {
            name: self.name().to_string(),
            reason: "AWS Secrets Manager is not configured. Set 'aws_profile' under [<profile>.security] in project.toml to enable aws_secret().".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_field_returns_string_value() {
        let secret = r#"{"username":"alice","password":"s3cr3t"}"#;
        assert_eq!(
            extract_json_field(secret, "password", "rds-creds").unwrap(),
            "s3cr3t"
        );
        assert_eq!(
            extract_json_field(secret, "username", "rds-creds").unwrap(),
            "alice"
        );
    }

    #[test]
    fn extract_json_field_errors_on_missing_key() {
        let secret = r#"{"username":"alice"}"#;
        let err = extract_json_field(secret, "password", "rds-creds").unwrap_err();
        assert!(err.contains("rds-creds"));
        assert!(err.contains("password"));
    }

    #[test]
    fn extract_json_field_errors_on_non_string_value() {
        let secret = r#"{"port":5432}"#;
        let err = extract_json_field(secret, "port", "rds-creds").unwrap_err();
        assert!(err.contains("rds-creds"));
        assert!(err.contains("port"));
        assert!(err.contains("not a string"));
    }

    #[test]
    fn extract_json_field_errors_on_invalid_json() {
        let secret = "not json at all";
        let err = extract_json_field(secret, "password", "rds-creds").unwrap_err();
        assert!(err.contains("rds-creds"));
        assert!(err.contains("not valid JSON"));
    }
}
