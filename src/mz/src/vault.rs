// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::{Context, Result};
use aws_sdk_secretsmanager::model::Tag;
use clap::Parser;
use mz_ore::option::OptionExt;
use serde::{de::Visitor, Deserialize, Serialize};

use crate::configuration::FronteggAPIToken;

const INLINE_PREFIX: &str = "mzp_";

#[cfg(target_os = "macos")]
const APPLE_KEYCHAIN: &str = "apple_keychain";

const SECRET_MANAGER_PREFIX: &str = "arn:aws:secretsmanager:";

/// A vault stores sensitive information and provides
/// back a token than can be used for latter retrieval.
/// If no specific vault is selected, values are inlined
/// directly in the token.
#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("vault").multiple(false))]
pub struct Vault {
    /// Store the app password in the local
    /// apple keychain.
    #[cfg(target_os = "macos")]
    #[clap(long, short, group = "vault")]
    apple: bool,

    /// Store the app password in an AWS
    /// Secret Manager using the default
    /// provider chain.
    #[clap(long, short, group = "vault")]
    secret_manager: bool,
}

impl Vault {
    pub async fn store(
        &self,
        profile: &str,
        email: &str,
        api_token: FronteggAPIToken,
    ) -> Result<Token> {
        let password = api_token.to_string();

        if self.secret_manager {
            let shared_config = aws_config::load_from_env().await;
            let client = aws_sdk_secretsmanager::Client::new(&shared_config);

            let name = {
                let mut hasher = DefaultHasher::new();
                email.hash(&mut hasher);
                format!("mz_{profile}_{}", hasher.finish())
            };

            let result = client
                .create_secret()
                .name(&name)
                .secret_string(&password)
                .description("App password for accessing Materialize")
                .tags(Tag::builder().key("user").value(email).build())
                .send()
                .await;

            let arn = match result {
                Ok(output) => output.arn().owned(),
                Err(e) => {
                    let service_error = e.into_service_error();
                    if service_error.is_resource_exists_exception() {
                        client
                            .update_secret()
                            .secret_id(&name)
                            .secret_string(&password)
                            .send()
                            .await
                            .context("failed to store password in secret manager")?
                            .arn()
                            .owned()
                    } else {
                        Err(service_error).context("failed to store password in secret manager")?
                    }
                }
            };

            let arn = arn.context("failed to extract arn from secret manager")?;

            return Ok(Token::SecretManager(arn));
        }

        #[cfg(target_os = "macos")]
        if self.apple {
            use security_framework::passwords;

            let account = apple_account_id(profile, email);
            passwords::set_generic_password("Materialize CLI", &account, password.as_bytes())
                .context("failed to store password in apple keychain")?;

            return Ok(Token::AppleKeyChain);
        }

        Ok(Token::Inline(password))
    }
}

pub enum Token {
    #[cfg(target_os = "macos")]
    AppleKeyChain,
    Inline(String),
    SecretManager(String),
}

impl Token {
    pub async fn retrieve(&self, profile: &str, email: &str) -> Result<String> {
        match self {
            #[cfg(target_os = "macos")]
            Token::AppleKeyChain => {
                use security_framework::passwords;

                let account = apple_account_id(profile, email);
                let bytes = passwords::get_generic_password("Materialize CLI", &account)
                    .context("failed to retrieve password from apple keychain")?;

                let app_password = String::from_utf8(bytes)
                    .context("failed decode password from apple keychain")?;

                Ok(app_password)
            }
            Token::Inline(app_password) => Ok(app_password.to_string()),
            Token::SecretManager(arn) => {
                let shared_config = aws_config::load_from_env().await;
                let client = aws_sdk_secretsmanager::Client::new(&shared_config);

                let result = client
                    .get_secret_value()
                    .secret_id(arn)
                    .send()
                    .await
                    .context("failed to retrieve password from secret manager")?;

                let app_password = result
                    .secret_string()
                    .owned()
                    .context("failed to retrieve password from secret manager")?;

                Ok(app_password)
            }
        }
    }
}

fn apple_account_id(profile: &str, email: &str) -> String {
    format!("{profile}:{email}")
}

/// Custom debug implementation
/// to avoid leaking sensitive data.
impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(target_os = "macos")]
            Self::AppleKeyChain => write!(f, "AppleKeyChain"),
            Self::Inline(_) => write!(f, "Inline(_)"),
            Self::SecretManager(_) => write!(f, "SecretManager(_)"),
        }
    }
}

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            #[cfg(target_os = "macos")]
            Token::AppleKeyChain => serializer.serialize_str(APPLE_KEYCHAIN),
            Token::Inline(app_password) => serializer.serialize_str(app_password),
            Token::SecretManager(arn) => serializer.serialize_str(arn),
        }
    }
}

struct TokenVisitor;

impl<'de> Visitor<'de> for TokenVisitor {
    type Value = Token;

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        #[cfg(target_os = "macos")]
        if v == APPLE_KEYCHAIN {
            return Ok(Token::AppleKeyChain);
        }

        if v.starts_with(SECRET_MANAGER_PREFIX) {
            return Ok(Token::SecretManager(v.to_string()));
        }

        if v.starts_with(INLINE_PREFIX) {
            return Ok(Token::Inline(v.to_string()));
        }

        Err(E::custom("unknown app password token"))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("app password token")
    }
}

impl<'de> Deserialize<'de> for Token {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TokenVisitor)
    }
}
