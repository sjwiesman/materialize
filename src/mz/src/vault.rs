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

use anyhow::Result;
use clap::Parser;
use serde::{de::Visitor, Deserialize, Serialize};

use crate::configuration::FronteggAPIToken;

const INLINE_PREFIX: &str = "mzp_";

/// A vault stores sensitive information and provides
/// back a token than can be used for latter retrieval.
/// If no specific vault is selected, values are inlined
/// directly in the token.
#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("vault").multiple(false))]
pub struct Vault {}

impl Vault {
    pub async fn store(
        &self,
        _profile: &str,
        _email: &str,
        api_token: FronteggAPIToken,
    ) -> Result<Token> {
        let password = api_token.to_string();

        Ok(Token::Inline(password))
    }
}

pub enum Token {
    Inline(String),
}

impl Token {
    pub async fn retrieve(&self, _profile: &str, _email: &str) -> Result<String> {
        match self {
            Token::Inline(app_password) => Ok(app_password.to_string()),
        }
    }
}

/// Custom debug implementation
/// to avoid leaking sensitive data.
impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inline(_) => write!(f, "Inline(_)"),
        }
    }
}

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Token::Inline(app_password) => serializer.serialize_str(app_password),
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
