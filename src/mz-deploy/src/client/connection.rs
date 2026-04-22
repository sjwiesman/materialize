//! Database client for mz-deploy.
//!
//! This module provides the main `Client` struct for interacting with Materialize.
//! The client handles connection management and delegates specialized operations
//! to domain-specific sub-clients.
//!
//! ## Sub-Client Architecture
//!
//! Operations are grouped into domain sub-clients accessed via accessor methods
//! on `Client`. Each sub-client borrows the `Client` and provides a focused API:
//!
//! | Sub-client | Accessor | Responsibility |
//! |------------|----------|---------------|
//! | `DeploymentsClient` | `.deployments()` | Deployment lifecycle (stage, promote, abort) |
//! | `DeploymentsClientMut` | `.deployments_mut()` | Mutable deployment ops (SUBSCRIBE cursors) |
//! | `IntrospectionClient` | `.introspection()` | Read-only catalog metadata queries |
//! | `ValidationClient` | `.validation()` | Pre-deployment environment checks |
//! | `TypeInfoClient` | `.types()` | Column/type introspection for type checking |
//! | `ProvisioningClient` | `.provisioning()` | Idempotent DDL for databases, schemas, clusters |
//!
//! ## TLS Policy
//!
//! - **Local** connections (localhost, 127.0.0.1, private IP ranges) → `NoTls`
//! - **Cloud** connections (all other hosts) → TLS with peer verification,
//!   using system CA certificates from platform-specific paths

use crate::client::errors::ConnectionError;
use crate::config::Profile;
use crate::info;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::types::ToSql;
use tokio_postgres::{
    Client as PgClient, NoTls, Row, SimpleQueryMessage, ToStatement, Transaction,
};

/// Database client for interacting with Materialize.
///
/// The `Client` struct provides methods for:
/// - Connecting to the database
/// - Schema and cluster management
/// - Deployment tracking
/// - Database introspection
/// - Project validation
pub struct Client {
    client: PgClient,
    profile: Profile,
}

/// Domain sub-client for deployment lifecycle operations.
pub struct DeploymentsClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for deployment operations that require mutable client access.
pub struct DeploymentsClientMut<'a> {
    pub(crate) client: &'a mut Client,
}

/// Domain sub-client for metadata and object introspection operations.
pub struct IntrospectionClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for project and privilege validation operations.
pub struct ValidationClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for column/type introspection used by type checking and tests.
pub struct TypeInfoClient<'a> {
    pub(crate) client: &'a Client,
}

/// Domain sub-client for provisioning databases, schemas, and clusters.
pub struct ProvisioningClient<'a> {
    pub(crate) client: &'a Client,
}

const APPLICATION_NAME: &str = "mz-deploy";

impl Client {
    /// Connect to the database using a Profile directly.
    ///
    /// Tries TLS connection first (required for Materialize Cloud), then falls back
    /// to NoTls for local connections (e.g., localhost, Docker).
    pub async fn connect_with_profile(profile: Profile) -> Result<Self, ConnectionError> {
        // Build connection string
        // Values with special characters need to be quoted with single quotes,
        // and single quotes/backslashes within values need to be escaped
        let mut conn_str = format!("host={} port={}", profile.host, profile.port);

        conn_str.push_str(&format!(
            " user='{}'",
            escape_conn_string_value(&profile.username)
        ));

        if let Some(ref password) = profile.password {
            conn_str.push_str(&format!(
                " password='{}'",
                escape_conn_string_value(password)
            ));
        }
        conn_str.push_str(&format!(
            " application_name='{}'",
            escape_conn_string_value(APPLICATION_NAME)
        ));

        // Determine if this is likely a cloud connection (not localhost)
        let is_local = profile.host == "localhost"
            || profile.host == "127.0.0.1"
            || profile.host.starts_with("192.168.")
            || profile.host.starts_with("10.")
            || profile.host.starts_with("172.");

        let client = if is_local {
            // Local connection - use NoTls
            let (client, connection) =
                tokio_postgres::connect(&conn_str, NoTls)
                    .await
                    .map_err(|source| ConnectionError::Connect {
                        host: profile.host.clone(),
                        port: profile.port,
                        source,
                    })?;

            // Spawn the connection handler
            mz_ore::task::spawn(|| "mz-deploy-connection", async move {
                if let Err(e) = connection.await {
                    info!("connection error: {}", e);
                }
            });

            client
        } else {
            // Cloud connection - use TLS
            let mut builder = SslConnector::builder(SslMethod::tls()).map_err(|e| {
                ConnectionError::Message(format!("Failed to create TLS builder: {}", e))
            })?;

            // Load CA certificates - try platform-specific paths
            // macOS: Homebrew OpenSSL or system certificates
            // Linux: Standard system paths
            let ca_paths = [
                "/etc/ssl/cert.pem",                    // macOS system
                "/opt/homebrew/etc/openssl@3/cert.pem", // macOS Homebrew ARM
                "/usr/local/etc/openssl@3/cert.pem",    // macOS Homebrew Intel
                "/opt/homebrew/etc/openssl/cert.pem",   // macOS Homebrew ARM (older)
                "/usr/local/etc/openssl/cert.pem",      // macOS Homebrew Intel (older)
                "/etc/ssl/certs/ca-certificates.crt",   // Debian/Ubuntu
                "/etc/pki/tls/certs/ca-bundle.crt",     // RHEL/CentOS
                "/etc/ssl/ca-bundle.pem",               // OpenSUSE
            ];

            let mut ca_loaded = false;
            for path in &ca_paths {
                if std::path::Path::new(path).exists() {
                    if builder.set_ca_file(path).is_ok() {
                        ca_loaded = true;
                        break;
                    }
                }
            }

            if !ca_loaded {
                let _ = builder.set_default_verify_paths();
            }

            builder.set_verify(SslVerifyMode::PEER);

            let connector = MakeTlsConnector::new(builder.build());

            let (client, connection) = tokio_postgres::connect(&conn_str, connector)
                .await
                .map_err(|source| ConnectionError::Connect {
                    host: profile.host.clone(),
                    port: profile.port,
                    source,
                })?;

            mz_ore::task::spawn(|| "mz-deploy-connection", async move {
                if let Err(e) = connection.await {
                    info!("connection error: {}", e);
                }
            });

            client
        };

        Ok(Client { client, profile })
    }

    /// Get the profile used for this connection.
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// Start a transaction on the underlying connection.
    pub(crate) async fn begin_transaction(&mut self) -> Result<Transaction<'_>, ConnectionError> {
        self.client
            .transaction()
            .await
            .map_err(ConnectionError::Query)
    }

    /// Access deployment lifecycle operations.
    pub fn deployments(&self) -> DeploymentsClient<'_> {
        DeploymentsClient { client: self }
    }

    /// Access mutable deployment lifecycle operations.
    pub fn deployments_mut(&mut self) -> DeploymentsClientMut<'_> {
        DeploymentsClientMut { client: self }
    }

    /// Access metadata and object introspection operations.
    pub fn introspection(&self) -> IntrospectionClient<'_> {
        IntrospectionClient { client: self }
    }

    /// Access database validation operations.
    pub fn validation(&self) -> ValidationClient<'_> {
        ValidationClient { client: self }
    }

    /// Access type/column introspection operations.
    pub fn types(&self) -> TypeInfoClient<'_> {
        TypeInfoClient { client: self }
    }

    /// Access provisioning operations for databases, schemas, and clusters.
    pub fn provisioning(&self) -> ProvisioningClient<'_> {
        ProvisioningClient { client: self }
    }

    /// Execute a SQL statement that doesn't return rows.
    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .execute(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query_one(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL statement using the simple query protocol (text-only, no binary encoding).
    pub async fn simple_query(
        &self,
        query: &str,
    ) -> Result<Vec<SimpleQueryMessage>, ConnectionError> {
        self.client
            .simple_query(query)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute one or more SQL statements that don't return rows, using the simple query protocol.
    pub async fn batch_execute(&self, query: &str) -> Result<(), ConnectionError> {
        self.client
            .batch_execute(query)
            .await
            .map_err(ConnectionError::Query)
    }
}

/// Escape a value for embedding inside the libpq `options` connection
/// parameter.
///
/// Within the `options` string, spaces separate `-c key=value` tokens unless
/// escaped, and backslash is the escape character. Only spaces and backslashes
/// are special; all other characters are literal.
fn escape_options_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str(r"\\"),
            ' ' => out.push_str(r"\ "),
            other => out.push(other),
        }
    }
    out
}

/// Build the inner value of the libpq `options` connection parameter from a
/// profile's options map.
///
/// Produces a space-separated string of `-c key=value` tokens in sorted-key
/// order, with each value inner-escaped per [`escape_options_value`].
/// Returns `None` when the map is empty so the caller can omit the fragment.
fn build_options_string(options: &std::collections::BTreeMap<String, String>) -> Option<String> {
    if options.is_empty() {
        return None;
    }
    let joined = options
        .iter()
        .map(|(k, v)| format!("-c {k}={}", escape_options_value(v)))
        .collect::<Vec<_>>()
        .join(" ");
    Some(joined)
}

/// Escape a value for use in a libpq connection string.
///
/// In connection strings, values containing special characters must be quoted
/// with single quotes, and any single quotes or backslashes within the value
/// must be escaped with a backslash.
fn escape_conn_string_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_escape_options_value_plain() {
        assert_eq!(escape_options_value("prod"), "prod");
    }

    #[test]
    fn test_escape_options_value_space() {
        assert_eq!(escape_options_value("prod cluster"), r"prod\ cluster");
    }

    #[test]
    fn test_escape_options_value_backslash() {
        assert_eq!(escape_options_value(r"a\b"), r"a\\b");
    }

    #[test]
    fn test_escape_options_value_mixed() {
        // Space then backslash
        assert_eq!(escape_options_value(r"a \b"), r"a\ \\b");
    }

    #[test]
    fn test_build_options_string_empty() {
        let options: BTreeMap<String, String> = BTreeMap::new();
        assert_eq!(build_options_string(&options), None);
    }

    #[test]
    fn test_build_options_string_single() {
        let mut options = BTreeMap::new();
        options.insert("cluster".to_string(), "prod".to_string());
        assert_eq!(
            build_options_string(&options),
            Some("-c cluster=prod".to_string())
        );
    }

    #[test]
    fn test_build_options_string_multiple_sorted() {
        let mut options = BTreeMap::new();
        // Insert in reverse order to verify BTreeMap iteration sorts keys.
        options.insert("search_path".to_string(), "public".to_string());
        options.insert("cluster".to_string(), "prod".to_string());
        assert_eq!(
            build_options_string(&options),
            Some("-c cluster=prod -c search_path=public".to_string())
        );
    }

    #[test]
    fn test_build_options_string_escapes_value_space() {
        let mut options = BTreeMap::new();
        options.insert("cluster".to_string(), "prod cluster".to_string());
        assert_eq!(
            build_options_string(&options),
            Some(r"-c cluster=prod\ cluster".to_string())
        );
    }

    #[test]
    fn test_build_options_string_composed_with_outer_escape() {
        // Full round-trip: the inner build plus the outer libpq-string escape
        // that `connect_with_profile` applies before wrapping in quotes.
        let mut options = BTreeMap::new();
        options.insert("cluster".to_string(), "prod cluster".to_string());
        let inner = build_options_string(&options).unwrap();
        let outer = escape_conn_string_value(&inner);
        // Inner space becomes `\ `; outer escape doubles each backslash.
        assert_eq!(outer, r"-c cluster=prod\\ cluster");
    }
}
