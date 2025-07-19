use tokio_postgres::Client;
use std::env;
use crate::db::tls::{TlsMode, TlsConfig, connect_with_tls, PgConnection};

#[derive(Clone)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub tls_config: TlsConfig,
}

impl DatabaseConfig {
    /// Parse connection URL and extract TLS configuration
    pub fn from_url(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Parse connection string like "postgres://user:pass@host:port/db?sslmode=require"
        let parsed_url = url::Url::parse(url)?;
        
        if parsed_url.scheme() != "postgres" && parsed_url.scheme() != "postgresql" {
            return Err("Invalid connection string scheme".into());
        }

        let host = parsed_url.host_str().unwrap_or("localhost").to_string();
        let port = parsed_url.port().unwrap_or(5432);
        let user = parsed_url.username().to_string();
        let password = parsed_url.password().unwrap_or("").to_string();
        let database = parsed_url.path().trim_start_matches('/').to_string();
        
        // Parse TLS configuration from query parameters
        let mut tls_config = TlsConfig::default();
        for (key, value) in parsed_url.query_pairs() {
            match key.as_ref() {
                "sslmode" => tls_config.mode = TlsMode::from_str(&value)?,
                "sslrootcert" => tls_config.root_cert = Some(value.to_string()),
                "sslcert" => tls_config.client_cert = Some(value.to_string()),
                "sslkey" => tls_config.client_key = Some(value.to_string()),
                _ => {} // Ignore other parameters
            }
        }

        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            tls_config,
        })
    }

    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let mut tls_config = TlsConfig::default();
        
        // Parse TLS configuration from environment variables
        if let Ok(sslmode) = env::var("PGSSLMODE") {
            tls_config.mode = TlsMode::from_str(&sslmode)?;
        }
        if let Ok(root_cert) = env::var("PGSSLROOTCERT") {
            tls_config.root_cert = Some(root_cert);
        }
        if let Ok(client_cert) = env::var("PGSSLCERT") {
            tls_config.client_cert = Some(client_cert);
        }
        if let Ok(client_key) = env::var("PGSSLKEY") {
            tls_config.client_key = Some(client_key);
        }
        
        Ok(Self {
            host: env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("PGPORT")
                .unwrap_or_else(|_| "5432".to_string())
                .parse()?,
            user: env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string()),
            password: env::var("PGPASSWORD").unwrap_or_default(),
            database: env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string()),
            tls_config,
        })
    }

    pub fn to_connection_string(&self) -> String {
        let base = if self.password.is_empty() {
            format!(
                "host={} port={} user={} dbname={}",
                self.host, self.port, self.user, self.database
            )
        } else {
            format!(
                "host={} port={} user={} password={} dbname={}",
                self.host, self.port, self.user, self.password, self.database
            )
        };
        
        // Note: tokio-postgres handles sslmode differently - it's not part of the connection string
        // TLS is configured through the connector parameter instead
        base
    }
    
    /// Merge TLS configuration from config file with this config
    /// Connection string parameters take precedence
    pub fn merge_tls_config(mut self, file_tls_config: TlsConfig) -> Self {
        // Only use file config values if not already set from connection string
        if self.tls_config.mode == TlsMode::Disable && file_tls_config.mode != TlsMode::Disable {
            self.tls_config.mode = file_tls_config.mode;
        }
        
        if self.tls_config.root_cert.is_none() {
            self.tls_config.root_cert = file_tls_config.root_cert;
        }
        
        if self.tls_config.client_cert.is_none() {
            self.tls_config.client_cert = file_tls_config.client_cert;
        }
        
        if self.tls_config.client_key.is_none() {
            self.tls_config.client_key = file_tls_config.client_key;
        }
        
        self
    }
}

pub async fn connect_to_database(
    config: &DatabaseConfig,
) -> Result<(Client, PgConnection), Box<dyn std::error::Error>> {
    let connection_string = config.to_connection_string();
    connect_with_tls(&connection_string, &config.tls_config).await
}

pub async fn connect_with_url(
    url: &str,
) -> Result<(Client, PgConnection), Box<dyn std::error::Error>> {
    let config = DatabaseConfig::from_url(url)?;
    connect_to_database(&config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_url() {
        let config = DatabaseConfig::from_url("postgres://user:pass@host:1234/mydb").unwrap();
        assert_eq!(config.host, "host");
        assert_eq!(config.port, 1234);
        assert_eq!(config.user, "user");
        assert_eq!(config.password, "pass");
        assert_eq!(config.database, "mydb");
        assert_eq!(config.tls_config.mode, TlsMode::Disable);
    }
    
    #[test]
    fn test_config_from_url_with_sslmode() {
        #[cfg(feature = "tls")]
        {
            let config = DatabaseConfig::from_url("postgres://user:pass@host:1234/mydb?sslmode=require").unwrap();
            assert_eq!(config.host, "host");
            assert_eq!(config.port, 1234);
            assert_eq!(config.user, "user");
            assert_eq!(config.password, "pass");
            assert_eq!(config.database, "mydb");
            assert_eq!(config.tls_config.mode, TlsMode::Require);
        }
        #[cfg(not(feature = "tls"))]
        {
            assert!(DatabaseConfig::from_url("postgres://user:pass@host:1234/mydb?sslmode=require").is_err());
        }
    }

    #[test]
    fn test_config_to_connection_string() {
        let config = DatabaseConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "secret".to_string(),
            database: "testdb".to_string(),
            tls_config: TlsConfig::default(),
        };

        let conn_str = config.to_connection_string();
        assert!(conn_str.contains("host=localhost"));
        assert!(conn_str.contains("port=5432"));
        assert!(conn_str.contains("user=postgres"));
        assert!(conn_str.contains("password=secret"));
        assert!(conn_str.contains("dbname=testdb"));
    }

    #[test]
    fn test_config_no_password() {
        let config = DatabaseConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "".to_string(),
            database: "testdb".to_string(),
            tls_config: TlsConfig::default(),
        };

        let conn_str = config.to_connection_string();
        assert!(!conn_str.contains("password"));
    }
    
    #[test]
    fn test_config_from_url_with_tls_params() {
        #[cfg(feature = "tls")]
        {
            let config = DatabaseConfig::from_url(
                "postgres://user:pass@host:1234/mydb?sslmode=verify-full&sslrootcert=/path/to/ca.crt&sslcert=/path/to/cert.crt&sslkey=/path/to/key.key"
            ).unwrap();
            
            assert_eq!(config.host, "host");
            assert_eq!(config.port, 1234);
            assert_eq!(config.user, "user");
            assert_eq!(config.password, "pass");
            assert_eq!(config.database, "mydb");
            assert_eq!(config.tls_config.mode, TlsMode::VerifyFull);
            assert_eq!(config.tls_config.root_cert, Some("/path/to/ca.crt".to_string()));
            assert_eq!(config.tls_config.client_cert, Some("/path/to/cert.crt".to_string()));
            assert_eq!(config.tls_config.client_key, Some("/path/to/key.key".to_string()));
        }
    }
    
    #[test]
    fn test_merge_tls_config() {
        let mut config = DatabaseConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "".to_string(),
            database: "testdb".to_string(),
            tls_config: TlsConfig::default(),
        };
        
        // Config from file
        let file_tls = TlsConfig {
            mode: TlsMode::from_str("require").unwrap(),
            root_cert: Some("/etc/ssl/ca.crt".to_string()),
            client_cert: Some("/etc/ssl/client.crt".to_string()),
            client_key: Some("/etc/ssl/client.key".to_string()),
        };
        
        // Merge - should use file config since connection has defaults
        config = config.merge_tls_config(file_tls.clone());
        #[cfg(feature = "tls")]
        assert_eq!(config.tls_config.mode, TlsMode::Require);
        assert_eq!(config.tls_config.root_cert, Some("/etc/ssl/ca.crt".to_string()));
        
        // Now set mode in connection config - it should take precedence
        #[cfg(feature = "tls")]
        {
            config.tls_config.mode = TlsMode::VerifyFull;
            config.tls_config.root_cert = Some("/override/ca.crt".to_string());
            
            let merged = config.merge_tls_config(file_tls);
            assert_eq!(merged.tls_config.mode, TlsMode::VerifyFull);
            assert_eq!(merged.tls_config.root_cert, Some("/override/ca.crt".to_string()));
            // Client cert/key from file should still be used
            assert_eq!(merged.tls_config.client_cert, Some("/etc/ssl/client.crt".to_string()));
        }
    }
}