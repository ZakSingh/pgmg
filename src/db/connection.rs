use tokio_postgres::{Client, Connection, Socket, NoTls, tls::NoTlsStream};
use std::env;

pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl DatabaseConfig {
    pub fn from_url(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Parse connection string like "postgres://user:pass@host:port/db"
        let url = url::Url::parse(url)?;
        
        if url.scheme() != "postgres" && url.scheme() != "postgresql" {
            return Err("Invalid connection string scheme".into());
        }

        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(5432);
        let user = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();
        let database = url.path().trim_start_matches('/').to_string();

        Ok(Self {
            host,
            port,
            user,
            password,
            database,
        })
    }

    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            host: env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("PGPORT")
                .unwrap_or_else(|_| "5432".to_string())
                .parse()?,
            user: env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string()),
            password: env::var("PGPASSWORD").unwrap_or_default(),
            database: env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string()),
        })
    }

    pub fn to_connection_string(&self) -> String {
        if self.password.is_empty() {
            format!(
                "host={} port={} user={} dbname={}",
                self.host, self.port, self.user, self.database
            )
        } else {
            format!(
                "host={} port={} user={} password={} dbname={}",
                self.host, self.port, self.user, self.password, self.database
            )
        }
    }
}

pub async fn connect_to_database(
    config: &DatabaseConfig,
) -> Result<(Client, Connection<Socket, NoTlsStream>), Box<dyn std::error::Error>> {
    let connection_string = config.to_connection_string();
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;
    Ok((client, connection))
}

pub async fn connect_with_url(
    url: &str,
) -> Result<(Client, Connection<Socket, NoTlsStream>), Box<dyn std::error::Error>> {
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
    }

    #[test]
    fn test_config_to_connection_string() {
        let config = DatabaseConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: "secret".to_string(),
            database: "testdb".to_string(),
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
        };

        let conn_str = config.to_connection_string();
        assert!(!conn_str.contains("password"));
    }
}