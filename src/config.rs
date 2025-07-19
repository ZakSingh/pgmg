use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;
use crate::db::tls::{TlsMode, TlsConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgmgConfig {
    /// Database connection string
    pub connection_string: Option<String>,
    
    /// Directory containing migration files
    pub migrations_dir: Option<PathBuf>,
    
    /// Directory containing SQL code files
    pub code_dir: Option<PathBuf>,
    
    /// Directory containing seed SQL files
    pub seed_dir: Option<PathBuf>,
    
    /// Path to output dependency graph (for plan command)
    pub output_graph: Option<PathBuf>,
    
    /// Enable development mode features
    pub development_mode: Option<bool>,
    
    /// Emit NOTIFY events when objects are loaded (requires development_mode)
    pub emit_notify_events: Option<bool>,
    
    /// Run plpgsql_check on modified functions (requires development_mode)
    pub check_plpgsql: Option<bool>,
    
    /// TLS/SSL configuration
    pub tls: Option<TlsConfigSection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfigSection {
    /// SSL mode (disable, prefer, require, verify-ca, verify-full)
    pub sslmode: Option<String>,
    
    /// Path to root certificate file for server verification
    pub sslrootcert: Option<PathBuf>,
    
    /// Path to client certificate file
    pub sslcert: Option<PathBuf>,
    
    /// Path to client key file
    pub sslkey: Option<PathBuf>,
}

impl PgmgConfig {
    /// Load configuration from pgmg.toml file in current directory
    pub fn load_from_file() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let config_path = PathBuf::from("pgmg.toml");
        
        if !config_path.exists() {
            return Ok(None);
        }
        
        let content = fs::read_to_string(&config_path)?;
        let config: PgmgConfig = toml::from_str(&content)?;
        
        Ok(Some(config))
    }
    
    /// Merge CLI arguments with config file values
    /// CLI arguments take precedence over config file values
    pub fn merge_with_cli(
        config_file: Option<Self>,
        cli_migrations_dir: Option<PathBuf>,
        cli_code_dir: Option<PathBuf>,
        cli_connection_string: Option<String>,
        cli_output_graph: Option<PathBuf>,
    ) -> Self {
        let base_config = config_file.unwrap_or_default();
        
        Self {
            connection_string: cli_connection_string.or(base_config.connection_string),
            migrations_dir: cli_migrations_dir.or(base_config.migrations_dir),
            code_dir: cli_code_dir.or(base_config.code_dir),
            seed_dir: base_config.seed_dir,
            output_graph: cli_output_graph.or(base_config.output_graph),
            development_mode: base_config.development_mode,
            emit_notify_events: base_config.emit_notify_events,
            check_plpgsql: base_config.check_plpgsql,
            tls: base_config.tls,
        }
    }
    
    /// Merge CLI arguments with config file values for seed command
    /// CLI arguments take precedence over config file values
    pub fn merge_with_cli_seed(
        config_file: Option<Self>,
        cli_seed_dir: Option<PathBuf>,
        cli_connection_string: Option<String>,
    ) -> Self {
        let base_config = config_file.unwrap_or_default();
        
        Self {
            connection_string: cli_connection_string.or(base_config.connection_string),
            migrations_dir: base_config.migrations_dir,
            code_dir: base_config.code_dir,
            seed_dir: cli_seed_dir.or(base_config.seed_dir),
            output_graph: base_config.output_graph,
            development_mode: base_config.development_mode,
            emit_notify_events: base_config.emit_notify_events,
            check_plpgsql: base_config.check_plpgsql,
            tls: base_config.tls,
        }
    }
    
    /// Apply development mode settings from CLI
    pub fn with_dev_mode(mut self, dev_mode: bool) -> Self {
        if dev_mode {
            self.development_mode = Some(true);
            // Enable notify events by default in dev mode unless explicitly disabled
            if self.emit_notify_events.is_none() {
                self.emit_notify_events = Some(true);
            }
            // Enable plpgsql_check by default in dev mode unless explicitly disabled
            if self.check_plpgsql.is_none() {
                self.check_plpgsql = Some(true);
            }
        }
        self
    }
    
    /// Create a sample configuration file
    pub fn write_sample_config() -> Result<(), Box<dyn std::error::Error>> {
        let sample_config = PgmgConfig {
            connection_string: Some("postgres://user:password@localhost:5432/database".to_string()),
            migrations_dir: Some(PathBuf::from("migrations")),
            code_dir: Some(PathBuf::from("sql")),
            seed_dir: Some(PathBuf::from("seeds")),
            output_graph: None,
            development_mode: Some(false),
            emit_notify_events: Some(false),
            check_plpgsql: Some(false),
            tls: None,
        };
        
        let content = toml::to_string_pretty(&sample_config)?;
        fs::write("pgmg.toml.example", content)?;
        
        Ok(())
    }
    
    /// Build TLS configuration from the config
    pub fn build_tls_config(&self) -> Result<TlsConfig, Box<dyn std::error::Error>> {
        let mut tls_config = TlsConfig::default();
        
        if let Some(tls_section) = &self.tls {
            if let Some(sslmode) = &tls_section.sslmode {
                tls_config.mode = TlsMode::from_str(sslmode)?;
            }
            
            if let Some(root_cert) = &tls_section.sslrootcert {
                tls_config.root_cert = Some(root_cert.to_string_lossy().to_string());
            }
            
            if let Some(client_cert) = &tls_section.sslcert {
                tls_config.client_cert = Some(client_cert.to_string_lossy().to_string());
            }
            
            if let Some(client_key) = &tls_section.sslkey {
                tls_config.client_key = Some(client_key.to_string_lossy().to_string());
            }
        }
        
        Ok(tls_config)
    }
}

impl Default for PgmgConfig {
    fn default() -> Self {
        Self {
            connection_string: None,
            migrations_dir: None,
            code_dir: None,
            seed_dir: None,
            output_graph: None,
            development_mode: None,
            emit_notify_events: None,
            check_plpgsql: None,
            tls: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::env;

    #[test]
    fn test_config_serialization() {
        let config = PgmgConfig {
            connection_string: Some("postgres://localhost/test".to_string()),
            migrations_dir: Some(PathBuf::from("migrations")),
            code_dir: Some(PathBuf::from("sql")),
            seed_dir: Some(PathBuf::from("seeds")),
            output_graph: Some(PathBuf::from("graph.dot")),
            development_mode: Some(true),
            emit_notify_events: Some(false),
            check_plpgsql: Some(true),
            tls: None,
        };
        
        let toml_str = toml::to_string(&config).unwrap();
        let parsed: PgmgConfig = toml::from_str(&toml_str).unwrap();
        
        assert_eq!(config.connection_string, parsed.connection_string);
        assert_eq!(config.migrations_dir, parsed.migrations_dir);
        assert_eq!(config.code_dir, parsed.code_dir);
        assert_eq!(config.seed_dir, parsed.seed_dir);
        assert_eq!(config.output_graph, parsed.output_graph);
        assert_eq!(config.development_mode, parsed.development_mode);
        assert_eq!(config.emit_notify_events, parsed.emit_notify_events);
        assert_eq!(config.check_plpgsql, parsed.check_plpgsql);
    }
    
    #[test]
    fn test_config_merge_cli_precedence() {
        let config_file = PgmgConfig {
            connection_string: Some("postgres://config/db".to_string()),
            migrations_dir: Some(PathBuf::from("config_migrations")),
            code_dir: Some(PathBuf::from("config_sql")),
            seed_dir: Some(PathBuf::from("config_seeds")),
            output_graph: Some(PathBuf::from("config_graph.dot")),
            development_mode: Some(false),
            emit_notify_events: Some(true),
            check_plpgsql: Some(false),
            tls: None,
        };
        
        let merged = PgmgConfig::merge_with_cli(
            Some(config_file),
            Some(PathBuf::from("cli_migrations")), // CLI override
            None, // Use config value
            Some("postgres://cli/db".to_string()), // CLI override
            None, // Use config value
        );
        
        assert_eq!(merged.connection_string, Some("postgres://cli/db".to_string()));
        assert_eq!(merged.migrations_dir, Some(PathBuf::from("cli_migrations")));
        assert_eq!(merged.code_dir, Some(PathBuf::from("config_sql")));
        assert_eq!(merged.seed_dir, Some(PathBuf::from("config_seeds")));
        assert_eq!(merged.output_graph, Some(PathBuf::from("config_graph.dot")));
    }
    
    #[test]
    fn test_config_load_nonexistent_file() {
        let temp_dir = tempdir().unwrap();
        env::set_current_dir(temp_dir.path()).unwrap();
        
        let result = PgmgConfig::load_from_file().unwrap();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_config_load_from_file() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("pgmg.toml");
        
        let config_content = r#"
connection_string = "postgres://test/db"
migrations_dir = "test_migrations"
code_dir = "test_sql"
"#;
        fs::write(&config_path, config_content).unwrap();
        
        // Change to temp directory
        let original_dir = env::current_dir().unwrap();
        env::set_current_dir(temp_dir.path()).unwrap();
        
        let loaded_config = PgmgConfig::load_from_file().unwrap().unwrap();
        
        assert_eq!(loaded_config.connection_string, Some("postgres://test/db".to_string()));
        assert_eq!(loaded_config.migrations_dir, Some(PathBuf::from("test_migrations")));
        assert_eq!(loaded_config.code_dir, Some(PathBuf::from("test_sql")));
        assert_eq!(loaded_config.seed_dir, None);
        assert_eq!(loaded_config.output_graph, None);
        
        // Restore original directory
        let _ = env::set_current_dir(original_dir);
    }
    
    #[test]
    fn test_write_sample_config() {
        let temp_dir = tempdir().unwrap();
        let original_dir = env::current_dir().unwrap();
        env::set_current_dir(temp_dir.path()).unwrap();
        
        PgmgConfig::write_sample_config().unwrap();
        
        // The file is written in the current directory, which is temp_dir
        let sample_path = PathBuf::from("pgmg.toml.example");
        assert!(sample_path.exists());
        
        let content = fs::read_to_string(&sample_path).unwrap();
        assert!(content.contains("connection_string"));
        assert!(content.contains("migrations_dir"));
        assert!(content.contains("code_dir"));
        
        // Restore original directory
        let _ = env::set_current_dir(original_dir);
    }
    
    #[test]
    fn test_with_dev_mode() {
        // Test enabling dev mode
        let config = PgmgConfig::default().with_dev_mode(true);
        assert_eq!(config.development_mode, Some(true));
        assert_eq!(config.emit_notify_events, Some(true));
        assert_eq!(config.check_plpgsql, Some(true));
        
        // Test that existing emit_notify_events setting is preserved
        let mut config_with_notify_false = PgmgConfig::default();
        config_with_notify_false.emit_notify_events = Some(false);
        let config_with_notify_false = config_with_notify_false.with_dev_mode(true);
        assert_eq!(config_with_notify_false.development_mode, Some(true));
        assert_eq!(config_with_notify_false.emit_notify_events, Some(false));
        
        // Test not enabling dev mode
        let config_no_dev = PgmgConfig::default().with_dev_mode(false);
        assert_eq!(config_no_dev.development_mode, None);
        assert_eq!(config_no_dev.emit_notify_events, None);
    }
}