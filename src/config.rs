use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgmgConfig {
    /// Database connection string
    pub connection_string: Option<String>,
    
    /// Directory containing migration files
    pub migrations_dir: Option<PathBuf>,
    
    /// Directory containing SQL code files
    pub code_dir: Option<PathBuf>,
    
    /// Path to output dependency graph (for plan command)
    pub output_graph: Option<PathBuf>,
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
            output_graph: cli_output_graph.or(base_config.output_graph),
        }
    }
    
    /// Create a sample configuration file
    pub fn write_sample_config() -> Result<(), Box<dyn std::error::Error>> {
        let sample_config = PgmgConfig {
            connection_string: Some("postgres://user:password@localhost:5432/database".to_string()),
            migrations_dir: Some(PathBuf::from("migrations")),
            code_dir: Some(PathBuf::from("sql")),
            output_graph: None,
        };
        
        let content = toml::to_string_pretty(&sample_config)?;
        fs::write("pgmg.toml.example", content)?;
        
        Ok(())
    }
}

impl Default for PgmgConfig {
    fn default() -> Self {
        Self {
            connection_string: None,
            migrations_dir: None,
            code_dir: None,
            output_graph: None,
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
            output_graph: Some(PathBuf::from("graph.dot")),
        };
        
        let toml_str = toml::to_string(&config).unwrap();
        let parsed: PgmgConfig = toml::from_str(&toml_str).unwrap();
        
        assert_eq!(config.connection_string, parsed.connection_string);
        assert_eq!(config.migrations_dir, parsed.migrations_dir);
        assert_eq!(config.code_dir, parsed.code_dir);
        assert_eq!(config.output_graph, parsed.output_graph);
    }
    
    #[test]
    fn test_config_merge_cli_precedence() {
        let config_file = PgmgConfig {
            connection_string: Some("postgres://config/db".to_string()),
            migrations_dir: Some(PathBuf::from("config_migrations")),
            code_dir: Some(PathBuf::from("config_sql")),
            output_graph: Some(PathBuf::from("config_graph.dot")),
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
}