use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Clone)]
#[command(name = "pgmg")]
#[command(about = "PostgreSQL Migration Manager")]
#[command(version = "0.1.0")]
pub struct Cli {
    /// Increase verbosity level (can be used multiple times)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: Option<u8>,
    
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Clone, Debug)]
pub enum Commands {
    /// Generate a sample configuration file
    Init,
    /// Analyze what changes need to be applied
    Plan {
        /// Directory containing sequential migration files
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        
        /// Directory containing declarative SQL objects (views, functions, types)
        #[arg(long)]
        code_dir: Option<PathBuf>,
        
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
        
        /// Output dependency graph in Graphviz DOT format to the specified file
        #[arg(long)]
        output_graph: Option<PathBuf>,
    },
    
    /// Apply pending changes
    Apply {
        /// Directory containing sequential migration files
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        
        /// Directory containing declarative SQL objects (views, functions, types)
        #[arg(long)]
        code_dir: Option<PathBuf>,
        
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
        
        /// Enable development mode (includes NOTIFY events)
        #[arg(long)]
        dev: bool,
    },
    
    /// Watch for file changes and automatically reload (always runs in development mode)
    Watch {
        /// Directory containing sequential migration files
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
        
        /// Directory containing declarative SQL objects (views, functions, types)
        #[arg(long)]
        code_dir: Option<PathBuf>,
        
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
        
        /// Debounce duration in milliseconds (default: 500ms)
        #[arg(long, default_value = "500")]
        debounce_ms: u64,
        
        /// Disable automatic apply after detecting changes
        #[arg(long)]
        no_auto_apply: bool,
    },
    
    /// Reset database (drop and recreate from scratch)
    Reset {
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
        
        /// Skip confirmation prompt (dangerous!)
        #[arg(long)]
        force: bool,
    },
    
    /// Run pgTAP tests
    Test {
        /// Path to test file or directory (searches for *.test.sql files)
        #[arg(value_name = "PATH")]
        path: Option<PathBuf>,
        
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
        
        /// Show raw TAP output instead of formatted results
        #[arg(long)]
        tap_output: bool,
        
        
        /// Run all tests in the project (searches all directories)
        #[arg(long)]
        all: bool,
    },
    
    /// Execute seed SQL files in alphanumeric order
    Seed {
        /// Directory containing seed SQL files
        #[arg(long)]
        seed_dir: Option<PathBuf>,
        
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
    },
    
    /// Create a new migration file
    New {
        /// Directory containing sequential migration files
        #[arg(long)]
        migrations_dir: Option<PathBuf>,
    },
    
    /// Run plpgsql_check on all user-defined functions
    Check {
        /// PostgreSQL connection string
        #[arg(long)]
        connection_string: Option<String>,
        
        /// Only check specific schema(s)
        #[arg(long)]
        schema: Option<Vec<String>>,
        
        /// Hide warnings and only show errors
        #[arg(long)]
        errors_only: bool,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_command_parsing() {
        let args = vec![
            "pgmg",
            "plan",
            "--migrations-dir", "/path/to/migrations",
            "--code-dir", "/path/to/sql",
            "--connection-string", "postgresql://user:pass@localhost/db"
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Plan { migrations_dir, code_dir, connection_string, output_graph } => {
                assert_eq!(migrations_dir, Some(PathBuf::from("/path/to/migrations")));
                assert_eq!(code_dir, Some(PathBuf::from("/path/to/sql")));
                assert_eq!(connection_string, Some("postgresql://user:pass@localhost/db".to_string()));
                assert_eq!(output_graph, None);
            }
            _ => panic!("Expected Plan command"),
        }
    }

    #[test]
    fn test_apply_command_parsing() {
        let args = vec![
            "pgmg",
            "apply",
            "--code-dir", "/path/to/sql",
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Apply { migrations_dir, code_dir, connection_string, dev } => {
                assert_eq!(migrations_dir, None);
                assert_eq!(code_dir, Some(PathBuf::from("/path/to/sql")));
                assert_eq!(connection_string, None);
                assert_eq!(dev, false);
            }
            _ => panic!("Expected Apply command"),
        }
    }

    #[test]
    fn test_watch_command_parsing() {
        let args = vec![
            "pgmg",
            "watch",
            "--migrations-dir", "/path/to/migrations",
            "--code-dir", "/path/to/sql",
            "--connection-string", "postgresql://localhost/db",
            "--debounce-ms", "1000",
            "--no-auto-apply"
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Watch { migrations_dir, code_dir, connection_string, debounce_ms, no_auto_apply } => {
                assert_eq!(migrations_dir, Some(PathBuf::from("/path/to/migrations")));
                assert_eq!(code_dir, Some(PathBuf::from("/path/to/sql")));
                assert_eq!(connection_string, Some("postgresql://localhost/db".to_string()));
                assert_eq!(debounce_ms, 1000);
                assert_eq!(no_auto_apply, true);
            }
            _ => panic!("Expected Watch command"),
        }
    }

    #[test]
    fn test_plan_command_with_output_graph() {
        let args = vec![
            "pgmg",
            "plan",
            "--code-dir", "/path/to/sql",
            "--output-graph", "/path/to/graph.dot"
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Plan { migrations_dir, code_dir, connection_string, output_graph } => {
                assert_eq!(migrations_dir, None);
                assert_eq!(code_dir, Some(PathBuf::from("/path/to/sql")));
                assert_eq!(connection_string, None);
                assert_eq!(output_graph, Some(PathBuf::from("/path/to/graph.dot")));
            }
            _ => panic!("Expected Plan command"),
        }
    }

    #[test]
    fn test_reset_command_parsing() {
        let args = vec![
            "pgmg",
            "reset",
            "--connection-string", "postgresql://localhost/test_db",
            "--force"
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Reset { connection_string, force } => {
                assert_eq!(connection_string, Some("postgresql://localhost/test_db".to_string()));
                assert_eq!(force, true);
            }
            _ => panic!("Expected Reset command"),
        }
    }

    #[test]
    fn test_test_command_parsing() {
        let args = vec![
            "pgmg",
            "test",
            "tests/",
            "--connection-string", "postgresql://localhost/test_db",
            "--tap-output"
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Test { path, connection_string, tap_output, all } => {
                assert_eq!(path, Some(PathBuf::from("tests/")));
                assert_eq!(connection_string, Some("postgresql://localhost/test_db".to_string()));
                assert_eq!(tap_output, true);
                assert_eq!(all, false);
            }
            _ => panic!("Expected Test command"),
        }
    }

    #[test]
    fn test_seed_command_parsing() {
        let args = vec![
            "pgmg",
            "seed",
            "--seed-dir", "/path/to/seeds",
            "--connection-string", "postgresql://localhost/test_db"
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Seed { seed_dir, connection_string } => {
                assert_eq!(seed_dir, Some(PathBuf::from("/path/to/seeds")));
                assert_eq!(connection_string, Some("postgresql://localhost/test_db".to_string()));
            }
            _ => panic!("Expected Seed command"),
        }
    }

    #[test]
    fn test_seed_command_minimal() {
        let args = vec![
            "pgmg",
            "seed",
        ];
        
        let cli = Cli::try_parse_from(args).unwrap();
        
        match cli.command {
            Commands::Seed { seed_dir, connection_string } => {
                assert_eq!(seed_dir, None);
                assert_eq!(connection_string, None);
            }
            _ => panic!("Expected Seed command"),
        }
    }
}