// pgmg - PostgreSQL Migration Manager
// Public API for the library

pub mod builtin_catalog;
pub mod sql;
pub mod analysis;
pub mod cli;
pub mod db;
pub mod commands;
pub mod config;
pub mod error;
pub mod logging;
pub mod notify;

// Re-export key public APIs for convenience
pub use builtin_catalog::BuiltinCatalog;
pub use sql::{analyze_statement, analyze_plpgsql, filter_builtins, Dependencies, QualifiedIdent, SqlObject, ObjectType};
pub use analysis::{DependencyGraph, ObjectRef, DependencyType};
pub use db::{StateManager, DatabaseConfig, connect_to_database, connect_with_url, scan_sql_files, scan_migrations};
pub use config::PgmgConfig;
pub use error::{PgmgError, Result, ErrorContext};