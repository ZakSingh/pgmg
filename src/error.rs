use std::path::PathBuf;
use thiserror::Error;
use std::env;

/// Main error type for pgmg
#[derive(Error, Debug)]
pub enum PgmgError {
    // SQL Parsing Errors
    #[error("Failed to parse SQL in {file}: {message}")]
    SqlParse {
        file: PathBuf,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Invalid SQL statement at line {line} in {file}: {message}")]
    InvalidSql {
        file: PathBuf,
        line: usize,
        message: String,
    },

    // Database Connection Errors
    #[error("Failed to connect to database: {message}")]
    DatabaseConnection {
        message: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("Database error: {message}")]
    Database {
        message: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("Invalid connection string: {0}")]
    InvalidConnectionString(String),

    // File System Errors
    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Permission denied: {0}")]
    PermissionDenied(PathBuf),

    #[error("Failed to read {path}: {message}")]
    FileRead {
        path: PathBuf,
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to write {path}: {message}")]
    FileWrite {
        path: PathBuf,
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Directory not found: {0}")]
    DirectoryNotFound(PathBuf),

    // Dependency Resolution Errors
    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Missing dependency: {object} depends on {dependency} which doesn't exist")]
    MissingDependency {
        object: String,
        dependency: String,
    },

    #[error("Failed to build dependency graph: {0}")]
    DependencyGraph(String),

    // State Tracking Errors
    #[error("Failed to initialize state tables: {0}")]
    StateInitialization(String),

    #[error("Failed to track object {object}: {message}")]
    StateTracking {
        object: String,
        message: String,
    },

    // Migration Errors
    #[error("Migration {name} failed at statement {statement}: {message}")]
    MigrationFailed {
        name: String,
        statement: usize,
        message: String,
    },

    #[error("Migration {0} already applied")]
    MigrationAlreadyApplied(String),

    #[error("Invalid migration file name: {0}")]
    InvalidMigrationName(String),

    // Configuration Errors
    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Failed to load configuration from {path}: {message}")]
    ConfigLoad {
        path: PathBuf,
        message: String,
    },

    // Watch Mode Errors (for future use)
    #[error("Failed to watch {path}: {message}")]
    WatchError {
        path: PathBuf,
        message: String,
    },

    // Resource Management Errors
    #[error("Mutex poisoned: {0}")]
    MutexPoisoned(String),
    
    #[error("Connection management failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Lock acquisition failed: {0}")]
    LockFailed(String),
    
    #[error("Resource cleanup failed: {0}")]
    CleanupFailed(String),
    
    // Directory Management Errors
    #[error("Failed to change directory to {path}: {message}")]
    DirectoryChange {
        path: PathBuf,
        message: String,
    },
    
    #[error("Failed to restore directory: {0}")]
    DirectoryRestore(String),
    
    // State Consistency Errors
    #[error("State inconsistency detected: {0}")]
    StateInconsistent(String),
    
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),
    
    // General Errors
    #[error("Operation cancelled by user")]
    Cancelled,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("{0}")]
    Other(String),
}

// Implement conversion from common error types
impl From<std::io::Error> for PgmgError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => {
                PgmgError::FileNotFound(PathBuf::from("unknown"))
            }
            std::io::ErrorKind::PermissionDenied => {
                PgmgError::PermissionDenied(PathBuf::from("unknown"))
            }
            _ => PgmgError::Other(err.to_string()),
        }
    }
}

impl From<tokio_postgres::Error> for PgmgError {
    fn from(err: tokio_postgres::Error) -> Self {
        // Check if it's a connection error by examining the error message
        if err.to_string().contains("connect") {
            PgmgError::DatabaseConnection {
                message: err.to_string(),
                source: err,
            }
        } else {
            PgmgError::Database {
                message: err.to_string(),
                source: err,
            }
        }
    }
}

impl From<pg_query::Error> for PgmgError {
    fn from(err: pg_query::Error) -> Self {
        PgmgError::SqlParse {
            file: PathBuf::from("unknown"),
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<Box<dyn std::error::Error>> for PgmgError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        PgmgError::Other(err.to_string())
    }
}

/// Result type alias for pgmg operations
pub type Result<T> = std::result::Result<T, PgmgError>;

/// Helper trait for adding context to errors
pub trait ErrorContext<T> {
    /// Add context about which file caused the error
    fn file_context(self, path: impl Into<PathBuf>) -> Result<T>;
    
    /// Add context about which SQL object caused the error
    fn object_context(self, object_type: &str, object_name: &str) -> Result<T>;
    
    /// Add context about which migration caused the error
    fn migration_context(self, migration_name: &str) -> Result<T>;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: Into<PgmgError>,
{
    fn file_context(self, path: impl Into<PathBuf>) -> Result<T> {
        self.map_err(|e| {
            let mut err = e.into();
            // Update file path if it's a file-related error
            match &mut err {
                PgmgError::SqlParse { file, .. } => *file = path.into(),
                PgmgError::InvalidSql { file, .. } => *file = path.into(),
                PgmgError::FileRead { path: p, .. } => *p = path.into(),
                PgmgError::FileWrite { path: p, .. } => *p = path.into(),
                _ => {}
            }
            err
        })
    }
    
    fn object_context(self, object_type: &str, object_name: &str) -> Result<T> {
        self.map_err(|e| {
            let err = e.into();
            match err {
                PgmgError::Other(msg) => PgmgError::Other(
                    format!("Error processing {} '{}': {}", object_type, object_name, msg)
                ),
                _ => err,
            }
        })
    }
    
    fn migration_context(self, migration_name: &str) -> Result<T> {
        self.map_err(|e| {
            let err = e.into();
            match err {
                PgmgError::Other(msg) => PgmgError::Other(
                    format!("Error in migration '{}': {}", migration_name, msg)
                ),
                _ => err,
            }
        })
    }
}

/// Helper function to format error with all its causes
pub fn format_error_chain(err: &PgmgError) -> String {
    use std::error::Error;
    
    let mut output = format!("Error: {}", err);
    
    let mut current_err: &dyn Error = err;
    while let Some(source) = current_err.source() {
        output.push_str(&format!("\n  Caused by: {}", source));
        current_err = source;
    }
    
    output
}

/// Helper function to suggest fixes for common errors
pub fn suggest_fix(err: &PgmgError) -> Option<String> {
    match err {
        PgmgError::DatabaseConnection { .. } => Some(
            "Suggestions:\n\
             - Check if PostgreSQL is running\n\
             - Verify the connection string is correct\n\
             - Ensure the database exists and you have permission to access it\n\
             - Try: psql <your-connection-string> to test the connection".to_string()
        ),
        PgmgError::InvalidConnectionString(_) => Some(
            "Connection string should be in format:\n\
             postgres://[user[:password]@][host][:port][/dbname][?param1=value1&...]".to_string()
        ),
        PgmgError::FileNotFound(path) => Some(
            format!("File not found: {}\n\
                    - Check if the path is correct\n\
                    - Ensure you're running pgmg from the right directory", path.display())
        ),
        PgmgError::PermissionDenied(path) => Some(
            format!("Permission denied for: {}\n\
                    - Check file permissions\n\
                    - You may need to run with appropriate privileges", path.display())
        ),
        PgmgError::CircularDependency(details) => Some(
            format!("Circular dependency detected: {}\n\
                    - Review your SQL object dependencies\n\
                    - Consider breaking the circular reference", details)
        ),
        PgmgError::MissingDependency { object, dependency } => Some(
            format!("Object '{}' depends on '{}' which doesn't exist.\n\
                    - Ensure '{}' is defined in your SQL files\n\
                    - Check for typos in object names", object, dependency, dependency)
        ),
        PgmgError::SqlParse { file, message, .. } => Some(
            format!("SQL parsing error in {}:\n{}\n\
                    - Check SQL syntax\n\
                    - Ensure the file contains valid PostgreSQL statements", file.display(), message)
        ),
        _ => None,
    }
}

// PostgreSQL error detail extraction
use tokio_postgres::error::ErrorPosition;

#[derive(Debug)]
pub struct PostgresErrorDetails {
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
    pub code: String,
    pub severity: String,
}

/// Extract detailed error information from a PostgreSQL error
pub fn extract_postgres_error_details(err: &tokio_postgres::Error) -> Option<PostgresErrorDetails> {
    if let Some(db_err) = err.as_db_error() {
        Some(PostgresErrorDetails {
            message: db_err.message().to_string(),
            detail: db_err.detail().map(|s| s.to_string()),
            hint: db_err.hint().map(|s| s.to_string()),
            position: db_err.position().and_then(|pos| {
                match pos {
                    ErrorPosition::Original(pos) => Some(*pos as usize),
                    ErrorPosition::Internal { position, .. } => Some(*position as usize),
                }
            }),
            code: db_err.code().code().to_string(),
            severity: db_err.severity().to_string(),
        })
    } else {
        None
    }
}

/// Calculate line and column number from a byte position in text
pub fn calculate_line_column(text: &str, byte_position: usize) -> (usize, usize) {
    let mut line = 1;
    let mut line_start_byte = 0;
    
    // Find the line containing the byte position
    for (i, ch) in text.char_indices() {
        if i >= byte_position {
            break;
        }
        
        if ch == '\n' {
            line += 1;
            line_start_byte = i + ch.len_utf8();
        }
    }
    
    // Calculate column by counting characters from start of line to error position
    let mut column = 1;
    let line_text = &text[line_start_byte..];
    
    for (i, _ch) in line_text.char_indices() {
        if line_start_byte + i >= byte_position {
            break;
        }
        column += 1;
    }
    
    (line, column)
}

/// Format a PostgreSQL error with enhanced details including line numbers
pub fn format_postgres_error_with_details(
    object_name: &str,
    source_file: Option<&std::path::Path>,
    start_line: Option<usize>,
    sql: &str,
    err: &tokio_postgres::Error,
) -> String {
    use owo_colors::OwoColorize;
    
    let mut output = format!("Failed to execute SQL for {}", object_name.red());
    
    if let Some(details) = extract_postgres_error_details(err) {
        // Add file location if available
        if let Some(file) = source_file {
            output.push_str(&format!("\n  {}: {}", "File".dimmed(), file.display()));
        }
        
        // Add SQL error position
        if let Some(pos) = details.position {
            let (line, col) = calculate_line_column(sql, pos - 1); // PostgreSQL positions are 1-based
            
            if let (Some(file_line), Some(_)) = (start_line, source_file) {
                let absolute_line = file_line + line - 1;
                output.push_str(&format!("\n  {} line {}, column {}", 
                    "Error at".yellow(), 
                    absolute_line.to_string().yellow().bold(),
                    col.to_string().yellow().bold()
                ));
            } else {
                output.push_str(&format!("\n  {} line {}, column {}", 
                    "Error at SQL".yellow(),
                    line.to_string().yellow().bold(),
                    col.to_string().yellow().bold()
                ));
            }
            
            // Show the problematic line with error marker
            if let Some(error_line) = sql.lines().nth(line - 1) {
                output.push_str(&format!("\n  {}", error_line.dimmed()));
                output.push_str(&format!("\n  {}{}", " ".repeat(col - 1), "^".red().bold()));
            }
        }
        
        output.push_str(&format!("\n  {}: {}", "Error".red().bold(), details.message));
        
        if let Some(detail) = details.detail {
            output.push_str(&format!("\n  {}: {}", "Detail".yellow(), detail));
        }
        
        if let Some(hint) = details.hint {
            output.push_str(&format!("\n  {}: {}", "Hint".green(), hint));
        }
        
        output.push_str(&format!("\n  {}: {} ({})", "Code".dimmed(), details.code, details.severity));
    } else {
        // Fallback to simple error message
        output.push_str(&format!(": {}", err));
    }
    
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_line_column() {
        let sql = "SELECT * FROM users\nWHERE id = 1\nAND name = 'test'";
        
        // Test position at start
        assert_eq!(calculate_line_column(sql, 0), (1, 1));
        
        // Test position on first line
        assert_eq!(calculate_line_column(sql, 7), (1, 8)); // at '*'
        
        // Test position on second line
        assert_eq!(calculate_line_column(sql, 20), (2, 1)); // at 'W' in WHERE
        assert_eq!(calculate_line_column(sql, 25), (2, 6)); // at 'i' in id
        
        // Test position on third line
        assert_eq!(calculate_line_column(sql, 33), (3, 1)); // at 'A' in AND
    }
    
    #[test]
    fn test_calculate_line_column_with_unicode() {
        let sql = "SELECT '🎉' FROM table\nWHERE x = 1";
        
        // Test position after emoji (4 bytes)
        assert_eq!(calculate_line_column(sql, 8), (1, 9)); // at '🎉'
        assert_eq!(calculate_line_column(sql, 12), (1, 10)); // after '🎉'
    }
    
    #[test]
    fn test_calculate_line_column_real_world_example() {
        // Test case from user's example: error on the 'dhl'::carrier_code
        let sql = "SELECT 'dhl'::carrier_code FROM shipments";
        
        // Position at 'd' in 'dhl'
        assert_eq!(calculate_line_column(sql, 8), (1, 9));
        
        // Position at :: cast operator
        assert_eq!(calculate_line_column(sql, 13), (1, 14));
        
        // Position at 'c' in carrier_code
        assert_eq!(calculate_line_column(sql, 15), (1, 16));
    }
}

/// A RAII guard for safely changing the current working directory
/// This ensures the directory is always restored when the guard is dropped
pub struct DirectoryGuard {
    original_dir: PathBuf,
    changed: bool,
}

impl DirectoryGuard {
    /// Change to a new directory, saving the current directory for restoration
    pub fn change_to(new_dir: impl Into<PathBuf>) -> Result<Self> {
        let new_path = new_dir.into();
        
        // Get the current directory before changing
        let original_dir = env::current_dir()
            .map_err(|e| PgmgError::DirectoryChange {
                path: new_path.clone(),
                message: format!("Failed to get current directory: {}", e),
            })?;
        
        // Change to the new directory
        env::set_current_dir(&new_path)
            .map_err(|e| PgmgError::DirectoryChange {
                path: new_path,
                message: format!("Failed to change directory: {}", e),
            })?;
        
        Ok(Self {
            original_dir,
            changed: true,
        })
    }
    
    /// Get the original directory that will be restored
    pub fn original_dir(&self) -> &PathBuf {
        &self.original_dir
    }
    
    /// Manually restore the original directory
    /// This is automatically called on Drop, but can be called explicitly for error handling
    pub fn restore(mut self) -> Result<()> {
        if self.changed {
            env::set_current_dir(&self.original_dir)
                .map_err(|e| PgmgError::DirectoryRestore(format!(
                    "Failed to restore directory to {}: {}", 
                    self.original_dir.display(), 
                    e
                )))?;
            self.changed = false;
        }
        Ok(())
    }
}

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        if self.changed {
            if let Err(e) = env::set_current_dir(&self.original_dir) {
                eprintln!("Warning: Failed to restore directory to {}: {}", 
                         self.original_dir.display(), e);
                // We can't return an error from Drop, so we just log the issue
            }
        }
    }
}

#[cfg(test)]
mod directory_guard_tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_directory_guard_restores_on_drop() {
        let original = env::current_dir().unwrap().canonicalize().unwrap();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().canonicalize().unwrap();
        
        {
            let _guard = DirectoryGuard::change_to(&temp_path).unwrap();
            assert_eq!(env::current_dir().unwrap().canonicalize().unwrap(), temp_path);
        } // guard is dropped here
        
        assert_eq!(env::current_dir().unwrap().canonicalize().unwrap(), original);
        
        // Keep temp_dir alive until end of test
        drop(temp_dir);
    }
    
    #[test]
    fn test_directory_guard_manual_restore() {
        let original = env::current_dir().unwrap().canonicalize().unwrap();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().canonicalize().unwrap();
        
        let guard = DirectoryGuard::change_to(&temp_path).unwrap();
        assert_eq!(env::current_dir().unwrap().canonicalize().unwrap(), temp_path);
        
        guard.restore().unwrap();
        assert_eq!(env::current_dir().unwrap().canonicalize().unwrap(), original);
        
        // Keep temp_dir alive until end of test
        drop(temp_dir);
    }
    
    #[test]
    fn test_directory_guard_original_dir() {
        let original = env::current_dir().unwrap();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_path_buf();
        
        let guard = DirectoryGuard::change_to(&temp_path).unwrap();
        assert_eq!(guard.original_dir(), &original);
        
        // Keep temp_dir alive until end of test
        drop(temp_dir);
    }
}