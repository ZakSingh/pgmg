# Error Handling and Logging Migration Guide

This guide shows how to migrate pgmg modules to use the new error handling and logging infrastructure.

## Example: Migrating the SQL Parser Module

### Before (using Box<dyn Error>):

```rust
use std::error::Error;

pub fn split_sql_file(content: &str) -> Result<Vec<Statement>, Box<dyn Error>> {
    let parse_result = pg_query::split_with_parser(content)?;
    
    let mut statements = Vec::new();
    for (sql, _) in parse_result {
        if sql.trim().is_empty() {
            continue;
        }
        statements.push(Statement { sql });
    }
    
    Ok(statements)
}
```

### After (using PgmgError):

```rust
use crate::error::{PgmgError, Result, ErrorContext};
use tracing::{debug, trace};

pub fn split_sql_file(content: &str) -> Result<Vec<Statement>> {
    debug!("Splitting SQL file into statements");
    
    let parse_result = pg_query::split_with_parser(content)
        .map_err(|e| PgmgError::SqlParse {
            file: PathBuf::from("unknown"),
            message: format!("Failed to split SQL: {}", e),
            source: Some(Box::new(e)),
        })?;
    
    let mut statements = Vec::new();
    for (sql, _) in parse_result {
        if sql.trim().is_empty() {
            continue;
        }
        trace!("Found statement: {} chars", sql.len());
        statements.push(Statement { sql });
    }
    
    debug!("Split into {} statements", statements.len());
    Ok(statements)
}

// When called with file context:
pub fn process_sql_file(path: &Path) -> Result<Vec<Statement>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| PgmgError::FileRead {
            path: path.to_path_buf(),
            message: e.to_string(),
            source: e,
        })?;
    
    split_sql_file(&content)
        .file_context(path)?  // Adds file context to any error
}
```

## Common Patterns

### 1. File Operations

```rust
// Reading files
let content = std::fs::read_to_string(&path)
    .map_err(|e| PgmgError::FileRead {
        path: path.clone(),
        message: e.to_string(),
        source: e,
    })?;

// Writing files
std::fs::write(&path, content)
    .map_err(|e| PgmgError::FileWrite {
        path: path.clone(),
        message: e.to_string(),
        source: e,
    })?;
```

### 2. Database Operations

```rust
// Connection errors
let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
    .await
    .map_err(|e| PgmgError::DatabaseConnection {
        message: format!("Failed to connect to {}", sanitized_conn_str),
        source: e,
    })?;

// Query errors
client.execute(sql, &[])
    .await
    .map_err(|e| PgmgError::Database {
        message: format!("Failed to execute statement"),
        source: e,
    })?;
```

### 3. SQL Parsing

```rust
// Parse SQL with context
let parsed = pg_query::parse(sql)
    .file_context(&file_path)?;

// Or with manual error construction
let parsed = pg_query::parse(sql)
    .map_err(|e| PgmgError::SqlParse {
        file: file_path.clone(),
        message: e.to_string(),
        source: Some(Box::new(e)),
    })?;
```

### 4. Migration-specific Errors

```rust
// Migration failures
if let Err(e) = client.batch_execute(&migration_sql).await {
    return Err(PgmgError::MigrationFailed {
        name: migration_name.to_string(),
        statement: statement_num,
        message: e.to_string(),
    });
}
```

### 5. Using Context Traits

```rust
// Add file context to any error
some_operation()
    .file_context("path/to/file.sql")?;

// Add object context
some_operation()
    .object_context("view", "user_stats")?;

// Add migration context
some_operation()
    .migration_context("001_initial_schema")?;
```

## Logging Best Practices

### 1. Log Levels

```rust
use tracing::{trace, debug, info, warn, error};

// TRACE: Very detailed information, usually only needed for debugging
trace!("Parsing statement: {}", sql);

// DEBUG: Information useful for debugging
debug!("Processing file: {}", path.display());

// INFO: General informational messages
info!("Applied migration: {}", migration_name);

// WARN: Warning messages for recoverable issues
warn!("Skipping invalid file: {}", path.display());

// ERROR: Error messages (but prefer returning errors)
error!("Fatal error: {}", err);
```

### 2. Structured Logging

```rust
// Use structured fields for better filtering
info!(
    migration = %migration_name,
    duration_ms = %elapsed.as_millis(),
    "Migration completed"
);

// For SQL operations
debug!(
    file = %file_path.display(),
    line = line_number,
    statement_type = %stmt_type,
    "Processing SQL statement"
);
```

### 3. Progress Tracking

```rust
use crate::logging::Progress;

// For operations with known count
let mut progress = Progress::with_total("Processing files", files.len());
for file in files {
    process_file(file)?;
    progress.increment();
}

// For operations without known count
let mut progress = Progress::new("Scanning directory");
for entry in walkdir::WalkDir::new(dir) {
    progress.increment();
    // ...
}
```

### 4. User-Facing Output

```rust
use crate::logging::output;

// Success messages
output::success("All migrations applied successfully");

// Error messages
output::error("Failed to connect to database");

// Warnings
output::warning("No migrations found in directory");

// Info messages
output::info("Using configuration from pgmg.toml");

// Step indicators
output::step("Applying migration 001_initial_schema");

// Headers for sections
output::header("Planning Changes");
output::subheader("New Migrations");
```

## Testing Error Scenarios

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_file_not_found_error() {
        let result = process_sql_file(Path::new("nonexistent.sql"));
        
        match result {
            Err(PgmgError::FileNotFound(path)) => {
                assert_eq!(path, Path::new("nonexistent.sql"));
            }
            _ => panic!("Expected FileNotFound error"),
        }
    }
    
    #[test]
    fn test_sql_parse_error() {
        let result = split_sql_file("INVALID SQL {{");
        
        match result {
            Err(PgmgError::SqlParse { message, .. }) => {
                assert!(message.contains("syntax error"));
            }
            _ => panic!("Expected SqlParse error"),
        }
    }
}
```

## Error Display Examples

The new error types provide rich error messages:

```
Error: Failed to parse SQL in migrations/001_schema.sql: syntax error at or near "CRAETE"
  Caused by: Parse("syntax error at or near \"CRAETE\"")

SQL parsing error in migrations/001_schema.sql:
syntax error at or near "CRAETE"
- Check SQL syntax
- Ensure the file contains valid PostgreSQL statements
```

## Summary

1. Replace `Box<dyn Error>` with `Result<T>` (which is `Result<T, PgmgError>`)
2. Use specific error variants for different error types
3. Add context using `.file_context()`, `.object_context()`, etc.
4. Use structured logging with tracing
5. Use `logging::output` for user-facing messages
6. Return errors instead of printing them (let main handle display)