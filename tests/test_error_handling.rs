use pgmg::error::{PgmgError, ErrorContext};
use std::path::PathBuf;

#[test]
fn test_error_display() {
    let err = PgmgError::SqlParse {
        file: PathBuf::from("test.sql"),
        message: "Invalid syntax".to_string(),
        source: None,
    };
    
    let display = err.to_string();
    assert!(display.contains("test.sql"));
    assert!(display.contains("Invalid syntax"));
}

#[test]
fn test_error_context() {
    // Test file context
    let result: Result<(), PgmgError> = Err(PgmgError::Other("test error".to_string()));
    let with_context = result.file_context("my_file.sql");
    
    match with_context {
        Err(e) => assert_eq!(e.to_string(), "test error"),
        Ok(_) => panic!("Expected error"),
    }
}

#[test]
fn test_error_suggestions() {
    use pgmg::error::suggest_fix;
    
    // Test invalid connection string suggestions
    let conn_str_err = PgmgError::InvalidConnectionString("bad string".to_string());
    let suggestion = suggest_fix(&conn_str_err);
    assert!(suggestion.is_some());
    assert!(suggestion.unwrap().contains("postgres://"));
    
    // Test file not found suggestions
    let file_err = PgmgError::FileNotFound(PathBuf::from("/tmp/missing.sql"));
    let suggestion = suggest_fix(&file_err);
    assert!(suggestion.is_some());
    assert!(suggestion.unwrap().contains("Check if the path is correct"));
    
    // Test circular dependency suggestions
    let circ_err = PgmgError::CircularDependency("A -> B -> A".to_string());
    let suggestion = suggest_fix(&circ_err);
    assert!(suggestion.is_some());
    assert!(suggestion.unwrap().contains("Circular dependency detected"));
}

#[test]
fn test_error_chain_formatting() {
    use pgmg::error::format_error_chain;
    
    let err = PgmgError::SqlParse {
        file: PathBuf::from("test.sql"),
        message: "Parse failed".to_string(),
        source: Some(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid UTF-8"
        ))),
    };
    
    let formatted = format_error_chain(&err);
    assert!(formatted.contains("Parse failed"));
    assert!(formatted.contains("Caused by:"));
    assert!(formatted.contains("Invalid UTF-8"));
}

#[test]
fn test_file_errors() {
    let err = PgmgError::FileNotFound(PathBuf::from("/tmp/missing.sql"));
    assert!(err.to_string().contains("/tmp/missing.sql"));
    
    let err = PgmgError::PermissionDenied(PathBuf::from("/root/file.sql"));
    assert!(err.to_string().contains("/root/file.sql"));
}

#[test]
fn test_dependency_errors() {
    let err = PgmgError::CircularDependency("A -> B -> C -> A".to_string());
    assert!(err.to_string().contains("Circular dependency"));
    assert!(err.to_string().contains("A -> B -> C -> A"));
    
    let err = PgmgError::MissingDependency {
        object: "view_a".to_string(),
        dependency: "table_b".to_string(),
    };
    assert!(err.to_string().contains("view_a"));
    assert!(err.to_string().contains("table_b"));
}

#[test]
fn test_migration_errors() {
    let err = PgmgError::MigrationFailed {
        name: "001_initial".to_string(),
        statement: 3,
        message: "syntax error".to_string(),
    };
    assert!(err.to_string().contains("001_initial"));
    assert!(err.to_string().contains("statement 3"));
    assert!(err.to_string().contains("syntax error"));
}