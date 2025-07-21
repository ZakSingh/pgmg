use std::path::{Path, PathBuf};
use std::fs;
use crate::sql::{SqlObject, splitter::split_sql_file, objects::identify_sql_object};
use crate::BuiltinCatalog;
use pg_query;

/// Scan a directory for .sql files and parse them into SQL objects
pub async fn scan_sql_files(
    directory: &Path,
    builtin_catalog: &BuiltinCatalog,
) -> Result<Vec<SqlObject>, Box<dyn std::error::Error>> {
    let mut sql_objects = Vec::new();
    
    scan_directory_recursive(directory, &mut sql_objects, builtin_catalog, directory)?;
    
    Ok(sql_objects)
}

fn scan_directory_recursive(
    dir: &Path,
    sql_objects: &mut Vec<SqlObject>,
    builtin_catalog: &BuiltinCatalog,
    _base_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = fs::read_dir(dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            // Recursively scan subdirectories
            scan_directory_recursive(&path, sql_objects, builtin_catalog, _base_path)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("sql") {
            // Skip test files - they should not be treated as database objects
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if file_name.contains(".test.") {
                    continue;
                }
            }
            
            // Process .sql files
            if let Err(e) = process_sql_file(&path, sql_objects, builtin_catalog, _base_path) {
                eprintln!("Warning: Failed to process {}: {}", path.display(), e);
                continue;
            }
        }
    }
    
    Ok(())
}

fn process_sql_file(
    file_path: &Path,
    sql_objects: &mut Vec<SqlObject>,
    _builtin_catalog: &BuiltinCatalog,
    _base_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Read file content
    let content = fs::read_to_string(file_path)?;
    
    // Skip empty files
    if content.trim().is_empty() {
        return Ok(());
    }
    
    // Split into statements
    let statements = split_sql_file(&content)?;
    
    // Identify objects in each statement
    for statement in statements {
        if let Some(mut object) = identify_sql_object(&statement.sql)? {
            // Set the file path and line numbers for the object
            object.source_file = Some(file_path.to_path_buf());
            object.start_line = statement.start_line;
            object.end_line = statement.end_line;
            sql_objects.push(object);
        } else {
            // Log warning for unprocessable statements
            warn_unprocessable_statement(file_path, &statement)?;
        }
    }
    
    Ok(())
}

/// Analyze and warn about unprocessable SQL statements
fn warn_unprocessable_statement(
    file_path: &Path,
    statement: &crate::sql::splitter::SqlStatement,
) -> Result<(), Box<dyn std::error::Error>> {
    let relative_path = file_path.strip_prefix(std::env::current_dir().unwrap_or_default())
        .unwrap_or(file_path);
    
    let statement_type = identify_statement_type(&statement.sql);
    let sql_preview = create_sql_preview(&statement.sql);
    
    let line_info = if let Some(start) = statement.start_line {
        if let Some(end) = statement.end_line {
            if start == end {
                format!("line {}", start)
            } else {
                format!("lines {}-{}", start, end)
            }
        } else {
            format!("line {}", start)
        }
    } else {
        "unknown line".to_string()
    };
    
    tracing::warn!(
        "Skipping unprocessable {} statement in {} ({}): {}",
        statement_type,
        relative_path.display(),
        line_info,
        sql_preview
    );
    
    Ok(())
}

/// Identify the type of SQL statement for warning messages
fn identify_statement_type(sql: &str) -> &'static str {
    let trimmed = sql.trim().to_uppercase();
    
    // Try to parse with pg_query first to distinguish parse failures
    match pg_query::parse(sql) {
        Err(_) => "malformed",
        Ok(_) => {
            // Statement parsed successfully but pgmg doesn't handle it
            if trimmed.starts_with("UPDATE") {
                "UPDATE"
            } else if trimmed.starts_with("DELETE") {
                "DELETE"
            } else if trimmed.starts_with("INSERT") {
                "INSERT"
            } else if trimmed.starts_with("SELECT") {
                "SELECT"
            } else if trimmed.starts_with("ALTER") {
                "ALTER"
            } else if trimmed.starts_with("DROP") {
                "DROP"
            } else if trimmed.starts_with("GRANT") || trimmed.starts_with("REVOKE") {
                "permission"
            } else if trimmed.starts_with("SET") {
                "SET"
            } else if trimmed.starts_with("TRUNCATE") {
                "TRUNCATE"
            } else if trimmed.starts_with("ANALYZE") || trimmed.starts_with("VACUUM") {
                "maintenance"
            } else if trimmed.starts_with("BEGIN") || trimmed.starts_with("COMMIT") || trimmed.starts_with("ROLLBACK") {
                "transaction"
            } else {
                "unsupported"
            }
        }
    }
}

/// Create a preview of the SQL statement for warning messages
fn create_sql_preview(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.len() <= 80 {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..77])
    }
}

/// Scan migrations directory for migration files
pub async fn scan_migrations(
    migrations_dir: &Path,
) -> Result<Vec<MigrationFile>, Box<dyn std::error::Error>> {
    let mut migrations = Vec::new();
    
    if !migrations_dir.exists() {
        return Ok(migrations);
    }
    
    let entries = fs::read_dir(migrations_dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("sql") {
            if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                migrations.push(MigrationFile {
                    name: name.to_string(),
                    path: path.clone(),
                });
            }
        }
    }
    
    // Sort migrations by name (assuming they follow a naming convention like 001_create_users.sql)
    migrations.sort_by(|a, b| a.name.cmp(&b.name));
    
    Ok(migrations)
}

#[derive(Debug, Clone)]
pub struct MigrationFile {
    pub name: String,
    pub path: PathBuf,
}

impl MigrationFile {
    pub fn read_content(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(fs::read_to_string(&self.path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_scan_empty_directory() {
        let temp_dir = tempdir().unwrap();
        let builtin_catalog = BuiltinCatalog::new();
        
        let objects = scan_sql_files(temp_dir.path(), &builtin_catalog).await.unwrap();
        assert!(objects.is_empty());
    }
    
    #[tokio::test]
    async fn test_scan_sql_files_excludes_test_files() {
        let temp_dir = tempdir().unwrap();
        let code_dir = temp_dir.path();
        
        // Create various SQL files
        fs::write(code_dir.join("create_table.sql"), "CREATE TABLE users (id SERIAL);").unwrap();
        fs::write(code_dir.join("create_function.sql"), "CREATE FUNCTION get_user() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;").unwrap();
        fs::write(code_dir.join("users.test.sql"), "BEGIN; SELECT plan(1); SELECT pass('test'); SELECT * FROM finish(); ROLLBACK;").unwrap();
        fs::write(code_dir.join("another.test.sql"), "BEGIN; SELECT plan(2); SELECT ok(true); SELECT ok(false); SELECT * FROM finish(); ROLLBACK;").unwrap();
        
        // Create a subdirectory with more files
        let sub_dir = code_dir.join("functions");
        fs::create_dir(&sub_dir).unwrap();
        fs::write(sub_dir.join("helper.sql"), "CREATE FUNCTION helper() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;").unwrap();
        fs::write(sub_dir.join("helper.test.sql"), "BEGIN; SELECT plan(1); SELECT is(helper(), 1); SELECT * FROM finish(); ROLLBACK;").unwrap();
        
        let builtin_catalog = BuiltinCatalog::new();
        let sql_objects = scan_sql_files(code_dir, &builtin_catalog).await.unwrap();
        
        // Should have found 3 SQL objects (excluding the 3 test files)
        assert_eq!(sql_objects.len(), 3);
        
        // Verify none of the objects are from test files
        for obj in &sql_objects {
            if let Some(source_file) = &obj.source_file {
                let file_name = source_file.file_name().unwrap().to_str().unwrap();
                assert!(!file_name.contains(".test."), "Test file {} was incorrectly included", file_name);
            }
        }
        
        // Verify we found the correct objects
        let object_names: Vec<String> = sql_objects.iter()
            .map(|obj| obj.qualified_name.name.clone())
            .collect();
        
        assert!(object_names.contains(&"users".to_string()));
        assert!(object_names.contains(&"get_user".to_string()));
        assert!(object_names.contains(&"helper".to_string()));
    }

    #[tokio::test]
    async fn test_scan_migrations() {
        let temp_dir = tempdir().unwrap();
        let migrations_dir = temp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        
        // Create some migration files
        fs::write(migrations_dir.join("001_create_users.sql"), "CREATE TABLE users (id SERIAL);").unwrap();
        fs::write(migrations_dir.join("002_add_indexes.sql"), "CREATE INDEX idx_users_id ON users(id);").unwrap();
        
        let migrations = scan_migrations(&migrations_dir).await.unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].name, "001_create_users");
        assert_eq!(migrations[1].name, "002_add_indexes");
    }

    #[test]
    fn test_migration_file_read_content() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.sql");
        let content = "CREATE TABLE test (id INT);";
        fs::write(&file_path, content).unwrap();
        
        let migration = MigrationFile {
            name: "test".to_string(),
            path: file_path,
        };
        
        assert_eq!(migration.read_content().unwrap(), content);
    }
}