use std::path::{Path, PathBuf};
use std::fs;
use crate::sql::{SqlObject, splitter::split_sql_file, objects::identify_sql_object};
use crate::BuiltinCatalog;

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
        }
    }
    
    Ok(())
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