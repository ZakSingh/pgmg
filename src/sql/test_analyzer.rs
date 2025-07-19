use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use crate::sql::{splitter::split_sql_file, parser::analyze_statement, Dependencies, ObjectType};
use crate::analysis::graph::ObjectRef;
use crate::builtin_catalog::BuiltinCatalog;

/// Represents a test file and its dependencies
#[derive(Debug, Clone)]
pub struct TestFile {
    pub path: PathBuf,
    pub dependencies: Dependencies,
}

/// Maps test files to their dependencies and vice versa
#[derive(Debug)]
pub struct TestDependencyMap {
    // Map from test file path to its dependencies
    pub tests: HashMap<PathBuf, Dependencies>,
    // Reverse map: object -> tests that depend on it
    object_to_tests: HashMap<ObjectRef, Vec<PathBuf>>,
}

impl TestDependencyMap {
    /// Create a new empty test dependency map
    pub fn new() -> Self {
        Self {
            tests: HashMap::new(),
            object_to_tests: HashMap::new(),
        }
    }
    
    /// Find all test files that depend on the given objects
    pub fn find_tests_for_objects(&self, objects: &[ObjectRef]) -> Vec<PathBuf> {
        let mut affected_tests = std::collections::HashSet::new();
        
        for obj in objects {
            if let Some(tests) = self.object_to_tests.get(obj) {
                for test_path in tests {
                    affected_tests.insert(test_path.clone());
                }
            }
        }
        
        affected_tests.into_iter().collect()
    }
    
    /// Get dependencies for a specific test file
    pub fn get_test_dependencies(&self, test_path: &Path) -> Option<&Dependencies> {
        self.tests.get(test_path)
    }
}

/// Analyze a single test file to extract its dependencies
pub async fn analyze_test_file(
    path: &Path,
    builtin_catalog: &BuiltinCatalog,
) -> Result<TestFile, Box<dyn std::error::Error>> {
    // Read the test file
    let content = fs::read_to_string(path)?;
    
    // Split into statements
    let statements = split_sql_file(&content)?;
    
    // Analyze each statement and collect dependencies
    let mut all_dependencies = Dependencies::default();
    
    for statement in statements {
        // Skip empty statements
        if statement.sql.trim().is_empty() {
            continue;
        }
        
        // Skip transaction control statements (BEGIN, ROLLBACK, COMMIT)
        let trimmed = statement.sql.trim().to_uppercase();
        if trimmed == "BEGIN" || trimmed == "ROLLBACK" || trimmed == "COMMIT" 
            || trimmed.starts_with("BEGIN;") || trimmed.starts_with("ROLLBACK;") || trimmed.starts_with("COMMIT;") {
            continue;
        }
        
        // Analyze the statement for dependencies
        match analyze_statement(&statement.sql) {
            Ok(deps) => {
                // Merge dependencies
                all_dependencies.relations.extend(deps.relations);
                all_dependencies.functions.extend(deps.functions);
                all_dependencies.types.extend(deps.types);
            }
            Err(e) => {
                // Log warning but continue - some pgTAP functions might not parse correctly
                eprintln!("Warning: Failed to analyze statement in test {}: {}", path.display(), e);
            }
        }
    }
    
    // Filter out built-in objects
    let filtered_deps = crate::sql::filter_builtins(all_dependencies, builtin_catalog);
    
    // Also filter out pgTAP functions (plan, pass, ok, is, isnt, etc.)
    let filtered_deps = filter_pgtap_functions(filtered_deps);
    
    Ok(TestFile {
        path: path.to_path_buf(),
        dependencies: filtered_deps,
    })
}

/// Filter out pgTAP-specific functions from dependencies
fn filter_pgtap_functions(mut deps: Dependencies) -> Dependencies {
    // List of common pgTAP functions to exclude
    let pgtap_functions = [
        "plan", "pass", "fail", "ok", "is", "isnt", "matches", "doesnt_match",
        "alike", "unalike", "cmp_ok", "isa_ok", "throws_ok", "lives_ok",
        "performs_ok", "finish", "no_plan", "skip", "todo", "todo_start", "todo_end",
        "diag", "note", "has_table", "has_view", "has_function", "has_type",
        "has_column", "has_index", "has_trigger", "has_schema", "has_role"
    ];
    
    deps.functions.retain(|func| {
        // Keep functions that are not in the pgTAP list
        !pgtap_functions.contains(&func.name.as_str())
    });
    
    deps
}

/// Scan a directory for test files and analyze them
pub async fn scan_test_files(
    dir: &Path,
    builtin_catalog: &BuiltinCatalog,
) -> Result<Vec<TestFile>, Box<dyn std::error::Error>> {
    let mut test_files = Vec::new();
    
    scan_directory_recursive(dir, &mut test_files, builtin_catalog).await?;
    
    Ok(test_files)
}

/// Recursively scan directory for .test.sql files
async fn scan_directory_recursive(
    dir: &Path,
    test_files: &mut Vec<TestFile>,
    builtin_catalog: &BuiltinCatalog,
) -> Result<(), Box<dyn std::error::Error>> {
    let entries = fs::read_dir(dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            // Recursively scan subdirectories
            Box::pin(scan_directory_recursive(&path, test_files, builtin_catalog)).await?;
        } else if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if file_name.ends_with(".test.sql") {
                    match analyze_test_file(&path, builtin_catalog).await {
                        Ok(test_file) => test_files.push(test_file),
                        Err(e) => eprintln!("Warning: Failed to analyze test file {}: {}", path.display(), e),
                    }
                }
            }
        }
    }
    
    Ok(())
}

/// Build a bidirectional dependency map from analyzed test files
pub fn build_test_dependency_map(tests: Vec<TestFile>) -> TestDependencyMap {
    let mut map = TestDependencyMap::new();
    
    for test in tests {
        // Add to forward map
        map.tests.insert(test.path.clone(), test.dependencies.clone());
        
        // Add to reverse map
        // Relations (tables, views, materialized views)
        for relation in &test.dependencies.relations {
            // We need to guess the object type since we only have the name
            // In practice, this would be matched against actual database objects
            let object_refs = vec![
                ObjectRef {
                    object_type: ObjectType::Table,
                    qualified_name: relation.clone(),
                },
                ObjectRef {
                    object_type: ObjectType::View,
                    qualified_name: relation.clone(),
                },
                ObjectRef {
                    object_type: ObjectType::MaterializedView,
                    qualified_name: relation.clone(),
                },
            ];
            
            for obj_ref in object_refs {
                map.object_to_tests
                    .entry(obj_ref)
                    .or_insert_with(Vec::new)
                    .push(test.path.clone());
            }
        }
        
        // Functions and procedures
        for function in &test.dependencies.functions {
            // Check both as function and procedure since we don't know which it is
            let func_refs = vec![
                ObjectRef {
                    object_type: ObjectType::Function,
                    qualified_name: function.clone(),
                },
                ObjectRef {
                    object_type: ObjectType::Procedure,
                    qualified_name: function.clone(),
                },
            ];
            
            for obj_ref in func_refs {
                map.object_to_tests
                    .entry(obj_ref)
                    .or_insert_with(Vec::new)
                    .push(test.path.clone());
            }
        }
        
        // Types
        for type_name in &test.dependencies.types {
            let type_refs = vec![
                ObjectRef {
                    object_type: ObjectType::Type,
                    qualified_name: type_name.clone(),
                },
                ObjectRef {
                    object_type: ObjectType::Domain,
                    qualified_name: type_name.clone(),
                },
            ];
            
            for obj_ref in type_refs {
                map.object_to_tests
                    .entry(obj_ref)
                    .or_insert_with(Vec::new)
                    .push(test.path.clone());
            }
        }
    }
    
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::QualifiedIdent;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_analyze_test_file() {
        let temp_dir = tempdir().unwrap();
        let test_file = temp_dir.path().join("example.test.sql");
        
        let content = r#"
BEGIN;
SELECT plan(3);

-- Test that users table exists
SELECT has_table('users');

-- Test function
SELECT is(get_user_count(), 5, 'Should have 5 users');

-- Test view
SELECT * FROM user_stats WHERE total > 0;

SELECT * FROM finish();
ROLLBACK;
"#;
        
        fs::write(&test_file, content).unwrap();
        
        let builtin_catalog = BuiltinCatalog::new();
        let test_analysis = analyze_test_file(&test_file, &builtin_catalog).await.unwrap();
        
        // Debug output
        println!("Relations found: {:?}", test_analysis.dependencies.relations);
        println!("Functions found: {:?}", test_analysis.dependencies.functions);
        
        // Should have found dependencies
        assert!(test_analysis.dependencies.relations.contains(&QualifiedIdent::from_name("user_stats".to_string())));
        assert!(test_analysis.dependencies.functions.contains(&QualifiedIdent::from_name("get_user_count".to_string())));
        
        // Should NOT include pgTAP functions
        assert!(!test_analysis.dependencies.functions.contains(&QualifiedIdent::from_name("plan".to_string())));
        assert!(!test_analysis.dependencies.functions.contains(&QualifiedIdent::from_name("has_table".to_string())));
        assert!(!test_analysis.dependencies.functions.contains(&QualifiedIdent::from_name("is".to_string())));
        assert!(!test_analysis.dependencies.functions.contains(&QualifiedIdent::from_name("finish".to_string())));
    }
    
    #[tokio::test]
    async fn test_scan_test_files() {
        let temp_dir = tempdir().unwrap();
        let code_dir = temp_dir.path();
        
        // Create test files
        fs::write(code_dir.join("users.test.sql"), "BEGIN; SELECT * FROM users; ROLLBACK;").unwrap();
        fs::write(code_dir.join("products.test.sql"), "BEGIN; SELECT * FROM products; ROLLBACK;").unwrap();
        
        // Create a subdirectory with more test files
        let sub_dir = code_dir.join("api");
        fs::create_dir(&sub_dir).unwrap();
        fs::write(sub_dir.join("api.test.sql"), "BEGIN; SELECT get_user(1); ROLLBACK;").unwrap();
        
        // Create non-test SQL files (should be ignored)
        fs::write(code_dir.join("schema.sql"), "CREATE TABLE ignored (id INT);").unwrap();
        
        let builtin_catalog = BuiltinCatalog::new();
        let test_files = scan_test_files(code_dir, &builtin_catalog).await.unwrap();
        
        // Should find exactly 3 test files
        assert_eq!(test_files.len(), 3);
        
        // Verify we found the right files
        let file_names: Vec<String> = test_files.iter()
            .map(|tf| tf.path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        
        assert!(file_names.contains(&"users.test.sql".to_string()));
        assert!(file_names.contains(&"products.test.sql".to_string()));
        assert!(file_names.contains(&"api.test.sql".to_string()));
    }
}