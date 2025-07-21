use serde::{Deserialize, Serialize};
use crate::sql::{SqlObject, ObjectType};
use owo_colors::OwoColorize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlpgsqlCheckResult {
    #[serde(rename = "functionid")]
    pub functionid: Option<String>,
    #[serde(rename = "level")]
    pub level: Option<String>,
    #[serde(rename = "message")]
    pub message: Option<String>,
    #[serde(rename = "sqlState")]
    pub sqlstate: Option<String>,
    #[serde(rename = "detail")]
    pub detail: Option<String>,
    #[serde(rename = "hint")]
    pub hint: Option<String>,
    #[serde(rename = "context")]
    pub context: Option<String>,
    #[serde(rename = "lineno")]
    pub lineno: Option<i32>,
    #[serde(rename = "position")]
    pub position: Option<i32>,
}

#[derive(Debug)]
pub struct PlpgsqlCheckError {
    pub function_name: String,
    pub source_file: Option<String>,
    pub source_line: Option<usize>,
    pub check_result: PlpgsqlCheckResult,
}

/// Check if the plpgsql_check extension is installed
pub async fn is_plpgsql_check_available(client: &tokio_postgres::Transaction<'_>) -> Result<bool, Box<dyn std::error::Error>> {
    let result = client.query_one(
        "SELECT EXISTS (
            SELECT 1 FROM pg_extension 
            WHERE extname = 'plpgsql_check'
        )",
        &[]
    ).await?;
    
    Ok(result.get(0))
}

/// Run plpgsql_check on a function and return results
pub async fn check_function(
    client: &tokio_postgres::Transaction<'_>,
    function_name: &str,
) -> Result<Vec<PlpgsqlCheckResult>, Box<dyn std::error::Error>> {
    // Parse the function name to extract schema and name
    let (schema, name) = if function_name.contains('.') {
        let parts: Vec<&str> = function_name.splitn(2, '.').collect();
        (parts[0], parts[1].trim_end_matches("()"))
    } else {
        ("public", function_name.trim_end_matches("()"))
    };
    
    // Query that finds the function by name and runs plpgsql_check on it
    let query = "
        SELECT result::text
        FROM pg_proc p 
        JOIN pg_namespace n ON n.oid = p.pronamespace,
        LATERAL plpgsql_check_function(p.oid, format:='json') AS result
        WHERE n.nspname = $1 AND p.proname = $2 AND p.prokind IN ('f', 'p')
    ";
    
    let rows = client.query(query, &[&schema, &name]).await?;
    let mut results = Vec::new();
    
    for row in rows {
        // The JSON is returned as a single column
        if let Ok(json_str) = row.try_get::<_, String>(0) {
            // Parse the JSON wrapper structure
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
                // Extract issues array from the wrapper
                if let Some(issues) = json_value.get("issues").and_then(|v| v.as_array()) {
                    for issue in issues {
                        if let Ok(check_result) = serde_json::from_value::<PlpgsqlCheckResult>(issue.clone()) {
                            results.push(check_result);
                        }
                    }
                }
            }
        }
    }
    
    Ok(results)
}

/// Check all functions that were created or updated
pub async fn check_modified_functions(
    client: &tokio_postgres::Transaction<'_>,
    modified_objects: &[&SqlObject],
) -> Result<Vec<PlpgsqlCheckError>, Box<dyn std::error::Error>> {
    let mut errors = Vec::new();
    
    // Filter to only functions and procedures (both can contain PL/pgSQL code)
    let functions: Vec<_> = modified_objects.iter()
        .filter(|obj| matches!(obj.object_type, ObjectType::Function | ObjectType::Procedure))
        .collect();
    
    if functions.is_empty() {
        return Ok(errors);
    }
    
    // Check if extension is available
    if !is_plpgsql_check_available(client).await? {
        eprintln!("{}: plpgsql_check extension is not installed. Skipping function/procedure checks.", 
            "Warning".yellow().bold());
        return Ok(errors);
    }
    
    for function in functions {
        let func_name = match &function.qualified_name.schema {
            Some(schema) => format!("{}.{}()", schema, function.qualified_name.name),
            None => format!("{}()", function.qualified_name.name),
        };
        
        match check_function(client, &func_name).await {
            Ok(results) => {
                for result in results {
                    // Only report errors and warnings (skip notices)
                    if let Some(level) = &result.level {
                        if level == "error" || level == "warning" {
                            let error = PlpgsqlCheckError {
                                function_name: func_name.clone(),
                                source_file: function.source_file.as_ref().map(|p| p.to_string_lossy().to_string()),
                                source_line: calculate_source_line(function, result.lineno),
                                check_result: result,
                            };
                            errors.push(error);
                        }
                    }
                }
            }
            Err(e) => {
                // Log but don't fail the operation
                eprintln!("{}: Failed to check function/procedure {}: {}", 
                    "Warning".yellow().bold(), 
                    func_name.cyan(), 
                    e);
            }
        }
    }
    
    Ok(errors)
}

/// Check functions that have soft dependencies on modified functions
/// These are functions that call the modified functions and need validation
pub async fn check_soft_dependent_functions(
    client: &tokio_postgres::Transaction<'_>,
    dependency_graph: &crate::analysis::DependencyGraph,
    modified_objects: &[&SqlObject],
    all_file_objects: &[SqlObject],
) -> Result<Vec<PlpgsqlCheckError>, Box<dyn std::error::Error>> {
    use crate::analysis::ObjectRef;
    
    let mut errors = Vec::new();
    
    // Check if extension is available
    if !is_plpgsql_check_available(client).await? {
        return Ok(errors);
    }
    
    // Find all soft dependents of modified functions
    let mut functions_to_check = std::collections::HashSet::new();
    
    for modified_obj in modified_objects {
        if matches!(modified_obj.object_type, ObjectType::Function | ObjectType::Procedure) {
            let obj_ref = ObjectRef::from(*modified_obj);
            
            // Get all soft dependents (functions that call this function)
            for dependent in dependency_graph.soft_dependents_of(&obj_ref) {
                if matches!(dependent.object_type, ObjectType::Function | ObjectType::Procedure) {
                    functions_to_check.insert(dependent);
                }
            }
        }
    }
    
    if functions_to_check.is_empty() {
        return Ok(errors);
    }
    
    println!("\n{}: Checking {} dependent functions for compatibility...", 
        "Info".blue().bold(), 
        functions_to_check.len().to_string().yellow());
    
    let num_functions_to_check = functions_to_check.len();
    
    // Check each dependent function
    for func_ref in functions_to_check {
        // Find the function in file objects to get source info
        let function = all_file_objects.iter()
            .find(|obj| obj.object_type == func_ref.object_type && 
                       obj.qualified_name == func_ref.qualified_name);
        
        let func_name = match &func_ref.qualified_name.schema {
            Some(schema) => format!("{}.{}()", schema, func_ref.qualified_name.name),
            None => format!("{}()", func_ref.qualified_name.name),
        };
        
        match check_function(client, &func_name).await {
            Ok(results) => {
                for result in results {
                    // Only report errors (not warnings for dependent functions)
                    if let Some(level) = &result.level {
                        if level == "error" {
                            let error = PlpgsqlCheckError {
                                function_name: func_name.clone(),
                                source_file: function.and_then(|f| f.source_file.as_ref().map(|p| p.to_string_lossy().to_string())),
                                source_line: function.and_then(|f| calculate_source_line(f, result.lineno)),
                                check_result: result,
                            };
                            errors.push(error);
                        }
                    }
                }
            }
            Err(e) => {
                // Log but don't fail the operation
                eprintln!("{}: Failed to check dependent function {}: {}", 
                    "Warning".yellow().bold(), 
                    func_name.cyan(), 
                    e);
            }
        }
    }
    
    if errors.is_empty() && num_functions_to_check > 0 {
        println!("  {} All dependent functions remain compatible", "✓".green().bold());
    }
    
    Ok(errors)
}

/// Calculate the source file line number from function line number
fn calculate_source_line(function: &SqlObject, function_line: Option<i32>) -> Option<usize> {
    match (function.start_line, function_line) {
        (Some(start), Some(line)) => {
            // Function line numbers start at 1, we need to add to start_line
            Some(start + (line as usize) - 1)
        }
        _ => None,
    }
}

/// Format and display plpgsql_check errors
pub fn display_check_errors(errors: &[PlpgsqlCheckError]) {
    if errors.is_empty() {
        return;
    }
    
    println!("\n{}", "=== PL/pgSQL Check Results ===".bold().yellow());
    
    for error in errors {
        let level_str = error.check_result.level.as_deref().unwrap_or("error");
        let level_colored = match level_str {
            "error" => format!("{}", level_str.red().bold()),
            "warning" => format!("{}", level_str.yellow().bold()),
            _ => format!("{}", level_str.blue().bold()),
        };
        
        // Format location
        let location = match (&error.source_file, error.source_line) {
            (Some(file), Some(line)) => format!("{}:{}", file, line),
            (Some(file), None) => file.clone(),
            _ => error.function_name.clone(),
        };
        
        println!("\n{} {} in {}", 
            level_colored,
            format!("[{}]", error.check_result.sqlstate.as_deref().unwrap_or("00000")).dimmed(),
            location.cyan()
        );
        
        // Display the main message
        if let Some(message) = &error.check_result.message {
            println!("  {}", message);
        }
        
        // Display detail if available
        if let Some(detail) = &error.check_result.detail {
            println!("  {}: {}", "Detail".dimmed(), detail);
        }
        
        // Display hint if available
        if let Some(hint) = &error.check_result.hint {
            println!("  {}: {}", "Hint".green().dimmed(), hint);
        }
        
        // Display context if available
        if let Some(context) = &error.check_result.context {
            println!("  {}: {}", "Context".dimmed(), context);
        }
    }
    
    println!("\n{} {} found by plpgsql_check", 
        errors.len().to_string().yellow().bold(),
        if errors.len() == 1 { "issue" } else { "issues" }
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::QualifiedIdent;
    use std::path::PathBuf;
    
    #[test]
    fn test_calculate_source_line() {
        let mut function = SqlObject::new(
            ObjectType::Function,
            QualifiedIdent::new(Some("test".to_string()), "my_func".to_string()),
            "CREATE FUNCTION...".to_string(),
            Default::default(),
            Some(PathBuf::from("test.sql")),
        );
        function.start_line = Some(10);
        
        // Function line 1 maps to source line 10
        assert_eq!(calculate_source_line(&function, Some(1)), Some(10));
        
        // Function line 5 maps to source line 14
        assert_eq!(calculate_source_line(&function, Some(5)), Some(14));
        
        // No function line number
        assert_eq!(calculate_source_line(&function, None), None);
        
        // No start line
        function.start_line = None;
        assert_eq!(calculate_source_line(&function, Some(1)), None);
    }
    
    #[test]
    fn test_plpgsql_check_result_deserialization() {
        let json = r#"{
            "functionid": "12345",
            "lineno": 5,
            "position": 10,
            "sqlstate": "42703",
            "message": "column \"foo\" does not exist",
            "detail": "There is a column named \"bar\" in table \"test\", but it cannot be referenced from this part of the query.",
            "hint": "Try using a different column name.",
            "level": "error",
            "context": "SQL expression \"SELECT foo FROM test\""
        }"#;
        
        let result: PlpgsqlCheckResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.functionid, Some("12345".to_string()));
        assert_eq!(result.lineno, Some(5));
        assert_eq!(result.sqlstate, Some("42703".to_string()));
        assert_eq!(result.level, Some("error".to_string()));
    }
}