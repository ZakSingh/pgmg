use crate::sql::{SqlObject, ObjectType};
use owo_colors::OwoColorize;

#[derive(Debug, Clone)]
pub struct PlpgsqlCheckResult {
    pub functionid: Option<String>,
    pub lineno: Option<i32>,
    pub statement: Option<String>,
    pub sqlstate: Option<String>,
    pub message: Option<String>,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub level: Option<String>,
    pub position: Option<i32>,
    pub query: Option<String>,
    pub context: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PlpgsqlCheckError {
    pub function_name: String,
    pub source_file: Option<String>,
    pub source_line: Option<usize>,
    pub check_result: PlpgsqlCheckResult,
}

/// Check if the plpgsql_check extension is installed
pub async fn is_plpgsql_check_available<C>(client: &C) -> Result<bool, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
    let result = client.query_one(
        "SELECT EXISTS (
            SELECT 1 FROM pg_extension 
            WHERE extname = 'plpgsql_check'
        )",
        &[]
    ).await?;
    
    Ok(result.get(0))
}

/// Run plpgsql_check on all functions using the bulk query approach
pub async fn check_all_functions<C>(
    client: &C,
    schema_filter: Option<&[String]>,
    function_name_filter: Option<&str>,
) -> Result<Vec<PlpgsqlCheckResult>, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
    // Base query from plpgsql_check README
    let base_query = "
        SELECT
          (pcf).functionid::regprocedure::text, (pcf).lineno, (pcf).statement,
          (pcf).sqlstate, (pcf).message, (pcf).detail, (pcf).hint, (pcf).level,
          (pcf).\"position\", (pcf).query, (pcf).context
        FROM
          (
            SELECT
              plpgsql_check_function_tb(pg_proc.oid, COALESCE(pg_trigger.tgrelid, 0),
                                        oldtable=>pg_trigger.tgoldtable,
                                        newtable=>pg_trigger.tgnewtable) AS pcf
            FROM pg_proc
                 LEFT JOIN pg_trigger
                           ON (pg_trigger.tgfoid = pg_proc.oid)
            WHERE
              prolang = (SELECT lang.oid FROM pg_language lang WHERE lang.lanname = 'plpgsql') AND
              pronamespace <> (SELECT nsp.oid FROM pg_namespace nsp WHERE nsp.nspname = 'pg_catalog')";

    // Build dynamic WHERE conditions
    let mut where_conditions = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Send + Sync>> = Vec::new();
    let mut param_index = 1;

    // Add schema filtering
    if let Some(schemas) = schema_filter {
        if !schemas.is_empty() {
            where_conditions.push(format!("AND pronamespace IN (SELECT oid FROM pg_namespace WHERE nspname = ANY(${}))", param_index));
            params.push(Box::new(schemas.to_vec()));
            param_index += 1;
        }
    } else {
        // Default: exclude pg_* and information_schema
        where_conditions.push("AND pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema')".to_string());
    }

    // Add function name filtering  
    if let Some(function_name) = function_name_filter {
        // Parse schema-qualified function names
        if function_name.contains('.') {
            let parts: Vec<&str> = function_name.splitn(2, '.').collect();
            let schema_name = parts[0].to_string();
            let func_name = parts[1].trim_end_matches("()").to_string();
            where_conditions.push(format!("AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = ${}) AND proname = ${}", param_index, param_index + 1));
            params.push(Box::new(schema_name));
            params.push(Box::new(func_name));
        } else {
            let func_name = function_name.trim_end_matches("()").to_string();
            where_conditions.push(format!("AND proname = ${}", param_index));
            params.push(Box::new(func_name));
        }
    } else {
        // When checking all functions, exclude internal ones starting with underscore
        where_conditions.push("AND proname NOT LIKE '\\_%'".to_string());
    }

    // Complete the query
    let full_query = format!("{}
              {}
              -- ignore unused triggers
              AND (pg_proc.prorettype <> (SELECT typ.oid FROM pg_type typ WHERE typ.typname = 'trigger') OR
                   pg_trigger.tgfoid IS NOT NULL)
            OFFSET 0
          ) ss
        ORDER BY (pcf).functionid::regprocedure::text, (pcf).lineno",
        base_query,
        where_conditions.join(" ")
    );

    let rows = if params.is_empty() {
        client.query(&full_query, &[] as &[&(dyn tokio_postgres::types::ToSql + Sync)]).await?
    } else {
        let params_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
            params.iter().map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        client.query(&full_query, &params_refs).await?
    };
    let mut results = Vec::new();

    for row in rows {
        // Parse the table output
        let result = PlpgsqlCheckResult {
            functionid: row.get::<_, Option<String>>(0),
            lineno: row.get::<_, Option<i32>>(1),
            statement: row.get::<_, Option<String>>(2),
            sqlstate: row.get::<_, Option<String>>(3),
            message: row.get::<_, Option<String>>(4),
            detail: row.get::<_, Option<String>>(5),
            hint: row.get::<_, Option<String>>(6),
            level: row.get::<_, Option<String>>(7),
            position: row.get::<_, Option<i32>>(8),
            query: row.get::<_, Option<String>>(9),
            context: row.get::<_, Option<String>>(10),
        };

        // Only include results with actual messages (skip empty rows)
        if result.level.is_some() && result.message.is_some() {
            results.push(result);
        }
    }

    Ok(results)
}

/// Check all functions that were created or updated using the bulk query approach
pub async fn check_modified_functions<C>(
    client: &C,
    modified_objects: &[&SqlObject],
) -> Result<Vec<PlpgsqlCheckError>, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
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
    
    // Use bulk query to check all functions, then filter results
    let all_results = check_all_functions(client, None, None).await?;
    
    // Create a map of modified function names for quick lookup
    let mut modified_function_names = std::collections::HashSet::new();
    for function in &functions {
        let func_name = match &function.qualified_name.schema {
            Some(schema) => format!("{}.{}", schema, function.qualified_name.name),
            None => function.qualified_name.name.clone(),
        };
        modified_function_names.insert(func_name);
    }
    
    // Filter bulk results to only modified functions
    for result in all_results {
        if let Some(functionid) = &result.functionid {
            // Extract function name from regprocedure format (schema.function or just function)
            let function_name = if functionid.contains('(') {
                // Remove parameters from function signature
                functionid.split('(').next().unwrap_or(functionid).to_string()
            } else {
                functionid.clone()
            };
            
            // Check if this function was modified
            if modified_function_names.contains(&function_name) {
                // Only report errors and warnings (skip notices)
                if let Some(level) = &result.level {
                    if level == "error" || level == "warning" {
                        // Find the corresponding SqlObject for source file info
                        let source_info = functions.iter()
                            .find(|f| {
                                let obj_name = match &f.qualified_name.schema {
                                    Some(schema) => format!("{}.{}", schema, f.qualified_name.name),
                                    None => f.qualified_name.name.clone(),
                                };
                                obj_name == function_name
                            });
                        
                        let error = PlpgsqlCheckError {
                            function_name: function_name.clone(),
                            source_file: source_info.and_then(|f| f.source_file.as_ref().map(|p| p.to_string_lossy().to_string())),
                            source_line: source_info.and_then(|f| calculate_source_line(f, result.lineno)),
                            check_result: result,
                        };
                        errors.push(error);
                    }
                }
            }
        }
    }
    
    Ok(errors)
}

/// Check functions that have soft dependencies on modified functions
/// These are functions that call the modified functions and need validation
pub async fn check_soft_dependent_functions<C>(
    client: &C,
    dependency_graph: &crate::analysis::DependencyGraph,
    modified_objects: &[&SqlObject],
    all_file_objects: &[SqlObject],
) -> Result<Vec<PlpgsqlCheckError>, Box<dyn std::error::Error>>
where
    C: tokio_postgres::GenericClient,
{
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
    
    // Don't print status message here to avoid breaking output flow
    
    let num_functions_to_check = functions_to_check.len();
    
    // Use bulk query to check all functions, then filter to dependents
    let all_results = check_all_functions(client, None, None).await?;
    
    // Create a map of function names to check
    let mut dependent_function_names = std::collections::HashSet::new();
    for func_ref in &functions_to_check {
        let func_name = match &func_ref.qualified_name.schema {
            Some(schema) => format!("{}.{}", schema, func_ref.qualified_name.name),
            None => func_ref.qualified_name.name.clone(),
        };
        dependent_function_names.insert(func_name);
    }
    
    // Filter bulk results to only dependent functions
    for result in all_results {
        if let Some(functionid) = &result.functionid {
            // Extract function name from regprocedure format
            let function_name = if functionid.contains('(') {
                functionid.split('(').next().unwrap_or(functionid).to_string()
            } else {
                functionid.clone()
            };
            
            // Check if this is a dependent function we need to check
            if dependent_function_names.contains(&function_name) {
                // Only report errors (not warnings for dependent functions)
                if let Some(level) = &result.level {
                    if level == "error" {
                        // Find the corresponding SqlObject for source file info
                        let source_info = all_file_objects.iter()
                            .find(|f| {
                                let obj_name = match &f.qualified_name.schema {
                                    Some(schema) => format!("{}.{}", schema, f.qualified_name.name),
                                    None => f.qualified_name.name.clone(),
                                };
                                obj_name == function_name && matches!(f.object_type, ObjectType::Function | ObjectType::Procedure)
                            });
                        
                        let error = PlpgsqlCheckError {
                            function_name: function_name.clone(),
                            source_file: source_info.and_then(|f| f.source_file.as_ref().map(|p| p.to_string_lossy().to_string())),
                            source_line: source_info.and_then(|f| calculate_source_line(f, result.lineno)),
                            check_result: result,
                        };
                        errors.push(error);
                    }
                }
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

/// Format and display plpgsql_check errors, sorted by severity (warnings first, then errors)
pub fn display_check_errors(errors: &[PlpgsqlCheckError]) {
    if errors.is_empty() {
        return;
    }
    
    println!("\n{}", "=== PL/pgSQL Check Results ===".bold().yellow());
    
    // Sort errors by level - warnings first, then errors
    let mut sorted_errors = errors.to_vec();
    sorted_errors.sort_by(|a, b| {
        let level_a = a.check_result.level.as_deref().unwrap_or("error");
        let level_b = b.check_result.level.as_deref().unwrap_or("error");
        
        // Define sort order: warning = 0, error = 1, other = 2
        let order_a = match level_a {
            "warning" => 0,
            "error" => 1,
            _ => 2,
        };
        let order_b = match level_b {
            "warning" => 0,
            "error" => 1,
            _ => 2,
        };
        
        order_a.cmp(&order_b)
    });
    
    for error in &sorted_errors {
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
    
    // Count warnings and errors
    let warnings = sorted_errors.iter().filter(|e| e.check_result.level.as_deref() == Some("warning")).count();
    let errors_count = sorted_errors.iter().filter(|e| e.check_result.level.as_deref() == Some("error")).count();
    
    // Display summary
    print!("\n{} ", sorted_errors.len().to_string().yellow().bold());
    if warnings > 0 && errors_count > 0 {
        print!("issues ({} warnings, {} errors) ", warnings, errors_count);
    } else if warnings > 0 {
        print!("warning{} ", if warnings == 1 { "" } else { "s" });
    } else if errors_count > 0 {
        print!("error{} ", if errors_count == 1 { "" } else { "s" });
    } else {
        print!("issue{} ", if sorted_errors.len() == 1 { "" } else { "s" });
    }
    println!("found by plpgsql_check");
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
    
}