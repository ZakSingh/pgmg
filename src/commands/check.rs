use crate::db::connect_with_url;
use crate::plpgsql_check::{check_all_functions, is_plpgsql_check_available, PlpgsqlCheckError, display_check_errors};
use owo_colors::OwoColorize;
use std::time::Instant;

#[derive(Debug)]
pub struct CheckResult {
    pub functions_checked: usize,
    pub errors_found: usize,
    pub warnings_found: usize,
    pub check_errors: Vec<PlpgsqlCheckError>,
    pub duration: std::time::Duration,
}

pub async fn execute_check(
    connection_string: String,
    function_name: Option<String>,
    schemas: Option<Vec<String>>,
    errors_only: bool,
) -> Result<CheckResult, Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    // Connect to database
    let (client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();
    
    // Check if plpgsql_check is available first
    if !is_plpgsql_check_available(&client).await? {
        return Err("plpgsql_check extension is not installed. Please install it with: CREATE EXTENSION plpgsql_check;".into());
    }
    
    // Build schema filter
    let schema_filter = if let Some(ref schema_list) = schemas {
        if schema_list.is_empty() {
            Some(vec!["public".to_string()])
        } else {
            Some(schema_list.clone())
        }
    } else {
        // Default: check all user schemas (excluding pg_* and information_schema)
        None
    };
    
    // Use the new bulk query approach
    let all_results = check_all_functions(&client, schema_filter.as_deref(), function_name.as_deref()).await?;
    
    if all_results.is_empty() {
        return Ok(CheckResult {
            functions_checked: 0,
            errors_found: 0,
            warnings_found: 0,
            check_errors: vec![],
            duration: start_time.elapsed(),
        });
    }
    
    // Group results by function to count how many functions were checked
    let mut function_names = std::collections::HashSet::new();
    for result in &all_results {
        if let Some(functionid) = &result.functionid {
            function_names.insert(functionid.clone());
        }
    }
    
    let functions_checked = function_names.len();
    println!("{} Checking {} PL/pgSQL functions/procedures...", "→".cyan(), functions_checked.to_string().yellow());
    
    let mut all_errors = Vec::new();
    let mut errors_found = 0;
    let mut warnings_found = 0;
    
    // Process results
    for result in all_results {
        if let Some(level) = &result.level {
            // Count errors and warnings
            match level.as_str() {
                "error" => errors_found += 1,
                "warning" => warnings_found += 1,
                _ => {}
            }
            
            // Collect errors and warnings (unless errors_only mode)
            if level == "error" || (level == "warning" && !errors_only) {
                let function_name = result.functionid.as_deref().unwrap_or("unknown");
                let error = PlpgsqlCheckError {
                    function_name: function_name.to_string(),
                    source_file: None, // No source file info for direct checks
                    source_line: None,
                    check_result: result,
                };
                all_errors.push(error);
            }
        }
    }
    
    // Display progress
    if functions_checked > 0 && all_errors.is_empty() {
        println!("  {} All checks passed!", "✓".green().bold());
    }
    
    Ok(CheckResult {
        functions_checked,
        errors_found,
        warnings_found,
        check_errors: all_errors,
        duration: start_time.elapsed(),
    })
}

pub fn print_check_summary(result: &CheckResult) {
    // Display any errors found
    display_check_errors(&result.check_errors);
    
    println!();
    println!("{}", "Check Summary".bold().bright_blue());
    println!("{}", "=".repeat(50).bright_black());
    
    // Overall status
    if result.errors_found == 0 && result.warnings_found == 0 {
        println!("{} {} All checks passed!", "✅".green(), "SUCCESS".green().bold());
    } else if result.errors_found > 0 {
        println!("{} {} Issues found", "❌".red(), "FAILURE".red().bold());
    } else {
        println!("{} {} Warnings found", "⚠️ ".yellow(), "WARNING".yellow().bold());
    }
    
    println!();
    println!("{} {} functions/procedures checked", "→".cyan(), result.functions_checked);
    
    if result.errors_found > 0 {
        println!("{} {} errors", "✗".red(), result.errors_found.to_string().red().bold());
    }
    
    if result.warnings_found > 0 {
        println!("{} {} warnings", "⚠".yellow(), result.warnings_found.to_string().yellow().bold());
    }
    
    println!("{} Check duration: {:.2?}", "⏱".bright_black(), result.duration);
    println!();
}