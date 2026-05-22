use crate::db::{connect_with_url, scan_sql_files};
use crate::plpgsql_check::{check_all_functions, is_plpgsql_check_available, resolve_source_location, PlpgsqlCheckError, display_check_errors};
use crate::BuiltinCatalog;
use owo_colors::OwoColorize;
use std::path::PathBuf;
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
    code_dir: Option<PathBuf>,
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

    // Scan source files so we can map plpgsql_check's function-relative lineno
    // back to file:line. Best-effort — if scanning fails or the dir is missing,
    // we fall back to function-name-only locations.
    let source_objects = match code_dir.as_ref() {
        Some(dir) if dir.exists() => {
            let catalog = BuiltinCatalog::new();
            match scan_sql_files(dir, &catalog).await {
                Ok(objs) => objs,
                Err(e) => {
                    eprintln!("{} Failed to scan {}: {} — line numbers will be function-relative",
                        "warning:".yellow().bold(), dir.display(), e);
                    Vec::new()
                }
            }
        }
        _ => Vec::new(),
    };

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
    let (all_results, functions_checked) = check_all_functions(&client, schema_filter.as_deref(), function_name.as_deref()).await?;

    if functions_checked == 0 {
        return Ok(CheckResult {
            functions_checked: 0,
            errors_found: 0,
            warnings_found: 0,
            check_errors: vec![],
            duration: start_time.elapsed(),
        });
    }

    println!("{} Checking {} PL/pgSQL functions/procedures...", "→".cyan(), functions_checked.to_string().yellow());

    let mut all_errors = Vec::new();
    let mut errors_found = 0;
    let mut warnings_found = 0;

    // Process results. plpgsql_check emits levels like "warning extra",
    // "warning performance", "warning security" — match on prefix, not equality.
    for result in all_results {
        if let Some(level) = &result.level {
            let is_error = level.starts_with("error");
            let is_warning = level.starts_with("warning");

            if is_error {
                errors_found += 1;
            } else if is_warning {
                warnings_found += 1;
            }

            if is_error || (is_warning && !errors_only) {
                let function_name = result.functionid.as_deref().unwrap_or("unknown");
                let (source_file, source_line) = match &result.functionid {
                    Some(fid) => resolve_source_location(&source_objects, fid, result.lineno),
                    None => (None, None),
                };
                let error = PlpgsqlCheckError {
                    function_name: function_name.to_string(),
                    source_file,
                    source_line,
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