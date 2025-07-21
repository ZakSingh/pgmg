use crate::db::connect_with_url;
use crate::plpgsql_check::{check_function, is_plpgsql_check_available, PlpgsqlCheckError, display_check_errors};
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
    schemas: Option<Vec<String>>,
    errors_only: bool,
) -> Result<CheckResult, Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    // Connect to database
    let (mut client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();
    
    // Check if plpgsql_check is available first
    {
        let transaction = client.transaction().await?;
        if !is_plpgsql_check_available(&transaction).await? {
            return Err("plpgsql_check extension is not installed. Please install it with: CREATE EXTENSION plpgsql_check;".into());
        }
        transaction.rollback().await?;
    }
    
    // Build schema filter
    let schema_filter = if let Some(ref schema_list) = schemas {
        if schema_list.is_empty() {
            vec!["public".to_string()]
        } else {
            schema_list.clone()
        }
    } else {
        // Default: check all user schemas (excluding pg_* and information_schema)
        vec![]
    };
    
    // Query all user-defined functions and procedures written in PL/pgSQL
    // Exclude system functions, extension functions, and internal pgTAP functions
    let query = if schema_filter.is_empty() {
        // Check all user schemas
        "SELECT n.nspname, p.proname, p.prokind::text
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         JOIN pg_language l ON l.oid = p.prolang
         LEFT JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
         LEFT JOIN pg_extension e ON e.oid = d.refobjid
         WHERE n.nspname NOT LIKE 'pg_%' 
           AND n.nspname != 'information_schema'
           AND p.prokind IN ('f', 'p')
           AND l.lanname = 'plpgsql'
           AND e.extname IS NULL  -- Exclude functions installed by extensions
           AND p.proname NOT LIKE '\\_%'  -- Exclude functions starting with underscore (convention for internal)
         ORDER BY n.nspname, p.proname"
    } else {
        // Check specific schemas
        "SELECT n.nspname, p.proname, p.prokind::text
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         JOIN pg_language l ON l.oid = p.prolang
         LEFT JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
         LEFT JOIN pg_extension e ON e.oid = d.refobjid
         WHERE n.nspname = ANY($1)
           AND p.prokind IN ('f', 'p')
           AND l.lanname = 'plpgsql'
           AND e.extname IS NULL  -- Exclude functions installed by extensions
           AND p.proname NOT LIKE '\\_%'  -- Exclude functions starting with underscore (convention for internal)
         ORDER BY n.nspname, p.proname"
    };
    
    let rows = if schema_filter.is_empty() {
        client.query(query, &[]).await?
    } else {
        client.query(query, &[&schema_filter]).await?
    };
    
    let total_functions = rows.len();
    if total_functions == 0 {
        return Ok(CheckResult {
            functions_checked: 0,
            errors_found: 0,
            warnings_found: 0,
            check_errors: vec![],
            duration: start_time.elapsed(),
        });
    }
    
    println!("{} Checking {} PL/pgSQL functions/procedures...", "→".cyan(), total_functions.to_string().yellow());
    
    let mut all_errors = Vec::new();
    let mut functions_checked = 0;
    let mut errors_found = 0;
    let mut warnings_found = 0;
    
    // Check each function
    for row in rows {
        let schema: String = row.get(0);
        let name: String = row.get(1);
        let kind: String = row.get(2);  // prokind cast to text
        
        let qualified_name = format!("{}.{}", schema, name);
        let object_type = if kind == "f" { "function" } else { "procedure" };
        
        // Use a separate transaction for each function check
        let transaction = match client.transaction().await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("{}: Failed to start transaction for {} {}: {}", 
                    "Warning".yellow().bold(), 
                    object_type,
                    qualified_name.cyan(), 
                    e);
                continue;
            }
        };
        
        match check_function(&transaction, &qualified_name).await {
            Ok(results) => {
                functions_checked += 1;
                
                
                for result in results {
                    if let Some(level) = &result.level {
                        // Count errors and warnings
                        match level.as_str() {
                            "error" => errors_found += 1,
                            "warning" => warnings_found += 1,
                            _ => {}
                        }
                        
                        // Collect errors and warnings (unless errors_only mode)
                        if level == "error" || (level == "warning" && !errors_only) {
                            let error = PlpgsqlCheckError {
                                function_name: qualified_name.clone(),
                                source_file: None, // No source file info for direct checks
                                source_line: None,
                                check_result: result,
                            };
                            all_errors.push(error);
                        }
                    }
                }
            }
            Err(e) => {
                // Log but continue checking other functions
                eprintln!("{}: Failed to check {} {}: {}", 
                    "Warning".yellow().bold(), 
                    object_type,
                    qualified_name.cyan(), 
                    e);
            }
        }
        
        // Commit or rollback the transaction
        let _ = transaction.rollback().await;
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