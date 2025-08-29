use std::path::{Path, PathBuf};
use std::fs;
use std::time::{Duration, Instant};
use crate::db::{connect_with_url, TestDatabase};
use crate::sql::splitter::split_sql_file;
use owo_colors::OwoColorize;
// Manual TAP parsing implementation

#[derive(Debug)]
pub struct TestResult {
    pub tests_run: usize,
    pub tests_passed: usize,
    pub tests_failed: usize,
    pub tests_skipped: usize,
    pub test_files: Vec<TestFileResult>,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct TestFileResult {
    pub file_path: PathBuf,
    pub passed: bool,
    pub test_count: usize,
    pub passed_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub failures: Vec<TestFailure>,
    pub tap_output: String,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct TestFailure {
    pub test_number: usize,
    pub description: String,
    pub diagnostic: Option<String>,
    pub detailed_error: Option<String>,
    pub sql_context: Option<String>,
}

pub async fn execute_test(
    path: Option<PathBuf>,
    connection_string: String,
    tap_output: bool,
    quiet: bool,
    config: &crate::config::PgmgConfig,
) -> Result<TestResult, Box<dyn std::error::Error>> {
    execute_test_with_options(path, connection_string, tap_output, !quiet, quiet, config).await
}

pub async fn execute_test_with_options(
    path: Option<PathBuf>,
    connection_string: String,
    tap_output: bool,
    show_immediate_results: bool,
    quiet: bool,
    config: &crate::config::PgmgConfig,
) -> Result<TestResult, Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    // Discover test files
    let test_files = discover_test_files(path)?;
    
    if test_files.is_empty() {
        return Err("No test files found. Looking for files matching *.test.sql".into());
    }
    
    println!("{} Found {} test file(s)", "→".cyan(), test_files.len());
    
    // Create isolated test database using template for speed
    println!("{} Creating isolated test database...", "→".cyan());
    let test_db = TestDatabase::new_with_template(
        &connection_string,
        config.migrations_dir.clone(),
        config.code_dir.clone(),
        config,
    ).await?;
    println!("  {} Created test database: {}", "✓".green(), test_db.name);
    
    // Connect to test database
    let (client, connection) = connect_with_url(&test_db.connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();
    
    // Run tests in a block to ensure cleanup happens even on error
    let test_result = async {
        // Check if pgTAP is available
        check_pgtap_availability(&client).await?;
        
        let mut test_results = Vec::new();
        let mut total_passed = 0;
        let mut total_failed = 0;
        let mut total_skipped = 0;
        let mut total_run = 0;
        
        // Run each test file
        for test_file in test_files {
            // Display relative path from current directory
            let display_path = std::env::current_dir()
                .ok()
                .and_then(|cwd| test_file.strip_prefix(cwd).ok())
                .unwrap_or(&test_file);
            if !quiet {
                println!("\n{} Running {}", "→".cyan(), display_path.display().to_string().bright_blue());
            }
            
            let file_result = run_test_file(&client, &test_file, tap_output, quiet).await?;
            
            total_run += file_result.test_count;
            total_passed += file_result.passed_count;
            total_failed += file_result.failed_count;
            total_skipped += file_result.skipped_count;
            
            // Print immediate results if requested and not in quiet mode
            if show_immediate_results && !quiet {
                if file_result.passed {
                    println!("  {} {} tests passed", "✓".green(), file_result.test_count);
                } else {
                    println!("  {} {} tests failed", "✗".red(), file_result.failed_count);
                }
            }
            
            test_results.push(file_result);
            
            // Clean up any aborted transaction before next test file
            // This ensures each test file starts with a clean connection state
            let _ = client.simple_query("ROLLBACK").await;
        }
        
        Ok::<_, Box<dyn std::error::Error>>(TestResult {
            tests_run: total_run,
            tests_passed: total_passed,
            tests_failed: total_failed,
            tests_skipped: total_skipped,
            test_files: test_results,
            duration: start_time.elapsed(),
        })
    }.await;
    
    // Clean up test database regardless of test outcome
    println!("\n{} Cleaning up test database...", "→".cyan());
    if let Err(e) = test_db.cleanup().await {
        eprintln!("{} Failed to drop test database: {}", "Warning:".yellow(), e);
    } else {
        println!("  {} Test database dropped", "✓".green());
    }
    
    // Return the test result (propagating any errors)
    test_result
}

fn discover_test_files(path: Option<PathBuf>) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let search_path = match path {
        Some(p) => p,
        None => {
            // When no path is specified (--all flag), search entire project from current directory
            PathBuf::from(".")
        }
    };
    
    let mut test_files = Vec::new();
    
    if search_path.is_file() {
        // Single file specified
        if search_path.extension().and_then(|s| s.to_str()) == Some("sql") 
            && search_path.file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.contains(".test."))
                .unwrap_or(false) {
            test_files.push(search_path);
        }
    } else if search_path.is_dir() {
        // Directory - search recursively for .test.sql files
        find_test_files_recursive(&search_path, &mut test_files)?;
    }
    
    // Sort files for consistent ordering
    test_files.sort();
    
    Ok(test_files)
}

fn find_test_files_recursive(dir: &Path, test_files: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let entries = fs::read_dir(dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            find_test_files_recursive(&path, test_files)?;
        } else if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if file_name.ends_with(".test.sql") {
                    test_files.push(path);
                }
            }
        }
    }
    
    Ok(())
}

async fn check_pgtap_availability(client: &tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // Check if pgTAP extension is available
    let query = "SELECT 1 FROM pg_available_extensions WHERE name = 'pgtap'";
    let rows = client.query(query, &[]).await?;
    
    if rows.is_empty() {
        return Err("pgTAP extension not available. Please install pgTAP: https://pgtap.org/".into());
    }
    
    Ok(())
}

async fn run_test_file(
    client: &tokio_postgres::Client,
    test_file: &Path,
    show_tap_output: bool,
    quiet: bool,
) -> Result<TestFileResult, Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    // Read test file content
    let test_content = fs::read_to_string(test_file)?;
    
    // For pgTAP tests, we need to run them without a transaction wrapper
    // because pgTAP manages its own transaction state
    
    // Create pgTAP extension if not exists
    match client.execute("CREATE EXTENSION IF NOT EXISTS pgtap", &[]).await {
        Ok(_) => {},
        Err(e) => {
            // If pgTAP is not available, return an error
            return Ok(TestFileResult {
                file_path: test_file.to_path_buf(),
                passed: false,
                test_count: 0,
                passed_count: 0,
                failed_count: 1,
                skipped_count: 0,
                failures: vec![TestFailure {
                    test_number: 0,
                    description: "pgTAP extension not available".to_string(),
                    diagnostic: Some(format!("Please install pgTAP extension: {}", e)),
                    detailed_error: None,
                    sql_context: None,
                }],
                tap_output: format!("# pgTAP extension error: {}", e),
                duration: start_time.elapsed(),
            });
        }
    }
    
    
    // First, check if the test content contains psql meta-commands
    if test_content.contains("\\set") || test_content.contains("\\pset") {
        return Ok(TestFileResult {
            file_path: test_file.to_path_buf(),
            passed: false,
            test_count: 0,
            passed_count: 0,
            failed_count: 1,
            skipped_count: 0,
            failures: vec![TestFailure {
                test_number: 0,
                description: "Test contains psql meta-commands".to_string(),
                diagnostic: Some("pgTAP tests should not contain \\set or \\pset commands when run through pgmg".to_string()),
                detailed_error: None,
                sql_context: None,
            }],
            tap_output: "# Error: Test contains psql meta-commands".to_string(),
            duration: start_time.elapsed(),
        });
    }
    
    // pgTAP tests need to be run in a specific way
    // We'll create a wrapper query that runs the test and collects all output
    let wrapped_test = format!(
        r#"-- Enable client_min_messages to see test output
SET client_min_messages TO 'INFO';

-- Run the test
{}
"#,
        test_content
    );
    
    let tap_output = match client.simple_query(&wrapped_test).await {
        Ok(results) => {
            // Collect TAP output from individual statements
            let mut output_lines = Vec::new();
            for result in results {
                match result {
                    tokio_postgres::SimpleQueryMessage::Row(row) => {
                        // pgTAP functions return single text columns
                        if row.len() > 0 {
                            if let Some(value) = row.get(0) {
                                output_lines.push(value.to_string());
                            }
                        }
                    }
                    tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {
                        // Command completed, continue
                    }
                    _ => {}
                }
            }
            output_lines.join("\n")
        }
        Err(e) => {
            // Extract detailed error information using the same formatting as apply command
            let detailed_error = if let Some(_pg_err) = e.as_db_error() {
                // We have a database error with details
                use crate::error::{extract_postgres_error_details, calculate_line_column};
                use owo_colors::OwoColorize;
                
                if let Some(details) = extract_postgres_error_details(&e) {
                    let mut output = format!("SQL Execution Error in {}", 
                        test_file.file_name().unwrap_or_default().to_string_lossy().red().bold());
                    
                    // Add the core error message prominently
                    output.push_str(&format!("\n  {}: {}", "Error".red().bold(), details.message));
                    
                    // If we have an error position, calculate it and show detailed context
                    if let Some(pos) = details.position {
                        // Calculate position in wrapped content
                        let (wrapped_line, col) = calculate_line_column(&wrapped_test, pos - 1);
                        
                        // The wrapper adds 4 lines before the actual test content
                        let wrapper_lines = 4;
                        
                        if wrapped_line > wrapper_lines {
                            let actual_line = wrapped_line - wrapper_lines;
                            
                            // Use our new SQL analysis function to show detailed context
                            if let Some(sql_analysis) = analyze_sql_error(&test_content, actual_line, col) {
                                output.push_str(&sql_analysis);
                            } else {
                                // Fallback to basic line display
                                output.push_str(&format!("\n  {} line {}, column {}", 
                                    "Error at".yellow(), 
                                    actual_line.to_string().yellow().bold(),
                                    col.to_string().yellow().bold()
                                ));
                                
                                if let Some(error_line) = test_content.lines().nth(actual_line - 1) {
                                    output.push_str(&format!("\n  → {}", error_line.red()));
                                    if col > 0 {
                                        output.push_str(&format!("\n    {}{}", " ".repeat(col - 1), "^".red().bold()));
                                    }
                                }
                            }
                        } else {
                            // Error is in the wrapper part
                            output.push_str(&format!("\n  {} SQL setup, line {}, column {}", 
                                "Error at".yellow(),
                                wrapped_line.to_string().yellow().bold(),
                                col.to_string().yellow().bold()
                            ));
                        }
                    } else {
                        // No position information available
                        output.push_str(&format!("\n  {}: Position information not available", "Note".yellow()));
                    }
                    
                    // Add additional PostgreSQL error details
                    if let Some(detail) = details.detail {
                        output.push_str(&format!("\n  {}: {}", "Detail".yellow(), detail));
                    }
                    
                    if let Some(hint) = details.hint {
                        output.push_str(&format!("\n  {}: {}", "Hint".green(), hint));
                    }
                    
                    output.push_str(&format!("\n  {}: {}", "File".dimmed(), test_file.display()));
                    output.push_str(&format!("\n  {}: {} ({})", "Code".dimmed(), details.code, details.severity));
                    
                    output
                } else {
                    e.to_string()
                }
            } else {
                e.to_string()
            };
            
            return Ok(TestFileResult {
                file_path: test_file.to_path_buf(),
                passed: false,
                test_count: 0,
                passed_count: 0,
                failed_count: 1,
                skipped_count: 0,
                failures: vec![TestFailure {
                    test_number: 0,
                    description: "SQL execution error - check the failing statement above".to_string(),
                    diagnostic: Some(e.to_string()),
                    detailed_error: Some(detailed_error),
                    sql_context: Some(test_content.clone()), // Store original test content
                }],
                tap_output: format!("# Test execution failed: {}", e),
                duration: start_time.elapsed(),
            });
        }
    };
    
    if show_tap_output {
        println!("{}", tap_output);
    }
    
    // Parse TAP output
    let parsed_results = parse_tap_output(&tap_output, quiet)?;
    
    let duration = start_time.elapsed();
    
    Ok(TestFileResult {
        file_path: test_file.to_path_buf(),
        passed: parsed_results.failures.is_empty(),
        test_count: parsed_results.test_count,
        passed_count: parsed_results.passed_count,
        failed_count: parsed_results.failed_count,
        skipped_count: parsed_results.skipped_count,
        failures: parsed_results.failures,
        tap_output,
        duration,
    })
}

struct ParsedTapResults {
    test_count: usize,
    passed_count: usize,
    failed_count: usize,
    skipped_count: usize,
    failures: Vec<TestFailure>,
}

fn parse_tap_output(tap_output: &str, quiet: bool) -> Result<ParsedTapResults, Box<dyn std::error::Error>> {
    let mut test_count = 0;
    let mut passed_count = 0;
    let mut failed_count = 0;
    let mut skipped_count = 0;
    let mut failures = Vec::new();
    
    let lines: Vec<&str> = tap_output.lines().collect();
    let mut i = 0;
    
    // Parse TAP output manually, capturing diagnostic information
    while i < lines.len() {
        let line = lines[i].trim();
        
        if line.is_empty() {
            i += 1;
            continue;
        }
        
        // Check for skipped tests first (handles both "ok N # SKIP" and "not ok N # SKIP")
        if line.contains("# SKIP") {
            test_count += 1;
            skipped_count += 1;
            let description = extract_test_description(line);
            if !quiet {
                println!("    {} {} {}", "↷".yellow(), "SKIP".yellow(), description.bright_black());
            }
        } else if line.starts_with("ok ") {
            test_count += 1;
            passed_count += 1;
            let description = extract_test_description(line);
            if !quiet && !description.is_empty() {
                println!("    {} {}", "✓".green(), description.bright_black());
            }
        } else if line.starts_with("not ok ") {
            test_count += 1;
            failed_count += 1;
            let description = extract_test_description(line);
            // Always show failures, even in quiet mode
            println!("    {} {}", "✗".red(), description.red());
            
            // Look ahead for diagnostic information
            let mut diagnostic_lines = Vec::new();
            let mut j = i + 1;
            
            while j < lines.len() {
                let next_line = lines[j].trim();
                if next_line.starts_with('#') {
                    // Capture diagnostic lines but skip the leading '#'
                    let diag_content = if next_line.len() > 1 {
                        next_line[1..].trim().to_string()
                    } else {
                        String::new()
                    };
                    
                    if !diag_content.is_empty() {
                        diagnostic_lines.push(diag_content);
                    }
                    j += 1;
                } else if next_line.starts_with("ok ") || next_line.starts_with("not ok ") || 
                         next_line.contains("# SKIP") || next_line.is_empty() {
                    // Next test or empty line, stop collecting diagnostics
                    break;
                } else {
                    j += 1;
                }
            }
            
            let diagnostic = if diagnostic_lines.is_empty() {
                None
            } else {
                Some(diagnostic_lines.join("\n"))
            };
            
            failures.push(TestFailure {
                test_number: test_count,
                description: description.clone(),
                diagnostic,
                detailed_error: None,
                sql_context: None,
            });
        }
        
        i += 1;
    }
    
    Ok(ParsedTapResults {
        test_count,
        passed_count,
        failed_count,
        skipped_count,
        failures,
    })
}

fn extract_test_description(line: &str) -> String {
    // Extract description from TAP line like "ok 1 - test description"
    if let Some(dash_pos) = line.find(" - ") {
        line[dash_pos + 3..].trim().to_string()
    } else if let Some(space_pos) = line.find(' ') {
        if let Some(second_space) = line[space_pos + 1..].find(' ') {
            line[space_pos + 1 + second_space + 1..].trim().to_string()
        } else {
            String::new()
        }
    } else {
        String::new()
    }
}

/// Find the SQL statement that caused an error and provide context
fn analyze_sql_error(test_content: &str, error_line: usize, error_col: usize) -> Option<String> {
    // Parse the test content into SQL statements
    let statements = match split_sql_file(test_content) {
        Ok(stmts) => stmts,
        Err(_) => return None,
    };
    
    // Find which statement contains the error line
    let mut statement_info = None;
    for (idx, stmt) in statements.iter().enumerate() {
        let stmt_start = stmt.start_line.unwrap_or(1);
        let stmt_end = stmt_start + stmt.sql.lines().count();
        
        if error_line >= stmt_start && error_line < stmt_end {
            statement_info = Some((idx + 1, stmt, stmt_start, stmt_end));
            break;
        }
    }
    
    if let Some((stmt_num, stmt, stmt_start, stmt_end)) = statement_info {
        let mut output = String::new();
        
        // Show the failing statement prominently
        output.push_str(&format!("\n  {}: Statement {} (lines {}-{})", 
            "Failing SQL".red().bold(), 
            stmt_num.to_string().yellow().bold(),
            stmt_start.to_string().yellow(),
            stmt_end.to_string().yellow()
        ));
        
        // Show the actual SQL statement with line numbers
        output.push_str(&format!("\n  {}", "Statement:".yellow().bold()));
        for (line_idx, line) in stmt.sql.lines().enumerate() {
            let actual_line_num = stmt_start + line_idx;
            let is_error_line = actual_line_num == error_line;
            
            if is_error_line {
                output.push_str(&format!("\n  → {:3}: {}", 
                    actual_line_num.to_string().yellow().bold(), 
                    line.red()
                ));
                // Add pointer to specific column if we have it
                if error_col > 0 {
                    let padding = format!("  → {:3}: ", actual_line_num);
                    output.push_str(&format!("\n  {}{}{}", 
                        " ".repeat(padding.len() + error_col - 1),
                        "^".red().bold(),
                        " error here".red().dimmed()
                    ));
                }
            } else {
                output.push_str(&format!("\n    {:3}: {}", 
                    actual_line_num.to_string().dimmed(), 
                    line.dimmed()
                ));
            }
        }
        
        // Show context around the statement
        let lines: Vec<&str> = test_content.lines().collect();
        let context_start = (stmt_start.saturating_sub(3)).max(1);
        let context_end = (stmt_end + 2).min(lines.len());
        
        if context_start < stmt_start || context_end > stmt_end {
            output.push_str(&format!("\n  {}", "Context:".yellow().bold()));
            for line_num in context_start..=context_end {
                if line_num >= stmt_start && line_num < stmt_end {
                    continue; // Skip lines we already showed above
                }
                
                if let Some(line) = lines.get(line_num - 1) {
                    output.push_str(&format!("\n    {:3}: {}", 
                        line_num.to_string().dimmed(), 
                        line.bright_black()
                    ));
                }
            }
        }
        
        Some(output)
    } else {
        // Fallback: show context around the error line
        let lines: Vec<&str> = test_content.lines().collect();
        let context_start = error_line.saturating_sub(3);
        let context_end = (error_line + 3).min(lines.len());
        
        let mut output = String::new();
        output.push_str(&format!("\n  {} line {}", "Error at".yellow().bold(), error_line.to_string().yellow().bold()));
        output.push_str(&format!("\n  {}", "Context:".yellow().bold()));
        
        for line_num in context_start..context_end {
            if let Some(line) = lines.get(line_num) {
                let actual_line_num = line_num + 1;
                if actual_line_num == error_line {
                    output.push_str(&format!("\n  → {:3}: {}", 
                        actual_line_num.to_string().yellow().bold(), 
                        line.red()
                    ));
                    if error_col > 0 {
                        let padding = format!("  → {:3}: ", actual_line_num);
                        output.push_str(&format!("\n  {}{}{}", 
                            " ".repeat(padding.len() + error_col - 1),
                            "^".red().bold(),
                            " error here".red().dimmed()
                        ));
                    }
                } else {
                    output.push_str(&format!("\n    {:3}: {}", 
                        actual_line_num.to_string().dimmed(), 
                        line.bright_black()
                    ));
                }
            }
        }
        
        Some(output)
    }
}

pub fn print_test_summary(result: &TestResult) {
    println!();
    println!("{}", "Test Summary".bold().bright_blue());
    println!("{}", "=".repeat(50).bright_black());
    
    // Overall results
    if result.tests_failed == 0 {
        println!("{} {} All tests passed!", "✅".green(), "SUCCESS".green().bold());
    } else {
        println!("{} {} Some tests failed", "❌".red(), "FAILURE".red().bold());
    }
    
    println!();
    println!("{} {} tests run", "→".cyan(), result.tests_run);
    println!("{} {} passed", "✓".green(), result.tests_passed);
    if result.tests_failed > 0 {
        println!("{} {} failed", "✗".red(), result.tests_failed);
    }
    if result.tests_skipped > 0 {
        println!("{} {} skipped", "↷".yellow(), result.tests_skipped);
    }
    println!("{} Test duration: {:.2?}", "⏱".bright_black(), result.duration);
    
    // Failed test details
    if result.tests_failed > 0 {
        println!();
        println!("{}", "Failed Tests:".red().bold());
        for file_result in &result.test_files {
            if !file_result.passed {
                // Display relative path from current directory
                let display_path = std::env::current_dir()
                    .ok()
                    .and_then(|cwd| file_result.file_path.strip_prefix(cwd).ok())
                    .unwrap_or(&file_result.file_path);
                println!("  {} {}", "📁".red(), display_path.display().to_string().red());
                
                for failure in &file_result.failures {
                    println!("    {} Test #{}: {}", "✗".red(), failure.test_number, failure.description);
                    
                    // Show detailed error if available (SQL execution errors)
                    if let Some(detailed_error) = &failure.detailed_error {
                        // The detailed error already includes formatting, so just print it with indentation
                        for line in detailed_error.lines() {
                            println!("      {}", line);
                        }
                    } else if let Some(diagnostic) = &failure.diagnostic {
                        // Show pgtap diagnostic information with proper formatting
                        println!("      {}: {}", "Diagnostic".yellow().bold(), "");
                        for diag_line in diagnostic.lines() {
                            if diag_line.trim().is_empty() {
                                continue;
                            }
                            
                            // Format specific pgtap diagnostic patterns
                            if diag_line.contains("Failed test") {
                                println!("        {}: {}", "Test".dimmed(), diag_line.replace("Failed test", "").trim().trim_matches('"').yellow());
                            } else if diag_line.contains("got:") || diag_line.contains("Got:") {
                                let got_value = diag_line.split(':').nth(1).unwrap_or("").trim();
                                println!("        {}: {}", "Got".red().bold(), got_value.red());
                            } else if diag_line.contains("expected:") || diag_line.contains("Expected:") {
                                let expected_value = diag_line.split(':').nth(1).unwrap_or("").trim();
                                println!("        {}: {}", "Expected".green().bold(), expected_value.green());
                            } else if diag_line.contains("DETAIL:") {
                                let detail = diag_line.replace("DETAIL:", "").trim().to_string();
                                println!("        {}: {}", "Detail".yellow(), detail);
                            } else if diag_line.contains("HINT:") {
                                let hint = diag_line.replace("HINT:", "").trim().to_string();
                                println!("        {}: {}", "Hint".green(), hint);
                            } else {
                                // Generic diagnostic line
                                println!("        {}", diag_line.bright_black());
                            }
                        }
                    }
                }
            }
        }
    }
    
    println!();
}