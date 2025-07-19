use std::path::{Path, PathBuf};
use std::fs;
use std::time::{Duration, Instant};
use crate::db::connect_with_url;
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
}

pub async fn execute_test(
    path: Option<PathBuf>,
    connection_string: String,
    tap_output: bool,
    continue_on_failure: bool,
) -> Result<TestResult, Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    
    // Discover test files
    let test_files = discover_test_files(path)?;
    
    if test_files.is_empty() {
        return Err("No test files found. Looking for files matching *.test.sql".into());
    }
    
    println!("{} Found {} test file(s)", "→".cyan(), test_files.len());
    
    // Connect to database
    let (client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();
    
    // Check if pgTAP is available
    check_pgtap_availability(&client).await?;
    
    let mut test_results = Vec::new();
    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_skipped = 0;
    let mut total_run = 0;
    
    // Run each test file
    for test_file in test_files {
        println!("\n{} Running {}", "→".cyan(), test_file.display().to_string().bright_blue());
        
        let file_result = run_test_file(&client, &test_file, tap_output).await?;
        
        total_run += file_result.test_count;
        total_passed += file_result.passed_count;
        total_failed += file_result.failed_count;
        total_skipped += file_result.skipped_count;
        
        // Print immediate results
        if file_result.passed {
            println!("  {} {} tests passed", "✓".green(), file_result.test_count);
        } else {
            println!("  {} {} tests failed", "✗".red(), file_result.failed_count);
            if !continue_on_failure {
                test_results.push(file_result);
                break;
            }
        }
        
        test_results.push(file_result);
    }
    
    let duration = start_time.elapsed();
    
    Ok(TestResult {
        tests_run: total_run,
        tests_passed: total_passed,
        tests_failed: total_failed,
        tests_skipped: total_skipped,
        test_files: test_results,
        duration,
    })
}

fn discover_test_files(path: Option<PathBuf>) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let search_path = match path {
        Some(p) => p,
        None => {
            // Try common test directories
            let candidates = vec!["sql", "tests", "test", "."];
            let mut found_path = None;
            
            for candidate in candidates {
                let path = PathBuf::from(candidate);
                if path.exists() {
                    found_path = Some(path);
                    break;
                }
            }
            
            found_path.unwrap_or_else(|| PathBuf::from("."))
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
            return Ok(TestFileResult {
                file_path: test_file.to_path_buf(),
                passed: false,
                test_count: 0,
                passed_count: 0,
                failed_count: 1,
                skipped_count: 0,
                failures: vec![TestFailure {
                    test_number: 0,
                    description: "Test execution failed".to_string(),
                    diagnostic: Some(e.to_string()),
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
    let parsed_results = parse_tap_output(&tap_output)?;
    
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

fn parse_tap_output(tap_output: &str) -> Result<ParsedTapResults, Box<dyn std::error::Error>> {
    let mut test_count = 0;
    let mut passed_count = 0;
    let mut failed_count = 0;
    let mut skipped_count = 0;
    let mut failures = Vec::new();
    
    // Parse TAP output manually since we need simple parsing
    for line in tap_output.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        
        if line.starts_with("ok ") {
            test_count += 1;
            passed_count += 1;
            let description = extract_test_description(line);
            if !description.is_empty() {
                println!("    {} {}", "✓".green(), description.bright_black());
            }
        } else if line.starts_with("not ok ") {
            test_count += 1;
            failed_count += 1;
            let description = extract_test_description(line);
            println!("    {} {}", "✗".red(), description.red());
            failures.push(TestFailure {
                test_number: test_count,
                description: description.clone(),
                diagnostic: None,
            });
        } else if line.contains("# SKIP") {
            test_count += 1;
            skipped_count += 1;
            let description = extract_test_description(line);
            println!("    {} {} {}", "↷".yellow(), "SKIP".yellow(), description.bright_black());
        }
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
                println!("  {} {}", "📁".red(), file_result.file_path.display().to_string().red());
                for failure in &file_result.failures {
                    println!("    {} Test #{}: {}", "✗".red(), failure.test_number, failure.description);
                    if let Some(diagnostic) = &failure.diagnostic {
                        println!("      {}", diagnostic.bright_black());
                    }
                }
            }
        }
    }
    
    println!();
}