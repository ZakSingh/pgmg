use std::path::{Path, PathBuf};
use std::fs;
use crate::db::connect_with_url;
use crate::sql::splitter::split_sql_file;
use owo_colors::OwoColorize;
use tracing::{debug, info};

#[derive(Debug)]
pub struct SeedResult {
    pub files_processed: Vec<String>,
    pub total_statements: usize,
    pub errors: Vec<String>,
}

pub async fn execute_seed(
    seed_dir: PathBuf,
    connection_string: String,
) -> Result<SeedResult, Box<dyn std::error::Error>> {
    // Connect to database
    let (mut client, connection) = connect_with_url(&connection_string).await?;
    
    // Spawn connection handler
    connection.spawn();

    let mut result = SeedResult {
        files_processed: Vec::new(),
        total_statements: 0,
        errors: Vec::new(),
    };

    // Scan seed directory for .sql files
    let seed_files = scan_seed_files(&seed_dir)?;
    
    if seed_files.is_empty() {
        info!("No seed files found in directory: {}", seed_dir.display());
        return Ok(result);
    }

    info!("Found {} seed files to execute", seed_files.len());
    
    // Start transaction for all seed files
    let transaction = client.transaction().await?;
    
    let mut transaction_aborted = false;
    
    for seed_file in &seed_files {
        if transaction_aborted {
            break;
        }
        
        let file_name = seed_file.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
            
        debug!("Processing seed file: {}", file_name);
        
        match process_seed_file(&transaction, seed_file).await {
            Ok(statement_count) => {
                result.files_processed.push(file_name.to_string());
                result.total_statements += statement_count;
                println!("  {} Executed {}: {} statements", 
                    "✓".green().bold(),
                    file_name.cyan(),
                    statement_count.to_string().yellow()
                );
            }
            Err(e) => {
                let error_msg = format!("Failed to process {}: {}", file_name, e);
                result.errors.push(error_msg.clone());
                println!("  {} {}", "✗".red().bold(), error_msg.red());
                transaction_aborted = true;
            }
        }
    }
    
    // Commit or rollback transaction
    if result.errors.is_empty() {
        transaction.commit().await?;
        println!("{}", "All seed files executed successfully!".green().bold());
    } else {
        transaction.rollback().await?;
        eprintln!("{} {} {}", 
            "Rolled back due to".red().bold(), 
            result.errors.len().to_string().yellow(), 
            "errors:".red().bold()
        );
        for error in &result.errors {
            eprintln!("  {} {}", "-".red().bold(), error.red());
        }
        return Err("Seed operation failed - all changes rolled back".into());
    }

    Ok(result)
}

/// Scan the seed directory for .sql files and return them in alphanumeric order
fn scan_seed_files(seed_dir: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let entries = fs::read_dir(seed_dir)?;
    let mut sql_files = Vec::new();
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        // Only include .sql files (not directories or other files)
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("sql") {
            sql_files.push(path);
        }
    }
    
    // Sort files alphanumerically (lexicographic order)
    sql_files.sort();
    
    Ok(sql_files)
}

/// Process a single seed file by executing all its statements
async fn process_seed_file(
    client: &tokio_postgres::Transaction<'_>,
    file_path: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(file_path)?;
    
    // Split file into individual statements
    let statements = split_sql_file(&content)?;
    
    let mut statement_count = 0;
    
    for statement in statements {
        if !statement.sql.trim().is_empty() {
            client.execute(&statement.sql, &[]).await?;
            statement_count += 1;
        }
    }
    
    Ok(statement_count)
}

pub fn print_seed_summary(result: &SeedResult) {
    println!("\n{}", "=== PGMG Seed Summary ===".bold().blue());
    
    if !result.files_processed.is_empty() {
        println!("\n{}:", "Files Processed".bold().green());
        for file in &result.files_processed {
            println!("  {} {}", "✓".green().bold(), file.cyan());
        }
        
        println!("\n{}: {} files, {} total statements", 
            "Summary".bold(),
            result.files_processed.len().to_string().yellow(),
            result.total_statements.to_string().yellow()
        );
    }
    
    if !result.errors.is_empty() {
        println!("\n{}:", "Errors".bold().red());
        for error in &result.errors {
            println!("  {} {}", "✗".red().bold(), error.red());
        }
    }
    
    if result.files_processed.is_empty() && result.errors.is_empty() {
        println!("\n{}", "No seed files found or processed.".yellow());
    } else if result.errors.is_empty() {
        println!("\n{} {} {}", 
            "✓".green().bold(), 
            "Successfully executed".green().bold(), 
            format!("{} seed files", result.files_processed.len()).yellow()
        );
    } else {
        println!("\n{} {} {}", 
            "✗".red().bold(), 
            "Seed operation failed with".red().bold(), 
            format!("{} errors", result.errors.len()).yellow()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;

    #[test]
    fn test_scan_seed_files_empty_directory() {
        let temp_dir = tempdir().unwrap();
        let files = scan_seed_files(temp_dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_scan_seed_files_with_sql_files() {
        let temp_dir = tempdir().unwrap();
        
        // Create some test files
        fs::write(temp_dir.path().join("002_products.sql"), "-- Products").unwrap();
        fs::write(temp_dir.path().join("001_users.sql"), "-- Users").unwrap();
        fs::write(temp_dir.path().join("003_orders.sql"), "-- Orders").unwrap();
        fs::write(temp_dir.path().join("readme.txt"), "Not SQL").unwrap(); // Should be ignored
        
        let files = scan_seed_files(temp_dir.path()).unwrap();
        
        assert_eq!(files.len(), 3);
        
        // Check that files are sorted alphanumerically
        let file_names: Vec<&str> = files.iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        
        assert_eq!(file_names, vec!["001_users.sql", "002_products.sql", "003_orders.sql"]);
    }

    #[test]
    fn test_scan_seed_files_ignores_non_sql() {
        let temp_dir = tempdir().unwrap();
        
        // Create mixed files
        fs::write(temp_dir.path().join("seed.sql"), "SQL").unwrap();
        fs::write(temp_dir.path().join("readme.md"), "Markdown").unwrap();
        fs::write(temp_dir.path().join("script.sh"), "Shell").unwrap();
        
        let files = scan_seed_files(temp_dir.path()).unwrap();
        
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_name().unwrap().to_str().unwrap(), "seed.sql");
    }
}