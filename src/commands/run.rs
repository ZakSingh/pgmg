use std::path::PathBuf;
use std::process::Command;
use crate::config::PgmgConfig;
#[cfg(feature = "cli")]
use owo_colors::OwoColorize;

/// Execute a SQL file using psql
pub async fn execute_run(
    file: PathBuf,
    connection_string: String,
    _config: &PgmgConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if file exists
    if !file.exists() {
        return Err(format!("File not found: {}", file.display()).into());
    }
    
    // Check if it's a file (not a directory)
    if !file.is_file() {
        return Err(format!("Not a file: {}", file.display()).into());
    }
    
    // Display file being run
    #[cfg(feature = "cli")]
    println!("{} Running: {} (via psql)", "→".cyan(), file.display().to_string().bright_blue());
    #[cfg(not(feature = "cli"))]
    println!("→ Running: {} (via psql)", file.display());
    println!();
    
    // Check if psql is available
    match Command::new("psql").arg("--version").output() {
        Ok(_) => {},
        Err(_) => {
            return Err("psql not found. Please ensure PostgreSQL client tools are installed.".into());
        }
    }
    
    // Execute using psql with the connection string
    let mut cmd = Command::new("psql");
    cmd.arg(&connection_string)
       .arg("-f")
       .arg(&file)
       .arg("-v")
       .arg("ON_ERROR_STOP=1");
    
    // Execute and stream output
    let status = cmd.status()?;
    
    if status.success() {
        #[cfg(feature = "cli")]
        println!("\n{} SQL file executed successfully", "✓".green().bold());
        #[cfg(not(feature = "cli"))]
        println!("\n✓ SQL file executed successfully");
        Ok(())
    } else {
        Err(format!("psql exited with status: {}", status).into())
    }
}

/// Library-friendly version of execute_run
pub async fn run_sql_file(
    file: PathBuf,
    config: &PgmgConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection_string = config.connection_string.clone()
        .ok_or("No database connection string configured")?;
    
    execute_run(file, connection_string, config).await
}