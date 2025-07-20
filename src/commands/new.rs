use std::path::PathBuf;
use std::fs;
use std::io::{self, Write};
use chrono::{Utc, DateTime};
use owo_colors::OwoColorize;
use crate::config::PgmgConfig;

#[derive(Debug)]
pub struct NewResult {
    pub migration_file: String,
    pub migration_path: PathBuf,
}

pub async fn execute_new(
    migrations_dir: Option<PathBuf>,
    config: &PgmgConfig,
) -> Result<NewResult, Box<dyn std::error::Error>> {
    // Get migrations directory
    let migrations_dir = migrations_dir
        .or_else(|| config.migrations_dir.clone())
        .unwrap_or_else(|| PathBuf::from("migrations"));

    // Ensure migrations directory exists
    if !migrations_dir.exists() {
        fs::create_dir_all(&migrations_dir)?;
        println!("{} Created migrations directory: {}", 
            "✓".green().bold(), 
            migrations_dir.display().to_string().cyan()
        );
    }

    // Prompt for migration name
    print!("Enter a name for the migration: ");
    io::stdout().flush()?;
    
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let migration_name = input.trim();
    
    if migration_name.is_empty() {
        return Err("Migration name cannot be empty".into());
    }

    // Validate migration name (only alphanumeric, underscores, and hyphens)
    if !migration_name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        return Err("Migration name can only contain alphanumeric characters, underscores, and hyphens".into());
    }

    // Generate timestamp
    let now: DateTime<Utc> = Utc::now();
    let timestamp = now.format("%Y%m%d%H%M%S").to_string();
    
    // Create migration filename
    let migration_filename = format!("{}_{}.sql", timestamp, migration_name);
    let migration_path = migrations_dir.join(&migration_filename);

    // Check if file already exists (very unlikely with timestamp, but good to check)
    if migration_path.exists() {
        return Err(format!("Migration file already exists: {}", migration_path.display()).into());
    }

    // Create empty migration file with helpful comment
    let migration_content = format!(
        "-- Migration: {}\n-- Created: {}\n\n-- Add your migration SQL here\n\n",
        migration_name,
        now.format("%Y-%m-%d %H:%M:%S UTC")
    );

    fs::write(&migration_path, migration_content)?;

    let result = NewResult {
        migration_file: migration_filename.clone(),
        migration_path: migration_path.clone(),
    };

    println!("{} Created migration: {}", 
        "✓".green().bold(), 
        migration_filename.cyan()
    );
    println!("  Path: {}", migration_path.display().to_string().dimmed());

    Ok(result)
}

pub fn print_new_summary(result: &NewResult) {
    println!("\n{}", "=== PGMG New Migration Summary ===".bold().blue());
    println!("\n{}:", "Migration Created".bold().green());
    println!("  {} {}", "File:".bold(), result.migration_file.cyan());
    println!("  {} {}", "Path:".bold(), result.migration_path.display().to_string().dimmed());
    println!("\n{} {}", 
        "✓".green().bold(), 
        "Migration file created successfully. You can now edit it and run 'pgmg apply' to apply the changes.".green()
    );
}