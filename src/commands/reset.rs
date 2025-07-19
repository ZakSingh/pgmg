use std::io::{self, Write};
use crate::db::{StateManager, connection::{DatabaseConfig, connect_to_database}};
use owo_colors::OwoColorize;

#[derive(Debug)]
pub struct ResetResult {
    pub database_name: String,
}

pub async fn execute_reset(
    connection_string: String,
    force: bool,
) -> Result<ResetResult, Box<dyn std::error::Error>> {
    // Parse the target database configuration
    let target_config = DatabaseConfig::from_url(&connection_string)?;
    let database_name = target_config.database.clone();

    // Show warning and get confirmation (unless forced)
    if !force {
        if !confirm_reset(&database_name).await? {
            return Err("Reset operation cancelled by user".into());
        }
    }

    // Create admin connection (connect to 'postgres' database to manage target database)
    let admin_config = DatabaseConfig {
        database: "postgres".to_string(),
        ..target_config.clone()
    };

    println!("{} Connecting to PostgreSQL server...", "→".cyan());
    let (admin_client, admin_connection) = connect_to_database(&admin_config).await?;
    
    // Spawn connection handler
    admin_connection.spawn();

    // Step 1: Terminate active connections to the target database
    println!("{} Terminating active connections to database '{}'...", "→".cyan(), database_name);
    terminate_active_connections(&admin_client, &database_name).await?;

    // Step 2: Drop the database if it exists
    println!("{} Dropping database '{}'...", "→".cyan(), database_name);
    let drop_query = format!("DROP DATABASE IF EXISTS \"{}\"", database_name);
    admin_client.execute(&drop_query, &[]).await?;

    // Step 3: Create a fresh database
    println!("{} Creating fresh database '{}'...", "→".cyan(), database_name);
    let create_query = format!("CREATE DATABASE \"{}\"", database_name);
    admin_client.execute(&create_query, &[]).await?;

    // Step 4: Connect to the new database and initialize state tables
    println!("{} Initializing pgmg state tables...", "→".cyan());
    let (target_client, target_connection) = connect_to_database(&target_config).await?;
    
    // Spawn connection handler for target database
    target_connection.spawn();

    // Initialize pgmg state tables
    let state_manager = StateManager::new(&target_client);
    state_manager.initialize().await?;

    Ok(ResetResult { database_name })
}

async fn confirm_reset(database_name: &str) -> Result<bool, Box<dyn std::error::Error>> {
    println!();
    println!("{}", "⚠️  WARNING: DESTRUCTIVE OPERATION".red().bold());
    println!("{}", "⚠️  This will completely destroy the database and all its data!".red());
    println!("{} Database: {}", "⚠️  Target:".red(), database_name.yellow().bold());
    println!("{}", "⚠️  All tables, views, functions, data, and objects will be permanently lost!".red());
    println!("{}", "⚠️  Make sure you have a backup if you need to preserve any data.".red());
    println!();
    
    print!("{} ", "Type the database name to confirm:".bold());
    io::stdout().flush()?;
    
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();
    
    if input == database_name {
        println!("{} Proceeding with database reset...", "✓".green());
        Ok(true)
    } else {
        println!("{} Database name mismatch. Reset cancelled.", "✗".red());
        Ok(false)
    }
}

async fn terminate_active_connections(
    admin_client: &tokio_postgres::Client,
    database_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Query to terminate all connections to the target database
    let terminate_query = r#"
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = $1 AND pid <> pg_backend_pid()
    "#;
    
    let rows = admin_client.query(terminate_query, &[&database_name]).await?;
    
    if !rows.is_empty() {
        println!("{} Terminated {} active connection(s)", "→".cyan(), rows.len());
    }
    
    Ok(())
}

pub fn print_reset_summary(result: &ResetResult) {
    println!();
    println!("{} {}", "✅".green(), "Database reset completed successfully!".green().bold());
    println!("{} Database '{}' has been dropped and recreated", "→".cyan(), result.database_name.yellow());
    println!("{} pgmg state tables have been initialized", "→".cyan());
    println!();
    println!("{} The database is now ready for migrations and SQL objects", "💡".cyan());
}