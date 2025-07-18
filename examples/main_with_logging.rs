use pgmg::{
    cli::{Cli, Commands},
    commands::{execute_apply, execute_plan, print_apply_summary, print_plan_summary},
    config::PgmgConfig,
    error::{PgmgError, Result},
    logging,
};
use tracing::{debug, info, warn};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Parse CLI args first to get verbosity level
    let cli = Cli::parse_args();
    
    // Initialize logging and error handling
    // Verbosity: 0 = warn, 1 = info, 2 = debug, 3+ = trace
    let verbosity = cli.verbose.unwrap_or(0);
    logging::init(verbosity)?;
    
    // Log startup
    info!("Starting pgmg v{}", env!("CARGO_PKG_VERSION"));
    debug!("Command: {:?}", cli.command);
    
    // Run the actual command
    if let Err(e) = run(cli).await {
        // Use the new error formatting
        logging::output::error(&pgmg::error::format_error_chain(&e));
        
        // Show suggestions if available
        if let Some(suggestion) = pgmg::error::suggest_fix(&e) {
            logging::output::info(&suggestion);
        }
        
        std::process::exit(1);
    }
    
    Ok(())
}

async fn run(cli: Cli) -> Result<()> {
    // Load configuration file if it exists
    let config_file = match PgmgConfig::load_from_file() {
        Ok(config) => {
            info!("Loaded configuration from pgmg.toml");
            config
        }
        Err(e) => {
            debug!("No configuration file found: {}", e);
            None
        }
    };

    match cli.command {
        Commands::Init => {
            logging::output::step("Generating sample configuration file...");
            
            PgmgConfig::write_sample_config()
                .map_err(|e| PgmgError::Configuration(format!("Failed to write config: {}", e)))?;
            
            logging::output::success("Created pgmg.toml.example - rename to pgmg.toml to use");
            Ok(())
        }
        
        Commands::Plan { migrations_dir, code_dir, connection_string, output_graph } => {
            logging::output::header("Planning Changes");
            
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                output_graph,
            );
            
            // Require connection string
            let conn_str = merged_config.connection_string
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Validate connection string format
            if !conn_str.starts_with("postgres://") && !conn_str.starts_with("postgresql://") {
                return Err(PgmgError::InvalidConnectionString(conn_str));
            }
            
            // Log configuration
            debug!("Connection: {}", conn_str.replace(|c: char| c == ':' || c == '@', "*"));
            if let Some(ref dir) = merged_config.migrations_dir {
                debug!("Migrations directory: {}", dir.display());
            }
            if let Some(ref dir) = merged_config.code_dir {
                debug!("Code directory: {}", dir.display());
            }
            
            // Execute plan with progress tracking
            let start = std::time::Instant::now();
            let plan_result = execute_plan(
                merged_config.migrations_dir,
                merged_config.code_dir,
                conn_str,
                merged_config.output_graph,
            ).await?;
            
            let elapsed = start.elapsed();
            info!("Planning completed in {}", logging::format_duration(elapsed));
            
            print_plan_summary(&plan_result);
            Ok(())
        }
        
        Commands::Apply { migrations_dir, code_dir, connection_string } => {
            logging::output::header("Applying Changes");
            
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                None,
            );
            
            // Require connection string
            let conn_str = merged_config.connection_string
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Warn if no directories specified
            if merged_config.migrations_dir.is_none() && merged_config.code_dir.is_none() {
                warn!("No migrations or code directory specified - nothing to apply");
                return Ok(());
            }
            
            // Execute apply with progress tracking
            let start = std::time::Instant::now();
            let apply_result = execute_apply(
                merged_config.migrations_dir,
                merged_config.code_dir,
                conn_str,
            ).await?;
            
            let elapsed = start.elapsed();
            info!("Apply completed in {}", logging::format_duration(elapsed));
            
            print_apply_summary(&apply_result);
            Ok(())
        }
        
        Commands::Watch { migrations_dir, code_dir, connection_string } => {
            logging::output::header("Watch Mode");
            
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                None,
            );
            
            // Require connection string
            let conn_str = merged_config.connection_string
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            logging::output::warning("Watch mode is not yet implemented");
            info!("Configuration:");
            info!("  Connection: {}", conn_str.replace(|c: char| c == ':' || c == '@', "*"));
            if let Some(ref dir) = merged_config.migrations_dir {
                info!("  Migrations: {}", dir.display());
            }
            if let Some(ref dir) = merged_config.code_dir {
                info!("  Code: {}", dir.display());
            }
            
            Err(PgmgError::Other("Watch mode not implemented".to_string()))
        }
    }
}

// Example of how to handle errors in specific contexts
#[allow(dead_code)]
async fn example_error_handling() -> Result<()> {
    use pgmg::error::ErrorContext;
    
    // Example: Reading a file with context
    let content = std::fs::read_to_string("some_file.sql")
        .map_err(|e| PgmgError::FileRead {
            path: "some_file.sql".into(),
            message: e.to_string(),
            source: e,
        })?;
    
    // Example: SQL parsing with file context
    let parsed = pg_query::parse(&content)
        .file_context("some_file.sql")?;
    
    // Example: Database operation with object context
    let result = some_database_operation()
        .await
        .object_context("view", "user_stats")?;
    
    Ok(())
}

async fn some_database_operation() -> std::result::Result<(), tokio_postgres::Error> {
    // Placeholder
    Ok(())
}