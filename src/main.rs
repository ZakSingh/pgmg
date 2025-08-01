use tokio_postgres::NoTls;
use pgmg::{analyze_statement, filter_builtins, BuiltinCatalog, DependencyGraph};
use pgmg::cli::{Cli, Commands};
use pgmg::commands::{execute_plan, print_plan_summary, execute_apply, print_apply_summary, execute_watch, WatchConfig, execute_reset, print_reset_summary, execute_test, print_test_summary, execute_seed, print_seed_summary, execute_new, print_new_summary, execute_check, print_check_summary, execute_run};
use pgmg::config::PgmgConfig;
use pgmg::error::{PgmgError, Result};
use pgmg::logging;
use std::path::PathBuf;
use tracing::{debug, info, warn};
use color_eyre;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Parse CLI args first to get verbosity level
    let cli = Cli::parse_args();
    
    // Initialize logging and error handling
    // Verbosity: 0 = warn, 1 = info, 2 = debug, 3+ = trace
    let verbosity = cli.verbose.unwrap_or(0);
    if let Err(e) = logging::init(verbosity) {
        eprintln!("Failed to initialize logging: {}", e);
        std::process::exit(1);
    }
    
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
            let conn_str = merged_config.connection_string.clone()
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
        
        Commands::Status { migrations_dir, code_dir, connection_string, output_graph } => {
            logging::output::header("Checking Status");
            
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                output_graph,
            );
            
            // Require connection string
            let conn_str = merged_config.connection_string.clone()
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
            info!("Status check completed in {}", logging::format_duration(elapsed));
            
            print_plan_summary(&plan_result);
            Ok(())
        }
        
        Commands::Apply { migrations_dir, code_dir, connection_string, dev } => {
            logging::output::header("Applying Changes");
            
            // Merge CLI args with config file (no output_graph for apply)
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                None, // apply command doesn't use output_graph
            ).with_dev_mode(dev);
            
            // Log configuration
            if let Some(ref dir) = merged_config.migrations_dir {
                debug!("Migrations directory: {}", dir.display());
            }
            if let Some(ref dir) = merged_config.code_dir {
                debug!("Code directory: {}", dir.display());
            }
            if merged_config.development_mode.unwrap_or(false) {
                info!("Development mode enabled - NOTIFY events will be emitted");
            }
            
            // Require connection string
            let conn_str = merged_config.connection_string.clone()
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
                merged_config.migrations_dir.clone(),
                merged_config.code_dir.clone(),
                conn_str,
                &merged_config,
            ).await?;
            
            let elapsed = start.elapsed();
            info!("Apply completed in {}", logging::format_duration(elapsed));
            
            print_apply_summary(&apply_result);
            Ok(())
        }
        
        Commands::Migrate { migrations_dir, code_dir, connection_string, dev } => {
            logging::output::header("Migrating Database");
            
            // Merge CLI args with config file (no output_graph for migrate)
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                None, // migrate command doesn't use output_graph
            ).with_dev_mode(dev);
            
            // Log configuration
            if let Some(ref dir) = merged_config.migrations_dir {
                debug!("Migrations directory: {}", dir.display());
            }
            if let Some(ref dir) = merged_config.code_dir {
                debug!("Code directory: {}", dir.display());
            }
            if merged_config.development_mode.unwrap_or(false) {
                info!("Development mode enabled - NOTIFY events will be emitted");
            }
            
            // Require connection string
            let conn_str = merged_config.connection_string.clone()
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Warn if no directories specified
            if merged_config.migrations_dir.is_none() && merged_config.code_dir.is_none() {
                warn!("No migrations or code directory specified - nothing to migrate");
                return Ok(());
            }
            
            // Execute apply with progress tracking
            let start = std::time::Instant::now();
            let apply_result = execute_apply(
                merged_config.migrations_dir.clone(),
                merged_config.code_dir.clone(),
                conn_str,
                &merged_config,
            ).await?;
            
            let elapsed = start.elapsed();
            info!("Migration completed in {}", logging::format_duration(elapsed));
            
            print_apply_summary(&apply_result);
            Ok(())
        }
        
        Commands::Watch { migrations_dir, code_dir, connection_string, debounce_ms, no_auto_apply } => {
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                migrations_dir,
                code_dir,
                connection_string,
                None, // watch command doesn't use output_graph
            ).with_dev_mode(true);
            
            // Require connection string
            let conn_str = merged_config.connection_string.clone()
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Create watch configuration
            let watch_config = WatchConfig {
                migrations_dir: merged_config.migrations_dir.clone(),
                code_dir: merged_config.code_dir.clone(),
                connection_string: conn_str,
                debounce_duration: std::time::Duration::from_millis(debounce_ms),
                auto_apply: !no_auto_apply,
                pgmg_config: merged_config,
            };
            
            // Log configuration
            debug!("Connection: {}", watch_config.connection_string.replace(|c: char| c == ':' || c == '@', "*"));
            if let Some(ref dir) = watch_config.migrations_dir {
                debug!("Migrations directory: {}", dir.display());
            }
            if let Some(ref dir) = watch_config.code_dir {
                debug!("Code directory: {}", dir.display());
            }
            if watch_config.pgmg_config.development_mode.unwrap_or(false) {
                info!("Development mode enabled - NOTIFY events will be emitted");
            }
            debug!("Debounce: {}ms", debounce_ms);
            debug!("Auto-apply: {}", watch_config.auto_apply);
            
            execute_watch(watch_config).await
        }
        Commands::Reset { connection_string, force } => {
            logging::output::header("Database Reset");
            
            // Get connection string from CLI arg, config file, or environment
            let conn_str = connection_string
                .or_else(|| config_file.as_ref().and_then(|c| c.connection_string.clone()))
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Validate connection string format
            if !conn_str.starts_with("postgres://") && !conn_str.starts_with("postgresql://") {
                return Err(PgmgError::InvalidConnectionString(conn_str));
            }
            
            // Log configuration (with masked credentials)
            debug!("Connection: {}", conn_str.replace(|c: char| c == ':' || c == '@', "*"));
            debug!("Force mode: {}", force);
            
            // Execute reset
            let result = execute_reset(conn_str, force).await
                .map_err(|e| PgmgError::Other(format!("Reset failed: {}", e)))?;
            
            print_reset_summary(&result);
            Ok(())
        }
        Commands::Test { path, connection_string, tap_output, quiet, all } => {
            logging::output::header("Running pgTAP Tests");
            
            // Get connection string from CLI arg, config file, or environment
            let conn_str = connection_string
                .or_else(|| config_file.as_ref().and_then(|c| c.connection_string.clone()))
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Validate connection string format
            if !conn_str.starts_with("postgres://") && !conn_str.starts_with("postgresql://") {
                return Err(PgmgError::InvalidConnectionString(conn_str));
            }
            
            // Handle --all flag
            let test_path = if all {
                if path.is_some() {
                    return Err(PgmgError::Configuration(
                        "Cannot specify both PATH and --all flag".to_string()
                    ));
                }
                None // Will search entire project
            } else {
                path
            };
            
            // Log configuration (with masked credentials)
            debug!("Connection: {}", conn_str.replace(|c: char| c == ':' || c == '@', "*"));
            debug!("Test path: {:?}", test_path);
            debug!("TAP output: {}", tap_output);
            debug!("Run all tests: {}", all);
            
            // Merge config for test command
            let merged_config = PgmgConfig::merge_with_cli(
                config_file,
                None, // test command doesn't override migrations_dir
                None, // test command doesn't override code_dir
                Some(conn_str.clone()),
                None, // no output_graph for test
            );
            
            // Execute tests
            let result = execute_test(test_path, conn_str, tap_output, quiet, &merged_config).await
                .map_err(|e| PgmgError::Other(format!("Test execution failed: {}", e)))?;
            
            print_test_summary(&result);
            
            // Exit with non-zero code if tests failed
            if result.tests_failed > 0 {
                std::process::exit(1);
            }
            
            Ok(())
        }
        
        Commands::Seed { seed_dir, connection_string } => {
            logging::output::header("Executing Seed Files");
            
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli_seed(
                config_file,
                seed_dir,
                connection_string,
            );
            
            // Require connection string
            let conn_str = merged_config.connection_string.clone()
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Validate connection string format
            if !conn_str.starts_with("postgres://") && !conn_str.starts_with("postgresql://") {
                return Err(PgmgError::InvalidConnectionString(conn_str));
            }
            
            // Require seed directory
            let seed_directory = merged_config.seed_dir
                .ok_or_else(|| PgmgError::Configuration(
                    "No seed directory provided. Use --seed-dir or specify seed_dir in pgmg.toml".to_string()
                ))?;
            
            // Validate seed directory exists
            if !seed_directory.exists() {
                return Err(PgmgError::Configuration(
                    format!("Seed directory does not exist: {}", seed_directory.display())
                ));
            }
            
            if !seed_directory.is_dir() {
                return Err(PgmgError::Configuration(
                    format!("Seed path is not a directory: {}", seed_directory.display())
                ));
            }
            
            // Log configuration (with masked credentials)
            debug!("Connection: {}", conn_str.replace(|c: char| c == ':' || c == '@', "*"));
            debug!("Seed directory: {}", seed_directory.display());
            
            // Execute seed with progress tracking
            let start = std::time::Instant::now();
            let result = execute_seed(seed_directory, conn_str).await
                .map_err(|e| PgmgError::Other(format!("Seed execution failed: {}", e)))?;
            
            let elapsed = start.elapsed();
            info!("Seed completed in {}", logging::format_duration(elapsed));
            
            print_seed_summary(&result);
            Ok(())
        }
        
        Commands::New { migrations_dir } => {
            logging::output::header("Creating New Migration");
            
            // Merge CLI args with config file
            let merged_config = PgmgConfig::merge_with_cli_new(
                config_file,
                migrations_dir,
            );
            
            // Log configuration
            if let Some(ref dir) = merged_config.migrations_dir {
                debug!("Migrations directory: {}", dir.display());
            }
            
            // Execute new migration creation
            let result = execute_new(
                merged_config.migrations_dir.clone(),
                &merged_config,
            ).await
                .map_err(|e| PgmgError::Other(format!("Migration creation failed: {}", e)))?;
            
            print_new_summary(&result);
            Ok(())
        }
        
        Commands::Check { function_name, connection_string, schema, errors_only } => {
            logging::output::header("Checking Functions with plpgsql_check");
            
            // Get connection string from CLI, env, or config
            let conn_str = connection_string
                .or(config_file.and_then(|c| c.connection_string))
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
            if let Some(ref schemas) = schema {
                debug!("Schemas: {:?}", schemas);
            }
            debug!("Errors only: {}", errors_only);
            
            // Execute check
            let result = execute_check(conn_str, function_name, schema, errors_only).await
                .map_err(|e| PgmgError::Other(format!("Check failed: {}", e)))?;
            
            print_check_summary(&result);
            
            // Exit with non-zero code if errors found
            if result.errors_found > 0 {
                std::process::exit(1);
            }
            
            Ok(())
        }
        
        Commands::Run { file, connection_string } => {
            logging::output::header("Running SQL File");
            
            // Get connection string from CLI, env, or config
            let conn_str = connection_string
                .or(config_file.as_ref().and_then(|c| c.connection_string.clone()))
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .ok_or_else(|| PgmgError::Configuration(
                    "No connection string provided. Use --connection-string, DATABASE_URL env var, or pgmg.toml".to_string()
                ))?;
            
            // Validate connection string format
            if !conn_str.starts_with("postgres://") && !conn_str.starts_with("postgresql://") {
                return Err(PgmgError::InvalidConnectionString(conn_str));
            }
            
            // Create a minimal config for execute_run
            let run_config = config_file.unwrap_or_default();
            
            // Execute the SQL file
            execute_run(file, conn_str, &run_config).await
                .map_err(|e| PgmgError::Other(format!("Run failed: {}", e)))?;
            
            Ok(())
        }
    }
}

// Keep the demo for testing, but adapt to new error handling
#[allow(dead_code)]
async fn demo_sql_analysis() -> Result<()> {
    // Connect to the database.
    let (client, connection) =
      tokio_postgres::connect("host=localhost user=postgres password=password dbname=postgres", NoTls)
        .await
        .map_err(|e| PgmgError::DatabaseConnection {
            message: "Failed to connect for demo".to_string(),
            source: e,
        })?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    
    // Load built-in catalog from the database
    let builtin_catalog = BuiltinCatalog::from_database(&client).await?;

    let sql = "create or replace function api.delete_parcel_template(
    p_template_id int,
    p_account_id  int
) returns void
    language plpgsql
    volatile as
$$
declare
    v_updated_count int;
begin
    update parcel_template
    set deleted_at = now()
    where parcel_template_id = p_template_id
      and account_id = p_account_id;

    get diagnostics v_updated_count = row_count;

    if v_updated_count = 0 then
        raise exception no_data_found using message = 'Parcel template not found or access denied';
    end if;
end;
$$;";


    // Analyze the SQL statement
    let dependencies = analyze_statement(sql)?;
    
    println!("Raw dependencies (including built-ins):");
    println!("Relations: {:?}", dependencies.relations);
    println!("Functions: {:?}", dependencies.functions);
    println!("Types: {:?}", dependencies.types);
    
    // Filter out built-ins
    let filtered_deps = filter_builtins(dependencies, &builtin_catalog);
    
    println!("\nFiltered dependencies (excluding built-ins):");
    println!("Relations: {:?}", filtered_deps.relations);
    println!("Functions: {:?}", filtered_deps.functions);
    println!("Types: {:?}", filtered_deps.types);
    
    Ok(())
}

#[allow(dead_code)]
async fn generate_dependency_graph(
    code_dir: &PathBuf,
    output_path: &PathBuf,
) -> Result<()> {
    use std::fs;
    
    println!("  Scanning SQL files in: {:?}", code_dir);
    
    // For now, create a simple example graph since we don't have full file scanning yet
    // In a complete implementation, this would:
    // 1. Scan the code_dir for .sql files
    // 2. Parse each file and identify SQL objects
    // 3. Build the dependency graph
    // 4. Output to graphviz format
    
    // Create a demo dependency graph for illustration
    let mut demo_objects = Vec::new();
    
    // Create some example SQL objects to demonstrate the graph
    use pgmg::sql::{SqlObject, ObjectType, QualifiedIdent, Dependencies};
    use std::collections::HashSet;
    
    // Example: users table (no dependencies)
    let users_table = SqlObject::new(
        ObjectType::View, // Using View as a placeholder since we don't have Table type
        QualifiedIdent::from_name("users".to_string()),
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)".to_string(),
        Dependencies {
            relations: HashSet::new(),
            functions: HashSet::new(),
            types: HashSet::new(),
        },
        Some(code_dir.join("tables/users.sql")),
    );
    
    // Example: user_stats view (depends on users)
    let mut user_stats_deps = Dependencies {
        relations: HashSet::new(),
        functions: HashSet::new(),
        types: HashSet::new(),
    };
    user_stats_deps.relations.insert(QualifiedIdent::from_name("users".to_string()));
    
    let user_stats_view = SqlObject::new(
        ObjectType::View,
        QualifiedIdent::from_name("user_stats".to_string()),
        "CREATE VIEW user_stats AS SELECT COUNT(*) FROM users".to_string(),
        user_stats_deps,
        Some(code_dir.join("views/user_stats.sql")),
    );
    
    // Example: calculate_total function (depends on user_stats)
    let mut calc_total_deps = Dependencies {
        relations: HashSet::new(),
        functions: HashSet::new(),
        types: HashSet::new(),
    };
    calc_total_deps.relations.insert(QualifiedIdent::from_name("user_stats".to_string()));
    
    let calc_total_func = SqlObject::new(
        ObjectType::Function,
        QualifiedIdent::new(Some("api".to_string()), "calculate_total".to_string()),
        "CREATE FUNCTION api.calculate_total() RETURNS INT AS $$ SELECT COUNT(*) FROM user_stats $$ LANGUAGE SQL".to_string(),
        calc_total_deps,
        Some(code_dir.join("functions/calculate_total.sql")),
    );
    
    demo_objects.push(users_table);
    demo_objects.push(user_stats_view);
    demo_objects.push(calc_total_func);
    
    // Create a simple builtin catalog for filtering
    let builtin_catalog = BuiltinCatalog::new();
    
    // Build the dependency graph
    let graph = DependencyGraph::build_from_objects(&demo_objects, &builtin_catalog)?;
    
    // Generate Graphviz output
    let graphviz_output = graph.to_graphviz();
    
    // Write to file
    fs::write(output_path, graphviz_output)?;
    
    println!("  Generated graph with {} nodes and {} edges", 
             graph.node_count(), graph.edge_count());
    
    Ok(())
}