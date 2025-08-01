//! Example of using pgmg as a library to run migrations on server startup
//!
//! This example demonstrates how to integrate pgmg into your Rust server
//! to automatically apply database migrations when your application starts.

use pgmg::{PgmgConfig, apply_migrations};
use std::env;
use tracing::{info, error, warn};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with your preferred configuration
    // This example uses a simple format, but you can customize it
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();
    
    info!("Starting application");
    
    // Method 1: Load configuration from pgmg.toml file
    let config = match PgmgConfig::load_from_file() {
        Ok(Some(config)) => config,
        Ok(None) | Err(_) => {
            // Method 2: Create configuration programmatically
            PgmgConfig {
                connection_string: Some(env::var("DATABASE_URL")
                    .unwrap_or_else(|_| "postgresql://localhost/myapp".to_string())),
                migrations_dir: Some("migrations".into()),
                code_dir: Some("sql".into()),
                seed_dir: None,
                output_graph: None,
                development_mode: Some(false),
                emit_notify_events: Some(false),
                check_plpgsql: Some(true),
                tls: None,
            }
        }
    };
    
    // Apply migrations - all output goes through tracing
    info!("Applying database migrations");
    match apply_migrations(&config).await {
        Ok(result) => {
            info!(
                migrations = result.migrations_applied.len(),
                objects_created = result.objects_created.len(),
                objects_updated = result.objects_updated.len(),
                "Migrations completed successfully"
            );
            
            // Check for any PL/pgSQL issues
            if result.plpgsql_errors_found > 0 {
                error!(
                    errors = result.plpgsql_errors_found,
                    "PL/pgSQL errors found - please review your functions!"
                );
                // You might want to exit here depending on your requirements
                // std::process::exit(1);
            }
            if result.plpgsql_warnings_found > 0 {
                warn!(
                    warnings = result.plpgsql_warnings_found,
                    "PL/pgSQL warnings found"
                );
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to apply migrations");
            
            // In a production server, you probably want to exit if migrations fail
            std::process::exit(1);
        }
    }
    
    // Now start your server
    info!("Starting server on port 8080");
    // your_server::start().await?;
    
    Ok(())
}

// Example: Custom tracing configuration for production
#[allow(dead_code)]
fn init_production_tracing() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    
    // You could send logs to multiple destinations
    tracing_subscriber::registry()
        // Console output with custom formatting
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
        )
        // JSON output for structured logging (great for log aggregation)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
        )
        .init();
}

// Example: Using with custom error handling
#[allow(dead_code)]
async fn apply_with_detailed_error_handling() -> Result<(), Box<dyn std::error::Error>> {
    use pgmg::{apply_migrations_with_options, PgmgConfig};
    use std::path::PathBuf;
    
    let config = PgmgConfig::load_from_file()?.unwrap_or_else(|| {
        panic!("pgmg.toml not found - cannot start without configuration");
    });
    
    // Apply migrations with custom directories
    let result = apply_migrations_with_options(
        &config,
        Some(PathBuf::from("db/migrations")),
        Some(PathBuf::from("db/functions")),
    ).await?;
    
    // Log detailed results
    info!("Migration summary:");
    for migration in &result.migrations_applied {
        info!("  ✓ {}", migration);
    }
    for object in &result.objects_created {
        info!("  + {}", object);
    }
    for object in &result.objects_updated {
        info!("  ~ {}", object);
    }
    for object in &result.objects_deleted {
        info!("  - {}", object);
    }
    
    // Handle any errors that occurred during migration
    if !result.errors.is_empty() {
        error!("Errors during migration:");
        for err in &result.errors {
            error!("  {}", err);
        }
        return Err("Migration completed with errors".into());
    }
    
    Ok(())
}

// Example: Integration with tokio-tracing for async context
#[allow(dead_code)]
async fn server_with_tracing_context() -> Result<(), Box<dyn std::error::Error>> {
    use tracing::Instrument;
    use pgmg::{PgmgConfig, apply_migrations};
    
    let config = PgmgConfig::load_from_file()?.expect("Config required");
    
    // Create a span for the entire startup process
    let startup_span = tracing::info_span!("server_startup");
    
    async {
        // Apply migrations within the startup context
        let migration_span = tracing::info_span!("database_migration");
        apply_migrations(&config)
            .instrument(migration_span)
            .await?;
        
        // Start server
        let server_span = tracing::info_span!("http_server");
        async {
            info!("Server starting on 0.0.0.0:8080");
            // your_server::run().await
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .instrument(server_span)
        .await
    }
    .instrument(startup_span)
    .await
}