# Using pgmg as a Library

pgmg can be used as a library in your Rust applications to programmatically run database migrations. This is particularly useful for running migrations on application startup.

## Philosophy

When used as a library, pgmg follows Rust best practices:
- All output goes through the `tracing` crate
- Errors are returned as `Result` types with full context
- You control logging configuration and output format
- No direct stdout/stderr printing

## Adding pgmg to Your Project

Add pgmg to your `Cargo.toml`:

```toml
[dependencies]
pgmg = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
tracing = "0.1"
tracing-subscriber = "0.3"
```

## Basic Usage

```rust
use pgmg::{PgmgConfig, apply_migrations};
use tracing::{info, error};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing - you control the output format
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // Load configuration from pgmg.toml
    let config = PgmgConfig::load_from_file()?.expect("Config required");
    
    // Apply migrations - all output goes through tracing
    info!("Starting database migrations");
    let result = apply_migrations(&config).await?;
    
    info!(
        migrations = result.migrations_applied.len(),
        objects = result.objects_created.len(),
        "Migrations completed"
    );
    
    // Start your application
    Ok(())
}
```

## Configuration

### Loading from File

```rust
let config = PgmgConfig::load_from_file()?.expect("pgmg.toml required");
```

### Programmatic Configuration

```rust
use pgmg::PgmgConfig;
use std::env;

let config = PgmgConfig {
    connection_string: Some(env::var("DATABASE_URL")?),
    migrations_dir: Some("migrations".into()),
    code_dir: Some("sql".into()),
    seed_dir: None,
    output_graph: None,
    development_mode: Some(false),
    emit_notify_events: Some(false),
    check_plpgsql: Some(true),
    tls: None,
};
```

## Custom Directories

You can override the configured directories:

```rust
use pgmg::apply_migrations_with_options;
use std::path::PathBuf;

let result = apply_migrations_with_options(
    &config,
    Some(PathBuf::from("db/migrations")),
    Some(PathBuf::from("db/functions")),
).await?;
```

## Logging Configuration

pgmg uses the `tracing` crate for all output. You have full control over how logs are displayed:

### Simple Console Output

```rust
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();
```

### JSON Logging for Production

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

tracing_subscriber::registry()
    .with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
    )
    .init();
```

### Custom Format with File Information

```rust
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .with_file(true)
    .with_line_number(true)
    .with_thread_ids(true)
    .init();
```

## Error Handling

The `ApplyResult` struct provides detailed information about the migration process:

```rust
let result = apply_migrations(&config).await?;

// Check for errors during migration
if !result.errors.is_empty() {
    error!("Migration had errors:");
    for err in &result.errors {
        error!("  {}", err);
    }
}

// Check for PL/pgSQL issues
if result.plpgsql_errors_found > 0 {
    error!(
        "Found {} PL/pgSQL errors in functions",
        result.plpgsql_errors_found
    );
    // Consider exiting if functions are critical
}

if result.plpgsql_warnings_found > 0 {
    warn!(
        "Found {} PL/pgSQL warnings",
        result.plpgsql_warnings_found
    );
}
```

## Logging Levels

pgmg uses different tracing levels for different types of information:

- `ERROR`: Migration failures, PL/pgSQL errors
- `WARN`: PL/pgSQL warnings, non-critical issues
- `INFO`: Migration progress, summary information
- `DEBUG`: Detailed operation logs, individual object changes
- `TRACE`: Very detailed internal operations

## Complete Example

See the [server_startup.rs](examples/server_startup.rs) example for a complete implementation showing:
- Tracing configuration
- Error handling
- Integration with server startup
- Custom logging formats

## Important Notes

1. **No Direct Output**: When used as a library, pgmg doesn't print directly to stdout/stderr. All output goes through tracing.

2. **Error Context**: Errors include full context and can be formatted using the `%` formatter:
   ```rust
   error!(error = %e, "Migration failed");
   ```

3. **Structured Logging**: Use structured fields for better log analysis:
   ```rust
   info!(
       migrations = result.migrations_applied.len(),
       duration_ms = elapsed.as_millis(),
       "Migration completed"
   );
   ```

4. **Concurrent Migrations**: pgmg uses advisory locks to prevent concurrent migrations. If another process is running migrations, you'll get a clear error.

## Migration Output

During migration, pgmg logs:
- Each migration applied (INFO level)
- Each object created/updated/deleted (DEBUG level)
- Any errors with full PostgreSQL error details
- PL/pgSQL check results if enabled

Configure your tracing subscriber to control which levels you see.