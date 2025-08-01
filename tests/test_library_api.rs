mod common;

use common::{TestEnvironment, assertions::*, fixtures};
use pgmg::{apply_migrations, apply_migrations_with_options, PgmgConfig};
use indoc::indoc;
use tracing_subscriber;
use tracing::info;

fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

#[tokio::test]
async fn test_apply_migrations_library_api() -> Result<(), Box<dyn std::error::Error>> {
    init_test_tracing();
    
    let env = TestEnvironment::new().await?;
    
    // Create migration files
    env.write_migration("001_initial_schema", fixtures::migrations::INITIAL_SCHEMA).await?;
    env.write_migration("002_add_users", fixtures::migrations::ADD_USERS_TABLE).await?;
    
    // Create config
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: Some(env.migrations_dir.clone()),
        code_dir: Some(env.sql_dir.clone()),
        seed_dir: None,
        output_graph: None,
        development_mode: Some(false),
        emit_notify_events: Some(false),
        check_plpgsql: Some(false),
        tls: None,
    };
    
    // Execute apply - all output goes through tracing
    info!("Running test migration");
    let result = apply_migrations(&config).await?;
    
    // Verify result
    assert_apply_successful(&result);
    assert_migrations_applied(&result, &["001_initial_schema", "002_add_users"]);
    
    // Verify database state
    assert!(env.table_exists("schema_version").await?);
    assert!(env.table_exists("users").await?);
    
    Ok(())
}

#[tokio::test]
async fn test_apply_migrations_with_custom_directories() -> Result<(), Box<dyn std::error::Error>> {
    init_test_tracing();
    
    let env = TestEnvironment::new().await?;
    
    // Create custom directories
    let custom_migrations_dir = env.temp_dir.path().join("custom_migrations");
    let custom_code_dir = env.temp_dir.path().join("custom_code");
    std::fs::create_dir(&custom_migrations_dir)?;
    std::fs::create_dir(&custom_code_dir)?;
    
    // Write migration to custom dir
    let migration_path = custom_migrations_dir.join("001_custom.sql");
    std::fs::write(&migration_path, indoc! {r#"
        -- Migration: Create custom table
        CREATE TABLE custom_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
    "#})?;
    
    // Write function to custom code dir
    let function_path = custom_code_dir.join("custom_function.sql");
    std::fs::write(&function_path, indoc! {r#"
        CREATE OR REPLACE FUNCTION custom_function()
        RETURNS TEXT AS $$
        BEGIN
            RETURN 'custom';
        END;
        $$ LANGUAGE plpgsql;
    "#})?;
    
    // Config without directories set
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: None,
        code_dir: None,
        seed_dir: None,
        output_graph: None,
        development_mode: Some(false),
        emit_notify_events: Some(false),
        check_plpgsql: Some(false),
        tls: None,
    };
    
    // Apply with custom directories
    let result = apply_migrations_with_options(
        &config,
        Some(custom_migrations_dir),
        Some(custom_code_dir),
    ).await?;
    
    assert_apply_successful(&result);
    assert_eq!(result.migrations_applied.len(), 1);
    assert_eq!(result.objects_created.len(), 1);
    assert!(result.objects_created[0].contains("custom_function"));
    
    // Verify database state
    assert!(env.table_exists("custom_table").await?);
    assert!(env.function_exists("custom_function").await?);
    
    Ok(())
}

#[tokio::test]
async fn test_apply_migrations_handles_errors() -> Result<(), Box<dyn std::error::Error>> {
    init_test_tracing();
    
    let env = TestEnvironment::new().await?;
    
    // Create invalid migration
    env.write_migration("001_invalid", indoc! {r#"
        -- This will fail
        CREATE TABLE;
    "#}).await?;
    
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: Some(env.migrations_dir.clone()),
        code_dir: Some(env.sql_dir.clone()),
        seed_dir: None,
        output_graph: None,
        development_mode: Some(false),
        emit_notify_events: Some(false),
        check_plpgsql: Some(false),
        tls: None,
    };
    
    // This should fail
    let result = apply_migrations(&config).await;
    
    // The function might return Ok with errors in the result or Err
    match result {
        Ok(apply_result) => {
            // Check if there were errors during apply
            assert!(!apply_result.errors.is_empty() || apply_result.migrations_applied.is_empty());
        }
        Err(e) => {
            // Expected - migration failed
            info!("Migration failed as expected: {}", e);
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_apply_migrations_with_plpgsql_check() -> Result<(), Box<dyn std::error::Error>> {
    init_test_tracing();
    
    let env = TestEnvironment::new().await?;
    
    // First ensure plpgsql_check is available
    match env.execute_sql("CREATE EXTENSION IF NOT EXISTS plpgsql_check").await {
        Ok(_) => {},
        Err(_) => {
            info!("Skipping test - plpgsql_check extension not available");
            return Ok(());
        }
    }
    
    // Create function with warning
    env.write_sql_file("func_with_warning.sql", indoc! {r#"
        CREATE OR REPLACE FUNCTION func_with_warning(x integer)
        RETURNS integer AS $$
        DECLARE
            unused_var integer;
        BEGIN
            RETURN x + 1;
        END;
        $$ LANGUAGE plpgsql;
    "#}).await?;
    
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: Some(env.migrations_dir.clone()),
        code_dir: Some(env.sql_dir.clone()),
        seed_dir: None,
        output_graph: None,
        development_mode: Some(true),  // Enable development mode
        emit_notify_events: Some(false),
        check_plpgsql: Some(true),      // Enable plpgsql_check
        tls: None,
    };
    
    let result = apply_migrations(&config).await?;
    
    // Should succeed but report warnings
    assert_apply_successful(&result);
    assert!(result.plpgsql_warnings_found > 0);
    
    Ok(())
}

#[tokio::test]
async fn test_migration_result_details() -> Result<(), Box<dyn std::error::Error>> {
    init_test_tracing();
    
    let env = TestEnvironment::new().await?;
    
    // Create a migration and a function
    env.write_migration("001_init", indoc! {r#"
        CREATE TABLE test_table (id integer);
    "#}).await?;
    
    env.write_sql_file("test_func.sql", indoc! {r#"
        CREATE OR REPLACE FUNCTION test_func()
        RETURNS void AS $$
        BEGIN
            RETURN;
        END;
        $$ LANGUAGE plpgsql;
    "#}).await?;
    
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: Some(env.migrations_dir.clone()),
        code_dir: Some(env.sql_dir.clone()),
        seed_dir: None,
        output_graph: None,
        development_mode: Some(false),
        emit_notify_events: Some(false),
        check_plpgsql: Some(false),
        tls: None,
    };
    
    let result = apply_migrations(&config).await?;
    
    assert_apply_successful(&result);
    assert_eq!(result.migrations_applied.len(), 1);
    assert_eq!(result.objects_created.len(), 1);
    assert!(result.objects_created[0].contains("test_func"));
    assert_eq!(result.errors.len(), 0);
    
    Ok(())
}

#[tokio::test]
async fn test_migration_idempotency() -> Result<(), Box<dyn std::error::Error>> {
    init_test_tracing();
    
    let env = TestEnvironment::new().await?;
    
    env.write_migration("001_test", indoc! {r#"
        CREATE TABLE idempotent_test (id integer);
    "#}).await?;
    
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: Some(env.migrations_dir.clone()),
        code_dir: Some(env.sql_dir.clone()),
        seed_dir: None,
        output_graph: None,
        development_mode: Some(false),
        emit_notify_events: Some(false),
        check_plpgsql: Some(false),
        tls: None,
    };
    
    // First application
    let result1 = apply_migrations(&config).await?;
    assert_eq!(result1.migrations_applied.len(), 1);
    
    // Second application - should be no-op
    let result2 = apply_migrations(&config).await?;
    assert_eq!(result2.migrations_applied.len(), 0);
    assert_eq!(result2.objects_created.len(), 0);
    assert_eq!(result2.objects_updated.len(), 0);
    
    Ok(())
}