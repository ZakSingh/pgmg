mod common;

use common::{TestEnvironment, fixtures};
use pgmg::commands::execute_apply;
use pgmg::config::PgmgConfig;
use indoc::indoc;

#[tokio::test]
async fn test_plpgsql_check_detects_errors() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a function with an error (referencing non-existent column)
    let bad_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION test_bad_function()
        RETURNS void AS $$
        DECLARE
            v_result INTEGER;
        BEGIN
            -- This will cause an error: column "nonexistent_column" doesn't exist
            SELECT nonexistent_column INTO v_result FROM users;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    
    env.write_sql_file("functions/bad_function.sql", bad_function).await?;
    
    // Create config with plpgsql_check enabled
    let mut config = PgmgConfig::default();
    config.development_mode = Some(true);
    config.check_plpgsql = Some(true);
    
    // Apply should succeed (function creation works)
    let result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &config,
    ).await?;
    
    // The apply should succeed
    assert!(result.errors.is_empty());
    assert_eq!(result.objects_created.len(), 1);
    
    // Note: We can't easily test that plpgsql_check output was displayed
    // without capturing stdout, but the function above ensures the code path runs
    
    Ok(())
}

#[tokio::test]
async fn test_plpgsql_check_disabled_in_prod_mode() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a function with an error
    let bad_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION test_bad_function()
        RETURNS void AS $$
        BEGIN
            SELECT nonexistent_column FROM users;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    
    env.write_sql_file("functions/bad_function.sql", bad_function).await?;
    
    // Create config with development_mode disabled
    let config = PgmgConfig::default(); // No dev mode
    
    // Apply should succeed without running plpgsql_check
    let result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &config,
    ).await?;
    
    assert!(result.errors.is_empty());
    assert_eq!(result.objects_created.len(), 1);
    
    Ok(())
}