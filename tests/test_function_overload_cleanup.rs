mod common;

use common::{TestEnvironment, assertions::*};
use pgmg::commands::{execute_plan, execute_apply};
use pgmg::config::PgmgConfig;
use indoc::indoc;

#[tokio::test]
async fn test_function_parameter_change_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Step 1: Create initial function with one parameter
    let initial_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION process_data(input_text TEXT)
        RETURNS TEXT
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN 'Processed: ' || input_text;
        END;
        $$;
    "#};
    
    env.write_sql_file("process_data.sql", initial_function).await?;
    
    // Apply the initial function
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    assert_objects_created(&apply_result, &["process_data"]);
    
    // Verify only one function exists
    let count = env.query_scalar::<i64>(
        "SELECT COUNT(*) FROM pg_proc WHERE proname = 'process_data'"
    ).await?;
    assert_eq!(count, 1, "Should have exactly one process_data function");
    
    // Step 2: Update function to add an optional parameter
    let updated_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION process_data(
            input_text TEXT,
            uppercase BOOLEAN DEFAULT false
        )
        RETURNS TEXT
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF uppercase THEN
                RETURN UPPER('Processed: ' || input_text);
            ELSE
                RETURN 'Processed: ' || input_text;
            END IF;
        END;
        $$;
    "#};
    
    env.write_sql_file("process_data.sql", updated_function).await?;
    
    // Apply the updated function
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_updated(&apply_result2, &["process_data"]);
    
    // Verify still only one function exists (old overload was cleaned up)
    let count_after = env.query_scalar::<i64>(
        "SELECT COUNT(*) FROM pg_proc WHERE proname = 'process_data'"
    ).await?;
    assert_eq!(count_after, 1, "Should still have exactly one process_data function after update");
    
    // Verify the new function works with both signatures
    let result1 = env.query_scalar::<String>(
        "SELECT process_data('test')"
    ).await?;
    assert_eq!(result1, "Processed: test");
    
    let result2 = env.query_scalar::<String>(
        "SELECT process_data('test', true)"
    ).await?;
    assert_eq!(result2, "PROCESSED: TEST");
    
    Ok(())
}

#[tokio::test]
async fn test_function_parameter_type_change_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Step 1: Create initial function with integer parameter
    let initial_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION calculate_tax(amount INTEGER)
        RETURNS NUMERIC
        LANGUAGE sql
        AS $$
            SELECT amount * 0.08;
        $$;
    "#};
    
    env.write_sql_file("calculate_tax.sql", initial_function).await?;
    
    // Apply the initial function
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    assert_objects_created(&apply_result, &["calculate_tax"]);
    
    // Step 2: Change parameter type to NUMERIC
    let updated_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION calculate_tax(amount NUMERIC)
        RETURNS NUMERIC
        LANGUAGE sql
        AS $$
            SELECT amount * 0.08;
        $$;
    "#};
    
    env.write_sql_file("calculate_tax.sql", updated_function).await?;
    
    // Apply the updated function
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_updated(&apply_result2, &["calculate_tax"]);
    
    // Verify only one function exists
    let count = env.query_scalar::<i64>(
        "SELECT COUNT(*) FROM pg_proc WHERE proname = 'calculate_tax'"
    ).await?;
    assert_eq!(count, 1, "Should have exactly one calculate_tax function");
    
    // Verify the function works with numeric input
    let result = env.query_scalar::<f64>(
        "SELECT calculate_tax(123.45)"
    ).await?;
    assert!((result - 9.876).abs() < 0.001);
    
    Ok(())
}

#[tokio::test]
async fn test_procedure_parameter_change_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a test table first
    env.execute_sql("CREATE TABLE test_log (id SERIAL PRIMARY KEY, message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)").await?;
    
    // Step 1: Create initial procedure
    let initial_procedure = indoc! {r#"
        CREATE OR REPLACE PROCEDURE log_message(msg TEXT)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO test_log (message) VALUES (msg);
        END;
        $$;
    "#};
    
    env.write_sql_file("log_message.sql", initial_procedure).await?;
    
    // Apply the initial procedure
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    assert_objects_created(&apply_result, &["log_message"]);
    
    // Step 2: Add optional parameter
    let updated_procedure = indoc! {r#"
        CREATE OR REPLACE PROCEDURE log_message(
            msg TEXT,
            severity TEXT DEFAULT 'INFO'
        )
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO test_log (message) VALUES ('[' || severity || '] ' || msg);
        END;
        $$;
    "#};
    
    env.write_sql_file("log_message.sql", updated_procedure).await?;
    
    // Apply the updated procedure
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_updated(&apply_result2, &["log_message"]);
    
    // Verify only one procedure exists
    let count = env.query_scalar::<i64>(
        "SELECT COUNT(*) FROM pg_proc WHERE proname = 'log_message' AND prokind = 'p'"
    ).await?;
    assert_eq!(count, 1, "Should have exactly one log_message procedure");
    
    // Test the procedure works
    env.execute_sql("CALL log_message('Test message')").await?;
    env.execute_sql("CALL log_message('Error occurred', 'ERROR')").await?;
    
    let messages: Vec<String> = env.query_all(
        "SELECT message FROM test_log ORDER BY id"
    ).await?;
    
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0], "[INFO] Test message");
    assert_eq!(messages[1], "[ERROR] Error occurred");
    
    Ok(())
}

#[tokio::test]
async fn test_aggregate_parameter_change_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create state transition function first
    let state_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION sum_product_state(state NUMERIC, value NUMERIC)
        RETURNS NUMERIC
        LANGUAGE sql
        AS $$
            SELECT COALESCE(state, 0) + value;
        $$;
    "#};
    
    env.write_sql_file("sum_product_state.sql", state_function).await?;
    
    // Step 1: Create initial aggregate
    let initial_aggregate = indoc! {r#"
        CREATE AGGREGATE sum_product(NUMERIC) (
            SFUNC = sum_product_state,
            STYPE = NUMERIC,
            INITCOND = '0'
        );
    "#};
    
    env.write_sql_file("sum_product.sql", initial_aggregate).await?;
    
    // Apply the initial setup
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Update state function to accept different type
    let updated_state_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION sum_product_state(state DOUBLE PRECISION, value DOUBLE PRECISION)
        RETURNS DOUBLE PRECISION
        LANGUAGE sql
        AS $$
            SELECT COALESCE(state, 0) + value;
        $$;
    "#};
    
    env.write_sql_file("sum_product_state.sql", updated_state_function).await?;
    
    // Step 2: Update aggregate to use DOUBLE PRECISION
    let updated_aggregate = indoc! {r#"
        CREATE AGGREGATE sum_product(DOUBLE PRECISION) (
            SFUNC = sum_product_state,
            STYPE = DOUBLE PRECISION,
            INITCOND = '0'
        );
    "#};
    
    env.write_sql_file("sum_product.sql", updated_aggregate).await?;
    
    // Apply the updates
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    
    // Verify only one aggregate exists
    let count = env.query_scalar::<i64>(
        "SELECT COUNT(*) FROM pg_proc WHERE proname = 'sum_product' AND prokind = 'a'"
    ).await?;
    assert_eq!(count, 1, "Should have exactly one sum_product aggregate");
    
    // Test the aggregate works with the new type
    let result = env.query_scalar::<f64>(
        "SELECT sum_product(value::double precision) FROM (VALUES (1.5), (2.5), (3.0)) AS t(value)"
    ).await?;
    assert!((result - 7.0).abs() < 0.001);
    
    Ok(())
}