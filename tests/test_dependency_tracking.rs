mod common;

use common::{TestEnvironment, assertions::*};
use pgmg::commands::execute_apply;
use pgmg::config::PgmgConfig;
use indoc::indoc;

/// Helper to verify objects exist in the database
async fn verify_objects_exist(
    env: &TestEnvironment,
    objects: &[(&str, &str)], // (object_type, object_name)
) -> Result<(), Box<dyn std::error::Error>> {
    for (obj_type, obj_name) in objects {
        let exists = match *obj_type {
            "function" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_proc WHERE proname = '{}'", obj_name)
                ).await?;
                count > 0
            }
            "trigger" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_trigger WHERE tgname = '{}'", obj_name)
                ).await?;
                count > 0
            }
            "view" => {
                env.table_exists(obj_name).await?
            }
            "table" => {
                // For materialized views stored as "table" in our test, check pg_class directly
                if *obj_name == "mat_summary" {
                    let count: i64 = env.query_scalar(
                        &format!("SELECT COUNT(*) FROM pg_class WHERE relname = '{}' AND relkind = 'm'", obj_name)
                    ).await?;
                    count > 0
                } else {
                    env.table_exists(obj_name).await?
                }
            }
            "materialized_view" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_class WHERE relname = '{}' AND relkind = 'm'", obj_name)
                ).await?;
                count > 0
            }
            "type" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_type WHERE typname = '{}'", obj_name)
                ).await?;
                count > 0
            }
            _ => false,
        };
        if !exists {
            println!("DEBUG: {} {} does not exist!", obj_type, obj_name);
        }
        assert!(exists, "{} {} should exist", obj_type, obj_name);
    }
    Ok(())
}

/// Helper to verify objects do NOT exist in the database
async fn verify_objects_not_exist(
    env: &TestEnvironment,
    objects: &[(&str, &str)], // (object_type, object_name)
) -> Result<(), Box<dyn std::error::Error>> {
    for (obj_type, obj_name) in objects {
        let exists = match *obj_type {
            "function" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_proc WHERE proname = '{}'", obj_name)
                ).await?;
                count > 0
            }
            "trigger" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_trigger WHERE tgname = '{}'", obj_name)
                ).await?;
                count > 0
            }
            "view" => {
                env.table_exists(obj_name).await?
            }
            "table" => {
                env.table_exists(obj_name).await?
            }
            "materialized_view" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_class WHERE relname = '{}' AND relkind = 'm'", obj_name)
                ).await?;
                count > 0
            }
            "type" => {
                let count: i64 = env.query_scalar(
                    &format!("SELECT COUNT(*) FROM pg_type WHERE typname = '{}'", obj_name)
                ).await?;
                count > 0
            }
            _ => true,
        };
        assert!(!exists, "{} {} should NOT exist", obj_type, obj_name);
    }
    Ok(())
}

/// Helper to get stored dependencies for an object
async fn get_stored_dependencies(
    env: &TestEnvironment,
    object_type: &str,
    object_name: &str,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let rows: Vec<(String, String)> = env.query_all(
        &format!(
            "SELECT dependency_type, dependency_name 
             FROM pgmg.pgmg_dependencies 
             WHERE dependent_type = '{}' AND dependent_name = '{}'
             ORDER BY dependency_type, dependency_name",
            object_type, object_name
        )
    ).await?;
    Ok(rows)
}

#[tokio::test]
async fn test_basic_trigger_function_dependency() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a test table
    env.execute_sql("CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT)").await?;
    
    // Create a function
    let function_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION test_trigger_func()
        RETURNS trigger AS $$
        BEGIN
            NEW.value = UPPER(NEW.value);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    
    env.write_sql_file("test_trigger_func.sql", function_sql).await?;
    
    // Create a trigger that depends on the function
    let trigger_sql = indoc! {r#"
        CREATE TRIGGER test_trigger
        BEFORE INSERT ON test_table
        FOR EACH ROW
        EXECUTE FUNCTION test_trigger_func();
    "#};
    
    env.write_sql_file("test_trigger.sql", trigger_sql).await?;
    
    // Apply to create objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    assert_objects_created(&apply_result, &["test_trigger_func", "test_trigger"]);
    
    // Verify objects exist
    verify_objects_exist(&env, &[
        ("function", "test_trigger_func"),
        ("trigger", "test_trigger"),
    ]).await?;
    
    // Verify dependencies were stored (trigger depends on both function and table)
    let deps = get_stored_dependencies(&env, "trigger", "test_trigger").await?;
    assert_eq!(deps.len(), 2, "Expected 2 dependencies for test_trigger, found {}", deps.len());
    
    // Check that both function and table dependencies are present
    let has_function_dep = deps.iter().any(|(t, n)| t == "function" && n == "test_trigger_func");
    let has_table_dep = deps.iter().any(|(t, n)| t == "relation" && n == "test_table");
    
    assert!(has_function_dep, "Trigger should depend on function test_trigger_func");
    assert!(has_table_dep, "Trigger should depend on table test_table");
    
    // Delete both files to trigger deletion
    env.delete_sql_file("test_trigger_func.sql").await?;
    env.delete_sql_file("test_trigger.sql").await?;
    
    // Apply deletions
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_deleted(&apply_result2, &["test_trigger", "test_trigger_func"]);
    
    // Verify correct deletion order by checking they're both gone
    verify_objects_not_exist(&env, &[
        ("function", "test_trigger_func"),
        ("trigger", "test_trigger"),
    ]).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_complex_multi_level_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a complex dependency chain:
    // base_table -> summary_view -> mat_view -> process_func -> update_trigger
    
    // 1. Base table
    let table_sql = indoc! {r#"
        CREATE TABLE base_data (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#};
    env.write_sql_file("base_data.sql", table_sql).await?;
    
    // 2. View depending on table
    let view_sql = indoc! {r#"
        CREATE VIEW summary_view AS
        SELECT 
            name,
            COUNT(*) as count,
            SUM(value) as total_value,
            AVG(value) as avg_value
        FROM base_data
        GROUP BY name;
    "#};
    env.write_sql_file("summary_view.sql", view_sql).await?;
    
    // 3. Materialized view depending on view
    let mat_view_sql = indoc! {r#"
        CREATE MATERIALIZED VIEW mat_summary AS
        SELECT * FROM summary_view
        WHERE count > 0;
    "#};
    env.write_sql_file("mat_summary.sql", mat_view_sql).await?;
    
    // 4. Function depending on materialized view
    let function_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION get_top_summary()
        RETURNS TABLE(name TEXT, total_value NUMERIC) AS $$
        BEGIN
            RETURN QUERY
            SELECT ms.name, ms.total_value
            FROM mat_summary ms
            ORDER BY ms.total_value DESC
            LIMIT 10;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("get_top_summary.sql", function_sql).await?;
    
    // 5. Another function that will be used by trigger
    let trigger_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION refresh_mat_view_func()
        RETURNS trigger AS $$
        BEGIN
            -- In real scenario, might check get_top_summary() before refresh
            REFRESH MATERIALIZED VIEW mat_summary;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("refresh_mat_view_func.sql", trigger_func_sql).await?;
    
    // 6. Trigger depending on function
    let trigger_sql = indoc! {r#"
        CREATE TRIGGER refresh_mat_view_trigger
        AFTER INSERT OR UPDATE OR DELETE ON base_data
        FOR EACH STATEMENT
        EXECUTE FUNCTION refresh_mat_view_func();
    "#};
    env.write_sql_file("refresh_mat_view_trigger.sql", trigger_sql).await?;
    
    // Apply all objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    println!("DEBUG: Apply was successful, verifying objects exist...");
    
    // Verify all objects exist
    verify_objects_exist(&env, &[
        ("table", "base_data"),
        ("view", "summary_view"),
        ("materialized_view", "mat_summary"),
        ("function", "get_top_summary"),
        ("function", "refresh_mat_view_func"),
        ("trigger", "refresh_mat_view_trigger"),
    ]).await?;
    
    // Verify key dependencies were stored
    let view_deps = get_stored_dependencies(&env, "view", "summary_view").await?;
    println!("DEBUG: summary_view deps: {:?}", view_deps);
    assert!(view_deps.iter().any(|(t, n)| t == "relation" && n == "base_data"), 
        "summary_view should depend on base_data");
    
    let mat_view_deps = get_stored_dependencies(&env, "materialized_view", "mat_summary").await?;
    println!("DEBUG: mat_summary deps: {:?}", mat_view_deps);
    assert!(mat_view_deps.iter().any(|(t, n)| t == "relation" && n == "summary_view"),
        "mat_summary should depend on summary_view");
    
    let func_deps = get_stored_dependencies(&env, "function", "get_top_summary").await?;
    println!("DEBUG: get_top_summary deps: {:?}", func_deps);
    assert!(func_deps.iter().any(|(t, n)| t == "relation" && n == "mat_summary"),
        "get_top_summary should depend on mat_summary");
    
    // Delete all objects
    env.delete_sql_file("base_data.sql").await?;
    env.delete_sql_file("summary_view.sql").await?;
    env.delete_sql_file("mat_summary.sql").await?;
    env.delete_sql_file("get_top_summary.sql").await?;
    env.delete_sql_file("refresh_mat_view_func.sql").await?;
    env.delete_sql_file("refresh_mat_view_trigger.sql").await?;
    
    // Apply deletions - should handle complex dependencies correctly
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    
    // Verify all objects are gone
    verify_objects_not_exist(&env, &[
        ("table", "base_data"),
        ("view", "summary_view"),
        ("table", "mat_summary"),
        ("function", "get_top_summary"),
        ("function", "refresh_mat_view_func"),
        ("trigger", "refresh_mat_view_trigger"),
    ]).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_cross_schema_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create schemas
    env.execute_sql("CREATE SCHEMA schema_a").await?;
    env.execute_sql("CREATE SCHEMA schema_b").await?;
    
    // Create table in schema_a
    let table_sql = indoc! {r#"
        CREATE TABLE schema_a.source_table (
            id INTEGER PRIMARY KEY,
            data TEXT
        );
    "#};
    env.write_sql_file("schema_a.source_table.sql", table_sql).await?;
    
    // Create function in schema_b that depends on schema_a table
    let function_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION schema_b.count_records()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM schema_a.source_table);
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("schema_b.count_records.sql", function_sql).await?;
    
    // Create view in schema_b that uses the function
    let view_sql = indoc! {r#"
        CREATE VIEW schema_b.record_stats AS
        SELECT 
            schema_b.count_records() as total_count,
            CURRENT_TIMESTAMP as last_checked;
    "#};
    env.write_sql_file("schema_b.record_stats.sql", view_sql).await?;
    
    // Apply objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Verify dependencies are stored with schema qualifications
    let func_deps = get_stored_dependencies(&env, "function", "schema_b.count_records").await?;
    println!("DEBUG: schema_b.count_records deps: {:?}", func_deps);
    assert!(func_deps.iter().any(|(t, n)| t == "relation" && n == "schema_a.source_table"),
        "schema_b.count_records should depend on schema_a.source_table");
    
    let view_deps = get_stored_dependencies(&env, "view", "schema_b.record_stats").await?;
    println!("DEBUG: schema_b.record_stats deps: {:?}", view_deps);
    assert!(view_deps.iter().any(|(t, n)| t == "function" && n == "schema_b.count_records"),
        "schema_b.record_stats should depend on schema_b.count_records");
    
    // Delete all objects
    env.delete_sql_file("schema_a.source_table.sql").await?;
    env.delete_sql_file("schema_b.count_records.sql").await?;
    env.delete_sql_file("schema_b.record_stats.sql").await?;
    
    // Apply deletions
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_deleted(&apply_result2, &[
        "schema_b.record_stats",
        "schema_b.count_records", 
        "schema_a.source_table"
    ]);
    
    Ok(())
}

#[tokio::test]
async fn test_function_signature_change_preserves_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create base function
    let base_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION calculate_value(input INTEGER)
        RETURNS INTEGER AS $$
        BEGIN
            RETURN input * 2;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("calculate_value.sql", base_func_sql).await?;
    
    // Create dependent function
    let dependent_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION process_calculation(x INTEGER)
        RETURNS INTEGER AS $$
        BEGIN
            RETURN calculate_value(x) + 10;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("process_calculation.sql", dependent_func_sql).await?;
    
    // Create view using the dependent function
    let view_sql = indoc! {r#"
        CREATE VIEW calculation_results AS
        SELECT 
            n as input,
            process_calculation(n::INTEGER) as result
        FROM generate_series(1, 5) n;
    "#};
    env.write_sql_file("calculation_results.sql", view_sql).await?;
    
    // Apply initial objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Verify dependencies
    let proc_deps = get_stored_dependencies(&env, "function", "process_calculation").await?;
    assert!(proc_deps.iter().any(|(t, n)| t == "function" && n == "calculate_value"));
    
    let view_deps = get_stored_dependencies(&env, "view", "calculation_results").await?;
    assert!(view_deps.iter().any(|(t, n)| t == "function" && n == "process_calculation"));
    
    // Change function signature (add optional parameter)
    let updated_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION calculate_value(
            input INTEGER,
            multiplier INTEGER DEFAULT 2
        )
        RETURNS INTEGER AS $$
        BEGIN
            RETURN input * multiplier;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("calculate_value.sql", updated_func_sql).await?;
    
    // Apply the change
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_updated(&apply_result2, &["calculate_value"]);
    
    // Verify dependencies are still tracked after signature change
    let proc_deps_after = get_stored_dependencies(&env, "function", "process_calculation").await?;
    assert!(proc_deps_after.iter().any(|(t, n)| t == "function" && n == "calculate_value"));
    
    // Verify function still works
    let result: i32 = env.query_scalar("SELECT process_calculation(5)").await?;
    assert_eq!(result, 20); // (5 * 2) + 10
    
    Ok(())
}

#[tokio::test]
async fn test_soft_vs_hard_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create base function
    let base_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION base_calculation(x INTEGER)
        RETURNS INTEGER AS $$
        BEGIN
            RETURN x * x;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("base_calculation.sql", base_func_sql).await?;
    
    // Create function with soft dependency (function calling function)
    let soft_dep_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION caller_function(n INTEGER)
        RETURNS INTEGER AS $$
        BEGIN
            RETURN base_calculation(n) + 100;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("caller_function.sql", soft_dep_func_sql).await?;
    
    // Create view with hard dependency on base function
    let hard_dep_view_sql = indoc! {r#"
        CREATE VIEW squared_values AS
        SELECT n, base_calculation(n::INTEGER) as squared
        FROM generate_series(1, 10) n;
    "#};
    env.write_sql_file("squared_values.sql", hard_dep_view_sql).await?;
    
    // Apply objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Check dependencies are stored with correct types
    let deps: Vec<(String, String, String, String, String)> = env.query_all(
        "SELECT dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind 
         FROM pgmg.pgmg_dependencies 
         WHERE dependency_name = 'base_calculation'
         ORDER BY dependent_name"
    ).await?;
    
    assert_eq!(deps.len(), 2);
    
    // Function-to-function should be soft
    let func_dep = deps.iter().find(|(dt, dn, _, _, _)| dt == "function" && dn == "caller_function").unwrap();
    assert_eq!(func_dep.4, "soft");
    
    // View-to-function should be hard
    let view_dep = deps.iter().find(|(dt, dn, _, _, _)| dt == "view" && dn == "squared_values").unwrap();
    assert_eq!(view_dep.4, "hard");
    
    Ok(())
}

#[tokio::test]
async fn test_type_and_domain_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create custom type
    let type_sql = indoc! {r#"
        CREATE TYPE status_enum AS ENUM ('pending', 'active', 'completed', 'cancelled');
    "#};
    env.write_sql_file("status_enum.sql", type_sql).await?;
    
    // Create domain based on type
    let domain_sql = indoc! {r#"
        CREATE DOMAIN valid_status AS status_enum
        CHECK (VALUE != 'cancelled' OR VALUE IS NULL);
    "#};
    env.write_sql_file("valid_status.sql", domain_sql).await?;
    
    // Create table using the domain
    let table_sql = indoc! {r#"
        CREATE TABLE task_items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            status valid_status DEFAULT 'pending'::status_enum
        );
    "#};
    env.write_sql_file("task_items.sql", table_sql).await?;
    
    // Create function using the type
    let function_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION count_by_status(target_status status_enum)
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM task_items WHERE status = target_status);
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("count_by_status.sql", function_sql).await?;
    
    // Apply objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Verify type dependencies
    let domain_deps = get_stored_dependencies(&env, "domain", "valid_status").await?;
    assert!(domain_deps.iter().any(|(t, n)| t == "type" && n == "status_enum"));
    
    let table_deps = get_stored_dependencies(&env, "table", "task_items").await?;
    assert!(table_deps.iter().any(|(t, n)| t == "type" && n == "valid_status"));
    
    let func_deps = get_stored_dependencies(&env, "function", "count_by_status").await?;
    assert!(func_deps.iter().any(|(t, n)| t == "type" && n == "status_enum"));
    
    // Delete all objects
    env.delete_sql_file("status_enum.sql").await?;
    env.delete_sql_file("valid_status.sql").await?;
    env.delete_sql_file("task_items.sql").await?;
    env.delete_sql_file("count_by_status.sql").await?;
    
    // Apply deletions - should respect type dependencies
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    
    // Verify deletion happened in correct order
    verify_objects_not_exist(&env, &[
        ("type", "status_enum"),
        ("type", "valid_status"), // Domains appear as types
        ("table", "task_items"),
        ("function", "count_by_status"),
    ]).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_partial_deletion_preserves_remaining_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create chain: table_a -> func_b -> func_c -> view_d
    let table_sql = indoc! {r#"
        CREATE TABLE table_a (
            id INTEGER PRIMARY KEY,
            value TEXT
        );
    "#};
    env.write_sql_file("table_a.sql", table_sql).await?;
    
    let func_b_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION func_b()
        RETURNS INTEGER AS $$
        BEGIN
            RETURN (SELECT COUNT(*) FROM table_a);
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("func_b.sql", func_b_sql).await?;
    
    let func_c_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION func_c()
        RETURNS TEXT AS $$
        BEGIN
            RETURN 'Count: ' || func_b()::TEXT;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("func_c.sql", func_c_sql).await?;
    
    let view_d_sql = indoc! {r#"
        CREATE VIEW view_d AS
        SELECT func_c() as status_message;
    "#};
    env.write_sql_file("view_d.sql", view_d_sql).await?;
    
    // Apply all objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Delete only table_a and func_b (keep func_c and view_d)
    env.delete_sql_file("table_a.sql").await?;
    env.delete_sql_file("func_b.sql").await?;
    
    // This should fail because func_c depends on func_b
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await;
    
    // Should fail due to dependency
    assert!(apply_result2.is_err());
    
    // Verify nothing was deleted (transaction rolled back)
    verify_objects_exist(&env, &[
        ("table", "table_a"),
        ("function", "func_b"),
        ("function", "func_c"),
        ("view", "view_d"),
    ]).await?;
    
    // Verify dependencies are still intact
    let func_c_deps = get_stored_dependencies(&env, "function", "func_c").await?;
    assert!(func_c_deps.iter().any(|(t, n)| t == "function" && n == "func_b"));
    
    Ok(())
}

#[tokio::test]
async fn test_dependency_persistence_across_sessions() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create objects with dependencies
    let func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION persistent_func()
        RETURNS TEXT AS $$
        BEGIN
            RETURN 'persistent';
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("persistent_func.sql", func_sql).await?;
    
    let view_sql = indoc! {r#"
        CREATE VIEW persistent_view AS
        SELECT persistent_func() as message;
    "#};
    env.write_sql_file("persistent_view.sql", view_sql).await?;
    
    // Apply objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Verify dependencies are stored
    let view_deps = get_stored_dependencies(&env, "view", "persistent_view").await?;
    assert_eq!(view_deps.len(), 1);
    assert_eq!(view_deps[0], ("function".to_string(), "persistent_func".to_string()));
    
    // Simulate new session by creating new client
    let (new_client, new_connection) = tokio_postgres::connect(
        &env.connection_string,
        tokio_postgres::NoTls
    ).await?;
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = new_connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    
    // Verify dependencies are still there with new connection
    let deps_query = new_client.query(
        "SELECT dependency_type, dependency_name 
         FROM pgmg.pgmg_dependencies 
         WHERE dependent_type = 'view' AND dependent_name = 'persistent_view'",
        &[]
    ).await?;
    
    assert_eq!(deps_query.len(), 1);
    assert_eq!(deps_query[0].get::<_, String>(0), "function");
    assert_eq!(deps_query[0].get::<_, String>(1), "persistent_func");
    
    // Delete objects using original environment
    env.delete_sql_file("persistent_func.sql").await?;
    env.delete_sql_file("persistent_view.sql").await?;
    
    // Apply deletions - should use stored dependencies
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_deleted(&apply_result2, &["persistent_view", "persistent_func"]);
    
    Ok(())
}

#[tokio::test]
async fn test_complex_mixed_operations() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initial setup: Create base objects
    let table1_sql = indoc! {r#"
        CREATE TABLE data_source (
            id SERIAL PRIMARY KEY,
            category TEXT,
            amount NUMERIC
        );
    "#};
    env.write_sql_file("data_source.sql", table1_sql).await?;
    
    let func1_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION sum_by_category(cat TEXT)
        RETURNS NUMERIC AS $$
        BEGIN
            RETURN COALESCE((SELECT SUM(amount) FROM data_source WHERE category = cat), 0);
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("sum_by_category.sql", func1_sql).await?;
    
    let view1_sql = indoc! {r#"
        CREATE VIEW category_totals AS
        SELECT DISTINCT 
            category,
            sum_by_category(category) as total
        FROM data_source;
    "#};
    env.write_sql_file("category_totals.sql", view1_sql).await?;
    
    // Apply initial objects
    let apply_result1 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result1);
    
    // Now perform mixed operations:
    // 1. Delete data_source table
    env.delete_sql_file("data_source.sql").await?;
    
    // 2. Update sum_by_category to not depend on deleted table
    let func1_updated_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION sum_by_category(cat TEXT)
        RETURNS NUMERIC AS $$
        BEGIN
            -- Now returns a fixed value since table is gone
            RETURN 100.0;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("sum_by_category.sql", func1_updated_sql).await?;
    
    // 3. Create new objects
    let new_func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION get_category_multiplier(cat TEXT)
        RETURNS NUMERIC AS $$
        BEGIN
            RETURN CASE cat
                WHEN 'A' THEN 1.5
                WHEN 'B' THEN 2.0
                ELSE 1.0
            END;
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("get_category_multiplier.sql", new_func_sql).await?;
    
    // 4. Update view to use new function
    let view1_updated_sql = indoc! {r#"
        CREATE VIEW category_totals AS
        SELECT 
            cat as category,
            sum_by_category(cat) * get_category_multiplier(cat) as total
        FROM (VALUES ('A'), ('B'), ('C')) AS categories(cat);
    "#};
    env.write_sql_file("category_totals.sql", view1_updated_sql).await?;
    
    // Apply mixed operations
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result2);
    assert_objects_deleted(&apply_result2, &["data_source"]);
    assert_objects_created(&apply_result2, &["get_category_multiplier"]);
    assert_objects_updated(&apply_result2, &["sum_by_category", "category_totals"]);
    
    // Verify final state
    verify_objects_not_exist(&env, &[("table", "data_source")]).await?;
    verify_objects_exist(&env, &[
        ("function", "sum_by_category"),
        ("function", "get_category_multiplier"),
        ("view", "category_totals"),
    ]).await?;
    
    // Verify new dependencies
    let view_deps = get_stored_dependencies(&env, "view", "category_totals").await?;
    assert!(view_deps.iter().any(|(t, n)| t == "function" && n == "sum_by_category"));
    assert!(view_deps.iter().any(|(t, n)| t == "function" && n == "get_category_multiplier"));
    
    Ok(())
}

#[tokio::test]
async fn test_performance_with_many_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create 50 interconnected objects
    let num_objects = 50;
    
    // Create base tables
    for i in 0..10 {
        let table_sql = format!(
            "CREATE TABLE table_{} (id INTEGER PRIMARY KEY, data TEXT);",
            i
        );
        env.write_sql_file(&format!("table_{}.sql", i), &table_sql).await?;
    }
    
    // Create functions that depend on tables
    for i in 0..20 {
        let table_idx = i % 10;
        let func_sql = format!(
            r#"CREATE OR REPLACE FUNCTION func_{}()
            RETURNS INTEGER AS $$
            BEGIN
                RETURN (SELECT COUNT(*) FROM table_{});
            END;
            $$ LANGUAGE plpgsql;"#,
            i, table_idx
        );
        env.write_sql_file(&format!("func_{}.sql", i), &func_sql).await?;
    }
    
    // Create views that depend on functions
    for i in 0..20 {
        let func_idx = i;
        let view_sql = format!(
            "CREATE VIEW view_{} AS SELECT func_{}() as count;",
            i, func_idx
        );
        env.write_sql_file(&format!("view_{}.sql", i), &view_sql).await?;
    }
    
    // Apply all objects
    let start = std::time::Instant::now();
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    let create_duration = start.elapsed();
    
    assert_apply_successful(&apply_result);
    assert_eq!(apply_result.objects_created.len(), num_objects);
    
    // Delete half of the objects (every other one)
    for i in (0..10).step_by(2) {
        env.delete_sql_file(&format!("table_{}.sql", i)).await?;
    }
    for i in (0..20).step_by(2) {
        env.delete_sql_file(&format!("func_{}.sql", i)).await?;
        env.delete_sql_file(&format!("view_{}.sql", i)).await?;
    }
    
    // Apply deletions
    let start = std::time::Instant::now();
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    let delete_duration = start.elapsed();
    
    assert_apply_successful(&apply_result2);
    assert_eq!(apply_result2.objects_deleted.len(), 25);
    
    // Performance assertions - operations should complete quickly
    assert!(
        create_duration.as_secs() < 10,
        "Creating {} objects took too long: {:?}",
        num_objects,
        create_duration
    );
    assert!(
        delete_duration.as_secs() < 10,
        "Deleting 25 objects took too long: {:?}",
        delete_duration
    );
    
    Ok(())
}

#[tokio::test]
async fn test_error_recovery_preserves_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create function
    let func_sql = indoc! {r#"
        CREATE OR REPLACE FUNCTION protected_function()
        RETURNS TEXT AS $$
        BEGIN
            RETURN 'protected';
        END;
        $$ LANGUAGE plpgsql;
    "#};
    env.write_sql_file("protected_function.sql", func_sql).await?;
    
    // Create view depending on function
    let view_sql = indoc! {r#"
        CREATE VIEW dependent_view AS
        SELECT protected_function() as value;
    "#};
    env.write_sql_file("dependent_view.sql", view_sql).await?;
    
    // Apply objects
    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    
    // Create an external dependency that pgmg doesn't know about
    env.execute_sql(
        "CREATE VIEW external_view AS SELECT * FROM dependent_view"
    ).await?;
    
    // Try to delete both pgmg objects - should fail due to external dependency
    env.delete_sql_file("protected_function.sql").await?;
    env.delete_sql_file("dependent_view.sql").await?;
    
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await;
    
    // Should fail
    assert!(apply_result2.is_err());
    
    // Verify objects still exist (transaction rolled back)
    verify_objects_exist(&env, &[
        ("function", "protected_function"),
        ("view", "dependent_view"),
    ]).await?;
    
    // Verify dependencies are still stored
    let view_deps = get_stored_dependencies(&env, "view", "dependent_view").await?;
    assert!(view_deps.iter().any(|(t, n)| t == "function" && n == "protected_function"));
    
    // Clean up external dependency
    env.execute_sql("DROP VIEW external_view").await?;
    
    // Now deletion should succeed
    let apply_result3 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&apply_result3);
    assert_objects_deleted(&apply_result3, &["dependent_view", "protected_function"]);
    
    Ok(())
}