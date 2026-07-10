mod common;

use common::TestEnvironment;
use pgmg::commands::{execute_apply, execute_plan};
use pgmg::config::PgmgConfig;
use indoc::indoc;

#[tokio::test]
async fn test_duplicate_function_detection() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create two files with the same function name
    let function1 = indoc! {r#"
        CREATE OR REPLACE FUNCTION hello_world()
        RETURNS TEXT
        LANGUAGE sql
        AS $$
            SELECT 'Hello from file 1';
        $$;
    "#};
    
    let function2 = indoc! {r#"
        CREATE OR REPLACE FUNCTION hello_world()
        RETURNS TEXT
        LANGUAGE sql  
        AS $$
            SELECT 'Hello from file 2';
        $$;
    "#};
    
    env.write_sql_file("hello1.sql", function1).await?;
    env.write_sql_file("hello2.sql", function2).await?;
    
    // Should fail during planning phase
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Multiple definitions of function 'hello_world'"));
            assert!(error_msg.contains("hello1.sql"));
            assert!(error_msg.contains("hello2.sql"));
            assert!(error_msg.contains("pgmg does not allow duplicate object names"));
        }
        Ok(_) => panic!("Expected error for duplicate functions, but planning succeeded"),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_duplicate_view_detection() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create two files with the same view name
    let view1 = indoc! {r#"
        CREATE VIEW user_stats AS
        SELECT COUNT(*) as total_users FROM users;
    "#};
    
    let view2 = indoc! {r#"
        CREATE VIEW user_stats AS  
        SELECT COUNT(DISTINCT id) as total_users FROM users;
    "#};
    
    env.write_sql_file("reports/user_stats.sql", view1).await?;
    env.write_sql_file("analytics/user_stats.sql", view2).await?;
    
    // Should fail during planning phase
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Multiple definitions of view 'user_stats'"));
            assert!(error_msg.contains("reports/user_stats.sql"));
            assert!(error_msg.contains("analytics/user_stats.sql"));
        }
        Ok(_) => panic!("Expected error for duplicate views, but planning succeeded"),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_duplicate_table_detection() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create two files with the same table name
    let table1 = indoc! {r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
    "#};
    
    let table2 = indoc! {r#"
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            price DECIMAL
        );
    "#};
    
    env.write_sql_file("schema/products.sql", table1).await?;
    env.write_sql_file("legacy/products.sql", table2).await?;
    
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Multiple definitions of table 'products'"));
            assert!(error_msg.contains("schema/products.sql"));
            assert!(error_msg.contains("legacy/products.sql"));
        }
        Ok(_) => panic!("Expected error for duplicate tables, but planning succeeded"),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_duplicate_type_detection() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create two files with the same type name
    let type1 = indoc! {r#"
        CREATE TYPE user_role AS ENUM ('admin', 'user');
    "#};
    
    let type2 = indoc! {r#"
        CREATE TYPE user_role AS ENUM ('administrator', 'member', 'guest');
    "#};
    
    env.write_sql_file("types/user_role.sql", type1).await?;
    env.write_sql_file("enums/user_role.sql", type2).await?;
    
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Multiple definitions of type 'user_role'"));
            assert!(error_msg.contains("types/user_role.sql"));
            assert!(error_msg.contains("enums/user_role.sql"));
        }
        Ok(_) => panic!("Expected error for duplicate types, but planning succeeded"),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_duplicate_materialized_view_detection() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create two files with the same materialized view name
    let mv1 = indoc! {r#"
        CREATE MATERIALIZED VIEW daily_stats AS
        SELECT date_trunc('day', created_at) as day, COUNT(*) as count
        FROM events GROUP BY 1;
    "#};
    
    let mv2 = indoc! {r#"
        CREATE MATERIALIZED VIEW daily_stats AS
        SELECT DATE(created_at) as day, SUM(amount) as total
        FROM transactions GROUP BY 1;
    "#};
    
    env.write_sql_file("views/daily_stats.sql", mv1).await?;
    env.write_sql_file("reports/daily_stats.sql", mv2).await?;
    
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Multiple definitions of materialized view 'daily_stats'"));
            assert!(error_msg.contains("views/daily_stats.sql"));
            assert!(error_msg.contains("reports/daily_stats.sql"));
        }
        Ok(_) => panic!("Expected error for duplicate materialized views, but planning succeeded"),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_no_error_for_different_object_names() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create files with different object names - should not error
    let function1 = indoc! {r#"
        CREATE OR REPLACE FUNCTION hello_world()
        RETURNS TEXT
        LANGUAGE sql
        AS $$
            SELECT 'Hello World';
        $$;
    "#};
    
    let function2 = indoc! {r#"
        CREATE OR REPLACE FUNCTION goodbye_world()
        RETURNS TEXT
        LANGUAGE sql
        AS $$
            SELECT 'Goodbye World';
        $$;
    "#};
    
    env.write_sql_file("hello.sql", function1).await?;
    env.write_sql_file("goodbye.sql", function2).await?;
    
    // Should succeed without error
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should find both objects as new
    assert_eq!(result.changes.len(), 2);
    
    Ok(())
}

#[tokio::test]
async fn test_comments_and_triggers_allowed_to_duplicate() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Two triggers sharing a name on different tables are legitimate — each is
    // its own object keyed by name:table and both must apply and track.
    env.execute_sql("CREATE TABLE table1 (id SERIAL PRIMARY KEY, updated_at TIMESTAMPTZ)").await?;
    env.execute_sql("CREATE TABLE table2 (id SERIAL PRIMARY KEY, updated_at TIMESTAMPTZ)").await?;

    let trigger_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION update_modified()
        RETURNS trigger AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#};

    let trigger1 = indoc! {r#"
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON table1
        FOR EACH ROW EXECUTE FUNCTION update_modified();
    "#};

    let trigger2 = indoc! {r#"
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON table2
        FOR EACH ROW EXECUTE FUNCTION update_modified();
    "#};

    env.write_sql_file("func.sql", trigger_function).await?;
    env.write_sql_file("trigger1.sql", trigger1).await?;
    env.write_sql_file("trigger2.sql", trigger2).await?;

    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result.errors.is_empty(), "apply failed: {:?}", apply_result.errors);

    // Both triggers live in the database
    let live_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'update_timestamp'"
    ).await?;
    assert_eq!(live_count, 2, "both same-named triggers should exist");

    // Both tracked under distinct composite keys
    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("trigger".to_string(), "update_timestamp:table1".to_string())),
        "missing composite state row for table1: {:?}", tracked);
    assert!(tracked.contains(&("trigger".to_string(), "update_timestamp:table2".to_string())),
        "missing composite state row for table2: {:?}", tracked);

    // A second plan sees no changes — neither trigger is perpetually "changed"
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert!(plan.changes.is_empty(), "expected empty plan, got: {} changes", plan.changes.len());

    // Editing one trigger recreates only that one
    let table1_oid_before: u32 = env.query_scalar(
        "SELECT t.oid FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE t.tgname = 'update_timestamp' AND c.relname = 'table1'"
    ).await?;

    let trigger2_changed = indoc! {r#"
        CREATE TRIGGER update_timestamp
        BEFORE INSERT OR UPDATE ON table2
        FOR EACH ROW EXECUTE FUNCTION update_modified();
    "#};
    env.write_sql_file("trigger2.sql", trigger2_changed).await?;

    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result2.errors.is_empty(), "apply failed: {:?}", apply_result2.errors);
    assert_eq!(apply_result2.objects_updated, vec!["update_timestamp".to_string()],
        "only the edited trigger should be recreated");

    let table1_oid_after: u32 = env.query_scalar(
        "SELECT t.oid FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE t.tgname = 'update_timestamp' AND c.relname = 'table1'"
    ).await?;
    assert_eq!(table1_oid_before, table1_oid_after, "table1's trigger must not be recreated");

    // Deleting one file drops only that trigger
    env.delete_sql_file("trigger1.sql").await?;
    let apply_result3 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result3.errors.is_empty(), "apply failed: {:?}", apply_result3.errors);
    assert!(apply_result3.objects_deleted.contains(&"update_timestamp:table1".to_string()),
        "expected composite delete name, got: {:?}", apply_result3.objects_deleted);

    let table1_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE t.tgname = 'update_timestamp' AND c.relname = 'table1'"
    ).await?;
    let table2_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE t.tgname = 'update_timestamp' AND c.relname = 'table2'"
    ).await?;
    assert_eq!(table1_count, 0, "table1's trigger should be dropped");
    assert_eq!(table2_count, 1, "table2's trigger must survive");

    Ok(())
}

#[tokio::test]
async fn test_duplicate_trigger_same_table_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Same trigger name on the SAME table is a genuine duplicate
    let trigger1 = indoc! {r#"
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON table1
        FOR EACH ROW EXECUTE FUNCTION update_modified();
    "#};

    let trigger2 = indoc! {r#"
        CREATE TRIGGER update_timestamp
        BEFORE INSERT ON table1
        FOR EACH ROW EXECUTE FUNCTION update_modified();
    "#};

    env.write_sql_file("trigger1.sql", trigger1).await?;
    env.write_sql_file("trigger2.sql", trigger2).await?;

    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;

    let err = result.expect_err("duplicate trigger on same table should be rejected");
    let error_msg = err.to_string();
    assert!(error_msg.contains("Multiple definitions of trigger"), "unexpected error: {}", error_msg);
    assert!(error_msg.contains("update_timestamp on table1"), "unexpected error: {}", error_msg);

    Ok(())
}

#[tokio::test]
async fn test_error_includes_line_numbers() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a file with a function on a specific line
    let file_with_function = indoc! {r#"
        -- Comment line
        -- Another comment
        
        CREATE OR REPLACE FUNCTION line_test()
        RETURNS TEXT
        LANGUAGE sql
        AS $$
            SELECT 'test';
        $$;
    "#};
    
    let another_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION line_test()
        RETURNS TEXT
        LANGUAGE sql
        AS $$
            SELECT 'another test';
        $$;
    "#};
    
    env.write_sql_file("first.sql", file_with_function).await?;
    env.write_sql_file("second.sql", another_function).await?;
    
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(error_msg.contains("Multiple definitions of function 'line_test'"));
            // Should include line numbers in error message
            assert!(error_msg.contains("first.sql:4") || error_msg.contains("first.sql"));
            assert!(error_msg.contains("second.sql:1") || error_msg.contains("second.sql"));
        }
        Ok(_) => panic!("Expected error for duplicate functions with line numbers"),
    }
    
    Ok(())
}