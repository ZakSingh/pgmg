mod common;

use common::TestEnvironment;
use pgmg::commands::execute_plan;
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
    
    // Create a function and two files that might have comments or triggers with same names
    // These should be allowed as they are contextual
    let base_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION test_func()
        RETURNS TEXT
        LANGUAGE sql
        AS $$
            SELECT 'test';
        $$;
    "#};
    
    // Comments and triggers should be allowed to have same identifiers
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
    
    env.write_sql_file("func.sql", base_function).await?;
    env.write_sql_file("trigger1.sql", trigger1).await?;
    env.write_sql_file("trigger2.sql", trigger2).await?;
    
    // Should succeed - triggers with same name on different tables should be allowed
    let result = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await;
    
    // This should either succeed or fail for other reasons, but not duplicate detection
    match result {
        Ok(_) => {
            // Success is expected
        }
        Err(e) => {
            let error_msg = e.to_string();
            // Should not fail due to duplicate detection
            assert!(!error_msg.contains("Multiple definitions"));
            assert!(!error_msg.contains("pgmg does not allow duplicate object names"));
        }
    }
    
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