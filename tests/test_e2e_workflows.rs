mod common;

use common::{TestEnvironment, assertions::*, fixtures};
use pgmg::commands::{execute_plan, execute_apply};
use pgmg::sql::ObjectType;
use indoc::indoc;

#[tokio::test]
async fn test_e2e_plan_apply_plan_workflow() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create initial files
    env.write_migration("001_schema", fixtures::migrations::ADD_USERS_TABLE).await?;
    env.write_migration("002_posts", fixtures::migrations::ADD_POSTS_TABLE).await?;
    env.write_sql_file("recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Step 1: Initial plan should show all changes
    let plan1 = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    assert_eq!(plan1.new_migrations.len(), 2);
    assert_eq!(plan1.changes.len(), 4); // 2 migrations + 2 views
    
    // Step 2: Apply all changes
    let apply_result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply_result);
    assert_migrations_applied(&apply_result, &["001_schema", "002_posts"]);
    assert_objects_created(&apply_result, &["recent_posts", "user_stats"]);
    
    // Step 3: Plan again - should show no changes
    let plan2 = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    assert_plan_empty(&plan2);
    
    Ok(())
}

#[tokio::test]
async fn test_e2e_incremental_changes() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initial setup
    env.write_migration("001_users", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_sql_file("user_view.sql", indoc! {r#"
        CREATE VIEW user_view AS
        SELECT id, username FROM users;
    "#}).await?;
    
    // Apply initial changes
    let apply1 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply1);
    assert_eq!(apply1.migrations_applied.len(), 1);
    assert_eq!(apply1.objects_created.len(), 1);
    
    // Add new migration
    env.write_migration("002_posts", fixtures::sql::CREATE_POSTS_TABLE).await?;
    
    // Modify existing view
    env.write_sql_file("user_view.sql", indoc! {r#"
        CREATE VIEW user_view AS
        SELECT id, username, email, created_at FROM users;
    "#}).await?;
    
    // Add new view
    env.write_sql_file("recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    
    // Plan should show incremental changes
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    assert_plan_contains_migration(&plan, "002_posts");
    assert_plan_contains_update(&plan, ObjectType::View, "user_view");
    assert_plan_contains_create(&plan, ObjectType::View, "recent_posts");
    
    // Apply incremental changes
    let apply2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply2);
    assert_migrations_applied(&apply2, &["002_posts"]);
    assert_objects_updated(&apply2, &["user_view"]);
    assert_objects_created(&apply2, &["recent_posts"]);
    
    // Final plan should be empty
    let final_plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    assert_plan_empty(&final_plan);
    
    Ok(())
}

#[tokio::test]
async fn test_e2e_object_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create base table
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    
    // Step 1: Create object
    env.write_sql_file("active_users.sql", indoc! {r#"
        CREATE VIEW active_users AS
        SELECT * FROM users WHERE created_at > NOW() - INTERVAL '30 days';
    "#}).await?;
    
    let apply1 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_objects_created(&apply1, &["active_users"]);
    assert!(env.view_exists("active_users").await?);
    
    // Step 2: Modify object
    env.write_sql_file("active_users.sql", indoc! {r#"
        CREATE VIEW active_users AS
        SELECT id, username, email 
        FROM users 
        WHERE created_at > NOW() - INTERVAL '7 days';
    "#}).await?;
    
    let apply2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_objects_updated(&apply2, &["active_users"]);
    assert!(env.view_exists("active_users").await?);
    
    // Step 3: Delete object
    std::fs::remove_file(env.sql_dir.join("active_users.sql"))?;
    
    let apply3 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_objects_deleted(&apply3, &["active_users"]);
    assert!(!env.view_exists("active_users").await?);
    
    // Verify state is clean
    let tracked = env.get_tracked_objects().await?;
    assert_eq!(tracked.len(), 0);
    
    Ok(())
}

#[tokio::test]
async fn test_e2e_complex_dependency_chain() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a complex dependency chain:
    // users -> posts -> comments (tables as migrations)
    //       -> user_stats (view)
    //       -> recent_posts (view) -> top_posts (view)
    
    // Create tables using migrations
    env.write_migration("001_users", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_migration("002_posts", fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.write_migration("003_comments", fixtures::sql::CREATE_COMMENTS_TABLE).await?;
    
    // Create views as SQL objects
    env.write_sql_file("04_user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    env.write_sql_file("05_recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    env.write_sql_file("06_top_posts.sql", indoc! {r#"
        CREATE VIEW top_posts AS
        SELECT * FROM recent_posts
        ORDER BY created_at DESC
        LIMIT 10;
    "#}).await?;
    
    // Plan should respect dependencies
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    assert_eq!(plan.new_migrations.len(), 3);
    assert_eq!(plan.changes.len(), 6); // 3 migrations + 3 views
    
    // Apply all
    let apply = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply);
    assert_eq!(apply.migrations_applied.len(), 3);
    assert_eq!(apply.objects_created.len(), 3);
    
    // Verify all objects exist
    assert!(env.table_exists("users").await?);
    assert!(env.table_exists("posts").await?);
    assert!(env.table_exists("comments").await?);
    assert!(env.view_exists("user_stats").await?);
    assert!(env.view_exists("recent_posts").await?);
    assert!(env.view_exists("top_posts").await?);
    
    Ok(())
}

#[tokio::test]
async fn test_e2e_migration_and_dependent_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Scenario: Migration creates table, then objects depend on it
    env.write_migration("001_create_products", indoc! {r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            category VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_products_category ON products(category);
    "#}).await?;
    
    env.write_sql_file("expensive_products.sql", indoc! {r#"
        CREATE VIEW expensive_products AS
        SELECT * FROM products
        WHERE price > 100.00;
    "#}).await?;
    
    env.write_sql_file("product_stats.sql", indoc! {r#"
        CREATE OR REPLACE FUNCTION product_category_stats()
        RETURNS TABLE(category VARCHAR, product_count BIGINT, avg_price NUMERIC)
        AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                p.category,
                COUNT(*) as product_count,
                AVG(p.price) as avg_price
            FROM products p
            GROUP BY p.category;
        END;
        $$ LANGUAGE plpgsql;
    "#}).await?;
    
    // Apply everything
    let apply = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply);
    assert_migrations_applied(&apply, &["001_create_products"]);
    assert_objects_created(&apply, &["expensive_products", "product_category_stats"]);
    
    // Verify migration tracking
    let migrations = env.get_applied_migrations().await?;
    assert_eq!(migrations.len(), 1);
    
    // Verify object tracking
    let objects = env.get_tracked_objects().await?;
    assert_eq!(objects.len(), 2);
    
    Ok(())
}

#[tokio::test]
async fn test_e2e_error_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create initial valid state
    env.write_migration("001_users", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_sql_file("user_view.sql", indoc! {r#"
        CREATE VIEW user_view AS
        SELECT id, username FROM users;
    "#}).await?;
    
    // Apply successfully
    let apply1 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply1);
    
    // Add invalid migration
    env.write_migration("002_bad", indoc! {r#"
        CREATE TABLE test (id INT);
        INVALID SQL STATEMENT;
    "#}).await?;
    
    // Apply should fail
    let apply2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await;
    
    assert!(apply2.is_err());
    
    // Fix the bad migration
    env.write_migration("002_bad", indoc! {r#"
        CREATE TABLE test (id SERIAL PRIMARY KEY);
    "#}).await?;
    
    // Apply should now succeed
    let apply3 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
    ).await?;
    
    assert_apply_successful(&apply3);
    assert_migrations_applied(&apply3, &["002_bad"]);
    
    // Verify final state
    assert!(env.table_exists("users").await?);
    assert!(env.table_exists("test").await?);
    assert!(env.view_exists("user_view").await?);
    
    Ok(())
}