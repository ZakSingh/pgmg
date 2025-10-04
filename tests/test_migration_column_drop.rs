mod common;

use common::{TestEnvironment, assertions::*};
use pgmg::commands::execute_apply;
use pgmg::config::PgmgConfig;
use indoc::indoc;

/// Test that dropping a column in a migration works when views reference that column
/// This tests the pre-drop functionality that drops managed objects before migrations
#[tokio::test]
async fn test_column_drop_with_dependent_view() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Step 1: Create initial table with multiple columns via migration
    let migration_001 = indoc! {r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            old_field TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    "#};

    env.write_migration("001_create_users", migration_001).await?;

    // Create a view that uses old_field
    let view_sql_v1 = indoc! {r#"
        CREATE VIEW user_view AS
        SELECT id, name, old_field
        FROM users;
    "#};

    env.write_sql_file("user_view.sql", view_sql_v1).await?;

    // Apply initial state
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result);
    assert_eq!(result.migrations_applied.len(), 1);
    assert_eq!(result.objects_created.len(), 1);

    // Verify table and view exist with old_field
    let has_old_field_in_table: bool = env.query_scalar(
        "SELECT COUNT(*) > 0 FROM information_schema.columns
         WHERE table_name = 'users' AND column_name = 'old_field'"
    ).await?;
    assert!(has_old_field_in_table, "old_field should exist in users table");

    let has_old_field_in_view: bool = env.query_scalar(
        "SELECT COUNT(*) > 0 FROM information_schema.columns
         WHERE table_name = 'user_view' AND column_name = 'old_field'"
    ).await?;
    assert!(has_old_field_in_view, "old_field should exist in user_view");

    // Step 2: Drop the column in a new migration
    let migration_002 = indoc! {r#"
        ALTER TABLE users DROP COLUMN old_field;
    "#};

    env.write_migration("002_drop_old_field", migration_002).await?;

    // Update view to not reference old_field
    let view_sql_v2 = indoc! {r#"
        CREATE VIEW user_view AS
        SELECT id, name, created_at
        FROM users;
    "#};

    env.write_sql_file("user_view.sql", view_sql_v2).await?;

    // Apply with pre-drop - this should succeed
    let result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result2);
    assert_eq!(result2.migrations_applied.len(), 1, "Should apply 1 migration");
    assert_eq!(result2.objects_updated.len(), 1, "Should update 1 object (user_view)");

    // Verify old_field is gone from both table and view
    let has_old_field_in_table_after: bool = env.query_scalar(
        "SELECT COUNT(*) > 0 FROM information_schema.columns
         WHERE table_name = 'users' AND column_name = 'old_field'"
    ).await?;
    assert!(!has_old_field_in_table_after, "old_field should be dropped from users table");

    let has_old_field_in_view_after: bool = env.query_scalar(
        "SELECT COUNT(*) > 0 FROM information_schema.columns
         WHERE table_name = 'user_view' AND column_name = 'old_field'"
    ).await?;
    assert!(!has_old_field_in_view_after, "old_field should not exist in updated user_view");

    // Verify view still works and has correct columns
    let view_columns: Vec<String> = env.query_all(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = 'user_view'
         ORDER BY ordinal_position"
    ).await?;
    assert_eq!(view_columns, vec!["id", "name", "created_at"]);

    Ok(())
}

/// Test that transitive dependencies are handled correctly during column drop
#[tokio::test]
async fn test_column_drop_with_transitive_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Step 1: Create table and two views with transitive dependency
    let migration_001 = indoc! {r#"
        CREATE TABLE products (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            legacy_code TEXT,
            price DECIMAL(10,2)
        );
    "#};

    env.write_migration("001_create_products", migration_001).await?;

    // View 1 depends on products table
    let view1_v1 = indoc! {r#"
        CREATE VIEW product_codes AS
        SELECT id, name, legacy_code
        FROM products;
    "#};

    env.write_sql_file("product_codes.sql", view1_v1).await?;

    // View 2 depends on view 1 (transitive dependency on products)
    let view2_v1 = indoc! {r#"
        CREATE VIEW product_summary AS
        SELECT id, name, legacy_code
        FROM product_codes
        WHERE legacy_code IS NOT NULL;
    "#};

    env.write_sql_file("product_summary.sql", view2_v1).await?;

    // Apply initial state
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result);
    assert_eq!(result.objects_created.len(), 2);

    // Step 2: Drop legacy_code column
    let migration_002 = indoc! {r#"
        ALTER TABLE products DROP COLUMN legacy_code;
    "#};

    env.write_migration("002_drop_legacy_code", migration_002).await?;

    // Update both views to not use legacy_code
    let view1_v2 = indoc! {r#"
        CREATE VIEW product_codes AS
        SELECT id, name
        FROM products;
    "#};

    env.write_sql_file("product_codes.sql", view1_v2).await?;

    let view2_v2 = indoc! {r#"
        CREATE VIEW product_summary AS
        SELECT id, name
        FROM product_codes;
    "#};

    env.write_sql_file("product_summary.sql", view2_v2).await?;

    // Apply - should pre-drop both views in correct order (product_summary before product_codes)
    let result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result2);
    assert_eq!(result2.migrations_applied.len(), 1);
    assert_eq!(result2.objects_updated.len(), 2, "Both views should be updated");

    // Verify both views exist and don't have legacy_code
    let view1_columns: Vec<String> = env.query_all(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = 'product_codes'
         ORDER BY ordinal_position"
    ).await?;
    assert_eq!(view1_columns, vec!["id", "name"]);

    let view2_columns: Vec<String> = env.query_all(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = 'product_summary'
         ORDER BY ordinal_position"
    ).await?;
    assert_eq!(view2_columns, vec!["id", "name"]);

    Ok(())
}

/// Test that when there are no migrations, the old behavior is unchanged
#[tokio::test]
async fn test_no_migrations_no_predrop() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Create table directly (not via migration)
    env.execute_sql(
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)"
    ).await?;

    // Create view
    let view_sql = indoc! {r#"
        CREATE VIEW item_view AS
        SELECT id, name
        FROM items;
    "#};

    env.write_sql_file("item_view.sql", view_sql).await?;

    // Apply - should create the view normally
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result);
    assert_eq!(result.migrations_applied.len(), 0);
    assert_eq!(result.objects_created.len(), 1);

    // Update the view
    let view_sql_v2 = indoc! {r#"
        CREATE VIEW item_view AS
        SELECT id, UPPER(name) as name
        FROM items;
    "#};

    env.write_sql_file("item_view.sql", view_sql_v2).await?;

    // Apply update - should use normal flow (no migrations, so no pre-drop)
    let result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result2);
    assert_eq!(result2.migrations_applied.len(), 0);
    assert_eq!(result2.objects_updated.len(), 1);

    Ok(())
}

/// Test column drop with materialized view
#[tokio::test]
async fn test_column_drop_with_materialized_view() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Create table
    let migration_001 = indoc! {r#"
        CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            deprecated_field TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    "#};

    env.write_migration("001_create_events", migration_001).await?;

    // Create materialized view using deprecated_field
    let matview_sql_v1 = indoc! {r#"
        CREATE MATERIALIZED VIEW event_stats AS
        SELECT event_type, deprecated_field, COUNT(*) as count
        FROM events
        GROUP BY event_type, deprecated_field;
    "#};

    env.write_sql_file("event_stats.sql", matview_sql_v1).await?;

    // Apply
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result);

    // Drop column
    let migration_002 = indoc! {r#"
        ALTER TABLE events DROP COLUMN deprecated_field;
    "#};

    env.write_migration("002_drop_deprecated", migration_002).await?;

    // Update materialized view
    let matview_sql_v2 = indoc! {r#"
        CREATE MATERIALIZED VIEW event_stats AS
        SELECT event_type, COUNT(*) as count
        FROM events
        GROUP BY event_type;
    "#};

    env.write_sql_file("event_stats.sql", matview_sql_v2).await?;

    // Apply - should pre-drop materialized view, run migration, recreate
    let result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    assert_apply_successful(&result2);
    assert_eq!(result2.migrations_applied.len(), 1);
    assert_eq!(result2.objects_updated.len(), 1);

    // Verify materialized view exists
    let matview_exists: bool = env.query_scalar(
        "SELECT COUNT(*) > 0 FROM pg_class WHERE relname = 'event_stats' AND relkind = 'm'"
    ).await?;
    assert!(matview_exists, "Materialized view should exist");

    // Verify columns - note: materialized views might not appear in information_schema.columns
    // Let's query pg_attribute instead
    let column_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_attribute a
         JOIN pg_class c ON a.attrelid = c.oid
         WHERE c.relname = 'event_stats' AND c.relkind = 'm' AND a.attnum > 0 AND NOT a.attisdropped"
    ).await?;
    assert_eq!(column_count, 2, "Should have 2 columns (event_type and count)");

    Ok(())
}
