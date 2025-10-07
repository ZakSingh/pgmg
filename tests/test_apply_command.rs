mod common;

use common::{TestEnvironment, assertions::*, fixtures};
use pgmg::commands::execute_apply;
use pgmg::config::PgmgConfig;
use indoc::indoc;

#[tokio::test]
async fn test_apply_new_migrations() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create migration files
    env.write_migration("001_initial_schema", fixtures::migrations::INITIAL_SCHEMA).await?;
    env.write_migration("002_add_users", fixtures::migrations::ADD_USERS_TABLE).await?;
    
    // Execute apply
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Assertions
    assert_apply_successful(&result);
    assert_migrations_applied(&result, &["001_initial_schema", "002_add_users"]);
    
    // Verify tables were created
    assert!(env.table_exists("schema_version").await?);
    assert!(env.table_exists("users").await?);
    
    // Verify migrations were tracked
    let applied = env.get_applied_migrations().await?;
    assert_eq!(applied.len(), 2);
    assert!(applied.contains(&"001_initial_schema".to_string()));
    assert!(applied.contains(&"002_add_users".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_apply_idempotency() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create migration file
    env.write_migration("001_initial_schema", fixtures::migrations::INITIAL_SCHEMA).await?;
    
    // Apply once
    let result1 = execute_apply(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&result1);
    assert_migrations_applied(&result1, &["001_initial_schema"]);
    
    // Apply again - should be idempotent
    let result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&result2);
    assert_eq!(result2.migrations_applied.len(), 0); // No new migrations
    
    // Verify only one record in migrations table
    let applied = env.get_applied_migrations().await?;
    assert_eq!(applied.len(), 1);
    
    Ok(())
}

#[tokio::test]
async fn test_apply_creates_sql_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // First create dependent tables
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_COMMENTS_TABLE).await?;
    
    // Create SQL object files
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    env.write_sql_file("user_activity.sql", fixtures::sql::create_user_activity_function()).await?;
    
    // Execute apply
    let result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Assertions
    assert_apply_successful(&result);
    assert_objects_created(&result, &["user_stats", "get_user_activity"]);
    
    // Verify objects exist
    assert!(env.view_exists("user_stats").await?);
    assert!(env.function_exists("get_user_activity").await?);
    
    // Verify state tracking
    let tracked = env.get_tracked_objects().await?;
    assert_eq!(tracked.len(), 2);
    assert!(tracked.contains(&("view".to_string(), "user_stats".to_string())));
    assert!(tracked.contains(&("function".to_string(), "get_user_activity".to_string())));
    
    Ok(())
}

#[tokio::test]
async fn test_apply_updates_modified_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create initial view
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(indoc! {r#"
        CREATE VIEW user_stats AS
        SELECT id, username FROM users;
    "#}).await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Track it with old hash
    env.execute_sql(indoc! {r#"
        INSERT INTO pgmg.pgmg_state (object_type, object_name, ddl_hash)
        VALUES ('view', 'user_stats', 'old_hash');
    "#}).await?;
    
    // Write modified view
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_COMMENTS_TABLE).await?;
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute apply
    let result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Assertions
    assert_apply_successful(&result);
    assert_objects_updated(&result, &["user_stats"]);
    
    // Verify view was updated
    assert!(env.view_exists("user_stats").await?);
    
    // Verify new hash was recorded
    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("view".to_string(), "user_stats".to_string())));
    
    Ok(())
}

#[tokio::test]
async fn test_apply_deletes_removed_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create objects in database
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(indoc! {r#"
        CREATE VIEW to_delete AS SELECT * FROM users;
    "#}).await?;
    env.execute_sql(indoc! {r#"
        CREATE VIEW to_keep AS SELECT id, username FROM users;
    "#}).await?;
    
    // Track both objects
    env.execute_sql(indoc! {r#"
        INSERT INTO pgmg.pgmg_state (object_type, object_name, ddl_hash)
        VALUES
            ('view', 'to_delete', 'hash1'),
            ('view', 'to_keep', 'hash2');
    "#}).await?;
    
    // Only write one view to file (the other should be deleted)
    env.write_sql_file("to_keep.sql", indoc! {r#"
        CREATE VIEW to_keep AS SELECT id, username FROM users;
    "#}).await?;
    
    // Execute apply
    let result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Assertions
    assert_apply_successful(&result);
    assert_objects_deleted(&result, &["to_delete"]);
    
    // Verify object was deleted
    assert!(!env.view_exists("to_delete").await?);
    assert!(env.view_exists("to_keep").await?);
    
    // Verify state tracking was updated
    let tracked = env.get_tracked_objects().await?;
    assert_eq!(tracked.len(), 1);
    assert!(tracked.contains(&("view".to_string(), "to_keep".to_string())));
    
    Ok(())
}

#[tokio::test]
async fn test_apply_rollback_on_migration_error() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Pre-seed database with a successful migration so it's not a "fresh build"
    // This ensures transaction mode is used for rollback testing
    env.write_migration("000_init", "SELECT 1;").await?;
    execute_apply(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    // Create migrations - second one has error
    env.write_migration("001_good", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_migration("002_bad", fixtures::migrations::MIGRATION_WITH_ERROR).await?;

    // Execute apply - should fail
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await;

    // Should have failed
    assert!(result.is_err());

    // Verify nothing was applied (transaction rolled back)
    assert!(!env.table_exists("users").await?);
    assert!(!env.table_exists("test_table").await?);

    // Verify only the initial migration was recorded (not the failed ones)
    let applied = env.get_applied_migrations().await?;
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0], "000_init");

    Ok(())
}

#[tokio::test]
async fn test_apply_rollback_on_object_error() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Pre-seed database with a successful migration so it's not a "fresh build"
    // This ensures transaction mode is used for rollback testing
    env.write_migration("000_init", "SELECT 1;").await?;
    execute_apply(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;

    // Create valid and invalid SQL files
    env.write_sql_file("users.sql", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_sql_file("bad_view.sql", indoc! {r#"
        CREATE VIEW bad_view AS
        SELECT * FROM non_existent_table;
    "#}).await?;

    // Execute apply - should fail
    let result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await;

    // Should have failed
    assert!(result.is_err());

    // Verify nothing was created (transaction rolled back)
    assert!(!env.table_exists("users").await?);
    assert!(!env.view_exists("bad_view").await?);

    // Verify no objects were tracked
    let tracked = env.get_tracked_objects().await?;
    assert_eq!(tracked.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_apply_with_complex_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create migrations for tables
    env.write_migration("001_users", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_migration("002_posts", fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.write_migration("003_comments", fixtures::sql::CREATE_COMMENTS_TABLE).await?;
    
    // Create SQL object files for views
    env.write_sql_file("recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute apply with both migrations and SQL objects
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Assertions
    assert_apply_successful(&result);
    assert_eq!(result.migrations_applied.len(), 3);
    assert_eq!(result.objects_created.len(), 2);
    
    // Verify all objects exist
    assert!(env.table_exists("users").await?);
    assert!(env.table_exists("posts").await?);
    assert!(env.table_exists("comments").await?);
    assert!(env.view_exists("recent_posts").await?);
    assert!(env.view_exists("user_stats").await?);
    
    Ok(())
}

#[tokio::test]
async fn test_apply_mixed_migrations_and_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create migrations
    env.write_migration("001_users", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_migration("002_posts", fixtures::sql::CREATE_POSTS_TABLE).await?;
    
    // Create SQL objects that depend on migrations
    env.write_sql_file("recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    env.write_sql_file("user_activity.sql", fixtures::sql::create_user_activity_function()).await?;
    
    // Execute apply
    let result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Assertions
    assert_apply_successful(&result);
    assert_migrations_applied(&result, &["001_users", "002_posts"]);
    assert_objects_created(&result, &["recent_posts", "get_user_activity"]);
    
    // Verify everything exists
    assert!(env.table_exists("users").await?);
    assert!(env.table_exists("posts").await?);
    assert!(env.view_exists("recent_posts").await?);
    assert!(env.function_exists("get_user_activity").await?);
    
    Ok(())
}

#[tokio::test]
async fn test_apply_twice_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create files
    env.write_migration("001_users", fixtures::sql::CREATE_USERS_TABLE).await?;
    env.write_migration("002_posts", fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // First apply
    let result1 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&result1);
    assert_eq!(result1.migrations_applied.len(), 2);
    assert_eq!(result1.objects_created.len(), 1);
    
    // Second apply - should do nothing
    let result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    assert_apply_successful(&result2);
    assert_eq!(result2.migrations_applied.len(), 0);
    assert_eq!(result2.objects_created.len(), 0);
    assert_eq!(result2.objects_updated.len(), 0);
    assert_eq!(result2.objects_deleted.len(), 0);
    
    Ok(())
}