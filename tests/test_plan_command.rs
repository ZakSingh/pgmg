mod common;

use common::{TestEnvironment, assertions::*, fixtures};
use pgmg::commands::execute_plan;
use pgmg::sql::ObjectType;
use indoc::indoc;

#[tokio::test]
async fn test_plan_empty_database_with_migrations() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create migration files
    env.write_migration("001_initial_schema", fixtures::migrations::INITIAL_SCHEMA).await?;
    env.write_migration("002_add_users", fixtures::migrations::ADD_USERS_TABLE).await?;
    
    // Execute plan
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Assertions
    assert_eq!(plan.new_migrations.len(), 2);
    assert_plan_contains_migration(&plan, "001_initial_schema");
    assert_plan_contains_migration(&plan, "002_add_users");
    
    Ok(())
}

#[tokio::test]
async fn test_plan_with_existing_migrations() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Apply first migration manually
    env.execute_sql(fixtures::migrations::INITIAL_SCHEMA).await?;
    env.execute_sql(
        "INSERT INTO pgmg_migrations (name) VALUES ('001_initial_schema')"
    ).await?;
    
    // Create migration files
    env.write_migration("001_initial_schema", fixtures::migrations::INITIAL_SCHEMA).await?;
    env.write_migration("002_add_users", fixtures::migrations::ADD_USERS_TABLE).await?;
    env.write_migration("003_add_posts", fixtures::migrations::ADD_POSTS_TABLE).await?;
    
    // Execute plan
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        None,
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should only detect new migrations
    assert_eq!(plan.new_migrations.len(), 2);
    assert_plan_contains_migration(&plan, "002_add_users");
    assert_plan_contains_migration(&plan, "003_add_posts");
    assert!(!plan.new_migrations.contains(&"001_initial_schema".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_plan_detects_new_sql_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create SQL object files
    env.write_sql_file("users_view.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    env.write_sql_file("user_activity.sql", fixtures::sql::create_user_activity_function()).await?;
    
    // Need to create the dependent tables first
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_COMMENTS_TABLE).await?;
    
    // Execute plan
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should detect new objects
    assert_plan_contains_create(&plan, ObjectType::View, "user_stats");
    assert_plan_contains_create(&plan, ObjectType::Function, "get_user_activity");
    
    Ok(())
}

#[tokio::test]
async fn test_plan_detects_modified_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create initial view in database
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(indoc! {r#"
        CREATE VIEW user_stats AS
        SELECT 
            u.id as user_id,
            u.username
        FROM users u;
    "#}).await?;
    
    // Track the view in pgmg_state with its hash
    env.execute_sql(indoc! {r#"
        INSERT INTO pgmg_state (object_type, object_name, ddl_hash)
        VALUES ('view', 'user_stats', 'old_hash_value');
    "#}).await?;
    
    // Write modified view to file
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute plan
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should detect the modification
    assert_plan_contains_update(&plan, ObjectType::View, "user_stats");
    
    Ok(())
}

#[tokio::test]
async fn test_plan_detects_deleted_objects() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create objects in database
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Track objects in pgmg_state
    env.execute_sql(indoc! {r#"
        INSERT INTO pgmg_state (object_type, object_name, ddl_hash)
        VALUES 
            ('view', 'user_stats', 'some_hash'),
            ('view', 'deleted_view', 'another_hash');
    "#}).await?;
    
    // Only write one view to file (the other is "deleted")
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute plan
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should detect the deletion
    assert_plan_contains_delete(&plan, ObjectType::View, "deleted_view");
    
    Ok(())
}

#[tokio::test]
async fn test_plan_with_complex_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create tables first (these would normally be migrations)
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    
    // Create SQL files with dependencies
    env.write_sql_file("recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute plan
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should detect 2 views as new
    assert_eq!(plan.changes.len(), 2);
    
    // Verify dependency graph exists
    assert!(plan.dependency_graph.is_some());
    let graph = plan.dependency_graph.as_ref().unwrap();
    assert_eq!(graph.node_count(), 2);
    
    Ok(())
}

#[tokio::test]
async fn test_plan_with_no_changes() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create objects in database
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Write same view to file
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Scan the file to get the actual normalized DDL and hash that would be stored
    let builtin_catalog = pgmg::BuiltinCatalog::from_database(&env.client).await?;
    let sql_objects = pgmg::scan_sql_files(&env.sql_dir, &builtin_catalog).await?;
    
    // Find the user_stats view and get its hash
    let user_stats_obj = sql_objects.iter()
        .find(|obj| obj.qualified_name.name == "user_stats")
        .expect("user_stats view should be found");
    
    env.execute_sql(&format!(
        "INSERT INTO pgmg_state (object_type, object_name, ddl_hash) VALUES ('view', 'user_stats', '{}')",
        user_stats_obj.ddl_hash
    )).await?;
    
    // Execute plan
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should have no changes
    assert_plan_empty(&plan);
    
    Ok(())
}

#[tokio::test]
async fn test_plan_with_graphviz_output() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let graph_output = env.temp_dir.path().join("dependency_graph.dot");
    
    // Create tables first
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    
    // Create SQL files for views
    env.write_sql_file("recent_posts.sql", fixtures::sql::CREATE_RECENT_POSTS_VIEW).await?;
    env.write_sql_file("user_stats.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute plan with graph output
    let _plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        Some(graph_output.clone()),
    ).await?;
    
    // Verify graph file was created
    assert!(graph_output.exists());
    let graph_content = std::fs::read_to_string(&graph_output)?;
    assert!(graph_content.contains("digraph dependency_graph"));
    assert!(graph_content.contains("recent_posts"));
    assert!(graph_content.contains("user_stats"));
    
    Ok(())
}

#[tokio::test]
async fn test_plan_with_mixed_changes() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tables first
    let state_manager = pgmg::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Set up existing state
    env.execute_sql(fixtures::sql::CREATE_USERS_TABLE).await?;
    env.execute_sql(indoc! {r#"
        CREATE VIEW old_view AS SELECT * FROM users;
    "#}).await?;
    
    env.execute_sql(indoc! {r#"
        INSERT INTO pgmg_state (object_type, object_name, ddl_hash)
        VALUES 
            ('view', 'old_view', 'old_view_hash'),
            ('view', 'deleted_view', 'deleted_hash');
    "#}).await?;
    
    env.execute_sql(
        "INSERT INTO pgmg_migrations (name) VALUES ('001_initial')"
    ).await?;
    
    // Create files for mixed changes
    env.write_migration("001_initial", "-- already applied").await?;
    env.write_migration("002_new_migration", fixtures::migrations::ADD_POSTS_TABLE).await?;
    
    env.write_sql_file("old_view.sql", indoc! {r#"
        CREATE VIEW old_view AS 
        SELECT id, username FROM users WHERE created_at > NOW() - INTERVAL '7 days';
    "#}).await?;
    
    // Create posts table that the view depends on
    env.execute_sql(fixtures::sql::CREATE_POSTS_TABLE).await?;
    
    env.write_sql_file("new_view.sql", fixtures::sql::CREATE_USER_STATS_VIEW).await?;
    
    // Execute plan
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Verify all types of changes detected
    assert_plan_contains_migration(&plan, "002_new_migration");
    assert_plan_contains_update(&plan, ObjectType::View, "old_view");
    assert_plan_contains_create(&plan, ObjectType::View, "user_stats");
    assert_plan_contains_delete(&plan, ObjectType::View, "deleted_view");
    
    Ok(())
}