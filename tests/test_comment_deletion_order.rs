use pgmg::{apply_migrations_with_options, PgmgConfig};
use tokio_postgres::NoTls;

/// Test that comments are deleted before their parent objects
/// This prevents "relation does not exist" errors when deleting column comments
#[tokio::test]
async fn test_comment_deletion_order() {
    // Skip if no database URL is provided
    let db_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }
    };

    // Create a temporary test directory
    let test_dir = tempfile::tempdir().unwrap();
    let code_dir = test_dir.path().join("code");
    std::fs::create_dir(&code_dir).unwrap();

    // Connect to database and create test schema
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await.unwrap();
    
    // Spawn connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up any existing test schema
    let _ = client.execute("DROP SCHEMA IF EXISTS test_comments CASCADE", &[]).await;
    client.execute("CREATE SCHEMA test_comments", &[]).await.unwrap();

    // Create initial objects with comments
    let initial_sql = r#"
-- Create a view
CREATE VIEW test_comments.product_summary AS
SELECT 1 as id, 'test' as name, 'brief' as category_brief;

-- Add comments
COMMENT ON VIEW test_comments.product_summary IS 'Product summary view';
COMMENT ON COLUMN test_comments.product_summary.category_brief IS 'Brief category description';
"#;

    std::fs::write(code_dir.join("01_initial.sql"), initial_sql).unwrap();

    // Configure pgmg
    let mut config = PgmgConfig::default();
    config.connection_string = Some(db_url.clone());
    config.code_dir = Some(code_dir.clone());
    config.development_mode = Some(false);
    config.check_plpgsql = Some(false);

    // Apply initial state
    let result = apply_migrations_with_options(&config, None, Some(code_dir.clone())).await.unwrap();
    assert_eq!(result.objects_created.len(), 3); // view + 2 comments
    assert!(result.errors.is_empty());

    // Now remove the file to trigger deletion
    std::fs::remove_file(code_dir.join("01_initial.sql")).unwrap();

    // Apply again - this should delete comments before the view
    let result = apply_migrations_with_options(&config, None, Some(code_dir.clone())).await.unwrap();
    
    // Should successfully delete all objects without errors
    assert_eq!(result.objects_deleted.len(), 3); // view + 2 comments
    assert!(result.errors.is_empty(), "Got errors: {:?}", result.errors);

    // Verify objects are actually deleted
    let exists = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.views 
                WHERE table_schema = 'test_comments' 
                AND table_name = 'product_summary'
            )",
            &[]
        )
        .await
        .unwrap();
    let view_exists: bool = exists.get(0);
    assert!(!view_exists, "View should have been deleted");

    // Clean up
    let _ = client.execute("DROP SCHEMA IF EXISTS test_comments CASCADE", &[]).await;
}

/// Test comment deletion order with tables and multiple column comments
#[tokio::test]
async fn test_table_comment_deletion_order() {
    // Skip if no database URL is provided
    let db_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }
    };

    // Create a temporary test directory
    let test_dir = tempfile::tempdir().unwrap();
    let code_dir = test_dir.path().join("code");
    std::fs::create_dir(&code_dir).unwrap();

    // Connect to database
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await.unwrap();
    
    // Spawn connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up any existing test schema
    let _ = client.execute("DROP SCHEMA IF EXISTS test_table_comments CASCADE", &[]).await;
    client.execute("CREATE SCHEMA test_table_comments", &[]).await.unwrap();

    // Create initial table with multiple column comments
    let initial_sql = r#"
-- Create a table
CREATE TABLE test_table_comments.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);

-- Add comments
COMMENT ON TABLE test_table_comments.users IS 'User accounts table';
COMMENT ON COLUMN test_table_comments.users.id IS 'User ID';
COMMENT ON COLUMN test_table_comments.users.name IS 'User full name';
COMMENT ON COLUMN test_table_comments.users.email IS 'User email address';
"#;

    std::fs::write(code_dir.join("02_table.sql"), initial_sql).unwrap();

    // Configure pgmg
    let mut config = PgmgConfig::default();
    config.connection_string = Some(db_url.clone());
    config.code_dir = Some(code_dir.clone());
    config.development_mode = Some(false);
    config.check_plpgsql = Some(false);

    // Apply initial state
    let result = apply_migrations_with_options(&config, None, Some(code_dir.clone())).await.unwrap();
    assert!(result.errors.is_empty());

    // Now remove the file to trigger deletion
    std::fs::remove_file(code_dir.join("02_table.sql")).unwrap();

    // Apply again - comments should be deleted before the table
    let result = apply_migrations_with_options(&config, None, Some(code_dir.clone())).await.unwrap();
    
    // Should successfully delete all objects without errors
    assert!(result.errors.is_empty(), "Got errors: {:?}", result.errors);
    assert!(result.objects_deleted.contains(&"test_table_comments.users".to_string()));

    // Clean up
    let _ = client.execute("DROP SCHEMA IF EXISTS test_table_comments CASCADE", &[]).await;
}