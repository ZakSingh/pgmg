use pgmg::{apply_migrations_with_options, PgmgConfig};
use std::path::PathBuf;
use tokio_postgres::{Client, NoTls};

#[tokio::test]
async fn test_trigger_comment_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the test database
    let (client, connection) = tokio_postgres::connect(
        "postgres://postgres:password@localhost/pgmg_test",
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up any existing test data
    cleanup_test_data(&client).await?;

    // Create test directories
    let test_dir = PathBuf::from("test_trigger_comment_cleanup");
    let code_dir = test_dir.join("code");
    std::fs::create_dir_all(&code_dir)?;

    // Create a table
    let table_sql = r#"
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    value TEXT
);
"#;
    std::fs::write(code_dir.join("table.sql"), table_sql)?;

    // Create a trigger function first
    let trigger_function_sql = r#"
CREATE OR REPLACE FUNCTION test_trigger_function()
RETURNS trigger AS $$
BEGIN
    NEW.value := LOWER(NEW.value);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"#;
    std::fs::write(code_dir.join("trigger_function.sql"), trigger_function_sql)?;

    // Create a trigger
    let trigger_sql = r#"
CREATE TRIGGER test_trigger
    BEFORE INSERT ON test_table
    FOR EACH ROW
    EXECUTE FUNCTION test_trigger_function();
"#;
    // Combine trigger and comment in one file to ensure they're executed together
    let trigger_with_comment_sql = format!("{}\n\n{}", trigger_sql, "COMMENT ON TRIGGER test_trigger ON test_table IS 'Test trigger comment';");
    std::fs::write(code_dir.join("trigger_with_comment.sql"), trigger_with_comment_sql)?;

    // Create config
    let config = PgmgConfig {
        connection_string: Some("postgres://postgres:password@localhost/pgmg_test".to_string()),
        migrations_dir: None,
        code_dir: Some(code_dir.clone()),
        ..Default::default()
    };

    // Apply the initial setup
    apply_migrations_with_options(&config, None, Some(code_dir.clone())).await?;

    // Verify trigger and comment exist
    let trigger_exists: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'test_trigger'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(trigger_exists, 1, "Trigger should exist");

    let comment_exists: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_description WHERE description = 'Test trigger comment'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_exists, 1, "Trigger comment should exist");

    // Debug: Check all pgmg state entries
    let all_state = client
        .query(
            "SELECT object_type, object_name FROM pgmg.pgmg_state ORDER BY object_type, object_name",
            &[],
        )
        .await?;
    
    println!("=== PGMG STATE AFTER INITIAL APPLY ===");
    for row in &all_state {
        let obj_type: String = row.get(0);
        let obj_name: String = row.get(1);
        println!("{}: {}", obj_type, obj_name);
    }
    println!("=====================================");

    // Check pgmg state tracking
    let comment_in_state: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name LIKE 'trigger:test_trigger:%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_in_state, 1, "Trigger comment should be tracked in pgmg state");

    // Now remove the trigger file (which includes the comment)
    std::fs::remove_file(code_dir.join("trigger_with_comment.sql"))?;

    // Apply again - this should remove the trigger and its comment
    apply_migrations_with_options(&config, None, Some(code_dir.clone())).await?;

    // Debug: Check all pgmg state entries after deletion
    let all_state_after = client
        .query(
            "SELECT object_type, object_name FROM pgmg.pgmg_state ORDER BY object_type, object_name",
            &[],
        )
        .await?;
    
    println!("=== PGMG STATE AFTER DELETION APPLY ===");
    for row in &all_state_after {
        let obj_type: String = row.get(0);
        let obj_name: String = row.get(1);
        println!("{}: {}", obj_type, obj_name);
    }
    println!("=====================================");

    // Verify trigger is gone
    let trigger_exists: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'test_trigger'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(trigger_exists, 0, "Trigger should be removed");

    // Verify comment is gone from pg_description
    let comment_exists: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pg_description WHERE description = 'Test trigger comment'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_exists, 0, "Trigger comment should be removed");

    // Most importantly, verify comment is removed from pgmg state tracking
    let comment_in_state: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name LIKE 'trigger:test_trigger:%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_in_state, 0, "Trigger comment should be removed from pgmg state");

    // Clean up
    cleanup_test_data(&client).await?;
    std::fs::remove_dir_all(&test_dir)?;

    Ok(())
}

async fn cleanup_test_data(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // Drop test objects if they exist
    client.execute("DROP TRIGGER IF EXISTS test_trigger ON test_table", &[]).await.ok();
    client.execute("DROP TABLE IF EXISTS test_table CASCADE", &[]).await.ok();
    
    // Clean up pgmg state
    client.execute("DELETE FROM pgmg.pgmg_state WHERE object_name LIKE '%test_%'", &[]).await.ok();
    client.execute("DELETE FROM pgmg.pgmg_dependencies WHERE dependent_name LIKE '%test_%' OR dependency_name LIKE '%test_%'", &[]).await.ok();
    
    Ok(())
}