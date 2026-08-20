use pgmg::{apply_migrations_with_options, PgmgConfig};

mod common;
use common::TestEnvironment;

#[tokio::test]
async fn test_trigger_comment_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Create a table
    env.write_sql_file("table.sql", r#"
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    value TEXT
);
"#).await?;

    // Create a trigger function first
    env.write_sql_file("trigger_function.sql", r#"
CREATE OR REPLACE FUNCTION test_trigger_function()
RETURNS trigger AS $$
BEGIN
    NEW.value := LOWER(NEW.value);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"#).await?;

    // Create a trigger with its comment in one file so they're applied together
    env.write_sql_file("trigger_with_comment.sql", r#"
CREATE TRIGGER test_trigger
    BEFORE INSERT ON test_table
    FOR EACH ROW
    EXECUTE FUNCTION test_trigger_function();

COMMENT ON TRIGGER test_trigger ON test_table IS 'Test trigger comment';
"#).await?;

    // Create config
    let config = PgmgConfig {
        connection_string: Some(env.connection_string.clone()),
        migrations_dir: None,
        code_dir: Some(env.sql_dir.clone()),
        ..Default::default()
    };

    // Apply the initial setup
    apply_migrations_with_options(&config, None, Some(env.sql_dir.clone())).await?;

    // Verify trigger and comment exist
    let trigger_exists: i64 = env.client
        .query_one(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'test_trigger'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(trigger_exists, 1, "Trigger should exist");

    let comment_exists: i64 = env.client
        .query_one(
            "SELECT COUNT(*) FROM pg_description WHERE description = 'Test trigger comment'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_exists, 1, "Trigger comment should exist");

    // Check pgmg state tracking
    let comment_in_state: i64 = env.client
        .query_one(
            "SELECT COUNT(*) FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name LIKE 'trigger:test_trigger:%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_in_state, 1, "Trigger comment should be tracked in pgmg state");

    // Now remove the trigger file (which includes the comment)
    env.delete_sql_file("trigger_with_comment.sql").await?;

    // Apply again - this should remove the trigger and its comment
    apply_migrations_with_options(&config, None, Some(env.sql_dir.clone())).await?;

    // Verify trigger is gone
    let trigger_exists: i64 = env.client
        .query_one(
            "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'test_trigger'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(trigger_exists, 0, "Trigger should be removed");

    // Verify comment is gone from pg_description
    let comment_exists: i64 = env.client
        .query_one(
            "SELECT COUNT(*) FROM pg_description WHERE description = 'Test trigger comment'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_exists, 0, "Trigger comment should be removed");

    // Most importantly, verify comment is removed from pgmg state tracking
    let comment_in_state: i64 = env.client
        .query_one(
            "SELECT COUNT(*) FROM pgmg.pgmg_state WHERE object_type = 'comment' AND object_name LIKE 'trigger:test_trigger:%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(comment_in_state, 0, "Trigger comment should be removed from pgmg state");

    Ok(())
}
