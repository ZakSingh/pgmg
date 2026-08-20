// Container-backed tests for client-compatibility severity classification.
//
// Each test seeds the database and pgmg_state directly (the pattern used by
// test_plan_command.rs), then asserts on the severities `execute_plan` assigns.

mod common;

use common::{TestEnvironment, assertions::*};
use pgmg::commands::{execute_apply, execute_plan};
use pgmg::config::PgmgConfig;
use pgmg::sql::{ObjectType, objects::calculate_ddl_hash};
use pgmg::Severity;
use indoc::indoc;

/// Create an object in the database and record it in pgmg_state exactly as an
/// earlier apply would have, so a later plan sees it as a tracked object.
async fn track_object(
    env: &TestEnvironment,
    object_type: &str,
    state_name: &str,
    ddl: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    env.execute_sql(ddl).await?;
    record_state(env, object_type, state_name, ddl).await
}

/// Record a state row without creating the object (for fabricating stale state).
async fn record_state(
    env: &TestEnvironment,
    object_type: &str,
    state_name: &str,
    ddl: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let hash = calculate_ddl_hash(ddl);
    env.client
        .execute(
            "INSERT INTO pgmg.pgmg_state (object_type, object_name, ddl_hash) VALUES ($1, $2, $3)
             ON CONFLICT (object_type, object_name) DO UPDATE SET ddl_hash = $3",
            &[&object_type, &state_name, &hash],
        )
        .await?;
    Ok(())
}

async fn init_state(env: &TestEnvironment) -> Result<(), Box<dyn std::error::Error>> {
    pgmg::StateManager::new(&env.client).initialize().await?;
    Ok(())
}

async fn plan(env: &TestEnvironment) -> Result<pgmg::commands::PlanResult, Box<dyn std::error::Error>> {
    execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    )
    .await
}

#[tokio::test]
async fn function_body_only_edit_is_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "add_nums",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;
    env.write_sql_file(
        "add_nums.sql",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT b + a $$;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::Function, "add_nums", Severity::Transient);
    assert_eq!(plan.severity, Severity::Transient);
    assert_eq!(plan.severity_counts.transient, 1);
    Ok(())
}

#[tokio::test]
async fn function_signature_change_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "add_nums",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;
    // Return type widened: identity args unchanged, result shape changed
    env.write_sql_file(
        "add_nums.sql",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS bigint LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::Function, "add_nums", Severity::Breaking);
    assert_eq!(plan.severity, Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn function_added_argument_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "add_nums",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;
    env.write_sql_file(
        "add_nums.sql",
        "CREATE FUNCTION add_nums(a int, b int, c int) RETURNS int LANGUAGE sql AS $$ SELECT a + b + c $$;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::Function, "add_nums", Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn function_param_rename_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "add_nums",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;
    // Same types, same body shape — but named-notation callers of `a` break
    env.write_sql_file(
        "add_nums.sql",
        "CREATE FUNCTION add_nums(x int, b int) RETURNS int LANGUAGE sql AS $$ SELECT x + b $$;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::Function, "add_nums", Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn new_view_is_safe() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE users (id int, name text);").await?;
    env.write_sql_file("user_list.sql", "CREATE VIEW user_list AS SELECT id, name FROM users;")
        .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Safe);
    assert_eq!(plan.severity, Severity::Safe);
    assert_eq!(plan.severity_counts.safe, 1);
    Ok(())
}

#[tokio::test]
async fn view_body_only_change_is_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE users (id int, name text);").await?;
    track_object(
        &env,
        "view",
        "user_list",
        "CREATE VIEW user_list AS SELECT id, name FROM users;",
    )
    .await?;
    // Filter added: hash changes, output columns identical
    env.write_sql_file(
        "user_list.sql",
        "CREATE VIEW user_list AS SELECT id, name FROM users WHERE id > 0;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Transient);
    assert_eq!(plan.severity, Severity::Transient);
    Ok(())
}

#[tokio::test]
async fn view_shape_change_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE users (id int, name text);").await?;
    track_object(
        &env,
        "view",
        "user_list",
        "CREATE VIEW user_list AS SELECT id, name FROM users;",
    )
    .await?;
    env.write_sql_file(
        "user_list.sql",
        "CREATE VIEW user_list AS SELECT id, name, upper(name) AS shouty FROM users;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn enum_append_is_transient_and_removal_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(&env, "type", "mood", "CREATE TYPE mood AS ENUM ('sad', 'ok');").await?;
    env.write_sql_file("mood.sql", "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
        .await?;
    let appended = plan(&env).await?;
    assert_change_severity(&appended, ObjectType::Type, "mood", Severity::Transient);

    env.write_sql_file("mood.sql", "CREATE TYPE mood AS ENUM ('ok');").await?;
    let removed = plan(&env).await?;
    assert_change_severity(&removed, ObjectType::Type, "mood", Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn composite_attr_add_is_breaking_and_respelling_is_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(&env, "type", "point2", "CREATE TYPE point2 AS (x int, y int);").await?;

    // int -> int4 changes the hash but resolves to the same attribute types
    env.write_sql_file("point2.sql", "CREATE TYPE point2 AS (x int4, y int4);").await?;
    let respelled = plan(&env).await?;
    assert_change_severity(&respelled, ObjectType::Type, "point2", Severity::Transient);

    env.write_sql_file("point2.sql", "CREATE TYPE point2 AS (x int, y int, z int);").await?;
    let widened = plan(&env).await?;
    assert_change_severity(&widened, ObjectType::Type, "point2", Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn migration_severity_follows_statements() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;
    env.execute_sql("CREATE TABLE users (id int);").await?;

    env.write_migration(
        "001_additive",
        indoc! {r#"
            CREATE TABLE audit_log (id serial primary key, entry text);
            INSERT INTO audit_log (entry) VALUES ('created');
        "#},
    )
    .await?;
    env.write_migration("002_reshape", "ALTER TABLE users ADD COLUMN age int;").await?;

    let plan = plan(&env).await?;
    assert_migration_severity(&plan, "001_additive", Severity::Safe);
    assert_migration_severity(&plan, "002_reshape", Severity::Breaking);
    assert_eq!(plan.severity, Severity::Breaking);
    assert_eq!(plan.severity_counts.safe, 1);
    assert_eq!(plan.severity_counts.breaking, 1);
    Ok(())
}

#[tokio::test]
async fn deleted_view_is_breaking_and_deleted_index_is_safe() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE users (id int, name text);").await?;
    track_object(&env, "view", "user_list", "CREATE VIEW user_list AS SELECT id FROM users;").await?;
    track_object(&env, "index", "idx_users_name", "CREATE INDEX idx_users_name ON users (name);").await?;
    // sql_dir stays empty: both objects disappear from code

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Breaking);
    assert_change_severity(&plan, ObjectType::Index, "idx_users_name", Severity::Safe);
    assert_eq!(plan.severity, Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn migration_cascade_keeps_breaking_rollup() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE users (id int, name text);").await?;
    let view_ddl = "CREATE VIEW user_list AS SELECT id, name FROM users;";
    track_object(&env, "view", "user_list", view_ddl).await?;
    env.client
        .execute(
            "INSERT INTO pgmg.pgmg_dependencies
             (dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind)
             VALUES ('view', 'user_list', 'relation', 'users', 'hard')",
            &[],
        )
        .await?;
    env.write_sql_file("user_list.sql", view_ddl).await?;
    env.write_migration("001_widen", "ALTER TABLE users ADD COLUMN age int;").await?;

    let plan = plan(&env).await?;
    // The unchanged view is recreated because the migration alters its table.
    // Probed against pre-apply state its shape is identical (Transient), but
    // the migration itself keeps the rollup Breaking.
    assert_migration_severity(&plan, "001_widen", Severity::Breaking);
    assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Transient);
    assert_eq!(plan.severity, Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn empty_plan_is_safe() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    let plan = plan(&env).await?;
    assert_plan_empty(&plan);
    assert_eq!(plan.severity, Severity::Safe);
    assert_eq!(plan.severity_counts, pgmg::SeverityCounts::default());

    // JSON carries the rollup and counts at the top level
    let v = serde_json::to_value(&plan)?;
    assert_eq!(v["severity"], "safe");
    assert_eq!(v["severity_counts"]["breaking"], 0);
    Ok(())
}

#[tokio::test]
async fn apply_initiated_notify_carries_severity() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Baseline apply so the next one runs against tracked state
    env.write_migration("001_init", "CREATE TABLE users (id int, name text);").await?;
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    )
    .await?;

    // Listen on both channels on a dedicated connection, started only now so
    // the baseline apply's events are not captured
    let (listen_client, mut listen_conn) =
        tokio_postgres::connect(&env.connection_string, tokio_postgres::NoTls).await?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let mut messages =
            futures_util::stream::poll_fn(move |cx| listen_conn.poll_message(cx));
        while let Some(message) = messages.next().await {
            if let Ok(tokio_postgres::AsyncMessage::Notification(n)) = message {
                let _ = tx.send((n.channel().to_string(), n.payload().to_string()));
            }
        }
    });
    listen_client
        .batch_execute(r#"LISTEN "pgmg.apply_initiated"; LISTEN "pgmg.apply_succeeded";"#)
        .await?;

    // A breaking apply: ALTER TABLE migration
    env.write_migration("002_widen", "ALTER TABLE users ADD COLUMN age int;").await?;
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    )
    .await?;

    const RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    // apply_initiated must arrive first (emitted before the changes run),
    // carrying the plan's severity rollup
    let (channel, payload) = tokio::time::timeout(RECV_TIMEOUT, rx.recv())
        .await?
        .expect("listener closed");
    assert_eq!(channel, "pgmg.apply_initiated");
    let initiated: serde_json::Value = serde_json::from_str(&payload)?;
    assert_eq!(initiated["severity"], "breaking");
    assert_eq!(initiated["severity_counts"]["breaking"], 1);
    assert_eq!(initiated["changes"], 1);

    let (channel, _) = tokio::time::timeout(RECV_TIMEOUT, rx.recv())
        .await?
        .expect("listener closed");
    assert_eq!(channel, "pgmg.apply_succeeded");
    Ok(())
}

#[tokio::test]
async fn apply_failed_notify_pairs_with_initiated() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    // Baseline apply so the next one runs against tracked state
    env.write_migration("001_init", "CREATE TABLE users (id int, name text);").await?;
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    )
    .await?;

    let (listen_client, mut listen_conn) =
        tokio_postgres::connect(&env.connection_string, tokio_postgres::NoTls).await?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let mut messages =
            futures_util::stream::poll_fn(move |cx| listen_conn.poll_message(cx));
        while let Some(message) = messages.next().await {
            if let Ok(tokio_postgres::AsyncMessage::Notification(n)) = message {
                let _ = tx.send((n.channel().to_string(), n.payload().to_string()));
            }
        }
    });
    listen_client
        .batch_execute(
            r#"LISTEN "pgmg.apply_initiated"; LISTEN "pgmg.apply_succeeded"; LISTEN "pgmg.apply_failed";"#,
        )
        .await?;

    // A migration that fails at execution time (the table doesn't exist)
    env.write_migration("002_bad", "ALTER TABLE nonexistent_table ADD COLUMN x int;").await?;
    let apply_result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    )
    .await;
    assert!(apply_result.is_err(), "apply should fail");

    const RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    let (channel, _) = tokio::time::timeout(RECV_TIMEOUT, rx.recv())
        .await?
        .expect("listener closed");
    assert_eq!(channel, "pgmg.apply_initiated");

    let (channel, payload) = tokio::time::timeout(RECV_TIMEOUT, rx.recv())
        .await?
        .expect("listener closed");
    assert_eq!(channel, "pgmg.apply_failed");
    let failed: serde_json::Value = serde_json::from_str(&payload)?;
    assert_eq!(failed["severity"], "breaking");
    assert_eq!(failed["changes"], 1);
    assert!(
        failed["error"].as_str().unwrap().contains("nonexistent_table"),
        "error should carry the failing statement's detail: {}",
        failed["error"]
    );

    // The rollback discarded the in-transaction succeeded NOTIFY: nothing
    // further arrives
    let extra = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await;
    assert!(extra.is_err(), "no further notification expected, got {:?}", extra);
    Ok(())
}

#[tokio::test]
async fn unclassifiable_update_defaults_to_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    // State row for a function that was never actually created: the catalog
    // probe finds nothing, so classification degrades to Breaking (with a
    // warning) instead of failing the plan.
    record_state(
        &env,
        "function",
        "ghost_fn",
        "CREATE FUNCTION ghost_fn() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;",
    )
    .await?;
    env.write_sql_file(
        "ghost_fn.sql",
        "CREATE FUNCTION ghost_fn() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::Function, "ghost_fn", Severity::Breaking);
    Ok(())
}
