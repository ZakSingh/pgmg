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

async fn apply(env: &TestEnvironment) -> Result<(), Box<dyn std::error::Error>> {
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn function_body_only_edit_is_safe() -> Result<(), Box<dyn std::error::Error>> {
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

    // Replaced in place: the OID survives, so nothing a client holds breaks.
    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::Function, "add_nums", Severity::Safe);
    assert_eq!(plan.severity, Severity::Safe);
    assert_eq!(plan.severity_counts.safe, 1);
    Ok(())
}

#[tokio::test]
async fn function_recreated_with_the_same_interface_is_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    // Dropping a parameter default is something CREATE OR REPLACE refuses, so
    // the function is dropped and recreated: same interface, new OID.
    track_object(
        &env,
        "function",
        "add_nums",
        "CREATE FUNCTION add_nums(a int, b int DEFAULT 1) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;
    env.write_sql_file(
        "add_nums.sql",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
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

/// Old calls still resolve: the existing parameters come first, unchanged, and
/// the new one has a default. PostgreSQL sees a different function, so it is
/// dropped and created, never replaced in place.
#[tokio::test]
async fn function_added_argument_with_a_default_is_transient() -> Result<(), Box<dyn std::error::Error>> {
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
        "CREATE FUNCTION add_nums(a int, b int, c int DEFAULT 0) RETURNS int LANGUAGE sql AS $$ SELECT a + b + c $$;",
    )
    .await?;

    let planned = plan(&env).await?;
    assert_change_severity(&planned, ObjectType::Function, "add_nums", Severity::Transient);
    assert_eq!(planned.severity, Severity::Transient);

    apply(&env).await?;
    let old_call: i32 = env.client.query_one("SELECT add_nums(1, 2)", &[]).await?.get(0);
    assert_eq!(old_call, 3);
    let overloads: i64 = env
        .client
        .query_one("SELECT count(*) FROM pg_proc WHERE proname = 'add_nums'", &[])
        .await?
        .get(0);
    assert_eq!(overloads, 1, "the old signature must not be left behind as an overload");
    Ok(())
}

#[tokio::test]
async fn function_argument_changes_that_strand_old_calls_are_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "add_nums",
        "CREATE FUNCTION add_nums(a int, b int) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
    )
    .await?;
    for (what, ddl) in [
        (
            "a defaulted parameter placed before the existing ones",
            "CREATE FUNCTION add_nums(c text DEFAULT '', a int DEFAULT 0, b int DEFAULT 0) RETURNS int LANGUAGE sql AS $$ SELECT a + b $$;",
        ),
        (
            "an existing parameter retyped alongside a new defaulted one",
            "CREATE FUNCTION add_nums(a int, b text, c int DEFAULT 0) RETURNS int LANGUAGE sql AS $$ SELECT a + c $$;",
        ),
        (
            "an existing parameter renamed alongside a new defaulted one",
            "CREATE FUNCTION add_nums(a int, y int, c int DEFAULT 0) RETURNS int LANGUAGE sql AS $$ SELECT a + y + c $$;",
        ),
        (
            "a new defaulted parameter with a changed result type",
            "CREATE FUNCTION add_nums(a int, b int, c int DEFAULT 0) RETURNS text LANGUAGE sql AS $$ SELECT (a + b + c)::text $$;",
        ),
    ] {
        env.write_sql_file("add_nums.sql", ddl).await?;
        let planned = plan(&env).await?;
        assert_eq!(planned.severity, Severity::Breaking, "{what}");
    }
    Ok(())
}

#[tokio::test]
async fn function_result_columns_added_is_transient_and_removed_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "people",
        "CREATE FUNCTION people() RETURNS TABLE (id int, name text) LANGUAGE sql AS $$ SELECT 1, 'ann'::text $$;",
    )
    .await?;

    env.write_sql_file(
        "people.sql",
        "CREATE FUNCTION people() RETURNS TABLE (id int, age int, name text) LANGUAGE sql AS $$ SELECT 1, 30, 'ann'::text $$;",
    )
    .await?;
    let added = plan(&env).await?;
    assert_change_severity(&added, ObjectType::Function, "people", Severity::Transient);

    for (what, ddl) in [
        ("removed", "CREATE FUNCTION people() RETURNS TABLE (id int) LANGUAGE sql AS $$ SELECT 1 $$;"),
        ("retyped", "CREATE FUNCTION people() RETURNS TABLE (id int, name int) LANGUAGE sql AS $$ SELECT 1, 2 $$;"),
        ("reordered", "CREATE FUNCTION people() RETURNS TABLE (name text, id int) LANGUAGE sql AS $$ SELECT 'ann'::text, 1 $$;"),
    ] {
        env.write_sql_file("people.sql", ddl).await?;
        let planned = plan(&env).await?;
        assert_change_severity(&planned, ObjectType::Function, "people", Severity::Breaking);
        assert_eq!(planned.severity, Severity::Breaking, "{what}");
    }
    Ok(())
}

/// One OUT parameter is a scalar result; a second turns it into a row.
#[tokio::test]
async fn second_out_parameter_on_a_scalar_result_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(
        &env,
        "function",
        "one_out",
        "CREATE FUNCTION one_out(OUT total int) LANGUAGE sql AS $$ SELECT 1 $$;",
    )
    .await?;
    env.write_sql_file(
        "one_out.sql",
        "CREATE FUNCTION one_out(OUT total int, OUT extra int) LANGUAGE sql AS $$ SELECT 1, 2 $$;",
    )
    .await?;

    let planned = plan(&env).await?;
    assert_change_severity(&planned, ObjectType::Function, "one_out", Severity::Breaking);
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

/// A client describing a query is told a domain column's base type, never the
/// domain, so a view over domain-typed columns has to be compared that way.
#[tokio::test]
async fn view_with_domain_columns_is_transient_when_only_its_body_changes() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql(indoc! {"
        CREATE DOMAIN nanoid AS text CHECK (length(value) > 0);
        CREATE DOMAIN short_nanoid AS nanoid CHECK (length(value) < 30);
        CREATE DOMAIN price AS numeric(12, 2) CHECK (value >= 0);
        CREATE TABLE listing (id nanoid, short_id short_nanoid, amount price, tags nanoid[]);
    "}).await?;
    track_object(
        &env,
        "view",
        "listing_list",
        "CREATE VIEW listing_list AS SELECT id, short_id, amount, tags FROM listing;",
    )
    .await?;
    env.write_sql_file(
        "listing_list.sql",
        "CREATE VIEW listing_list AS SELECT id, short_id, amount, tags FROM listing WHERE amount > 0;",
    )
    .await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::View, "listing_list", Severity::Transient);
    assert_eq!(plan.severity, Severity::Transient);
    Ok(())
}

#[tokio::test]
async fn view_column_changing_base_type_under_a_domain_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql(indoc! {"
        CREATE DOMAIN nanoid AS text;
        CREATE DOMAIN seq_id AS bigint;
        CREATE TABLE listing (id nanoid, seq seq_id);
    "}).await?;
    track_object(&env, "view", "listing_list", "CREATE VIEW listing_list AS SELECT id AS ref FROM listing;").await?;
    // Same column name, another domain, and a different type on the wire.
    env.write_sql_file("listing_list.sql", "CREATE VIEW listing_list AS SELECT seq AS ref FROM listing;").await?;

    let plan = plan(&env).await?;
    assert_change_severity(&plan, ObjectType::View, "listing_list", Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn view_columns_added_is_transient() -> Result<(), Box<dyn std::error::Error>> {
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
    for ddl in [
        "CREATE VIEW user_list AS SELECT id, name, upper(name) AS shouty FROM users;",
        "CREATE VIEW user_list AS SELECT id, upper(name) AS shouty, name FROM users;",
    ] {
        env.write_sql_file("user_list.sql", ddl).await?;
        let plan = plan(&env).await?;
        assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Transient);
        assert_eq!(plan.severity, Severity::Transient, "{ddl}");
    }
    Ok(())
}

#[tokio::test]
async fn view_existing_column_changes_are_breaking() -> Result<(), Box<dyn std::error::Error>> {
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
    for (what, ddl) in [
        ("removed", "CREATE VIEW user_list AS SELECT id FROM users;"),
        ("renamed", "CREATE VIEW user_list AS SELECT id, name AS full_name FROM users;"),
        ("retyped", "CREATE VIEW user_list AS SELECT id::bigint AS id, name FROM users;"),
        ("reordered", "CREATE VIEW user_list AS SELECT name, id FROM users;"),
        ("removed while another is added", "CREATE VIEW user_list AS SELECT id, upper(name) AS shouty FROM users;"),
    ] {
        env.write_sql_file("user_list.sql", ddl).await?;
        let plan = plan(&env).await?;
        assert_change_severity(&plan, ObjectType::View, "user_list", Severity::Breaking);
        assert_eq!(plan.severity, Severity::Breaking, "{what}");
    }
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
async fn composite_attr_add_is_transient_and_removal_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    track_object(&env, "type", "point2", "CREATE TYPE point2 AS (x int, y int);").await?;

    // int -> int4 changes the hash but resolves to the same attribute types
    env.write_sql_file("point2.sql", "CREATE TYPE point2 AS (x int4, y int4);").await?;
    let respelled = plan(&env).await?;
    assert_change_severity(&respelled, ObjectType::Type, "point2", Severity::Transient);

    env.write_sql_file("point2.sql", "CREATE TYPE point2 AS (x int, y int, z int);").await?;
    let widened = plan(&env).await?;
    assert_change_severity(&widened, ObjectType::Type, "point2", Severity::Transient);

    for ddl in [
        "CREATE TYPE point2 AS (x int);",
        "CREATE TYPE point2 AS (x int, y text);",
        "CREATE TYPE point2 AS (y int, x int);",
        "CREATE TYPE point2 AS (x int, z int);",
    ] {
        env.write_sql_file("point2.sql", ddl).await?;
        let changed = plan(&env).await?;
        assert_change_severity(&changed, ObjectType::Type, "point2", Severity::Breaking);
    }
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
    env.write_migration("002_widen", "ALTER TABLE users ADD COLUMN age int;").await?;

    let widened = plan(&env).await?;
    assert_migration_severity(&widened, "001_additive", Severity::Safe);
    assert_migration_severity(&widened, "002_widen", Severity::Transient);
    assert_eq!(widened.severity, Severity::Transient);

    env.write_migration("003_reshape", "ALTER TABLE users DROP COLUMN id;").await?;

    let plan = plan(&env).await?;
    assert_migration_severity(&plan, "003_reshape", Severity::Breaking);
    assert_eq!(plan.severity, Severity::Breaking);
    assert_eq!(plan.severity_counts.safe, 1);
    assert_eq!(plan.severity_counts.transient, 1);
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
    env.write_migration("001_retype", "ALTER TABLE users ALTER COLUMN name TYPE varchar(50);").await?;

    let plan = plan(&env).await?;
    // The unchanged view is recreated because the migration alters its table.
    // Probed against pre-apply state its shape is identical (Transient), but
    // the migration itself keeps the rollup Breaking.
    assert_migration_severity(&plan, "001_retype", Severity::Breaking);
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

    // A breaking apply: a migration that removes a column
    env.write_migration("002_narrow", "ALTER TABLE users DROP COLUMN name;").await?;
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
    env.write_migration("002_bad", "ALTER TABLE nonexistent_table DROP COLUMN x;").await?;
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

// ---------------------------------------------------------------------------
// Views that use something the same apply adds
// ---------------------------------------------------------------------------

async fn shadow_schemas_left_behind(env: &TestEnvironment) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(env
        .client
        .query_one("SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'pgmg_shadow%'", &[])
        .await?
        .get(0))
}

/// The usual additive change: a migration adds a column and the view exposes
/// it. The view's new query cannot be prepared before the apply, so its shape
/// has to come from stand-ins.
#[tokio::test]
async fn view_exposing_a_column_the_migration_adds_is_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql(indoc! {"
        CREATE DOMAIN nanoid AS text;
        CREATE TABLE seller (id nanoid, name text NOT NULL);
    "}).await?;
    track_object(&env, "view", "seller_details", "CREATE VIEW seller_details AS SELECT s.id, s.name FROM seller s;").await?;
    env.write_sql_file(
        "seller_details.sql",
        "CREATE VIEW seller_details AS SELECT s.id, s.bnpl_enabled, s.name FROM seller s;",
    )
    .await?;
    env.write_migration(
        "001_bnpl",
        "ALTER TABLE seller ADD COLUMN bnpl_enabled boolean NOT NULL DEFAULT false;",
    )
    .await?;

    let planned = plan(&env).await?;
    assert_migration_severity(&planned, "001_bnpl", Severity::Transient);
    assert_change_severity(&planned, ObjectType::View, "seller_details", Severity::Transient);
    assert_eq!(planned.severity, Severity::Transient);
    assert_eq!(shadow_schemas_left_behind(&env).await?, 0, "planning must leave nothing behind");
    let columns: i64 = env
        .client
        .query_one("SELECT count(*) FROM pg_attribute WHERE attrelid = 'seller'::regclass AND attnum > 0", &[])
        .await?
        .get(0);
    assert_eq!(columns, 2, "planning must not touch the real table");

    apply(&env).await?;
    env.client.query("SELECT id, bnpl_enabled, name FROM seller_details", &[]).await?;
    Ok(())
}

/// The new column travels through a chain of views in two schemas, and one of
/// them is a materialized view.
#[tokio::test]
async fn views_downstream_of_a_view_that_gains_a_column_are_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql(indoc! {"
        CREATE SCHEMA core;
        CREATE SCHEMA api;
        CREATE TABLE core.listing (id int, price numeric);
    "}).await?;
    track_object(&env, "view", "core.listing_stats", "CREATE VIEW core.listing_stats AS SELECT l.id, l.price FROM core.listing l;").await?;
    track_object(&env, "materialized_view", "core.listing_cache", "CREATE MATERIALIZED VIEW core.listing_cache AS SELECT ls.id, ls.price FROM core.listing_stats ls;").await?;
    track_object(&env, "view", "api.inventory", "CREATE VIEW api.inventory AS SELECT lc.id, lc.price FROM core.listing_cache lc;").await?;

    env.write_sql_file("a_inventory.sql", "CREATE VIEW api.inventory AS SELECT lc.id, lc.price, lc.views_30d FROM core.listing_cache lc;").await?;
    env.write_sql_file("b_listing_cache.sql", "CREATE MATERIALIZED VIEW core.listing_cache AS SELECT ls.id, ls.price, ls.views_30d FROM core.listing_stats ls;").await?;
    env.write_sql_file("c_listing_stats.sql", "CREATE VIEW core.listing_stats AS SELECT l.id, l.price, l.views_30d FROM core.listing l;").await?;
    env.write_migration("001_views", "ALTER TABLE core.listing ADD COLUMN views_30d int;").await?;

    let planned = plan(&env).await?;
    assert_change_severity(&planned, ObjectType::View, "listing_stats", Severity::Transient);
    assert_change_severity(&planned, ObjectType::MaterializedView, "listing_cache", Severity::Transient);
    assert_change_severity(&planned, ObjectType::View, "inventory", Severity::Transient);
    assert_eq!(planned.severity, Severity::Transient);
    assert_eq!(shadow_schemas_left_behind(&env).await?, 0);

    apply(&env).await?;
    Ok(())
}

/// The added column is of a type the same migration creates.
#[tokio::test]
async fn view_exposing_a_new_column_of_a_new_type_is_transient() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE item (id int, name text);").await?;
    track_object(&env, "view", "item_details", "CREATE VIEW item_details AS SELECT i.id, i.name FROM item i;").await?;
    env.write_sql_file(
        "item_details.sql",
        "CREATE VIEW item_details AS SELECT i.id, i.name, i.reason FROM item i;",
    )
    .await?;
    env.write_migration(
        "001_reason",
        indoc! {"
            CREATE TYPE blacklist_reason AS ENUM ('spam', 'fake');
            ALTER TABLE item ADD COLUMN reason blacklist_reason;
        "},
    )
    .await?;

    let planned = plan(&env).await?;
    assert_change_severity(&planned, ObjectType::View, "item_details", Severity::Transient);
    assert_eq!(planned.severity, Severity::Transient);
    assert_eq!(shadow_schemas_left_behind(&env).await?, 0);
    let leaked: i64 = env
        .client
        .query_one("SELECT count(*) FROM pg_type WHERE typname = 'blacklist_reason'", &[])
        .await?
        .get(0);
    assert_eq!(leaked, 0, "planning must not create the migration's type");

    apply(&env).await?;
    Ok(())
}

/// Using a new column does not excuse what the view does to its old ones.
#[tokio::test]
async fn view_using_a_new_column_while_changing_old_ones_is_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE seller (id int, name text);").await?;
    track_object(&env, "view", "seller_details", "CREATE VIEW seller_details AS SELECT s.id, s.name FROM seller s;").await?;
    env.write_migration("001_bnpl", "ALTER TABLE seller ADD COLUMN bnpl_enabled boolean;").await?;

    for (what, ddl) in [
        ("removed", "CREATE VIEW seller_details AS SELECT s.id, s.bnpl_enabled FROM seller s;"),
        ("retyped", "CREATE VIEW seller_details AS SELECT s.id::text AS id, s.name, s.bnpl_enabled FROM seller s;"),
        ("renamed", "CREATE VIEW seller_details AS SELECT s.id, s.name AS label, s.bnpl_enabled FROM seller s;"),
        ("reordered", "CREATE VIEW seller_details AS SELECT s.name, s.id, s.bnpl_enabled FROM seller s;"),
    ] {
        env.write_sql_file("seller_details.sql", ddl).await?;
        let planned = plan(&env).await?;
        assert_change_severity(&planned, ObjectType::View, "seller_details", Severity::Breaking);
        assert_eq!(planned.severity, Severity::Breaking, "{what}");
    }
    assert_eq!(shadow_schemas_left_behind(&env).await?, 0);
    Ok(())
}

/// When no stand-in can be built, the conservative answer stands.
#[tokio::test]
async fn view_that_cannot_be_checked_stays_breaking() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    init_state(&env).await?;

    env.execute_sql("CREATE TABLE seller (id int, name text);").await?;
    track_object(&env, "view", "seller_details", "CREATE VIEW seller_details AS SELECT s.id, s.name FROM seller s;").await?;
    // Uses a column nothing adds: no stand-in can be built for it.
    env.write_sql_file(
        "seller_details.sql",
        "CREATE VIEW seller_details AS SELECT s.id, s.name, s.missing FROM seller s;",
    )
    .await?;

    let planned = plan(&env).await?;
    assert_change_severity(&planned, ObjectType::View, "seller_details", Severity::Breaking);
    assert_eq!(shadow_schemas_left_behind(&env).await?, 0);
    Ok(())
}
