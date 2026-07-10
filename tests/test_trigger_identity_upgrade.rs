mod common;

use common::TestEnvironment;
use pgmg::commands::{execute_apply, execute_plan};
use pgmg::config::PgmgConfig;
use indoc::indoc;

const TRIGGER_FUNCTION: &str = indoc! {r#"
    CREATE OR REPLACE FUNCTION update_modified()
    RETURNS trigger AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
"#};

const TRIGGER_ON_TABLE1: &str = indoc! {r#"
    CREATE TRIGGER update_timestamp
    BEFORE UPDATE ON table1
    FOR EACH ROW EXECUTE FUNCTION update_modified();
"#};

const TRIGGER_ON_TABLE2: &str = indoc! {r#"
    CREATE TRIGGER update_timestamp
    BEFORE UPDATE ON table2
    FOR EACH ROW EXECUTE FUNCTION update_modified();
"#};

async fn setup_two_tables(env: &TestEnvironment) -> Result<(), Box<dyn std::error::Error>> {
    env.execute_sql("CREATE TABLE table1 (id SERIAL PRIMARY KEY, updated_at TIMESTAMPTZ)").await?;
    env.execute_sql("CREATE TABLE table2 (id SERIAL PRIMARY KEY, updated_at TIMESTAMPTZ)").await?;
    Ok(())
}

/// A single legacy trigger row with one relation dependency is renamed
/// in place to the composite key, and the unchanged trigger plans clean.
#[tokio::test]
async fn test_legacy_single_trigger_upgraded_in_place() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    setup_two_tables(&env).await?;

    env.write_sql_file("func.sql", TRIGGER_FUNCTION).await?;
    env.write_sql_file("trigger1.sql", TRIGGER_ON_TABLE1).await?;

    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result.errors.is_empty(), "apply failed: {:?}", apply_result.errors);

    // Rewrite the state rows to the legacy (pre table-in-identity) format
    env.execute_sql(
        "UPDATE pgmg.pgmg_state SET object_name = 'update_timestamp' \
         WHERE object_type = 'trigger' AND object_name = 'update_timestamp:table1'"
    ).await?;
    env.execute_sql(
        "UPDATE pgmg.pgmg_dependencies SET dependent_name = 'update_timestamp' \
         WHERE dependent_type = 'trigger' AND dependent_name = 'update_timestamp:table1'"
    ).await?;

    // Plan runs initialize(), which upgrades the legacy rows
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;

    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("trigger".to_string(), "update_timestamp:table1".to_string())),
        "legacy row should be upgraded to composite: {:?}", tracked);
    assert!(!tracked.contains(&("trigger".to_string(), "update_timestamp".to_string())),
        "legacy row should be gone: {:?}", tracked);

    // Hash carried over → the unchanged trigger produces no plan changes
    assert!(plan.changes.is_empty(), "expected empty plan, got {} changes", plan.changes.len());

    // Dependencies rewritten under the composite identity
    let dep_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pgmg.pgmg_dependencies \
         WHERE dependent_type = 'trigger' AND dependent_name = 'update_timestamp:table1'"
    ).await?;
    assert_eq!(dep_count, 2, "relation + function deps should be under the composite key");
    let legacy_dep_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pgmg.pgmg_dependencies \
         WHERE dependent_type = 'trigger' AND dependent_name = 'update_timestamp'"
    ).await?;
    assert_eq!(legacy_dep_count, 0, "no legacy dependency rows should remain");

    // Upgrade is idempotent
    let plan2 = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert!(plan2.changes.is_empty());

    Ok(())
}

/// The reported staging scenario: two same-named triggers collided on one
/// legacy state row whose hash matches only one of them, while both relation
/// deps were recorded under the shared name. The upgrade expands the row per
/// relation dep; the hash-matching trigger plans clean and the other is
/// recreated against the CORRECT table.
#[tokio::test]
async fn test_legacy_collided_triggers_expand_and_self_heal() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    setup_two_tables(&env).await?;

    env.write_sql_file("func.sql", TRIGGER_FUNCTION).await?;
    env.write_sql_file("trigger1.sql", TRIGGER_ON_TABLE1).await?;
    env.write_sql_file("trigger2.sql", TRIGGER_ON_TABLE2).await?;

    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result.errors.is_empty(), "apply failed: {:?}", apply_result.errors);

    // Simulate the legacy collision: one state row keyed by bare name holding
    // trigger1's hash, and both relation deps merged under that name.
    env.execute_sql(
        "DELETE FROM pgmg.pgmg_state \
         WHERE object_type = 'trigger' AND object_name = 'update_timestamp:table2'"
    ).await?;
    env.execute_sql(
        "UPDATE pgmg.pgmg_state SET object_name = 'update_timestamp' \
         WHERE object_type = 'trigger' AND object_name = 'update_timestamp:table1'"
    ).await?;
    env.execute_sql("DELETE FROM pgmg.pgmg_dependencies WHERE dependent_type = 'trigger'").await?;
    env.execute_sql(
        "INSERT INTO pgmg.pgmg_dependencies \
         (dependent_type, dependent_name, dependency_type, dependency_name, dependency_kind) VALUES \
         ('trigger', 'update_timestamp', 'relation', 'table1', 'hard'), \
         ('trigger', 'update_timestamp', 'relation', 'table2', 'hard'), \
         ('trigger', 'update_timestamp', 'function', 'update_modified', 'hard')"
    ).await?;

    let table1_oid_before: u32 = env.query_scalar(
        "SELECT t.oid FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE t.tgname = 'update_timestamp' AND c.relname = 'table1'"
    ).await?;

    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result2.errors.is_empty(), "apply failed: {:?}", apply_result2.errors);

    // Both triggers still live, each on its own table
    let live_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'update_timestamp'"
    ).await?;
    assert_eq!(live_count, 2);

    // trigger1's hash matched the expanded row — it must not have been touched
    let table1_oid_after: u32 = env.query_scalar(
        "SELECT t.oid FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE t.tgname = 'update_timestamp' AND c.relname = 'table1'"
    ).await?;
    assert_eq!(table1_oid_before, table1_oid_after, "table1's trigger must survive the upgrade untouched");

    // State converged to two correct composite rows
    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("trigger".to_string(), "update_timestamp:table1".to_string())));
    assert!(tracked.contains(&("trigger".to_string(), "update_timestamp:table2".to_string())));
    assert!(!tracked.contains(&("trigger".to_string(), "update_timestamp".to_string())));

    // And everything now plans clean
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert!(plan.changes.is_empty(), "expected empty plan after self-heal, got {} changes", plan.changes.len());

    Ok(())
}

/// A legacy trigger row with no recorded relation dependency can't be
/// attributed to a table; it is dropped and the trigger re-plans as a create,
/// which succeeds even though the trigger is still live (defensive pre-drop).
#[tokio::test]
async fn test_legacy_trigger_without_relation_dep_recreated() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    setup_two_tables(&env).await?;

    env.write_sql_file("func.sql", TRIGGER_FUNCTION).await?;
    env.write_sql_file("trigger1.sql", TRIGGER_ON_TABLE1).await?;

    let apply_result = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result.errors.is_empty(), "apply failed: {:?}", apply_result.errors);

    // Legacy row with its dependency records lost entirely
    env.execute_sql(
        "UPDATE pgmg.pgmg_state SET object_name = 'update_timestamp' \
         WHERE object_type = 'trigger' AND object_name = 'update_timestamp:table1'"
    ).await?;
    env.execute_sql("DELETE FROM pgmg.pgmg_dependencies WHERE dependent_type = 'trigger'").await?;

    // The trigger is still live in PG; the untracked file object re-plans as a
    // create and must not fail with "already exists"
    let apply_result2 = execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result2.errors.is_empty(), "recreate failed: {:?}", apply_result2.errors);
    assert!(apply_result2.objects_created.contains(&"update_timestamp".to_string()),
        "trigger should be re-created: {:?}", apply_result2.objects_created);

    let live_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'update_timestamp'"
    ).await?;
    assert_eq!(live_count, 1);

    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("trigger".to_string(), "update_timestamp:table1".to_string())));

    Ok(())
}
