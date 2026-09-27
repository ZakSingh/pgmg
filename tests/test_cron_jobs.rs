//! pg_cron job handling.
//!
//! The test container has no pg_cron, so these tests install a stand-in `cron`
//! schema whose `schedule`/`unschedule` behave like pg_cron >= 1.3 (named
//! upsert; unschedule errors on a missing row) and record every call in
//! `cron.calls`. That is enough to pin down what pgmg sends to pg_cron.

mod common;

use common::{TestEnvironment, assertions::*};
use pgmg::commands::{execute_apply, execute_plan, ChangeOperation};
use pgmg::config::PgmgConfig;
use pgmg::sql::ObjectType;
use indoc::indoc;

const FAKE_PG_CRON: &str = indoc! {r#"
    CREATE SCHEMA cron;
    CREATE TABLE cron.job (
        jobid    bigserial PRIMARY KEY,
        jobname  text UNIQUE NOT NULL,
        schedule text NOT NULL,
        command  text NOT NULL
    );
    CREATE TABLE cron.calls (fn text NOT NULL, jobname text NOT NULL);

    CREATE FUNCTION cron.schedule(p_name text, p_schedule text, p_command text) RETURNS bigint
    LANGUAGE plpgsql AS $$
    DECLARE id bigint;
    BEGIN
        INSERT INTO cron.calls VALUES ('schedule', p_name);
        INSERT INTO cron.job (jobname, schedule, command) VALUES (p_name, p_schedule, p_command)
        ON CONFLICT (jobname) DO UPDATE SET schedule = EXCLUDED.schedule, command = EXCLUDED.command
        RETURNING jobid INTO id;
        RETURN id;
    END $$;

    CREATE FUNCTION cron.unschedule(p_name text) RETURNS boolean
    LANGUAGE plpgsql AS $$
    BEGIN
        INSERT INTO cron.calls VALUES ('unschedule', p_name);
        DELETE FROM cron.job WHERE jobname = p_name;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'could not find valid entry for job ''%''', p_name;
        END IF;
        RETURN true;
    END $$;
"#};

const FUNCTION_V1: &str = indoc! {r#"
    CREATE OR REPLACE FUNCTION refresh_counts() RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
        PERFORM 1;
    END;
    $$;
"#};

const FUNCTION_V2: &str = indoc! {r#"
    CREATE OR REPLACE FUNCTION refresh_counts() RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
        PERFORM 2;
    END;
    $$;
"#};

const JOB_V1: &str =
    "SELECT cron.schedule('refresh_counts_job', '*/5 * * * *', 'SELECT refresh_counts()');\n";

const JOB_V2: &str =
    "SELECT cron.schedule('refresh_counts_job', '*/10 * * * *', 'SELECT refresh_counts()');\n";

async fn calls(env: &TestEnvironment, fn_name: &str) -> Result<i64, Box<dyn std::error::Error>> {
    env.query_scalar(&format!("SELECT count(*) FROM cron.calls WHERE fn = '{}'", fn_name)).await
}

async fn jobid(env: &TestEnvironment, name: &str) -> Result<i64, Box<dyn std::error::Error>> {
    env.query_scalar(&format!("SELECT jobid FROM cron.job WHERE jobname = '{}'", name)).await
}

async fn apply(env: &TestEnvironment) -> Result<pgmg::commands::ApplyResult, Box<dyn std::error::Error>> {
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await
}

async fn plan(env: &TestEnvironment) -> Result<pgmg::commands::PlanResult, Box<dyn std::error::Error>> {
    execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await
}

fn cron_job_changes(plan: &pgmg::commands::PlanResult) -> Vec<&ChangeOperation> {
    plan.changes.iter().filter(|change| match change {
        ChangeOperation::CreateObject { object, .. }
        | ChangeOperation::UpdateObject { object, .. } => object.object_type == ObjectType::CronJob,
        ChangeOperation::DeleteObject { object_type, .. } => *object_type == ObjectType::CronJob,
        ChangeOperation::ApplyMigration { .. } => false,
    }).collect()
}

/// Function + job scheduled once; returns the job's id.
async fn bootstrap(env: &TestEnvironment) -> Result<i64, Box<dyn std::error::Error>> {
    env.execute_sql(FAKE_PG_CRON).await?;
    env.write_sql_file("refresh_counts.sql", FUNCTION_V1).await?;
    env.write_sql_file("refresh_counts_job.sql", JOB_V1).await?;

    let result = apply(env).await?;
    assert_apply_successful(&result);
    assert_objects_created(&result, &["refresh_counts", "refresh_counts_job"]);
    assert_eq!(calls(env, "schedule").await?, 1);
    assert_eq!(calls(env, "unschedule").await?, 0);

    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("cron_job".to_string(), "refresh_counts_job".to_string())));

    jobid(env, "refresh_counts_job").await
}

#[tokio::test]
async fn test_function_change_does_not_reschedule_dependent_cron_job() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let id = bootstrap(&env).await?;

    env.write_sql_file("refresh_counts.sql", FUNCTION_V2).await?;

    let plan = plan(&env).await?;
    assert_plan_contains_update(&plan, ObjectType::Function, "refresh_counts");
    assert!(
        cron_job_changes(&plan).is_empty(),
        "a function change must not touch the cron job that calls it, got {:?}",
        cron_job_changes(&plan)
    );

    let result = apply(&env).await?;
    assert_apply_successful(&result);
    assert_objects_updated(&result, &["refresh_counts"]);
    assert_eq!(calls(&env, "schedule").await?, 1, "cron.schedule must not be re-run");
    assert_eq!(calls(&env, "unschedule").await?, 0, "cron.unschedule must not be run");
    assert_eq!(jobid(&env, "refresh_counts_job").await?, id);

    Ok(())
}

#[tokio::test]
async fn test_cron_job_change_upserts_without_unschedule() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    let id = bootstrap(&env).await?;

    env.write_sql_file("refresh_counts_job.sql", JOB_V2).await?;

    let plan = plan(&env).await?;
    assert_plan_contains_update(&plan, ObjectType::CronJob, "refresh_counts_job");
    assert_eq!(plan.changes.len(), 1, "only the job should change: {:?}", plan.changes);

    let result = apply(&env).await?;
    assert_apply_successful(&result);
    assert_objects_updated(&result, &["refresh_counts_job"]);
    assert_eq!(calls(&env, "schedule").await?, 2);
    assert_eq!(calls(&env, "unschedule").await?, 0, "updates rely on the named upsert, not unschedule");
    assert_eq!(jobid(&env, "refresh_counts_job").await?, id, "jobid (and run history) must survive");

    let schedule: String = env.query_scalar(
        "SELECT schedule FROM cron.job WHERE jobname = 'refresh_counts_job'"
    ).await?;
    assert_eq!(schedule, "*/10 * * * *");

    Ok(())
}

#[tokio::test]
async fn test_deleting_function_still_used_by_cron_job_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    bootstrap(&env).await?;

    env.delete_sql_file("refresh_counts.sql").await?;

    let err = plan(&env).await.err().expect("plan must refuse to delete a function a cron job calls");
    let msg = err.to_string();
    assert!(msg.contains("refresh_counts") && msg.contains("depends on it"), "unexpected error: {}", msg);

    Ok(())
}

#[tokio::test]
async fn test_deleting_cron_job_file_unschedules() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    bootstrap(&env).await?;

    env.delete_sql_file("refresh_counts_job.sql").await?;

    let plan = plan(&env).await?;
    assert_plan_contains_delete(&plan, ObjectType::CronJob, "refresh_counts_job");

    let result = apply(&env).await?;
    assert_apply_successful(&result);
    assert_objects_deleted(&result, &["refresh_counts_job"]);
    assert_eq!(calls(&env, "unschedule").await?, 1);

    let remaining: i64 = env.query_scalar("SELECT count(*) FROM cron.job").await?;
    assert_eq!(remaining, 0);
    let tracked = env.get_tracked_objects().await?;
    assert!(!tracked.iter().any(|(t, _)| t == "cron_job"));

    Ok(())
}

#[tokio::test]
async fn test_deleting_cron_job_tolerates_missing_row() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    bootstrap(&env).await?;

    // Someone unscheduled it by hand; cron.unschedule will now raise.
    env.execute_sql("DELETE FROM cron.job").await?;
    env.delete_sql_file("refresh_counts_job.sql").await?;

    let result = apply(&env).await?;
    assert_apply_successful(&result);
    assert_objects_deleted(&result, &["refresh_counts_job"]);

    // The function, updated in the same transaction, must not have been rolled back with it.
    let tracked = env.get_tracked_objects().await?;
    assert!(!tracked.iter().any(|(t, _)| t == "cron_job"));
    assert!(tracked.contains(&("function".to_string(), "refresh_counts".to_string())));

    Ok(())
}

#[tokio::test]
async fn test_migration_altering_table_does_not_reschedule_cron_job() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    env.execute_sql(FAKE_PG_CRON).await?;

    env.write_migration("001_events", "CREATE TABLE events (id int PRIMARY KEY);").await?;
    env.write_sql_file("events_count.sql", "CREATE VIEW events_count AS SELECT count(*) AS n FROM events;\n").await?;
    env.write_sql_file("vacuum_events_job.sql",
        "SELECT cron.schedule('vacuum_events', '0 2 * * *', 'VACUUM ANALYZE events');\n").await?;

    let result = apply(&env).await?;
    assert_apply_successful(&result);
    assert_eq!(calls(&env, "schedule").await?, 1);

    // The job's stored dependency on the table is what the migration path looks up.
    let deps: i64 = env.query_scalar(
        "SELECT count(*) FROM pgmg.pgmg_dependencies \
         WHERE dependent_type = 'cron_job' AND dependent_name = 'vacuum_events' AND dependency_type = 'relation'"
    ).await?;
    assert_eq!(deps, 1, "cron job should record its relation dependency");

    env.write_migration("002_events_kind", "ALTER TABLE events ADD COLUMN kind text;").await?;

    let plan = plan(&env).await?;
    assert_plan_contains_migration(&plan, "002_events_kind");
    // The view really depends on the table's shape and is recreated ...
    assert_plan_contains_update(&plan, ObjectType::View, "events_count");
    // ... the job's command text does not, so it is left alone.
    assert!(
        cron_job_changes(&plan).is_empty(),
        "an altered table must not reschedule a cron job naming it, got {:?}",
        cron_job_changes(&plan)
    );

    let result = apply(&env).await?;
    assert_apply_successful(&result);
    assert_eq!(calls(&env, "schedule").await?, 1);
    assert_eq!(calls(&env, "unschedule").await?, 0);

    Ok(())
}
