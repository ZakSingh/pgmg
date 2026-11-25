//! Edge cases of leaving objects in place instead of dropping and recreating
//! them: situations where an object that stays put is nevertheless bound to,
//! or broken by, something the same apply changes.
//!
//! Every scenario that is expected to apply goes through `common::evolution`,
//! which also requires the result to equal a fresh build of the files.

mod common;

use common::evolution::*;
use indoc::indoc;
use pgmg::analysis::Severity;
use pgmg::commands::ChangeOperation;

const T: &str = "create table t (id int primary key, a int, b int);";

fn project() -> Project {
    Project::default().migration("001_t", T)
}

/// (in_place, severity) of the planned update of `name`.
fn planned_update(step: &Step, name: &str) -> (bool, Severity) {
    step.plan.changes.iter().find_map(|change| match change {
        ChangeOperation::UpdateObject { object, in_place, severity, .. }
            if pgmg::sql::format_qualified_name(&object.qualified_name) == name => Some((*in_place, *severity)),
        _ => None,
    }).unwrap_or_else(|| panic!("no update of {name} in the plan"))
}

/// Apply `next` on top of `evolution`, expecting it to fail and leave the
/// database as it was. Returns the errors reported.
async fn failed_apply(evolution: &Evolution, next: &Project) -> Result<String, Box<dyn std::error::Error>> {
    let before = schema_fingerprint(&evolution.env).await?;
    next.write_to(&evolution.env)?;
    let outcome = apply(&evolution.env).await;
    let message = match outcome {
        Ok(result) => {
            assert!(!result.errors.is_empty(), "the apply succeeded");
            result.errors.join("\n")
        }
        Err(e) => e.to_string(),
    };
    assert_eq!(schema_fingerprint(&evolution.env).await?, before, "a failed apply changed the database");
    Ok(message)
}

// ---------------------------------------------------------------------------
// 1. SQL-language functions left in place are still validated
// ---------------------------------------------------------------------------

const GET_A_FROM_T: &str = "create function get_a() returns int language sql as $$ select a from t limit 1 $$;";

#[tokio::test]
async fn sql_function_broken_by_a_dropped_column_fails_the_apply() -> TestResult {
    let base = project().file("get_a.sql", GET_A_FROM_T);
    let evolution = Evolution::start(&base).await?;

    // The function only reads `t` from its body, so nothing stops the column
    // from being dropped; without a re-validation the apply would succeed and
    // leave a function that fails when called.
    failed_apply(&evolution, &base.clone().migration("002_drop_a", "alter table t drop column a;")).await?;
    assert_eq!(count(&evolution.env, "select count(get_a())").await?, 0);
    Ok(())
}

#[tokio::test]
async fn sql_function_broken_by_an_edited_view_fails_the_apply() -> TestResult {
    let base = project()
        .file("v.sql", "create view v as select id, a, b from t;")
        .file("get_a.sql", "create function get_a() returns int language sql as $$ select a from v limit 1 $$;");
    let evolution = Evolution::start(&base).await?;

    failed_apply(&evolution, &base.clone().file("v.sql", "create view v as select id, b from t;")).await?;
    assert_eq!(count(&evolution.env, "select count(get_a())").await?, 0);
    Ok(())
}

#[tokio::test]
async fn sql_function_broken_by_a_replaced_table_fails_the_apply() -> TestResult {
    let base = project().file("get_a.sql", GET_A_FROM_T);
    let evolution = Evolution::start(&base).await?;

    failed_apply(&evolution, &base.clone().migration(
        "002_replace_t",
        "drop table t; create table t (id int primary key, b int);",
    )).await?;
    Ok(())
}

#[tokio::test]
async fn sql_function_that_still_validates_keeps_its_identity() -> TestResult {
    let base = project()
        .file("v.sql", "create view v as select id, a, b from t;")
        .file("get_a.sql", GET_A_FROM_T)
        .file("get_b.sql", "create function get_b() returns int language sql as $$ select b from v limit 1 $$;");
    let evolution = Evolution::start(&base).await?;
    evolution.env.execute_sql("revoke execute on function get_a() from public").await?;
    let acl_before: String = evolution.env.query_scalar("select proacl::text from pg_proc where oid = 'get_a'::regproc").await?;

    // Re-validation must not cost the functions their OID or privileges.
    let step = evolution.advance(
        &base.clone()
            .migration("002_drop_b", "alter table t alter column a type bigint;")
            .file("v.sql", "create view v as select id, b from t;"),
    ).await?;
    step.assert_left_in_place(&["function public.get_a", "function public.get_b"]);
    let acl: String = evolution.env.query_scalar("select proacl::text from pg_proc where oid = 'get_a'::regproc").await?;
    assert_eq!(acl, acl_before, "privileges on get_a were reset");
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. A migration that drops or renames a table takes what is bound to its
//    row type along, even a function that could otherwise be replaced in place
// ---------------------------------------------------------------------------

const LIST_T: &str = "create function list_t() returns setof t language sql as $$ select * from t $$;";
const LIST_T_EDITED: &str = "create function list_t() returns setof t language sql as $$ select * from t order by id $$;";

#[tokio::test]
async fn edited_function_returning_a_table_the_migration_drops_and_recreates() -> TestResult {
    let base = project().file("list_t.sql", LIST_T);
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(
        &base.clone()
            .migration("002_replace_t", "drop table t; create table t (id bigint primary key, a text, c int);")
            .file("list_t.sql", LIST_T_EDITED),
    ).await?;
    step.assert_recreated(&["function public.list_t"]);
    // Not announced as a harmless in-place replacement.
    assert!(!planned_update(&step, "list_t").0);
    Ok(())
}

#[tokio::test]
async fn edited_function_returning_a_table_the_migration_renames_aside() -> TestResult {
    let base = project().file("list_t.sql", LIST_T);
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(
        &base.clone()
            .migration("002_replace_t", "alter table t rename to t_old; create table t (id bigint primary key, a text, c int);")
            .file("list_t.sql", LIST_T_EDITED),
    ).await?;
    step.assert_recreated(&["function public.list_t"]);
    Ok(())
}

#[tokio::test]
async fn unedited_function_returning_a_table_the_migration_renames_aside() -> TestResult {
    let base = project().file("list_t.sql", LIST_T);
    let evolution = Evolution::start(&base).await?;

    // Left alone, the function would keep returning the renamed table's rows.
    let step = evolution.advance(
        &base.clone()
            .migration("002_replace_t", "alter table t rename to t_old; create table t (id bigint primary key, a text, c int);"),
    ).await?;
    step.assert_recreated(&["function public.list_t"]);
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. What has to be dropped is decided by the installed definition
// ---------------------------------------------------------------------------

#[tokio::test]
async fn function_converted_from_a_sql_standard_body_while_its_column_is_retyped() -> TestResult {
    let base = project().file(
        "get_a.sql",
        "create function get_a() returns bigint language sql begin atomic select a::bigint from t limit 1; end;",
    );
    let evolution = Evolution::start(&base).await?;

    // The installed body is bound to t.a, whatever the new one looks like.
    let step = evolution.advance(
        &base.clone()
            .migration("002_retype", "alter table t alter column a type bigint;")
            .file("get_a.sql", "create function get_a() returns bigint language sql as $$ select a::bigint from t limit 1 $$;"),
    ).await?;
    step.assert_recreated(&["function public.get_a"]);
    Ok(())
}

#[tokio::test]
async fn function_converted_from_a_sql_standard_body_while_its_view_is_deleted() -> TestResult {
    let base = project()
        .file("v.sql", "create view v as select id, a from t;")
        .file("get_a.sql", "create function get_a() returns int language sql begin atomic select a from v limit 1; end;");
    let evolution = Evolution::start(&base).await?;

    let mut next = base.clone()
        .file("get_a.sql", "create function get_a() returns int language sql as $$ select a from t limit 1 $$;");
    next.files.remove("v.sql");
    let step = evolution.advance(&next).await?;
    step.assert_recreated(&["function public.get_a"]);
    Ok(())
}

#[tokio::test]
async fn sql_standard_body_that_stops_using_a_deleted_view() -> TestResult {
    let base = project()
        .file("v.sql", "create view v as select id, a from t;")
        .file("get_a.sql", "create function get_a() returns int language sql begin atomic select a from v limit 1; end;");
    let evolution = Evolution::start(&base).await?;

    // Both bodies are SQL-standard and the interface is unchanged, but the
    // installed one still holds the view in place.
    let mut next = base.clone()
        .file("get_a.sql", "create function get_a() returns int language sql begin atomic select a from t limit 1; end;");
    next.files.remove("v.sql");
    let step = evolution.advance(&next).await?;
    step.assert_recreated(&["function public.get_a"]);
    Ok(())
}

#[tokio::test]
async fn sql_standard_body_is_bound_to_the_functions_it_calls() -> TestResult {
    let base = project()
        .file("g.sql", "create function g(x int) returns int language sql as $$ select x $$;")
        .file("f.sql", "create function f() returns int language sql begin atomic select g(1); end;");
    let evolution = Evolution::start(&base).await?;

    // g's interface changes, so it is dropped; f's parsed body pins it.
    let step = evolution.advance(
        &base.clone().file("g.sql", "create function g(x int, y int default 0) returns int language sql as $$ select x + y $$;"),
    ).await?;
    step.assert_recreated(&["function public.f", "function public.g"]);
    Ok(())
}

// ---------------------------------------------------------------------------
// 4 & 5. Star expansions an added column changes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn added_column_reaches_a_star_over_a_function_returning_the_table() -> TestResult {
    let base = project()
        .file("list_t.sql", "create function list_t() returns setof t language sql stable as $$ select * from t $$;")
        .file("v.sql", "create view v as select * from list_t();")
        .file("ids.sql", "create view ids as select l.id from list_t() l;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone().migration("002_add_c", "alter table t add column c int;")).await?;
    // A view over the function recorded its result columns when it was
    // created, whether it expands them with a star or names the ones it
    // wants. The function itself follows the table and stays.
    step.assert_recreated(&["view public.ids", "view public.v"]);
    step.assert_left_in_place(&["function public.list_t"]);
    Ok(())
}

#[tokio::test]
async fn added_column_reaches_a_view_defined_with_the_table_command() -> TestResult {
    let base = project().file("v.sql", "create view v as table t;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone().migration("002_add_c", "alter table t add column c int;")).await?;
    step.assert_recreated(&["view public.v"]);
    Ok(())
}

// ---------------------------------------------------------------------------
// Smaller gaps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stored_results_are_rebuilt_through_a_chain_of_function_calls() -> TestResult {
    let base = project()
        .file("base.sql", "create function base() returns int language sql immutable as $$ select 1 $$;")
        .file("wrap.sql", "create function wrap() returns int language sql immutable as $$ select base() $$;")
        .file("mv.sql", "create materialized view mv as select wrap() as n;")
        .file("plain.sql", "create view plain as select wrap() as n;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(
        &base.clone().file("base.sql", "create function base() returns int language sql immutable as $$ select 2 $$;"),
    ).await?;
    // wrap() computes something else now, so what stores its results is
    // stale; nothing else moves.
    step.assert_recreated(&["materialized view public.mv"]);
    assert_eq!(count(&evolution.env, "select n::bigint from mv").await?, 2);
    Ok(())
}

#[tokio::test]
async fn view_joining_a_table_the_same_migration_creates_is_additive() -> TestResult {
    let base = project().file("v.sql", "create view v as select t.id, t.a from t;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(
        &base.clone()
            .migration("002_u", "create table u (id int primary key references t (id), note text not null default '');")
            .file("v.sql", "create view v as select t.id, t.a, u.note from t left join u on u.id = t.id;"),
    ).await?;
    assert_eq!(planned_update(&step, "v").1, Severity::Transient);
    assert_eq!(step.plan.severity, Severity::Transient);
    Ok(())
}

#[tokio::test]
async fn view_over_a_new_table_that_loses_a_column_is_still_breaking() -> TestResult {
    let base = project().file("v.sql", "create view v as select t.id, t.a from t;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(
        &base.clone()
            .migration("002_u", "create table u (id int, note text); alter table u add column extra int;")
            .file("v.sql", "create view v as select t.id, u.note, u.extra from t left join u on u.id = t.id;"),
    ).await?;
    assert_eq!(planned_update(&step, "v").1, Severity::Breaking);
    Ok(())
}

#[tokio::test]
async fn dropping_a_constraint_that_does_not_exist_recreates_nothing() -> TestResult {
    let base = project().file("v.sql", "create view v as select id, a from t;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone().migration(
        "002_check",
        "alter table t drop constraint if exists a_positive; alter table t add constraint a_positive check (a > 0);",
    )).await?;
    step.assert_recreated(&[]);
    Ok(())
}

#[tokio::test]
async fn dropping_a_constraint_renamed_by_the_same_migration_stays_conservative() -> TestResult {
    // The view relies on the primary key (it selects `a` while grouping by id).
    let base = project().file("v.sql", "create view v as select id, a from t group by id;");
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone().migration(
        "002_pk",
        "alter table t rename constraint t_pkey to t_pk; alter table t drop constraint t_pk; alter table t add primary key (id);",
    )).await?;
    step.assert_recreated(&["view public.v"]);
    Ok(())
}

#[tokio::test]
async fn altering_a_managed_view_or_index_recreates_nothing() -> TestResult {
    let base = project()
        .file("v.sql", "create view v as select id, a from t;")
        .file("w.sql", "create view w as select id from v;")
        .file("idx.sql", "create index t_a_idx on t (a);");
    let evolution = Evolution::start(&base).await?;

    // Neither statement touches what the dependents are bound to.
    base.clone()
        .migration("002_alter", indoc! {"
            alter view v alter column a set default 0;
            alter index t_a_idx set (fillfactor = 70);
        "})
        .write_to(&evolution.env)?;
    let plan = plan_for(&evolution.env).await?;
    assert!(
        !plan.changes.iter().any(|change| matches!(change, ChangeOperation::UpdateObject { .. })),
        "{:?}", plan.changes,
    );
    Ok(())
}
