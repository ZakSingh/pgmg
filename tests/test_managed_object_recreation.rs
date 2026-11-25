//! What editing a managed object does to the objects around it.
//!
//! Two rules keep an edit from rippling further than it has to:
//!
//! - a function or procedure whose interface is unchanged is replaced in place
//!   (`CREATE OR REPLACE`), keeps its OID, and takes nothing with it except
//!   materialized views and indexes that store its results;
//! - a routine that only queries a relation from its string body is not bound
//!   to that relation, so it stays put when the relation is recreated.
//!
//! Every scenario goes through `common::evolution`, which checks which objects
//! kept their identity and that the result equals a fresh build of the files.

mod common;

use common::evolution::*;
use indoc::indoc;
use pgmg::analysis::Severity;
use pgmg::commands::ChangeOperation;

/// (in_place, severity) of the planned update of `name`, e.g. `core.audit_row`.
fn planned_update(step: &Step, name: &str) -> (bool, Severity) {
    step.plan.changes.iter().find_map(|change| match change {
        ChangeOperation::UpdateObject { object, in_place, severity, .. }
            if pgmg::sql::format_qualified_name(&object.qualified_name) == name => Some((*in_place, *severity)),
        _ => None,
    }).unwrap_or_else(|| panic!("no update of {name} in the plan"))
}

// ---------------------------------------------------------------------------
// Editing a function: replaced in place when its interface is unchanged
// ---------------------------------------------------------------------------

const AUDIT_V2: &str = indoc! {r#"
    create function core.audit_row()
      returns trigger
      language plpgsql
    as $$
    begin
      insert into audit_log (table_name, row_id) values ('v2:' || tg_table_name, new.id);
      return new;
    end;
    $$;

    create trigger audit_item
      after insert or update on item
      for each row execute function core.audit_row();
"#};

#[tokio::test]
async fn body_edit_replaces_the_function_in_place() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    // A privilege set outside the files only survives if nothing is dropped.
    env.execute_sql("revoke execute on function core.audit_row() from public").await?;
    let acl_before: String = env.query_scalar("select proacl::text from pg_proc where oid = 'core.audit_row'::regproc").await?;

    let step = evolution.advance(&base.clone().file("core/audit.sql", AUDIT_V2)).await?;

    // Nothing is dropped: not the function, not the trigger that calls it.
    step.assert_recreated(&[]);
    step.assert_left_in_place(&[obj::AUDIT_ROW_FN, obj::AUDIT_ITEM_TRG]);

    // It is still reported as the one update, now harmless to running clients.
    assert_eq!(step.result.objects_updated, vec!["core.audit_row"]);
    assert_eq!(planned_update(&step, "core.audit_row"), (true, Severity::Safe));
    assert_eq!(step.planned_updates()["function core.audit_row"], "DDL content has changed; replaced in place");
    assert_eq!(step.plan.severity, Severity::Safe);

    let acl_after: String = env.query_scalar("select proacl::text from pg_proc where oid = 'core.audit_row'::regproc").await?;
    assert_eq!(acl_after, acl_before, "privileges on the function were lost");

    // And the trigger runs the new body.
    env.execute_sql("update item set title = 'Wyrm' where id = 10").await?;
    assert_eq!(count(env, "select count(*) from audit_log where table_name = 'v2:item'").await?, 1);
    Ok(())
}

#[tokio::test]
async fn replaced_function_is_picked_up_by_sessions_that_already_ran_it() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    // Compiled and cached in this session before the edit.
    assert_eq!(count(env, "select api.listing_price(100)::bigint").await?, 2500);

    let step = evolution.advance(&base.clone().file("api/listing_price.sql", indoc! {r#"
        create function api.listing_price(p_listing_id bigint)
          returns integer
          language plpgsql stable
        as $$
        declare
          v_price integer;
        begin
          select l.price_cents * l.quantity into v_price from listing l where l.id = p_listing_id;
          return v_price;
        end;
        $$;
    "#})).await?;

    step.assert_recreated(&[]);
    assert_eq!(count(env, "select api.listing_price(100)::bigint").await?, 5000);
    Ok(())
}

#[tokio::test]
async fn in_place_edits_cover_language_attributes_defaults_and_procedures() -> TestResult {
    let base = marketplace()
        .file("api/discounted.sql", indoc! {r#"
            create function api.discounted(p_cents bigint, p_percent integer)
              returns bigint
              language sql immutable
            as $$ select p_cents - p_cents * p_percent / 100 $$;
        "#})
        .file("core/purge_audit.sql", indoc! {r#"
            create procedure core.purge_audit()
              language plpgsql
            as $$
            begin
              delete from audit_log;
            end;
            $$;
        "#});
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone()
        // Another language, another volatility, a new default, written with
        // OR REPLACE in the file: all things CREATE OR REPLACE accepts.
        .file("api/discounted.sql", indoc! {r#"
            create or replace function api.discounted(p_cents bigint, p_percent integer default 10)
              returns bigint
              language plpgsql stable
            as $$
            begin
              return p_cents - p_cents * p_percent / 100;
            end;
            $$;
        "#})
        .file("core/purge_audit.sql", indoc! {r#"
            create procedure core.purge_audit()
              language plpgsql
            as $$
            begin
              delete from audit_log where table_name <> 'keep';
            end;
            $$;
        "#}),
    ).await?;

    step.assert_recreated(&[]);
    step.assert_left_in_place(&["function api.discounted", "function core.purge_audit"]);
    assert_eq!(planned_update(&step, "api.discounted"), (true, Severity::Safe));
    assert_eq!(planned_update(&step, "core.purge_audit"), (true, Severity::Safe));
    assert_eq!(count(env, "select api.discounted(1000)").await?, 900);
    env.execute_sql("call core.purge_audit()").await?;
    Ok(())
}

/// A pure function with one of everything built on it: a view that calls it,
/// an index on it, and a materialized view storing what it returned.
fn with_price_bands(project: Project, band_sql: &str) -> Project {
    project
        .file("core/price_band.sql", band_sql)
        .file("core/banded_listing.sql", indoc! {r#"
            create view core.banded_listing as
            select l.id, core.price_band(l.price_cents) as band
            from listing l;
        "#})
        .file("core/listing_band_idx.sql", "create index listing_band_idx on listing (core.price_band(price_cents));")
        .file("core/band_counts.sql", indoc! {r#"
            create materialized view core.band_counts as
            select core.price_band(l.price_cents) as band, count(*) as listings
            from listing l
            group by 1;
        "#})
}

/// `signature` runs from the first parameter through the result type, e.g.
/// `p_cents integer) returns text`.
fn price_band_sql(signature: &str, threshold: i32) -> String {
    format!(
        "create function core.price_band({signature}\n  language sql immutable\nas $$ select case when $1 >= {threshold} then 'premium' else 'budget' end $$;\n"
    )
}

const BAND_DEPENDENTS: &[&str] = &[
    "view core.banded_listing", "index public.listing_band_idx", "materialized view core.band_counts",
];

#[tokio::test]
async fn in_place_edit_still_rebuilds_what_stores_the_functions_results() -> TestResult {
    let base = with_price_bands(marketplace(), &price_band_sql("p_cents integer) returns text", 2000));
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    env.execute_sql("refresh materialized view core.band_counts").await?;
    // Seeded prices are 2500 and 900: one premium, one budget.
    assert_eq!(count(env, "select listings from core.band_counts where band = 'premium'").await?, 1);

    // Lower the bar so both listings are premium.
    let next = with_price_bands(marketplace(), &price_band_sql("p_cents integer) returns text", 500));
    let step = evolution.advance(&next).await?;

    // The function and the plain view stay. The index and the materialized
    // view hold values the old body computed, so they are rebuilt.
    step.assert_recreated(&["index public.listing_band_idx", "materialized view core.band_counts"]);
    step.assert_left_in_place(&["function core.price_band", "view core.banded_listing"]);
    assert_eq!(planned_update(&step, "core.price_band").0, true);

    // Neither the index nor the materialized view serves stale bands.
    env.execute_sql("insert into listing (id, item_id, price_cents) values (102, 10, 700)").await?;
    env.execute_sql("set enable_seqscan = off").await?;
    assert_eq!(count(env, "select count(*) from listing where core.price_band(price_cents) = 'premium'").await?, 3);
    env.execute_sql("reset enable_seqscan").await?;
    env.execute_sql("refresh materialized view core.band_counts").await?;
    assert_eq!(count(env, "select listings from core.band_counts where band = 'premium'").await?, 3);
    assert_eq!(count(env, "select count(*) from core.banded_listing where band = 'premium'").await?, 3);
    Ok(())
}

#[tokio::test]
async fn interface_changes_still_drop_and_recreate_with_dependents() -> TestResult {
    // Each of these is something CREATE OR REPLACE refuses (or would turn into
    // a second overload), so the function and everything bound to it go
    // through DROP + CREATE as before.
    for (what, before, after) in [
        ("argument type", "p_cents integer) returns text", "p_cents bigint) returns text"),
        ("added argument", "p_cents integer) returns text", "p_cents integer, p_floor integer default 0) returns text"),
        ("return type", "p_cents integer) returns text", "p_cents integer) returns varchar"),
        ("renamed parameter", "p_cents integer) returns text", "p_amount integer) returns text"),
        ("removed default", "p_cents integer default 0) returns text", "p_cents integer) returns text"),
        ("out parameter", "p_cents integer) returns text", "p_cents integer, out o_band text)"),
    ] {
        let base = with_price_bands(marketplace(), &price_band_sql(before, 2000));
        let evolution = Evolution::start(&base).await?;
        evolution.env.execute_sql(SEED).await?;

        let next = with_price_bands(marketplace(), &price_band_sql(after, 2000));
        let step = evolution.advance(&next).await?;

        assert_eq!(planned_update(&step, "core.price_band").0, false, "{what}: planned in place");
        assert!(step.recreated.contains("function core.price_band"), "{what}: function kept its OID");
        for dependent in BAND_DEPENDENTS {
            assert!(step.recreated.contains(*dependent), "{what}: {dependent} was not recreated");
        }
    }
    Ok(())
}

#[tokio::test]
async fn function_bound_to_a_recreated_view_is_dropped_even_if_only_its_body_changed() -> TestResult {
    // api.list_live_listings returns the view's row type. With the view being
    // recreated in the same apply it cannot be replaced in place: the view
    // could not be dropped while the function exists.
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone()
        .file("core/live_listing.sql", &LIVE_LISTING_SQL.replace("where l.quantity > 0", "where l.quantity > 1"))
        .file("api/list_live_listings.sql", indoc! {r#"
            comment on function api.list_live_listings is 'Every listing a buyer can see.';

            create function api.list_live_listings()
              returns setof core.live_listing
              language plpgsql stable
            as $$
            begin
              return query select * from core.live_listing order by listing_id;
            end;
            $$;
        "#}),
    ).await?;

    step.assert_recreated(&[obj::LIVE_LISTING, obj::LIST_LIVE_LISTINGS]);
    assert_eq!(planned_update(&step, "api.list_live_listings").0, false);
    assert_eq!(count(env, "select count(*) from api.list_live_listings()").await?, 1);
    Ok(())
}

#[tokio::test]
async fn in_place_edit_alongside_a_migration() -> TestResult {
    // The function is replaced after the migration has run, with a body that
    // uses the column the migration added.
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone()
        .migration("003_listing_discount", indoc! {r#"
            alter table listing add column discount_cents integer not null default 0;
            update listing set discount_cents = price_cents / 10;
        "#})
        .file("api/listing_price.sql", indoc! {r#"
            create function api.listing_price(p_listing_id bigint)
              returns integer
              language plpgsql stable
            as $$
            declare
              v_price integer;
            begin
              select l.price_cents - l.discount_cents into v_price from listing l where l.id = p_listing_id;
              return v_price;
            end;
            $$;
        "#}),
    ).await?;

    step.assert_recreated(&[]);
    assert_eq!(planned_update(&step, "api.listing_price"), (true, Severity::Safe));
    assert_eq!(count(env, "select api.listing_price(100)::bigint").await?, 2250);
    Ok(())
}

#[tokio::test]
async fn edited_function_a_migration_has_to_drop_is_not_replaced_in_place() -> TestResult {
    // api.set_price takes `listing.price_cents%type`, so the column's type
    // change forces it out. Replacing it in place instead would resolve the
    // parameter to bigint and leave a second api.set_price beside the old one.
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone()
        .migration("003_price_bigint", "alter table listing alter column price_cents type bigint;")
        .file("api/set_price.sql", indoc! {r#"
            create function api.set_price(p_listing_id bigint, p_price listing.price_cents%type)
              returns void
              language plpgsql
            as $$
            begin
              update listing set price_cents = greatest(p_price, 0) where id = p_listing_id;
            end;
            $$;
        "#}),
    ).await?;

    assert_eq!(planned_update(&step, "api.set_price").0, false);
    assert!(step.recreated.contains(obj::SET_PRICE), "recreated: {:?}", step.recreated);
    assert_eq!(count(env, "select count(*) from pg_proc where proname = 'set_price'").await?, 1);
    let arguments: String = env.query_scalar("select pg_get_function_identity_arguments('api.set_price'::regproc)").await?;
    assert_eq!(arguments, "p_listing_id bigint, p_price bigint");
    Ok(())
}

#[tokio::test]
async fn edited_function_bound_to_a_type_a_migration_replaces_is_not_replaced_in_place() -> TestResult {
    let base = marketplace()
        .migration("003_grades", indoc! {r#"
            create type grade as enum ('poor', 'good', 'unclear');
            create table inspection (id bigint primary key, result grade);
        "#})
        .file("api/inspections_with.sql", indoc! {r#"
            create function api.inspections_with(p_result grade) returns bigint language sql stable
            as $$ select count(*) from inspection i where i.result = p_result $$;
        "#});
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone()
        .migration("004_grade_without_unclear", indoc! {r#"
            alter type grade rename to grade_old;
            create type grade as enum ('poor', 'good');
            alter table inspection alter column result type grade using result::text::grade;
            drop type grade_old;
        "#})
        .file("api/inspections_with.sql", indoc! {r#"
            create function api.inspections_with(p_result grade) returns bigint language sql stable
            as $$ select count(*) from inspection i where i.result is not distinct from p_result $$;
        "#}),
    ).await?;

    assert_eq!(planned_update(&step, "api.inspections_with").0, false);
    assert!(step.recreated.contains("function api.inspections_with"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Editing a view: routines that only query it stay in place
// ---------------------------------------------------------------------------

#[tokio::test]
async fn view_edit_leaves_functions_that_only_query_it() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    // Planned and cached in this session against the view about to be dropped.
    assert_eq!(count(env, "select api.count_live_listings()").await?, 2);

    let step = evolution.advance(&base.clone().file(
        "core/live_listing.sql",
        &LIVE_LISTING_SQL.replace("where l.quantity > 0", "where l.quantity > 1"),
    )).await?;

    // The view, and the function whose result type is the view's row type.
    // api.count_live_listings only selects from it.
    step.assert_recreated(&[obj::LIVE_LISTING, obj::LIST_LIVE_LISTINGS]);
    step.assert_left_in_place(&[obj::COUNT_LIVE]);
    assert_eq!(step.planned_updates()["function api.list_live_listings"], "Dependency requires recreation");

    // The same session sees the new view through the old function.
    assert_eq!(count(env, "select api.count_live_listings()").await?, 1);
    Ok(())
}

#[tokio::test]
async fn view_edit_rebuilds_what_stores_a_late_bound_functions_results() -> TestResult {
    // core.live_total never mentions the view, but its contents come from it
    // by way of the function, which stays in place.
    let base = marketplace().file("core/live_total.sql", indoc! {r#"
        create materialized view core.live_total as
        select api.count_live_listings() as listings;
    "#});
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    env.execute_sql("refresh materialized view core.live_total").await?;
    assert_eq!(count(env, "select listings from core.live_total").await?, 2);

    let step = evolution.advance(&base.clone().file(
        "core/live_listing.sql",
        &LIVE_LISTING_SQL.replace("where l.quantity > 0", "where l.quantity > 1"),
    )).await?;

    step.assert_recreated(&[obj::LIVE_LISTING, obj::LIST_LIVE_LISTINGS, "materialized view core.live_total"]);
    step.assert_left_in_place(&[obj::COUNT_LIVE]);
    assert_eq!(count(env, "select listings from core.live_total").await?, 1);
    Ok(())
}

#[tokio::test]
async fn view_edit_and_functions_that_use_its_row_type_in_their_body() -> TestResult {
    // Four ways a body can use the view's row type. All four must keep working
    // in a session that compiled them before the view was dropped and
    // recreated with a different set of columns.
    let base = marketplace()
        // %ROWTYPE: PL/pgSQL looks the type up again by name when it has gone.
        .file("api/first_by_rowtype.sql", indoc! {r#"
            create function api.first_by_rowtype() returns bigint language plpgsql stable as $$
            declare r core.live_listing%rowtype;
            begin
              select * into r from core.live_listing order by listing_id limit 1;
              return r.listing_id;
            end; $$;
        "#})
        // A variable declared with the type, and casts to it: tracked as type
        // dependencies, which bind.
        .file("api/first_by_variable.sql", indoc! {r#"
            create function api.first_by_variable() returns bigint language plpgsql stable as $$
            declare r core.live_listing;
            begin
              select * into r from core.live_listing order by listing_id limit 1;
              return r.listing_id;
            end; $$;
        "#})
        .file("api/first_by_cast.sql", indoc! {r#"
            create function api.first_by_cast() returns bigint language plpgsql stable as $$
            declare v bigint;
            begin
              select (l::core.live_listing).listing_id into v from core.live_listing l order by listing_id limit 1;
              return v;
            end; $$;
        "#})
        .file("api/first_by_sql_cast.sql", indoc! {r#"
            create function api.first_by_sql_cast() returns bigint language sql stable as $$
              select (l::core.live_listing).listing_id from core.live_listing l order by listing_id limit 1
            $$;
        "#});
    let functions = ["first_by_rowtype", "first_by_variable", "first_by_cast", "first_by_sql_cast"];
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    for function in functions {
        // Twice, so plans are cached as well as the function compiled.
        assert_eq!(count(env, &format!("select api.{function}()")).await?, 100);
        assert_eq!(count(env, &format!("select api.{function}()")).await?, 100);
    }

    let step = evolution.advance(&base.clone().file(
        "core/live_listing.sql",
        &LIVE_LISTING_SQL
            .replace("l.quantity\nfrom", "l.quantity,\n  l.updated_at\nfrom")
            .replace("where l.quantity > 0", "where l.quantity < 2"),
    )).await?;

    step.assert_left_in_place(&["function api.first_by_rowtype", obj::COUNT_LIVE]);
    for bound in ["first_by_variable", "first_by_cast", "first_by_sql_cast"] {
        assert!(step.recreated.contains(&format!("function api.{bound}")), "recreated: {:?}", step.recreated);
    }
    for function in functions {
        assert_eq!(count(env, &format!("select api.{function}()")).await?, 101, "api.{function}");
    }
    Ok(())
}

#[tokio::test]
async fn a_run_of_edits_stays_equal_to_a_fresh_build_at_every_step() -> TestResult {
    let v1 = with_price_bands(marketplace(), &price_band_sql("p_cents integer) returns text", 2000));
    let evolution = Evolution::start(&v1).await?;
    evolution.env.execute_sql(SEED).await?;

    // Body edit to a trigger function.
    let v2 = v1.clone().file("core/audit.sql", AUDIT_V2);
    evolution.advance(&v2).await?.assert_recreated(&[]);

    // Body edit to a function with stored results downstream.
    let v3 = with_price_bands(v2.clone(), &price_band_sql("p_cents integer) returns text", 500));
    evolution.advance(&v3).await?
        .assert_recreated(&["index public.listing_band_idx", "materialized view core.band_counts"]);

    // View edit.
    let v4 = v3.clone().file("core/live_listing.sql", &LIVE_LISTING_SQL.replace("where l.quantity > 0", "where l.quantity > 1"));
    evolution.advance(&v4).await?.assert_recreated(&[obj::LIVE_LISTING, obj::LIST_LIVE_LISTINGS]);

    // Signature change: back to DROP + CREATE with everything bound to it.
    let v5 = with_price_bands(v4.clone(), &price_band_sql("p_cents bigint) returns text", 500));
    let step = evolution.advance(&v5).await?;
    assert!(step.recreated.contains("function core.price_band") && step.recreated.contains("view core.banded_listing"));

    // A migration and a body edit together.
    let v6 = v5.clone()
        .migration("003_item_condition", "alter table item add column condition text;")
        .file("core/audit.sql", &AUDIT_V2.replace("v2:", "v3:"));
    evolution.advance(&v6).await?.assert_recreated(&[obj::ITEM_DETAIL, obj::ITEM_CARD]);

    // Nothing left over.
    evolution.advance(&v6).await?.assert_recreated(&[]);
    Ok(())
}
