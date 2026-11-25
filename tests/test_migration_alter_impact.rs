//! What a migration's ALTER TABLE does to the managed objects that depend on
//! the altered table.
//!
//! pgmg drops dependents before a migration and recreates them afterwards only
//! when the ALTER calls for it (see `pgmg::sql::AlterImpact`). Every scenario
//! here goes through `common::evolution`, which checks which objects kept
//! their identity and that the result equals a fresh build of the same files.

mod common;

use std::collections::BTreeSet;

use common::evolution::*;
use indoc::indoc;

#[tokio::test]
async fn fixture_builds_and_names_every_object() -> TestResult {
    let evolution = Evolution::start(&marketplace()).await?;
    let names: BTreeSet<String> = object_identities(&evolution.env).await?.into_keys().collect();
    let expected: BTreeSet<String> = obj::ALL.iter().map(|s| s.to_string()).collect();
    assert_eq!(names, expected);
    Ok(())
}

// ---------------------------------------------------------------------------
// ADD COLUMN: dependents are left in place unless the column changes them
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_column_leaves_every_dependent_in_place() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;

    // State that only survives if nothing is dropped: the materialized view's
    // contents (stale on purpose) and a privilege granted outside the files.
    env.execute_sql(SEED).await?;
    env.execute_sql("refresh materialized view core.seller_stats").await?;
    env.execute_sql("insert into item (id, seller_id, title) values (12, 1, 'Late arrival')").await?;
    env.execute_sql("grant select on core.live_listing to public").await?;
    let acl_before: String = env.query_scalar("select relacl::text from pg_class where oid = 'core.live_listing'::regclass").await?;

    let step = evolution.advance(&base.clone().migration("003_listing_currency", indoc! {r#"
        alter table listing add column currency text not null default 'USD';
    "#})).await?;

    assert_eq!(step.result.migrations_applied, vec!["003_listing_currency"]);
    assert!(step.planned_updates().is_empty(), "planned: {:?}", step.planned_updates());
    assert!(step.result.objects_updated.is_empty(), "updated: {:?}", step.result.objects_updated);
    step.assert_recreated(&[]);

    // The plpgsql functions reading the table are flagged for a re-check instead.
    assert_eq!(
        step.recheck_names(),
        BTreeSet::from(["api.listing_price".to_string(), "api.set_price".to_string()]),
    );

    // Still stale: two sellers' rows from before the late item, not rebuilt.
    assert_eq!(count(env, "select sum(item_count)::bigint from core.seller_stats").await?, 2);
    let acl_after: String = env.query_scalar("select relacl::text from pg_class where oid = 'core.live_listing'::regclass").await?;
    assert_eq!(acl_after, acl_before, "grant on the view was lost");

    // Everything still works against the widened table.
    assert_eq!(count(env, "select count(*) from api.list_live_listings()").await?, 2);
    assert_eq!(count(env, "select api.listing_price(100)::bigint").await?, 2500);
    env.execute_sql("update listing set quantity = 3 where id = 100").await?;
    Ok(())
}

#[tokio::test]
async fn add_column_recreates_views_that_select_star() -> TestResult {
    // Spelled both ways: the managed files say `item`, never `public.item`.
    for table in ["item", "public.item"] {
        let base = marketplace();
        let evolution = Evolution::start(&base).await?;
        evolution.env.execute_sql(SEED).await?;

        let step = evolution.advance(&base.clone().migration(
            "003_item_condition",
            &format!("alter table {table} add column condition text default 'new';"),
        )).await?;

        // `select i.*` must pick the column up, and what is built on that view
        // is recreated with it. Nothing else that reads `item` is touched.
        step.assert_recreated(&[obj::ITEM_DETAIL, obj::ITEM_CARD]);
        let planned = step.planned_updates();
        assert_eq!(planned["view core.item_detail"], "Migration adds columns its definition picks up");
        assert_eq!(planned["view core.item_card"], "Dependency requires recreation");
        assert_eq!(planned.len(), 2, "planned: {planned:?}");

        let env = &evolution.env;
        assert_eq!(count(env, "select count(*) from core.item_detail where condition = 'new'").await?, 2);
        // The string-bodied `select *` function sees the column without help.
        assert_eq!(count(env, "select count(*) from api.get_item(10) g where g.condition = 'new'").await?, 1);
        assert_eq!(step.recheck_names(), BTreeSet::from(["api.get_item".to_string()]));
    }
    Ok(())
}

#[tokio::test]
async fn add_column_with_edited_files_updates_only_what_changed() -> TestResult {
    // The everyday workflow: a migration adds a column, one view is edited to
    // expose it, and a new function and index use it straight away.
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    evolution.env.execute_sql(SEED).await?;

    let next = base.clone()
        .migration("003_listing_currency", "alter table listing add column currency text not null default 'USD';")
        .file("core/live_listing.sql", &LIVE_LISTING_SQL.replace("l.quantity\n", "l.quantity,\n  l.currency\n"))
        .file("api/listing_currency.sql", indoc! {r#"
            create function api.listing_currency(p_listing_id bigint)
              returns text
              language sql stable
            as $$ select l.currency from listing l where l.id = p_listing_id $$;
        "#})
        .file("core/listing_currency_idx.sql", "create index listing_currency_idx on listing (currency);");
    assert_ne!(next.files["core/live_listing.sql"], LIVE_LISTING_SQL);

    let step = evolution.advance(&next).await?;

    // The edited view, and the function whose row type is that view.
    step.assert_recreated(&[obj::LIVE_LISTING, obj::LIST_LIVE_LISTINGS]);
    step.assert_left_in_place(&[obj::SELLER_STATS, obj::LISTING_PRICE, obj::TOUCH_LISTING_TRG]);
    assert_eq!(step.result.objects_created.len(), 2, "created: {:?}", step.result.objects_created);

    let env = &evolution.env;
    assert_eq!(count(env, "select count(*) from api.list_live_listings() where currency = 'USD'").await?, 2);
    let currency: String = env.query_scalar("select api.listing_currency(100)").await?;
    assert_eq!(currency, "USD");
    Ok(())
}

#[tokio::test]
async fn add_column_recreates_a_view_whose_reference_it_captures() -> TestResult {
    // `title` inside the subquery is item's column, reached from the outer
    // query. Once listing has a `title` too, the same text means listing's.
    let base = marketplace().file("core/titled_listing_count.sql", indoc! {r#"
        create view core.titled_listing_count as
        select
          i.id,
          (select count(*) from listing l where l.item_id = i.id and length(title) > 0) as listings
        from item i;
    "#});
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    assert_eq!(count(env, "select listings from core.titled_listing_count where id = 10").await?, 1);

    // `advance` compares against a fresh build, whose view reads l.title.
    let step = evolution.advance(&base.clone().migration(
        "003_listing_title",
        "alter table listing add column title text;",
    )).await?;

    step.assert_recreated(&["view core.titled_listing_count"]);
    assert_eq!(count(env, "select listings from core.titled_listing_count where id = 10").await?, 0);
    Ok(())
}

#[tokio::test]
async fn add_column_that_makes_a_view_ambiguous_fails_the_apply() -> TestResult {
    // A fresh build of these files cannot succeed once both tables have a
    // `title`, so the incremental apply must not quietly succeed either.
    let base = marketplace().file("core/listing_summary.sql", indoc! {r#"
        create view core.listing_summary as
        select l.id, title, l.price_cents
        from listing l
          join item i on i.id = l.item_id;
    "#});
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;

    base.clone()
        .migration("003_listing_title", "alter table listing add column title text;")
        .write_to(env)?;
    let error = match apply(env).await {
        Ok(result) => {
            assert!(!result.errors.is_empty(), "apply succeeded but a fresh build would fail");
            result.errors.join("\n")
        }
        Err(e) => e.to_string(),
    };
    assert!(!error.is_empty());

    // Rolled back whole: no column, no migration record, view intact.
    assert_eq!(count(env, "select count(*) from information_schema.columns where table_name = 'listing' and column_name = 'title'").await?, 0);
    assert_eq!(count(env, "select count(*) from pgmg.pgmg_migrations where name = '003_listing_title'").await?, 0);
    assert_eq!(count(env, "select count(*) from core.listing_summary").await?, 0);
    Ok(())
}

#[tokio::test]
async fn add_column_recreates_natural_joins() -> TestResult {
    // NATURAL JOIN fixes its join columns when the view is created. A new
    // common column changes what a fresh build joins on.
    let base = marketplace()
        .migration("003_shipping", indoc! {r#"
            create table shipment (listing_id bigint primary key, carrier text not null);
            create table shipment_event (listing_id bigint not null, tracking text not null);
        "#})
        .file("core/shipment_tracking.sql", indoc! {r#"
            create view core.shipment_tracking as
            select listing_id, tracking
            from shipment natural join shipment_event;
        "#});
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql("insert into shipment values (100, 'ups'); insert into shipment_event values (100, '1Z999');").await?;
    assert_eq!(count(env, "select count(*) from core.shipment_tracking").await?, 1);

    let step = evolution.advance(&base.clone().migration(
        "004_event_carrier",
        "alter table shipment_event add column carrier text;",
    )).await?;

    step.assert_recreated(&["view core.shipment_tracking"]);
    // Now joined on (listing_id, carrier); the event's carrier is null.
    assert_eq!(count(env, "select count(*) from core.shipment_tracking").await?, 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// Subcommands that never touch dependents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn constraint_default_and_storage_changes_leave_dependents_in_place() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    evolution.env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone().migration("003_tighten", indoc! {r#"
        alter table listing add constraint listing_price_positive check (price_cents > 0);
        alter table listing add constraint listing_item_fk2 foreign key (item_id) references item (id) not valid;
        alter table listing validate constraint listing_item_fk2;
        alter table listing alter column quantity set default 0;
        alter table listing alter column price_cents set statistics 200;
        alter table listing set (fillfactor = 90);
        alter table listing owner to current_user;

        alter table item alter column description set not null;
        alter table item alter column description drop not null;
        alter table item add constraint item_title_not_blank check (title <> '');
        alter table item enable row level security;
        alter table item force row level security;
        alter table item no force row level security;
        alter table item disable row level security;
        alter table item replica identity full;

        alter table account alter column created_at drop default;
        alter table account add constraint account_display_name_key unique (display_name);
    "#})).await?;

    assert!(step.planned_updates().is_empty(), "planned: {:?}", step.planned_updates());
    step.assert_recreated(&[]);
    assert!(step.recheck_names().is_empty());
    Ok(())
}

#[tokio::test]
async fn migration_can_toggle_a_managed_trigger_by_name() -> TestResult {
    // A migration that names a managed trigger has to guard on its existence,
    // because on a fresh build migrations run before any managed object is
    // created. On an incremental apply the trigger is still on the table (an
    // ADD COLUMN does not drop it), so the guard finds it and the backfill
    // runs with it disabled.
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    let audited_before = count(env, "select count(*) from audit_log").await?;

    let step = evolution.advance(&base.clone().migration("003_item_slug", indoc! {r#"
        alter table item add column slug text;

        do $$
        begin
          if exists (select 1 from pg_trigger where tgname = 'audit_item') then
            alter table item disable trigger audit_item;
          end if;
        end
        $$;

        update item set slug = lower(title);

        do $$
        begin
          if exists (select 1 from pg_trigger where tgname = 'audit_item') then
            alter table item enable trigger audit_item;
          end if;
        end
        $$;
    "#})).await?;

    step.assert_left_in_place(&[obj::AUDIT_ITEM_TRG]);
    assert_eq!(count(env, "select count(*) from item where slug is not null").await?, 2);
    assert_eq!(count(env, "select count(*) from audit_log").await?, audited_before, "disabled trigger fired");

    env.execute_sql("update item set title = 'Wyrm' where id = 10").await?;
    assert_eq!(count(env, "select count(*) from audit_log").await?, audited_before + 1, "trigger was left disabled");
    Ok(())
}

#[tokio::test]
async fn disable_trigger_user_suppresses_managed_triggers_during_a_backfill() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    let audited_before = count(env, "select count(*) from audit_log").await?;

    let step = evolution.advance(&base.clone().migration("003_item_slug", indoc! {r#"
        alter table item add column slug text;
        alter table item disable trigger user;
        update item set slug = lower(title);
        alter table item enable trigger user;
    "#})).await?;

    step.assert_left_in_place(&[obj::AUDIT_ITEM_TRG]);
    assert_eq!(count(env, "select count(*) from audit_log").await?, audited_before);
    env.execute_sql("update item set title = 'Wyrm' where id = 10").await?;
    assert_eq!(count(env, "select count(*) from audit_log").await?, audited_before + 1);
    Ok(())
}

#[tokio::test]
async fn triggers_fire_during_an_add_column_backfill() -> TestResult {
    // Managed triggers stay on the table through an ADD COLUMN migration, so a
    // backfill in that migration fires them, exactly as it does in a migration
    // with no ALTER TABLE at all. A migration that must not fire them says so
    // with DISABLE TRIGGER (see the tests above).
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    let audited_before = count(env, "select count(*) from audit_log").await?;

    evolution.advance(&base.clone().migration("003_item_slug", indoc! {r#"
        alter table item add column slug text;
        update item set slug = lower(title);
    "#})).await?;

    assert_eq!(count(env, "select count(*) from audit_log").await?, audited_before + 2);
    Ok(())
}

// ---------------------------------------------------------------------------
// Subcommands that still rebuild what is bound to the table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn alter_column_type_rebuilds_what_is_bound_to_the_table() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    // Compile api.listing_price in this session, so its cached plan and its
    // integer variable are exercised across the change.
    assert_eq!(count(env, "select api.listing_price(100)::bigint").await?, 2500);

    // PostgreSQL refuses this while the view and materialized view exist.
    let step = evolution.advance(&base.clone().migration(
        "003_price_bigint",
        "alter table listing alter column price_cents type bigint;",
    )).await?;

    step.assert_recreated(BOUND_TO_LISTING);
    let planned = step.planned_updates();
    assert_eq!(planned["view core.live_listing"], "Migration alters dependent table");
    // `p_price listing.price_cents%type` was resolved to integer when the
    // function was created; only recreating it makes the parameter bigint.
    assert_eq!(planned["function api.set_price"], "Migration alters dependent table");
    let parameter_type: String = env.query_scalar(
        "select pg_get_function_identity_arguments('api.set_price'::regproc)",
    ).await?;
    assert_eq!(parameter_type, "p_listing_id bigint, p_price bigint");

    // The functions that only query the table from their bodies stay, are
    // flagged for a re-check, and keep working in a session that had already
    // run them.
    step.assert_left_in_place(&[obj::LISTING_PRICE, obj::COUNT_LIVE]);
    assert_eq!(step.recheck_names(), BTreeSet::from(["api.listing_price".to_string()]));
    assert_eq!(count(env, "select api.listing_price(100)::bigint").await?, 2500);

    let column_type: String = env.query_scalar(
        "select format_type(atttypid, atttypmod) from pg_attribute where attrelid = 'core.live_listing'::regclass and attname = 'price_cents'",
    ).await?;
    assert_eq!(column_type, "bigint");
    assert_eq!(count(env, "select count(*) from api.list_live_listings()").await?, 2);
    assert_eq!(count(env, "select api.count_live_listings()").await?, 2);
    Ok(())
}

#[tokio::test]
async fn rebuild_refreshes_what_stores_a_left_in_place_functions_results() -> TestResult {
    // api.item_price reads `listing` only from its body, so a type change
    // leaves it in place. The materialized view and the index below hold
    // values it computed, though, and neither reads `listing` itself. They
    // are rebuilt, as they always were.
    let base = marketplace()
        .file("api/item_price.sql", indoc! {r#"
            create function api.item_price(p_item_id bigint)
              returns bigint
              language sql immutable
            as $$ select min(l.price_cents)::bigint from listing l where l.item_id = p_item_id $$;
        "#})
        .file("core/price_book.sql", indoc! {r#"
            create materialized view core.price_book as
            select i.id as item_id, api.item_price(i.id) as price
            from item i;
        "#})
        .file("core/item_price_idx.sql", "create index item_price_idx on item (api.item_price(id));")
        .file("core/priced_item.sql", indoc! {r#"
            create view core.priced_item as
            select i.id, api.item_price(i.id) as price from item i;
        "#});
    let evolution = Evolution::start(&base).await?;
    evolution.env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone().migration(
        "003_price_bigint",
        "alter table listing alter column price_cents type bigint;",
    )).await?;

    step.assert_left_in_place(&["function api.item_price"]);
    assert!(step.recreated.contains("materialized view core.price_book"), "recreated: {:?}", step.recreated);
    assert!(step.recreated.contains("index public.item_price_idx"), "recreated: {:?}", step.recreated);
    assert_eq!(count(&evolution.env, "select count(*) from core.price_book where price is not null").await?, 2);
    Ok(())
}

#[tokio::test]
async fn drop_column_rebuilds_what_is_bound_to_the_table() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;

    // core.item_detail exposes `description` through `i.*`.
    let step = evolution.advance(&base.clone().migration(
        "003_drop_description",
        "alter table item drop column description;",
    )).await?;

    step.assert_recreated(BOUND_TO_ITEM);
    assert_eq!(count(env, "select count(*) from information_schema.columns where table_schema = 'core' and table_name = 'item_detail' and column_name = 'description'").await?, 0);
    assert_eq!(count(env, "select count(*) from core.item_card").await?, 2);
    Ok(())
}

// ---------------------------------------------------------------------------
// DROP CONSTRAINT: a rebuild only if something depends on the constraint
// ---------------------------------------------------------------------------

/// A table whose primary key a view relies on: grouping by it lets the view
/// select the other columns, which makes the view depend on the constraint.
fn with_tags(project: Project) -> Project {
    project
        .migration("003_tags", indoc! {r#"
            create table tag (
              id    bigint constraint tag_pkey primary key,
              label text not null constraint tag_label_not_blank check (label <> ''),
              item_id bigint constraint tag_item_fk references item (id)
            );
        "#})
        .file("core/tag_usage.sql", indoc! {r#"
            create view core.tag_usage as
            select t.id, t.label, count(*) as uses
            from tag t
            group by t.id;
        "#})
        .file("core/tag_label_idx.sql", "create index tag_label_idx on tag (lower(label));")
        .file("api/tag_count.sql", indoc! {r#"
            create function api.tag_count() returns bigint language sql stable
            as $$ select count(*) from tag $$;
        "#})
}

const TAG_OBJECTS: &[&str] = &["view core.tag_usage", "index public.tag_label_idx", "function api.tag_count"];

#[tokio::test]
async fn dropping_a_constraint_nothing_depends_on_leaves_dependents_in_place() -> TestResult {
    // Spelled both ways, and with the usual drop-and-replace of a check.
    for migration in [
        "alter table tag drop constraint tag_label_not_blank;",
        "alter table public.tag drop constraint tag_label_not_blank;",
        "alter table tag drop constraint if exists tag_label_not_blank cascade;",
        "alter table tag drop constraint tag_label_not_blank, add constraint tag_label_short check (length(label) < 50);",
        "alter table tag drop constraint tag_item_fk;",
        "alter table tag drop constraint tag_label_not_blank;\nalter table tag drop constraint tag_item_fk;",
    ] {
        let base = with_tags(marketplace());
        let evolution = Evolution::start(&base).await?;

        let step = evolution.advance(&base.clone().migration("004_loosen_tags", migration)).await?;

        assert!(step.planned_updates().is_empty(), "{migration}: planned {:?}", step.planned_updates());
        step.assert_recreated(&[]);
        step.assert_left_in_place(TAG_OBJECTS);
    }
    Ok(())
}

#[tokio::test]
async fn dropping_a_constraint_a_view_depends_on_rebuilds_dependents() -> TestResult {
    let base = with_tags(marketplace());
    let evolution = Evolution::start(&base).await?;

    // PostgreSQL refuses to drop the key while core.tag_usage groups by it.
    let step = evolution.advance(&base.clone().migration("004_rekey_tags", indoc! {r#"
        alter table tag drop constraint tag_pkey;
        alter table tag add constraint tag_pkey primary key (id);
    "#})).await?;

    // Everything bound to the table goes; the body-only function stays.
    step.assert_recreated(&["view core.tag_usage", "index public.tag_label_idx"]);
    step.assert_left_in_place(&["function api.tag_count"]);
    step.assert_left_in_place(obj::ALL);
    Ok(())
}

#[tokio::test]
async fn one_depended_on_constraint_among_several_is_enough() -> TestResult {
    let base = with_tags(marketplace());
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone().migration("004_rework_tags", indoc! {r#"
        alter table tag drop constraint tag_label_not_blank;
        alter table tag drop constraint tag_item_fk, drop constraint tag_pkey;
        alter table tag add constraint tag_pkey primary key (id);
    "#})).await?;

    step.assert_recreated(&["view core.tag_usage", "index public.tag_label_idx"]);
    Ok(())
}

#[tokio::test]
async fn dropping_a_constraint_the_catalog_does_not_know_rebuilds_dependents() -> TestResult {
    // By the time the DROP runs the key has a new name, which the catalog at
    // plan time has never heard of. Unknown means rebuild.
    let base = with_tags(marketplace());
    let evolution = Evolution::start(&base).await?;

    let step = evolution.advance(&base.clone().migration("004_rekey_tags", indoc! {r#"
        alter table tag rename constraint tag_pkey to tag_primary;
        alter table tag drop constraint tag_primary;
        alter table tag add constraint tag_pkey primary key (id);
    "#})).await?;

    step.assert_recreated(&["view core.tag_usage", "index public.tag_label_idx"]);
    Ok(())
}

#[tokio::test]
async fn dropping_a_constraint_alongside_other_changes_keeps_their_impact() -> TestResult {
    let base = with_tags(marketplace())
        .file("core/tag_row.sql", "create view core.tag_row as select t.* from tag t;");
    let evolution = Evolution::start(&base).await?;

    // The added column is what recreates the `select *` view; the constraint
    // drop adds nothing to that.
    let step = evolution.advance(&base.clone().migration(
        "004_tag_colour",
        "alter table tag drop constraint tag_label_not_blank, add column colour text;",
    )).await?;

    step.assert_recreated(&["view core.tag_row"]);
    step.assert_left_in_place(TAG_OBJECTS);
    Ok(())
}

// ---------------------------------------------------------------------------
// Types changed by a migration
// ---------------------------------------------------------------------------

/// An enum created by a migration, used by a table column, by the signature of
/// a function that reads the table only from its body, and by a managed
/// composite type.
fn with_grades(project: Project) -> Project {
    project
        .migration("003_grades", indoc! {r#"
            create type grade as enum ('poor', 'fair', 'good', 'unclear');
            create table inspection (
              id      bigint primary key,
              item_id bigint not null references item (id),
              result  grade
            );
        "#})
        .file("api/inspections_with.sql", indoc! {r#"
            create function api.inspections_with(p_result grade)
              returns bigint
              language sql stable
            as $$ select count(*) from inspection i where i.result = p_result $$;
        "#})
        .file("api/inspection_summary.sql", indoc! {r#"
            create type api.inspection_summary as (item_id bigint, worst grade);
        "#})
        .file("api/inspection_count.sql", indoc! {r#"
            create function api.inspection_count() returns bigint language sql stable
            as $$ select count(*) from inspection $$;
        "#})
}

#[tokio::test]
async fn replacing_a_type_rebuilds_what_is_bound_to_it() -> TestResult {
    // The usual way to remove an enum label: rename the type aside, create its
    // replacement, move the column over, drop the old one. Everything bound to
    // the old type has to be recreated against the new one, or the DROP TYPE
    // is refused. api.inspections_with reads the table only from its body, so
    // nothing about the table forces it out; its parameter type does.
    let base = with_grades(marketplace());
    let evolution = Evolution::start(&base).await?;
    let env = &evolution.env;
    env.execute_sql(SEED).await?;
    env.execute_sql("insert into inspection values (1, 10, 'good'), (2, 11, 'unclear')").await?;

    let step = evolution.advance(&base.clone()
        .migration("004_grade_without_unclear", indoc! {r#"
            update inspection set result = null where result = 'unclear';
            alter type grade rename to grade_old;
            create type grade as enum ('poor', 'fair', 'good');
            alter table inspection alter column result type grade using result::text::grade;
        "#})
        .migration("005_drop_old_grade", "drop type grade_old;"),
    ).await?;

    step.assert_recreated(&["function api.inspections_with"]);
    assert_eq!(step.planned_updates()["function api.inspections_with"], "Migration alters dependent type");
    assert!(step.planned_updates().contains_key("type api.inspection_summary"), "planned: {:?}", step.planned_updates());
    // Reads the table from its body and has nothing to do with the type.
    step.assert_left_in_place(&["function api.inspection_count"]);

    assert_eq!(count(env, "select api.inspections_with('good')").await?, 1);
    assert_eq!(count(env, "select count(*) from pg_type where typname = 'grade_old'").await?, 0);
    Ok(())
}

#[tokio::test]
async fn dropping_or_moving_a_type_rebuilds_what_is_bound_to_it() -> TestResult {
    for (what, migration) in [
        // Recreated under the same name: dependents must bind to the new one.
        ("drop and recreate", indoc! {r#"
            alter table inspection alter column result type text;
            drop type grade;
            create type grade as enum ('poor', 'fair', 'good', 'unclear');
            alter table inspection alter column result type grade using result::grade;
        "#}),
        // Spelled with its schema, where the managed files spell it bare.
        ("schema-qualified rename", indoc! {r#"
            alter type public.grade rename to grade_old;
            create type grade as enum ('poor', 'fair', 'good', 'unclear');
            alter table inspection alter column result type grade using result::text::grade;
            drop type grade_old;
        "#}),
    ] {
        let base = with_grades(marketplace());
        let evolution = Evolution::start(&base).await?;

        let step = evolution.advance(&base.clone().migration("004_rework_grade", migration)).await?;

        assert!(step.recreated.contains("function api.inspections_with"), "{what}: recreated {:?}", step.recreated);
        step.assert_left_in_place(&["function api.inspection_count"]);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Several alterations in one apply
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_table_is_handled_by_its_own_worst_alteration() -> TestResult {
    let base = marketplace();
    let evolution = Evolution::start(&base).await?;
    evolution.env.execute_sql(SEED).await?;

    let step = evolution.advance(&base.clone()
        .migration("003_listing_currency", "alter table listing add column currency text;")
        .migration("004_account_locale", "alter table account alter column display_name set not null;")
        .migration("005_item_title", "alter table item alter column title type varchar(200);"),
    ).await?;

    // item is rebuilt; listing only gained a column and account a constraint,
    // so what reads just those is untouched.
    step.assert_recreated(BOUND_TO_ITEM);
    step.assert_left_in_place(&[obj::LISTING_PRICE, obj::TOUCH_LISTING_TRG, obj::TOUCH_LISTING_FN, obj::AUDIT_ROW_FN]);
    Ok(())
}

#[tokio::test]
async fn a_rebuild_anywhere_in_the_pending_migrations_wins_for_that_table() -> TestResult {
    // In either order, and whichever way each migration spells the table.
    for (first, second) in [
        ("alter table listing add column currency text;", "alter table public.listing alter column price_cents type bigint;"),
        ("alter table public.listing alter column price_cents type bigint;", "alter table listing add column currency text;"),
    ] {
        let base = marketplace();
        let evolution = Evolution::start(&base).await?;
        evolution.env.execute_sql(SEED).await?;

        let step = evolution.advance(&base.clone()
            .migration("003_first", first)
            .migration("004_second", second),
        ).await?;

        step.assert_recreated(BOUND_TO_LISTING);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// A longer history
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_project_history_stays_equal_to_a_fresh_build_at_every_step() -> TestResult {
    // One database carried through a run of ordinary changes. `advance` checks
    // it against a from-scratch build after each one, so drift introduced by
    // any step (and carried into later ones) is caught where it starts.
    let v1 = marketplace();
    let evolution = Evolution::start(&v1).await?;
    evolution.env.execute_sql(SEED).await?;

    // Add a column nobody reads yet.
    let v2 = v1.clone().migration("003_listing_currency", "alter table listing add column currency text not null default 'USD';");
    evolution.advance(&v2).await?.assert_recreated(&[]);

    // Constrain it, and add a function that uses it.
    let v3 = v2.clone()
        .migration("004_currency_check", "alter table listing add constraint listing_currency_len check (length(currency) = 3);")
        .file("api/listing_currency.sql", indoc! {r#"
            create function api.listing_currency(p_listing_id bigint)
              returns text
              language sql stable
            as $$ select l.currency from listing l where l.id = p_listing_id $$;
        "#});
    let step = evolution.advance(&v3).await?;
    step.assert_recreated(&[]);
    assert_eq!(step.result.objects_created, vec!["api.listing_currency"]);

    // Add a column a `select *` view picks up.
    let v4 = v3.clone().migration("005_item_condition", "alter table item add column condition text;");
    evolution.advance(&v4).await?.assert_recreated(&[obj::ITEM_DETAIL, obj::ITEM_CARD]);

    // Widen a column: everything reading the table is rebuilt.
    let v5 = v4.clone().migration("006_price_bigint", "alter table listing alter column price_cents type bigint;");
    let step = evolution.advance(&v5).await?;
    step.assert_left_in_place(&[obj::ITEM_DETAIL, obj::ITEM_CARD, obj::GET_ITEM, obj::AUDIT_ITEM_TRG, obj::ITEM_TITLE_IDX]);
    assert!(step.recreated.contains(obj::LIVE_LISTING) && step.recreated.contains(obj::SET_PRICE));
    step.assert_left_in_place(&[obj::LISTING_PRICE, "function api.listing_currency"]);

    // Drop a column and a view in the same change, with a backfill before it.
    let mut v6 = v5.clone().migration("007_merge_description", indoc! {r#"
        update item set title = title || ' - ' || description where description is not null;
        alter table item drop column description;
    "#});
    v6.files.remove("core/item_card.sql");
    let step = evolution.advance(&v6).await?;
    assert_eq!(step.result.objects_deleted.len(), 1, "deleted: {:?}", step.result.objects_deleted);

    // Settle with a change that touches nothing.
    let v7 = v6.clone().migration("008_owner", "alter table item owner to current_user;");
    evolution.advance(&v7).await?.assert_recreated(&[]);

    let env = &evolution.env;
    assert_eq!(count(env, "select count(*) from api.list_live_listings()").await?, 2);
    assert_eq!(count(env, "select count(*) from core.item_detail where title like '% - %'").await?, 2);
    Ok(())
}
