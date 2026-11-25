//! A harness for walking a database through successive states of a pgmg
//! project and checking each incremental apply two ways:
//!
//! - **what was left alone**, by object identity: an object that was not
//!   dropped keeps its OID;
//! - **that the result is right**, by building the same project from scratch
//!   in a second database and requiring the incrementally migrated schema, and
//!   pgmg's own state, to be identical to it. An object wrongly left in place
//!   shows up as a difference from the fresh build.
//!
//! The `marketplace` fixture is shaped like a real pgmg project: data tables in
//! `public` created by migrations, views and helpers in `core`, and an `api`
//! schema of functions on top, with triggers, a materialized view, managed
//! indexes and comments.

use std::collections::{BTreeMap, BTreeSet};

use super::TestEnvironment;
use indoc::indoc;
use pgmg::commands::{execute_apply, execute_plan, ApplyResult, ChangeOperation, PlanResult};
use pgmg::config::PgmgConfig;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A pgmg project at one point in its history: its migrations and its managed
/// SQL files.
#[derive(Clone, Default)]
pub struct Project {
    pub migrations: BTreeMap<String, String>,
    pub files: BTreeMap<String, String>,
}

impl Project {
    pub fn migration(mut self, name: &str, sql: &str) -> Self {
        self.migrations.insert(name.to_string(), sql.to_string());
        self
    }

    pub fn file(mut self, path: &str, sql: &str) -> Self {
        self.files.insert(path.to_string(), sql.to_string());
        self
    }

    /// Make the environment's directories hold exactly this project.
    pub fn write_to(&self, env: &TestEnvironment) -> TestResult {
        for dir in [&env.migrations_dir, &env.sql_dir] {
            std::fs::remove_dir_all(dir)?;
            std::fs::create_dir_all(dir)?;
        }
        for (name, sql) in &self.migrations {
            std::fs::write(env.migrations_dir.join(format!("{name}.sql")), sql)?;
        }
        for (path, sql) in &self.files {
            let path = env.sql_dir.join(path);
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(path, sql)?;
        }
        Ok(())
    }
}

/// The result of taking a database from one project state to the next.
pub struct Step {
    pub plan: PlanResult,
    pub result: ApplyResult,
    /// Objects that exist before and after the step under a different OID,
    /// i.e. that were dropped and recreated. Named like `view core.live_listing`.
    pub recreated: BTreeSet<String>,
    /// Every object that existed before the step (for negative assertions).
    pub existed_before: BTreeSet<String>,
}

impl Step {
    /// Objects the plan scheduled for drop-and-recreate, with the reason given.
    pub fn planned_updates(&self) -> BTreeMap<String, String> {
        self.plan.changes.iter().filter_map(|change| match change {
            ChangeOperation::UpdateObject { object, reason, .. } => Some((
                format!(
                    "{:?} {}",
                    object.object_type,
                    pgmg::sql::format_qualified_name(&object.qualified_name)
                ).to_lowercase(),
                reason.clone(),
            )),
            _ => None,
        }).collect()
    }

    pub fn recheck_names(&self) -> BTreeSet<String> {
        self.plan.recheck_objects.iter()
            .map(|object| pgmg::sql::format_qualified_name(&object.qualified_name))
            .collect()
    }

    #[track_caller]
    pub fn assert_recreated(&self, expected: &[&str]) {
        for name in expected {
            assert!(self.existed_before.contains(*name), "fixture has no object {name}");
        }
        let expected: BTreeSet<String> = expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(self.recreated, expected, "set of dropped-and-recreated objects");
    }

    #[track_caller]
    pub fn assert_left_in_place(&self, names: &[&str]) {
        for name in names {
            assert!(self.existed_before.contains(*name), "fixture has no object {name}");
            assert!(!self.recreated.contains(*name), "{name} was dropped and recreated");
        }
    }
}

/// A database being walked through successive project states.
pub struct Evolution {
    pub env: TestEnvironment,
}

impl Evolution {
    /// Build `base` from scratch in a new database.
    pub async fn start(base: &Project) -> Result<Self, Box<dyn std::error::Error>> {
        let env = TestEnvironment::new().await?;
        base.write_to(&env)?;
        let result = apply(&env).await?;
        assert!(result.errors.is_empty(), "base apply failed: {:?}", result.errors);
        Ok(Self { env })
    }

    /// Move the database to `next` with an incremental apply, then check the
    /// outcome against a from-scratch build of `next`.
    pub async fn advance(&self, next: &Project) -> Result<Step, Box<dyn std::error::Error>> {
        let before = object_identities(&self.env).await?;

        next.write_to(&self.env)?;
        let plan = plan(&self.env).await?;
        let result = apply(&self.env).await?;
        assert!(result.errors.is_empty(), "incremental apply failed: {:?}", result.errors);

        let after = object_identities(&self.env).await?;
        let recreated = before.iter()
            .filter(|(name, oid)| after.get(*name).is_some_and(|new_oid| new_oid != *oid))
            .map(|(name, _)| name.clone())
            .collect();

        // The apply must leave nothing pending.
        let follow_up = plan_for(&self.env).await?;
        assert!(
            follow_up.changes.is_empty() && follow_up.new_migrations.is_empty(),
            "a second plan found more to do: {:?}",
            follow_up.changes,
        );

        // And it must land exactly where a fresh build of the same files does.
        let fresh = TestEnvironment::new().await?;
        next.write_to(&fresh)?;
        let fresh_result = apply(&fresh).await?;
        assert!(fresh_result.errors.is_empty(), "fresh build failed: {:?}", fresh_result.errors);
        assert_same_schema(&self.env, &fresh).await?;

        Ok(Step { plan, result, recreated, existed_before: before.into_keys().collect() })
    }
}

pub async fn apply(env: &TestEnvironment) -> Result<ApplyResult, Box<dyn std::error::Error>> {
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await
}

pub async fn plan_for(env: &TestEnvironment) -> Result<PlanResult, Box<dyn std::error::Error>> {
    execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await
}

pub use plan_for as plan;

/// OIDs of every view, materialized view, standalone index, function and
/// trigger in the project's schemas, keyed by a readable name.
pub async fn object_identities(env: &TestEnvironment) -> Result<BTreeMap<String, u32>, Box<dyn std::error::Error>> {
    let rows = env.client.query(indoc! {r#"
        WITH ns AS (SELECT oid, nspname FROM pg_namespace WHERE nspname IN ('public', 'api', 'core'))
        SELECT CASE c.relkind WHEN 'v' THEN 'view ' WHEN 'm' THEN 'materialized view ' ELSE 'index ' END
                 || ns.nspname || '.' || c.relname, c.oid
        FROM pg_class c JOIN ns ON ns.oid = c.relnamespace
        WHERE c.relkind IN ('v', 'm')
           OR (c.relkind = 'i' AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = c.oid))
        UNION ALL
        SELECT 'function ' || ns.nspname || '.' || p.proname, p.oid
        FROM pg_proc p JOIN ns ON ns.oid = p.pronamespace
        UNION ALL
        SELECT 'trigger ' || t.tgname || ' on ' || ns.nspname || '.' || c.relname, t.oid
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN ns ON ns.oid = c.relnamespace
        WHERE NOT t.tgisinternal
    "#}, &[]).await?;
    Ok(rows.iter().map(|row| (row.get::<_, String>(0), row.get::<_, u32>(1))).collect())
}

/// Everything about the schema that a fresh build and an incremental apply
/// must agree on, with nothing that legitimately differs (OIDs, timestamps).
pub async fn schema_fingerprint(env: &TestEnvironment) -> Result<BTreeMap<(String, String), String>, Box<dyn std::error::Error>> {
    let rows = env.client.query(indoc! {r#"
        WITH ns AS (SELECT oid, nspname FROM pg_namespace WHERE nspname IN ('public', 'api', 'core')),
        rel AS (SELECT c.oid, c.relkind, ns.nspname || '.' || c.relname AS name, c.relrowsecurity, c.relforcerowsecurity
                FROM pg_class c JOIN ns ON ns.oid = c.relnamespace),
        fn AS (SELECT p.oid, p.prokind,
                      ns.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS name
               FROM pg_proc p JOIN ns ON ns.oid = p.pronamespace)
        SELECT 'columns', rel.name,
               string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
                          || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
                          || coalesce(' default ' || pg_get_expr(d.adbin, d.adrelid), ''),
                          ', ' ORDER BY a.attnum)
        FROM rel
        JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE rel.relkind IN ('r', 'v', 'm', 'p')
        GROUP BY rel.name
        UNION ALL
        SELECT 'definition', rel.name, pg_get_viewdef(rel.oid, true) FROM rel WHERE rel.relkind IN ('v', 'm')
        UNION ALL
        SELECT 'function', fn.name, pg_get_functiondef(fn.oid) FROM fn WHERE fn.prokind IN ('f', 'p')
        UNION ALL
        SELECT 'trigger', rel.name || '.' || t.tgname, pg_get_triggerdef(t.oid) || ' enabled=' || t.tgenabled::text
        FROM pg_trigger t JOIN rel ON rel.oid = t.tgrelid WHERE NOT t.tgisinternal
        UNION ALL
        SELECT 'index', rel.name, pg_get_indexdef(rel.oid) FROM rel WHERE rel.relkind = 'i'
        UNION ALL
        SELECT 'constraint', rel.name || '.' || k.conname, pg_get_constraintdef(k.oid)
        FROM pg_constraint k JOIN rel ON rel.oid = k.conrelid
        UNION ALL
        SELECT 'row security', rel.name, rel.relrowsecurity::text || '/' || rel.relforcerowsecurity::text
        FROM rel WHERE rel.relkind = 'r'
        UNION ALL
        SELECT 'comment', rel.name || CASE WHEN d.objsubid > 0 THEN '#' || d.objsubid ELSE '' END, d.description
        FROM pg_description d JOIN rel ON rel.oid = d.objoid WHERE d.classoid = 'pg_class'::regclass
        UNION ALL
        SELECT 'comment', fn.name, d.description
        FROM pg_description d JOIN fn ON fn.oid = d.objoid WHERE d.classoid = 'pg_proc'::regclass
        UNION ALL
        SELECT 'pgmg state', object_type || ' ' || object_name, ddl_hash FROM pgmg.pgmg_state
        UNION ALL
        SELECT 'pgmg dependency',
               dependent_type || ' ' || dependent_name || ' -> ' || dependency_type || ' ' || dependency_name,
               dependency_kind
        FROM pgmg.pgmg_dependencies
        UNION ALL
        SELECT 'pgmg migration', name, '' FROM pgmg.pgmg_migrations
    "#}, &[]).await?;
    Ok(rows.iter()
        .map(|row| ((row.get::<_, String>(0), row.get::<_, String>(1)), row.get::<_, String>(2)))
        .collect())
}

pub async fn assert_same_schema(incremental: &TestEnvironment, fresh: &TestEnvironment) -> TestResult {
    let incremental = schema_fingerprint(incremental).await?;
    let fresh = schema_fingerprint(fresh).await?;
    assert!(!fresh.is_empty(), "fingerprint query returned nothing");

    let mut differences = Vec::new();
    let keys: BTreeSet<_> = incremental.keys().chain(fresh.keys()).collect();
    for key in keys {
        let (ours, theirs) = (incremental.get(key), fresh.get(key));
        if ours != theirs {
            differences.push(format!(
                "{} {}\n    incremental: {}\n    fresh build: {}",
                key.0, key.1,
                ours.map(String::as_str).unwrap_or("<missing>"),
                theirs.map(String::as_str).unwrap_or("<missing>"),
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "incremental apply diverged from a fresh build:\n{}",
        differences.join("\n"),
    );
    Ok(())
}

pub async fn count(env: &TestEnvironment, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(env.client.query_one(sql, &[]).await?.get(0))
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Names of the fixture's managed objects, as `object_identities` reports them.
pub mod obj {
    pub const LIVE_LISTING: &str = "view core.live_listing";
    pub const ITEM_DETAIL: &str = "view core.item_detail";
    pub const ITEM_CARD: &str = "view core.item_card";
    pub const SELLER_STATS: &str = "materialized view core.seller_stats";
    pub const SELLER_STATS_IDX: &str = "index core.seller_stats_seller_id_idx";
    pub const ITEM_TITLE_IDX: &str = "index public.item_title_idx";
    pub const LIST_LIVE_LISTINGS: &str = "function api.list_live_listings";
    pub const LISTING_PRICE: &str = "function api.listing_price";
    pub const GET_ITEM: &str = "function api.get_item";
    pub const COUNT_LIVE: &str = "function api.count_live_listings";
    pub const SET_PRICE: &str = "function api.set_price";
    pub const TOUCH_LISTING_FN: &str = "function core.touch_listing";
    pub const AUDIT_ROW_FN: &str = "function core.audit_row";
    pub const TOUCH_LISTING_TRG: &str = "trigger touch_listing on public.listing";
    pub const AUDIT_ITEM_TRG: &str = "trigger audit_item on public.item";

    pub const ALL: &[&str] = &[
        LIVE_LISTING, ITEM_DETAIL, ITEM_CARD, SELLER_STATS, SELLER_STATS_IDX, ITEM_TITLE_IDX,
        LIST_LIVE_LISTINGS, LISTING_PRICE, GET_ITEM, COUNT_LIVE, SET_PRICE, TOUCH_LISTING_FN, AUDIT_ROW_FN,
        TOUCH_LISTING_TRG, AUDIT_ITEM_TRG,
    ];
}

pub const LIVE_LISTING_SQL: &str = indoc! {r#"
    comment on view core.live_listing is 'Listings that can currently be bought.';

    create view core.live_listing as
    select
      l.id           as listing_id,
      i.id           as item_id,
      i.title,
      a.display_name as seller_name,
      l.price_cents,
      l.quantity
    from listing l
      join item i on i.id = l.item_id
      join account a on a.id = i.seller_id
    where l.quantity > 0;
"#};

/// A small marketplace. What each managed object reads, and how:
///
/// | object                      | reads                       | bound to it by                     |
/// |-----------------------------|-----------------------------|------------------------------------|
/// | core.live_listing (view)    | account, item, listing      | its query (every column qualified) |
/// | core.item_detail (view)     | account, item               | its query (`select i.*`)           |
/// | core.item_card (view)       | core.item_detail            | its query                          |
/// | core.seller_stats (matview) | item, listing               | its query (`count(*)`, `a * b`)    |
/// |   seller_stats_seller_id_idx|                             | the matview                        |
/// | item_title_idx (index)      | item                        | the table                          |
/// | api.list_live_listings      | core.live_listing           | result type `setof core.live_listing` |
/// | api.count_live_listings     | core.live_listing           | nothing: plpgsql body only         |
/// | api.listing_price           | listing                     | nothing: plpgsql body only         |
/// | api.set_price               | listing                     | parameter `listing.price_cents%type` |
/// | api.get_item                | item                        | result type `item`                 |
/// | touch_listing (trigger)     | listing, core.touch_listing | the table and the function         |
/// | audit_item (trigger)        | item, core.audit_row        | the table and the function         |
pub fn marketplace() -> Project {
    Project::default()
        .migration("001_schemas", indoc! {r#"
            create schema api;
            create schema core;
        "#})
        .migration("002_tables", indoc! {r#"
            create table account (
              id           bigint primary key,
              email        text not null unique,
              display_name text not null,
              created_at   timestamptz not null default now()
            );

            create table item (
              id          bigint primary key,
              seller_id   bigint not null references account (id),
              title       text not null,
              description text
            );

            create table listing (
              id          bigint primary key,
              item_id     bigint not null references item (id),
              price_cents integer not null,
              quantity    integer not null default 1,
              updated_at  timestamptz not null default now()
            );

            create table audit_log (
              id         bigint generated always as identity primary key,
              table_name text not null,
              row_id     bigint not null
            );
        "#})
        .file("core/live_listing.sql", LIVE_LISTING_SQL)
        .file("core/item_detail.sql", indoc! {r#"
            create view core.item_detail as
            select i.*, a.display_name as seller_name
            from item i
              join account a on a.id = i.seller_id;
        "#})
        .file("core/item_card.sql", indoc! {r#"
            create view core.item_card as
            select d.id, d.title, d.seller_name
            from core.item_detail d;
        "#})
        .file("core/seller_stats.sql", indoc! {r#"
            create materialized view core.seller_stats as
            select
              i.seller_id,
              count(*)                          as item_count,
              sum(l.price_cents * l.quantity)   as stock_value
            from item i
              left join listing l on l.item_id = i.id
            group by i.seller_id;

            create unique index seller_stats_seller_id_idx on core.seller_stats (seller_id);
        "#})
        .file("core/item_title_idx.sql", indoc! {r#"
            create index item_title_idx on item (lower(title));
        "#})
        .file("api/list_live_listings.sql", indoc! {r#"
            comment on function api.list_live_listings is 'Every listing a buyer can see.';

            create function api.list_live_listings()
              returns setof core.live_listing
              language plpgsql stable
            as $$
            begin
              return query select * from core.live_listing;
            end;
            $$;
        "#})
        .file("api/listing_price.sql", indoc! {r#"
            create function api.listing_price(p_listing_id bigint)
              returns integer
              language plpgsql stable
            as $$
            declare
              v_price integer;
            begin
              select l.price_cents into v_price from listing l where l.id = p_listing_id;
              return v_price;
            end;
            $$;
        "#})
        .file("api/count_live_listings.sql", indoc! {r#"
            create function api.count_live_listings()
              returns bigint
              language plpgsql stable
            as $$
            declare
              v_count bigint;
            begin
              select count(*) into v_count from core.live_listing;
              return v_count;
            end;
            $$;
        "#})
        .file("api/set_price.sql", indoc! {r#"
            create function api.set_price(p_listing_id bigint, p_price listing.price_cents%type)
              returns void
              language plpgsql
            as $$
            begin
              update listing set price_cents = p_price where id = p_listing_id;
            end;
            $$;
        "#})
        .file("api/get_item.sql", indoc! {r#"
            create function api.get_item(p_item_id bigint)
              returns item
              language sql stable
            as $$
              select * from item where id = p_item_id;
            $$;
        "#})
        .file("core/touch_listing.sql", indoc! {r#"
            create function core.touch_listing()
              returns trigger
              language plpgsql
            as $$
            begin
              new.updated_at := now();
              return new;
            end;
            $$;

            create trigger touch_listing
              before update on listing
              for each row execute function core.touch_listing();
        "#})
        .file("core/audit.sql", indoc! {r#"
            create function core.audit_row()
              returns trigger
              language plpgsql
            as $$
            begin
              insert into audit_log (table_name, row_id) values (tg_table_name, new.id);
              return new;
            end;
            $$;

            create trigger audit_item
              after insert or update on item
              for each row execute function core.audit_row();
        "#})
}

pub const SEED: &str = indoc! {r#"
    insert into account (id, email, display_name) values (1, 'ann@example.com', 'Ann'), (2, 'bo@example.com', 'Bo');
    insert into item (id, seller_id, title, description) values (10, 1, 'Dragon', 'Red'), (11, 2, 'Knight', 'Unpainted');
    insert into listing (id, item_id, price_cents, quantity) values (100, 10, 2500, 2), (101, 11, 900, 1);
"#};

/// Objects bound to `item`, directly or through something bound to it: what a
/// rebuild of the table takes with it.
pub const BOUND_TO_ITEM: &[&str] = &[
    obj::LIVE_LISTING, obj::ITEM_DETAIL, obj::ITEM_CARD, obj::SELLER_STATS, obj::SELLER_STATS_IDX,
    obj::ITEM_TITLE_IDX, obj::LIST_LIVE_LISTINGS, obj::GET_ITEM, obj::AUDIT_ITEM_TRG,
];

/// Objects bound to `listing`, directly or through something bound to it.
/// api.listing_price and api.count_live_listings read it too, but only from
/// their bodies.
pub const BOUND_TO_LISTING: &[&str] = &[
    obj::LIVE_LISTING, obj::SELLER_STATS, obj::SELLER_STATS_IDX, obj::LIST_LIVE_LISTINGS,
    obj::SET_PRICE, obj::TOUCH_LISTING_TRG,
];
