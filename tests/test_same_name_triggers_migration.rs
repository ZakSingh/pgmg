mod common;

use common::TestEnvironment;
use pgmg::commands::{execute_apply, execute_plan};
use pgmg::config::PgmgConfig;
use indoc::indoc;

/// Regression test for the reported 2BP01 failure: two triggers named
/// `reprice_on_price_adjustment_change` on different tables, both invoking a
/// function that depends on `listing`. A migration altering `listing` forces
/// the function to drop/recreate, which requires BOTH triggers to be
/// pre-dropped first. With table-less trigger identity, both triggers merged
/// into one graph node, the pre-drop targeted the wrong table (a silent no-op
/// under IF EXISTS), and DROP FUNCTION failed with 2BP01 because the real
/// trigger was still live.
#[tokio::test]
async fn test_migration_recreates_function_under_same_name_triggers() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;

    env.execute_sql("CREATE TABLE listing (id SERIAL PRIMARY KEY, price NUMERIC, quantity INT)").await?;
    env.execute_sql("CREATE TABLE ebay_account (id SERIAL PRIMARY KEY, price_adjustment NUMERIC)").await?;
    env.execute_sql("CREATE TABLE shopify_account (id SERIAL PRIMARY KEY, price_adjustment NUMERIC)").await?;

    let reprice_function = indoc! {r#"
        CREATE OR REPLACE FUNCTION reprice_listings()
        RETURNS trigger AS $$
        BEGIN
            UPDATE listing SET price = price * 1.01;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#};

    let ebay_trigger = indoc! {r#"
        CREATE TRIGGER reprice_on_price_adjustment_change
        AFTER UPDATE ON ebay_account
        FOR EACH ROW EXECUTE FUNCTION reprice_listings();
    "#};

    let shopify_trigger = indoc! {r#"
        CREATE TRIGGER reprice_on_price_adjustment_change
        AFTER UPDATE ON shopify_account
        FOR EACH ROW EXECUTE FUNCTION reprice_listings();
    "#};

    env.write_sql_file("reprice_listings.sql", reprice_function).await?;
    env.write_sql_file("ebay_trigger.sql", ebay_trigger).await?;
    env.write_sql_file("shopify_trigger.sql", shopify_trigger).await?;

    let apply_result = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result.errors.is_empty(), "initial apply failed: {:?}", apply_result.errors);

    let live_count: i64 = env.query_scalar(
        "SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'reprice_on_price_adjustment_change'"
    ).await?;
    assert_eq!(live_count, 2, "both triggers should exist after initial apply");

    // A migration altering `listing` — the function depends on it, so pgmg
    // must drop/recreate the function, and therefore both triggers first.
    env.write_migration(
        "20260707201331_relax_listing_initial_quantity_gte_zero",
        "ALTER TABLE listing ALTER COLUMN quantity SET DEFAULT 0;",
    ).await?;

    let apply_result2 = execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    assert!(apply_result2.errors.is_empty(),
        "apply with migration failed (pre-fix: 2BP01 dependent objects still exist): {:?}",
        apply_result2.errors);
    assert!(apply_result2.migrations_applied.contains(
        &"20260707201331_relax_listing_initial_quantity_gte_zero".to_string()));

    // Both triggers survived the drop/recreate cycle, one per table
    for table in ["ebay_account", "shopify_account"] {
        let count: i64 = env.query_scalar(&format!(
            "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
             WHERE t.tgname = 'reprice_on_price_adjustment_change' AND c.relname = '{}'",
            table
        )).await?;
        assert_eq!(count, 1, "trigger on {} should exist after migration apply", table);
    }

    // Both tracked under composite keys, and nothing plans as changed
    let tracked = env.get_tracked_objects().await?;
    assert!(tracked.contains(&("trigger".to_string(),
        "reprice_on_price_adjustment_change:ebay_account".to_string())), "{:?}", tracked);
    assert!(tracked.contains(&("trigger".to_string(),
        "reprice_on_price_adjustment_change:shopify_account".to_string())), "{:?}", tracked);

    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert!(plan.changes.is_empty() && plan.new_migrations.is_empty(),
        "expected clean plan after migration apply, got {} changes", plan.changes.len());

    Ok(())
}
