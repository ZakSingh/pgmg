use pgmg::{apply_migrations_with_options, PgmgConfig};
use tokio_postgres::NoTls;

/// Test that comments are properly handled when their parent objects were already dropped in Phase 1
/// This tests the exact scenario from the error log where a view is dropped for update, 
/// then a comment on a column that wasn't in the update tries to be deleted
#[tokio::test]
async fn test_comment_deletion_with_parent_dropped_in_phase1() {
    // Skip if no database URL is provided
    let db_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: DATABASE_URL not set");
            return;
        }
    };

    // Create a temporary test directory
    let test_dir = tempfile::tempdir().unwrap();
    let code_dir = test_dir.path().join("code");
    std::fs::create_dir(&code_dir).unwrap();

    // Connect to database and create test schema
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await.unwrap();
    
    // Spawn connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up any existing test schema
    let _ = client.execute("DROP SCHEMA IF EXISTS test_phase1 CASCADE", &[]).await;
    client.execute("CREATE SCHEMA test_phase1", &[]).await.unwrap();

    // Create initial view with comments
    let initial_sql = r#"
-- Create a view with multiple columns
CREATE VIEW test_phase1.product_details AS
SELECT 
    1 as product_id,
    'gadget' as product_type,
    'Amazing Product' as name,
    'amazing-product' as slug,
    'SKU123' as sku,
    false as is_discontinued,
    'Electronics' as category,
    'Primary Category Path' as primary_category_path,
    ARRAY['img1.jpg', 'img2.jpg'] as product_images,
    5 as listing_count,
    NOW() as created_at,
    NOW() as updated_at;

-- Add comments on the view and its columns
COMMENT ON VIEW test_phase1.product_details IS 'Product details view';
COMMENT ON COLUMN test_phase1.product_details.product_id IS 'Product ID';
COMMENT ON COLUMN test_phase1.product_details.product_type IS 'Type of product';
COMMENT ON COLUMN test_phase1.product_details.name IS 'Product name';
COMMENT ON COLUMN test_phase1.product_details.slug IS 'URL slug';
COMMENT ON COLUMN test_phase1.product_details.sku IS 'Stock keeping unit';
COMMENT ON COLUMN test_phase1.product_details.is_discontinued IS 'Whether product is discontinued';
COMMENT ON COLUMN test_phase1.product_details.category IS 'Product category';
COMMENT ON COLUMN test_phase1.product_details.primary_category_path IS 'Primary category path';
COMMENT ON COLUMN test_phase1.product_details.product_images IS 'Product image URLs';
COMMENT ON COLUMN test_phase1.product_details.listing_count IS 'Number of listings';
COMMENT ON COLUMN test_phase1.product_details.created_at IS 'Creation timestamp';
COMMENT ON COLUMN test_phase1.product_details.updated_at IS 'Last update timestamp';
"#;

    std::fs::write(code_dir.join("01_view.sql"), initial_sql).unwrap();

    // Configure pgmg
    let mut config = PgmgConfig::default();
    config.connection_string = Some(db_url.clone());
    config.code_dir = Some(code_dir.clone());
    config.development_mode = Some(false);
    config.check_plpgsql = Some(false);

    // Apply initial state
    let result = apply_migrations_with_options(&config, None, Some(code_dir.clone())).await.unwrap();
    assert!(result.errors.is_empty());

    // Now update the view but remove the primary_category_path column
    // This simulates the scenario where a view is updated but some column comments remain
    let updated_sql = r#"
-- Updated view without primary_category_path column
CREATE VIEW test_phase1.product_details AS
SELECT 
    1 as product_id,
    'gadget' as product_type,
    'Amazing Product' as name,
    'amazing-product' as slug,
    'SKU123' as sku,
    false as is_discontinued,
    'Electronics' as category,
    -- primary_category_path removed
    ARRAY['img1.jpg', 'img2.jpg'] as product_images,
    5 as listing_count,
    NOW() as created_at,
    NOW() as updated_at;

-- Comments for existing columns (but not for primary_category_path since it's gone)
COMMENT ON VIEW test_phase1.product_details IS 'Product details view - updated';
COMMENT ON COLUMN test_phase1.product_details.product_id IS 'Product ID';
COMMENT ON COLUMN test_phase1.product_details.product_type IS 'Type of product';
COMMENT ON COLUMN test_phase1.product_details.name IS 'Product name';
COMMENT ON COLUMN test_phase1.product_details.slug IS 'URL slug';
COMMENT ON COLUMN test_phase1.product_details.sku IS 'Stock keeping unit';
COMMENT ON COLUMN test_phase1.product_details.is_discontinued IS 'Whether product is discontinued';
COMMENT ON COLUMN test_phase1.product_details.category IS 'Product category';
COMMENT ON COLUMN test_phase1.product_details.product_images IS 'Product image URLs';
COMMENT ON COLUMN test_phase1.product_details.listing_count IS 'Number of listings';
COMMENT ON COLUMN test_phase1.product_details.created_at IS 'Creation timestamp';
COMMENT ON COLUMN test_phase1.product_details.updated_at IS 'Last update timestamp';
"#;

    std::fs::write(code_dir.join("01_view.sql"), updated_sql).unwrap();

    // Apply update - this should handle the orphaned comment gracefully
    let result = apply_migrations_with_options(&config, None, Some(code_dir.clone())).await.unwrap();
    
    // Should succeed without errors
    assert!(result.errors.is_empty(), "Got errors: {:?}", result.errors);
    
    // Verify the view was updated
    assert!(result.objects_updated.contains(&"test_phase1.product_details".to_string()));

    // Clean up
    let _ = client.execute("DROP SCHEMA IF EXISTS test_phase1 CASCADE", &[]).await;
}

/// Test unit function for extract_parent_from_comment_identifier
#[test]
fn test_extract_parent_from_comment_identifier() {
    // Test column comments
    assert_eq!(
        extract_parent_from_comment_identifier_test("column:api.product_details.primary_category_path"),
        Some("api.product_details".to_string())
    );
    
    assert_eq!(
        extract_parent_from_comment_identifier_test("column:users.email"),
        Some("users".to_string())
    );
    
    assert_eq!(
        extract_parent_from_comment_identifier_test("column:email"),
        None
    );
    
    // Test table/view comments
    assert_eq!(
        extract_parent_from_comment_identifier_test("table:api.users"),
        Some("api.users".to_string())
    );
    
    assert_eq!(
        extract_parent_from_comment_identifier_test("view:product_summary"),
        Some("product_summary".to_string())
    );
    
    assert_eq!(
        extract_parent_from_comment_identifier_test("materialized_view:search_index"),
        Some("search_index".to_string())
    );
    
    // Test other types (no parent)
    assert_eq!(
        extract_parent_from_comment_identifier_test("function:api.get_user()"),
        None
    );
    
    assert_eq!(
        extract_parent_from_comment_identifier_test("trigger:update_timestamp:users"),
        None
    );
}

// Copy of the function for testing (since it's not exported)
fn extract_parent_from_comment_identifier_test(comment_identifier: &str) -> Option<String> {
    let parts: Vec<&str> = comment_identifier.split(':').collect();
    
    match parts.as_slice() {
        ["column", column_path] => {
            let column_parts: Vec<&str> = column_path.rsplitn(2, '.').collect();
            if column_parts.len() == 2 {
                Some(column_parts[1].to_string())
            } else {
                None
            }
        }
        ["table", name] | ["view", name] | ["materialized_view", name] => {
            Some(name.to_string())
        }
        _ => None,
    }
}