mod common;

use common::{TestEnvironment, plan_output::*};
use pgmg::commands::plan::{execute_plan, ChangeOperation};
use pgmg::sql::ObjectType;
use indoc::indoc;

#[tokio::test]
async fn test_comment_display_with_function() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // First create the schema
    env.execute_sql("CREATE SCHEMA IF NOT EXISTS api").await?;
    
    // Create a function with comment
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE OR REPLACE FUNCTION api.claim_tasks(
            p_user_id uuid,
            p_limit integer DEFAULT 10
        )
        RETURNS TABLE(task_id uuid, task_name text)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- Function implementation
            RETURN QUERY 
            SELECT id, name 
            FROM api.task 
            WHERE assigned_user_id IS NULL
            LIMIT p_limit;
        END;
        $$;
        
        COMMENT ON FUNCTION api.claim_tasks(uuid, integer) 
        IS 'Claims available tasks for a user with optional limit';
    "#}).await?;
    
    // Execute plan
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Verify the plan contains both function and comment
    assert_eq!(plan.changes.len(), 2, "Expected 2 changes (function and comment)");
    
    // Find the function and comment in the plan
    let mut found_function = false;
    let mut found_comment = false;
    let mut comment_index = None;
    
    for (i, change) in plan.changes.iter().enumerate() {
        match change {
            ChangeOperation::CreateObject { object, .. } => {
                if object.object_type == ObjectType::Function && 
                   object.qualified_name.name == "claim_tasks" {
                    found_function = true;
                }
                if object.object_type == ObjectType::Comment {
                    found_comment = true;
                    comment_index = Some(i);
                    // Verify comment name format
                    assert!(
                        object.qualified_name.name.starts_with("function:api.claim_tasks"),
                        "Comment name should start with 'function:api.claim_tasks', got: {}",
                        object.qualified_name.name
                    );
                }
            }
            _ => {}
        }
    }
    
    assert!(found_function, "Function not found in plan");
    assert!(found_comment, "Comment not found in plan");
    
    // Verify comment comes after function (for grouping to work)
    if let Some(comment_idx) = comment_index {
        assert!(comment_idx > 0, "Comment should come after function in plan");
    }
    
    // Use utility functions to verify comment grouping
    assert_comment_grouped_with_parent(
        &plan,
        ObjectType::Function,
        "api.claim_tasks",
        "Claims available tasks for a user with optional limit"
    )?;
    
    // Verify comment naming format
    assert_comment_name_format(&plan, "function:", "api.claim_tasks")?;
    
    // Verify all comments are ordered correctly
    verify_comment_ordering(&plan)?;
    
    Ok(())
}

#[tokio::test]
async fn test_comment_display_with_table() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    env.write_sql_file("table.sql", indoc! {r#"
        CREATE TABLE users (
            id uuid PRIMARY KEY,
            email text NOT NULL UNIQUE,
            created_at timestamptz DEFAULT now()
        );
        
        COMMENT ON TABLE users IS 'User accounts in the system';
        COMMENT ON COLUMN users.email IS 'User email address (must be unique)';
        COMMENT ON COLUMN users.created_at IS 'When the user account was created';
    "#}).await?;
    
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should have table + 3 comments (1 table, 2 columns)
    assert!(plan.changes.len() >= 4, "Expected at least 4 changes");
    
    // Verify comment naming
    let mut table_comment_found = false;
    let mut column_comments_found = 0;
    
    for change in &plan.changes {
        if let ChangeOperation::CreateObject { object, .. } = change {
            if object.object_type == ObjectType::Comment {
                if object.qualified_name.name == "table:users" {
                    table_comment_found = true;
                } else if object.qualified_name.name.starts_with("column:users.") {
                    column_comments_found += 1;
                }
            }
        }
    }
    
    assert!(table_comment_found, "Table comment not found");
    assert_eq!(column_comments_found, 2, "Expected 2 column comments");
    
    Ok(())
}

#[tokio::test]
async fn test_comment_display_with_updates() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tracking
    let state_manager = pgmg::db::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // First apply: create function with comment
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE FUNCTION test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
        COMMENT ON FUNCTION test_func() IS 'Original comment';
    "#}).await?;
    
    // Apply it
    let apply_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(apply_result.errors.len(), 0, "Should apply without errors");
    
    // Update comment only
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE FUNCTION test_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
        COMMENT ON FUNCTION test_func() IS 'Updated comment text';
    "#}).await?;
    
    // Plan should show comment update only
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should only have the comment update
    assert_eq!(plan.changes.len(), 1, "Expected 1 change (comment update)");
    
    if let ChangeOperation::UpdateObject { object, .. } = &plan.changes[0] {
        assert_eq!(object.object_type, ObjectType::Comment);
        assert!(object.qualified_name.name.starts_with("function:test_func"));
    } else {
        panic!("Expected UpdateObject for comment");
    }
    
    Ok(())
}

#[tokio::test] 
async fn test_comment_display_multiple_objects() -> Result<(), Box<dyn std::error::Error>> {
    // Test with multiple objects to ensure comments are matched to correct parents
    let env = TestEnvironment::new().await?;
    
    env.write_sql_file("objects.sql", indoc! {r#"
        -- First function
        CREATE FUNCTION func1() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
        COMMENT ON FUNCTION func1() IS 'First function comment';
        
        -- Second function  
        CREATE FUNCTION func2() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;
        COMMENT ON FUNCTION func2() IS 'Second function comment';
        
        -- Table
        CREATE TABLE test_table (id int PRIMARY KEY);
        COMMENT ON TABLE test_table IS 'Test table comment';
        
        -- Type
        CREATE TYPE status_enum AS ENUM ('active', 'inactive');
        COMMENT ON TYPE status_enum IS 'Status enumeration type';
    "#}).await?;
    
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should have 8 changes (4 objects + 4 comments)
    assert_eq!(plan.changes.len(), 8, "Expected 8 changes");
    
    // Verify each comment is correctly named
    let mut comments_found = std::collections::HashMap::new();
    
    for change in &plan.changes {
        if let ChangeOperation::CreateObject { object, .. } = change {
            if object.object_type == ObjectType::Comment {
                let name = &object.qualified_name.name;
                if name.starts_with("function:func1") {
                    comments_found.insert("func1", true);
                } else if name.starts_with("function:func2") {
                    comments_found.insert("func2", true);
                } else if name == "table:test_table" {
                    comments_found.insert("table", true);
                } else if name == "type:status_enum" {
                    comments_found.insert("type", true);
                }
            }
        }
    }
    
    assert_eq!(comments_found.len(), 4, "Should find all 4 comments");
    assert!(comments_found.get("func1").is_some(), "func1 comment not found");
    assert!(comments_found.get("func2").is_some(), "func2 comment not found");
    assert!(comments_found.get("table").is_some(), "table comment not found");
    assert!(comments_found.get("type").is_some(), "type comment not found");
    
    Ok(())
}

#[tokio::test]
async fn test_comment_display_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create schema first
    env.execute_sql("CREATE SCHEMA myschema").await?;
    
    env.write_sql_file("schema_objects.sql", indoc! {r#"
        -- Function in custom schema
        CREATE FUNCTION myschema.process_data(input text) 
        RETURNS text AS $$ 
        BEGIN 
            RETURN upper(input); 
        END; 
        $$ LANGUAGE plpgsql;
        
        COMMENT ON FUNCTION myschema.process_data(text) IS 'Processes input data';
        
        -- Table in custom schema
        CREATE TABLE myschema.data_table (
            id serial PRIMARY KEY,
            value text
        );
        
        COMMENT ON TABLE myschema.data_table IS 'Stores processed data';
    "#}).await?;
    
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Verify comments include schema in their identifiers
    for change in &plan.changes {
        if let ChangeOperation::CreateObject { object, .. } = change {
            if object.object_type == ObjectType::Comment {
                let name = &object.qualified_name.name;
                if name.starts_with("function:") {
                    assert!(
                        name.contains("myschema.process_data"),
                        "Function comment should include schema: {}",
                        name
                    );
                } else if name.starts_with("table:") {
                    assert_eq!(
                        name, "table:myschema.data_table",
                        "Table comment should include schema"
                    );
                }
            }
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_comment_without_parent_object() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create a comment without its parent object (should still parse correctly)
    env.write_sql_file("orphan_comment.sql", indoc! {r#"
        COMMENT ON TABLE nonexistent_table IS 'This table does not exist';
    "#}).await?;
    
    let plan = execute_plan(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    
    // Should have 1 change (the comment)
    assert_eq!(plan.changes.len(), 1, "Expected 1 change");
    
    if let ChangeOperation::CreateObject { object, .. } = &plan.changes[0] {
        assert_eq!(object.object_type, ObjectType::Comment);
        assert_eq!(object.qualified_name.name, "table:nonexistent_table");
    } else {
        panic!("Expected CreateObject for comment");
    }
    
    Ok(())
}

#[tokio::test]
async fn test_comment_update_without_error() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tracking
    let state_manager = pgmg::db::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // First apply: create function with comment
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE FUNCTION api.create_stripe_event() RETURNS void AS $$ 
        BEGIN 
            -- Do something
        END; 
        $$ LANGUAGE plpgsql;
        
        COMMENT ON FUNCTION api.create_stripe_event() IS 'Creates a new Stripe event';
    "#}).await?;
    
    // Create the schema first
    env.execute_sql("CREATE SCHEMA IF NOT EXISTS api").await?;
    
    // Apply it
    let apply_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(apply_result.errors.len(), 0, "First apply should succeed");
    assert_eq!(apply_result.objects_created.len(), 2, "Should create function and comment");
    
    // Update the comment only
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE FUNCTION api.create_stripe_event() RETURNS void AS $$ 
        BEGIN 
            -- Do something
        END; 
        $$ LANGUAGE plpgsql;
        
        COMMENT ON FUNCTION api.create_stripe_event() IS 'Creates and processes a new Stripe webhook event';
    "#}).await?;
    
    // Apply the update - this should NOT fail with DROP COMMENT error
    let update_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(update_result.errors.len(), 0, "Update should succeed without DROP COMMENT error");
    assert_eq!(update_result.objects_updated.len(), 1, "Should update the comment");
    
    // Verify the comment was updated in the database
    let row = env.client.query_one(
        "SELECT obj_description('api.create_stripe_event()'::regprocedure)",
        &[],
    ).await?;
    let comment_text: Option<String> = row.get(0);
    assert_eq!(comment_text, Some("Creates and processes a new Stripe webhook event".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_comment_deletion_sets_to_null() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tracking
    let state_manager = pgmg::db::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create table with comment
    env.write_sql_file("table.sql", indoc! {r#"
        CREATE TABLE users (
            id serial PRIMARY KEY,
            email text NOT NULL
        );
        
        COMMENT ON TABLE users IS 'User accounts table';
    "#}).await?;
    
    // Apply it
    let apply_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(apply_result.errors.len(), 0, "Should apply without errors");
    
    // Verify comment exists
    let row = env.client.query_one(
        "SELECT obj_description('users'::regclass)",
        &[],
    ).await?;
    let comment_text: Option<String> = row.get(0);
    assert_eq!(comment_text, Some("User accounts table".to_string()));
    
    // Remove comment from SQL file (keep table)
    env.write_sql_file("table.sql", indoc! {r#"
        CREATE TABLE users (
            id serial PRIMARY KEY,
            email text NOT NULL
        );
    "#}).await?;
    
    // Apply again
    let delete_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(delete_result.errors.len(), 0, "Should apply without errors");
    assert_eq!(delete_result.objects_deleted.len(), 1, "Should delete the comment");
    
    // Verify comment is now NULL
    let row = env.client.query_one(
        "SELECT obj_description('users'::regclass)",
        &[],
    ).await?;
    let comment_text: Option<String> = row.get(0);
    assert_eq!(comment_text, None, "Comment should be NULL after deletion");
    
    Ok(())
}

#[tokio::test]
async fn test_comment_readded_on_object_recreation() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tracking
    let state_manager = pgmg::db::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create function with comment (using parameterless function to avoid signature issues)
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE FUNCTION get_tax_rate() RETURNS numeric AS $$ 
        BEGIN 
            RETURN 0.1;
        END; 
        $$ LANGUAGE plpgsql;
        
        COMMENT ON FUNCTION get_tax_rate() IS 'Returns the current tax rate (10%)';
    "#}).await?;
    
    // Apply it
    let apply_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(apply_result.errors.len(), 0, "Should apply without errors");
    
    // Verify comment exists
    let row = env.client.query_one(
        "SELECT obj_description('get_tax_rate()'::regprocedure)",
        &[],
    ).await?;
    let comment_text: Option<String> = row.get(0);
    assert_eq!(comment_text, Some("Returns the current tax rate (10%)".to_string()));
    
    // Modify function body (forces drop/recreate)
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE FUNCTION get_tax_rate() RETURNS numeric AS $$ 
        BEGIN 
            -- Updated to 15% tax rate
            RETURN 0.15;
        END; 
        $$ LANGUAGE plpgsql;
        
        COMMENT ON FUNCTION get_tax_rate() IS 'Returns the current tax rate (10%)';
    "#}).await?;
    
    // Apply the update
    let update_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(update_result.errors.len(), 0, "Should apply without errors");
    assert_eq!(update_result.objects_updated.len(), 2, "Should update both function and comment");
    
    // Verify comment is still present after recreation
    let row = env.client.query_one(
        "SELECT obj_description('get_tax_rate()'::regprocedure)",
        &[],
    ).await?;
    let comment_text: Option<String> = row.get(0);
    assert_eq!(comment_text, Some("Returns the current tax rate (10%)".to_string()),
        "Comment should be preserved after function recreation");
    
    Ok(())
}

#[tokio::test]
async fn test_comment_update_different_object_types() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tracking
    let state_manager = pgmg::db::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Test with different object types to ensure comment updates work for all
    env.write_sql_file("objects.sql", indoc! {r#"
        -- Type with comment
        CREATE TYPE status AS ENUM ('active', 'inactive');
        COMMENT ON TYPE status IS 'Original type comment';
        
        -- Domain with comment
        CREATE DOMAIN email AS text CHECK (value ~ '^[^@]+@[^@]+\.[^@]+$');
        COMMENT ON DOMAIN email IS 'Original domain comment';
        
        -- View with comment
        CREATE VIEW active_items AS SELECT 1 as id;
        COMMENT ON VIEW active_items IS 'Original view comment';
    "#}).await?;
    
    // Apply
    let apply_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(apply_result.errors.len(), 0, "Should apply without errors");
    
    // Update all comments
    env.write_sql_file("objects.sql", indoc! {r#"
        -- Type with comment
        CREATE TYPE status AS ENUM ('active', 'inactive');
        COMMENT ON TYPE status IS 'Updated type comment';
        
        -- Domain with comment
        CREATE DOMAIN email AS text CHECK (value ~ '^[^@]+@[^@]+\.[^@]+$');
        COMMENT ON DOMAIN email IS 'Updated domain comment';
        
        -- View with comment
        CREATE VIEW active_items AS SELECT 1 as id;
        COMMENT ON VIEW active_items IS 'Updated view comment';
    "#}).await?;
    
    // Apply updates
    let update_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(update_result.errors.len(), 0, "All comment updates should succeed");
    assert_eq!(update_result.objects_updated.len(), 3, "Should update 3 comments");
    
    Ok(())
}

#[tokio::test]
async fn test_function_with_parameters_update() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Initialize state tracking
    let state_manager = pgmg::db::StateManager::new(&env.client);
    state_manager.initialize().await?;
    
    // Create required schema and tables
    env.execute_sql("CREATE SCHEMA IF NOT EXISTS api").await?;
    env.execute_sql("CREATE SCHEMA IF NOT EXISTS mq").await?;
    env.execute_sql("CREATE TABLE stripe_event (event_id text PRIMARY KEY, event_type text, object_id text)").await?;
    env.execute_sql("CREATE TABLE mq.task (task_name text, payload jsonb)").await?;
    
    // First apply: create function with parameters
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE OR REPLACE FUNCTION api.create_stripe_event(
            p_event_id   text,
            p_event_type text,
            p_payload    jsonb,
            p_object_id  text default null
        ) RETURNS void
        LANGUAGE sql
        VOLATILE AS $$
        INSERT INTO stripe_event (event_id, event_type, object_id)
        VALUES (p_event_id, p_event_type, p_object_id);
        
        INSERT INTO mq.task (task_name, payload)
        VALUES (p_event_type, p_payload);
        $$;
        
        COMMENT ON FUNCTION api.create_stripe_event IS 'Records Stripe webhook events';
    "#}).await?;
    
    // Apply it
    let apply_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(apply_result.errors.len(), 0, "First apply should succeed");
    
    // Update the function body
    env.write_sql_file("function.sql", indoc! {r#"
        CREATE OR REPLACE FUNCTION api.create_stripe_event(
            p_event_id   text,
            p_event_type text,
            p_payload    jsonb,
            p_object_id  text default null
        ) RETURNS void
        LANGUAGE sql
        VOLATILE AS $$
        -- Updated implementation
        INSERT INTO stripe_event (event_id, event_type, object_id)
        VALUES (p_event_id, p_event_type, COALESCE(p_object_id, 'unknown'));
        
        INSERT INTO mq.task (task_name, payload)
        VALUES (p_event_type, jsonb_build_object('event_id', p_event_id, 'data', p_payload));
        $$;
        
        COMMENT ON FUNCTION api.create_stripe_event IS 'Records Stripe webhook events';
    "#}).await?;
    
    // Apply the update - this should NOT fail with "function does not exist"
    let update_result = pgmg::commands::execute_apply(
        None,
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &Default::default(),
    ).await?;
    
    assert_eq!(update_result.errors.len(), 0, "Update should succeed without 'function does not exist' error");
    assert!(update_result.objects_updated.len() > 0, "Should update the function");
    
    // Verify the function exists and works
    let result = env.client.execute(
        "SELECT api.create_stripe_event('test_123', 'payment.succeeded', '{\"amount\": 100}'::jsonb, null)",
        &[],
    ).await?;
    assert_eq!(result, 1, "Function should execute successfully");
    
    Ok(())
}