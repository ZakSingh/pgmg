use indoc::indoc;
use pgmg::sql::ObjectType;
use pgmg::commands::{execute_plan, execute_apply};
use pgmg::config::PgmgConfig;

mod common;
use common::TestEnvironment;
use common::assertions::{assert_plan_contains_create, assert_plan_contains_update, assert_plan_contains_delete};

#[tokio::test]
async fn test_basic_operator_creation() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create the implementation function first
    env.write_sql_file("distance_func.sql", indoc! {r#"
        CREATE FUNCTION distance(point, point) RETURNS float8
        AS 'SELECT sqrt(($1[0] - $2[0])^2 + ($1[1] - $2[1])^2)'
        LANGUAGE sql IMMUTABLE;
    "#}).await?;
    
    // Apply the function
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Create the operator
    env.write_sql_file("distance_op.sql", indoc! {r#"
        CREATE OPERATOR <-> (
            LEFTARG = point,
            RIGHTARG = point,
            FUNCTION = distance,
            COMMUTATOR = <->
        );
    "#}).await?;
    
    // Plan should detect the new operator
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert_plan_contains_create(&plan, ObjectType::Operator, "<->");
    
    // Apply the operator
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Verify operator exists
    let exists: bool = env.query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_operator o
            JOIN pg_namespace n ON n.oid = o.oprnamespace
            WHERE n.nspname = 'public' AND o.oprname = '<->'
        )"
    ).await?;
    assert!(exists);
    
    Ok(())
}

#[tokio::test]
async fn test_operator_with_type_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create custom types
    env.write_sql_file("types.sql", indoc! {r#"
        CREATE TYPE currency AS (
            amount numeric,
            code text
        );
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Create the implementation function
    env.write_sql_file("currency_func.sql", indoc! {r#"
        CREATE FUNCTION add_currency(currency, currency) RETURNS currency AS $$
            SELECT ROW($1.amount + $2.amount, $1.code)::currency
        $$ LANGUAGE sql IMMUTABLE;
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Create the operator
    env.write_sql_file("currency_op.sql", indoc! {r#"
        CREATE OPERATOR + (
            LEFTARG = currency,
            RIGHTARG = currency,
            FUNCTION = add_currency
        );
    "#}).await?;
    
    // Apply and verify
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    let exists: bool = env.query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_operator o
            JOIN pg_namespace n ON n.oid = o.oprnamespace
            WHERE n.nspname = 'public' AND o.oprname = '+'
            AND o.oprleft = 'currency'::regtype AND o.oprright = 'currency'::regtype
        )"
    ).await?;
    assert!(exists);
    
    Ok(())
}

#[tokio::test]
async fn test_operator_update() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create function and operator
    env.write_sql_file("func.sql", indoc! {r#"
        CREATE FUNCTION my_eq(text, text) RETURNS boolean
        AS 'SELECT $1 = $2'
        LANGUAGE sql IMMUTABLE;
    "#}).await?;
    
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR === (
            LEFTARG = text,
            RIGHTARG = text,
            FUNCTION = my_eq
        );
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Update the operator (add commutator)
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR === (
            LEFTARG = text,
            RIGHTARG = text,
            FUNCTION = my_eq,
            COMMUTATOR = ===
        );
    "#}).await?;
    
    // Plan should detect the update
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert_plan_contains_update(&plan, ObjectType::Operator, "===");
    
    // Apply the update
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_operator_deletion() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create function and operator
    env.write_sql_file("func.sql", indoc! {r#"
        CREATE FUNCTION my_func(int, int) RETURNS int
        AS 'SELECT $1 + $2'
        LANGUAGE sql;
    "#}).await?;
    
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR ++ (
            LEFTARG = int,
            RIGHTARG = int,
            FUNCTION = my_func
        );
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Delete the operator file
    env.delete_sql_file("op.sql").await?;
    
    // Plan should detect the deletion
    let plan = execute_plan(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        None,
    ).await?;
    assert_plan_contains_delete(&plan, ObjectType::Operator, "++");
    
    // Apply the deletion
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Verify operator is gone
    let exists: bool = env.query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_operator
            WHERE oprname = '++'
        )"
    ).await?;
    assert!(!exists);
    
    Ok(())
}

#[tokio::test]
async fn test_operator_comment() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create function and operator
    env.write_sql_file("func.sql", indoc! {r#"
        CREATE FUNCTION distance_squared(point, point) RETURNS float8
        AS 'SELECT ($1[0] - $2[0])^2 + ($1[1] - $2[1])^2'
        LANGUAGE sql IMMUTABLE;
    "#}).await?;
    
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR <#> (
            LEFTARG = point,
            RIGHTARG = point,
            FUNCTION = distance_squared
        );
        
        COMMENT ON OPERATOR <#> (point, point) IS 'Squared distance between two points';
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Verify comment exists
    let comment: Option<String> = env.query_scalar(
        "SELECT obj_description(o.oid, 'pg_operator')
         FROM pg_operator o
         WHERE o.oprname = '<#>'"
    ).await?;
    assert_eq!(comment, Some("Squared distance between two points".to_string()));
    
    // Update the comment
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR <#> (
            LEFTARG = point,
            RIGHTARG = point,
            FUNCTION = distance_squared
        );
        
        COMMENT ON OPERATOR <#> (point, point) IS 'Euclidean distance squared';
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Verify comment was updated
    let comment: Option<String> = env.query_scalar(
        "SELECT obj_description(o.oid, 'pg_operator')
         FROM pg_operator o
         WHERE o.oprname = '<#>'"
    ).await?;
    assert_eq!(comment, Some("Euclidean distance squared".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_prefix_operator() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create function for prefix operator
    env.write_sql_file("func.sql", indoc! {r#"
        CREATE FUNCTION factorial(bigint) RETURNS numeric
        AS $$
            SELECT CASE 
                WHEN $1 = 0 THEN 1::numeric
                ELSE $1 * factorial($1 - 1)
            END
        $$ LANGUAGE sql;
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Create prefix operator
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR ! (
            RIGHTARG = bigint,
            FUNCTION = factorial
        );
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Test the operator - verify it exists
    let exists: bool = env.query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_operator o
            WHERE o.oprname = '!' 
            AND o.oprleft IS NULL 
            AND o.oprright = 'bigint'::regtype
        )"
    ).await?;
    assert!(exists, "Prefix operator ! should exist for bigint");
    
    Ok(())
}

#[tokio::test] 
async fn test_operator_with_schema() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::new().await?;
    
    // Create schema
    env.execute_sql("CREATE SCHEMA myschema").await?;
    
    // Create function in schema
    env.write_sql_file("func.sql", indoc! {r#"
        CREATE FUNCTION myschema.my_concat(text, text) RETURNS text
        AS 'SELECT $1 || $2'
        LANGUAGE sql;
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Create operator in schema
    env.write_sql_file("op.sql", indoc! {r#"
        CREATE OPERATOR myschema.|| (
            LEFTARG = text,
            RIGHTARG = text,
            FUNCTION = myschema.my_concat
        );
    "#}).await?;
    
    execute_apply(
        Some(env.migrations_dir.clone()),
        Some(env.sql_dir.clone()),
        env.connection_string.clone(),
        &PgmgConfig::default(),
    ).await?;
    
    // Verify operator exists in correct schema
    let exists: bool = env.query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_operator o
            JOIN pg_namespace n ON n.oid = o.oprnamespace
            WHERE n.nspname = 'myschema' AND o.oprname = '||'
        )"
    ).await?;
    assert!(exists);
    
    Ok(())
}