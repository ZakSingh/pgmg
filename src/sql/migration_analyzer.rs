//! Analyze migration SQL to extract tables affected by ALTER TABLE statements
//! and enum values added by ALTER TYPE ... ADD VALUE statements.
//!
//! This module helps identify which managed objects need to be pre-dropped
//! before migrations that alter tables they depend on, and which enum ADD VALUE
//! statements need to be pre-committed outside a transaction to avoid PostgreSQL's
//! "unsafe use of new value" error.

use std::collections::HashSet;
use crate::sql::QualifiedIdent;
use pg_query::NodeEnum;

/// Extract tables affected by ALTER TABLE statements in migration SQL.
///
/// Returns a set of qualified table names that are being altered.
/// This is used to find managed objects (views, functions, etc.) that
/// depend on these tables and need to be pre-dropped before the migration.
pub fn extract_altered_tables(sql: &str) -> Result<HashSet<QualifiedIdent>, Box<dyn std::error::Error>> {
    let parsed = pg_query::parse(sql)?;
    let mut tables = HashSet::new();

    for stmt in &parsed.protobuf.stmts {
        if let Some(node) = &stmt.stmt {
            if let Some(NodeEnum::AlterTableStmt(alter)) = &node.node {
                if let Some(relation) = &alter.relation {
                    let schema = if relation.schemaname.is_empty() {
                        None
                    } else {
                        Some(relation.schemaname.clone())
                    };
                    tables.insert(QualifiedIdent::new(schema, relation.relname.clone()));
                }
            }
        }
    }

    Ok(tables)
}

/// Extract `ALTER TYPE ... ADD VALUE` statements from migration SQL and return
/// them rewritten with `IF NOT EXISTS`.
///
/// These statements cannot be used inside a transaction alongside statements that
/// reference the new enum value. By extracting and pre-committing them before the
/// main migration transaction, we avoid PostgreSQL's "unsafe use of new value" error.
///
/// Returns a vec of (original_sql, rewritten_sql) pairs. The original SQL is used
/// to identify which statements to skip during the main migration, and the rewritten
/// SQL (with IF NOT EXISTS) is executed in the pre-commit phase for idempotency.
pub fn extract_enum_add_value_statements(sql: &str) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let parsed = pg_query::parse(sql)?;
    let mut results = Vec::new();

    for stmt in &parsed.protobuf.stmts {
        if let Some(node) = &stmt.stmt {
            if let Some(NodeEnum::AlterEnumStmt(alter_enum)) = &node.node {
                // Only handle ADD VALUE (new_val is non-empty, old_val is empty)
                if !alter_enum.new_val.is_empty() && alter_enum.old_val.is_empty() {
                    // Get the original statement text
                    let original = NodeEnum::AlterEnumStmt(alter_enum.clone()).deparse()?;

                    // Create a modified version with IF NOT EXISTS
                    let mut modified = alter_enum.clone();
                    modified.skip_if_new_val_exists = true;
                    let rewritten = NodeEnum::AlterEnumStmt(modified).deparse()?;

                    results.push((original, rewritten));
                }
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alter_column_type() {
        let sql = r#"ALTER TABLE "order" ALTER COLUMN seller_id TYPE bigint;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(None, "order".to_string())));
    }

    #[test]
    fn test_alter_column_type_with_schema() {
        let sql = r#"ALTER TABLE public."order" ALTER COLUMN seller_id TYPE bigint;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(Some("public".to_string()), "order".to_string())));
    }

    #[test]
    fn test_alter_table_drop_column() {
        let sql = r#"ALTER TABLE users DROP COLUMN old_field;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(None, "users".to_string())));
    }

    #[test]
    fn test_alter_table_add_column() {
        let sql = r#"ALTER TABLE api.users ADD COLUMN new_field text;"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
    }

    #[test]
    fn test_multiple_alter_tables() {
        let sql = r#"
            ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,2);
            ALTER TABLE api.users ADD COLUMN status text;
            ALTER TABLE products DROP COLUMN deprecated_field;
        "#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&QualifiedIdent::new(None, "orders".to_string())));
        assert!(tables.contains(&QualifiedIdent::new(Some("api".to_string()), "users".to_string())));
        assert!(tables.contains(&QualifiedIdent::new(None, "products".to_string())));
    }

    #[test]
    fn test_no_alter_tables() {
        let sql = r#"
            CREATE TABLE new_table (id serial primary key);
            INSERT INTO users (name) VALUES ('test');
        "#;
        let tables = extract_altered_tables(sql).unwrap();

        assert!(tables.is_empty());
    }

    #[test]
    fn test_alter_table_add_constraint() {
        let sql = r#"ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id);"#;
        let tables = extract_altered_tables(sql).unwrap();

        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&QualifiedIdent::new(None, "orders".to_string())));
    }

    #[test]
    fn test_extract_enum_add_value() {
        let sql = r#"ALTER TYPE "public"."enum_status" ADD VALUE 'active' BEFORE 'inactive';"#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert_eq!(results.len(), 1);
        // The rewritten SQL should contain IF NOT EXISTS
        assert!(results[0].1.to_uppercase().contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_extract_enum_add_value_already_if_not_exists() {
        let sql = r#"ALTER TYPE status ADD VALUE IF NOT EXISTS 'active';"#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].1.to_uppercase().contains("IF NOT EXISTS"));
    }

    #[test]
    fn test_extract_enum_add_value_multiple() {
        let sql = r#"
            ALTER TYPE "public"."enum_status" ADD VALUE 'active' BEFORE 'inactive';
            ALTER TABLE users ADD COLUMN status text;
            ALTER TYPE "public"."enum_role" ADD VALUE 'admin';
        "#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_extract_enum_add_value_none_present() {
        let sql = r#"
            ALTER TABLE users ADD COLUMN status text;
            CREATE TABLE orders (id serial primary key);
        "#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_extract_enum_rename_value_not_extracted() {
        // RENAME VALUE should not be extracted - it's not ADD VALUE
        let sql = r#"ALTER TYPE status RENAME VALUE 'old' TO 'new';"#;
        let results = extract_enum_add_value_statements(sql).unwrap();

        assert!(results.is_empty());
    }
}
