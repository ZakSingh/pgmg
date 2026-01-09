//! Analyze migration SQL to extract tables affected by ALTER TABLE statements.
//!
//! This module helps identify which managed objects need to be pre-dropped
//! before migrations that alter tables they depend on.

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
}
